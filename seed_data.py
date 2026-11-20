#!/usr/bin/env python3
"""
Seed all test databases with stress-test data.
Schema: customers (1M rows) + orders (10M rows) per DB.
Run after containers are healthy:  python3 seed_data.py [--db vertica|iris|...]

pip install faker vertica-python sqlalchemy-iris pyexasol jaydebeapi CUBRIDdb mimerpy requests
"""
import argparse
import random
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

START_DATE = datetime(2022, 1, 1)
END_DATE   = datetime(2024, 12, 31)
CUST_COUNT = 1_000_000
ORD_COUNT  = 10_000_000
BATCH      = 10_000

STATUSES  = ['active', 'inactive', 'churned']
ORD_STATS = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'refunded']


def rdate(start=START_DATE, end=END_DATE):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def customer_rows(n=CUST_COUNT):
    for i in range(1, n + 1):
        yield (i, fake.name()[:100], f"user{i}@example.com",
               rdate(), rdate(), random.choice(STATUSES))


def order_rows(n=ORD_COUNT):
    for i in range(1, n + 1):
        created = rdate()
        yield (i, random.randint(1, CUST_COUNT),
               round(random.uniform(1.00, 4999.99), 2),
               random.choice(ORD_STATS),
               created,
               created + timedelta(hours=random.randint(0, 72)))


def insert_batches(cursor, sql, generator, total, label):
    batch = []
    done = 0
    for row in generator(total):
        batch.append(row)
        if len(batch) >= BATCH:
            cursor.executemany(sql, batch)
            done += len(batch)
            batch = []
            if done % 100_000 == 0:
                print(f"  {label}: {done:,}/{total:,}")
    if batch:
        cursor.executemany(sql, batch)
        done += len(batch)
    print(f"  {label}: {done:,} rows done")


# ── Vertica ────────────────────────────────────────────────────────────────────
def seed_vertica(host, password):
    import vertica_python
    conn_info = dict(host=host, port=5433, user='dbadmin', password=password, database='')
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id          INT NOT NULL,
                name        VARCHAR(100),
                email       VARCHAR(200) UNIQUE,
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP,
                status      VARCHAR(20)
            ) ORDER BY id SEGMENTED BY HASH(id) ALL NODES""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id          INT NOT NULL,
                customer_id INT,
                amount      NUMERIC(10,2),
                status      VARCHAR(20),
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP
            ) ORDER BY id SEGMENTED BY HASH(id) ALL NODES""")
        conn.commit()
        insert_batches(cur, "INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s)",
                       customer_rows, CUST_COUNT, "customers")
        conn.commit()
        insert_batches(cur, "INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s)",
                       order_rows, ORD_COUNT, "orders")
        conn.commit()
    print("Vertica seeded.")


# ── InterSystems IRIS ──────────────────────────────────────────────────────────
def seed_iris(host, password):
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        f"iris://_SYSTEM:{password}@{host}:1972/USER",
        connect_args={"timeout": 30}
    )
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS SQLUser.customers (
                id INT NOT NULL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                status VARCHAR(20)
            )"""))
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS SQLUser.orders (
                id INT NOT NULL PRIMARY KEY,
                customer_id INT,
                amount NUMERIC(10,2),
                status VARCHAR(20),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"""))

    # Insert in batches using raw connection
    raw = engine.raw_connection()
    cur = raw.cursor()
    insert_batches(cur,
        "INSERT INTO SQLUser.customers VALUES (?,?,?,?,?,?)",
        customer_rows, CUST_COUNT, "customers")
    insert_batches(cur,
        "INSERT INTO SQLUser.orders VALUES (?,?,?,?,?,?)",
        order_rows, ORD_COUNT, "orders")
    raw.commit()
    raw.close()
    print("IRIS seeded.")


# ── Exasol ─────────────────────────────────────────────────────────────────────
def seed_exasol(host, password):
    import pyexasol
    with pyexasol.connect(dsn=f"{host}:8563", user='sys', password=password,
                          websocket_sslopt={"cert_reqs": 0}) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS testdb")
        conn.execute("OPEN SCHEMA testdb")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT, name VARCHAR(100), email VARCHAR(200),
                created_at TIMESTAMP, updated_at TIMESTAMP, status VARCHAR(20)
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT, customer_id INT, amount DECIMAL(10,2),
                status VARCHAR(20), created_at TIMESTAMP, updated_at TIMESTAMP
            )""")
        conn.import_from_iterable(customer_rows(CUST_COUNT), "customers")
        conn.import_from_iterable(order_rows(ORD_COUNT),    "orders")
    print("Exasol seeded.")


# ── VoltDB ─────────────────────────────────────────────────────────────────────
def seed_voltdb(host, password):
    import requests
    base = f"http://{host}:8080/api/1.0/"
    def sql(q):
        r = requests.post(base, data={"Procedure": "@AdHoc", "Parameters": f'["{q}"]'})
        return r.json()
    sql("CREATE TABLE customers (id INT NOT NULL, name VARCHAR(100), email VARCHAR(200), "
        "created_at TIMESTAMP, updated_at TIMESTAMP, status VARCHAR(20), PRIMARY KEY(id))")
    sql("PARTITION TABLE customers ON COLUMN id")
    sql("CREATE TABLE orders (id INT NOT NULL, customer_id INT, amount FLOAT, "
        "status VARCHAR(20), created_at TIMESTAMP, updated_at TIMESTAMP, PRIMARY KEY(id))")
    sql("PARTITION TABLE orders ON COLUMN id")
    print("VoltDB schema created — use voltdb bulk loader CSV for 10M rows.")


# ── H2 ─────────────────────────────────────────────────────────────────────────
def seed_h2(host):
    import jaydebeapi, os, urllib.request
    jar = "/opt/h2-data/h2.jar"
    if not os.path.exists(jar):
        os.makedirs("/opt/h2-data", exist_ok=True)
        print("Downloading H2 jar...")
        urllib.request.urlretrieve(
            "https://repo1.maven.org/maven2/com/h2database/h2/2.2.224/h2-2.2.224.jar", jar)
    conn = jaydebeapi.connect("org.h2.Driver",
                              f"jdbc:h2:tcp://{host}:9092//opt/h2-data/test",
                              ["sa", ""], jar)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200),
            created_at TIMESTAMP, updated_at TIMESTAMP, status VARCHAR(20)
        )""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2),
            status VARCHAR(20), created_at TIMESTAMP, updated_at TIMESTAMP
        )""")
    insert_batches(cur, "INSERT INTO customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers")
    insert_batches(cur, "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders")
    conn.commit()
    conn.close()
    print("H2 seeded.")


# ── CUBRID ──────────────────────────────────────────────────────────────────────
def seed_cubrid(host, password):
    import CUBRIDdb
    conn = CUBRIDdb.connect(f"CUBRID:{host}:33000:testdb:::", "dba", password)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200),
            created_at DATETIME, updated_at DATETIME, status VARCHAR(20)
        )""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT PRIMARY KEY, customer_id INT, amount NUMERIC(10,2),
            status VARCHAR(20), created_at DATETIME, updated_at DATETIME
        )""")
    insert_batches(cur, "INSERT INTO customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers")
    insert_batches(cur, "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders")
    conn.commit()
    conn.close()
    print("CUBRID seeded.")


# ── Mimer ───────────────────────────────────────────────────────────────────────
def seed_mimer(host, password):
    import mimerpy
    conn = mimerpy.connect(dsn=f"{host}:1360", user="SYSADM", password=password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS SYSADM.customers (
            id INTEGER PRIMARY KEY, name NVARCHAR(100), email NVARCHAR(200),
            created_at TIMESTAMP(6), updated_at TIMESTAMP(6), status NVARCHAR(20)
        )""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS SYSADM.orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2),
            status NVARCHAR(20), created_at TIMESTAMP(6), updated_at TIMESTAMP(6)
        )""")
    # Mimer doesn't support executemany well; use autocommit + individual inserts in batches
    insert_batches(cur, "INSERT INTO SYSADM.customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers")
    insert_batches(cur, "INSERT INTO SYSADM.orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders")
    conn.close()
    print("Mimer seeded.")


# ── Geode ───────────────────────────────────────────────────────────────────────
def seed_geode(host):
    import requests
    base = f"http://{host}:7070/geode/v1"

    # Create regions (idempotent — ignore 409 conflict)
    for region in ("customers", "orders"):
        r = requests.post(f"{base}/regions",
                          json={"name": region, "type": "REPLICATE_PERSISTENT"})
        if r.status_code not in (200, 201, 409):
            r.raise_for_status()

    print("Seeding Geode customers...")
    for i, row in enumerate(customer_rows(CUST_COUNT), 1):
        id_, name, email, created_at, updated_at, status = row
        requests.put(f"{base}/customers/{id_}", json={
            "id": id_, "name": name, "email": email,
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
            "status": status
        })
        if i % 10_000 == 0:
            print(f"  customers: {i:,}/{CUST_COUNT:,}")

    print("Seeding Geode orders...")
    for i, row in enumerate(order_rows(ORD_COUNT), 1):
        id_, customer_id, amount, status, created_at, updated_at = row
        requests.put(f"{base}/orders/{id_}", json={
            "id": id_, "customer_id": customer_id,
            "amount": float(amount), "status": status,
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat()
        })
        if i % 100_000 == 0:
            print(f"  orders: {i:,}/{ORD_COUNT:,}")

    print("Geode seeded.")


if __name__ == "__main__":
    ALL_DBS = ["vertica", "iris", "exasol", "voltdb", "h2", "cubrid", "mimer", "geode"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, choices=ALL_DBS)
    parser.add_argument("--host", default="<VM_STATIC_IP>")
    parser.add_argument("--password", default="")
    parser.add_argument("--customers", type=int, default=CUST_COUNT)
    parser.add_argument("--orders", type=int, default=ORD_COUNT)
    args = parser.parse_args()

    CUST_COUNT = args.customers
    ORD_COUNT  = args.orders

    start = time.time()
    if args.db == "vertica":
        seed_vertica(args.host, args.password)
    elif args.db == "iris":
        seed_iris(args.host, args.password)
    elif args.db == "exasol":
        seed_exasol(args.host, args.password)
    elif args.db == "voltdb":
        seed_voltdb(args.host, args.password)
    elif args.db == "h2":
        seed_h2(args.host)
    elif args.db == "cubrid":
        seed_cubrid(args.host, args.password)
    elif args.db == "mimer":
        seed_mimer(args.host, args.password)
    elif args.db == "geode":
        seed_geode(args.host)
    print(f"Done in {time.time()-start:.1f}s")
