#!/usr/bin/env python3
"""
Seed all test databases with stress-test data.
Schema: customers (1M rows) + orders (10M rows) per DB.
Run after containers are healthy:  python3 seed_data.py [--db vertica|iris|...]

pip install faker vertica-python sqlalchemy-iris pyexasol voltdb3 jaydebeapi
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
    import iris  # pip install sqlalchemy-iris installs the driver
    import sqlalchemy
    engine = sqlalchemy.create_engine(f"iris://_SYSTEM:{password}@{host}:1972/USER")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT NOT NULL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                status VARCHAR(20)
            )"""))
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT NOT NULL PRIMARY KEY,
                customer_id INT,
                amount NUMERIC(10,2),
                status VARCHAR(20),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"""))
    # IRIS needs row-by-row via ODBC for large inserts; use pyodbc batch approach
    print("IRIS schema created — use bulk load or the IRIS %SQL.Statement for large inserts.")


# ── Exasol ─────────────────────────────────────────────────────────────────────
def seed_exasol(host, password):
    import pyexasol
    with pyexasol.connect(dsn=f"{host}:9563", user='sys', password=password) as conn:
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
    # VoltDB uses its own Java/Python client; DDL via sqlcmd or REST
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
    import jaydebeapi
    conn = jaydebeapi.connect("org.h2.Driver",
                              f"jdbc:h2:tcp://{host}:9092/~/test",
                              ["sa", ""], "/opt/h2-data/h2.jar")
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


DB_SEEDERS = {
    "vertica": seed_vertica,
    "exasol":  seed_exasol,
    "h2":      seed_h2,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, choices=list(DB_SEEDERS) + ["iris","voltdb"])
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
    print(f"Done in {time.time()-start:.1f}s")
