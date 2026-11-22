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


def jdbc_row(row):
    """jaydebeapi can't bind Python datetimes; send them as timestamp strings."""
    return tuple(v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v for v in row)


def insert_batches(cursor, sql, generator, total, label, convert=None):
    batch = []
    done = 0
    for row in generator(total):
        if convert:
            row = convert(row)
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
    # URL.create keeps '@' etc. in the password intact; an f-string URL breaks on it.
    url = sqlalchemy.engine.URL.create(
        "iris", username="_SYSTEM", password=password,
        host=host, port=1972, database="USER")
    engine = sqlalchemy.create_engine(url, connect_args={"timeout": 30})
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

    # Start from empty tables so a re-run after a partial failure can't hit primary-key conflicts.
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE TABLE SQLUser.customers"))
        conn.execute(sqlalchemy.text("TRUNCATE TABLE SQLUser.orders"))

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
        # No primary keys here, so a re-run would append duplicates without this
        conn.execute("TRUNCATE TABLE customers")
        conn.execute("TRUNCATE TABLE orders")
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
    # The client jar must match the server (the oscarfonts/h2 container ships 2.1.214).
    jar = os.environ.get("H2_JAR", "/opt/h2-data/h2-2.1.214.jar")
    if not os.path.exists(jar):
        os.makedirs(os.path.dirname(jar), exist_ok=True)
        print("Downloading H2 jar...")
        urllib.request.urlretrieve(
            "https://repo1.maven.org/maven2/com/h2database/h2/2.1.214/h2-2.1.214.jar", jar)
    # Host 9092 maps to the container's 1521. The server runs with -baseDir /opt/h2-data and
    # -ifNotExists, so the database name is relative to that directory.
    conn = jaydebeapi.connect("org.h2.Driver",
                              f"jdbc:h2:tcp://{host}:9092/test",
                              ["sa", ""], jar)
    conn.jconn.setAutoCommit(False)
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
    cur.execute("TRUNCATE TABLE customers")
    cur.execute("TRUNCATE TABLE orders")
    insert_batches(cur, "INSERT INTO customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers", convert=jdbc_row)
    insert_batches(cur, "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders", convert=jdbc_row)
    conn.commit()
    conn.close()
    print("H2 seeded.")


# ── CUBRID ──────────────────────────────────────────────────────────────────────
def seed_cubrid(host, password):
    # CUBRID-Python (C extension) fails on Python 3.14 with "Invalid connection handle",
    # so go through the JDBC driver instead (seed_all.sh copies the jar out of the container).
    import jaydebeapi, os
    jar = os.environ.get("CUBRID_JDBC_JAR", "/opt/sdk-test-dbs/jars/cubrid_jdbc.jar")
    conn = jaydebeapi.connect("cubrid.jdbc.driver.CUBRIDDriver",
                              f"jdbc:cubrid:{host}:33000:testdb:::", ["dba", password], jar)
    conn.jconn.setAutoCommit(False)
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
    cur.execute("TRUNCATE TABLE customers")
    cur.execute("TRUNCATE TABLE orders")
    insert_batches(cur, "INSERT INTO customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers", convert=jdbc_row)
    insert_batches(cur, "INSERT INTO orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders", convert=jdbc_row)
    conn.commit()
    conn.close()
    print("CUBRID seeded.")


# ── Mimer ───────────────────────────────────────────────────────────────────────
def seed_mimer(host, password):
    import mimerpy
    conn = mimerpy.connect(dsn="mimerdb", user="SYSADM", password=password)
    conn.autocommit = True
    cur = conn.cursor()
    # Mimer has no CREATE TABLE IF NOT EXISTS (IF is a reserved word), so check the catalog first.
    # SYSADM has no default databank, so tables go explicitly IN SEEDDB (the built-in transaction databank).
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.EXT_DATABANKS WHERE DATABANK_NAME = 'SEEDDB'")
    if cur.fetchone()[0] == 0:
        cur.execute("CREATE DATABANK SEEDDB")

    def create_if_missing(table, ddl):
        cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = 'SYSADM' AND TABLE_NAME = ?", (table.upper(),))
        if cur.fetchone()[0] == 0:
            cur.execute(ddl)

    create_if_missing("customers", """
        CREATE TABLE SYSADM.customers (
            id INTEGER PRIMARY KEY, name NVARCHAR(100), email NVARCHAR(200),
            created_at TIMESTAMP(6), updated_at TIMESTAMP(6), status NVARCHAR(20)
        ) IN SEEDDB""")
    create_if_missing("orders", """
        CREATE TABLE SYSADM.orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2),
            status NVARCHAR(20), created_at TIMESTAMP(6), updated_at TIMESTAMP(6)
        ) IN SEEDDB""")
    cur.execute("DELETE FROM SYSADM.customers")
    cur.execute("DELETE FROM SYSADM.orders")
    # Mimer doesn't support executemany well; use autocommit + individual inserts in batches
    insert_batches(cur, "INSERT INTO SYSADM.customers VALUES (?,?,?,?,?,?)",
                   customer_rows, CUST_COUNT, "customers")
    insert_batches(cur, "INSERT INTO SYSADM.orders VALUES (?,?,?,?,?,?)",
                   order_rows, ORD_COUNT, "orders")
    conn.close()
    print("Mimer seeded.")


# ── Geode ───────────────────────────────────────────────────────────────────────
# The dev REST API runs on the cache server; the locator in the same container already owns 7070.
GEODE_REST_PORT = 8081
# Keys per multi-key PUT; keeps the comma-separated key list well under Jetty's 8KB header limit.
GEODE_BATCH = 500


def create_geode_regions():
    # The dev REST API can't create regions, so use gfsh inside the container.
    import subprocess
    names = subprocess.run(["docker", "ps", "--filter", "name=geode", "--format", "{{.Names}}"],
                           capture_output=True, text=True, check=True).stdout.split()
    if not names:
        raise RuntimeError("No running Geode container found")
    cmd = ["docker", "exec", names[0], "gfsh", "-e", "connect --locator=localhost[10334]"]
    for region in ("customers", "orders"):
        cmd += ["-e", f"create region --name={region} --type=REPLICATE_PERSISTENT --if-not-exists"]
        # Start empty so re-runs don't leave stale or stray entries behind
        cmd += ["-e", f"remove --region=/{region} --all"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"gfsh region creation failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")


def seed_geode(host):
    import json
    import requests
    base = f"http://{host}:{GEODE_REST_PORT}/geode/v1"
    s = requests.Session()

    deadline = time.time() + 180
    while True:
        try:
            s.get(base, timeout=5)
            break
        except requests.ConnectionError:
            if time.time() > deadline:
                raise RuntimeError(f"Geode REST API not reachable at {base} after 3 minutes")
            print("  waiting for Geode REST API...")
            time.sleep(10)

    create_geode_regions()

    def load(region, rows, total, to_doc):
        done, batch = 0, []
        for row in rows:
            batch.append(to_doc(row))
            if len(batch) == GEODE_BATCH or done + len(batch) == total:
                keys = ",".join(str(d["id"]) for d in batch)
                r = s.put(f"{base}/{region}/{keys}", data=json.dumps(batch),
                          headers={"Content-Type": "application/json"}, timeout=120)
                if not r.ok:
                    raise RuntimeError(f"PUT {region} failed ({r.status_code}): {r.text[:500]}")
                done += len(batch)
                batch = []
                if done % 100_000 == 0:
                    print(f"  {region}: {done:,}/{total:,}")
        print(f"  {region}: {done:,} entries done")

    load("customers", customer_rows(CUST_COUNT), CUST_COUNT, lambda r: {
        "id": r[0], "name": r[1], "email": r[2],
        "created_at": r[3].isoformat(), "updated_at": r[4].isoformat(), "status": r[5]})
    load("orders", order_rows(ORD_COUNT), ORD_COUNT, lambda r: {
        "id": r[0], "customer_id": r[1], "amount": float(r[2]), "status": r[3],
        "created_at": r[4].isoformat(), "updated_at": r[5].isoformat()})
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
