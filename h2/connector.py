import jaydebeapi
import os
import urllib.request
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# The client jar must match the server version (the oscarfonts/h2 container ships 2.1.214).
H2_JAR_URL = "https://repo1.maven.org/maven2/com/h2database/h2/2.1.214/h2-2.1.214.jar"
H2_JAR_PATH = os.path.join(os.path.dirname(__file__), "h2-2.1.214.jar")

TABLES = {
    "customers": {
        "primary_key": ["id"],
        "cursor_col": "updated_at",
        "columns": ["id", "name", "email", "created_at", "updated_at", "status"],
    },
    "orders": {
        "primary_key": ["id"],
        "cursor_col": "updated_at",
        "columns": ["id", "customer_id", "amount", "status", "created_at", "updated_at"],
    },
}

BATCH_SIZE = 10_000


def _ensure_jar(configuration):
    """Use the configured jar, else the copy seed_all.sh extracts from the container, else download."""
    for jar in (configuration.get("h2_jar"), os.environ.get("H2_JAR"), "/opt/sdk-test-dbs/jars/h2.jar"):
        if jar and os.path.exists(jar):
            return jar
    if not os.path.exists(H2_JAR_PATH):
        log.info("Downloading H2 JAR from Maven...")
        urllib.request.urlretrieve(H2_JAR_URL, H2_JAR_PATH)
        log.info("H2 JAR downloaded.")
    return H2_JAR_PATH


def _connect(configuration):
    host = configuration["host"]
    port = configuration.get("port", "9092")
    database = configuration.get("database", "test")
    username = configuration.get("username", "sa")
    password = configuration.get("password", "")
    jar = _ensure_jar(configuration)
    jdbc_url = f"jdbc:h2:tcp://{host}:{port}/{database}"
    return jaydebeapi.connect(
        "org.h2.Driver",
        jdbc_url,
        [username, password],
        jar,
    )


def schema(configuration):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
        },
        {
            "table": "orders",
            "primary_key": ["id"],
        },
    ]


def update(configuration, state):
    conn = _connect(configuration)
    try:
        cursor = conn.cursor()
        for table, meta in TABLES.items():
            cursor_col = meta["cursor_col"]
            last_cursor = state.get(f"{table}_cursor")

            if last_cursor:
                sql = (
                    f"SELECT {', '.join(meta['columns'])} FROM {table.upper()} "
                    f"WHERE {cursor_col} > CAST(? AS TIMESTAMP) "
                    f"ORDER BY {cursor_col}"
                )
                log.info(f"{table}: incremental from {last_cursor}")
            else:
                sql = (
                    f"SELECT {', '.join(meta['columns'])} FROM {table.upper()} "
                    f"ORDER BY {cursor_col}"
                )
                log.info(f"{table}: full initial sync")

            cursor.execute(sql, (last_cursor,)) if last_cursor else cursor.execute(sql)
            cols = meta["columns"]
            new_cursor = last_cursor
            row_count = 0

            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(cols, row))
                    # Normalize timestamps to ISO string for state tracking
                    for col in ["created_at", "updated_at"]:
                        if col in record and record[col] is not None:
                            record[col] = str(record[col])
                    yield op.upsert(table, record)
                    row_ts = record.get(cursor_col)
                    if row_ts and (new_cursor is None or row_ts > new_cursor):
                        new_cursor = row_ts
                    row_count += 1

                if new_cursor:
                    state[f"{table}_cursor"] = new_cursor
                    yield op.checkpoint(state)

            log.info(f"{table}: synced {row_count} rows, cursor={new_cursor}")

    finally:
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
