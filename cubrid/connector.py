import os

import jaydebeapi
import fivetran_connector_sdk as fdk
from fivetran_connector_sdk import Connector, Operations as op

TABLES = ["customers", "orders"]

SCHEMAS = {
    "customers": {
        "id":         {"type": "INT", "primary_key": True},
        "name":       {"type": "STRING"},
        "email":      {"type": "STRING"},
        "created_at": {"type": "UTC_DATETIME"},
        "updated_at": {"type": "UTC_DATETIME"},
        "status":     {"type": "STRING"},
    },
    "orders": {
        "id":          {"type": "INT", "primary_key": True},
        "customer_id": {"type": "INT"},
        "amount":      {"type": "DECIMAL", "precision": 10, "scale": 2},
        "status":      {"type": "STRING"},
        "created_at":  {"type": "UTC_DATETIME"},
        "updated_at":  {"type": "UTC_DATETIME"},
    },
}

COLUMNS = {
    "customers": ["id", "name", "email", "created_at", "updated_at", "status"],
    "orders":    ["id", "customer_id", "amount", "status", "created_at", "updated_at"],
}

BATCH_SIZE = 10_000


def _connect(cfg):
    # CUBRID-Python (C extension) fails on Python 3.14 ("Invalid connection handle"),
    # so connect over JDBC. The jar ships inside the CUBRID container:
    #   docker cp cubrid:/home/cubrid/CUBRID/jdbc/JDBC-<ver>-cubrid.jar cubrid_jdbc.jar
    host = cfg["host"]
    port = int(cfg.get("port", 33000))
    database = cfg.get("database", "testdb")
    jar = cfg.get("jdbc_jar") or os.environ.get("CUBRID_JDBC_JAR", "/opt/sdk-test-dbs/jars/cubrid_jdbc.jar")
    return jaydebeapi.connect(
        "cubrid.jdbc.driver.CUBRIDDriver",
        f"jdbc:cubrid:{host}:{port}:{database}:::",
        [cfg["username"], cfg["password"]],
        jar,
    )


def _to_utc_iso(value):
    """JDBC returns 'YYYY-MM-DD HH:MM:SS[.fff]'; the SDK needs ISO 8601 with a zone for UTC_DATETIME."""
    return str(value).replace(" ", "T") + "+00:00"


def schema(configuration):
    return [
        {
            "table": table,
            "primary_key": [col for col, attrs in SCHEMAS[table].items() if attrs.get("primary_key")],
            # The SDK requires DECIMAL columns to carry precision and scale as a dict
            "columns": {
                col: ({k: v for k, v in attrs.items() if k != "primary_key"} if "precision" in attrs else attrs["type"])
                for col, attrs in SCHEMAS[table].items()
            },
        }
        for table in TABLES
    ]


def update(configuration, state):
    conn = _connect(configuration)
    try:
        for table in TABLES:
            cursor_val = state.get(f"{table}_cursor")
            cols = ", ".join(COLUMNS[table])
            cur = conn.cursor()

            if cursor_val is None:
                # Initial sync — full table scan
                query = f"SELECT {cols} FROM {table} ORDER BY updated_at"
                cur.execute(query)
            else:
                # Incremental — rows updated at or after the last cursor. >= (not >) so rows sharing the
                # cursor timestamp are never skipped; upserts by primary key make the re-read harmless.
                # CUBRID uses DATETIME literals; pass as string param
                query = (
                    f"SELECT {cols} FROM {table} "
                    f"WHERE updated_at >= ? "
                    f"ORDER BY updated_at"
                )
                cur.execute(query, (cursor_val,))

            new_cursor = cursor_val
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                col_names = COLUMNS[table]
                for row in rows:
                    record = dict(zip(col_names, row))
                    # JDBC hands back JPype numeric wrappers; the SDK wants plain int / str
                    for col, attrs in SCHEMAS[table].items():
                        if record.get(col) is not None:
                            if attrs["type"] == "INT":
                                record[col] = int(record[col])
                            elif attrs["type"] == "DECIMAL":
                                record[col] = str(record[col])
                    # Keep the cursor in the database's own format: it is passed back as the query parameter.
                    raw_updated = str(record["updated_at"]) if record.get("updated_at") is not None else None
                    for dt_col in ("created_at", "updated_at"):
                        if record.get(dt_col) is not None:
                            record[dt_col] = _to_utc_iso(record[dt_col])
                    yield op.upsert(table, record)
                    if raw_updated and raw_updated > (new_cursor or ""):
                        new_cursor = raw_updated

            cur.close()

            if new_cursor and new_cursor != cursor_val:
                state[f"{table}_cursor"] = new_cursor
                yield op.checkpoint(state)
    finally:
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
