import CUBRIDdb
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
        "amount":      {"type": "DECIMAL"},
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
    host = cfg["host"]
    port = int(cfg.get("port", 33000))
    database = cfg.get("database", "testdb")
    # CUBRID connection string: CUBRID:host:port:database:::?
    conn_str = f"CUBRID:{host}:{port}:{database}:::?"
    return CUBRIDdb.connect(conn_str, cfg["username"], cfg["password"])


def schema(configuration):
    return [
        {
            "table": table,
            "primary_key": [col for col, attrs in SCHEMAS[table].items() if attrs.get("primary_key")],
            "columns": {col: attrs["type"] for col, attrs in SCHEMAS[table].items()},
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
                # Incremental — only rows updated since last cursor
                # CUBRID uses DATETIME literals; pass as string param
                query = (
                    f"SELECT {cols} FROM {table} "
                    f"WHERE updated_at > ? "
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
                    # Serialize DATETIME objects to ISO string for Fivetran
                    for dt_col in ("created_at", "updated_at"):
                        if record.get(dt_col) is not None:
                            record[dt_col] = record[dt_col].isoformat()
                    yield op.upsert(table, record)
                    if record["updated_at"] and record["updated_at"] > (new_cursor or ""):
                        new_cursor = record["updated_at"]

            cur.close()

            if new_cursor and new_cursor != cursor_val:
                state[f"{table}_cursor"] = new_cursor
                yield op.checkpoint(state)
    finally:
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
