import vertica_python
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


TABLES = {
    "customers": {
        "primary_key": ["id"],
        "cursor_col": "updated_at",
        "columns": "id, name, email, created_at, updated_at, status",
    },
    "orders": {
        "primary_key": ["id"],
        "cursor_col": "updated_at",
        "columns": "id, customer_id, amount, status, created_at, updated_at",
    },
}


def _connect(config: dict):
    return vertica_python.connect(
        host=config["host"],
        port=int(config.get("port", 5433)),
        database=config.get("database", "VMart"),
        user=config["user"],
        password=config["password"],
        connection_timeout=30,
        read_timeout=60,
    )


def schema(configuration: dict):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id":         "INT",
                "name":       "STRING",
                "email":      "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "status":     "STRING",
            },
        },
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id":          "INT",
                "customer_id": "INT",
                "amount":      "DECIMAL",
                "status":      "STRING",
                "created_at":  "UTC_DATETIME",
                "updated_at":  "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    batch_size = int(configuration.get("batch_size", 10_000))

    with _connect(configuration) as conn:
        cur = conn.cursor("dict")

        for table, meta in TABLES.items():
            cursor_col = meta["cursor_col"]
            columns    = meta["columns"]
            last_cursor = state.get(table, {}).get(cursor_col)

            if last_cursor:
                log.info(f"{table}: incremental from {last_cursor}")
                sql = (
                    f"SELECT {columns} FROM {table} "
                    f"WHERE {cursor_col} > '{last_cursor}' "
                    f"ORDER BY {cursor_col} ASC"
                )
            else:
                log.info(f"{table}: initial full sync")
                sql = f"SELECT {columns} FROM {table} ORDER BY {cursor_col} ASC"

            cur.execute(sql)
            batch_max_cursor = last_cursor
            rows_fetched = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                for row in rows:
                    # Convert datetime objects to ISO strings for state
                    row_cursor = row[cursor_col]
                    if hasattr(row_cursor, "isoformat"):
                        row_cursor = row_cursor.isoformat()

                    if batch_max_cursor is None or row_cursor > batch_max_cursor:
                        batch_max_cursor = row_cursor

                    # Serialize datetimes in the row itself
                    serialized = {
                        k: v.isoformat() if hasattr(v, "isoformat") else v
                        for k, v in row.items()
                    }
                    yield op.upsert(table, serialized)

                rows_fetched += len(rows)
                log.info(f"{table}: {rows_fetched:,} rows so far")

                # Checkpoint state after each batch
                if batch_max_cursor is not None:
                    new_state = {**state, table: {cursor_col: batch_max_cursor}}
                    yield op.checkpoint(new_state)
                    state = new_state

            log.info(f"{table}: done — {rows_fetched:,} total rows")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
