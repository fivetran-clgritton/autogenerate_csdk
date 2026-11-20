import mimerpy
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

TABLES = ["customers", "orders"]
BATCH_SIZE = 10_000


def _connect(config):
    return mimerpy.connect(
        dsn=config["host"],
        user=config.get("username", "SYSADM"),
        password=config["password"],
        port=int(config.get("port", 1360)),
    )


def _cursor_col(table):
    return "updated_at"


def schema(configuration):
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


def update(configuration, state):
    schema_name = configuration.get("username", "SYSADM").upper()

    with _connect(configuration) as conn:
        conn.autocommit = True
        for table in TABLES:
            cursor_col = _cursor_col(table)
            last_cursor = state.get(f"{table}_cursor")
            qualified = f"{schema_name}.{table}"

            if last_cursor:
                # Mimer TIMESTAMP literal format: TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ffffff'
                ts = last_cursor.replace("T", " ")
                if "." not in ts:
                    ts += ".000000"
                query = (
                    f"SELECT * FROM {qualified} "
                    f"WHERE {cursor_col} > TIMESTAMP '{ts}' "
                    f"ORDER BY {cursor_col}"
                )
                log.info(f"{table}: incremental from {last_cursor}")
            else:
                query = f"SELECT * FROM {qualified} ORDER BY {cursor_col}"
                log.info(f"{table}: initial full sync")

            cur = conn.cursor()
            cur.execute(query)
            cols = [d[0].lower() for d in cur.description]

            new_cursor = last_cursor
            rows_synced = 0

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(cols, row))
                    # Convert datetime objects to ISO strings for Fivetran
                    for k, v in record.items():
                        if hasattr(v, "isoformat"):
                            record[k] = v.isoformat()
                    yield op.upsert(table, record)
                    if record.get(cursor_col):
                        new_cursor = record[cursor_col]
                rows_synced += len(rows)

            cur.close()

            if new_cursor and new_cursor != last_cursor:
                state[f"{table}_cursor"] = new_cursor
                yield op.checkpoint(state)
                log.info(f"{table}: synced {rows_synced} rows, cursor → {new_cursor}")
            else:
                log.info(f"{table}: synced {rows_synced} rows, no cursor advance")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
