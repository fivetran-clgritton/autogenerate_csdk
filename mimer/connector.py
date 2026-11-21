from datetime import datetime, timezone

import mimerpy
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

TABLES = ["customers", "orders"]
BATCH_SIZE = 10_000
CURSOR_COL = "updated_at"
CURSOR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
ZERO_CURSOR = "1970-01-01 00:00:00.000000"

COLUMNS = {
    "customers": ["id", "name", "email", "created_at", "updated_at", "status"],
    "orders": ["id", "customer_id", "amount", "status", "created_at", "updated_at"],
}


def _connect(config):
    # The Mimer client resolves the database name through an SQLHOSTS file (set MIMER_SQLHOSTS or
    # use /etc/sqlhosts) and the native libmimerapi.so; host:port is not passed to connect().
    return mimerpy.connect(
        dsn=config.get("database", "mimerdb"),
        user=config.get("username", "SYSADM"),
        password=config["password"],
    )


def schema(configuration):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "email": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "status": "STRING",
            },
        },
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "customer_id": "INT",
                # The SDK requires DECIMAL columns to carry precision and scale
                "amount": {"type": "DECIMAL", "precision": 10, "scale": 2},
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
    ]


def _cursor_literal(cursor):
    """Parse the stored cursor and re-format it, so only a valid timestamp ever reaches the SQL text."""
    text = cursor.replace("T", " ")
    if "." not in text:
        text += ".000000"
    return datetime.strptime(text, CURSOR_FORMAT).strftime(CURSOR_FORMAT)


def update(configuration, state):
    schema_name = configuration.get("username", "SYSADM").upper()

    conn = _connect(configuration)
    try:
        conn.autocommit = True
        for table in TABLES:
            last_cursor = state.get(f"{table}_cursor")
            cols = COLUMNS[table]
            qualified = f"{schema_name}.{table}"

            # >= (not >) so rows sharing the last cursor timestamp are never skipped;
            # upserts by primary key make re-reading them harmless.
            if last_cursor:
                ts = _cursor_literal(last_cursor)
                query = (
                    f"SELECT {', '.join(cols)} FROM {qualified} "
                    f"WHERE {CURSOR_COL} >= TIMESTAMP '{ts}' ORDER BY {CURSOR_COL}"
                )
                log.info(f"{table}: incremental from {last_cursor}")
            else:
                query = f"SELECT {', '.join(cols)} FROM {qualified} ORDER BY {CURSOR_COL}"
                log.info(f"{table}: initial full sync")

            cur = conn.cursor()
            cur.execute(query)

            max_dt = None
            rows_synced = 0
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(cols, row))
                    updated = record.get(CURSOR_COL)
                    if isinstance(updated, datetime) and (max_dt is None or updated > max_dt):
                        max_dt = updated
                    # Normalise to what the SDK accepts: ISO 8601 with a zone, str for DECIMAL
                    for col in ("created_at", "updated_at"):
                        if isinstance(record.get(col), datetime):
                            record[col] = record[col].replace(tzinfo=timezone.utc).isoformat()
                    if record.get("amount") is not None:
                        record["amount"] = str(record["amount"])
                    yield op.upsert(table, record)
                rows_synced += len(rows)
            cur.close()

            if max_dt is not None:
                state[f"{table}_cursor"] = max_dt.strftime(CURSOR_FORMAT)
                yield op.checkpoint(state)
            log.info(f"{table}: synced {rows_synced} rows, cursor={state.get(f'{table}_cursor', last_cursor)}")
    finally:
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
