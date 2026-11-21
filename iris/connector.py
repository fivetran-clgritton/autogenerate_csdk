from datetime import datetime, timezone

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

TABLES = ["customers", "orders"]
SCHEMA_PREFIX = "SQLUser"
BATCH_SIZE = 10_000
ZERO_CURSOR = "1970-01-01 00:00:00"

COLUMNS = {
    "customers": ["id", "name", "email", "created_at", "updated_at", "status"],
    "orders": ["id", "customer_id", "amount", "status", "created_at", "updated_at"],
}


def _engine(cfg):
    # URL.create keeps characters like '@' in the password intact; an f-string URL breaks on them.
    url = URL.create(
        "iris",
        username=cfg.get("username", "_SYSTEM"),
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg.get("port", 1972)),
        database=cfg.get("namespace", "USER"),
    )
    return create_engine(url)


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


def _to_record(cols, row):
    """Convert a database row to a dict the SDK accepts. Returns (record, raw updated_at)."""
    record = dict(zip(cols, row))
    raw_updated = None
    for key, value in record.items():
        if isinstance(value, datetime):
            if key == "updated_at":
                # Keep the cursor in the database's own format; it is passed back to CAST(... AS TIMESTAMP)
                raw_updated = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            record[key] = value.replace(tzinfo=value.tzinfo or timezone.utc).isoformat()
        elif key == "amount" and value is not None:
            record[key] = str(value)  # DECIMAL values must be str or int
    return record, raw_updated


def update(configuration, state):
    engine = _engine(configuration)
    try:
        for table in TABLES:
            cursor = state.get(f"{table}_cursor", ZERO_CURSOR)
            new_cursor = cursor
            total = 0
            cols = COLUMNS[table]

            # >= (not >) so rows sharing the last cursor timestamp are never skipped when a sync
            # resumes from a mid-sync checkpoint. Upserts by primary key make the re-read harmless.
            # IRIS quirk: selecting updated_at while also ORDER BY-ing on it returns the internal
            # sort-encoded value (e.g. '1154657064703846976') instead of a timestamp. Casting fixes it.
            select_list = ", ".join(f"CAST({c} AS TIMESTAMP)" if c == "updated_at" else c for c in cols)
            sql = text(
                f"SELECT {select_list} FROM {SCHEMA_PREFIX}.{table} "
                f"WHERE updated_at >= CAST(:cursor AS TIMESTAMP) "
                f"ORDER BY updated_at, id"
            )
            log.info(f"{table}: syncing rows with updated_at >= {cursor}")

            with engine.connect() as conn:
                # Stream rows instead of buffering the whole result client-side
                result = conn.execution_options(yield_per=BATCH_SIZE).execute(sql, {"cursor": cursor})
                for row in result:
                    record, raw_updated = _to_record(cols, row)
                    op.upsert(table=table, data=record)
                    total += 1
                    if raw_updated and raw_updated > new_cursor:
                        new_cursor = raw_updated
                    if total % BATCH_SIZE == 0:
                        # Safe to resume from here: the next sync re-reads from this timestamp (>=)
                        state[f"{table}_cursor"] = new_cursor
                        op.checkpoint(state=state)

            state[f"{table}_cursor"] = new_cursor
            op.checkpoint(state=state)
            log.info(f"{table}: synced {total} rows, cursor={new_cursor}")
    finally:
        engine.dispose()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
