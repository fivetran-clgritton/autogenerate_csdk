import time
from datetime import datetime, timezone

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

TABLES = ["customers", "orders"]
SCHEMA_PREFIX = "SQLUser"
# Rows per query. Each batch is its own bounded query: a single streamed query let the IRIS driver
# grow memory linearly (about 2.4 GB after 6M rows) and would hit the 4 GB limit on large tables.
BATCH_SIZE = 50_000
# The SDK buffers upserted rows in memory until the next checkpoint (a checkpoint flushes them), so
# checkpoint cadence bounds memory. Measured: 2M rows with one checkpoint peaked at 3.5 GB; every
# 10k rows peaked at 290 MB. The SDK reference allows at most one checkpoint a minute, so use that.
CHECKPOINT_INTERVAL_SECONDS = 60
PROGRESS_LOG_EVERY = 250_000
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


def _batch_query(table, cols, after_id):
    # IRIS quirk: selecting updated_at while also ORDER BY-ing on it returns the internal
    # sort-encoded value (e.g. '1154657064703846976') instead of a timestamp. Casting fixes it.
    select_list = ", ".join(f"CAST({c} AS TIMESTAMP)" if c == "updated_at" else c for c in cols)
    if after_id is None:
        # Start of a table: >= (not >) so rows sharing the cursor timestamp are never skipped,
        # including rows written at that timestamp after the previous sync. Re-reads are harmless
        # because upserts are keyed on the primary key.
        where = "updated_at >= CAST(:ts AS TIMESTAMP)"
    else:
        # Within a table: continue exactly after the last (updated_at, id) delivered.
        where = ("updated_at > CAST(:ts AS TIMESTAMP) "
                 "OR (updated_at = CAST(:ts AS TIMESTAMP) AND id > :after_id)")
    return text(
        f"SELECT TOP {BATCH_SIZE} {select_list} FROM {SCHEMA_PREFIX}.{table} "
        f"WHERE {where} ORDER BY updated_at, id"
    )


def update(configuration, state):
    engine = _engine(configuration)
    try:
        for table in TABLES:
            cursor = state.get(f"{table}_cursor", ZERO_CURSOR)
            # Present only when a previous sync stopped part-way through this table
            after_id = state.get(f"{table}_last_id")
            cols = COLUMNS[table]
            total = 0
            last_checkpoint = time.monotonic()
            log.info(f"{table}: syncing from updated_at={cursor}" + (f", id>{after_id}" if after_id is not None else ""))

            with engine.connect() as conn:
                while True:
                    params = {"ts": cursor} if after_id is None else {"ts": cursor, "after_id": after_id}
                    rows = conn.execute(_batch_query(table, cols, after_id), params).fetchall()
                    for row in rows:
                        record, raw_updated = _to_record(cols, row)
                        op.upsert(table=table, data=record)
                        # Rows arrive ordered by (updated_at, id), so the last one processed is the keyset position
                        cursor, after_id = raw_updated, record["id"]
                        total += 1
                        if total % PROGRESS_LOG_EVERY == 0:
                            log.info(f"{table}: {total:,} rows synced, at updated_at={cursor}")

                    if len(rows) < BATCH_SIZE:
                        break
                    if time.monotonic() - last_checkpoint >= CHECKPOINT_INTERVAL_SECONDS:
                        state[f"{table}_cursor"] = cursor
                        state[f"{table}_last_id"] = after_id
                        op.checkpoint(state=state)
                        last_checkpoint = time.monotonic()

            # Table finished: keep the timestamp, drop the id so the next sync starts at >= cursor
            state[f"{table}_cursor"] = cursor
            state.pop(f"{table}_last_id", None)
            op.checkpoint(state=state)
            log.info(f"{table}: synced {total:,} rows, cursor={cursor}")
    finally:
        engine.dispose()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
