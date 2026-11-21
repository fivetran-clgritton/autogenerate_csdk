import ssl
from datetime import datetime, timezone

import pyexasol
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

TABLES = ["customers", "orders"]
CURSOR_COL = "updated_at"
BATCH_SIZE = 10_000
# Exasol timestamps are stored in the database's own format; the cursor is kept in that format too
# and passed back as a bound value (never interpolated into the SQL by hand).
CURSOR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def _connect(configuration: dict) -> pyexasol.ExaConnection:
    host = configuration["host"]
    port = configuration.get("port", "8563")
    kwargs = {}
    # The test instance uses a self-signed certificate, so validation can be turned off per config.
    if str(configuration.get("validate_certificate", "true")).lower() != "true":
        kwargs["websocket_sslopt"] = {"cert_reqs": ssl.CERT_NONE}
    return pyexasol.connect(
        dsn=f"{host}:{port}",
        user=configuration["username"],
        password=configuration["password"],
        fetch_size_bytes=50 * 1024 * 1024,  # 50MB fetch buffer
        # Without a mapper pyexasol returns timestamps as strings; this gives datetime / Decimal objects
        fetch_mapper=pyexasol.exasol_mapper,
        **kwargs,
    )


def schema(configuration: dict):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
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
                "id": "LONG",
                "customer_id": "LONG",
                # The SDK requires DECIMAL columns to carry precision and scale
                "amount": {"type": "DECIMAL", "precision": 10, "scale": 2},
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    db_schema = configuration.get("schema", "testdb")
    conn = _connect(configuration)
    try:
        for table in TABLES:
            cursor = state.get(table, {}).get(CURSOR_COL)
            # Unquoted identifiers are folded to upper case by Exasol, so "testdb" is stored as TESTDB.
            qualified = f'"{db_schema.upper()}"."{table.upper()}"' if db_schema else f'"{table.upper()}"'

            # >= (not >) so rows sharing the last cursor timestamp are never skipped;
            # upserts by primary key make re-reading them harmless.
            if cursor:
                stmt = conn.execute(
                    f"SELECT * FROM {qualified} WHERE {CURSOR_COL} >= CAST({{cursor}} AS TIMESTAMP) "
                    f"ORDER BY {CURSOR_COL} ASC",
                    {"cursor": cursor},
                )
                log.info(f"{table}: incremental from {cursor}")
            else:
                stmt = conn.execute(f"SELECT * FROM {qualified} ORDER BY {CURSOR_COL} ASC")
                log.info(f"{table}: full initial sync")

            columns = [name.lower() for name in stmt.columns()]
            max_dt = None
            total = 0

            while True:
                rows = stmt.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(columns, row))
                    updated = record.get(CURSOR_COL)
                    if isinstance(updated, datetime) and (max_dt is None or updated > max_dt):
                        max_dt = updated
                    # Normalise to what the SDK accepts: ISO 8601 with a zone, str for DECIMAL
                    for col in ("created_at", "updated_at"):
                        if isinstance(record.get(col), datetime):
                            record[col] = record[col].replace(tzinfo=timezone.utc).isoformat()
                    if record.get("amount") is not None:
                        record["amount"] = str(record["amount"])
                    op.upsert(table=table, data=record)
                    total += 1

            if max_dt is not None:
                state.setdefault(table, {})[CURSOR_COL] = max_dt.strftime(CURSOR_FORMAT)
                op.checkpoint(state=state)
            log.info(f"{table}: synced {total} rows, cursor={state.get(table, {}).get(CURSOR_COL)}")
    finally:
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
