import fivetran_connector_sdk as fdk
from fivetran_connector_sdk import Connector, Operations as op
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

TABLES = ["customers", "orders"]
SCHEMA_PREFIX = "SQLUser"
BATCH_SIZE = 10_000
ZERO_CURSOR = "1970-01-01 00:00:00"


def _engine(cfg):
    host = cfg["host"]
    port = cfg.get("port", "1972")
    user = cfg.get("username", "_SYSTEM")
    pw = cfg["password"]
    ns = cfg.get("namespace", "USER")
    return create_engine(f"iris://{user}:{pw}@{host}:{port}/{ns}")


def schema(configuration):
    return [
        fdk.SchemaBuilder()
        .table("customers", primary_key=["id"])
        .column("id",         fdk.DataType.INT)
        .column("name",       fdk.DataType.STRING)
        .column("email",      fdk.DataType.STRING)
        .column("created_at", fdk.DataType.UTC_DATETIME)
        .column("updated_at", fdk.DataType.UTC_DATETIME)
        .column("status",     fdk.DataType.STRING)
        .build(),
        fdk.SchemaBuilder()
        .table("orders", primary_key=["id"])
        .column("id",          fdk.DataType.INT)
        .column("customer_id", fdk.DataType.INT)
        .column("amount",      fdk.DataType.DECIMAL)
        .column("status",      fdk.DataType.STRING)
        .column("created_at",  fdk.DataType.UTC_DATETIME)
        .column("updated_at",  fdk.DataType.UTC_DATETIME)
        .build(),
    ]


def update(configuration, state):
    engine = _engine(configuration)

    for table in TABLES:
        cursor = state.get(f"{table}_cursor", ZERO_CURSOR)
        qualified = f"{SCHEMA_PREFIX}.{table}"
        new_cursor = cursor
        total = 0

        with engine.connect() as conn:
            # IRIS uses TOP n, not LIMIT n; for unbounded pagination use cursor
            # ordering by updated_at ensures we can checkpoint safely per batch
            sql = text(
                f"SELECT * FROM {qualified} "
                f"WHERE updated_at > CAST(:cursor AS TIMESTAMP) "
                f"ORDER BY updated_at, id"
            )
            result = conn.execute(sql, {"cursor": cursor})
            cols = list(result.keys())

            batch = []
            for row in result:
                record = dict(zip(cols, row))
                # Normalise datetime objects to ISO strings for Fivetran
                for k, v in record.items():
                    if isinstance(v, datetime):
                        if v.tzinfo is None:
                            v = v.replace(tzinfo=timezone.utc)
                        record[k] = v.isoformat()
                batch.append(record)

                if record.get("updated_at") and record["updated_at"] > new_cursor:
                    new_cursor = record["updated_at"]

                if len(batch) >= BATCH_SIZE:
                    yield op.upsert(table, batch)
                    total += len(batch)
                    batch = []
                    # Checkpoint after each batch so a mid-sync restart
                    # resumes from the last committed cursor, not the start.
                    yield op.checkpoint({**state, f"{table}_cursor": new_cursor})

            if batch:
                yield op.upsert(table, batch)
                total += len(batch)

        state = {**state, f"{table}_cursor": new_cursor}
        yield op.checkpoint(state)
        fdk.log(f"iris: synced {total} rows from {table}, cursor={new_cursor}")

    engine.dispose()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
