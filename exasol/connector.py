import pyexasol
import fivetran_connector_sdk as sdk
from datetime import datetime, timezone

TABLES = ["customers", "orders"]
CURSOR_COL = "updated_at"
BATCH_SIZE = 10_000


def _connect(configuration: dict) -> pyexasol.ExaConnection:
    host = configuration["host"]
    port = configuration.get("port", "9563")
    return pyexasol.connect(
        dsn=f"{host}:{port}",
        user=configuration["username"],
        password=configuration["password"],
        fetch_size_bytes=50 * 1024 * 1024,  # 50MB fetch buffer
    )


def schema(configuration: dict):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id":         sdk.DataType.LONG,
                "name":       sdk.DataType.STRING,
                "email":      sdk.DataType.STRING,
                "created_at": sdk.DataType.UTC_DATETIME,
                "updated_at": sdk.DataType.UTC_DATETIME,
                "status":     sdk.DataType.STRING,
            },
        },
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id":          sdk.DataType.LONG,
                "customer_id": sdk.DataType.LONG,
                "amount":      sdk.DataType.DECIMAL,
                "status":      sdk.DataType.STRING,
                "created_at":  sdk.DataType.UTC_DATETIME,
                "updated_at":  sdk.DataType.UTC_DATETIME,
            },
        },
    ]


def update(configuration: dict, state: dict):
    db_schema = configuration.get("schema", "")
    conn = _connect(configuration)

    for table in TABLES:
        cursor = state.get(table, {}).get(CURSOR_COL)
        qualified = f'"{db_schema}"."{table.upper()}"' if db_schema else f'"{table.upper()}"'

        if cursor:
            sql = (
                f"SELECT * FROM {qualified} "
                f"WHERE {CURSOR_COL} > CAST('{cursor}' AS TIMESTAMP) "
                f"ORDER BY {CURSOR_COL} ASC"
            )
        else:
            sql = f"SELECT * FROM {qualified} ORDER BY {CURSOR_COL} ASC"

        stmt = conn.execute(sql)
        columns = [col[0].lower() for col in stmt.description]
        max_cursor = cursor

        while True:
            rows = stmt.fetchmany(BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                record = dict(zip(columns, row))
                # Normalise datetime to ISO string for Fivetran
                for col in ("created_at", "updated_at"):
                    if record.get(col) and isinstance(record[col], datetime):
                        record[col] = record[col].replace(tzinfo=timezone.utc).isoformat()
                yield sdk.upsert(table, record)
                if record.get(CURSOR_COL) and record[CURSOR_COL] > (max_cursor or ""):
                    max_cursor = record[CURSOR_COL]

        if max_cursor and max_cursor != cursor:
            state.setdefault(table, {})[CURSOR_COL] = max_cursor
            yield sdk.checkpoint(state)

    conn.close()


connector = sdk.Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
