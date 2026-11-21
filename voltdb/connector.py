import json
import requests
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


TABLES = ["customers", "orders"]
PAGE_SIZE = 10_000


def _adhoc(base_url, sql, auth=None):
    """Execute a SQL string via VoltDB's @AdHoc REST endpoint."""
    params = {"Procedure": "@AdHoc", "Parameters": json.dumps([sql])}
    r = requests.post(base_url, data=params, auth=auth, timeout=30)
    r.raise_for_status()
    body = r.json()
    if body.get("status") != 1:
        raise RuntimeError(f"VoltDB error: {body.get('statusstring')}")
    return body["results"][0]


def _rows_to_dicts(result):
    """Convert VoltDB result (schema + data arrays) to list of dicts."""
    cols = [c["name"].lower() for c in result["schema"]]
    rows = []
    for row in result["data"]:
        d = dict(zip(cols, row))
        # VoltDB timestamps come back as microseconds since epoch; convert to ISO string
        for k, v in d.items():
            if isinstance(v, (int, float)) and ("_at" in k or "time" in k):
                try:
                    d[k] = datetime.fromtimestamp(v / 1_000_000, tz=timezone.utc).isoformat()
                except (OSError, OverflowError, ValueError):
                    pass
        rows.append(d)
    return rows


def schema(configuration: dict):
    return [
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id":         {"type": "INT"},
                "name":       {"type": "STRING"},
                "email":      {"type": "STRING"},
                "created_at": {"type": "UTC_DATETIME"},
                "updated_at": {"type": "UTC_DATETIME"},
                "status":     {"type": "STRING"},
            },
        },
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id":          {"type": "INT"},
                "customer_id": {"type": "INT"},
                "amount":      {"type": "DECIMAL"},
                "status":      {"type": "STRING"},
                "created_at":  {"type": "UTC_DATETIME"},
                "updated_at":  {"type": "UTC_DATETIME"},
            },
        },
    ]


def update(configuration: dict, state: dict):
    host = configuration["host"]
    port = configuration.get("port", 8080)
    base_url = f"http://{host}:{port}/api/1.0/"

    username = configuration.get("username", "").strip()
    password = configuration.get("password", "").strip()
    auth = (username, password) if username else None

    for table in TABLES:
        cursor = state.get(f"{table}_cursor")
        last_id = state.get(f"{table}_last_id", 0)

        if cursor:
            # Incremental: rows updated after last cursor, paginated by id
            log.info(f"{table}: incremental from updated_at > {cursor}, id > {last_id}")
            where = f"updated_at > '{cursor}' AND id > {last_id}"
        else:
            # Initial sync: full table, paginated by id
            log.info(f"{table}: initial full sync from id > {last_id}")
            where = f"id > {last_id}"

        while True:
            sql = (
                f"SELECT * FROM {table} "
                f"WHERE {where} "
                f"ORDER BY updated_at ASC, id ASC "
                f"LIMIT {PAGE_SIZE} ALLOW PARTITIONS"
            )
            result = _adhoc(base_url, sql, auth)
            rows = _rows_to_dicts(result)

            if not rows:
                break

            for row in rows:
                op.upsert(table=table, data=row)

            last_row = rows[-1]
            last_id = last_row["id"]
            new_cursor = last_row.get("updated_at", cursor)

            state[f"{table}_last_id"] = last_id
            state[f"{table}_cursor"] = new_cursor
            op.checkpoint(state=state)

            if len(rows) < PAGE_SIZE:
                break
            # Next page: same cursor window, pick up from last_id
            where = (
                f"(updated_at = '{new_cursor}' AND id > {last_id}) "
                f"OR updated_at > '{new_cursor}'"
            ) if cursor else f"id > {last_id}"

        # Reset last_id between tables so it doesn't bleed across
        state[f"{table}_last_id"] = 0
        op.checkpoint(state=state)

    log.info("Sync complete.")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
