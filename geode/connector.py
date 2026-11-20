import json
import requests
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


def _base(cfg):
    return f"http://{cfg['host']}:{cfg.get('rest_port', '8081')}/geode/v1"


def _regions(cfg):
    return [r.strip() for r in cfg.get("regions", "customers,orders").split(",")]


def _get(url, **kwargs):
    r = requests.get(url, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()


def _oql(base, query, bind_values=None):
    """Execute an OQL query and return the result list."""
    params = {"q": query}
    if bind_values:
        params["bind-values"] = ",".join(str(v) for v in bind_values)
    data = _get(f"{base}/queries/adhoc", params=params)
    # Geode REST returns {"result": [[TypeInfo, {fields}], ...]} or a flat list
    results = data.get("result", data) if isinstance(data, dict) else data
    rows = []
    for item in results:
        if isinstance(item, list) and len(item) == 2:
            # [TypeInfo, {fields}] pair
            rows.append(item[1])
        elif isinstance(item, dict):
            rows.append(item)
    return rows


def _flatten(obj):
    """Normalize a Geode object (possibly nested) to a flat string-keyed dict."""
    if not isinstance(obj, dict):
        return {"_value": str(obj)}
    out = {}
    for k, v in obj.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v)
        else:
            out[k] = v
    return out


def schema(configuration):
    base = _base(configuration)
    tables = []
    for region in _regions(configuration):
        # Probe one row to discover columns
        try:
            rows = _oql(base, f"SELECT * FROM /{region} LIMIT 1")
        except Exception:
            rows = []
        columns = []
        if rows:
            for col, val in _flatten(rows[0]).items():
                dtype = "STRING"
                if isinstance(val, bool):
                    dtype = "BOOLEAN"
                elif isinstance(val, int):
                    dtype = "LONG"
                elif isinstance(val, float):
                    dtype = "DOUBLE"
                columns.append({"name": col, "type": dtype})
        # Ensure id always present as primary key
        if not any(c["name"] == "id" for c in columns):
            columns.insert(0, {"name": "id", "type": "LONG"})
        tables.append({
            "table_name": region,
            "primary_key": ["id"],
            "columns": columns,
        })
    return tables


def update(configuration, state):
    base = _base(configuration)

    for region in _regions(configuration):
        region_state = state.get(region, {})
        last_updated = region_state.get("last_updated_at")

        log.info(f"Syncing region /{region}, cursor={last_updated}")

        try:
            if last_updated:
                # Incremental: filter by updated_at if the field exists
                rows = _oql(
                    base,
                    f"SELECT * FROM /{region} r WHERE r.updated_at > $1 ORDER BY r.updated_at",
                    bind_values=[last_updated],
                )
            else:
                # Initial full scan — paginate in chunks of 10 000
                rows = []
                offset = 0
                page = 10_000
                while True:
                    chunk = _oql(
                        base,
                        f"SELECT * FROM /{region} LIMIT {page} OFFSET {offset}",
                    )
                    rows.extend(chunk)
                    if len(chunk) < page:
                        break
                    offset += page
        except Exception as e:
            log.warning(f"Region /{region} query failed: {e}. Falling back to full key scan.")
            rows = _key_scan(base, region)

        new_cursor = last_updated
        for raw in rows:
            row = _flatten(raw)
            yield op.upsert(region, row)
            # Advance cursor to max updated_at seen
            if "updated_at" in row:
                ts = str(row["updated_at"])
                if new_cursor is None or ts > new_cursor:
                    new_cursor = ts

        if new_cursor and new_cursor != last_updated:
            state[region] = {"last_updated_at": new_cursor}
            yield op.checkpoint(state)

    # Final checkpoint if nothing updated the cursor
    yield op.checkpoint(state)


def _key_scan(base, region):
    """Fallback: fetch all keys then GET each object individually."""
    try:
        keys_data = _get(f"{base}/regions/{region}/keys")
        keys = keys_data.get("keys", [])
    except Exception:
        return []
    rows = []
    for key in keys:
        try:
            obj = _get(f"{base}/regions/{region}/{key}")
            rows.append(obj)
        except Exception:
            pass
    return rows


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
