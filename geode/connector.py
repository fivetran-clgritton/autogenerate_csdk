import json
import re

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

CURSOR_FIELD = "updated_at"
DEFAULT_PAGE_SIZE = 50_000
# The cursor is interpolated into OQL text (the dev REST API's bind-values parameter does not work),
# so only well-formed ISO timestamps are ever allowed through.
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")


def _base(cfg):
    return f"http://{cfg['host']}:{cfg.get('rest_port', '8081')}/geode/v1"


def _regions(cfg):
    return [r.strip() for r in cfg.get("regions", "customers,orders").split(",")]


def _page_size(cfg):
    return int(cfg.get("page_size", DEFAULT_PAGE_SIZE))


def _oql(base, query):
    """Run an OQL query through the dev REST API and return a list of row dicts."""
    r = requests.get(f"{base}/queries/adhoc", params={"q": query}, timeout=300)
    r.raise_for_status()
    data = r.json()
    results = data.get("result", data) if isinstance(data, dict) else data
    rows = []
    for item in results:
        if isinstance(item, list) and len(item) == 2:
            rows.append(item[1])  # [TypeInfo, {fields}] pair
        elif isinstance(item, dict):
            rows.append(item)
    return rows


def _is_datetime_col(name):
    return name.endswith("_at")


def _flatten(obj):
    """Normalize a Geode object (possibly nested) to a flat string-keyed dict."""
    if not isinstance(obj, dict):
        return {"_value": str(obj)}
    out = {}
    for k, v in obj.items():
        out[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
    return out


def _to_record(raw):
    row = _flatten(raw)
    for col, val in row.items():
        # Geode stores these as naive ISO strings; the SDK needs a zone for UTC_DATETIME
        if _is_datetime_col(col) and isinstance(val, str) and not re.search(r"(Z|[+-]\d{2}:?\d{2})$", val):
            row[col] = val + "+00:00"
    return row


def schema(configuration):
    base = _base(configuration)
    tables = []
    for region in _regions(configuration):
        # Probe one row to discover columns. Errors propagate: a silently empty schema hides real problems.
        rows = _oql(base, f"SELECT * FROM /{region} LIMIT 1")
        columns = {}
        if rows:
            for col, val in _flatten(rows[0]).items():
                if _is_datetime_col(col):
                    dtype = "UTC_DATETIME"
                elif isinstance(val, bool):
                    dtype = "BOOLEAN"
                elif isinstance(val, int):
                    dtype = "LONG"
                elif isinstance(val, float):
                    dtype = "DOUBLE"
                else:
                    dtype = "STRING"
                columns[col] = dtype
        columns.setdefault("id", "LONG")
        tables.append({"table": region, "primary_key": ["id"], "columns": columns})
    return tables


def _page(base, region, cursor, page, strict):
    """Fetch one page ordered by updated_at. >= (not >) so rows sharing the cursor timestamp are not skipped."""
    where = ""
    if cursor:
        if not _TS_RE.match(cursor):
            raise ValueError(f"Refusing to use malformed cursor in a query: {cursor!r}")
        where = f"WHERE r.{CURSOR_FIELD} {'>' if strict else '>='} '{cursor}' "
    return _oql(base, f"SELECT * FROM /{region} r {where}ORDER BY r.{CURSOR_FIELD} LIMIT {page}")


def update(configuration, state):
    base = _base(configuration)
    page = _page_size(configuration)

    for region in _regions(configuration):
        last_updated = state.get(region, {}).get("last_updated_at")
        cursor = last_updated
        strict = False
        total = 0
        log.info(f"Syncing region /{region}, cursor={last_updated}")

        # Geode OQL has no OFFSET, so page by keyset on updated_at. Re-read boundary rows are harmless
        # because upserts are keyed on the primary key.
        while True:
            rows = _page(base, region, cursor, page, strict)
            if not rows:
                break
            for raw in rows:
                op.upsert(table=region, data=_to_record(raw))
            total += len(rows)

            last_ts = rows[-1].get(CURSOR_FIELD)
            if last_ts is None:
                raise ValueError(f"/{region} rows have no {CURSOR_FIELD} field; cannot page incrementally")

            if len(rows) < page:
                cursor = last_ts
                break

            if last_ts == cursor and not strict:
                # A full page that never moved past the cursor: more than `page` rows share this timestamp.
                # Take every row at exactly this timestamp in one query, then continue strictly after it.
                same = _oql(base, f"SELECT * FROM /{region} r WHERE r.{CURSOR_FIELD} = '{last_ts}'")
                for raw in same:
                    op.upsert(table=region, data=_to_record(raw))
                total += len(same)
                strict = True
            else:
                strict = False
            cursor = last_ts

            state[region] = {"last_updated_at": cursor}
            op.checkpoint(state=state)

        if cursor and cursor != last_updated:
            state[region] = {"last_updated_at": cursor}
        log.info(f"/{region}: synced {total} rows, cursor={cursor}")
        op.checkpoint(state=state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
