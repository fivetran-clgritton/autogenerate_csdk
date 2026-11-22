"""Fivetran Connector SDK connector for InterSystems IRIS (and Caché through IRIS SQL).

Discovers tables, columns and primary keys from INFORMATION_SCHEMA and picks a strategy per table:

- Incremental: the table has a primary key and a usable cursor column (configurable, default
  `updated_at`). Reads `cursor >= last value` in bounded batches with keyset paging on
  (cursor, primary key). Rows whose cursor is NULL are re-read every sync, so they are never lost.
- Full reload: tables with a primary key but no usable cursor. Deferred truncate, then upsert every
  row in keyset batches on the primary key, so a failed sync deletes nothing.

Tables without a primary key are skipped (with a dashboard warning listing them). Without a key,
Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses rows that
are exact duplicates.

This connector is the template for the other SQL connectors in this repo. Database-specific parts
are marked "IRIS:".
"""

import gc
import re
import time
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Rows per query for a table of up to WIDE_TABLE_COLUMNS columns (scaled down for wider tables).
# Each batch is its own bounded query: one streamed query let the IRIS driver grow memory linearly
# (about 2.4 GB after 6M rows), which would hit the 4 GB limit on large tables.
BATCH_SIZE = 50_000
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_BATCH_SIZE = 1_000
MIN_CHECKPOINT_ROW_CAP = 10_000
# Tables with stream columns (LONGVARCHAR / LONGVARBINARY) can have very large rows
LOB_BATCH_SIZE = 1_000
LOB_CHECKPOINT_ROW_CAP = 10_000
PROGRESS_LOG_EVERY = 250_000
# Connection attempts before a sync fails; refused or dropped connections are retried, a refused login is not
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_SECONDS = 5

DEFAULT_CURSOR_COLUMNS = "updated_at"

# IRIS: '%' schemas hold system classes, Ens* are interoperability (Ensemble) classes and HS* are
# HealthShare classes. A schema named explicitly in the `schemas` setting is synced even if listed here.
SYSTEM_SCHEMAS = {"information_schema", "ens"}
SYSTEM_SCHEMA_PREFIXES = ("%", "ens_", "enslib_", "ensportal_", "hs_", "hsfhir", "hsmod_")

# IRIS: INFORMATION_SCHEMA.COLUMNS.DATA_TYPE values (compared lower-cased)
TIMESTAMP_TYPES = {"timestamp", "datetime"}
DATE_TYPES = {"date"}
TIME_TYPES = {"time"}
INTEGER_TYPES = {"tinyint", "smallint", "integer", "int", "bigint"}
DECIMAL_TYPES = {"numeric", "decimal"}
FLOAT_TYPES = {"double", "float", "real"}  # IRIS: the catalog reports all three as double
STRING_TYPES = {"varchar", "char", "longvarchar", "time"}  # TIME is delivered as an ISO string
BOOLEAN_TYPES = {"bit"}
BINARY_TYPES = {"binary", "varbinary", "longvarbinary"}
LOB_TYPES = {"longvarchar", "longvarbinary"}
CURSOR_TYPES = TIMESTAMP_TYPES | DATE_TYPES | INTEGER_TYPES
MAX_DECIMAL_PRECISION = 38  # the SDK's DECIMAL limit; wider values are delivered as strings


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _validate_configuration(configuration: dict):
    for key in ("host", "password"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if not str(configuration.get("port", "1972")).isdigit():
        raise ValueError("port must be a number")
    for entry in _csv(configuration, "cursor_overrides"):
        name, sep, _ = entry.partition("=")
        if not sep or "." not in name:
            raise ValueError(
                f"Invalid cursor_overrides entry '{entry}': use schema.table=column, "
                "or schema.table= to force a full reload"
            )


def _engine(configuration: dict):
    # URL.create keeps characters like '@' in the password intact; an f-string URL breaks on them.
    url = URL.create(
        "iris",
        username=configuration.get("username") or "_SYSTEM",
        password=configuration["password"],
        host=configuration["host"],
        port=int(configuration.get("port") or 1972),
        database=configuration.get("namespace") or "USER",
    )
    return create_engine(url)


def _connect(engine, configuration: dict):
    """Open a connection, retrying network failures, with an error that names the server."""
    target = (f"{configuration['host']}:{configuration.get('port') or 1972}"
              f"/{configuration.get('namespace') or 'USER'}")
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return engine.connect()
        except Exception as e:  # IRIS: the driver raises a plain RuntimeError for connection failures
            message = str(getattr(e, "orig", None) or e)
            if "Access Denied" in message:
                # IRIS: a wrong password, an unknown namespace and an unresolvable host all look like this
                raise RuntimeError(
                    f"IRIS at {target} refused the login for {configuration.get('username') or '_SYSTEM'} "
                    f"(Access Denied): check the username, password, namespace and host. {message}"
                ) from None
            if attempt == CONNECT_ATTEMPTS:
                raise RuntimeError(
                    f"Could not connect to IRIS at {target} after {CONNECT_ATTEMPTS} attempts: {message}"
                ) from None
            delay = CONNECT_RETRY_SECONDS * 2 ** (attempt - 1)
            log.warning(f"Connecting to IRIS at {target} failed (attempt {attempt} of {CONNECT_ATTEMPTS}), "
                        f"retrying in {delay}s: {message}")
            time.sleep(delay)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_ref(spec: dict) -> str:
    return f"{_quote(spec['schema'])}.{_quote(spec['table'])}"


def _table_key(spec: dict) -> str:
    return f"{spec['schema']}.{spec['table']}"


def _is_system_schema(schema_name: str) -> bool:
    lowered = schema_name.lower()
    return lowered in SYSTEM_SCHEMAS or lowered.startswith(SYSTEM_SCHEMA_PREFIXES)


def _matches(patterns: list, schema_name: str, table_name: str) -> bool:
    """True if a pattern ('schema.table' or bare 'table', case-insensitive) names this table."""
    qualified = f"{schema_name}.{table_name}".lower()
    bare = table_name.lower()
    return any(p.lower() in (qualified, bare) for p in patterns)


# ---------------------------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------------------------


def _discover(conn, configuration: dict, verbose: bool = True) -> list:
    """Return a spec dict for each table to sync, built from the catalog and the configuration.

    Runs from both schema() and update(); only the update() run logs (verbose), to avoid duplicates.
    """
    schemas = {s.lower() for s in _csv(configuration, "schemas")}
    include = _csv(configuration, "include_tables")
    exclude = _csv(configuration, "exclude_tables")
    cursor_candidates = [c.lower() for c in _csv(configuration, "cursor_columns", DEFAULT_CURSOR_COLUMNS)]
    overrides = {}
    for entry in _csv(configuration, "cursor_overrides"):
        name, _, column = entry.partition("=")
        overrides[name.strip().lower()] = column.strip().lower()

    # IRIS: SYSTEM TABLE / SYSTEM VIEW rows are catalog tables; views are not synced.
    # Avoid DISTINCT on catalog columns: IRIS collation upper-cases the returned values.
    catalog_tables = conn.execute(
        text("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    ).fetchall()
    selected = []
    for schema_name, table_name in catalog_tables:
        if schemas:
            if schema_name.lower() not in schemas:
                continue
        elif _is_system_schema(schema_name):
            continue
        if include and not _matches(include, schema_name, table_name):
            continue
        if exclude and _matches(exclude, schema_name, table_name):
            continue
        selected.append((schema_name, table_name))

    selected.sort(key=lambda t: (t[0].lower(), t[1].lower()))
    selected_keys = {f"{s}.{t}".lower() for s, t in selected}
    for name in overrides:
        if verbose and name not in selected_keys:
            op.warning(f"cursor_overrides names {name}, which is not among the tables being synced")

    columns, primary_keys, indexed = {}, {}, set()
    for schema_name in sorted({s for s, _ in selected}):
        _load_schema_catalog(conn, schema_name, columns, primary_keys, indexed)

    colliding = _colliding_tables(selected, columns)
    if verbose and colliding:
        shown = ", ".join(sorted(colliding)[:20]) + (f" and {len(colliding) - 20} more" if len(colliding) > 20 else "")
        op.warning(
            f"{len(colliding)} tables skipped because their schema, table or column names differ only in case or "
            f"punctuation and would be merged in the destination: {shown}"
        )

    specs, no_primary_key = [], []
    for schema_name, table_name in selected:
        key = (schema_name, table_name)
        if f"{schema_name}.{table_name}" in colliding:
            continue
        table_columns = columns.get(key, [])
        if not table_columns:
            if verbose:
                op.warning(f"{schema_name}.{table_name}: no columns found in the catalog; table skipped")
            continue
        if not primary_keys.get(key):
            # Not supported yet: without a key, Fivetran hashes every value into _fivetran_id, which
            # makes upserts slow and collapses rows that are exact duplicates.
            no_primary_key.append(f"{schema_name}.{table_name}")
            continue
        spec = {
            "schema": schema_name,
            "table": table_name,
            "columns": table_columns,
            "primary_key": primary_keys.get(key, []),
        }
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        if verbose and spec["cursor"] and (schema_name, table_name, spec["cursor"]["name"]) not in indexed:
            log.warning(
                f"{_table_key(spec)}: cursor column {spec['cursor']['name']} is not the leading column "
                "of any index, so every batch query scans the table. An index on it makes syncs much faster."
            )
        spec["batch_size"], spec["row_cap"] = _batch_limits(table_columns)
        specs.append(spec)

    if verbose and no_primary_key:
        shown = ", ".join(no_primary_key[:20]) + (f" and {len(no_primary_key) - 20} more" if len(no_primary_key) > 20 else "")
        op.warning(f"{len(no_primary_key)} tables skipped because they have no primary key: {shown}")
        log.info(f"Tables skipped for having no primary key: {', '.join(no_primary_key)}")
    return specs


def _destination_name(name: str) -> str:
    """Approximation of Fivetran's destination naming (lower snake_case, camelCase split, other
    characters replaced by '_'), used only to detect names that would collide."""
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _colliding_tables(selected: list, columns: dict) -> set:
    """'schema.table' names to skip: selected tables whose destination names collide with another
    selected table, and tables with two columns whose destination names collide. IRIS: delimited
    identifiers allow names such as "order-items" next to order_items, or "CamelCase" next to camel_case."""
    groups = {}
    for schema_name, table_name in selected:
        groups.setdefault((_destination_name(schema_name), _destination_name(table_name)), []).append(
            f"{schema_name}.{table_name}")
    colliding = {name for names in groups.values() if len(names) > 1 for name in names}
    for schema_name, table_name in selected:
        destination = [_destination_name(c["name"]) for c in columns.get((schema_name, table_name), [])]
        if len(set(destination)) < len(destination):
            colliding.add(f"{schema_name}.{table_name}")
    return colliding


def _load_schema_catalog(conn, schema_name: str, columns: dict, primary_keys: dict, indexed: set):
    params = {"schema": schema_name}
    rows = conn.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        ),
        params,
    ).fetchall()
    for table_name, column_name, data_type, precision, scale, nullable in rows:
        columns.setdefault((schema_name, table_name), []).append(
            {
                "name": column_name,
                "type": (data_type or "").lower(),
                "precision": precision,
                "scale": scale,
                "nullable": (nullable or "YES").upper() != "NO",
            }
        )

    rows = conn.execute(
        text(
            # IRIS: KEY_COLUMN_USAGE carries CONSTRAINT_TYPE itself (no join to TABLE_CONSTRAINTS needed)
            "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = :schema AND CONSTRAINT_TYPE = 'PRIMARY KEY' "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        ),
        params,
    ).fetchall()
    for table_name, column_name in rows:
        primary_keys.setdefault((schema_name, table_name), []).append(column_name)

    rows = conn.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES "
            "WHERE TABLE_SCHEMA = :schema AND ORDINAL_POSITION = 1"
        ),
        params,
    ).fetchall()
    for table_name, column_name in rows:
        indexed.add((schema_name, table_name, column_name))


def _choose_cursor(spec: dict, candidates: list, overrides: dict, verbose: bool):
    """Return the cursor column for an incremental sync, or None for a full reload."""
    key = _table_key(spec)
    by_name = {c["name"].lower(): c for c in spec["columns"]}
    override = overrides.get(key.lower())

    if override is not None:
        if override == "":
            if verbose:
                log.info(f"{key}: cursor_overrides forces a full reload")
            return None
        column = by_name.get(override)
        if column is None:
            raise ValueError(f"cursor_overrides: column {override} not found in {key}")
        if column["type"] not in CURSOR_TYPES:
            raise ValueError(
                f"cursor_overrides: {key}.{column['name']} has type {column['type']}; "
                "a cursor must be a timestamp, date or integer column"
            )
        return column

    for name in candidates:
        column = by_name.get(name)
        if column and column["type"] in CURSOR_TYPES:
            return column
    return None


def _batch_limits(columns: list):
    """Rows per query and per checkpoint, smaller for wide tables and tables with stream columns."""
    if any(c["type"] in LOB_TYPES for c in columns):
        return LOB_BATCH_SIZE, LOB_CHECKPOINT_ROW_CAP
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(columns)))
    return (
        max(MIN_BATCH_SIZE, int(BATCH_SIZE * scale)),
        max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale)),
    )


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _declared_type(column: dict):
    """Type to declare in schema(), wherever the SDK would infer the wrong one from the delivered value.

    Integers are left to inference. Floats and strings are not: the SDK infers an undeclared Python
    float as a 32-bit FLOAT (3.141592653589793 arrived as 3.1415927, -1e300 as -inf), and an undeclared
    string column from its content (VARCHAR 't' became BOOLEAN, '2024-01-01' DATE, '1.50' DOUBLE 1.5).
    """
    data_type = column["type"]
    if data_type in DECIMAL_TYPES:
        precision = column["precision"]
        if precision and precision <= MAX_DECIMAL_PRECISION:
            return {"type": "DECIMAL", "precision": int(precision), "scale": int(column["scale"] or 0)}
        return "STRING"  # delivered as a string
    if data_type in FLOAT_TYPES:
        return "DOUBLE"
    if data_type in STRING_TYPES:
        return "STRING"
    if data_type in TIMESTAMP_TYPES:
        return "NAIVE_DATETIME"  # IRIS: TIMESTAMP has no time zone
    if data_type in DATE_TYPES:
        return "NAIVE_DATE"
    if data_type in BOOLEAN_TYPES:
        return "BOOLEAN"
    if data_type in BINARY_TYPES:
        return "BINARY"
    return None


def _table_definition(spec: dict) -> dict:
    definition = {"schema": spec["schema"], "table": spec["table"]}
    if spec["primary_key"]:
        definition["primary_key"] = spec["primary_key"]
    declared = {}
    for column in spec["columns"]:
        declared_type = _declared_type(column)
        if declared_type:
            declared[column["name"]] = declared_type
    if declared:
        definition["columns"] = declared
    return definition


# Timestamps and dates are passed to the SDK as datetime/date objects, not ISO strings: the SDK
# re-parses strings with strptime, which took about 13% of sync time in a profile of 200k rows.
def _datetime_value(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    return str(value).replace(" ", "T", 1)


def _date_value(value):
    return value if isinstance(value, date) else str(value)


def _time_value(value):
    return value.isoformat() if isinstance(value, dt_time) else str(value)


def _decimal_value(value):
    return format(value, "f") if isinstance(value, Decimal) else str(value)


def _binary_value(value):
    return bytes(value) if isinstance(value, (memoryview, bytearray)) else value


def _converter(column: dict):
    """Function turning a driver value into one the SDK accepts, or None to pass it through."""
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return _datetime_value
    if data_type in DATE_TYPES:
        return _date_value
    if data_type in TIME_TYPES:
        return _time_value
    if data_type in DECIMAL_TYPES:
        return _decimal_value
    if data_type in BOOLEAN_TYPES:
        return bool
    if data_type in BINARY_TYPES:
        return _binary_value
    return None


def _key_value(value):
    """JSON-serialisable keyset value, stored in state and bound back into the next query."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, (date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


# ---------------------------------------------------------------------------------------------
# Batched keyset reads
# ---------------------------------------------------------------------------------------------


def _select_expr(column: dict, ordered: bool) -> str:
    quoted = _quote(column["name"])
    if not ordered:
        return quoted
    # IRIS: a column that is both selected and in ORDER BY comes back in its internal form (e.g.
    # timestamps as '1154657064703846976', dates as day numbers, numerics as strings). Casting it
    # to its own type returns the normal value. Integer and string columns are not affected.
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return f"CAST({quoted} AS TIMESTAMP)"
    if data_type in DATE_TYPES:
        return f"CAST({quoted} AS DATE)"
    if data_type in TIME_TYPES:
        return f"CAST({quoted} AS TIME)"
    if data_type in DECIMAL_TYPES and column.get("precision"):
        return f"CAST({quoted} AS NUMERIC({int(column['precision'])},{int(column['scale'] or 0)}))"
    if data_type in FLOAT_TYPES:
        return f"CAST({quoted} AS DOUBLE)"
    return quoted


def _param_expr(column: dict, param: str) -> str:
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return f"CAST(:{param} AS TIMESTAMP)"
    if data_type in DATE_TYPES:
        return f"CAST(:{param} AS DATE)"
    if data_type in TIME_TYPES:
        return f"CAST(:{param} AS TIME)"
    return f":{param}"


def _keyset_condition(order_columns: list, after: list, params: dict) -> str:
    """Rows strictly after `after` in (order_columns) order, expanded for databases without row-value comparison."""
    clauses = []
    for i, column in enumerate(order_columns):
        parts = [f"{_quote(order_columns[j]['name'])} = {_param_expr(order_columns[j], f'k{j}')}" for j in range(i)]
        parts.append(f"{_quote(column['name'])} > {_param_expr(column, f'k{i}')}")
        clauses.append("(" + " AND ".join(parts) + ")")
    for i, value in enumerate(after):
        params[f"k{i}"] = value
    return "(" + " OR ".join(clauses) + ")"


# Total seconds spent in Python's garbage collector and the number of full (generation 2) collections,
# measured with gc.callbacks. GC pauses happen inside the stages below, so the timer reports them
# separately ("of which gc ...") rather than as a stage of their own.
_GC = {"seconds": 0.0, "full": 0, "started": 0.0}


def _gc_callback(phase: str, info: dict):
    if phase == "start":
        _GC["started"] = time.perf_counter()
    else:
        _GC["seconds"] += time.perf_counter() - _GC["started"]
        if info.get("generation") == 2:
            _GC["full"] += 1


if _gc_callback not in gc.callbacks:
    gc.callbacks.append(_gc_callback)


class _Timer:
    """Wall time per sync stage, so slow syncs can be diagnosed from the logs.

    query      execute(): the database runs the batch query (for an ORDER BY over an unindexed column
               this includes scanning and sorting the table) until the driver has the first rows
    fetch      fetchall(): transferring and decoding the rest of the batch in the driver
    convert    turning driver values into SDK records
    upsert     op.upsert() calls (SDK validation and buffering)
    checkpoint op.checkpoint() calls (the SDK flushes buffered rows to the destination)
    other      everything else (generator overhead, state updates, logging)
    """

    STAGES = ("query", "fetch", "convert", "upsert", "checkpoint")

    def __init__(self, key: str):
        self.key = key
        self.total = dict.fromkeys(self.STAGES, 0.0)
        self.interval = dict.fromkeys(self.STAGES, 0.0)
        self.queries = self.interval_queries = 0
        self.slowest_query = self.interval_slowest = 0.0
        self.start = self.interval_start = time.perf_counter()
        self.gc_start = self.interval_gc = (_GC["seconds"], _GC["full"])

    def add(self, stage: str, seconds: float):
        self.total[stage] += seconds
        self.interval[stage] += seconds

    def add_query(self, seconds: float):
        self.add("query", seconds)
        self.queries += 1
        self.interval_queries += 1
        self.slowest_query = max(self.slowest_query, seconds)
        self.interval_slowest = max(self.interval_slowest, seconds)

    def _format(self, stages: dict, wall: float, queries: int, slowest: float, gc_since: tuple) -> str:
        parts = ", ".join(f"{s} {stages[s]:.1f}s" for s in self.STAGES)
        other = wall - sum(stages.values())
        average = stages["query"] / queries if queries else 0.0
        gc_seconds, gc_full = _GC["seconds"] - gc_since[0], _GC["full"] - gc_since[1]
        return (f"{wall:.1f}s wall = {parts}, other {other:.1f}s; "
                f"{queries} queries, avg {average:.2f}s, slowest {slowest:.2f}s; "
                f"of which gc {gc_seconds:.1f}s ({gc_full} full collections)")

    def report_interval(self, label: str):
        now = time.perf_counter()
        log.info(f"{self.key}: timing ({label}): "
                 + self._format(self.interval, now - self.interval_start, self.interval_queries, self.interval_slowest,
                                self.interval_gc))
        self.interval_gc = (_GC["seconds"], _GC["full"])
        self.interval = dict.fromkeys(self.STAGES, 0.0)
        self.interval_queries = 0
        self.interval_slowest = 0.0
        self.interval_start = now

    def report_total(self, label: str):
        log.info(f"{self.key}: timing ({label}): "
                 + self._format(self.total, time.perf_counter() - self.start, self.queries, self.slowest_query, self.gc_start))


def _read_rows(conn, spec: dict, order_columns: list, conditions: list, params: dict, after, timer: _Timer):
    """Yield (record, keyset position) in order_columns order, one bounded query per batch."""
    batch_size = spec["batch_size"]
    columns = spec["columns"]
    ordered = {c["name"] for c in order_columns}
    converters = [_converter(c) for c in columns]
    select_list = [_select_expr(c, c["name"] in ordered) for c in columns]
    # Where each keyset value sits in the row. Keyset columns are delivered columns and are selected
    # only once: IRIS rejects ORDER BY on a column selected twice as ambiguous.
    names = [c["name"] for c in columns]
    key_positions = [names.index(c["name"]) for c in order_columns]
    order_by = ", ".join(_quote(c["name"]) for c in order_columns)

    while True:
        where = list(conditions)
        query_params = dict(params)
        if after is not None:
            where.append(_keyset_condition(order_columns, after, query_params))
        sql = f"SELECT TOP {batch_size} {', '.join(select_list)} FROM {_table_ref(spec)}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by}"

        if timer.queries == 0:
            log.info(f"{timer.key}: batch query: {sql}")
        started = time.perf_counter()
        result = conn.execute(text(sql), query_params)
        executed = time.perf_counter()
        rows = result.fetchall()
        fetched = time.perf_counter()
        timer.add_query(executed - started)
        timer.add("fetch", fetched - executed)

        # Convert the whole batch at once so the conversion can be timed separately from the upserts
        batch = []
        for row in rows:
            record = {}
            for column, convert, value in zip(columns, converters, row):
                record[column["name"]] = convert(value) if convert and value is not None else value
            batch.append((record, [_key_value(row[i]) for i in key_positions]))
        row_count = len(rows)
        del rows
        timer.add("convert", time.perf_counter() - fetched)

        yield from batch
        if row_count < batch_size:
            return
        # The next batch starts after the last row of this one. Fail rather than loop if it would not move.
        position = batch[-1][1]
        if position == after:
            raise RuntimeError(f"{timer.key}: keyset position did not advance between batches; stopping to avoid re-reading the same rows")
        after = position


class _Checkpointer:
    """Checkpoint on whichever comes first: the time interval or the row cap."""

    def __init__(self, state: dict, row_cap: int, timer: _Timer):
        self.state = state
        self.row_cap = row_cap
        self.timer = timer
        self.rows = 0
        self.last = time.monotonic()

    def row_done(self):
        self.rows += 1
        if self.rows >= self.row_cap or time.monotonic() - self.last >= CHECKPOINT_INTERVAL_SECONDS:
            self.checkpoint()

    def checkpoint(self):
        started = time.perf_counter()
        op.checkpoint(state=self.state)
        self.timer.add("checkpoint", time.perf_counter() - started)
        self.rows = 0
        self.last = time.monotonic()


def _upsert(spec: dict, record: dict, timer: _Timer):
    started = time.perf_counter()
    op.upsert(table=spec["table"], data=record, schema=spec["schema"])
    timer.add("upsert", time.perf_counter() - started)


def _log_progress(timer: _Timer, total: int, detail: str = ""):
    if total % PROGRESS_LOG_EVERY == 0:
        log.info(f"{timer.key}: {total:,} rows synced{detail}")
        timer.report_interval(f"last {PROGRESS_LOG_EVERY:,} rows")


# ---------------------------------------------------------------------------------------------
# Sync strategies
# ---------------------------------------------------------------------------------------------


def _table_state(spec: dict, previous):
    """State for one table, reset when its strategy or cursor column changes; reports column drift."""
    key = _table_key(spec)
    mode = "incremental" if spec["cursor"] else "full"
    cursor_name = spec["cursor"]["name"] if spec["cursor"] else None
    column_names = sorted(c["name"] for c in spec["columns"])

    if previous and (previous.get("mode") != mode or previous.get("cursor_column") != cursor_name):
        log.warning(
            f"{key}: sync strategy changed from {previous.get('mode')} ({previous.get('cursor_column')}) "
            f"to {mode} ({cursor_name}); syncing this table from the beginning"
        )
        previous = None

    if previous:
        known = set(previous.get("columns", []))
        added = [c for c in column_names if c not in known]
        dropped = sorted(known - set(column_names))
        if added and known:
            if mode == "incremental":
                op.warning(
                    f"{key}: new columns {', '.join(added)}. Existing rows get values for them only when "
                    "they next change; re-sync the table to backfill."
                )
            else:
                log.info(f"{key}: new columns {', '.join(added)}")
        if dropped:
            log.info(f"{key}: columns no longer in the source: {', '.join(dropped)} (kept in the destination; NULL in rows synced from now on)")
        table_state = previous
    else:
        table_state = {"mode": mode}
        if cursor_name:
            table_state["cursor_column"] = cursor_name
    table_state["columns"] = column_names
    return table_state


def _column(spec: dict, name: str) -> dict:
    return next(c for c in spec["columns"] if c["name"] == name)


def _sync_incremental(conn, spec: dict, table_state: dict, state: dict):
    key = _table_key(spec)
    cursor = spec["cursor"]
    order_columns = [cursor] + [_column(spec, name) for name in spec["primary_key"]]
    conditions, params = [], {}
    after = table_state.get("after")
    if after is not None:
        # Resume exactly after the last (cursor, primary key) delivered before the interruption.
        # Log only the cursor value: primary-key values can be personal data.
        log.info(f"{key}: resuming incremental sync after {cursor['name']}={after[0]}")
    elif table_state.get("cursor") is None:
        conditions.append(f"{_quote(cursor['name'])} IS NOT NULL")
        log.info(f"{key}: initial incremental sync on {cursor['name']}")
    else:
        # >= (not >) so rows sharing the cursor value are never skipped, including rows written at
        # that value after the previous sync. Re-reads are harmless: upserts are keyed on the primary key.
        conditions.append(f"{_quote(cursor['name'])} >= {_param_expr(cursor, 'cursor')}")
        params["cursor"] = table_state["cursor"]
        log.info(f"{key}: incremental sync from {cursor['name']} >= {table_state['cursor']}")

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_rows(conn, spec, order_columns, conditions, params, after, timer):
        _upsert(spec, record, timer)
        table_state["after"] = position
        total += 1
        _log_progress(timer, total, f", at {cursor['name']}={position[0]}")
        checkpointer.row_done()

    if "after" in table_state:
        # Table finished: keep the cursor value, drop the key so the next sync starts at >= cursor
        table_state["cursor"] = table_state.pop("after")[0]
    checkpointer.checkpoint()
    log.info(f"{key}: {total:,} rows synced, {cursor['name']}={table_state.get('cursor')}")
    timer.report_total("table total")

    if cursor["nullable"]:
        _sync_null_cursor_rows(conn, spec, state)


def _sync_null_cursor_rows(conn, spec: dict, state: dict):
    """Re-send rows whose cursor is NULL. The cursor can never select them, so read them by key every sync."""
    key = _table_key(spec)
    cursor = spec["cursor"]
    order_columns = [_column(spec, name) for name in spec["primary_key"]]
    conditions = [f"{_quote(cursor['name'])} IS NULL"]
    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, _ in _read_rows(conn, spec, order_columns, conditions, {}, None, timer):
        _upsert(spec, record, timer)
        total += 1
        _log_progress(timer, total, f" with NULL {cursor['name']}")
        checkpointer.row_done()
    if total:
        checkpointer.checkpoint()
        log.info(f"{key}: {total:,} rows with NULL {cursor['name']} re-sent (they are re-sent every sync)")
    timer.report_total(f"NULL {cursor['name']} rows")


def _sync_full(conn, spec: dict, table_state: dict, state: dict):
    key = _table_key(spec)
    order_columns = [_column(spec, name) for name in spec["primary_key"]]
    after = table_state.get("after")
    if after is None:
        # Applied only after update() returns successfully, so a failed sync deletes nothing. Rows
        # upserted after this call are kept; rows not sent again are marked deleted.
        op.truncate(table=spec["table"], schema=spec["schema"], deferred=True)
        log.info(f"{key}: full reload")
    else:
        # The deferred truncate from the interrupted sync still applies; calling it again would
        # mark the rows delivered before the interruption as deleted.
        log.info(f"{key}: resuming full reload from its last checkpoint")

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_rows(conn, spec, order_columns, [], {}, after, timer):
        _upsert(spec, record, timer)
        table_state["after"] = position
        total += 1
        _log_progress(timer, total)
        checkpointer.row_done()

    table_state.pop("after", None)
    checkpointer.checkpoint()
    log.info(f"{key}: full reload done, {total:,} rows")
    timer.report_total("table total")


# ---------------------------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------------------------


def schema(configuration: dict):
    _validate_configuration(configuration)
    engine = _engine(configuration)
    try:
        with _connect(engine, configuration) as conn:
            specs = _discover(conn, configuration, verbose=False)
    finally:
        engine.dispose()
    return [_table_definition(spec) for spec in specs]


def update(configuration: dict, state: dict):
    _validate_configuration(configuration)
    tables_state = state.setdefault("tables", {})
    engine = _engine(configuration)
    try:
        with _connect(engine, configuration) as conn:
            specs = _discover(conn, configuration)
            if not specs:
                op.warning("No tables matched the schemas / include_tables / exclude_tables settings")
                return
            incremental = sum(1 for s in specs if s["cursor"])
            log.info(f"Syncing {len(specs)} tables: {incremental} incremental, {len(specs) - incremental} full reload")
            for number, spec in enumerate(specs, 1):
                key = _table_key(spec)
                table_state = _table_state(spec, tables_state.get(key))
                tables_state[key] = table_state
                log.info(f"{key}: table {number} of {len(specs)}")
                if spec["cursor"]:
                    _sync_incremental(conn, spec, table_state, state)
                else:
                    _sync_full(conn, spec, table_state, state)
    finally:
        engine.dispose()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
