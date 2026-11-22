"""Fivetran Connector SDK connector for Exasol.

Discovers tables, columns and primary keys from the EXA_ALL_* catalog views and picks a strategy
per table:

- Incremental: the table has a primary key and a usable cursor column (configurable, default
  `updated_at`). Reads `cursor >= last value`, ordered by (cursor, primary key) with keyset
  continuation. Rows whose cursor is NULL are re-read every sync, so they are never lost.
- Full reload: tables with a primary key but no usable cursor. Deferred truncate, then upsert every
  row ordered by the primary key, so a failed sync deletes nothing.

Tables without a primary key are skipped (with a dashboard warning listing them). Without a key,
Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses rows that
are exact duplicates. Disabled (unenforced) primary-key constraints count as primary keys: they are
common in Exasol, but the database does not check them, so duplicate key values in the source
collapse into one destination row.

Reads are streamed: each query's result set stays on the Exasol server and pyexasol fetches it in
chunks of FETCH_SIZE_BYTES through the result-set handle, so the client holds one chunk at a time.
Each query covers at most ROWS_PER_QUERY rows; the next query continues after the last
(cursor, primary key) delivered. Exasol has no user-defined indexes, so every query sorts the rows
it selects; with large chunks that cost is paid a few times per table instead of once per small
batch (measured on 10M rows: one sort about 4 s, versus about 1.8 s for each 50k-row batch).

Ported from the IRIS connector (iris/connector.py), the template for this repo's SQL connectors.
Exasol-specific parts are marked "Exasol:".
"""

import gc
import re
import ssl
import time
from datetime import date, datetime, timezone

import pyexasol
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Rows per query. Only the server holds the result set (the client streams it in FETCH_SIZE_BYTES
# chunks), so this bounds the server-side result size and how long its handle stays open, not
# client memory. Each query re-sorts the remaining rows (about 2-4 s on 10M rows), so 500k costs
# roughly 20 queries per 10M rows, small next to upsert time.
ROWS_PER_QUERY = 500_000
# Rows taken from the result-set stream at a time (timed as "fetch"), and the byte size of each
# chunk pyexasol requests from the server
FETCH_ROWS = 10_000
FETCH_SIZE_BYTES = 10 * 1024 * 1024
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_FETCH_ROWS = 500
MIN_CHECKPOINT_ROW_CAP = 10_000
# Exasol: tables with very wide columns (VARCHAR up to 2M characters, GEOMETRY) can have very large rows
LARGE_COLUMN_BYTES = 65_536
LARGE_FETCH_ROWS = 100
LARGE_CHECKPOINT_ROW_CAP = 10_000
PROGRESS_LOG_EVERY = 250_000
# Exasol: seconds to wait for a server response. An ORDER BY over a large table runs before the
# first rows come back, so this is far above pyexasol's 30 s default.
SOCKET_TIMEOUT_SECONDS = 3600
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_SECONDS = 5

DEFAULT_CURSOR_COLUMNS = "updated_at"

# Exasol: SYS holds the system catalog and EXA_STATISTICS the statistics tables. Virtual schemas
# (EXA_ALL_VIRTUAL_SCHEMAS) are views over other systems and are skipped too. A schema named
# explicitly in the `schemas` setting is synced even if it is one of these.
SYSTEM_SCHEMAS = {"sys", "exa_statistics"}
SYSTEM_SCHEMA_PREFIXES = ()

# Exasol: base type names, parsed from EXA_ALL_COLUMNS.COLUMN_TYPE (e.g. 'DECIMAL(18,0)',
# 'VARCHAR(100) UTF8', 'TIMESTAMP(3) WITH LOCAL TIME ZONE') and compared lower-cased
TIMESTAMP_TYPES = {"timestamp"}
TIMESTAMP_TZ_TYPES = {"timestamp with local time zone"}
DATE_TYPES = {"date"}
DECIMAL_TYPES = {"decimal"}
FLOAT_TYPES = {"double"}
BOOLEAN_TYPES = {"boolean"}
CURSOR_TYPES = TIMESTAMP_TYPES | TIMESTAMP_TZ_TYPES | DATE_TYPES  # plus DECIMAL with scale 0
MAX_LONG_DIGITS = 18  # DECIMAL(p,0) with p <= 18 always fits a 64-bit LONG
MAX_DECIMAL_PRECISION = 38  # the SDK's DECIMAL limit (Exasol's own limit is 36)

# Exasol: every session renders and parses timestamps and dates in these formats (full nanosecond
# precision, so keyset values round-trip exactly) and in UTC, so TIMESTAMP WITH LOCAL TIME ZONE
# values come back as UTC.
SESSION_SETTINGS = (
    "ALTER SESSION SET TIME_ZONE = 'UTC'",
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'",
    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
    "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
)

_NUMBER = re.compile(r"^[+-]?[0-9]+(\.[0-9]+)?$")


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _validate_configuration(configuration: dict):
    for key in ("host", "username", "password"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if not str(configuration.get("port") or "8563").isdigit():
        raise ValueError("port must be a number")
    if str(configuration.get("validate_certificate") or "true").lower() not in ("true", "false"):
        raise ValueError("validate_certificate must be true or false")
    for entry in _csv(configuration, "cursor_overrides"):
        name, sep, _ = entry.partition("=")
        if not sep or "." not in name:
            raise ValueError(
                f"Invalid cursor_overrides entry '{entry}': use schema.table=column, "
                "or schema.table= to force a full reload"
            )


def _connect(configuration: dict):
    """Open a connection, retrying transient network failures; bad credentials fail at once."""
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return _connect_once(configuration)
        except pyexasol.ExaAuthError as e:
            raise RuntimeError(f"Exasol rejected the username or password: {e.message}") from None
        except (pyexasol.ExaConnectionError, pyexasol.ExaCommunicationError) as e:
            if attempt == CONNECT_ATTEMPTS:
                raise RuntimeError(
                    f"Could not connect to Exasol at {configuration['host']}:{configuration.get('port') or 8563} "
                    f"after {CONNECT_ATTEMPTS} attempts: {e.message}"
                ) from None
            delay = CONNECT_RETRY_SECONDS * 2 ** (attempt - 1)
            log.warning(f"Connecting to Exasol failed (attempt {attempt} of {CONNECT_ATTEMPTS}), retrying in {delay}s: {e.message}")
            time.sleep(delay)


def _connect_once(configuration: dict):
    kwargs = {}
    if str(configuration.get("validate_certificate") or "true").lower() == "false":
        # For servers with a self-signed certificate. The connection is still encrypted.
        kwargs["websocket_sslopt"] = {"cert_reqs": ssl.CERT_NONE}
    conn = pyexasol.connect(
        dsn=f"{configuration['host']}:{configuration.get('port') or 8563}",
        user=configuration["username"],
        password=configuration["password"],
        encryption=True,
        autocommit=True,
        fetch_size_bytes=FETCH_SIZE_BYTES,
        socket_timeout=SOCKET_TIMEOUT_SECONDS,
        client_name="fivetran-connector-sdk",
        # Exasol: no fetch_mapper. Values arrive as JSON (int, float, bool, str); the connector
        # converts them itself and keeps the raw strings as keyset values, because a Python datetime
        # would truncate TIMESTAMP(9) values to microseconds and break the keyset comparison.
        **kwargs,
    )
    for statement in SESSION_SETTINGS:
        conn.execute(statement)
    return conn


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_ref(spec: dict) -> str:
    return f"{_quote(spec['schema'])}.{_quote(spec['table'])}"


def _table_key(spec: dict) -> str:
    return f"{spec['schema']}.{spec['table']}"


def _is_system_schema(schema_name: str) -> bool:
    lowered = schema_name.lower()
    return lowered in SYSTEM_SCHEMAS or (bool(SYSTEM_SCHEMA_PREFIXES) and lowered.startswith(SYSTEM_SCHEMA_PREFIXES))


def _matches(patterns: list, schema_name: str, table_name: str) -> bool:
    """True if a pattern ('schema.table' or bare 'table', case-insensitive) names this table."""
    qualified = f"{schema_name}.{table_name}".lower()
    bare = table_name.lower()
    return any(p.lower() in (qualified, bare) for p in patterns)


def _query(conn, sql: str) -> list:
    """Run a catalog query and return all rows. Never passes query parameters: pyexasol formats
    parameters with str.format, which would misread braces in quoted identifiers or literals."""
    statement = conn.execute(sql)
    try:
        return statement.fetchall()
    finally:
        statement.close()


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

    # Exasol: EXA_ALL_TABLES lists the tables the user can access (views are in EXA_ALL_VIEWS and are
    # not synced). Virtual schemas are skipped unless named in `schemas`.
    virtual = {row[0].lower() for row in _query(conn, "SELECT SCHEMA_NAME FROM EXA_ALL_VIRTUAL_SCHEMAS")}
    catalog_tables = _query(conn, "SELECT TABLE_SCHEMA, TABLE_NAME FROM EXA_ALL_TABLES")
    selected = []
    for schema_name, table_name in catalog_tables:
        if schemas:
            if schema_name.lower() not in schemas:
                continue
        elif _is_system_schema(schema_name) or schema_name.lower() in virtual:
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

    columns, primary_keys, disabled_keys = {}, {}, set()
    for schema_name in sorted({s for s, _ in selected}):
        _load_schema_catalog(conn, schema_name, columns, primary_keys, disabled_keys)

    colliding = _colliding_tables(selected, columns)
    if verbose and colliding:
        op.warning(
            f"{len(colliding)} tables skipped because their table or column names differ only in case or "
            "punctuation and would be merged in the destination: " + ", ".join(sorted(colliding)[:20])
            + ". Exclude one of each pair with exclude_tables, or rename the columns."
        )

    specs, no_primary_key, unenforced = [], [], []
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
        if key in disabled_keys:
            unenforced.append(f"{schema_name}.{table_name}")
        spec = {
            "schema": schema_name,
            "table": table_name,
            "columns": table_columns,
            "primary_key": primary_keys[key],
        }
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        spec["fetch_rows"], spec["row_cap"] = _batch_limits(table_columns)
        specs.append(spec)

    if verbose and no_primary_key:
        shown = ", ".join(no_primary_key[:20]) + (f" and {len(no_primary_key) - 20} more" if len(no_primary_key) > 20 else "")
        op.warning(f"{len(no_primary_key)} tables skipped because they have no primary key: {shown}")
        log.info(f"Tables skipped for having no primary key: {', '.join(no_primary_key)}")
    if verbose and unenforced:
        log.info(
            "Primary keys taken from disabled (unenforced) constraints; duplicate key values in the "
            f"source would collapse into one destination row: {', '.join(unenforced)}"
        )
    return specs


def _destination_name(name: str) -> str:
    """Approximation of Fivetran's destination naming (lower snake_case, camelCase split,
    other characters replaced by '_'), used only to detect names that would collide."""
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _colliding_tables(selected: list, columns: dict) -> set:
    """'schema.table' names to skip: tables whose destination names collide with another selected
    table, and tables with two columns whose destination names collide. Exasol: quoted identifiers
    are case-sensitive, so "Orders" and ORDERS can both exist."""
    groups = {}
    for schema_name, table_name in selected:
        groups.setdefault((_destination_name(schema_name), _destination_name(table_name)), []).append(
            f"{schema_name}.{table_name}")
    colliding = {name for names in groups.values() if len(names) > 1 for name in names}
    for (schema_name, table_name), table_columns in columns.items():
        destination = [_destination_name(c["name"]) for c in table_columns]
        if len(set(destination)) < len(destination):
            colliding.add(f"{schema_name}.{table_name}")
    return colliding


def _base_type(column_type: str) -> str:
    """Exasol: 'TIMESTAMP(3) WITH LOCAL TIME ZONE' -> 'timestamp with local time zone',
    'VARCHAR(100) UTF8' -> 'varchar', 'INTERVAL DAY(2) TO SECOND(3)' -> 'interval day to second'."""
    base = re.sub(r"\([^)]*\)", "", column_type)
    base = re.sub(r"\s+(UTF8|ASCII)$", "", base.strip(), flags=re.IGNORECASE)
    return " ".join(base.lower().split())


def _load_schema_catalog(conn, schema_name: str, columns: dict, primary_keys: dict, disabled_keys: set):
    schema_literal = _string_literal(schema_name)
    rows = _query(
        conn,
        "SELECT COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, "
        "COLUMN_IS_NULLABLE, COLUMN_MAXSIZE FROM EXA_ALL_COLUMNS "
        f"WHERE COLUMN_SCHEMA = {schema_literal} AND COLUMN_OBJECT_TYPE = 'TABLE' "
        "ORDER BY COLUMN_TABLE, COLUMN_ORDINAL_POSITION",
    )
    for table_name, column_name, column_type, precision, scale, nullable, max_size in rows:
        columns.setdefault((schema_name, table_name), []).append(
            {
                "name": column_name,
                "type": _base_type(column_type),
                # The full catalog type, e.g. 'TIMESTAMP(9)', used to cast keyset literals
                "sql_type": column_type,
                "precision": precision,
                "scale": scale,
                "nullable": nullable is not False,
                "max_size": max_size or 0,
            }
        )

    # Exasol: disabled primary-key constraints are listed too (CONSTRAINT_ENABLED = FALSE)
    rows = _query(
        conn,
        "SELECT c.CONSTRAINT_TABLE, c.COLUMN_NAME, k.CONSTRAINT_ENABLED "
        "FROM EXA_ALL_CONSTRAINT_COLUMNS c JOIN EXA_ALL_CONSTRAINTS k "
        "ON k.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA AND k.CONSTRAINT_TABLE = c.CONSTRAINT_TABLE "
        "AND k.CONSTRAINT_NAME = c.CONSTRAINT_NAME "
        f"WHERE c.CONSTRAINT_SCHEMA = {schema_literal} AND c.CONSTRAINT_TYPE = 'PRIMARY KEY' "
        "ORDER BY c.CONSTRAINT_TABLE, c.ORDINAL_POSITION",
    )
    for table_name, column_name, enabled in rows:
        primary_keys.setdefault((schema_name, table_name), []).append(column_name)
        if enabled is False:
            disabled_keys.add((schema_name, table_name))


def _is_cursor_type(column: dict) -> bool:
    if column["type"] in CURSOR_TYPES:
        return True
    return column["type"] in DECIMAL_TYPES and not column["scale"]


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
        if not _is_cursor_type(column):
            raise ValueError(
                f"cursor_overrides: {key}.{column['name']} has type {column['sql_type']}; "
                "a cursor must be a timestamp, date or integer (DECIMAL with scale 0) column"
            )
        return column

    for name in candidates:
        column = by_name.get(name)
        if column and _is_cursor_type(column):
            return column
    return None


def _batch_limits(columns: list):
    """Rows per fetch and per checkpoint, smaller for wide tables and tables with very wide columns."""
    if any(c["max_size"] > LARGE_COLUMN_BYTES for c in columns):
        return LARGE_FETCH_ROWS, LARGE_CHECKPOINT_ROW_CAP
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(columns)))
    return (
        max(MIN_FETCH_ROWS, int(FETCH_ROWS * scale)),
        max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale)),
    )


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _is_long(column: dict) -> bool:
    return column["type"] in DECIMAL_TYPES and not column["scale"] and (column["precision"] or 99) <= MAX_LONG_DIGITS


def _declared_type(column: dict):
    """Type to declare in schema(), only where the SDK cannot infer it from the delivered value."""
    data_type = column["type"]
    if data_type in DECIMAL_TYPES:
        # Exasol: INT / BIGINT are DECIMAL(18,0) / DECIMAL(36,0)
        if _is_long(column):
            return "LONG"
        precision = column["precision"]
        if precision and precision <= MAX_DECIMAL_PRECISION:
            return {"type": "DECIMAL", "precision": int(precision), "scale": int(column["scale"] or 0)}
        return None  # delivered as a string
    if data_type in TIMESTAMP_TYPES:
        return "NAIVE_DATETIME"
    if data_type in TIMESTAMP_TZ_TYPES:
        return "UTC_DATETIME"
    if data_type in DATE_TYPES:
        return "NAIVE_DATE"
    if data_type in FLOAT_TYPES:
        # Declared: the SDK infers Python floats as 32-bit FLOAT, which rounded 3.14159265358979 to
        # 3.1415927 and turned -1e300 into -inf in a test
        return "DOUBLE"
    if data_type in BOOLEAN_TYPES:
        return "BOOLEAN"
    return None


def _table_definition(spec: dict) -> dict:
    definition = {"schema": spec["schema"], "table": spec["table"], "primary_key": spec["primary_key"]}
    declared = {}
    for column in spec["columns"]:
        declared_type = _declared_type(column)
        if declared_type:
            declared[column["name"]] = declared_type
    if declared:
        definition["columns"] = declared
    return definition


# Exasol: raw values from the session formats in SESSION_SETTINGS. Timestamps are
# 'YYYY-MM-DD HH:MM:SS.fffffffff'; Python keeps microseconds, so the last three digits are dropped.
# They are passed to the SDK as datetime/date objects, not ISO strings: the SDK re-parses strings
# with strptime, which took about 13% of sync time in a profile of the IRIS connector.
def _datetime_value(value):
    return datetime.fromisoformat(value[:26])


def _utc_datetime_value(value):
    return datetime.fromisoformat(value[:26]).replace(tzinfo=timezone.utc)


def _date_value(value):
    return date.fromisoformat(value)


def _converter(column: dict):
    """Function turning a raw driver value into one the SDK accepts, or None to pass it through.

    Exasol: DECIMAL(p,0) with p <= 18 arrives as int; wider and scaled DECIMALs arrive as str, which
    the SDK accepts for DECIMAL. DOUBLE arrives as float and BOOLEAN as bool. CHAR, VARCHAR,
    INTERVAL, GEOMETRY (WKT) and HASHTYPE (hex) arrive as str and are delivered as strings.
    """
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return _datetime_value
    if data_type in TIMESTAMP_TZ_TYPES:
        return _utc_datetime_value
    if data_type in DATE_TYPES:
        return _date_value
    if data_type in DECIMAL_TYPES:
        return int if _is_long(column) else str
    return None


def _literal(column: dict, value) -> str:
    """SQL literal for a keyset value (a raw driver value, as stored in state)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"CAST('{value!r}' AS DOUBLE)"
    text = str(value)
    if column["type"] in DECIMAL_TYPES:
        if not _NUMBER.match(text):
            # The value itself is not shown: primary-key values can be personal data
            raise ValueError(f"Invalid numeric keyset value for {column['name']} in the saved state")
        return text
    # Exasol: casting to the column's own catalog type parses the string with the session formats
    # set in SESSION_SETTINGS (TIMESTAMP WITH LOCAL TIME ZONE in the session time zone, UTC).
    return f"CAST({_string_literal(text)} AS {column['sql_type']})"


# ---------------------------------------------------------------------------------------------
# Streamed keyset reads
# ---------------------------------------------------------------------------------------------


def _keyset_condition(order_columns: list, after: list) -> str:
    """Rows strictly after `after` in (order_columns) order, expanded for databases without row-value comparison."""
    clauses = []
    for i, column in enumerate(order_columns):
        parts = [f"{_quote(order_columns[j]['name'])} = {_literal(order_columns[j], after[j])}" for j in range(i)]
        parts.append(f"{_quote(column['name'])} > {_literal(column, after[i])}")
        clauses.append("(" + " AND ".join(parts) + ")")
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

    query      execute(): Exasol runs the query (selecting and sorting the rows) and returns the
               result-set handle with the first chunk
    fetch      taking rows from the result-set stream (pyexasol requests further chunks as needed)
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


def _read_rows(conn, spec: dict, order_columns: list, conditions: list, after, timer: _Timer):
    """Yield (record, keyset position) in order_columns order.

    Each query selects at most ROWS_PER_QUERY rows and is streamed from the server's result set;
    the next query continues after the last position delivered.
    """
    rows_per_query = ROWS_PER_QUERY
    fetch_rows = spec["fetch_rows"]
    columns = spec["columns"]
    names = [c["name"] for c in columns]
    converters = [_converter(c) for c in columns]
    key_positions = [names.index(c["name"]) for c in order_columns]
    select_list = ", ".join(_quote(name) for name in names)
    order_by = ", ".join(_quote(c["name"]) for c in order_columns)

    while True:
        where = list(conditions)
        if after is not None:
            where.append(_keyset_condition(order_columns, after))
        sql = f"SELECT {select_list} FROM {_table_ref(spec)}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by} LIMIT {int(rows_per_query)}"

        if timer.queries == 0:
            # Logged with the keyset values left out: primary-key values can be personal data
            logged = list(conditions) + (["<after the last row delivered>"] if after is not None else [])
            log.info(f"{timer.key}: query: SELECT {select_list} FROM {_table_ref(spec)}"
                     + (" WHERE " + " AND ".join(logged) if logged else "")
                     + f" ORDER BY {order_by} LIMIT {int(rows_per_query)}")
        started = time.perf_counter()
        statement = conn.execute(sql)
        timer.add_query(time.perf_counter() - started)
        row_count = 0
        position = after
        try:
            while True:
                fetch_started = time.perf_counter()
                rows = statement.fetchmany(fetch_rows)
                fetched = time.perf_counter()
                timer.add("fetch", fetched - fetch_started)
                if not rows:
                    break

                # Convert the whole fetch at once so conversion is timed separately from the upserts
                batch = []
                for row in rows:
                    record = {}
                    for name, convert, value in zip(names, converters, row):
                        record[name] = convert(value) if convert and value is not None else value
                    batch.append((record, [row[i] for i in key_positions]))
                row_count += len(rows)
                del rows
                timer.add("convert", time.perf_counter() - fetched)

                position = batch[-1][1]
                yield from batch
        finally:
            statement.close()

        if row_count < rows_per_query:
            return
        # The next query starts after the last row of this one. Fail rather than loop if it would not move.
        if position == after:
            raise RuntimeError(f"{timer.key}: keyset position did not advance between queries; stopping to avoid re-reading the same rows")
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
    conditions = []
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
        conditions.append(f"{_quote(cursor['name'])} >= {_literal(cursor, table_state['cursor'])}")
        log.info(f"{key}: incremental sync from {cursor['name']} >= {table_state['cursor']}")

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_rows(conn, spec, order_columns, conditions, after, timer):
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
    for record, _ in _read_rows(conn, spec, order_columns, conditions, None, timer):
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
    for record, position in _read_rows(conn, spec, order_columns, [], after, timer):
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
    conn = _connect(configuration)
    try:
        specs = _discover(conn, configuration, verbose=False)
    finally:
        conn.close()
    return [_table_definition(spec) for spec in specs]


def update(configuration: dict, state: dict):
    _validate_configuration(configuration)
    tables_state = state.setdefault("tables", {})
    conn = _connect(configuration)
    try:
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
        conn.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
