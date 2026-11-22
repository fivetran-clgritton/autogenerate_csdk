"""Fivetran Connector SDK connector for H2 Database (2.x, TCP server mode).

Discovers tables, columns and primary keys from INFORMATION_SCHEMA and picks a strategy per table:

- Incremental: the table has a primary key and a usable cursor column (configurable, default
  `updated_at`). Each sync first records the cursor's current maximum (the high-water mark), then
  reads every row with `cursor >= previous high-water mark` in bounded batches in primary-key order.
  When the table is done, the recorded maximum becomes the lower bound of the next sync. Rows whose
  cursor is NULL are re-read every sync, so they are never lost.
- Full reload: tables with a primary key but no usable cursor. Deferred truncate, then upsert every
  row in keyset batches on the primary key, so a failed sync deletes nothing.

Tables without a primary key are skipped (with a dashboard warning listing them). Without a key,
Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses rows that
are exact duplicates.

H2: why primary-key order instead of (cursor, primary key) order like the IRIS template. H2 sorts
large results by spilling them to disk, and that is very slow: a query over the 10M-row test table
ordered by an unindexed `updated_at` ran for more than 25 minutes without returning a row,
and a `LIMIT` batch in that order pays for the same sort every time. In primary-key order H2 walks
the primary-key index and stops after each batch, so nothing is sorted. The high-water mark is
taken before the read starts, so rows that change during the read get a cursor value at or above
it and are picked up by the next sync.

Based on the IRIS connector in this repo; database-specific parts are marked "H2:".
"""

import gc
import json
import os
import re
import time
from datetime import date, datetime, timezone

import jpype
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Rows per query for a table of up to WIDE_TABLE_COLUMNS columns (scaled down for wider tables).
# Each batch is its own bounded query in primary-key order, which H2 answers by walking the
# primary-key index and stopping after `LIMIT` rows.
BATCH_SIZE = 50_000
# Rows the H2 client fetches per network round trip (its default is 100)
FETCH_SIZE = 2_000
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_BATCH_SIZE = 1_000
MIN_CHECKPOINT_ROW_CAP = 10_000
# Tables with LOB columns (CLOB / BLOB / JSON / JAVA_OBJECT) can have very large rows
LOB_BATCH_SIZE = 1_000
LOB_CHECKPOINT_ROW_CAP = 10_000
LOB_FETCH_SIZE = 100
PROGRESS_LOG_EVERY = 250_000

DEFAULT_CURSOR_COLUMNS = "updated_at"
DEFAULT_PORT = "9092"
# H2: the JDBC driver jar, unless jar_path or the H2_JAR environment variable names one. The client
# must speak the server's protocol; matching the server version is safest.
DEFAULT_JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "drivers", "h2.jar")
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_SECONDS = 10
H2_CONNECTION_BROKEN = 90067  # H2 ErrorCode.CONNECTION_BROKEN_1
# Heap for the in-process JVM that runs the JDBC driver. It holds one fetch of rows at a time.
JVM_MAX_HEAP = "-Xmx512m"

# H2: 2.x has a single system schema. A schema named in the `schemas` setting is synced even if listed here.
SYSTEM_SCHEMAS = {"information_schema"}
SYSTEM_SCHEMA_PREFIXES = ()

# H2: INFORMATION_SCHEMA.COLUMNS.DATA_TYPE values in 2.x (compared lower-cased)
TIMESTAMP_TYPES = {"timestamp"}
TIMESTAMP_TZ_TYPES = {"timestamp with time zone"}
DATE_TYPES = {"date"}
INTEGER_TYPES = {"tinyint", "smallint", "integer", "bigint"}
DECIMAL_TYPES = {"numeric", "decimal"}
FLOAT_TYPES = {"real", "double precision"}
BOOLEAN_TYPES = {"boolean"}
JSON_TYPES = {"json"}
BINARY_TYPES = {"binary", "binary varying", "binary large object", "java_object"}
LOB_TYPES = {"character large object", "binary large object", "json", "java_object"}
CURSOR_TYPES = TIMESTAMP_TYPES | TIMESTAMP_TZ_TYPES | DATE_TYPES | INTEGER_TYPES
MAX_DECIMAL_PRECISION = 38  # the SDK's DECIMAL limit; wider values are delivered as strings
# Everything else (character types, DECFLOAT, TIME, TIME WITH TIME ZONE, INTERVAL, UUID, ENUM, ARRAY,
# ROW, GEOMETRY) is delivered as the string H2 returns for it, e.g. '[1, 2, 3]' for an ARRAY.

# H2: a host name or IP address, and a database name or path. Both go into the JDBC URL, where a ';'
# would let a configuration value add connection settings (H2 accepts settings such as INIT there).
HOST_PATTERN = re.compile(r"^[A-Za-z0-9.\-:\[\]]+$")
DATABASE_PATTERN = re.compile(r"^[A-Za-z0-9_.\-/~]+$")


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _validate_configuration(configuration: dict):
    for key in ("host", "database"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if not HOST_PATTERN.match(configuration["host"]):
        raise ValueError("host must be a host name or IP address")
    if not DATABASE_PATTERN.match(configuration["database"]):
        raise ValueError("database must be a database name or path (letters, digits, _ . - / ~)")
    if not str(configuration.get("port") or DEFAULT_PORT).isdigit():
        raise ValueError("port must be a number")
    for entry in _csv(configuration, "cursor_overrides"):
        name, sep, _ = entry.partition("=")
        if not sep or "." not in name:
            raise ValueError(
                f"Invalid cursor_overrides entry '{entry}': use schema.table=column, "
                "or schema.table= to force a full reload"
            )


# ---------------------------------------------------------------------------------------------
# JDBC access through JPype (H2)
# ---------------------------------------------------------------------------------------------


def _jar_path(configuration: dict) -> str:
    candidates = [configuration.get("jar_path"), os.environ.get("H2_JAR"), DEFAULT_JAR_PATH]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    raise ValueError(
        "H2 JDBC driver jar not found. Set jar_path in the configuration or the H2_JAR environment "
        f"variable, or place the jar at {DEFAULT_JAR_PATH}"
    )


def _start_jvm(jar: str):
    if jpype.isJVMStarted():
        return
    # convertStrings=True returns Java strings as Python str, converted in JPype's C++ layer
    jpype.startJVM(JVM_MAX_HEAP, classpath=[jar], convertStrings=True)


def _connect(configuration: dict):
    _start_jvm(_jar_path(configuration))
    DriverManager = jpype.JClass("java.sql.DriverManager")
    Properties = jpype.JClass("java.util.Properties")

    # IFEXISTS=TRUE: never create an empty database if the name is wrong (servers started with
    # -ifNotExists would otherwise create one).
    url = (
        f"jdbc:h2:tcp://{configuration['host']}:{configuration.get('port') or DEFAULT_PORT}/"
        f"{configuration['database']};IFEXISTS=TRUE"
    )
    properties = Properties()
    properties.setProperty("user", configuration.get("username") or "sa")
    properties.setProperty("password", configuration.get("password") or "")
    SQLException = jpype.JClass("java.sql.SQLException")
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            conn = DriverManager.getConnection(url, properties)
            conn.setReadOnly(True)
            return conn
        except SQLException as e:
            # JPype raises the Java exception; its message names the cause. H2 reports network
            # failures (refused, timed out, reset) as error 90067 and only those are retried; a wrong
            # database name or password fails at once.
            if e.getErrorCode() != H2_CONNECTION_BROKEN or attempt == CONNECT_ATTEMPTS:
                raise ConnectionError(f"Could not connect to H2 at {url}: {e}") from None
            log.warning(f"Connecting to H2 failed (attempt {attempt} of {CONNECT_ATTEMPTS}), retrying in {CONNECT_RETRY_SECONDS * attempt}s: {e}")
            time.sleep(CONNECT_RETRY_SECONDS * attempt)


def _bind(statement, params: list):
    for index, (value, kind) in enumerate(params, 1):
        if kind == "bytes":
            statement.setBytes(index, jpype.JArray(jpype.JByte)(bytes.fromhex(value)))
        else:
            statement.setString(index, None if value is None else str(value))


def _query(conn, sql: str, params: list = ()) -> list:
    """Small query (catalog, MAX): every value as a string."""
    statement = conn.prepareStatement(sql)
    try:
        _bind(statement, list(params))
        result = statement.executeQuery()
        count = result.getMetaData().getColumnCount()
        rows = []
        while result.next():
            rows.append(tuple(result.getString(i) for i in range(1, count + 1)))
        return rows
    finally:
        statement.close()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


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


def _int_or_none(value):
    return int(value) if value is not None else None


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

    # H2: TABLE_TYPE is 'BASE TABLE', 'VIEW', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY', 'TABLE LINK' or
    # 'EXTERNAL'. Only base tables are synced.
    catalog_tables = _query(
        conn, "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    )
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

    columns, primary_keys, indexes = {}, {}, {}
    for schema_name in sorted({s for s, _ in selected}):
        _load_schema_catalog(conn, schema_name, columns, primary_keys, indexes)

    specs, no_primary_key = [], []
    for schema_name, table_name in selected:
        key = (schema_name, table_name)
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
            "primary_key": primary_keys[key],
        }
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        table_indexes = indexes.get(key, {})
        # H2: index names for USE INDEX hints. The primary-key index gives primary-key order without a sort.
        spec["pk_index"] = table_indexes.get("primary")
        spec["cursor_index"] = table_indexes.get("leading", {}).get(spec["cursor"]["name"]) if spec["cursor"] else None
        if verbose and spec["cursor"] and not spec["cursor_index"]:
            log.info(
                f"{_table_key(spec)}: cursor column {spec['cursor']['name']} is not the leading column of "
                "any index, so each incremental sync scans the whole table. An index on it makes "
                "syncs of small changes much faster."
            )
        spec["batch_size"], spec["row_cap"], spec["fetch_size"] = _batch_limits(table_columns)
        specs.append(spec)

    if verbose and no_primary_key:
        shown = ", ".join(no_primary_key[:20]) + (f" and {len(no_primary_key) - 20} more" if len(no_primary_key) > 20 else "")
        op.warning(f"{len(no_primary_key)} tables skipped because they have no primary key: {shown}")
        log.info(f"Tables skipped for having no primary key: {', '.join(no_primary_key)}")
    return specs


def _load_schema_catalog(conn, schema_name: str, columns: dict, primary_keys: dict, indexes: dict):
    params = [(schema_name, "string")]
    rows = _query(
        conn,
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, "
        "CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION",
        params,
    )
    for table_name, column_name, data_type, precision, scale, nullable, length, datetime_precision in rows:
        columns.setdefault((schema_name, table_name), []).append(
            {
                "name": column_name,
                "type": (data_type or "").lower(),
                "precision": _int_or_none(precision),
                "scale": _int_or_none(scale),
                "length": _int_or_none(length),
                "datetime_precision": _int_or_none(datetime_precision),
                "nullable": (nullable or "YES").upper() != "NO",
            }
        )

    rows = _query(
        conn,
        "SELECT k.TABLE_NAME, k.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k "
        "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c "
        "ON c.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
        "WHERE k.TABLE_SCHEMA = ? AND c.CONSTRAINT_TYPE = 'PRIMARY KEY' "
        "ORDER BY k.TABLE_NAME, k.ORDINAL_POSITION",
        params,
    )
    for table_name, column_name in rows:
        primary_keys.setdefault((schema_name, table_name), []).append(column_name)

    # Leading column of each index, and which index enforces the primary key
    rows = _query(
        conn,
        "SELECT ic.TABLE_NAME, ic.INDEX_NAME, ic.COLUMN_NAME, i.INDEX_TYPE_NAME "
        "FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic JOIN INFORMATION_SCHEMA.INDEXES i "
        "ON i.INDEX_SCHEMA = ic.INDEX_SCHEMA AND i.INDEX_NAME = ic.INDEX_NAME "
        "WHERE ic.TABLE_SCHEMA = ? AND ic.ORDINAL_POSITION = 1 ORDER BY ic.TABLE_NAME, ic.INDEX_NAME",
        params,
    )
    for table_name, index_name, column_name, index_type in rows:
        table_indexes = indexes.setdefault((schema_name, table_name), {"leading": {}})
        if index_type == "PRIMARY KEY":
            table_indexes["primary"] = index_name
        table_indexes["leading"].setdefault(column_name, index_name)


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
    """Rows per query, per checkpoint and per fetch; smaller for wide tables and tables with LOBs."""
    if any(c["type"] in LOB_TYPES for c in columns):
        return LOB_BATCH_SIZE, LOB_CHECKPOINT_ROW_CAP, LOB_FETCH_SIZE
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(columns)))
    return (
        max(MIN_BATCH_SIZE, int(BATCH_SIZE * scale)),
        max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale)),
        FETCH_SIZE,
    )


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _declared_type(column: dict):
    """Type to declare in schema(), or None to let the SDK infer it from the delivered value.

    Only integers are left to inference (Python ints become LONG). The others are declared because
    inference gets them wrong: the SDK types an undeclared Python float as a 32-bit FLOAT (a DOUBLE
    PRECISION value 3.141592653589793 arrived as 3.1415927), and it infers the type of an undeclared
    string from its text, so a VARCHAR holding 't', '123' or '2024-01-01' would become a BOOLEAN,
    number or date column, and a NUMERIC(50,10) delivered as a string became a DOUBLE.
    """
    data_type = column["type"]
    if data_type in INTEGER_TYPES:
        return None
    if data_type in DECIMAL_TYPES:
        precision = column["precision"]
        if precision and precision <= MAX_DECIMAL_PRECISION:
            return {"type": "DECIMAL", "precision": precision, "scale": column["scale"] or 0}
        return "STRING"  # wider than the SDK's DECIMAL allows; delivered as exact text
    if data_type == "double precision":
        return "DOUBLE"
    if data_type == "real":
        return "FLOAT"
    if data_type in TIMESTAMP_TYPES:
        return "NAIVE_DATETIME"
    if data_type in TIMESTAMP_TZ_TYPES:
        return "UTC_DATETIME"
    if data_type in DATE_TYPES:
        return "NAIVE_DATE"
    if data_type in BOOLEAN_TYPES:
        return "BOOLEAN"
    if data_type in BINARY_TYPES:
        return "BINARY"
    if data_type in JSON_TYPES:
        return "JSON"
    return "STRING"  # character types, DECFLOAT, TIME, INTERVAL, UUID, ENUM, ARRAY, ROW, GEOMETRY


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


# Timestamps and dates are passed to the SDK as datetime/date objects, not ISO strings: the SDK
# re-parses strings with strptime, which took about 13% of sync time in a profile of the IRIS
# connector. H2: values are read with getString, not getTimestamp, which would shift them through
# the JVM's default time zone (and change times that fall in a daylight-saving gap). H2's text
# ('2024-01-01 12:00:00.5', '2024-06-01 12:00:00+02', nanosecond fractions) is parsed by
# datetime.fromisoformat from Python 3.11 on (the SDK runtime supports 3.11 and later); fractions
# beyond microseconds are truncated.
def _datetime_value(value: str):
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value  # outside Python's range (year > 9999 or before year 1); the SDK reports it


def _datetime_tz_value(value: str):
    try:
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    except ValueError:
        return value


def _date_value(value: str):
    try:
        return date.fromisoformat(value)
    except ValueError:
        return value


def _boolean_value(value: str):
    return value == "TRUE"


def _converter(column: dict):
    """Function turning H2's string for a value into what the SDK takes, or None to keep the string."""
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return _datetime_value
    if data_type in TIMESTAMP_TZ_TYPES:
        return _datetime_tz_value
    if data_type in DATE_TYPES:
        return _date_value
    if data_type in INTEGER_TYPES:
        return int
    if data_type in FLOAT_TYPES:
        return float  # also parses H2's 'Infinity', '-Infinity' and 'NaN'
    if data_type in BOOLEAN_TYPES:
        return _boolean_value
    if data_type in JSON_TYPES:
        return json.loads
    # DECIMAL values stay strings: H2 returns them in plain notation (BigDecimal.toPlainString)
    return None


# ---------------------------------------------------------------------------------------------
# Batched keyset reads
# ---------------------------------------------------------------------------------------------


def _type_sql(column: dict):
    """SQL type a bound string is cast to, so comparisons use the column's own type and collation."""
    data_type = column["type"]
    if data_type in DECIMAL_TYPES:
        if column["precision"]:
            return f"NUMERIC({column['precision']},{column['scale'] or 0})"
        return "NUMERIC"
    if data_type == "character" and column["length"]:
        return f"CHARACTER({column['length']})"
    if data_type in TIMESTAMP_TYPES | TIMESTAMP_TZ_TYPES and column["datetime_precision"] is not None:
        return f"{data_type.upper()}({column['datetime_precision']})"
    if data_type in ("enum", "array", "row", "geometry") or data_type in BINARY_TYPES:
        return None
    return data_type.upper()


def _param_expr(column: dict) -> str:
    type_sql = _type_sql(column)
    return f"CAST(? AS {type_sql})" if type_sql else "?"


def _param(column: dict, value):
    return (value, "bytes" if column["type"] in BINARY_TYPES else "string")


def _keyset_condition(order_columns: list, after: list, params: list) -> str:
    """Rows strictly after `after` in (order_columns) order, expanded for databases without row-value comparison.

    H2: a leading `first >= value` term is added for multi-column keys so H2 can start the index scan
    at the right place instead of reading the index from the beginning and filtering.
    """
    clauses, own_params = [], []
    for i, column in enumerate(order_columns):
        parts = []
        for j in range(i):
            parts.append(f"{_quote(order_columns[j]['name'])} = {_param_expr(order_columns[j])}")
            own_params.append(_param(order_columns[j], after[j]))
        parts.append(f"{_quote(column['name'])} > {_param_expr(column)}")
        own_params.append(_param(column, after[i]))
        clauses.append("(" + " AND ".join(parts) + ")")
    condition = "(" + " OR ".join(clauses) + ")"
    if len(order_columns) > 1:
        first = order_columns[0]
        own_params.insert(0, _param(first, after[0]))
        condition = f"({_quote(first['name'])} >= {_param_expr(first)} AND {condition})"
    # The condition is appended to the WHERE clause, so its parameters follow any already in `params`
    params.extend(own_params)
    return condition


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

    query      executeQuery(): H2 plans the batch query and produces the first rows
    fetch      reading the batch from the JDBC result set (network round trips and per-value JNI calls)
    convert    turning H2's values into SDK records
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


def _read_rows(conn, spec: dict, conditions: list, params: list, after, timer: _Timer, index: str = None):
    """Yield (record, primary-key position) in primary-key order, one bounded query per batch.

    H2: `index` names the index H2 must use (USE INDEX). The default is the primary-key index, which
    H2 reads in order and stops after LIMIT rows. Left to itself, H2 picks an index on a filtered
    column whenever one exists, however many rows match, and then sorts every match on each batch.
    """
    index = index or spec["pk_index"]
    hint = f" USE INDEX ({_quote(index)})" if index else ""
    batch_size = spec["batch_size"]
    columns = spec["columns"]
    order_columns = [_column(spec, name) for name in spec["primary_key"]]
    converters = [_converter(c) for c in columns]
    is_binary = [c["type"] in BINARY_TYPES for c in columns]
    names = [c["name"] for c in columns]
    key_positions = [names.index(c["name"]) for c in order_columns]
    key_binary = [c["type"] in BINARY_TYPES for c in order_columns]
    select_list = ", ".join(_quote(n) for n in names)
    order_by = ", ".join(_quote(c["name"]) for c in order_columns)
    count = len(columns)

    while True:
        where = list(conditions)
        query_params = list(params)
        if after is not None:
            where.append(_keyset_condition(order_columns, after, query_params))
        sql = f"SELECT {select_list} FROM {_table_ref(spec)}{hint}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by} LIMIT {batch_size}"

        if timer.queries == 0:
            log.info(f"{timer.key}: batch query: {sql}")
        started = time.perf_counter()
        statement = conn.prepareStatement(sql)
        try:
            statement.setFetchSize(spec["fetch_size"])
            _bind(statement, query_params)
            result = statement.executeQuery()
            executed = time.perf_counter()
            # H2: one getString per value (or getBytes for binary columns). getString returns the
            # SQL-standard text of every type and None for NULL, so no wasNull() calls are needed.
            get_string, get_bytes, next_row = result.getString, result.getBytes, result.next
            rows = []
            while next_row():
                row = []
                for i in range(count):
                    if is_binary[i]:
                        value = get_bytes(i + 1)
                        row.append(None if value is None else bytes(value))
                    else:
                        row.append(get_string(i + 1))
                rows.append(row)
        finally:
            statement.close()
        fetched = time.perf_counter()
        timer.add_query(executed - started)
        timer.add("fetch", fetched - executed)

        # Convert the whole batch at once so the conversion can be timed separately from the upserts.
        # Keyset values are kept as H2's own text (hex for binary), which is exact (e.g. nanosecond
        # timestamps) and JSON-serialisable for the state.
        batch = []
        for row in rows:
            record = {}
            for name, convert, value in zip(names, converters, row):
                record[name] = convert(value) if convert and value is not None else value
            position = [row[p].hex() if b else row[p] for p, b in zip(key_positions, key_binary)]
            batch.append((record, position))
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
    """One pass in primary-key order over rows with cursor >= the previous high-water mark (and NULL cursors).

    State during a pass: `cursor` is the lower bound (the previous pass's high-water mark, absent on
    the first sync), `high_water` the cursor's maximum taken when this pass started, and `after` the
    last primary key delivered. A resumed pass keeps `cursor` and `high_water` and continues after
    `after`. When the pass finishes, `high_water` becomes `cursor`.
    """
    key = _table_key(spec)
    cursor = spec["cursor"]
    quoted_cursor = _quote(cursor["name"])
    lower = table_state.get("cursor")
    after = table_state.get("after")

    if after is not None and "high_water" in table_state:
        # Log only the cursor values: primary-key values can be personal data.
        bound = f"{cursor['name']} >= {lower}" if lower is not None else "initial sync"
        log.info(f"{key}: resuming incremental sync ({bound}, high-water mark {table_state['high_water']})")
    else:
        after = None
        table_state.pop("after", None)
        sql = f"SELECT MAX({quoted_cursor}) FROM {_table_ref(spec)}"
        params = []
        if lower is not None:
            sql += f" WHERE {quoted_cursor} >= {_param_expr(cursor)}"
            params.append(_param(cursor, lower))
        started = time.perf_counter()
        table_state["high_water"] = _query(conn, sql, params)[0][0]
        log.info(
            f"{key}: incremental sync on {cursor['name']}"
            + (f" from {lower}" if lower is not None else " (initial)")
            + f"; high-water mark {table_state['high_water']} ({time.perf_counter() - started:.1f}s)"
        )

    conditions, params, index, null_pass = [], [], None, False
    if lower is not None:
        # >= (not >) so rows at the previous high-water mark are read again, including rows written
        # at that value after it was taken. Re-reads are harmless: upserts are keyed on the primary key.
        condition = f"{quoted_cursor} >= {_param_expr(cursor)}"
        params.append(_param(cursor, lower))
        if spec["cursor_index"]:
            index = _choose_index(conn, spec, condition, params, f"{cursor['name']} >= {lower}")
            # Rows with a NULL cursor are read in their own pass below, which can also use the index
            null_pass = cursor["nullable"]
        elif cursor["nullable"]:
            # Rows with a NULL cursor can never be selected by the cursor, so they are re-sent every
            # sync. Without a cursor index this pass scans the whole table anyway, so read them here.
            condition = f"({condition} OR {quoted_cursor} IS NULL)"
        conditions.append(condition)
    # On the first sync there is no condition: every row is read, including NULL cursors.

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = null_cursor = 0
    for record, position in _read_rows(conn, spec, conditions, params, after, timer, index):
        _upsert(spec, record, timer)
        if record[cursor["name"]] is None:
            null_cursor += 1
        table_state["after"] = position
        total += 1
        _log_progress(timer, total)
        checkpointer.row_done()

    table_state.pop("after", None)
    high_water = table_state.pop("high_water", None)
    if high_water is not None:
        table_state["cursor"] = high_water
    checkpointer.checkpoint()
    detail = f" ({null_cursor:,} with NULL {cursor['name']}, re-sent every sync)" if null_cursor else ""
    log.info(f"{key}: {total:,} rows synced{detail}; next sync reads {cursor['name']} >= {table_state.get('cursor')}")
    timer.report_total("table total")

    if null_pass:
        _sync_null_cursor_rows(conn, spec, state)


def _choose_index(conn, spec: dict, condition: str, params: list, label: str) -> str:
    """Index for a filtered read: the cursor index if the matching rows fit in one batch, else the primary key.

    H2: through the cursor index H2 finds the matching rows quickly but must sort them by primary key
    on every batch, which is cheap for a few rows and very slow for millions. Through the primary-key
    index it reads in order and never sorts, at the cost of scanning the table.
    """
    started = time.perf_counter()
    matching = int(_query(conn, f"SELECT COUNT(*) FROM {_table_ref(spec)} USE INDEX ({_quote(spec['cursor_index'])}) WHERE {condition}", params)[0][0])
    use_cursor_index = matching <= spec["batch_size"]
    log.info(
        f"{_table_key(spec)}: {matching:,} rows with {label} ({time.perf_counter() - started:.1f}s); reading them "
        + (f"through index {spec['cursor_index']}" if use_cursor_index else "in primary-key order")
    )
    return spec["cursor_index"] if use_cursor_index else spec["pk_index"]


def _sync_null_cursor_rows(conn, spec: dict, state: dict):
    """Re-send rows whose cursor is NULL. The cursor can never select them, so read them every sync.

    Not resumable within the pass: an interrupted pass is simply repeated by the next sync.
    """
    key = _table_key(spec)
    cursor = spec["cursor"]
    condition = f"{_quote(cursor['name'])} IS NULL"
    index = _choose_index(conn, spec, condition, [], f"NULL {cursor['name']}")
    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, _ in _read_rows(conn, spec, [condition], [], None, timer, index):
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
    for record, position in _read_rows(conn, spec, [], [], after, timer):
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
