"""Fivetran Connector SDK connector for Mimer SQL.

Discovers tables, columns and primary keys from INFORMATION_SCHEMA and picks a strategy per table:

- Incremental: the table has a primary key and a usable cursor column (configurable, default
  `updated_at`). Reads `cursor >= last value` ordered by (cursor, primary key) and resumes after the
  last (cursor, primary key) delivered. Rows whose cursor is NULL are re-read every sync, so they
  are never lost.
- Full reload: tables with a primary key but no usable cursor. Deferred truncate, then upsert every
  row in primary-key order, so a failed sync deletes nothing.

Tables without a primary key are skipped (with a dashboard warning listing them). Without a key,
Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses rows that
are exact duplicates.

Reads use one server-side cursor per table pass (Mimer holds the result set; the client holds one
batch), in a read-only transaction that is committed at the end of the pass. Ported from the IRIS
connector in this repo; Mimer-specific parts are marked "Mimer:".

Mimer: the client is mimerpy, a ctypes wrapper around the native libmimerapi.so. The library is
found through MIMERAPI_PATH, a libmimerapi.so next to this file, or the system loader path. The
client resolves database names through an SQLHOSTS file; this connector writes one from the host,
port and database settings.
"""

import ctypes
import gc
import os
import re
import socket
import tempfile
import time
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from uuid import UUID

from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Rows per fetchmany() call for a table of up to WIDE_TABLE_COLUMNS columns (scaled down for wider
# tables). This bounds the rows held in Python at once; the server keeps the rest of the result set.
BATCH_SIZE = 10_000
# Mimer: rows per network round trip (MimerSetArraySize). mimerpy leaves it at 1, which made
# fetching about 5x slower (11k rows/s against 63k rows/s for the seeded orders table).
ARRAY_SIZE = 1_000
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_BATCH_SIZE = 500
MIN_CHECKPOINT_ROW_CAP = 10_000
# Tables with LOB columns (CLOB / NCLOB / BLOB) can have very large rows
LOB_BATCH_SIZE = 100
LOB_ARRAY_SIZE = 100
LOB_CHECKPOINT_ROW_CAP = 10_000
PROGRESS_LOG_EVERY = 250_000
CONNECT_TIMEOUT_SECONDS = 30

DEFAULT_PORT = 1360
DEFAULT_DATABASE = "mimerdb"
DEFAULT_CURSOR_COLUMNS = "updated_at"

# Mimer: SYSTEM holds the data dictionary, MIMER and ODBC hold views created for the drivers, and
# BUILTIN holds the built-in types and routines. A schema named explicitly in the `schemas` setting
# is synced even if listed here.
SYSTEM_SCHEMAS = {"system", "information_schema", "mimer", "odbc", "builtin"}
SYSTEM_SCHEMA_PREFIXES = ()

# Mimer: INFORMATION_SCHEMA.COLUMNS.DATA_TYPE values (compared lower-cased). User-defined types are
# recorded as "<schema>.<name>", e.g. builtin.uuid.
TIMESTAMP_TYPES = {"timestamp"}
DATE_TYPES = {"date"}
TIME_TYPES = {"time"}
INTEGER_TYPES = {"tinyint", "smallint", "integer", "bigint"}
DECIMAL_TYPES = {"decimal", "numeric"}
# Mimer: FLOAT(p) is a decimal floating-point type that mimerpy returns as Decimal
DECIMAL_FLOAT_TYPES = {"float"}
FLOAT_TYPES = {"real", "double precision"}
BOOLEAN_TYPES = {"boolean"}
BINARY_TYPES = {"binary", "binary varying", "binary large object"}
LOB_TYPES = {"character large object", "national char large object", "binary large object"}
UUID_TYPES = {"builtin.uuid"}
CURSOR_TYPES = TIMESTAMP_TYPES | DATE_TYPES | INTEGER_TYPES
MAX_DECIMAL_PRECISION = 38  # the SDK's DECIMAL limit; wider values (Mimer allows 45) are delivered as strings
MAX_FLOAT_DIGITS = 15  # FLOAT(p) up to this precision fits a double exactly; wider is delivered as a string
PYTHON_DATETIME_PRECISION = 6  # datetime and time hold microseconds; Mimer allows up to 9 digits


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


# Mimer: values written into the SQLHOSTS file, so they must be single tokens
_SQLHOSTS_TOKEN = re.compile(r"^[A-Za-z0-9_.:\-]+$")


def _validate_configuration(configuration: dict):
    for key in ("host", "password"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if not str(configuration.get("port") or DEFAULT_PORT).isdigit():
        raise ValueError("port must be a number")
    for key in ("host", "database"):
        value = configuration.get(key) or (DEFAULT_DATABASE if key == "database" else "")
        if not _SQLHOSTS_TOKEN.match(value):
            raise ValueError(f"{key} may contain only letters, digits and . _ - :")
    for entry in _csv(configuration, "cursor_overrides"):
        name, sep, _ = entry.partition("=")
        if not sep or "." not in name:
            raise ValueError(
                f"Invalid cursor_overrides entry '{entry}': use schema.table=column, "
                "or schema.table= to force a full reload"
            )


# ---------------------------------------------------------------------------------------------
# Mimer: client library, SQLHOSTS and connection
# ---------------------------------------------------------------------------------------------

_MIMER = {"module": None, "set_array_size": None, "array_size_warned": False, "sqlhosts_dir": None}


def _mimerpy():
    """Import mimerpy, pointing it at a libmimerapi.so shipped next to this file if there is one.

    mimerpy loads the native library when it is imported, so MIMERAPI_PATH must be set first.
    """
    if _MIMER["module"] is None:
        bundled = os.path.join(os.path.dirname(os.path.abspath(__file__)), "libmimerapi.so")
        if not os.environ.get("MIMERAPI_PATH") and os.path.exists(bundled):
            os.environ["MIMERAPI_PATH"] = bundled
        try:
            import mimerpy
        except (ImportError, OSError) as exc:
            raise RuntimeError(
                "Could not load the Mimer SQL client library (libmimerapi.so). Set MIMERAPI_PATH, "
                "put libmimerapi.so next to connector.py, or install it on the loader path."
            ) from exc
        _MIMER["module"] = mimerpy
    return _MIMER["module"]


def _write_sqlhosts(configuration: dict) -> str:
    """Write an SQLHOSTS file mapping the database name to host:port and return the name.

    Mimer: the client takes a database name, not host:port, and looks it up in the file named by
    MIMER_SQLHOSTS. The name must be the server's database name. The file is re-read for each new
    session, so rewriting it is safe.
    """
    database = configuration.get("database") or DEFAULT_DATABASE
    if _MIMER["sqlhosts_dir"] is None:
        _MIMER["sqlhosts_dir"] = tempfile.mkdtemp(prefix="mimer_sqlhosts_")
    path = os.path.join(_MIMER["sqlhosts_dir"], "sqlhosts")
    port = int(configuration.get("port") or DEFAULT_PORT)
    with open(path, "w") as handle:
        handle.write(f"REMOTE:\n   {database}    {configuration['host']}    ''    ''    {port}\n")
    os.environ["MIMER_SQLHOSTS"] = path
    return database


def _connect(configuration: dict):
    mimerpy = _mimerpy()
    host, port = configuration["host"], int(configuration.get("port") or DEFAULT_PORT)
    # Fail fast with a clear message: the Mimer client waits for the OS connect timeout (about two
    # minutes) on an unreachable host and then reports a generic error.
    try:
        socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_SECONDS).close()
    except OSError as exc:
        raise RuntimeError(f"Cannot reach Mimer SQL at {host}:{port}: {exc}") from exc
    database = _write_sqlhosts(configuration)
    # Read-only transactions never conflict with writers and see a consistent snapshot. mimerpy
    # starts one at the first statement; the connector commits after each table pass to end it.
    return mimerpy.connect(
        dsn=database,
        user=configuration.get("username") or "SYSADM",
        password=configuration["password"],
        readonly=True,
    )


def _set_array_size(cursor, rows: int):
    """Fetch `rows` rows per network round trip instead of one.

    Mimer: mimerpy does not expose MimerSetArraySize, so this calls the C API on the cursor's
    statement handle (a private attribute; requirements.txt pins the mimerpy version it was tested
    with). If that fails, the sync still works, only slower.
    """
    try:
        if _MIMER["set_array_size"] is None:
            from mimerpy import mimerapi

            function = mimerapi._lib.MimerSetArraySize
            function.argtypes = [mimerapi.MimerStatement, ctypes.c_int32]
            function.restype = ctypes.c_int32
            _MIMER["set_array_size"] = (function, mimerapi.MimerStatement)
        function, statement_type = _MIMER["set_array_size"]
        rc = function(statement_type(cursor._Cursor__statement), rows)
        if rc != 0:
            raise RuntimeError(f"MimerSetArraySize returned {rc}")
    except Exception as exc:  # any failure here only costs speed
        if not _MIMER["array_size_warned"]:
            _MIMER["array_size_warned"] = True
            op.warning(f"Could not set the Mimer fetch array size ({exc}); rows are fetched one per round trip")


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


def _query(conn, sql: str, params=()):
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, list(params))
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


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

    # Mimer: identifiers keep the case they were created with but compare case-insensitively (even
    # when quoted), and so do string comparisons on the catalog views.
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

    columns, primary_keys, indexed = {}, {}, set()
    for schema_name in sorted({s for s, _ in selected}):
        _load_schema_catalog(conn, schema_name, columns, primary_keys, indexed)

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
            "primary_key": primary_keys.get(key, []),
        }
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        if verbose and spec["cursor"] and (schema_name, table_name, spec["cursor"]["name"].lower()) not in indexed:
            log.warning(
                f"{_table_key(spec)}: cursor column {spec['cursor']['name']} is not the leading column "
                "of any index, so every sync scans and sorts the rows it reads. An index on it makes syncs faster."
            )
        spec["batch_size"], spec["array_size"], spec["row_cap"] = _batch_limits(table_columns)
        specs.append(spec)

    if verbose and no_primary_key:
        shown = ", ".join(no_primary_key[:20]) + (f" and {len(no_primary_key) - 20} more" if len(no_primary_key) > 20 else "")
        op.warning(f"{len(no_primary_key)} tables skipped because they have no primary key: {shown}")
        log.info(f"Tables skipped for having no primary key: {', '.join(no_primary_key)}")
    return specs


def _load_schema_catalog(conn, schema_name: str, columns: dict, primary_keys: dict, indexed: set):
    params = [schema_name]
    rows = _query(
        conn,
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, "
        "DATETIME_PRECISION, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION",
        params,
    )
    for table_name, column_name, data_type, precision, scale, nullable, dt_precision, udt_schema, udt_name in rows:
        data_type = (data_type or "").lower()
        if data_type == "user-defined" and udt_name:
            data_type = f"{udt_schema or ''}.{udt_name}".lower()
        columns.setdefault((schema_name, table_name), []).append(
            {
                "name": column_name,
                "type": data_type,
                "precision": precision,
                "scale": scale,
                "datetime_precision": dt_precision,
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

    # Mimer: EXT_ACCESS_PATHS lists the columns of primary keys, unique constraints and indexes. An
    # index always lives in the schema of its table.
    rows = _query(
        conn,
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.EXT_ACCESS_PATHS "
        "WHERE INDEX_SCHEMA = ? AND ORDINAL_POSITION = 1",
        params,
    )
    for table_name, column_name in rows:
        indexed.add((schema_name, table_name, column_name.lower()))


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
    """Rows per fetch, per network round trip and per checkpoint; smaller for wide and LOB tables."""
    if any(c["type"] in LOB_TYPES for c in columns):
        return LOB_BATCH_SIZE, LOB_ARRAY_SIZE, LOB_CHECKPOINT_ROW_CAP
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(columns)))
    batch_size = max(MIN_BATCH_SIZE, int(BATCH_SIZE * scale))
    return (
        batch_size,
        min(ARRAY_SIZE, batch_size),
        max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale)),
    )


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _declared_type(column: dict):
    """Type to declare in schema(), only where the SDK cannot infer it from the delivered value."""
    data_type = column["type"]
    if data_type in DECIMAL_TYPES:
        precision = column["precision"]
        if precision and precision <= MAX_DECIMAL_PRECISION:
            return {"type": "DECIMAL", "precision": int(precision), "scale": int(column["scale"] or 0)}
        # Delivered as a string. Declared, because the SDK infers numeric-looking strings as DOUBLE
        # (DECIMAL(45,5) arrived as 1.2345678901234568e+39 when left to inference).
        return "STRING"
    if data_type in DECIMAL_FLOAT_TYPES:
        precision = column.get("precision") or 0
        return "DOUBLE" if 0 < precision <= MAX_FLOAT_DIGITS else "STRING"
    if data_type == "double precision":
        # Declared, because the SDK infers a Python float as FLOAT (32-bit): 1.5e300 arrived as inf
        return "DOUBLE"
    if data_type in TIMESTAMP_TYPES:
        return "NAIVE_DATETIME"  # Mimer: TIMESTAMP has no time zone (there is no WITH TIME ZONE type)
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


def _other_value(value):
    """Values of types the SDK does not take (UUIDs, GIS and other user-defined types) as strings."""
    if isinstance(value, (str, int, float, bool, bytes)):
        return value
    return str(value)


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
    if data_type in DECIMAL_FLOAT_TYPES:
        precision = column.get("precision") or 0
        return float if 0 < precision <= MAX_FLOAT_DIGITS else _decimal_value
    if data_type in BOOLEAN_TYPES:
        return bool  # Mimer: mimerpy returns BOOLEAN as 0/1
    if data_type in BINARY_TYPES:
        return _binary_value
    if data_type in INTEGER_TYPES or data_type in FLOAT_TYPES:
        return None
    if "." in data_type:
        return _other_value  # user-defined types, e.g. builtin.uuid
    return None


def _key_value(value):
    """JSON-serialisable keyset value, stored in state and bound back into the next query."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, (date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value


# ---------------------------------------------------------------------------------------------
# Keyset reads through a server-side cursor
# ---------------------------------------------------------------------------------------------


def _needs_exact_text(column: dict) -> bool:
    """True for keyset columns whose values Python cannot hold exactly.

    Mimer: TIMESTAMP(7..9) and TIME(7..9) keep nanoseconds, but mimerpy returns datetime/time with
    microseconds, so a position built from the delivered value could skip rows on resume. Those
    keyset values are read as text instead.
    """
    return (
        column["type"] in TIMESTAMP_TYPES | TIME_TYPES
        and (column.get("datetime_precision") or 0) > PYTHON_DATETIME_PRECISION
    )


def _exact_text_value(value: str) -> str:
    # Mimer: CAST(timestamp AS VARCHAR) gives the literal form, e.g. TIMESTAMP'2024-01-01 00:00:00.123456100'
    match = re.search(r"'(.*)'", value or "")
    return match.group(1) if match else value


def _param_expr(column: dict) -> str:
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES:
        return f"CAST(? AS TIMESTAMP({int(column.get('datetime_precision') or 6)}))"
    if data_type in DATE_TYPES:
        return "CAST(? AS DATE)"
    if data_type in TIME_TYPES:
        return f"CAST(? AS TIME({int(column.get('datetime_precision') or 0)}))"
    if data_type in UUID_TYPES:
        return "BUILTIN.UUID_FROM_TEXT(?)"
    return "?"


def _bind_value(column: dict, value):
    if column["type"] in BINARY_TYPES and isinstance(value, str):
        return bytes.fromhex(value)
    return value


def _keyset_condition(order_columns: list, after: list, params: list) -> str:
    """Rows strictly after `after` in (order_columns) order, expanded as OR/AND comparisons.

    Mimer supports row-value comparison too; the expanded form is kept from the template.
    """
    clauses = []
    for i, column in enumerate(order_columns):
        parts = []
        for j in range(i):
            parts.append(f"{_quote(order_columns[j]['name'])} = {_param_expr(order_columns[j])}")
            params.append(_bind_value(order_columns[j], after[j]))
        parts.append(f"{_quote(column['name'])} > {_param_expr(column)}")
        params.append(_bind_value(column, after[i]))
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

    query      execute() and the first fetch: the database runs the query (for an ORDER BY over an
               unindexed column this includes scanning and sorting the rows) until the first rows arrive
    fetch      fetchmany(): transferring and decoding rows in the driver
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


def _read_rows(conn, spec: dict, order_columns: list, conditions: list, params: list, after, timer: _Timer):
    """Yield (record, keyset position) in order_columns order from one server-side cursor.

    Mimer: mimerpy's default cursor is a forward-only server cursor (MimerOpenCursor / MimerFetch),
    so the database sorts once and the client holds one fetchmany() batch. Measured on the seeded
    orders table: flat ~50 MB client memory over 1M rows, against a re-sort of the remaining rows for
    every batch with FETCH FIRST n queries.
    """
    batch_size = spec["batch_size"]
    columns = spec["columns"]
    converters = [_converter(c) for c in columns]
    select_list = [_quote(c["name"]) for c in columns]
    names = [c["name"] for c in columns]
    # Where each keyset value sits in the row: the delivered column, or an extra exact-text column
    key_positions, exact = [], []
    for column in order_columns:
        if _needs_exact_text(column):
            key_positions.append(len(select_list))
            exact.append(True)
            select_list.append(f"CAST({_quote(column['name'])} AS VARCHAR(48))")
        else:
            key_positions.append(names.index(column["name"]))
            exact.append(False)
    order_by = ", ".join(_quote(c["name"]) for c in order_columns)

    where = list(conditions)
    query_params = list(params)
    if after is not None:
        where.append(_keyset_condition(order_columns, after, query_params))
    sql = f"SELECT {', '.join(select_list)} FROM {_table_ref(spec)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {order_by}"
    log.info(f"{timer.key}: query: {sql}")

    cursor = conn.cursor()
    try:
        started = time.perf_counter()
        if query_params:
            cursor.execute(sql, query_params)
        else:
            cursor.execute(sql)
        _set_array_size(cursor, spec["array_size"])
        # The first fetch waits for the database to produce the first row (the sort, if any)
        rows = cursor.fetchmany(1)
        timer.add_query(time.perf_counter() - started)
        while rows:
            fetched = time.perf_counter()
            batch = []
            for row in rows:
                record = {}
                for column, convert, value in zip(columns, converters, row):
                    record[column["name"]] = convert(value) if convert and value is not None else value
                position = [
                    _exact_text_value(row[i]) if is_exact else _key_value(row[i])
                    for i, is_exact in zip(key_positions, exact)
                ]
                batch.append((record, position))
            del rows
            timer.add("convert", time.perf_counter() - fetched)
            yield from batch
            started = time.perf_counter()
            rows = cursor.fetchmany(batch_size)
            timer.add("fetch", time.perf_counter() - started)
    finally:
        cursor.close()
    # End the read-only transaction so the server can release the snapshot
    conn.commit()


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
    conditions, params = [], []
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
        conditions.append(f"{_quote(cursor['name'])} >= {_param_expr(cursor)}")
        params.append(table_state["cursor"])
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
    for record, _ in _read_rows(conn, spec, order_columns, conditions, [], None, timer):
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
    for record, position in _read_rows(conn, spec, order_columns, [], [], after, timer):
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
        conn.commit()
    finally:
        conn.close()
    return [_table_definition(spec) for spec in specs]


def update(configuration: dict, state: dict):
    _validate_configuration(configuration)
    tables_state = state.setdefault("tables", {})
    conn = _connect(configuration)
    try:
        specs = _discover(conn, configuration)
        conn.commit()
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
