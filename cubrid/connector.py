"""Fivetran Connector SDK connector for CUBRID (11.x; owner-qualified table names from 11.2).

Discovers tables, columns and primary keys from the CUBRID catalog (db_class, db_attribute, db_index,
db_index_key) and picks a strategy per table:

- Incremental: the table has a primary key and a usable cursor column (configurable, default
  `updated_at`).
  - The first sync reads the whole table in primary-key order and then stores the cursor's maximum,
    captured before the read started, as the cursor value. CUBRID's default sort buffer is 2 MB, and
    ordering a 10M-row table by an unindexed cursor column produced no first row after 25 minutes,
    while the primary-key index returns rows in key order without a sort.
  - Later syncs read `cursor >= last value` ordered by (cursor, primary key): one query whose sort
    covers only the rows changed since the last sync. Rows whose cursor is NULL are re-read every
    sync, so they are never lost.
- Full reload: tables with a primary key but no usable cursor. Deferred truncate, then upsert every
  row in primary-key order, so a failed sync deletes nothing.

Rows are streamed through the JDBC driver's fetch size (the server holds the result; the client holds
one fetch). Reads in primary-key order are also split into LIMIT chunks, which the primary-key index
serves cheaply, so no single query materializes the whole table on the server.

Tables without a primary key are skipped (with a dashboard warning listing them). Without a key,
Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses rows that
are exact duplicates.

The connector talks to the CUBRID JDBC driver through JPype rather than jaydebeapi: jaydebeapi's
converters corrupt values (timestamps with milliseconds below 100 are shifted, NUMERIC values with a
scale become floats). It needs a Java runtime and the CUBRID JDBC jar.

Ported from the IRIS connector in this repo; database-specific parts are marked "CUBRID:".
"""

import gc
import json
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal

import jpype
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Rows per JDBC fetch (one network round trip) for a table of up to WIDE_TABLE_COLUMNS columns, scaled
# down for wider tables. The client holds one fetch; streaming 2M rows of orders stayed at 287 MB RSS.
FETCH_SIZE = 10_000
# Rows per query for reads in primary-key order. The CUBRID server materializes a query's whole result
# before returning the first row (13 s for 10M rows), so chunks keep each query and its temp space
# bounded. Chunks continue after the last key delivered.
CHUNK_ROWS = 500_000
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_FETCH_SIZE = 500
MIN_CHECKPOINT_ROW_CAP = 10_000
# Tables with LOB columns (BLOB / CLOB) can have very large rows
LOB_FETCH_SIZE = 100
LOB_CHECKPOINT_ROW_CAP = 10_000
# Rows read from the driver before converting them, so the stages can be timed separately
READ_GROUP = 1_000
PROGRESS_LOG_EVERY = 250_000

DEFAULT_CURSOR_COLUMNS = "updated_at"
DEFAULT_PORT = 33000
DEFAULT_JDBC_JAR = "/opt/sdk-test-dbs/jars/cubrid_jdbc.jar"
JDBC_DRIVER_CLASS = "cubrid.jdbc.driver.CUBRIDDriver"
JVM_MAX_HEAP = "-Xmx512m"

# CUBRID: system catalog tables are flagged is_system_class = 'YES' and are excluded by the catalog
# query, so no schema (owner) is excluded by name. A schema named in the `schemas` setting is synced.
SYSTEM_SCHEMAS = set()
SYSTEM_SCHEMA_PREFIXES = ()

# CUBRID: db_attribute.data_type values (compared lower-cased)
TIMESTAMP_TYPES = {"timestamp", "datetime"}
TIMESTAMP_TZ_TYPES = {"timestamptz", "timestampltz"}
DATETIME_TZ_TYPES = {"datetimetz", "datetimeltz"}
ZONED_TYPES = TIMESTAMP_TZ_TYPES | DATETIME_TZ_TYPES
DATE_TYPES = {"date"}
TIME_TYPES = {"time"}
INTEGER_TYPES = {"short", "smallint", "integer", "int", "bigint"}
DECIMAL_TYPES = {"numeric", "decimal"}
FLOAT_TYPES = {"float", "real"}
DOUBLE_TYPES = {"double", "monetary"}
BIT_TYPES = {"bit", "varbit"}
BLOB_TYPES = {"blob"}
BINARY_TYPES = BIT_TYPES | BLOB_TYPES
COLLECTION_TYPES = {"set", "multiset", "sequence"}
LOB_TYPES = {"blob", "clob"}
CURSOR_TYPES = TIMESTAMP_TYPES | ZONED_TYPES | DATE_TYPES | INTEGER_TYPES
STRING_TYPES = {"char", "varchar", "string", "nchar", "nchar varying"}
# Columns that cannot be compared or bound back into a keyset condition. CUBRID: ENUM sorts by its
# declaration index but compares with strings as text, so a keyset on it would skip or repeat rows.
UNKEYABLE_TYPES = LOB_TYPES | COLLECTION_TYPES | {"json", "object", "enum"}
# CUBRID: column charsets (db_attribute.charset) and the Python codecs that give their bytes back
CHARSET_CODECS = {"utf8": "utf-8", "iso88591": "latin-1", "euckr": "euc-kr"}
MAX_DECIMAL_PRECISION = 38  # the SDK's DECIMAL limit; wider values are delivered as strings
# CUBRID: TO_CHAR with an offset is the only lossless way to read zoned values over JDBC. getTimestamp
# drops the zone (12:00 +05:30 came back as 12:00 UTC) and getString appends zone names.
TIMESTAMP_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SS TZH:TZM"
DATETIME_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM"


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _validate_configuration(configuration: dict):
    for key in ("host", "database", "username"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
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
# Connection (CUBRID: JDBC through JPype)
# ---------------------------------------------------------------------------------------------


def _jdbc_jar(configuration: dict) -> str:
    jar = configuration.get("jdbc_jar") or os.environ.get("CUBRID_JDBC_JAR") or DEFAULT_JDBC_JAR
    if not os.path.isfile(jar):
        raise ValueError(f"CUBRID JDBC jar not found at {jar}; set jdbc_jar in the configuration")
    return jar


def _start_jvm(jar: str):
    if jpype.isJVMStarted():
        return  # schema() and update() can run in one process; a JVM cannot be restarted
    # convertStrings: java.lang.String values arrive as Python str (NULL as None)
    jpype.startJVM(JVM_MAX_HEAP, classpath=[jar], convertStrings=True)


def _connect(configuration: dict):
    _start_jvm(_jdbc_jar(configuration))
    jpype.JClass(JDBC_DRIVER_CLASS)  # registers the driver with DriverManager
    driver_manager = jpype.JClass("java.sql.DriverManager")
    properties = jpype.JClass("java.util.Properties")()
    # User and password go in properties, not the URL, whose fields are ':'-separated
    properties.setProperty("user", configuration["username"])
    properties.setProperty("password", configuration.get("password") or "")
    # CUBRID: zero dates ('0000-00-00') otherwise make the driver throw while fetching the whole batch
    url = (
        f"jdbc:cubrid:{configuration['host']}:{int(configuration.get('port') or DEFAULT_PORT)}:"
        f"{configuration['database']}:::?zeroDateTimeBehavior=convertToNull"
    )
    try:
        conn = driver_manager.getConnection(url, properties)
    except jpype.JException as exc:
        raise RuntimeError(f"Could not connect to CUBRID at {configuration['host']}: {exc.getMessage()}") from None
    conn.setReadOnly(True)
    return conn


def _query(conn, sql: str, params: list = ()) -> list:
    """Run a small query and return all rows as tuples of strings (NULL as None)."""
    statement = conn.prepareStatement(sql)
    try:
        for index, value in enumerate(params, 1):
            statement.setString(index, value)
        result = statement.executeQuery()
        count = result.getMetaData().getColumnCount()
        rows = []
        while result.next():
            rows.append(tuple(result.getString(i) for i in range(1, count + 1)))
        result.close()
        return rows
    finally:
        statement.close()


def _server_version(conn) -> tuple:
    version = _query(conn, "SELECT VERSION()")[0][0]
    return tuple(int(part) for part in version.split(".")[:2])


def _quote(identifier: str) -> str:
    # CUBRID: double quotes delimit identifiers only when ansi_quotes=yes; brackets always do
    return "[" + identifier.replace("]", "]]") + "]"


def _table_ref(spec: dict) -> str:
    if spec["qualified"]:
        return f"{_quote(spec['schema'])}.{_quote(spec['table'])}"
    return _quote(spec["table"])  # CUBRID before 11.2: table names are global


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

    qualified = _server_version(conn) >= (11, 2)
    # CUBRID: class_type 'CLASS' is a table ('VCLASS' is a view). Partitions are classes of their own,
    # listed in db_partition; they are read through their parent table.
    catalog_tables = _query(
        conn,
        "SELECT owner_name, class_name FROM db_class "
        "WHERE class_type = 'CLASS' AND is_system_class = 'NO' "
        "AND class_name NOT IN (SELECT partition_class_name FROM db_partition)",
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
            "qualified": qualified,
            "columns": table_columns,
            "primary_key": primary_keys[key],
        }
        unkeyable = [c["name"] for c in table_columns if c["name"] in spec["primary_key"] and c["type"] in UNKEYABLE_TYPES]
        if unkeyable:
            if verbose:
                op.warning(f"{_table_key(spec)}: primary key column(s) {', '.join(unkeyable)} cannot be used for paging; table skipped")
            continue
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        if verbose and spec["cursor"] and (schema_name, table_name, spec["cursor"]["name"]) not in indexed:
            log.warning(
                f"{_table_key(spec)}: cursor column {spec['cursor']['name']} is not the leading column "
                "of any index, so every incremental sync scans the table. An index on it makes syncs much faster."
            )
        spec["fetch_size"], spec["row_cap"] = _batch_limits(table_columns)
        specs.append(spec)

    if verbose and no_primary_key:
        shown = ", ".join(no_primary_key[:20]) + (f" and {len(no_primary_key) - 20} more" if len(no_primary_key) > 20 else "")
        op.warning(f"{len(no_primary_key)} tables skipped because they have no primary key: {shown}")
        log.info(f"Tables skipped for having no primary key: {', '.join(no_primary_key)}")
    return specs


def _load_schema_catalog(conn, schema_name: str, columns: dict, primary_keys: dict, indexed: set):
    params = [schema_name]
    # CUBRID: attr_type 'INSTANCE' are row columns ('CLASS' / 'SHARED' attributes are not per row)
    rows = _query(
        conn,
        "SELECT class_name, attr_name, data_type, prec, scale, is_nullable, charset FROM db_attribute "
        "WHERE owner_name = ? AND attr_type = 'INSTANCE' ORDER BY class_name, def_order",
        params,
    )
    for table_name, column_name, data_type, precision, scale, nullable, charset in rows:
        columns.setdefault((schema_name, table_name), []).append(
            {
                "name": column_name,
                "type": (data_type or "").lower(),
                "precision": int(precision) if precision is not None else None,
                "scale": int(scale) if scale is not None else None,
                "nullable": (nullable or "YES").upper() != "NO",
                "charset": (charset or "").lower(),
            }
        )

    rows = _query(
        conn,
        "SELECT k.class_name, k.key_attr_name FROM db_index i JOIN db_index_key k "
        "ON k.index_name = i.index_name AND k.class_name = i.class_name AND k.owner_name = i.owner_name "
        "WHERE i.owner_name = ? AND i.is_primary_key = 'YES' ORDER BY k.class_name, k.key_order",
        params,
    )
    for table_name, column_name in rows:
        primary_keys.setdefault((schema_name, table_name), []).append(column_name)

    rows = _query(
        conn,
        "SELECT class_name, key_attr_name FROM db_index_key WHERE owner_name = ? AND key_order = 0",
        params,
    )
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
                "a cursor must be a timestamp, datetime, date or integer column"
            )
        return column

    for name in candidates:
        column = by_name.get(name)
        if column and column["type"] in CURSOR_TYPES:
            return column
    return None


def _batch_limits(columns: list):
    """Rows per fetch and per checkpoint, smaller for wide tables and tables with LOB columns."""
    if any(c["type"] in LOB_TYPES for c in columns):
        return LOB_FETCH_SIZE, LOB_CHECKPOINT_ROW_CAP
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(columns)))
    return (
        max(MIN_FETCH_SIZE, int(FETCH_SIZE * scale)),
        max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale)),
    )


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _is_binary_string(column: dict) -> bool:
    """CHAR / VARCHAR with the binary charset: raw bytes, which getString returns as spaced hex."""
    return column["type"] in STRING_TYPES and column.get("charset") == "binary"


def _declared_type(column: dict):
    """Type to declare in schema(), only where the SDK cannot infer it from the delivered value."""
    data_type = column["type"]
    if _is_binary_string(column):
        return "BINARY"
    if data_type in DECIMAL_TYPES:
        precision = column["precision"]
        if precision and precision <= MAX_DECIMAL_PRECISION:
            return {"type": "DECIMAL", "precision": int(precision), "scale": int(column["scale"] or 0)}
        return None  # delivered as a string
    if data_type in TIMESTAMP_TYPES:
        return "NAIVE_DATETIME"  # CUBRID: TIMESTAMP / DATETIME carry no time zone
    if data_type in ZONED_TYPES:
        return "UTC_DATETIME"
    if data_type in DATE_TYPES:
        return "NAIVE_DATE"
    if data_type in FLOAT_TYPES:
        return "FLOAT"
    if data_type in DOUBLE_TYPES:
        return "DOUBLE"
    if data_type in BINARY_TYPES:
        return "BINARY"
    if data_type in COLLECTION_TYPES:
        return "JSON"
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


# Raw values come from getString (str) for most types, so the converters parse strings. Timestamps and
# dates are passed to the SDK as datetime/date objects, not ISO strings: the SDK re-parses strings with
# strptime, which took about 13% of sync time in a profile of 200k rows.
def _datetime_value(value: str):
    return datetime.fromisoformat(value)


def _zoned_value(value: str):
    # TO_CHAR output 'YYYY-MM-DD HH:MM:SS[.fff] +HH:MM': drop the space before the offset
    return datetime.fromisoformat(value[:-7] + value[-6:]).astimezone(timezone.utc)


def _date_value(value: str):
    return date.fromisoformat(value)


def _decimal_value(value: str):
    # CUBRID's getString prints NUMERIC in plain notation; normalise anything else (e.g. '1E-10')
    return format(Decimal(value), "f") if "E" in value or "e" in value else value


def _collection_value(value):
    """SET / MULTISET / SEQUENCE arrive as Java arrays; deliver a JSON list."""
    items = []
    for item in value:
        if item is None or isinstance(item, (str, bool)):
            items.append(item)
        elif isinstance(item, int):  # boxed java.lang.Integer / Long / Short
            items.append(int(item))
        elif isinstance(item, float):
            items.append(float(item))
        elif hasattr(item, "toPlainString"):  # java.math.BigDecimal, kept exact as text
            items.append(str(item.toPlainString()))
        else:  # dates, timestamps, nested values
            items.append(str(item))
    return items


def _converter(column: dict):
    """Function turning a raw driver value into one the SDK accepts, or None to pass it through."""
    data_type = column["type"]
    if _is_binary_string(column):
        return bytes.fromhex  # getString gives 'C3 BC 41 '; fromhex ignores the spaces
    if data_type in TIMESTAMP_TYPES:
        return _datetime_value
    if data_type in ZONED_TYPES:
        return _zoned_value
    if data_type in DATE_TYPES:
        return _date_value
    if data_type in INTEGER_TYPES:
        return int
    if data_type in DECIMAL_TYPES:
        return _decimal_value
    if data_type in FLOAT_TYPES | DOUBLE_TYPES:
        return float
    if data_type in BINARY_TYPES:
        return bytes
    if data_type in COLLECTION_TYPES:
        return _collection_value
    return None  # strings (CHAR, VARCHAR, ENUM, JSON, CLOB) and TIME ('HH:MM:SS')


def _getter(column: dict) -> str:
    """ResultSet method that reads this column's raw value."""
    data_type = column["type"]
    if data_type in BINARY_TYPES:
        return "getBytes"  # BIT / BIT VARYING as byte[], BLOB contents
    if data_type in COLLECTION_TYPES:
        return "getObject"  # Java array (getString fails with a type conversion error)
    return "getString"


def _key_value(column: dict, raw):
    """JSON-serialisable keyset value, stored in state and bound back into the next query."""
    if raw is None:
        return None
    if column["type"] in INTEGER_TYPES:
        return int(raw)
    if column["type"] in BINARY_TYPES:
        return bytes(raw).hex()
    if _is_binary_string(column):
        return raw.replace(" ", "").lower()
    return raw  # the database's own text form, bound back as a string and cast to the column type


# ---------------------------------------------------------------------------------------------
# Streamed keyset reads
# ---------------------------------------------------------------------------------------------


def _select_expr(column: dict) -> str:
    quoted = _quote(column["name"])
    data_type = column["type"]
    if data_type in TIMESTAMP_TZ_TYPES:
        return f"TO_CHAR({quoted}, '{TIMESTAMP_TZ_FORMAT}')"
    if data_type in DATETIME_TZ_TYPES:
        return f"TO_CHAR({quoted}, '{DATETIME_TZ_FORMAT}')"
    return quoted


def _sql_type(column: dict) -> str:
    """CUBRID type name to cast a bound string to, or None to compare the string directly."""
    data_type = column["type"]
    if data_type in TIMESTAMP_TYPES | DATE_TYPES | TIME_TYPES:
        return data_type.upper()
    if data_type in TIMESTAMP_TZ_TYPES:
        return "TIMESTAMPTZ"  # the bound text carries an offset; compared as an instant
    if data_type in DATETIME_TZ_TYPES:
        return "DATETIMETZ"
    if data_type in DECIMAL_TYPES and column.get("precision"):
        return f"NUMERIC({int(column['precision'])},{int(column['scale'] or 0)})"
    if data_type in FLOAT_TYPES | DOUBLE_TYPES:
        return "DOUBLE"
    return None


def _string_literal(column: dict, value: str):
    """SQL rebuilding a string key value from its bytes in the column's charset, or None to bind it.

    CUBRID: the driver sends bound strings as the database charset, so a non-ASCII value does not
    compare equal to itself in a column with another charset (e.g. a utf8 column in an iso88591
    database), or in an iso88591 column holding UTF-8 bytes. CHR(n USING charset) builds each
    character from its exact bytes, and the comparison still uses the column's collation.
    """
    charset = column.get("charset")
    if charset == "binary":
        chars = [bytes([b]) for b in bytes.fromhex(value)]
    elif value.isascii() or charset not in CHARSET_CODECS:
        return None
    else:
        try:
            chars = [ch.encode(CHARSET_CODECS[charset]) for ch in value]
        except UnicodeEncodeError:
            return None
    if not chars:
        return "''"
    return "CONCAT(" + ", ".join(f"CHR({int.from_bytes(c, 'big')} USING {charset})" for c in chars) + ")"


def _param(column: dict, value, params: list) -> str:
    """SQL for a value compared with `column`: a placeholder (appending the binding) or a literal."""
    if column["type"] in STRING_TYPES and isinstance(value, str):
        literal = _string_literal(column, value)
        if literal:
            return literal
    params.append((column, value))
    sql_type = _sql_type(column)
    return f"CAST(? AS {sql_type})" if sql_type else "?"


def _keyset_condition(order_columns: list, after: list, params: list) -> str:
    """Rows strictly after `after` in (order_columns) order, expanded for databases without row-value comparison."""
    clauses = []
    for i, column in enumerate(order_columns):
        parts = [f"{_quote(order_columns[j]['name'])} = {_param(order_columns[j], after[j], params)}" for j in range(i)]
        parts.append(f"{_quote(column['name'])} > {_param(column, after[i], params)}")
        clauses.append("(" + " AND ".join(parts) + ")")
    return "(" + " OR ".join(clauses) + ")"


def _bind(statement, params: list):
    for index, (column, value) in enumerate(params, 1):
        if column["type"] in INTEGER_TYPES:
            statement.setLong(index, int(value))
        elif column["type"] in BINARY_TYPES:
            statement.setBytes(index, bytes.fromhex(value))
        else:
            statement.setString(index, str(value))


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

    query      executeQuery(): the server runs the query (scans, sorts and materializes the result)
               until the driver has the first fetch
    fetch      ResultSet.next() and the column getters: later fetches and decoding in the driver
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


def _read_rows(conn, spec: dict, order_columns: list, conditions: list, params: list, after, timer: _Timer,
               chunk_rows=None):
    """Yield (record, keyset position) in order_columns order.

    Each query is streamed through the JDBC fetch size. With chunk_rows, each query reads at most that
    many rows and the next continues after the last position delivered; without it, one query reads
    everything (used where every chunk would repeat a sort).
    """
    columns = spec["columns"]
    names = [c["name"] for c in columns]
    converters = [_converter(c) for c in columns]
    getters = [_getter(c) for c in columns]
    select_list = ", ".join(_select_expr(c) for c in columns)
    # Where each keyset value sits in the row (keyset columns are delivered columns)
    key_positions = [names.index(c["name"]) for c in order_columns]
    order_by = ", ".join(_quote(c["name"]) for c in order_columns)
    column_numbers = list(range(1, len(columns) + 1))

    while True:
        where = list(conditions)
        query_params = list(params)
        if after is not None:
            where.append(_keyset_condition(order_columns, after, query_params))
        sql = f"SELECT {select_list} FROM {_table_ref(spec)}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by}"
        if chunk_rows:
            sql += f" LIMIT {int(chunk_rows)}"

        if timer.queries == 0:
            log.info(f"{timer.key}: query: {sql}")
        started = time.perf_counter()
        statement = conn.prepareStatement(sql)
        try:
            statement.setFetchSize(spec["fetch_size"])
            _bind(statement, query_params)
            result = statement.executeQuery()
            timer.add_query(time.perf_counter() - started)
            read = [getattr(result, g) for g in getters]
            next_row = result.next
            row_count = 0
            position = None
            while True:
                # Read a group of raw rows, then convert it, so the two stages are timed separately
                fetch_started = time.perf_counter()
                raw_rows = []
                while len(raw_rows) < READ_GROUP and next_row():
                    raw_rows.append([get(n) for get, n in zip(read, column_numbers)])
                converted = time.perf_counter()
                timer.add("fetch", converted - fetch_started)
                if not raw_rows:
                    break
                batch = []
                for raw in raw_rows:
                    record = {}
                    for name, convert, value in zip(names, converters, raw):
                        record[name] = convert(value) if convert and value is not None else value
                    position = [_key_value(order_columns[k], raw[i]) for k, i in enumerate(key_positions)]
                    batch.append((record, position))
                row_count += len(raw_rows)
                del raw_rows
                timer.add("convert", time.perf_counter() - converted)
                yield from batch
            result.close()
        finally:
            statement.close()

        if not chunk_rows or row_count < chunk_rows:
            return
        if position == after:
            raise RuntimeError(f"{timer.key}: keyset position did not advance between chunks; stopping to avoid re-reading the same rows")
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


def _cursor_max(conn, spec: dict):
    """The cursor column's current maximum in its keyset text form (None if the table has no values)."""
    cursor = spec["cursor"]
    expr = f"MAX({_quote(cursor['name'])})"
    if cursor["type"] in TIMESTAMP_TZ_TYPES:
        expr = f"TO_CHAR({expr}, '{TIMESTAMP_TZ_FORMAT}')"
    elif cursor["type"] in DATETIME_TZ_TYPES:
        expr = f"TO_CHAR({expr}, '{DATETIME_TZ_FORMAT}')"
    return _key_value(cursor, _query(conn, f"SELECT {expr} FROM {_table_ref(spec)}")[0][0])


def _sync_incremental(conn, spec: dict, table_state: dict, state: dict):
    if table_state.get("cursor") is None:
        _sync_incremental_initial(conn, spec, table_state, state)
        return

    key = _table_key(spec)
    cursor = spec["cursor"]
    order_columns = [cursor] + [_column(spec, name) for name in spec["primary_key"]]
    # >= (not >) so rows sharing the cursor value are never skipped, including rows written at that
    # value after the previous sync. Re-reads are harmless: upserts are keyed on the primary key.
    params = []
    conditions = [f"{_quote(cursor['name'])} >= {_param(cursor, table_state['cursor'], params)}"]
    after = table_state.get("after")
    if after is not None:
        # Resume exactly after the last (cursor, primary key) delivered before the interruption.
        # Log only the cursor value: primary-key values can be personal data.
        log.info(f"{key}: resuming incremental sync after {cursor['name']}={after[0]}")
    else:
        log.info(f"{key}: incremental sync from {cursor['name']} >= {table_state['cursor']}")

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    # One query: chunks would each repeat the sort of the remaining changed rows
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


def _sync_incremental_initial(conn, spec: dict, table_state: dict, state: dict):
    """First sync of an incremental table: every row in primary-key order, then cursor = maximum at start.

    CUBRID: ordering the whole table by an unindexed cursor column needs a full sort on the server
    (no first row after 25 minutes for 10M rows with the default 2 MB sort buffer). The primary-key
    index returns rows in key order without one. The cursor's maximum is captured before the read, so
    a row changed during the read gets a cursor value at or above it and is read again next sync.
    Rows with a NULL cursor are included here.
    """
    key = _table_key(spec)
    cursor = spec["cursor"]
    order_columns = [_column(spec, name) for name in spec["primary_key"]]
    after = table_state.get("after")
    if after is not None and "high_water" in table_state:
        log.info(f"{key}: resuming initial sync in primary-key order (cursor will be {cursor['name']}={table_state['high_water']})")
    else:
        after = None
        table_state.pop("after", None)
        table_state["high_water"] = _cursor_max(conn, spec)
        log.info(f"{key}: initial sync in primary-key order; {cursor['name']} maximum at start is {table_state['high_water']}")

    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_rows(conn, spec, order_columns, [], [], after, timer, CHUNK_ROWS):
        _upsert(spec, record, timer)
        table_state["after"] = position
        total += 1
        _log_progress(timer, total)
        checkpointer.row_done()

    table_state.pop("after", None)
    table_state["cursor"] = table_state.pop("high_water")
    checkpointer.checkpoint()
    log.info(f"{key}: initial sync done, {total:,} rows, {cursor['name']}={table_state['cursor']}")
    timer.report_total("table total")


def _sync_null_cursor_rows(conn, spec: dict, state: dict):
    """Re-send rows whose cursor is NULL. The cursor can never select them, so read them by key every sync."""
    key = _table_key(spec)
    cursor = spec["cursor"]
    order_columns = [_column(spec, name) for name in spec["primary_key"]]
    conditions = [f"{_quote(cursor['name'])} IS NULL"]
    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, _ in _read_rows(conn, spec, order_columns, conditions, [], None, timer, CHUNK_ROWS):
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
    for record, position in _read_rows(conn, spec, order_columns, [], [], after, timer, CHUNK_ROWS):
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
