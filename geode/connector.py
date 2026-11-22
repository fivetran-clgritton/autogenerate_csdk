"""Fivetran Connector SDK connector for Apache Geode (and VMware GemFire through the same REST API).

Geode is a key-value data grid, not a SQL database, so the IRIS template's requirements map as follows:

- Tables are regions, listed by the developer REST API (`GET /geode/v1`). Geode has no schemas; every
  region goes to the connection's destination schema. Subregions are not reachable through the REST
  API and are skipped with a warning.
- The primary key is the region key, delivered in its own column (`key_column`, default `_key`). Only
  string and integer keys are supported; regions with other key types are skipped with a warning.
- Columns are the fields of the entry values. There is no catalog for them, so the connector samples
  entries (`sample_size`) and infers a type per field. A value that is not an object (e.g. a plain
  string) goes into a column named `value`.
- Incremental: a field named in `cursor_columns` (default `updated_at`) whose sampled values are all
  ISO-8601 timestamp/date strings or all integers. Reads `cursor >= last value` in keyset order on
  (cursor, key). Entries without the field, or with it NULL, are re-read every sync.
- Full reload: regions without a usable cursor. Deferred truncate, then upsert every entry in key order.

Reads are two-step because Geode's OQL has no OFFSET or server-side cursor, and an ORDER BY query that
also returns the values is about three times slower than one returning only the key and the cursor
(15 s vs 5 s per query on 1M entries): one ordered query returns up to KEY_BATCH_SIZE (key, cursor)
pairs, then the values are fetched by key (`e.key IN SET(...)`, a direct lookup) in chunks bounded by
URL length and response size. All queries are ad-hoc OQL over HTTP GET: the REST API's bind
parameters work only for named queries, which are stored on the server, and a sync should not write to
the cluster. Literals are therefore escaped by the connector.

This connector is a port of the IRIS template (iris/connector.py). Geode-specific parts are marked "Geode:".
"""

import gc
import json
import re
import time
from datetime import date, datetime, timezone
from urllib.parse import quote as url_quote

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Geode: (key, cursor) pairs per ordered query. Each query scans the region and sorts the remaining
# matching entries (OQL has no OFFSET or server-side cursor), so larger batches mean fewer sorts; the
# pairs are small (about 100 bytes each in memory).
KEY_BATCH_SIZE = 100_000
# Geode: values are fetched with `e.key IN SET(...)` over HTTP GET; Jetty rejects request lines over
# 8 KB (HTTP 414), so the encoded query is kept under MAX_URL_BYTES, and a chunk's values are kept to
# about TARGET_RESPONSE_BYTES (estimated from the largest value seen so far).
MAX_URL_BYTES = 7_000
TARGET_RESPONSE_BYTES = 16_000_000
# The SDK buffers upserted rows in memory until the next checkpoint flushes them, so checkpoint on
# whichever comes first: CHECKPOINT_INTERVAL_SECONDS or the row cap. Measured with a test connector
# (2M rows): one checkpoint at the end peaked at 3.5 GB, every 60 s at 1.4 GB, every 10k rows at
# 290 MB.
CHECKPOINT_INTERVAL_SECONDS = 60
CHECKPOINT_ROW_CAP = 100_000
WIDE_TABLE_COLUMNS = 20
MIN_CHECKPOINT_ROW_CAP = 10_000
# Regions whose sampled values are large (e.g. documents of hundreds of KB) checkpoint more often
LARGE_VALUE_BYTES = 100_000
LARGE_VALUE_CHECKPOINT_ROW_CAP = 1_000
PROGRESS_LOG_EVERY = 250_000

DEFAULT_CURSOR_COLUMNS = "updated_at"
DEFAULT_KEY_COLUMN = "_key"
DEFAULT_SAMPLE_SIZE = 1_000
SCALAR_VALUE_COLUMN = "value"

RETRY_STATUSES = {429, 502, 503, 504}  # Geode answers 500 for OQL errors, which are not transient
REQUEST_TIMEOUT = (30, 900)  # connect, read: an ORDER BY over a large region runs before any bytes come back
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_SECONDS = 5

# Geode: region names starting with "__" are internal (e.g. __ParameterizedQueries__). A region named
# in include_tables is synced even if it is internal.
SYSTEM_REGION_PREFIX = "__"
# Geode: region names are letters, digits, '_' and '-'; anything else is not put into a query.
REGION_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")

LONG_MIN, LONG_MAX = -(2 ** 63), 2 ** 63 - 1
INT_MIN, INT_MAX = -(2 ** 31), 2 ** 31 - 1

# Field kinds observed while sampling, and the declared type they map to
_TS_NAIVE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?$")
_TS_ZONED_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:?\d{2})$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
CURSOR_KINDS = {"ts_naive", "ts_zoned", "date", "int"}


def _csv(configuration: dict, key: str, default: str = "") -> list:
    raw = configuration.get(key) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _flag(configuration: dict, key: str, default: bool) -> bool:
    raw = str(configuration.get(key) or "").strip().lower()
    if not raw:
        return default
    if raw not in ("true", "false"):
        raise ValueError(f"{key} must be true or false")
    return raw == "true"


def _validate_configuration(configuration: dict):
    if not configuration.get("host"):
        raise ValueError("Missing required configuration value: host")
    if not str(configuration.get("rest_port") or "8081").isdigit():
        raise ValueError("rest_port must be a number")
    if not str(configuration.get("sample_size") or DEFAULT_SAMPLE_SIZE).isdigit():
        raise ValueError("sample_size must be a number")
    if bool(configuration.get("username")) != bool(configuration.get("password")):
        raise ValueError("Set both username and password, or neither")
    _flag(configuration, "use_https", False)
    _flag(configuration, "validate_certificate", True)
    _flag(configuration, "detect_timestamps", True)
    for entry in _csv(configuration, "cursor_overrides"):
        name, sep, _ = entry.partition("=")
        if not sep or not name.strip():
            raise ValueError(
                f"Invalid cursor_overrides entry '{entry}': use region=field, or region= to force a full reload"
            )
    key_column = configuration.get("key_column") or DEFAULT_KEY_COLUMN
    if key_column.lower().startswith("_fivetran"):
        raise ValueError("key_column must not start with _fivetran")


def _matches(patterns: list, region: str) -> bool:
    return any(p.lower() == region.lower() for p in patterns)


# ---------------------------------------------------------------------------------------------
# Geode: REST client
# ---------------------------------------------------------------------------------------------


class GeodeError(RuntimeError):
    pass


class _Client:
    """The Geode developer REST API (`/geode/v1`): the region list and ad-hoc OQL queries."""

    def __init__(self, configuration: dict):
        scheme = "https" if _flag(configuration, "use_https", False) else "http"
        self.base = f"{scheme}://{configuration['host']}:{configuration.get('rest_port') or 8081}/geode/v1"
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        if configuration.get("username"):
            self.session.auth = (configuration["username"], configuration["password"])
        self.session.verify = _flag(configuration, "validate_certificate", True)

    def close(self):
        self.session.close()

    def _get(self, url: str, params=None):
        """GET with retries on connection failures; HTTP errors are raised at once with Geode's message."""
        for attempt in range(1, CONNECT_ATTEMPTS + 1):
            try:
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if response.status_code not in RETRY_STATUSES:
                    break
                problem = f"HTTP {response.status_code}"
            except (requests.ConnectionError, requests.Timeout) as e:
                problem = _without_url(e)
            if attempt == CONNECT_ATTEMPTS:
                raise GeodeError(f"Could not reach the Geode REST API at {self.base} after {CONNECT_ATTEMPTS} attempts: {problem}")
            delay = CONNECT_RETRY_SECONDS * 2 ** (attempt - 1)
            log.warning(f"Geode REST API request failed (attempt {attempt} of {CONNECT_ATTEMPTS}), retrying in {delay}s: {problem}")
            time.sleep(delay)
        if response.status_code in (401, 403):
            raise GeodeError(f"Geode REST API refused the request (HTTP {response.status_code}): check username, password and permissions (DATA:READ)")
        if response.status_code >= 400:
            raise GeodeError(f"Geode REST API returned HTTP {response.status_code}: {_error_message(response)}")
        return response

    def regions(self) -> list:
        return self._get(self.base).json().get("regions", [])

    def oql(self, query: str) -> list:
        return self.oql_sized(query)[0]

    def oql_sized(self, query: str):
        """Run an ad-hoc OQL query; return the result rows and the response size in bytes."""
        response = self._get(f"{self.base}/queries/adhoc", params={"q": _double_decode_safe(query)})
        return response.json(), len(response.content)

    def query_url_bytes(self, query: str) -> int:
        return len(self.base) + len("/queries/adhoc?q=") + _encoded_bytes(query)


def _without_url(error: Exception) -> str:
    """The error text without the request URL: a query URL holds region keys, which can be personal data."""
    return re.sub(r"(with url: )\S+", r"\1<query>", f"{type(error).__name__}: {error}")[:500]


def _double_decode_safe(query: str) -> str:
    """Geode: the REST API URL-decodes the `q` parameter a second time (java.net.URLDecoder), so '%xx'
    in a literal would become another character, a lone '%' fails the request and '+' becomes a space
    (found with a key '%25' that silently went missing, and a '+02:00' timestamp). Escape both, so the
    second decode restores the query text."""
    return query.replace("%", "%25").replace("+", "%2B")


def _encoded_bytes(text: str) -> int:
    return len(url_quote(_double_decode_safe(text), safe=""))


def _error_message(response) -> str:
    try:
        body = response.json()
        if isinstance(body, dict):
            return str(body.get("message") or body.get("cause") or body)[:500]
    except ValueError:
        pass
    return response.text[:500]


# ---------------------------------------------------------------------------------------------
# Geode: OQL text
# ---------------------------------------------------------------------------------------------


def _region_path(spec: dict) -> str:
    return "/" + spec["region"]  # REGION_NAME_RE guarantees the name is safe in a query


def _field_path(field: str) -> str:
    # Double-quoted identifiers allow any field name, including OQL keywords; names containing a
    # double quote are never used in a query (they cannot be cursors).
    return f'e.value."{field}"'


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _literal(value, kind: str) -> str:
    """OQL literal for a key or cursor value. Integers use the long form (comparisons promote Integer keys)."""
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"Expected an integer keyset value, got {type(value).__name__}")
        return f"{value}L"
    if not isinstance(value, str):
        raise ValueError(f"Expected a string keyset value, got {type(value).__name__}")
    return _string_literal(value)


def _key_set_literals(key, kind: str) -> list:
    """Literals for `e.key IN SET(...)`. IN SET compares with equals(), so an integer key matches only a
    literal of the same class: list both the Integer and the Long form."""
    if kind == "int":
        return [str(key), f"{key}L"] if INT_MIN <= key <= INT_MAX else [f"{key}L"]
    return [_string_literal(key)]


# ---------------------------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------------------------


def _value_kind(value, detect_timestamps: bool) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int" if LONG_MIN <= value <= LONG_MAX else "bigint"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (dict, list)):
        return "json"
    if isinstance(value, str):
        if _DATE_RE.match(value):
            return "date" if detect_timestamps else "date_str"
        if _TS_NAIVE_RE.match(value):
            return "ts_naive" if detect_timestamps else "ts_str"
        if _TS_ZONED_RE.match(value):
            return "ts_zoned" if detect_timestamps else "ts_str"
        return "str"
    return "str"


def _declared_type(kinds: set):
    """Type to declare for a field from the kinds of its sampled non-null values, or None (all NULL).

    Every type is declared: an undeclared float is inferred as 32-bit FLOAT, and an undeclared string
    gets a type guessed from its content (found on the Exasol and H2 ports).
    """
    kinds = kinds - {"null"}
    if not kinds:
        return None
    if "json" in kinds:
        return "JSON"
    if kinds == {"bool"}:
        return "BOOLEAN"
    if kinds == {"int"}:
        return "LONG"
    if kinds <= {"int", "float"}:
        return "DOUBLE"
    if kinds == {"ts_naive"}:
        return "NAIVE_DATETIME"
    if kinds == {"ts_zoned"}:
        return "UTC_DATETIME"
    if kinds == {"date"}:
        return "NAIVE_DATE"
    return "STRING"


def _discover(client: _Client, configuration: dict, verbose: bool = True) -> list:
    """Return a spec dict for each region to sync, built from the region list, a sample of entries and
    the configuration. Runs from both schema() and update(); only the update() run logs (verbose)."""
    include = _csv(configuration, "include_tables")
    exclude = _csv(configuration, "exclude_tables")
    cursor_candidates = [c.lower() for c in _csv(configuration, "cursor_columns", DEFAULT_CURSOR_COLUMNS)]
    overrides = {}
    for entry in _csv(configuration, "cursor_overrides"):
        name, _, field = entry.partition("=")
        overrides[name.strip().lower()] = field.strip()
    key_column = configuration.get("key_column") or DEFAULT_KEY_COLUMN
    sample_size = max(1, int(configuration.get("sample_size") or DEFAULT_SAMPLE_SIZE))
    detect_timestamps = _flag(configuration, "detect_timestamps", True)

    selected = []
    for region in client.regions():
        name = region.get("name") or ""
        if include:
            if not _matches(include, name):
                continue
        elif name.startswith(SYSTEM_REGION_PREFIX):
            continue
        if exclude and _matches(exclude, name):
            continue
        selected.append(region)
    selected.sort(key=lambda r: r["name"].lower())

    selected_names = {r["name"].lower() for r in selected}
    if verbose:
        for name in include:
            if name.lower() not in selected_names:
                op.warning(f"include_tables names {name}, which is not a region the REST API lists")
        for name in overrides:
            if name not in selected_names:
                op.warning(f"cursor_overrides names {name}, which is not among the regions being synced")

    specs, skipped = [], []
    by_destination = {}
    for region in selected:
        name = region["name"]
        if not REGION_NAME_RE.match(name):
            skipped.append(f"{name} (name contains characters Geode region names cannot have)")
            continue
        spec, reason = _build_spec(client, region, key_column, sample_size, detect_timestamps)
        if spec is None:
            skipped.append(f"{name} ({reason})")
            continue
        cursor = _choose_cursor(spec, cursor_candidates, overrides, verbose=False)
        if cursor and spec["sampled"]:
            # Geode: also sample the entries with the highest cursor values. Fields added by recent
            # writes are most likely there, and an arbitrary sample of a large region can miss them.
            spec, reason = _build_spec(client, region, key_column, sample_size, detect_timestamps, cursor)
            if spec is None:
                skipped.append(f"{name} ({reason})")
                continue
        spec["cursor"] = _choose_cursor(spec, cursor_candidates, overrides, verbose)
        spec["row_cap"] = _row_cap(spec)
        by_destination.setdefault(name.lower(), []).append(spec)

    for destination_name, group in sorted(by_destination.items()):
        if len(group) > 1:
            # Region names are case-sensitive; destination table names are not
            skipped.extend(f"{s['region']} (same destination table name as another region; exclude one)" for s in group)
            continue
        specs.extend(group)

    if verbose and skipped:
        shown = "; ".join(skipped[:20]) + (f"; and {len(skipped) - 20} more" if len(skipped) > 20 else "")
        op.warning(f"{len(skipped)} regions skipped: {shown}")
    return specs


# Sample profiles are cached for the length of one sync, so update() reuses what schema() read (the
# recent-entries sample sorts the region). update() clears the cache when it finishes. Only the profile
# (field names, value kinds, sizes) is kept, never the sampled values.
_SAMPLE_CACHE = {}
SAMPLE_CACHE_SECONDS = 1800
# Until the first value query of a sample returns, assume large values, so the first chunk stays small
SAMPLE_FIRST_GUESS_BYTES = 1_000_000


class _Profile:
    """What a sample of entries says about a region: key kinds, field kinds in first-seen order, and the
    largest value size."""

    def __init__(self, detect_timestamps: bool):
        self.detect_timestamps = detect_timestamps
        self.key_kinds = set()
        self.fields = {}
        self.max_value_bytes = 0
        self.count = 0

    def add(self, key, value):
        self.count += 1
        self.key_kinds.add(_key_kind(key))
        fields = value if isinstance(value, dict) else {SCALAR_VALUE_COLUMN: value}
        for field, field_value in fields.items():
            self.fields.setdefault(field, set()).add(_value_kind(field_value, self.detect_timestamps))
        self.max_value_bytes = max(self.max_value_bytes, len(json.dumps(value)))

    def merge(self, other):
        self.count += other.count
        self.key_kinds |= other.key_kinds
        for field, kinds in other.fields.items():
            self.fields.setdefault(field, set()).update(kinds)
        self.max_value_bytes = max(self.max_value_bytes, other.max_value_bytes)


def _cached(client: _Client, key: tuple, load):
    key = (client.base,) + key
    hit = _SAMPLE_CACHE.get(key)
    if hit and time.monotonic() - hit[0] < SAMPLE_CACHE_SECONDS:
        return hit[1]
    value = load()
    _SAMPLE_CACHE[key] = (time.monotonic(), value)
    return value


def _profile_keys(client: _Client, name: str, key_kind: str, keys: list, detect_timestamps: bool) -> _Profile:
    """Fetch the values of `keys` in size-bounded chunks and profile them as they arrive."""
    profile = _Profile(detect_timestamps)
    fetcher = _ValueFetcher(client, {"region": name, "key_kind": key_kind}, initial_value_bytes=SAMPLE_FIRST_GUESS_BYTES)
    for found in fetcher.batches(keys, lambda key: key):
        for key, value in found:
            profile.add(key, value)
    return profile


def _key_kind(key) -> str:
    return "int" if isinstance(key, int) and not isinstance(key, bool) else type(key).__name__


def _arbitrary_sample(client: _Client, name: str, sample_size: int, detect_timestamps: bool) -> _Profile:
    """Profile of up to sample_size entries in the region's own iteration order."""
    keys = [row["k"] for row in client.oql(f"SELECT e.key AS k FROM /{name}.entrySet e LIMIT {sample_size}")]
    key_kinds = {_key_kind(k) for k in keys}
    if key_kinds in ({"str"}, {"int"}):
        return _profile_keys(client, name, key_kinds.pop(), keys, detect_timestamps)
    profile = _Profile(detect_timestamps)  # empty region, or keys the connector cannot use (region skipped)
    profile.key_kinds = key_kinds
    return profile


def _recent_sample(client: _Client, name: str, key_kind: str, cursor: dict, sample_size: int, detect_timestamps: bool) -> _Profile:
    """Profile of up to sample_size entries with the highest cursor values."""
    path = _field_path(cursor["name"])
    # Geode: on a partitioned region every ORDER BY expression must also be projected
    query = (f"SELECT e.key AS k, {path} AS c FROM /{name}.entrySet e WHERE {_defined(cursor)} "
             f"ORDER BY {path} DESC, e.key DESC LIMIT {sample_size}")
    try:
        keys = [row["k"] for row in client.oql(query)]
    except GeodeError as e:
        raise GeodeError(f"{name}: sampling the most recent entries failed: {e}. Check that every "
                         f"entry's {cursor['name']} field has the same type (Geode cannot order mixed types).") from None
    return _profile_keys(client, name, key_kind, keys, detect_timestamps)


def _build_spec(client: _Client, region: dict, key_column: str, sample_size: int, detect_timestamps: bool, cursor=None):
    """Sample a region and return (spec, None), or (None, reason the region is skipped). With a cursor,
    the sample also includes the entries with the highest cursor values."""
    name = region["name"]
    try:
        arbitrary = _cached(client, (name, "arbitrary", sample_size, detect_timestamps),
                            lambda: _arbitrary_sample(client, name, sample_size, detect_timestamps))
    except GeodeError as e:
        if "cannot be resolved" in str(e) or "not found" in str(e):
            # Geode: the REST API lists subregions by their own name, but they cannot be queried by it
            return None, "not reachable as a root region; subregions are not supported"
        raise
    profile = _Profile(detect_timestamps)
    profile.merge(arbitrary)
    key_kinds = set(profile.key_kinds)
    if not key_kinds:
        # An empty region: take the key type from the region's key constraint, else string (keys written
        # through the REST API are always strings). It syncs as a full reload with no rows.
        constraint = (region.get("key-constraint") or "").rsplit(".", 1)[-1]
        key_kinds = {"int"} if constraint in ("Integer", "Long", "Short", "Byte") else {"str"}
    if key_kinds not in ({"str"}, {"int"}):
        return None, f"keys of type {', '.join(sorted(key_kinds))}; only string or integer keys are supported"
    key_kind = "int" if key_kinds == {"int"} else "str"
    if cursor:
        profile.merge(_cached(client, (name, "recent", cursor["name"], sample_size, detect_timestamps),
                              lambda: _recent_sample(client, name, key_kind, cursor, sample_size, detect_timestamps)))

    lowered = {}
    for field in profile.fields:
        lowered.setdefault(field.lower(), []).append(field)
    if key_column.lower() in lowered:
        return None, f"its values have a field named {key_column}, the key column; set key_column to another name"
    collisions = [names for names in lowered.values() if len(names) > 1]
    if collisions:
        return None, "fields that differ only in case: " + ", ".join("/".join(n) for n in collisions[:5])

    columns = [{"name": key_column, "type": "LONG" if key_kind == "int" else "STRING", "kinds": {key_kind}, "key": True}]
    for field, kinds in profile.fields.items():
        columns.append({"name": field, "type": _declared_type(kinds), "kinds": kinds - {"null"}, "key": False})
    return {
        "region": name,
        "table": name,
        "key_column": key_column,
        "key_kind": key_kind,
        "columns": columns,
        "sampled": profile.count,
        "max_value_bytes": profile.max_value_bytes,
    }, None


def _choose_cursor(spec: dict, candidates: list, overrides: dict, verbose: bool):
    """Return the cursor field for an incremental sync, or None for a full reload.

    Geode: a usable cursor is a field whose sampled values are all ISO-8601 timestamp or date strings
    of one form (Geode compares them as strings, which matches time order for one consistent format),
    or all integers.
    """
    region = spec["region"]
    by_name = {c["name"].lower(): c for c in spec["columns"] if not c["key"]}
    override = overrides.get(region.lower())

    def usable(column):
        return '"' not in column["name"] and len(column["kinds"]) == 1 and column["kinds"] <= CURSOR_KINDS | {"ts_str", "date_str"}

    if override is not None:
        if override == "":
            if verbose:
                log.info(f"{region}: cursor_overrides forces a full reload")
            return None
        column = by_name.get(override.lower())
        if column is None:
            raise ValueError(f"cursor_overrides: field {override} not found in the sampled entries of {region}")
        if not usable(column):
            raise ValueError(
                f"cursor_overrides: {region}.{column['name']} has sampled values of kind "
                f"{', '.join(sorted(column['kinds'])) or 'NULL only'}; a cursor must hold ISO-8601 timestamp or "
                "date strings of one form, or integers"
            )
        return _cursor(column)

    for name in candidates:
        column = by_name.get(name)
        if column and usable(column):
            return _cursor(column)
    if verbose:
        for name in candidates:
            column = by_name.get(name)
            if column:
                log.warning(
                    f"{region}: field {column['name']} is not used as a cursor because its sampled values are "
                    f"{', '.join(sorted(column['kinds'])) or 'all NULL'}; the region is fully reloaded every sync"
                )
    return None


def _cursor(column: dict) -> dict:
    (kind,) = column["kinds"]
    return {"name": column["name"], "kind": "int" if kind == "int" else "str"}


def _row_cap(spec: dict) -> int:
    """Rows per checkpoint, smaller for wide regions and regions with large values."""
    if spec["max_value_bytes"] >= LARGE_VALUE_BYTES:
        return LARGE_VALUE_CHECKPOINT_ROW_CAP
    scale = min(1.0, WIDE_TABLE_COLUMNS / max(1, len(spec["columns"])))
    return max(MIN_CHECKPOINT_ROW_CAP, int(CHECKPOINT_ROW_CAP * scale))


# ---------------------------------------------------------------------------------------------
# Schema and value conversion
# ---------------------------------------------------------------------------------------------


def _table_definition(spec: dict) -> dict:
    declared = {c["name"]: c["type"] for c in spec["columns"] if c["type"]}
    return {"table": spec["table"], "primary_key": [spec["key_column"]], "columns": declared}


# Timestamps and dates are passed to the SDK as datetime/date objects, not ISO strings: the SDK
# re-parses strings with strptime, which took about 13% of sync time in a profile of 200k rows.
def _parse_iso(value: str) -> datetime:
    text = value.replace(" ", "T", 1)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    head, dot, rest = text.partition(".")
    if dot:
        # Python keeps at most microseconds; drop digits beyond 6 (the raw string stays in the keyset)
        digits = re.match(r"\d*", rest).group(0)
        text = f"{head}.{digits[:6]}{rest[len(digits):]}"
    return datetime.fromisoformat(text)


def _to_naive_datetime(value):
    if isinstance(value, str) and _TS_NAIVE_RE.match(value):
        return _parse_iso(value)
    raise ValueError


def _to_utc_datetime(value):
    if isinstance(value, str) and _TS_ZONED_RE.match(value):
        return _parse_iso(value).astimezone(timezone.utc)
    raise ValueError


def _to_date(value):
    if isinstance(value, str) and _DATE_RE.match(value):
        return date.fromisoformat(value)
    raise ValueError


def _to_long(value):
    if isinstance(value, bool):
        raise ValueError
    if isinstance(value, int) and LONG_MIN <= value <= LONG_MAX:
        return value
    if isinstance(value, float) and value.is_integer() and LONG_MIN <= value <= LONG_MAX:
        return int(value)
    raise ValueError


def _to_double(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError
    return float(value)


def _to_boolean(value):
    if isinstance(value, bool):
        return value
    raise ValueError


def _to_string(value):
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def _passthrough(value):
    return value


CONVERTERS = {
    "NAIVE_DATETIME": _to_naive_datetime,
    "UTC_DATETIME": _to_utc_datetime,
    "NAIVE_DATE": _to_date,
    "LONG": _to_long,
    "DOUBLE": _to_double,
    "BOOLEAN": _to_boolean,
    "STRING": _to_string,
    "JSON": _passthrough,
}


class _Converter:
    """Turns an entry into a record. A value that does not fit its field's declared type (the sample
    did not see that kind of value) is delivered as NULL and counted; the counts are reported once
    per table, so the sync does not fail on one odd entry."""

    def __init__(self, spec: dict):
        self.key_column = spec["key_column"]
        self.converters = {c["name"]: CONVERTERS[c["type"]] for c in spec["columns"] if c["type"] and not c["key"]}
        self.mismatches = {}

    def record(self, key, value) -> dict:
        fields = value if isinstance(value, dict) else {SCALAR_VALUE_COLUMN: value}
        if self.key_column in fields:
            raise GeodeError(f"An entry's value has a field named {self.key_column}, the key column; set key_column to another name")
        record = {self.key_column: key}
        for field, field_value in fields.items():
            if field_value is None:
                record[field] = None
                continue
            convert = self.converters.get(field)
            if convert is None:
                # Not in the sample: let the SDK infer it; the next sync's schema() declares it.
                # Nested objects are serialised so an inferred column never holds a dict.
                record[field] = json.dumps(field_value, ensure_ascii=False) if isinstance(field_value, (dict, list)) else field_value
                continue
            try:
                record[field] = convert(field_value)
            except (ValueError, OverflowError):
                record[field] = None
                self.mismatches[field] = self.mismatches.get(field, 0) + 1
        return record

    def report(self, table: str):
        if self.mismatches:
            detail = ", ".join(f"{field} ({count:,} rows)" for field, count in sorted(self.mismatches.items()))
            op.warning(
                f"{table}: values that did not match the type inferred from the sampled entries were delivered as "
                f"NULL: {detail}. Raise sample_size, or set detect_timestamps to false if these are timestamp fields "
                "with irregular values, then re-sync the table."
            )


# ---------------------------------------------------------------------------------------------
# Geode: two-step reads
# ---------------------------------------------------------------------------------------------


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

    query      the ordered (key, cursor) queries: Geode scans the region and sorts the matching entries
    fetch      the value queries by key (`e.key IN SET(...)`), including HTTP transfer and JSON decoding
    convert    turning entries into SDK records
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
                f"{queries} ordered queries, avg {average:.2f}s, slowest {slowest:.2f}s; "
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


def _keyset_condition(order: list, after: list) -> str:
    """Entries strictly after `after` in `order` order. order: [(OQL expression, literal kind)]."""
    clauses = []
    for i, (expression, kind) in enumerate(order):
        parts = [f"{order[j][0]} = {_literal(after[j], order[j][1])}" for j in range(i)]
        parts.append(f"{expression} > {_literal(after[i], kind)}")
        clauses.append("(" + " AND ".join(parts) + ")")
    return "(" + " OR ".join(clauses) + ")"


def _ordered_positions(client: _Client, spec: dict, cursor, conditions: list, after, timer: _Timer):
    """Yield keyset positions ([cursor, key] or [key]) in order, one bounded ordered query per batch."""
    order = ([(_field_path(cursor["name"]), cursor["kind"])] if cursor else []) + [("e.key", spec["key_kind"])]
    projection = (f"e.key AS k, {_field_path(cursor['name'])} AS c" if cursor else "e.key AS k")
    order_by = ", ".join(expression for expression, _ in order)
    while True:
        where = list(conditions)
        if after is not None:
            where.append(_keyset_condition(order, after))
        query = f"SELECT {projection} FROM {_region_path(spec)}.entrySet e"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += f" ORDER BY {order_by} LIMIT {KEY_BATCH_SIZE}"
        if timer.queries == 0:
            log.info(f"{timer.key}: ordered query: {query if after is None else _redacted(query)}")

        started = time.perf_counter()
        try:
            rows = client.oql(query)
        except GeodeError as e:
            hint = (f" Check that every entry's {cursor['name']} field has the same type (Geode cannot order "
                    "mixed types)." if cursor else "")
            raise GeodeError(f"{timer.key}: ordered query failed: {e}.{hint}") from None
        timer.add_query(time.perf_counter() - started)

        positions = [[row["c"], row["k"]] if cursor else [row["k"]] for row in rows]
        del rows
        yield from positions
        if len(positions) < KEY_BATCH_SIZE:
            return
        # The next batch starts after the last entry of this one. Fail rather than loop if it would not move.
        if positions[-1] == after:
            raise RuntimeError(f"{timer.key}: keyset position did not advance between batches; stopping to avoid re-reading the same entries")
        after = positions[-1]


def _redacted(query: str) -> str:
    # Keyset values (region keys) can be personal data; keep them out of the logs
    return query.split(" WHERE ")[0] + " WHERE <conditions, after the last entry delivered> ..."


class _ValueFetcher:
    """Fetches entry values by key with `e.key IN SET(...)`, in chunks bounded by URL length and by an
    estimate of the response size. Keys that are gone by the time of the fetch are skipped and counted."""

    def __init__(self, client: _Client, spec: dict, timer=None, initial_value_bytes: int = 0):
        self.client = client
        self.key_kind = spec["key_kind"]
        self.base_query = f"SELECT e.key AS k, e.value AS v FROM {_region_path(spec)}.entrySet e WHERE e.key IN SET()"
        self.base_bytes = client.query_url_bytes(self.base_query)
        # Bytes per value used to size chunks: the sample's largest value (a floor that stays), or a guess
        # that holds only until the first response; then the largest average per response seen
        self.floor = max(spec.get("max_value_bytes", 0), 100)
        self.estimate = max(self.floor, initial_value_bytes)
        self.timer = timer
        self.vanished = 0

    def batches(self, items, key_of):
        """Yield lists of (item, value) in item order, one value query per list."""
        chunk, url_bytes = [], 0
        for item in items:
            literal_bytes = sum(_encoded_bytes(lit) + 3 for lit in _key_set_literals(key_of(item), self.key_kind))
            chunk_limit = max(1, TARGET_RESPONSE_BYTES // self.estimate)
            if chunk and (self.base_bytes + url_bytes + literal_bytes > MAX_URL_BYTES or len(chunk) >= chunk_limit):
                yield self._fetch(chunk, key_of)
                chunk, url_bytes = [], 0
            chunk.append(item)
            url_bytes += literal_bytes
        if chunk:
            yield self._fetch(chunk, key_of)

    def _fetch(self, chunk: list, key_of) -> list:
        literals = [lit for item in chunk for lit in _key_set_literals(key_of(item), self.key_kind)]
        query = self.base_query[:-1] + ", ".join(literals) + ")"
        started = time.perf_counter()
        response_rows, response_bytes = self.client.oql_sized(query)
        if self.timer:
            self.timer.add("fetch", time.perf_counter() - started)
        # Size later chunks from the largest average value size seen, to stay near TARGET_RESPONSE_BYTES
        self.floor = max(self.floor, response_bytes // max(1, len(response_rows)))
        self.estimate = self.floor
        values = {row["k"]: row["v"] for row in response_rows}
        del response_rows
        found = []
        for item in chunk:
            key = key_of(item)
            if key in values:
                found.append((item, values[key]))
            else:
                self.vanished += 1  # removed since the ordered query
        return found


def _read_entries(client: _Client, spec: dict, cursor, conditions: list, after, timer: _Timer, converter: _Converter):
    """Yield (record, keyset position) in keyset order: ordered positions, then their values fetched by key."""
    key_index = 1 if cursor else 0
    fetcher = _ValueFetcher(client, spec, timer)
    positions = _ordered_positions(client, spec, cursor, conditions, after, timer)
    for found in fetcher.batches(positions, lambda position: position[key_index]):
        started = time.perf_counter()
        batch = [(converter.record(position[key_index], value), position) for position, value in found]
        del found
        timer.add("convert", time.perf_counter() - started)
        yield from batch
    if fetcher.vanished:
        log.info(f"{timer.key}: {fetcher.vanished:,} entries were removed between the ordered query and the value fetch (deleted during the sync)")


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
    op.upsert(table=spec["table"], data=record)
    timer.add("upsert", time.perf_counter() - started)


def _log_progress(timer: _Timer, total: int, detail: str = ""):
    if total % PROGRESS_LOG_EVERY == 0:
        log.info(f"{timer.key}: {total:,} rows synced{detail}")
        timer.report_interval(f"last {PROGRESS_LOG_EVERY:,} rows")


# ---------------------------------------------------------------------------------------------
# Sync strategies
# ---------------------------------------------------------------------------------------------


def _table_state(spec: dict, previous):
    """State for one region, reset when its strategy or cursor field changes; reports field drift."""
    key = spec["region"]
    mode = "incremental" if spec["cursor"] else "full"
    cursor_name = spec["cursor"]["name"] if spec["cursor"] else None
    column_names = sorted(c["name"] for c in spec["columns"])

    if previous and (previous.get("mode") != mode or previous.get("cursor_column") != cursor_name):
        log.warning(
            f"{key}: sync strategy changed from {previous.get('mode')} ({previous.get('cursor_column')}) "
            f"to {mode} ({cursor_name}); syncing this region from the beginning"
        )
        previous = None

    if previous:
        known = set(previous.get("columns", []))
        added = [c for c in column_names if c not in known]
        dropped = sorted(known - set(column_names))
        if added and known:
            if mode == "incremental":
                op.warning(
                    f"{key}: new fields {', '.join(added)}. Existing rows get values for them only when "
                    "they next change; re-sync the table to backfill."
                )
            else:
                log.info(f"{key}: new fields {', '.join(added)}")
        if dropped:
            # Geode: fields come from a sample, so a field can also disappear because the sample no longer
            # contains an entry that has it.
            log.info(f"{key}: fields no longer in the sampled entries: {', '.join(dropped)} (kept in the destination; NULL in rows synced from now on unless an entry still has them)")
        table_state = previous
    else:
        table_state = {"mode": mode}
        if cursor_name:
            table_state["cursor_column"] = cursor_name
    table_state["columns"] = column_names
    return table_state


def _defined(cursor: dict) -> str:
    path = _field_path(cursor["name"])
    return f"IS_DEFINED({path}) AND {path} != null"


def _sync_incremental(client: _Client, spec: dict, table_state: dict, state: dict):
    key = spec["region"]
    cursor = spec["cursor"]
    conditions = [_defined(cursor)]
    after = table_state.get("after")
    if after is not None:
        # Resume exactly after the last (cursor, key) delivered before the interruption.
        # Log only the cursor value: keys can be personal data.
        log.info(f"{key}: resuming incremental sync after {cursor['name']}={after[0]}")
    elif table_state.get("cursor") is None:
        log.info(f"{key}: initial incremental sync on {cursor['name']}")
    else:
        # >= (not >) so entries sharing the cursor value are never skipped, including entries written at
        # that value after the previous sync. Re-reads are harmless: upserts are keyed on the region key.
        conditions.append(f"{_field_path(cursor['name'])} >= {_literal(table_state['cursor'], cursor['kind'])}")
        log.info(f"{key}: incremental sync from {cursor['name']} >= {table_state['cursor']}")

    timer = _Timer(key)
    converter = _Converter(spec)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_entries(client, spec, cursor, conditions, after, timer, converter):
        _upsert(spec, record, timer)
        table_state["after"] = position
        total += 1
        _log_progress(timer, total, f", at {cursor['name']}={position[0]}")
        checkpointer.row_done()

    if "after" in table_state:
        # Region finished: keep the cursor value, drop the key so the next sync starts at >= cursor
        table_state["cursor"] = table_state.pop("after")[0]
    checkpointer.checkpoint()
    log.info(f"{key}: {total:,} rows synced, {cursor['name']}={table_state.get('cursor')}")
    timer.report_total("table total")

    # Geode: any entry can lack the field (or not be an object at all), so this runs for every region
    _sync_null_cursor_rows(client, spec, state, converter)
    converter.report(key)


def _sync_null_cursor_rows(client: _Client, spec: dict, state: dict, converter: _Converter):
    """Re-send entries whose cursor field is missing or NULL. The cursor can never select them, so read
    them by key every sync."""
    key = spec["region"]
    cursor = spec["cursor"]
    path = _field_path(cursor["name"])
    conditions = [f"(IS_UNDEFINED({path}) OR {path} = null)"]
    timer = _Timer(key)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, _ in _read_entries(client, spec, None, conditions, None, timer, converter):
        _upsert(spec, record, timer)
        total += 1
        _log_progress(timer, total, f" without {cursor['name']}")
        checkpointer.row_done()
    if total:
        checkpointer.checkpoint()
        log.info(f"{key}: {total:,} entries without a {cursor['name']} value re-sent (they are re-sent every sync)")
    timer.report_total(f"entries without {cursor['name']}")


def _sync_full(client: _Client, spec: dict, table_state: dict, state: dict):
    key = spec["region"]
    after = table_state.get("after")
    if after is None:
        # Applied only after update() returns successfully, so a failed sync deletes nothing. Rows
        # upserted after this call are kept; rows not sent again are marked deleted.
        op.truncate(table=spec["table"], deferred=True)
        log.info(f"{key}: full reload")
    else:
        # The deferred truncate from the interrupted sync still applies; calling it again would
        # mark the rows delivered before the interruption as deleted.
        log.info(f"{key}: resuming full reload from its last checkpoint")

    timer = _Timer(key)
    converter = _Converter(spec)
    checkpointer = _Checkpointer(state, spec["row_cap"], timer)
    total = 0
    for record, position in _read_entries(client, spec, None, [], after, timer, converter):
        _upsert(spec, record, timer)
        table_state["after"] = position
        total += 1
        _log_progress(timer, total)
        checkpointer.row_done()

    table_state.pop("after", None)
    checkpointer.checkpoint()
    log.info(f"{key}: full reload done, {total:,} rows")
    timer.report_total("table total")
    converter.report(key)


# ---------------------------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------------------------


def schema(configuration: dict):
    _validate_configuration(configuration)
    client = _Client(configuration)
    try:
        specs = _discover(client, configuration, verbose=False)
    finally:
        client.close()
    return [_table_definition(spec) for spec in specs]


def update(configuration: dict, state: dict):
    _validate_configuration(configuration)
    tables_state = state.setdefault("tables", {})
    client = _Client(configuration)
    try:
        specs = _discover(client, configuration)
        if not specs:
            op.warning("No regions matched the include_tables / exclude_tables settings")
            return
        incremental = sum(1 for s in specs if s["cursor"])
        log.info(f"Syncing {len(specs)} regions: {incremental} incremental, {len(specs) - incremental} full reload")
        for number, spec in enumerate(specs, 1):
            key = spec["region"]
            table_state = _table_state(spec, tables_state.get(key))
            tables_state[key] = table_state
            log.info(f"{key}: region {number} of {len(specs)} (types inferred from {spec['sampled']:,} sampled entries)")
            if spec["cursor"]:
                _sync_incremental(client, spec, table_state, state)
            else:
                _sync_full(client, spec, table_state, state)
    finally:
        client.close()
        _SAMPLE_CACHE.clear()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
