# H2 connector notes

Port of the IRIS template (`iris/connector.py`) to H2 2.x over the TCP server. Tested on the VM
against the `oscarfonts/h2` container (H2 2.1.214, database `test`, port 9092) on 2026-11-22.

## Status

Meets Connector Requirements 1-7, passed the edge-case tests below, and passed a full initial sync
of the seeded `PUBLIC.CUSTOMERS` (1M) and `PUBLIC.ORDERS` (10M) with a content comparison against
the source (everything matched).
The 12 test scenarios in `test_scenarios.sql` have not been run. Not deployed to Fivetran: the
runtime has no Java (see **Deploying**).

## How it differs from the IRIS template

### Reads in primary-key order, with a cursor high-water mark

IRIS reads `cursor >= last` ordered by `(cursor, primary key)`. On H2 that order is unusable for
large tables without an index on the cursor: H2 sorts by spilling the result to temporary files,
and `SELECT ... FROM ORDERS ORDER BY UPDATED_AT, ID` (10M rows) ran for more than 25 minutes
without returning a row (16 GB of disk I/O, 3.8 GB of temp files) before I cancelled it. A
`LIMIT` batch in that order pays for the same sort on every batch.

Instead, each incremental sync:

1. Records `high_water = MAX(cursor)` (with `WHERE cursor >= last` after the first sync). 7 s on
   10M rows without an index; instant with one.
2. Reads `cursor >= last` (no condition on the first sync) in bounded batches ordered by the
   primary key, with keyset paging on the key: `... ORDER BY pk LIMIT 50000`. H2 walks the
   primary-key index and stops after `LIMIT` rows ("index sorted" in `EXPLAIN`): 0.07 s per query,
   about 1 s to fetch 50k rows, at any position in the table.
3. When the table is done, stores `high_water` as the next sync's lower bound.

The mark is taken before the read, so a row that changes during the read gets a cursor at or above
it and is read by the next sync (`>=`). The trade-off: every incremental sync of a table without
an index on the cursor scans the table twice (MAX and the filtered read), about 7 + 9 s for 10M
rows, even when nothing changed. Both strategies miss rows written with a backdated cursor value
(tested: a row inserted with `updated_at` in 2020 was not picked up; a full reload picks it up).

State per table: `{"mode", "cursor_column", "cursor", "high_water", "after", "columns"}`. `after`
holds primary-key values only (not `[cursor, pk...]` as in IRIS); `high_water` exists only while a
pass is in progress. A resumed pass keeps its `cursor` and `high_water` and continues after
`after`. Keyset values are stored as H2's own text (exact for nanosecond timestamps and decimals;
binary keys as hex).

### Index hints (`USE INDEX`)

H2's planner picks an index on a filtered column whenever one exists, however many rows match.
With an index on `updated_at`, `WHERE updated_at >= '2022-01-01' ORDER BY id LIMIT 50000` (every
row matches) used the cursor index and then had to sort all matches on every batch: the same sort
problem. So every read names its index:

- the primary-key index by default;
- the cursor index only when a `COUNT(*)` through that index shows the delta fits in one batch.
  Tested on a 1M-row copy of customers with an index on `updated_at`: a 4-row delta and 2 NULL rows
  were read through the cursor index (0.0 s); after a 200k-row bulk update the delta was read in
  primary-key order.

NULL cursors: without a cursor index the NULL rows are read in the same pass
(`cursor >= last OR cursor IS NULL`, which also keeps H2 on the primary-key index). With a cursor
index they get their own pass (`cursor IS NULL`, same count-based index choice), as in IRIS.

For composite keys the keyset condition gets a redundant leading `first_col >= value` so H2 starts
the index range at the right place (checked with `EXPLAIN`).

### Driver: JPype directly instead of jaydebeapi

jaydebeapi converts `DECIMAL`/`NUMERIC` through `double` (precision loss) and timestamps through
`getTimestamp`, which shifts values through the JVM's default time zone (and moves times in a DST
gap). The connector starts the JVM with JPype (`-Xmx512m`, `convertStrings=True`) and reads every
value with `getString` (or `getBytes` for binary types): one JNI call per value, NULL detection
for free, and H2's exact SQL text for every type. Measured about 20 µs per 6-column row for the
fetch. Parameters are bound as strings with `CAST(? AS <column type>)` so comparisons use the
column's own type and collation (matters for `VARCHAR_IGNORECASE` and `CHAR(n)` keys).

Requirements: `JPype1==1.7.1` and a JVM. The jar comes from `jar_path` (configuration),
`H2_JAR` (environment) or `drivers/h2.jar` next to `connector.py`. The connector never downloads
it. The `fivetran debug` runs used `jar_path=/opt/sdk-test-dbs/jars/h2.jar`.

### Declared types (applies to the other connectors too)

The SDK infers the type of an undeclared column from the value, and the local tester mirrors the
Java side's rules (`fivetran_connector_sdk_core/cells.py`, `_text_type`):

- An undeclared Python `float` is coerced as a 32-bit FLOAT: DOUBLE PRECISION `3.141592653589793`
  arrived as `3.1415927410125732`, `1e-10` as `1.000000013351432e-10`.
- An undeclared string is typed from its text: `'t'`/`'false'` become BOOLEAN, `'123'` an integer,
  `'2024-01-01'` a DATE, `'1.5'` a DOUBLE. A NUMERIC(50,10) delivered as a string became DOUBLE.

So the H2 connector declares every column except integers: DOUBLE PRECISION as `DOUBLE`, REAL as
`FLOAT`, character and other text-like types (DECFLOAT, TIME, INTERVAL, UUID, ENUM, ARRAY, ROW,
GEOMETRY) as `STRING`, NUMERIC up to precision 38 as `DECIMAL(p,s)` and wider ones as `STRING`,
JSON as `JSON` (parsed with `json.loads`). New columns still appear because `schema()` is rebuilt
from the catalog on every sync. **IRIS (the template) and the other ports have the same exposure:
IRIS leaves DOUBLE and VARCHAR columns undeclared.**

### Smaller differences

- `IFEXISTS=TRUE` in the URL: the test server runs with `-ifNotExists`, so a typo in the database
  name would otherwise create an empty database (and sync nothing, successfully).
- `host` and `database` are validated against strict patterns: the JDBC URL accepts `;SETTING=...`
  suffixes, including `INIT=` (runs SQL on connect).
- Connection retries only for H2 error 90067 (connection refused/timed out/broken; H2 reports
  these as `SQLNonTransientConnectionException`, so the class alone can't decide). A wrong database
  or password fails at once.
- System schemas: only `INFORMATION_SCHEMA` exists in H2 2.x.
- Unquoted identifiers are upper-cased (`PUBLIC.CUSTOMERS`, `UPDATED_AT`); configuration matching
  is case-insensitive, and `--naming SOURCE` keeps the upper-case names locally.

## Type mapping (verified on `FTTEST.TYPESDEMO`)

| H2 type | Delivered as | Destination (local) |
|---------|--------------|---------------------|
| TINYINT, SMALLINT, INTEGER, BIGINT | int | BIGINT |
| NUMERIC(p,s), p ≤ 38 | exact string, declared DECIMAL(p,s) | DECIMAL(p,s) |
| NUMERIC(p>38), DECFLOAT | exact string, STRING | VARCHAR |
| REAL / DOUBLE PRECISION | float, FLOAT / DOUBLE | FLOAT / DOUBLE (NaN stored as NULL by the SDK) |
| BOOLEAN | bool | BOOLEAN |
| CHAR(n) (padded), VARCHAR, VARCHAR_IGNORECASE, CLOB | str | VARCHAR |
| BINARY, VARBINARY, BLOB, JAVA_OBJECT | bytes | BLOB |
| DATE | date | DATE |
| TIMESTAMP(p) | naive datetime (fractions beyond µs truncated) | TIMESTAMP |
| TIMESTAMP WITH TIME ZONE | datetime converted to UTC | TIMESTAMP WITH TIME ZONE |
| TIME, TIME WITH TIME ZONE, INTERVAL, UUID, ENUM, ARRAY, ROW, GEOMETRY | H2's text, e.g. `'[1, 2, 3]'`, `"INTERVAL '1 02:03:04' DAY TO SECOND"` | VARCHAR |
| JSON | parsed JSON | JSON |

Edge values checked: max BIGINT, 40-digit NUMERIC, `1.5E+300` DECFLOAT, `-0.0001`, `0.0000001`
(plain notation from H2, no exponent), a DST-gap timestamp `2024-03-10 02:30:00.123456`, year 0001,
9999-12-31, `+02:00` zone converted to UTC, Infinity / NaN, multibyte UTF-8.

## Tests run (test schema `FTTEST`)

Tables: `TYPESDEMO` (every column type, nullable cursor with NULLs), `COMPOSITEPK` (3-column
string/int key with mixed-case and non-ASCII values), `NOPK`, `"MixedCase"` (quoted mixed-case
table and column names, case-sensitive string PK `k1`/`K1`), `NUMTEST`, `TIES` (11 rows, cursor
values tied in groups of 3), `CUSTIDX` (1M-row copy of `PUBLIC.CUSTOMERS` with an index on
`UPDATED_AT`).

| Test | Result |
|------|--------|
| First sync | 4 tables, NOPK skipped with one `op.warning`; incremental for tables with `UPDATED_AT`, full reload otherwise; all values as in the type table |
| Second sync after add column, drop column, update, delete, insert at the high-water mark, NULL-cursor row change | Updated row, tied row and NULL-cursor row re-sent; `op.warning` for the new column; dropped column logged and NULL in re-sent rows; full-reload table marked the deleted row `_fivetran_deleted`. Hard deletes in incremental tables are not detected (expected) |
| `cursor_overrides` (`FTTEST.TYPESDEMO=` and `fttest.mixedcase=Some Col`) | Both strategy changes logged and the tables restarted; the full reload picked up the backdated row and marked the deleted one |
| Multi-batch, `BATCH_SIZE = 2`, checkpoint every 3 rows | TIES 11/11 and COMPOSITEPK 12/12 rows, mid-table checkpoints with `after` and `high_water` |
| Composite key + lower bound + several batches | 11/11 rows. Added after code review found a parameter-order bug in the first version of the keyset condition (multi-column key plus a cursor condition), which the earlier tests did not exercise |
| Resume from hand-written state (`after: [7]` on TIES, `after: ["EU","a","3"]` on COMPOSITEPK) | Exactly the 4 and 9 rows after the position |
| Indexed cursor (CUSTIDX) | 1M-row initial sync 117 s, peak 508 MB; small delta and NULL rows through the cursor index; 200k-row delta in primary-key order |
| Connection errors | Wrong database: fails at once with H2's message. Refused port: retried twice (10 s, 20 s), then fails |

## Full initial sync of the seeded tables

`flock ~/.full-sync.lock fivetran debug --naming SOURCE` with `PYTHONUNBUFFERED=1`, schemas
`PUBLIC`, from empty state, on 2026-11-22 (14 GB available at the start).

| | CUSTOMERS (1M) | ORDERS (10M) |
|---|---|---|
| Wall time | 104 s | 1,941 s (32 min) |
| MAX(UPDATED_AT) | 0.9 s | 6.0 s |
| Queries | 21, avg 0.05 s | 201, avg 0.04 s, slowest 0.33 s |
| fetch / convert / upsert / checkpoint | 19 / 7 / 59 / 17 s | 842 / 179 / 719 / 174 s |
| of which GC | 12 s | 1,010 s (1,532 full collections) |

Whole sync: 34 min 13 s, 11,000,000 upserts, 112 checkpoints, **peak 1.40 GB** (average 0.99 GB),
as reported by the SDK for the whole process (Python + the in-process JVM).

Query time stays flat (each primary-key batch is an index range, not a scan or sort). The time per
250k orders rose from 26 s to 62 s over the run, almost entirely GC: 4 s per 250k rows at the
start, 39 s at the end, inside `fetch` and `convert` (the stages that allocate). Same pattern as
IRIS, where it was attributed to the local debug runner keeping a set of every upserted key, which
makes each full collection slower as the heap grows. Not mitigated in the connector.

Content comparison against the source (script `compare.py` in the session scratchpad, not
committed), all equal for both tables: row counts, distinct ids, `SUM(ID)`, `SUM(CUSTOMER_ID)`,
`SUM(AMOUNT)` (25,000,773,486.26), summed string lengths and NULL counts of NAME / EMAIL / STATUS,
MIN / MAX / COUNT of CREATED_AT and UPDATED_AT, per-status counts, and a field-by-field comparison
of 2,000 random rows per table (0 mismatches).

A following incremental sync with no changes took 19 s: CUSTOMERS 1 + 1 s, ORDERS 7.8 s (MAX) +
8 s (filtered scan), 1 row re-read per table (the row at the high-water mark).

## Performance notes

- 1M rows of customers (6 columns): about 117 s, of which upsert 52% (SDK), fetch 25%, checkpoint
  15%, convert 5%, query 1%. GC time per 250k rows grew from 3.5 s to 7.1 s over the run (the
  same pattern IRIS showed, attributed to the debug runner's set of upserted keys).
- `LAZY_QUERY_EXECUTION=1` makes no difference for these reads (the primary-key batches are
  bounded and index-ordered either way). Without it, an unbounded `SELECT ... ORDER BY ID` took
  11.6 s to execute because the server materializes the whole result first; with it, 0.05 s. Not
  used, since the connector never runs unbounded queries.
- Fetch size 2,000 (H2's default is 100): 23 vs 27 µs per row.

## Quirks found

- **A killed client does not stop the query on the server.** After the 25-minute sort benchmark
  was killed, the H2 server kept sorting (session still listed in `INFORMATION_SCHEMA.SESSIONS`,
  CPU busy). `SELECT CANCEL_SESSION(<id>)` as an admin requests cancellation, but the sort only
  noticed it at its next check. The connector's queries are short, so this matters only for manual
  experiments.
- The `oscarfonts/h2` server runs Java 11 with H2's default 16 MB page cache; the 10M-row table is
  a 1.35 GB file.
- The container's `h2.jar` (2.1.214) has a different checksum from Maven Central's 2.1.214 jar
  (SHA-1 `0614b952...` vs `d5c2005c...`); both identify as 2.1.214.

## Deploying (not done)

The Fivetran runtime has no Java. Deploying needs the Custom Database Drivers mechanism (Private
Preview): a `drivers/installation.sh` that installs a headless JRE (11+) and puts the H2 jar
(matching the customer's server version) at `drivers/h2.jar`, or ships the jar in `drivers/`.
JPype finds the JVM through `JAVA_HOME` or the default locations. Untested: whether
`installation.sh` can install system packages, and JPype's JVM startup inside the runtime.

## Open questions

- Production behaviour of the declared types (the local tester says it mirrors the Java side).
- Whether to add a `configuration_form` (the evaluation flags it as optional).
- H2 embedded / file mode (`jdbc:h2:file:`) is not supported, only the TCP server.
