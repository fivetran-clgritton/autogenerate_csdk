# CUBRID connector: porting notes

Ported from `iris/connector.py` (branch `cubrid-general`). Tested against the VM's CUBRID 11.3.6
(`cubrid/cubrid:11.3`, database `testdb`, database charset iso88591, session time zone UTC).

## Design differences from the IRIS template

| Area | IRIS | CUBRID | Why |
|------|------|--------|-----|
| Driver | sqlalchemy-iris | CUBRID JDBC jar through **JPype** (`JPype1==1.7.1`), no jaydebeapi | jaydebeapi's converters corrupt data (see Quirks) |
| Reads | `SELECT TOP n` batches, `fetchall()` | One streamed query (`setFetchSize`); primary-key ordered reads are split into `LIMIT 500000` chunks | The driver streams with flat memory, and the server materializes each query's whole result before the first row |
| First sync of an incremental table | `ORDER BY cursor, pk` keyset batches | Whole table in **primary-key order**, then cursor = `MAX(cursor)` captured **before** the read | Sorting 10M rows by the unindexed `updated_at` gave no first row after 25 min (default 2 MB `sort_buffer_size`). The PK index returns rows in key order without a sort |
| Later incremental syncs | batches on `(cursor, pk)` | One query `cursor >= last ORDER BY cursor, pk` (sort covers only the changed rows) | Chunks would repeat the sort of the remaining delta every time |
| Identifier quoting | `"name"` | `[name]` | Double quotes delimit identifiers only with `ansi_quotes=yes` |
| String keyset values | bound parameters | bound if ASCII; otherwise rebuilt server-side from their bytes as `CONCAT(CHR(n USING <column charset>), ...)` | See Quirks: bound non-ASCII strings do not compare equal to themselves |

State layout is the template's (`{"tables": {"<owner>.<table>": {"mode", "cursor_column", "cursor", "after", "columns"}}}`)
plus `high_water` while the first incremental sync is in progress (`after` then holds primary-key
values only). Schema = table owner (`DBA`, `FTTEST`, ...); names are owner-qualified from CUBRID 11.2.
For older servers the connector uses bare table names (not tested: no pre-11.2 server available).

Catalog: tables from `db_class` (`class_type='CLASS'`, `is_system_class='NO'`, partitions excluded
via `db_partition`), columns from `db_attribute` (`attr_type='INSTANCE'`, with `charset`), primary keys
from `db_index` + `db_index_key` (key order), leading index columns from `db_index_key.key_order = 0`.
No owner is excluded by name: system catalogs are flagged by `is_system_class`.

## Type mapping (verified value by value)

| CUBRID | Read with | Delivered / declared |
|--------|-----------|----------------------|
| SHORT, INTEGER, BIGINT | getString → int | inferred (LONG) |
| NUMERIC(p,s) | getString (plain notation; `E` normalised) | DECIMAL(p,s) up to 38, else string |
| FLOAT / DOUBLE, MONETARY | getString → float | FLOAT / DOUBLE |
| DATE | getString → date | NAIVE_DATE |
| TIME | getString (`HH:MM:SS`) | string (SDK has no time type) |
| TIMESTAMP, DATETIME | getString → datetime | NAIVE_DATETIME |
| TIMESTAMPTZ/LTZ, DATETIMETZ/LTZ | `TO_CHAR(col, '... TZH:TZM')` → aware → UTC | UTC_DATETIME |
| BIT, BIT VARYING, BLOB | getBytes | BINARY |
| CHAR/VARCHAR with charset `binary` | getString (spaced hex) → bytes | BINARY |
| CHAR, VARCHAR/STRING, ENUM, JSON, CLOB | getString | string (CHAR keeps its padding; JSON kept as text, lossless) |
| SET, MULTISET, SEQUENCE | getObject (Java array) → list | JSON |

Zero dates (`'0000-00-00'`) arrive as NULL (`zeroDateTimeBehavior=convertToNull`; without it the
driver throws while fetching the batch).

## Quirks found

- **jaydebeapi corrupts values.** `_to_datetime` appends `str(getNanos())[:6]` without zero padding,
  so 5 ms becomes 0.5 s (any millisecond value below 100 is wrong). `_to_decimal` turns NUMERIC with a
  scale into a float. The old connector used it; this one does not.
- **Zoned types over JDBC:** `getTimestamp` drops the zone (`12:00 +05:30` came back as 12:00 UTC) and
  `getString` appends zone names (`... America/New_York EDT`). `TO_CHAR` with `TZH:TZM` is lossless.
  Zoned cursors are bound back as `CAST(? AS DATETIMETZ)` and compare as instants.
- **Bound non-ASCII strings:** the driver sends parameters as the database charset. In this iso88591
  database a bound `'ü'` did not equal the stored `'ü'` in a utf8 column, so `code > 'ü'` returned
  `'ü'` again (re-reads; in other cases it could skip rows). Neither `charset=utf-8` nor `charSet=...`
  fixes utf8 columns (`charset=ISO-8859-1` fixes iso88591 columns only). `CHR(n USING charset)` with
  the value's exact bytes works for utf8 (including 4-byte characters), iso88591, euckr and binary,
  and comparisons still use the column collation (tested with `utf8_en_ci`). There is no
  `_utf8 X'..'` or `UNHEX` in CUBRID.
- **Reads decode by column charset** regardless of the connection charset, so values come back as
  stored. Rows the seeder/clients wrote as UTF-8 bytes into iso88591 columns read back as mojibake
  (`'héllo'` → `'hÃ©llo'`); that is the stored data, not a driver error. Open question: offer an
  option to re-decode iso88591 columns as UTF-8 (a common legacy setup).
- **ENUM keys:** `ORDER BY` uses the declaration index but `>` compares text, so keyset paging on an
  ENUM would skip/repeat rows. Tables whose primary key includes an ENUM (or LOB/collection/JSON) are
  skipped with one dashboard warning.
- **Abandoned queries keep running.** Killing the client did not stop a running `ORDER BY` on the
  server; it had to be killed with `cubrid killtran -i <n> -p <dba password> testdb`.
- `CREATE USER` creates a schema; DBA can create `owner.table` directly (11.2+).
- The compose `CUBRID_PASSWORD` does not set the dba password (already noted in CLAUDE.md).

## Tests run (all with `fivetran debug --naming SOURCE`, test schema `FTTEST`)

Edge-case tables: `typesdemo` (every type above, NULLs, 38-digit NUMERIC, zero NUMERIC, year 0001,
millisecond values below 100), `strpk` (VARCHAR `utf8_en_ci` key, mixed case, `'ü'`, NULL cursors,
tied cursors), `compositepk` (INT + VARCHAR), `nopk`, `fullreload` (no cursor), `batchtest`
(25 rows, 4 distinct cursor values, NULLs), `unicodepk` (iso88591 non-ASCII + utf8 4-byte keys,
composite), `binkey` (binary-charset key), `charsets` (euckr, binary), `enumpk`.

| Test | Result |
|------|--------|
| Initial sync | 4 tables; `nopk` skipped with one warning; deferred truncate on the full-reload table; every value checked in DuckDB (zoned values converted to UTC correctly, e.g. `12:00 +05:30` → 06:30Z) |
| Second sync after ADD COLUMN, DROP COLUMN, UPDATE, DELETE, INSERT at the cursor value | new column delivered + drift warning; dropped column NULL from then on; tie at the cursor picked up (`>=`); full reload marked the deleted row `_fivetran_deleted`; NULL-cursor rows re-sent. Hard delete in an incremental table is not detected (expected) |
| `cursor_overrides` strategy change | incremental→full, full→incremental on an INTEGER cursor, cursor switched to a DATETIMETZ column; each reset its table; next sync incremental on both (`id >= 3`; zoned cursor compared as instants) |
| Multi-chunk (scratch copy: chunk 2 rows, fetch 2, checkpoint every 3 rows) | 38 upserts for 38 rows across 2-row chunks (no repeats/gaps), incl. the case-insensitive key; incremental with ties: 17/17 rows; with non-ASCII/binary keys 22 upserts for 22 rows |
| Resume from hand-written `after` | `(updated_at, id)` resume: 11/11 expected rows; first-sync resume on the ci string key after `'B'`: 6/6; full-reload resume: no second truncate, 2/2; non-ASCII composite key: 5/5; binary key: 1/1 |
| Bad password | `Could not connect to CUBRID at localhost: Incorrect or missing password` (password masked) |
| Streaming memory (raw JDBC, 2M orders rows, fetch 10,000) | RSS flat at 287 MB, ~40k rows/s for 6 columns via getString |

## Full sync of the seeded tables (`DBA.customers` 1M + `DBA.orders` 10M)

`fivetran debug --naming SOURCE`, `PYTHONUNBUFFERED=1`, empty state, under the VM-wide lock, 14 GB free at start.

| Run | Code | Total | customers | orders | Peak memory | Result |
|-----|------|-------|-----------|--------|-------------|--------|
| 1 | `10fc130` | 1 h 29 min | 111 s | 87 min (5,242 s) | 1,354 MB (avg 1,021) | 11,000,000 upserts, 119 checkpoints, SUCCEEDED |
| 2 | `ebc982b` (gc freeze) | **26.5 min** | 137 s | 24 min (1,446 s) | 1,395 MB (avg 907) | 11,000,000 upserts, 112 checkpoints, SUCCEEDED |

Run 1 orders breakdown: fetch 4,362 s (**of which gc 4,207 s, 7,539 full collections**), upsert 613 s,
checkpoint 176 s, convert 59 s, query 12 s (21 chunk queries of 500k rows, about 0.6 s each: the
primary-key index means no sort). Without GC, orders took about 17 min.

Run 2 orders breakdown: upsert 607 s, fetch 578 s (of which gc 423 s, 9,911 full collections), checkpoint
174 s (includes the `gc.collect()`/`gc.freeze()`), convert 57 s, query 12 s. There are still about 250
forced full collections per 250k rows, but each takes about 0.04 s and **stays flat** (gc 8.4 s per 250k rows
at the start, 10.0 s at 9M rows) instead of growing to 187 s. Customers gc rose from 11 s to 37 s because the
first collections happen before anything is frozen. Peak memory is the same. At exit the debug runner prints
`nanobind: leaked N instances` for DuckDB objects (they were frozen, so shutdown doesn't collect them).
That's harmless: it's the local runner's DuckDB module, not the connector.

**GC cause (JPype):** `jpype.startJVM` adds two `_collect` callbacks to `gc.callbacks`, linking Python
and Java garbage collection, and while rows are fetched they keep forcing full Python collections
(about 190 per 250k rows here, versus 20 on customers early on). A full collection walks every
long-lived object, and in `fivetran debug` that includes the runner's set of every upserted key, so
each one got slower as the sync went on (0.13 s per collection at the start of orders, 0.97 s by 10M rows). Reproduced in
isolation (`fetch 250k orders rows with 5M long-lived tuples in the heap`): 192 full collections,
94.6 s; with `gc.freeze()` beforehand, 0.0 s. Plain Python allocation with the same heap caused no full
collections, and Java young GCs (7 per 250k rows with `-Xmn256m`) are not what triggers them.
Fix (`ebc982b`): after each checkpoint the connector runs `gc.collect()` then `gc.freeze()`, so objects
that survived are not walked again. Production may hold less in memory than the debug runner, but
JPype's forced collections happen either way.

Content comparison (`compare.py` in the session scratchpad: JPype on the source, DuckDB on
`warehouse.db`, destination rows with `NOT _fivetran_deleted`). Run 1 and run 2 gave the same output,
ALL CHECKS PASSED:

| Check | customers | orders |
|-------|-----------|--------|
| count, distinct ids | 1,000,000 / 1,000,000 OK | 10,000,000 / 10,000,000 OK |
| sums | id 500000500000 OK | id 50000005000000, customer_id 5000160757611, amount 25000773486.26 OK |
| min/max created_at, updated_at | OK (2022-01-01 00:01:15 … 2024-12-30 23:59:46) | OK (… updated_at max 2025-01-02 23:40:04) |
| per-status counts | 3 statuses OK | 6 statuses OK |
| NULL counts per column | all 0, OK | all 0, OK |
| field-by-field sample (`id % 500 = 7` / `id % 5000 = 7`) | 2,000 rows, 0 differ | 2,000 rows, 0 differ |

Final state: `cursor` = the `MAX(updated_at)` captured before each table's read (2024-12-30
23:59:46.000 and 2025-01-02 23:40:04.000); no `after` / `high_water` left behind.

The old CLAUDE.md open question (a run with 9,548 upserts instead of 9,989) was with the old
jaydebeapi connector. This one delivered exactly 11,000,000 upserts for 11,000,000 rows.

## Production deployment

Needs a Java runtime and the CUBRID JDBC jar in the Fivetran container. Neither is in the runtime, so
deployment needs the SDK's custom-driver mechanism (`drivers/installation.sh`, Private Preview):
install a headless JRE (e.g. `openjdk-17-jre-headless`) and put the jar at the path given in
`jdbc_jar`. JPype finds the JVM through `JAVA_HOME` or the default system JVM. Not tried yet.
The JVM is started with `-Xmx512m`; its memory counts toward the 4 GB limit.

## Open questions / not done

- `gc.freeze()` is process-wide. It's safe here (the connector owns the process), but if a later SDK
  version keeps long-lived cyclic garbage it would stay frozen until the process exits.

- 12 test scenarios in `test_scenarios.sql` not run.
- CUBRID before 11.2 (unqualified names) untested.
- A first sync captures `MAX(cursor)` with one full scan when the cursor is unindexed.
- Long-running queries hold an MVCC snapshot for the whole read (primary-key reads are chunked to
  500k rows; the later-incremental query is one query).
- No retry on transient connection drops (the next sync resumes from the last checkpoint).
- No `configuration_form`.
- Option to re-decode iso88591 columns that hold UTF-8 bytes.
