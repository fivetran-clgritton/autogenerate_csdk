# Exasol connector: porting notes (2026-11-22)

`exasol/connector.py` is a port of the IRIS template (`iris/connector.py`) and meets Connector
Requirements 1-7. It keeps the same config keys, state layout, checkpoint cadence, deferred
truncate, NULL-cursor re-send, strategy-change reset and drift warnings. Exasol-specific parts
are marked `Exasol:`.

Tested against Exasol 2026.1.2 (docker `exasol/docker-db`) with pyexasol 4.1.0 and
fivetran-connector-sdk 3.1.2.

## Design (what differs from IRIS)

- **Catalog:** `EXA_ALL_TABLES`, `EXA_ALL_COLUMNS` (`COLUMN_OBJECT_TYPE = 'TABLE'`), and
  `EXA_ALL_CONSTRAINT_COLUMNS` joined to `EXA_ALL_CONSTRAINTS` for primary keys. `SYS`,
  `EXA_STATISTICS` and virtual schemas (`EXA_ALL_VIRTUAL_SCHEMAS`) are excluded unless named in
  `schemas`. The base type is parsed from `COLUMN_TYPE` (e.g. `TIMESTAMP(3) WITH LOCAL TIME ZONE`).
- **Disabled primary keys count.** `PRIMARY KEY ... DISABLE` is common in Exasol (it is metadata
  only). The connector uses it and logs which tables rely on one. The database does not enforce it,
  so duplicate key values in the source collapse into one destination row.
- **Streamed reads, not small batches.** pyexasol reads through a server-side result-set handle:
  the server holds the result, and the client pulls 10 MB chunks by position (`FETCH_SIZE_BYTES`)
  and hands the loop 10,000 rows at a time (`FETCH_ROWS`). Each query is
  `ORDER BY cursor, pk LIMIT 500000` (`ROWS_PER_QUERY`); the next query continues after the last
  `(cursor, pk)` delivered. Exasol has no user-defined indexes, so each query sorts all remaining
  rows. Measured on `orders` (10M rows):
  | Approach | Query time | Client memory |
  |---|---|---|
  | One streamed query over all 10M rows | 4.0 s sort, then about 600k rows/s fetch | flat 125 MB |
  | `LIMIT 50000` batches | about 1.8 s per batch, about 6 min of sorting per 10M rows | about 100 MB |
  | `LIMIT 1000000` batches, each fetched whole (`fetchall`) | 1.3-2.2 s per batch | 946 MB |
  | `LIMIT 500000`, as the connector runs it | 1.5-3.5 s per query, about 20 queries per 10M rows | streamed (one chunk at a time) |
  500k per query was chosen (with the user) to keep each server-side result and the time its
  handle stays open small. The full sync below ran with 5M per query. The first 5M-row result
  handle stayed open for about 10 minutes of slow client fetching without problems.
- **No pyexasol `fetch_mapper`.** Values arrive as JSON and the connector converts them itself.
  Keyset positions and the stored cursor are the raw server strings
  (`'YYYY-MM-DD HH:MM:SS.fffffffff'`). A Python `datetime` keeps only microseconds, which would
  break `=` comparisons on `TIMESTAMP(9)` keys, and could loop or skip rows.
- **Session settings** on every connection: `TIME_ZONE = 'UTC'`,
  `NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'`, `NLS_DATE_FORMAT = 'YYYY-MM-DD'`,
  `NLS_NUMERIC_CHARACTERS = '.,'`. Keyset literals are `CAST('<raw>' AS <catalog type>)`, so they
  parse with those formats. The test DB's own zone is `EUROPE/BERLIN`.
- **No query parameters.** pyexasol formats parameters with `str.format`, which breaks on braces in
  quoted identifiers or values. The connector builds escaped literals itself; integers are inlined
  and decimal strings are validated against a numeric pattern.
- **No "cursor not indexed" warning:** Exasol creates indexes itself.
- **Name collisions:** quoted identifiers are case-sensitive, so `"Orders"` and `ORDERS` can both
  exist and would merge in the destination. Such tables, and tables with columns that collide the
  same way, are skipped with one `op.warning` (tested). The check approximates Fivetran's naming
  (lower snake_case, camelCase split).
- **Connection:** always encrypted. `validate_certificate=false` skips certificate checks (for
  self-signed certificates). Transient connection errors are retried 3 times with backoff (5 s,
  then 10 s). A bad password fails at once with a clear message. `socket_timeout` is 3600 s,
  because an `ORDER BY` over a large table runs before the first rows come back.

## Type mapping

| Exasol | Delivered as | Declared |
|---|---|---|
| `DECIMAL(p,0)`, p <= 18 (`INT`) | int | `LONG` |
| `DECIMAL(p,s)` otherwise (max p = 36) | str | `DECIMAL(p,s)` |
| `DOUBLE` | float | `DOUBLE` (see bug below) |
| `BOOLEAN` | bool | `BOOLEAN` |
| `DATE` | `date` | `NAIVE_DATE` |
| `TIMESTAMP(p)` | naive `datetime` (truncated to microseconds) | `NAIVE_DATETIME` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `datetime` in UTC | `UTC_DATETIME` |
| `CHAR`, `VARCHAR`, `INTERVAL ...`, `GEOMETRY` (WKT), `HASHTYPE` (hex) | str | inferred |

Cursor types are `TIMESTAMP`, `TIMESTAMP WITH LOCAL TIME ZONE`, `DATE` and `DECIMAL` with
scale 0.

**Bug found (may affect IRIS too):** an undeclared Python float is inferred by the SDK as 32-bit
`FLOAT`. In the test, `3.14159265358979` arrived as `3.1415927` and `-1e300` as `-inf`. Exasol now
declares `DOUBLE`. IRIS does not declare `DOUBLE`/`FLOAT` columns, so it probably has the same loss.
Not changed here (outside this database's scope).

## Tests run (test schema `FTTEST`)

Tables: `TYPESDEMO` (every Exasol type, plus a row with a NULL cursor), `STRPK` (string PK with
quotes, braces and unicode, `TIMESTAMP(9)` cursor with nanosecond ties), `COMPOSITEPK` (no cursor,
so full reload), `NOPK` (must be skipped), `odd "name" {x}` (quotes and braces in the table name,
lower-case quoted columns, a `TIMESTAMP WITH LOCAL TIME ZONE` cursor), plus `Collide`/`COLLIDE` and
`COLCOLLIDE` for the name-collision check.

| Test | Result |
|---|---|
| Initial sync; type fidelity of every column type | Passed after the `DOUBLE` fix. Local-time-zone value converted from Berlin to UTC correctly. |
| Keyless table | Skipped with one `op.warning` |
| NULL cursor rows | Re-sent every sync |
| Second sync: add column, drop column, update, NULL-cursor update, hard delete, row inserted at exactly the stored cursor value, delete from the full-reload table | Passed. Drift warning for the new column; dropped column NULL in new rows; `>=` picked up the boundary row; hard delete not seen by incremental (expected); full reload marked the deleted row `_fivetran_deleted` |
| `cursor_overrides`: STRPK forced to full reload, COMPOSITEPK to incremental on integer `A`, an unknown table name | Both strategy changes reset and re-synced; the full reload marked the earlier hard-deleted row deleted; unknown name produced a warning |
| Resume from hand-written `after`: STRPK mid-tie `('...000000002', "it's")`, COMPOSITEPK full reload after `(1, 'x')` | Exact: 4 rows and 1 row; no second truncate |
| Multiple queries (scratch copy, `ROWS_PER_QUERY = 2`) with nanosecond-tied cursors | All rows delivered; ties split across queries; quoted and braced string keys worked as boundaries |
| Name collisions, `exclude_tables` resolving one | Skipped with a warning; synced once excluded |
| Wrong password; closed port | Immediate clear failure; 3 attempts with backoff, then a clear failure |
| Resume query log | Keyset values replaced by `<after the last row delivered>` (primary keys can be personal data) |

## Full initial sync (seeded `TESTDB`: 1M customers, 10M orders)

Run under the shared lock with `--naming SOURCE` and `PYTHONUNBUFFERED=1`, at 5M rows per query:

- **SYNC SUCCEEDED: 11,000,000 upserts in 22 min 9 s, 112 checkpoints.**
- **Peak memory 1.60 GB** (SDK report; average 1.06 GB). Sampled process-tree RSS peaked at 1.70 GB.
- `CUSTOMERS`: 95 s. `ORDERS`: 1,233 s = query 4.5 s, fetch 17.7 s, convert 219 s, upsert 761 s,
  checkpoint 208 s; GC 406 s (700 full collections).
- Exasol itself takes less than 2% of the time. Upsert and checkpoint (the SDK) take about 80%.
  Time per 250k rows rose from 26 s to 32+ s as GC time grew from 3 s to 12 s per 250k rows. This
  is the same heap-growth pattern IRIS showed, most likely the debug runner keeping every upserted
  key.
- Content comparison against the source: **all checks passed**.
  - Counts, `_fivetran_deleted` all false, distinct ids.
  - Sums: `ID`, `CUSTOMER_ID`, `AMOUNT` (25,000,773,486.26).
  - Min, max and non-null count of `CREATED_AT` and `UPDATED_AT`.
  - Per-status counts.
  - A 2,000-row field-by-field sample per table, with 0 mismatches.
- Destination types: `ID`/`CUSTOMER_ID` `BIGINT`, `AMOUNT` `DECIMAL(10,2)`, timestamps `TIMESTAMP`.
- A follow-up sync from the saved state re-read only the row at each cursor value (2 upserts, 0.7 s).

## Changes made outside the connector

- **Added disabled primary keys to the seeded tables** (approved by the user):
  `ALTER TABLE TESTDB.CUSTOMERS ADD CONSTRAINT CUSTOMERS_PK PRIMARY KEY (ID) DISABLE` and the same
  for `ORDERS` (`ORDERS_PK`). This is metadata only; no data was changed. The seeder creates both
  tables without a key, so the connector would skip them. **`seed_data.py` should create the key**
  (e.g. `id INT, ..., PRIMARY KEY (id) DISABLE` or an enabled key). Otherwise the next reseed
  drops it and the connector syncs nothing from `TESTDB`.
- Created the test schema `FTTEST` (tables listed above). It is not part of the seeded data.

## Quirks

- Unquoted identifiers are upper-cased (`TESTDB`, `CUSTOMERS`, `UPDATED_AT`). Without
  `--naming SOURCE`, Fivetran's naming lower-cases them.
- An empty string `''` is stored as NULL (Exasol semantics).
- `CHAR(n)` values keep their trailing-space padding.
- `TIMESTAMP` defaults to `TIMESTAMP(3)`. Precision up to 9 is supported. Destination values keep
  microseconds, but the keyset and cursor keep all 9 digits.
- `DECIMAL` precision is at most 36.
- pyexasol's `exasol_mapper` is deprecated in 4.x (in favour of
  `pyexasol.data_types.convert_websocket_to_python`). The connector uses neither.
- The test container listens with TLS on 8563 (self-signed certificate), so tests use
  `validate_certificate=false`.

## Deployment / open questions

- **Dependencies:** pyexasol is pure Python (websocket-client, rsa, packaging,
  exasol-telemetry-client), so no custom driver mechanism is needed. `requirements.txt` pins
  `pyexasol==4.1.0`. Check that the Fivetran runtime's Python version satisfies pyexasol 4.x.
- **Disabled primary keys** with duplicate or NULL key values collapse or misbehave. The connector
  logs which tables rely on one, but does not check uniqueness. A `COUNT(*)` vs
  `COUNT(DISTINCT pk)` check per table would cost a scan each sync; not done.
- **Very large tables:** every query re-sorts the remaining rows, so a table of hundreds of millions
  of rows pays one sort of the remaining rows per 500k delivered: O(n²/500k) in total (one sort of
  10M rows takes about 4 s; not measured at larger sizes). If that becomes a problem, scale
  `ROWS_PER_QUERY` with table size (`EXA_ALL_TABLES.TABLE_ROW_COUNT` is available), trading it
  against how long a server-side result stays open.
- No `configuration_form` (optional, and the IRIS template has none either).
- The 12 test scenarios in `test_scenarios.sql` have not been run.
- `/evaluate-connector`: no required SDK violations remain. Issues it raised and that are fixed:
  keyset values in logs, connection retry and clear errors, destination-name collisions. Remaining
  good-to-have items: a setup form, and long functions (`_discover`, `_read_rows`).
