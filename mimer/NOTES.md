# Mimer SQL connector: porting notes (2026-11-22)

`mimer/connector.py` is a general connector ported from the IRIS template. It meets Connector
Requirements 1-7 in CLAUDE.md. The 12 test scenarios in `test_scenarios.sql` have not been run.

## Design

- **Catalog**: `INFORMATION_SCHEMA.TABLES` (`BASE TABLE`), `COLUMNS` (with `DATETIME_PRECISION` and
  `USER_DEFINED_TYPE_*`), `TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` for primary keys, and
  `EXT_ACCESS_PATHS` (Mimer extension: primary keys, unique constraints and indexes) for the
  "cursor not indexed" warning. Default excluded schemas: `SYSTEM`, `INFORMATION_SCHEMA`, `MIMER`,
  `ODBC`, `BUILTIN`.
- **Reads**: one server-side cursor per table pass instead of `FETCH FIRST n` batches. mimerpy's
  default cursor is forward-only (`MimerOpenCursor` / `MimerFetch`). The database sorts once and
  the client holds one `fetchmany()` batch (10,000 rows, 100 for LOB tables). Resume uses the same
  `(cursor, pk) > after` keyset condition as IRIS.
- **Fetch array size**: mimerpy never calls `MimerSetArraySize`, so it fetches **one row per
  network round trip** (about 11k rows/s). The connector calls the C function through ctypes on the
  cursor's private statement handle (`cursor._Cursor__statement`, `mimerapi._lib`): 1,000 rows per
  round trip (100 for LOB tables) gives about 63k rows/s, with identical data (checked by hashing
  300k ordered rows plus the LOB / all-types table). Because this depends on private attributes,
  `requirements.txt` pins `mimerpy==1.3.11`. If the call fails, the sync continues slower and
  raises an `op.warning`.
- **Transactions**: `connect(readonly=True)`. Read-only transactions see a snapshot and never block
  or conflict with writers (tested: a writer committed in 2 ms while a read cursor was open on the
  same table, and the reader kept seeing the old value). The connector commits after each table
  pass to release the snapshot.
- **Connection**: the Mimer client takes a database name, not host:port, and resolves it through
  the SQLHOSTS file named by `MIMER_SQLHOSTS`. The connector writes its own file to a temp
  directory from the `host` / `port` / `database` settings, so no client setup is needed. The name
  must be the **server's** database name (`mimerdb`); an alias gives error -18501. `host` and
  `database` are validated as single tokens before they go into the file. The client waits about
  130 s (the OS connect timeout) on an unreachable host, so the connector first opens a TCP socket
  with a 30 s timeout and fails with a clear message.

## Driver and type findings

| Mimer type | `DATA_TYPE` | Python value from mimerpy | Delivered as / declared |
|---|---|---|---|
| SMALLINT / INTEGER / BIGINT | as named | int | inferred (LONG) |
| DECIMAL / NUMERIC (p ≤ 38) | `DECIMAL` | Decimal | DECIMAL(p,s) |
| DECIMAL (p 39-45) | `DECIMAL` | Decimal | **STRING** (declared; inference made it DOUBLE and lost digits) |
| DOUBLE PRECISION / FLOAT | `DOUBLE PRECISION` | float | **DOUBLE** (declared; inference made it 32-bit FLOAT and 1.5e300 became `inf`) |
| REAL | `REAL` | float | inferred FLOAT |
| FLOAT(p) | `Float` | **Decimal** (decimal floating point) | DOUBLE if p ≤ 15, else STRING |
| CHAR / VARCHAR / NCHAR / NVARCHAR / CLOB / NCLOB | `CHARACTER ...`, `NATIONAL ...` | str (CHAR is blank-padded) | inferred STRING |
| BINARY / VARBINARY / BLOB | `BINARY ...` | bytes | BINARY |
| DATE | `DATE` | date | NAIVE_DATE |
| TIME(p) | `TIME` | time | ISO string |
| TIMESTAMP(p) | `TIMESTAMP` | datetime, **truncated to µs** | NAIVE_DATETIME (Mimer has no time-zone type) |
| BOOLEAN | `BOOLEAN` | **int 0/1** (mimerpy's bool conversion checks the wrong variable) | BOOLEAN |
| INTERVAL ... | `INTERVAL` | str, e.g. `'2-03'` | inferred STRING |
| BUILTIN.UUID | `USER-DEFINED` (+ `BUILTIN.UUID`) | uuid.UUID | str |

Other quirks:

- **Identifiers** keep the case they were created with but compare case-insensitively, even when
  quoted (`"mixedcase"` collides with `"MixedCase"`). Catalog string comparisons are
  case-insensitive too. Data comparisons are case-sensitive (`code = 'b'` does not match `'B'`).
- No multi-row `INSERT ... VALUES (...), (...)`, no `IF NOT EXISTS`, and no `TIMESTAMP WITH TIME ZONE`.
- An index must be named in its table's schema: `CREATE INDEX FTTest.ix ON FTTest.t (...)`.
- Row-value comparison `(a, b) > (?, ?)` works; the connector keeps the template's OR/AND form.
- The ordering quirk IRIS has does not exist here: ordered columns come back as normal values.
- **Nanosecond keys**: for TIMESTAMP(7-9) / TIME(7-9) keyset columns, a position built from the
  µs-truncated datetime could skip rows on resume. The connector also selects
  `CAST(col AS VARCHAR(48))`, which gives `TIMESTAMP'2024-01-01 00:00:00.123456100'`, and stores
  the exact text, bound back as `CAST(? AS TIMESTAMP(9))`. The delivered column value stays
  µs-truncated (Python's limit).
- UUID keys are bound with `BUILTIN.UUID_FROM_TEXT(?)`. Strings bind fine against DATE / TIMESTAMP /
  DECIMAL columns, but the connector casts explicitly.

## Tests run

Test objects live in schema `FTTest`, in a databank `FTTESTDB` created for them (they are not part
of the seeded data). Tables: `typesdemo` (every type above, 2 rows with NULL cursor), `strpk`
(NVARCHAR key with mixed case, unicode, spaces), `compositepk` (VARCHAR+INT), `nopk`, `t9`
(TIMESTAMP(9), UUID, date; full reload), `ts9cur` (TIMESTAMP(9) cursor with ties below 1 µs),
`uuidpk` (UUID key, full reload), `uq` (empty table with a unique constraint), `lobtest` (CLOB/BLOB up
to 1 MB / 1.5 MB per row), `ties` (25 rows, 3 cursor values, string key).

| Test | Result |
|---|---|
| Initial sync of all edge tables | OK. `nopk` skipped with one `op.warning`. Values checked in warehouse.db; LOB lengths match. Found and fixed the DOUBLE→`inf` and DECIMAL(45)→DOUBLE problems. |
| Second sync after ADD COLUMN, DROP COLUMN, update, delete, insert, update of a NULL-cursor row | OK. New-column warning, dropped-column info, updated and new rows delivered, NULL-cursor rows re-sent. Deleted row in a full-reload table marked `_fivetran_deleted`. Hard deletes in incremental tables are not detected (same as IRIS). |
| Nanosecond cursor | OK. Cursor stored as `2024-01-01 00:00:01.000000001`; a row 1 ns later was picked up. |
| `cursor_overrides` strategy change (`strpk=` → full, `t9=d` → incremental on a DATE) | OK. Strategy-change warning, table re-synced from the start, deferred truncate for the new full reload. Unknown table → `op.warning`. A VARCHAR override fails the sync with a clear error. |
| Resume from hand-written `after` | OK for composite key (3 rows), string key with ties (6), UUID full reload (1, no second truncate), nanosecond cursor (4). Counts match the expected rows exactly. |
| Multi-batch scratch copy (`BATCH_SIZE=2`, `ARRAY_SIZE=2`, checkpoint every 3 rows) with a failure injected at row 7 of `ties` | Checkpoint held `after=[2024-07-01, k15]`. The resume delivered the remaining 19 rows; 25/25 distinct keys, sum matches the source. |
| Full sync of seeded `SYSADM.customers` (1M) + `orders` (10M), `--naming SOURCE`, under the lock | **OK: 24 min 23 s, peak 1.56 GB** (SDK report; RSS monitor 1.64 GB), 11,000,000 upserts, 112 checkpoints. |
| Content comparison against the source | **All OK**: counts, distinct ids, sum(id) / sum(customer_id) / sum(amount), min/max created_at and updated_at, per-status counts, and a 2,000-row field-by-field sample per table with 0 mismatches. |
| Incremental sync right after the full sync | OK. 1 boundary row per table (`>=` cursor); the unindexed scan of orders took 3.7 s; peak 19 MB. |
| `/evaluate-connector` | 92/100, no required issues. Applied: `op.warning` for the array-size fallback. Not done: `configuration_form`, splitting the long `_read_rows` / `_discover`, reconnect on a dropped connection (resume from checkpoint covers it). |

Timing breakdown, orders (10M rows, 1,349 s): query 21 s (one sort of 10M rows), fetch 154 s,
convert 233 s, upsert 720 s, checkpoint 200 s; gc 378 s of that. Fetch stayed flat at 3.9 s per 250k
rows for the whole run, so the driver does not grow. Convert and upsert time per 250k rows rose
(26 s → 40 s wall) with GC time (3.5 s → 16.7 s per 250k), the same pattern IRIS showed and
attributed to the debug runner's set of upserted keys. For comparison, IRIS took 29 min with peak 1.99 GB.

`FETCH FIRST n` batches were rejected after measuring: each 50k batch on the unindexed `updated_at`
took 5.7-10 s (a re-sort of the remaining rows every time), against a single 21 s sort for the whole
table with the server cursor.

## Deployment (not yet tried)

- mimerpy 1.3.11 is pure Python (ctypes). It needs only the native `libmimerapi.so` (355 KB;
  depends on libc, libm, `libcrypto.so.3`). The connector sets `MIMERAPI_PATH` to a
  `libmimerapi.so` placed next to `connector.py` if one exists, so the library could ship in the
  deploy package without the custom-driver mechanism, as long as the runtime has
  `libcrypto.so.3`. Otherwise `drivers/installation.sh` would have to place the library.
  **Open: can Mimer's client library be redistributed in the package?** (licence question). The
  copy on the VM is at `/usr/local/lib/libmimerapi.so`, taken from the container by `seed_all.sh`.
- No SQLHOSTS setup is needed at runtime (the connector writes it).

## Open questions / not done

- 12 test scenarios not run.
- A full pass over one table is one read-only transaction (22 min for orders). Mimer handled it
  with concurrent writers in a small test; how a long snapshot affects a busy production server
  (old row versions kept) is unknown. The Redshift-style chunking (bounded transactions with an
  upper bound per chunk) is not implemented: on an unindexed cursor each chunk would rescan the
  table.
- No `configuration_form` (setup form).
- Seeded tables have no index on `updated_at`, so each incremental sync scans the table (3.7 s for 10M).
- Also affects other connectors: the SDK infers a Python float as 32-bit FLOAT, so `DOUBLE` columns left
  undeclared lose precision or overflow to `inf`. IRIS does not declare its `double` columns and
  probably has the same problem (not checked).
- Left in the database: schema `FTTest`, databank `FTTESTDB` (test only; drop with
  `DROP DATABANK FTTESTDB CASCADE` and `DROP SCHEMA FTTest CASCADE` when done).

## Running it on the VM

```bash
export MIMER_SQLHOSTS=/opt/sdk-test-dbs/sqlhosts      # optional: the connector writes its own
export PYTHONPATH=~/connector-deps                     # mimerpy 1.3.11 for system Python 3.11
export LD_LIBRARY_PATH=/usr/local/lib                  # or MIMERAPI_PATH=/usr/local/lib/libmimerapi.so
cd ~/wt-mimer/mimer
fivetran debug --configuration <scratch config> --state <scratch state> --naming SOURCE
```

Seeded tables are `SYSADM.customers` and `SYSADM.orders` (databank `SEEDDB`); set `schemas` to
`SYSADM` to sync only them.
