# IRIS connector: test notes

`iris/connector.py` is the general InterSystems IRIS connector and the template the other SQL
connectors were ported from. It meets Connector Requirements 1-7 in `CLAUDE.md`. Tested on the
VM's IRIS Community container (port 1972, namespace `USER`) with `fivetran debug --naming SOURCE`.

Status (2026-11-22): edge-case tests, a full initial sync with a content comparison, and the 12 test
scenarios on the small fixture all pass. `/evaluate-connector` raised one required issue (name
collisions), which is fixed. Not deployed yet.

## Fixes made in this round (ports should pick these up)

Found while running the scenarios and `/evaluate-connector`, or reported by the ports and confirmed
on IRIS. All three are in `iris/connector.py` now.

| Fix | What went wrong before | Ports |
|---|---|---|
| **Declare `DOUBLE` for `double` columns, `STRING` for `varchar`/`char`/`longvarchar`/`time`, and `STRING` for NUMERIC wider than 38** | The SDK infers an undeclared Python float as a 32-bit FLOAT and an undeclared string column from its content. Confirmed on IRIS (`FTTest.typeguess`, `FTTest.strguess`): DOUBLE `3.141592653589793` arrived as `3.1415927410125732`, `-1e300` as `-inf`, `1e-10` as `1.000000013351432e-10`; VARCHAR `'t'`/`'false'` arrived as BOOLEAN, `'2024-01-01'` as DATE, `'1.50'` as DOUBLE `1.5`. IRIS reports `DOUBLE`, `FLOAT` and `REAL` all as `double`. Integers are still left to inference. This departs from requirement 7's "declare only where a type must be forced": for these types the type must always be forced. New columns are still picked up, because `schema()` declares whatever the catalog lists on every sync. | Exasol, H2 and Mimer already declare both. **CUBRID declares no `STRING`, so it probably has the string problem.** |
| **Skip tables whose schema, table or column names would merge in the destination** (one `op.warning` listing them) | IRIS accepts delimited names such as `"coll-ide"` next to `coll_ide`, `"CamelCase"` next to `camel_case`, and columns `"a-b"` next to `a_b`. With default naming, the two tables were written into one destination table `coll_ide` and the two columns into one `a_b`, with no error (the local runner only warns about the column case). Copied from Exasol's `_colliding_tables`: checks only the selected tables. | Exasol has it. **H2, Mimer and CUBRID do not**; check whether their quoted identifiers allow such pairs. |
| **Connection errors name the server; network failures are retried** (`_connect`: 3 attempts, 5 s then 10 s) | A closed port failed with the driver's bare `<COMMUNICATION LINK ERROR> Failed to connect to server; Details: Error code: -1`, no host or port, no retry. Now: `Could not connect to IRIS at localhost:1/USER after 3 attempts: ...`. A refused login (`Access Denied`) fails at once with "check the username, password, namespace and host". | Exasol and H2 retry; Mimer and CUBRID do not. |

IRIS driver quirk found on the way: a wrong password, an unknown namespace and an **unresolvable host
name** all fail with the same `<COMMUNICATION ERROR> Invalid Message received; Details: Access Denied`
after about 2 s. A closed port fails at once with `Error code: -1`. So the connector can only retry
the second kind, and the login message lists the host as a possible cause.

## Edge-case tests (2026-11-22, before the fixes above)

Test schema `FTTest`: `typesdemo` (every IRIS type the first pass covered: varchar, bit, date,
time, timestamp, numeric(18,4), longvarchar, varbinary; nullable `updated_at` with NULLs),
`compositepk` (composite key, no cursor, so full reload), `nopk` (no primary key).

| Test | Result |
|---|---|
| String and composite primary keys | Pass |
| Table without a primary key | Skipped, listed in one `op.warning` |
| Every column type in `typesdemo` | Pass |
| NULL cursor values | Re-sent every sync |
| Second sync after add column, drop column, update, delete | Pass; drift warning for the new column, dropped column logged |
| `cursor_overrides` strategy change | State reset, table re-synced from the start |
| Resume from a hand-written `after` position | Resumed exactly after it |
| Multi-batch paging with tied cursor values (`BATCH_SIZE = 2`) | Pass |
| Five exact-duplicate rows without a key (before keyless tables were skipped) | Became 2 rows: why keyless tables are skipped |

`typesdemo` has no DOUBLE, TINYINT/SMALLINT/BIGINT or text that looks like another type, which is
why the edge tests missed the inference problems. Added in this round: `FTTest.typeguess` (double,
float, real, tinyint, smallint, bigint, money, numeric(29,10), posixtime, time, smalldatetime,
longvarbinary) and `FTTest.strguess` (varchar/char values that look like booleans, numbers, dates).
Both sync correctly with the fixes: `DOUBLE` keeps `3.141592653589793`, `-1e+300` and `1e-10`; every
string column arrives as VARCHAR with its text unchanged. Catalog notes: `CHAR(n)` is reported as
`varchar`; `POSIXTIME` and `SMALLDATETIME` as `timestamp`; `MONEY` as `numeric(15,4)`; IRIS caps
NUMERIC precision (a declared `NUMERIC(50,10)` became `numeric(29,10)`, and a 19-digit value lost
its fraction on insert, at the source).

## Test scenarios (small fixture, 2026-11-22)

Script: `iris/scenarios.py` (adapted from `exasol/scenarios.py` and `h2/scenarios.py`). Run on the VM:

```bash
PYTHONPATH=~/connector-deps /usr/bin/python3 iris/scenarios.py --config <scratch>/config.json --work <scratch>/scenarios
```

The config holds `host`, `port`, `username`, `password`, `namespace` (keep it outside the repo). The
script rebuilds the fixture in `FTTest` (`customers` = seeded `SQLUser.customers` with `id <= 1000`,
`orders` = `SQLUser.orders` with `id <= 10000`, via `CREATE TABLE ... AS SELECT` plus a primary key
on `id`; the seeded tables are only read), runs each sync as `fivetran debug --naming SOURCE` on a copy
of `connector.py` with `schemas=FTTest` and `include_tables=customers,orders`, and after every
scenario compares the whole destination with the source field by field (counts, duplicate keys,
every column of every row). Expected upserts per incremental sync = rows with `updated_at >=` the
saved cursor (the `>=` boundary rows are re-read by design) plus NULL-cursor rows. Scenario 7 runs a
patched copy that checkpoints every 1,000 rows and raises after the 5th checkpoint; scenario 12 runs a
patched copy (1,000 rows per query) that runs writes on a second connection just before the third
`orders` query. The whole run takes about 3 minutes.

Final run, after all fixes (an earlier run before the fixes had the same results, except scenario 6
had no host/port in the error):

| # | Scenario | Result | Detail |
|---|---|---|---|
| 1 | Initial sync baseline | PASS | 11,000 upserts; customers 1,000 = 1,000, orders 10,000 = 10,000, every field equal |
| 2 | Incremental: 500 new rows | PASS | 502 upserts (500 + 2 boundary rows), as expected; 10,500 orders equal |
| 3 | Incremental: 100 updated rows (all at one `updated_at`) | PASS | 601 upserts as expected; new amounts in the destination; count unchanged |
| 4 | Soft deletes: 50 rows set to `cancelled` | PASS | 151 upserts as expected; status updated |
| 5 | Hard deletes: 10 rows | PASS (known gap) | 51 upserts as expected; the 10 deleted rows stay in the destination, not flagged deleted: a cursor-based sync cannot see deletes |
| 6 | Outage, then a 200-row backlog | PASS | Sync with `port=1`: failed (rc 1) after 2 logged retries with `Could not connect to IRIS at localhost:1/USER after 3 attempts: <COMMUNICATION LINK ERROR> ...`; state file unchanged. Then 200 orders inserted and a normal sync: 251 upserts as expected, no duplicates. No container stopped |
| 7 | Mid-sync failure and resume | PASS | Own destination, empty state. Crashed after 5 checkpoints with 1,000 customers and 3,000 orders delivered; state file mid-table (`after = ['2022-12-09 12:50:00.000000', 6811]`, i.e. (cursor, id)). 300 orders inserted, then resumed with the real connector: "resuming incremental sync", 7,991 upserts; 10,990 orders equal, no duplicates |
| 8 | Schema drift: add `discount_pct DECIMAL(5,2)`, set on 100 rows | PASS | 601 upserts as expected; new-column warning logged; destination column `DECIMAL(5,2)`, values on exactly 100 rows, NULL elsewhere |
| 9 | Schema drift: add `notes`, sync, drop it, sync | PASS | Both syncs succeed (111 and 21 upserts, as expected); "columns no longer in the source: notes" logged; the destination keeps `notes`, and the 20 rows re-synced after the drop have it NULL |
| 10 | Large payloads: `LONGVARCHAR`, 5 rows of ~1 MB | PASS | 26 upserts as expected; 850,000 characters / 1,000,000 UTF-8 bytes each (includes `é` and `€`); length and SHA-256 equal for all 5 |
| 11 | Cursor boundary: 20 rows at exactly the saved cursor | PASS | Saved cursor `2025-02-10 00:00:00.000000`; all 20 arrive (26 upserts as expected) |
| 12 | Concurrent writes during a sync | PASS (known gap) | Writes ran between queries 2 and 3: 50 rows already read moved forward, 50 not yet read moved forward, 50 new rows, 10 rows updated with an old `updated_at` (a late commit). The sync upserted 3,126; the 50 rows updated behind the reader were read again later in the same sync (IRIS reads to the end of the table, with no upper bound fixed at the start of the pass, unlike the H2/CUBRID `high_water`), so right after it only the 10 late-commit rows differ. Follow-up sync: 51 upserts; the same 10 still differ: a cursor-based sync cannot see a write with a timestamp below the cursor |

Known gaps (by design for a cursor-based sync): hard deletes (scenario 5) and late commits with an
old `updated_at` (scenario 12). A new column with a `DEFAULT` that fills existing rows without
bumping `updated_at` reaches those rows only when they next change (the drift warning says to re-sync).

## Full initial sync (seeded `SQLUser`: 1M customers, 10M orders, 2026-11-22, before this round's fixes)

`fivetran debug --naming SOURCE`, empty state: **29 min, peak 1.99 GB** (with a 100k-row checkpoint
cap; with 60 s only it peaked at 2.54 GB). Content comparison against the source passed: counts,
distinct ids, column sums, min/max timestamps, per-status counts, and a 2,000-row field-by-field
sample per table.

- Orders breakdown (10M rows): upsert 38%, query 22% (no index on `updated_at`, so each 50k batch
  re-scans and sorts the remaining rows, ~1.8 s), fetch 13%, convert 13%, checkpoint 13%.
- Fetch and convert time per row doubled over the run while process memory grew to ~2 GB: about 16
  full GC collections per 250k rows, each slower as the heap grows (1.3 s to 6.2 s of GC per 250k rows
  by 4M rows). The connector holds at most one batch; the growth is most likely the debug runner's
  set of every upserted key. Not mitigated (CUBRID added `gc.collect()` + `gc.freeze()` after each
  checkpoint for a JPype-specific cause).
- One streamed query (before batching) grew the driver's memory linearly, 2.4 GB after 6M rows, which
  is why IRIS reads in `TOP n` batches.

Effect of this round's fixes on speed: see **A/B timing** below. Memory is unaffected (the fixes add
no per-row state).

## A/B timing

PENDING

## Quirks (IRIS)

- A column that is both selected and in `ORDER BY` comes back in its internal form (timestamps as
  `'1154657064703846976'`, dates as day numbers, numerics as strings). The connector casts ordered
  columns to their own type (`_select_expr`). The scenario script never orders by a selected column.
- The `_SYSTEM` password contains `@`: build SQLAlchemy URLs with `URL.create`, never an f-string.
- `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` carries `CONSTRAINT_TYPE` itself. `DISTINCT` on catalog
  columns upper-cases the values (IRIS collation), so discovery avoids it.
- `CREATE TABLE ... AS SELECT` keeps `NOT NULL` on the key column; `ALTER TABLE ... ADD col type` and
  `DROP COLUMN` work as usual; `DATEADD('s', n, ts)` for second offsets.
- `fivetran debug` writes the state file even when the sync fails (scenario 7 resumed from it directly).

## Open questions / not done

- No `configuration_form` (optional; none of the ports has one either).
- Hard deletes are not detected for incremental tables (full-reload tables get them through the
  deferred truncate).
- `iris/scenarios.py` sits in the connector directory, as in the ports. `fivetran deploy` packages
  the project directory, so either exclude it (`.gitignore`) or move the scenario scripts out before
  deploying.
- Deployment: `intersystems-irispython` ships a native extension wheel; check it installs in the
  Fivetran runtime (Python 3.14 default) before deploying.
- Left in the database: schema `FTTest` with `typesdemo`, `compositepk`, `nopk`, `typeguess`,
  `strguess`, the collision test tables `coll-ide`, `coll_ide`, `colcoll`, `CamelCase`, `camel_case`,
  and the scenario fixture `customers` / `orders` (rebuilt by every scenario run).
