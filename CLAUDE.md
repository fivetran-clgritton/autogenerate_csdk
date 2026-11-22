# Fivetran SDK Connector Project

## What this is
Building Fivetran Connector SDK connectors for legacy databases that have no managed Fivetran connector and are not in `fivetran/community_connectors`. Priority ordered by Gong call demand + build feasibility.

## Where to run Claude Code

- **Anything that touches the databases** (seeding, `fivetran debug`, test scenarios, container logs): run Claude Code **on the VM**, inside tmux so a dropped SSH doesn't kill long jobs:
  ```bash
  tmux new -s claude        # reattach later with: tmux attach -t claude
  cd /opt/sdk-test-dbs/connectors
  claude
  ```
- **Claude Code cloud sessions** can edit code, change GCP firewall rules (via `GCLOUD_SERVICE_ACCOUNT_KEY`) and query BigQuery, but cannot SSH to the VM (IAP tunnel blocked by the egress proxy) or reach any DB port.
- The VM checkout needs GitHub push credentials (e.g. `gh auth login` or a PAT) before a VM session can commit fixes; `seed_all.sh` resets the checkout to `origin/main` on every run.

## VM layout

| Path | What |
|------|------|
| `/opt/sdk-test-dbs/connectors` | Clone of this repo (`fivetran-clgritton/autogenerate_csdk`, **public**) |
| `/opt/sdk-test-dbs/docker-compose.yml` | The compose file the running containers come from. It is a **separate copy**, not this repo's `docker-compose.yml` (container names differ, e.g. `exasol` vs `connectors-exasol-1`). Port or flag changes made in the repo copy must be applied here by hand. Never run `docker compose` from the repo directory — it creates a second project with blank passwords. |
| `/opt/sdk-test-dbs/.env` | `VERTICA_PASS`, `CUBRID_PASS`, `MIMER_PASS` used by compose |
| `/opt/sdk-test-dbs/venv` | Python 3.14 venv (3.14 built from source; Debian only ships 3.11) |
| `/opt/sdk-test-dbs/jars` | `cubrid_jdbc.jar` and `h2.jar`, copied out of the containers by `seed_all.sh` (JDBC seeders and connectors use them) |
| `/opt/sdk-test-dbs/sqlhosts` | Mimer client SQLHOSTS file (`MIMER_SQLHOSTS`), written by `seed_all.sh` |
| `/mnt/data/<service>` | Container data (500GB SSD) |

## GCP Test Infrastructure

| Item | Value |
|------|-------|
| VM | `sdk-test-dbs` |
| IP | `35.202.214.246` |
| Zone | `us-central1-a` |
| Project | `internal-sales` |
| Machine | `n2-standard-8` (8 vCPU, 32GB RAM) |
| Data disk | `sdk-test-data` — 500GB SSD at `/mnt/data` |
| Firewall | `sdk-test-db-ports` (DB ports + 8563 + 52773, allowlisted source IPs only), `sdk-test-ssh` (22) |

SSH from a laptop:
```bash
gcloud compute ssh sdk-test-dbs --zone=us-central1-a --project=internal-sales --tunnel-through-iap
```

gcloud from a cloud session (API calls only, e.g. firewall rules — no SSH):
```bash
unset CLOUDSDK_AUTH_ACCESS_TOKEN   # set to `proxy-injected` by the environment
echo "${GCLOUD_SERVICE_ACCOUNT_KEY}" > /tmp/sa-key.json
gcloud auth activate-service-account --key-file=/tmp/sa-key.json
gcloud config set project internal-sales
```

## VM Database Passwords

**These are committed to a public repo.** TODO (user decision pending): move them to a gitignored `.env` and rotate. Do not add new secrets to tracked files; generate random passwords when one is needed.

| Database | Password |
|----------|----------|
| Exasol | `exasol` (default) |
| Vertica | `ISn7D4o7psvJJ9WAxqPIm8H` |
| CUBRID | `TapSnlMKuWhuiGirAWlTCquZ` |
| Mimer | `cHGH1buf8XGnLS5CeuI8xjZ` |
| IRIS | `I1DzGh0z0@YoJw` (`_SYSTEM`; changed in the portal on 2026-11-20) |
| H2 | no password (`sa`/``) |

## Running Databases (Docker Compose on VM)

| Database | Ports | Status |
|----------|-------|--------|
| InterSystems IRIS Community | 1972, portal 52773 | Running |
| Exasol | 8563 (unencrypted — used by seeder and connector), 9563 (TLS) | Running |
| Apache Geode | 10334 locator / 40404 server / 7070 locator HTTP / 8081 dev REST | Running |
| CUBRID | 33000 | Running |
| Mimer SQL | 1360 | Running |
| H2 | 9092 TCP, 8082 web console | Running |
| VoltDB | 21212 | SKIPPED — Developer Edition requires a paid license file |
| Vertica | 5433 | Needs Docker Hub login to pull `vertica/vertica-ce` — TODO |

To start Vertica once Docker Hub credentials are available (on the VM):
```bash
echo "$DOCKER_HUB_TOKEN" | docker login --username "$DOCKER_HUB_USER" --password-stdin
cd /opt/sdk-test-dbs && docker compose pull vertica && docker compose up -d vertica
```

## Seeding

Each database gets `customers` (1M rows: id, name, email, created_at, updated_at, status) and `orders` (10M rows: id, customer_id, amount, status, created_at, updated_at). Geode gets 100K / 1M because it loads over REST. Data is deterministic (fixed Faker/random seeds).

```bash
sudo bash /opt/sdk-test-dbs/connectors/seed_all.sh iris cubrid   # any subset; no args = all six
```

- The script resets the checkout to `origin/main` and re-runs the updated copy, so uncommitted edits on the VM are wiped. To test local edits: `sudo SEED_SKIP_UPDATE=1 bash /opt/sdk-test-dbs/connectors/seed_all.sh <db>`.
- Piping into `tee` hides the exit code; read the final `FAILED databases:` / `All requested databases seeded` line.
- To iterate on one DB, run the seeder directly with small counts first:
  ```bash
  source /opt/sdk-test-dbs/venv/bin/activate
  python /opt/sdk-test-dbs/connectors/seed_data.py --db h2 --host localhost --customers 1000 --orders 10000
  ```
- Every seeder clears its tables/regions before loading, so re-running after a partial failure is safe (a failed run on a big load can take a while to clear).

### Seeding status (2026-11-21)

All six databases are seeded: 1M customers / 10M orders each, except Geode (100K / 1M). Row counts were checked directly in each database.

| DB | Notes |
|----|-------|
| Exasol | Reloaded cleanly (truncates first); no duplicate ids. |
| IRIS | The password contains `@`, so the seeder builds the URL with `sqlalchemy.engine.URL.create` (an f-string URL breaks the sqlalchemy-iris dialect). |
| CUBRID | CUBRID-Python fails on Python 3.14 ("Invalid connection handle"), so seeder and connector use the JDBC jar via jaydebeapi. The compose `CUBRID_PASSWORD` env var does **not** set the `dba` password; it was empty until set with `csql ... ALTER USER dba PASSWORD ...`. If the container is rebuilt, set it again. |
| Mimer | The client resolves the database name (`mimerdb`) through an SQLHOSTS file, not host:port. `libmimerapi.so` is a symlink inside the container (copy the real file under `/opt/mimersqlsrv*/lib`). There is no `CREATE TABLE IF NOT EXISTS` (`IF` is reserved), and `SYSADM` has no default databank, so tables live in a `SEEDDB` databank the seeder creates. |
| Geode | Persistent regions need persistent PDX metadata or every REST write fails with a misleading "Json doc ... invalid" 400. Fixed with `configure pdx --disk-store --read-serialized=true` (stored in cluster config). Do not pass `--bind-address=0.0.0.0` to the locator or server (JGroups rejects it and the container crash-loops). The compose command removes stale `.pid` files so restarts work. |
| H2 | The `oscarfonts/h2` image listens on 1521 (TCP) and 81 (web) inside the container, so compose maps `9092:1521` and `8082:81`, and runs with `-ifNotExists`. The client jar must match the server (2.1.214). Database name is `test` (relative to the server's baseDir). |
| Vertica | Not running; needs Docker Hub login |
| VoltDB | Skipped (no free license); the container crash-loops |

Seeders for IRIS, CUBRID, Mimer, H2 and Geode now clear their tables/regions before loading, so re-runs are safe.

## Connector Requirements

These are **general connectors** for customers running these databases, not harnesses for the seeded test data. Every connector must meet these requirements (decided 2026-11-21):

1. **Discover tables and columns from the database catalog.** Never hardcode table names, column lists, primary keys or cursor columns. Use the source's catalog: `INFORMATION_SCHEMA` (IRIS, Mimer, H2), `EXA_ALL_TABLES` / `EXA_ALL_COLUMNS` / `EXA_ALL_CONSTRAINT_COLUMNS` (Exasol), `db_class` / `db_attribute` / `db_index` (CUBRID), the region list from the dev REST API (Geode, sampling entries for fields), and so on for new databases.
2. **Let the user scope the sync.** Configuration should allow selecting schemas and including or excluding tables (flat string values, e.g. comma-separated lists, per the SDK's `configuration.json` rules). Exclude system schemas by default.
3. **Take primary keys from the catalog. Sync only tables that have one** (decided 2026-11-22, first pass). Skip tables without a primary key and list them in one `op.warning`. Without a key, Fivetran hashes every value into `_fivetran_id`, which makes upserts slow and collapses exact-duplicate rows (IRIS test: 5 source rows became 2). Support for them may come later.
4. **Choose a sync strategy per table.**
   - **Incremental:** when a usable cursor column exists (configurable, defaulting to a convention such as `updated_at`). Read `cursor >= last value` with keyset paging on `(cursor, primary key)`.
   - **Full reload:** for tables with no usable cursor. Use `op.truncate(table, deferred=True)` and then upsert every row, so a failed sync deletes nothing.
5. **Avoid unbounded reads of large tables.** This is a guideline, not a hard rule. A single unbounded `SELECT *` over tens or hundreds of millions of rows is usually a bad pattern, so prefer bounded batches (`TOP n` / `LIMIT n` / `FETCH FIRST n`) continuing after the last `(cursor, pk)` delivered. Decide per database: some drivers stream a single query well with server-side cursors, others buffer client-side. Check memory with a full sync of the 10M-row orders table. Example: the IRIS driver grew memory linearly (2.4 GB after 6M rows) on one streamed query and would have hit the 4 GB limit that `fivetran debug` and production enforce, so IRIS uses batches.
   - **Server-side cursors are a good pattern where the database supports them.** The reference is the Redshift `large_data_volume` connector in `fivetran/community_connectors` (`redshift/large_data_volume/redshift_client.py`): `BEGIN`, `DECLARE <name> NO SCROLL CURSOR FOR <query>`, a loop of `FETCH <n> FROM <name>`, then `CLOSE` and `COMMIT`. The database holds the result set, the client holds one batch, and there is no repeated per-batch sort. Where a driver exposes the equivalent natively (a server-side result-set handle, a JDBC fetch size, lazy query execution), that counts too, but verify it with a memory check.
   - For very large incremental tables, the same connector **chunks** the cursor. It finds an upper bound with `ORDER BY rk LIMIT 1 OFFSET chunk_size-1`, declares a cursor over `rk > bookmark AND rk <= upper_bound`, and checkpoints `upper_bound` after each chunk. The `<=` keeps all rows with the boundary value in one chunk, so ties are safe and each transaction stays bounded. (Its extra row-count checkpoints inside a chunk use the last row's value with `>`, which can skip ties on resume. Don't copy that part.)
   - The same connector is also the reference for discovery: `information_schema` tables, columns and primary keys, replication-key inference from column types, include/exclude projection, full-resync fallback, and parallel tables through a connection pool.
   - Unverified per-database notes, to check during the full-sync memory tests: Exasol (pyexasol reads through a server-side result handle in chunks), IRIS (SQL `DECLARE CURSOR` may exist only in embedded SQL; also test the raw DB-API cursor without SQLAlchemy), H2 (materializes results unless `LAZY_QUERY_EXECUTION=1`), CUBRID (JDBC fetch size), Mimer (mimerpy server cursor), Vertica (streams natively).
6. **Checkpoint cadence bounds memory.** The SDK buffers upserted rows in memory until the next `op.checkpoint()` flushes them, so memory scales with the rows between checkpoints, not with table size. Measured with a database-free test connector (2M rows): one checkpoint at the end peaked at 3.5 GB; every 60 s peaked at 1.4 GB; every 10,000 rows peaked at 290 MB. The SDK reference says to checkpoint no more than once a minute, which doesn't account for this. Checkpoint on **whichever comes first: about 60 s or a row cap** (~100k rows, lower for wide rows), plus at the end of each table. A mid-table checkpoint stores the keyset position so a resume is exact. (IRIS with 60 s only peaked at 2.54 GB on the full sync; with a 100k-row cap it peaked at 1.99 GB.)
7. **Handle schema drift:** pick up new columns and cope with dropped ones without failing the sync. Declare column types only where a type must be forced (e.g. `DECIMAL` precision/scale, `UTC_DATETIME`), so the SDK can infer the rest and evolve the schema.
8. **Follow the SDK reference** in the `fivetran-connector-sdk` Claude Code plugin (https://github.com/fivetran/connector_sdk_tools, installed on the VM). In particular: call `op.*` directly (never `yield`), run `/evaluate-connector` before calling a connector done, and keep dependencies the Fivetran runtime lacks (Java/JDBC jars, native client libraries) in mind. Those need the SDK's custom-driver mechanism (`drivers/installation.sh`, Private Preview) or a pure-Python driver.

**Current state (2026-11-22):** IRIS meets requirements 1-7 and is the template (see **Porting the IRIS template**). It passed edge-case tests (string/composite keys, keyless table skipped, every column type, NULL cursors, add/drop column, update, delete, strategy change, resume from a saved position, multi-batch paging with tied cursor values) and a full initial sync of 1M customers + 10M orders (29 min, peak 1.99 GB, `--naming SOURCE`) with a content comparison against the source: counts, distinct ids, column sums, min/max timestamps, per-status counts and a 2,000-row field-by-field sample per table. The 12 test scenarios have not been run yet. The other connectors are still hardcoded to `customers` / `orders` with `id` / `updated_at` (Geode reads regions from config and samples one entry).

## Porting the IRIS template

`iris/connector.py` is the reference implementation of the requirements above. Database-specific parts are marked `IRIS:`; everything else should carry over with little change. To port it:

1. **Catalog queries** (`_discover`, `_load_schema_catalog`): base tables, columns (name, type, precision, scale, nullable), primary-key columns in key order, and leading index columns (for the "cursor not indexed" warning). Exclude system schemas by default (`SYSTEM_SCHEMAS`, `SYSTEM_SCHEMA_PREFIXES`); a schema listed in `schemas` is synced even if it is a system schema.
2. **Type sets** (`TIMESTAMP_TYPES`, `DECIMAL_TYPES`, ...): map the catalog's type names. Declare in `schema()` only DECIMAL (precision/scale, max 38), timestamps (`NAIVE_DATETIME` unless the type carries a zone, then `UTC_DATETIME`), dates, booleans and binary. Everything else is inferred.
3. **Batched reads** (`_read_rows`): `TOP n` becomes `LIMIT n` / `FETCH FIRST n ROWS ONLY` as the dialect needs. `_keyset_condition` expands `(a, b) > (x, y)` into OR/AND form, so it works without row-value comparison. Or replace batching with a server-side cursor where the driver supports one (requirement 5), and verify memory with the full sync.
4. **Driver quirks**: IRIS returns ordered columns in an internal form unless cast (`_select_expr`). Check your driver: select a timestamp, date and numeric column while ordering by it and look at the Python types that come back.
5. **Tables without a primary key** are skipped (requirement 3), so every keyset is (cursor, primary key) or the primary key alone; no row-id paging is needed.
6. **Keep unchanged**: config keys (`schemas`, `include_tables`, `exclude_tables`, `cursor_columns`, `cursor_overrides`), state layout (`{"tables": {"<schema>.<table>": {"mode", "cursor_column", "cursor", "after", "columns"}}}`), `schema=` on every operation, checkpoint cadence (60 s or row cap), deferred truncate for full reloads (not re-issued on resume), NULL-cursor re-send, strategy-change reset, drift warnings.

Test with the same steps IRIS used: small edge-case tables in a separate schema (string PK, composite PK, no PK (must be skipped with a warning), nullable cursor with NULLs, every column type), a second sync after add/drop column + update + delete, a `cursor_overrides` strategy change, a resume from a hand-written `after` position, then the full sync of the seeded tables with a content comparison. Run `/evaluate-connector`.

## Parallel work (one agent per database)

After IRIS, each remaining SQL connector is ported by its own Claude session in its own git worktree (`/opt/sdk-test-dbs/wt-<db>`, branch `<db>-general`). Rules:

- **Own one database.** Only touch your connector directory and your database. Test tables go in a schema of your own (e.g. `FTTest`). Do not reseed or change the seeded `customers` / `orders`.
- **Never run `seed_all.sh`** while other agents are working: it resets the main checkout and reloads data under other agents' tests. Ask the user.
- **Check `free -g` before a full sync** of the 11M-row tables and do not start one with less than 8 GB available. Peak-memory numbers are only meaningful when no other full sync is running; note in your results whether one was.
- **Run `fivetran debug` from your worktree** (or a scratch copy) so the `files/warehouse.db` output is yours. Keep config/state files with passwords in your session scratchpad, never in the repo.
- **Record results in `<db>/NOTES.md`**, not in this file: what was tested, numbers, quirks found. CLAUDE.md is merged once at the end.
- **Commit and push your branch**; the user opens the PRs.

## Connector Code

One directory per connector in this repo (`<name>/connector.py`, `configuration.json`, `requirements.txt`). Set the host in `configuration.json` (`localhost` when running `fivetran debug` on the VM, `35.202.214.246` from elsewhere).

| Connector | Driver | Sync strategy | Status |
|-----------|--------|---------------|--------|
| vertica | vertica-python | cursor on updated_at | Not yet run against a real DB |
| iris | sqlalchemy-iris | catalog discovery; incremental on `updated_at` (configurable) or full reload | General connector; edge-case tests + full initial sync with content comparison passed (2026-11-22); 12 scenarios not yet run |
| exasol | pyexasol | cursor on updated_at | Smoke-tested only: small incremental sync, row counts match (2026-11-21) |
| voltdb | requests (REST) | cursor on updated_at | SKIPPED |
| geode | requests (REST + OQL, port 8081) | cursor on updated_at | Smoke-tested: small incremental sync + one full initial sync, row counts match (2026-11-21) |
| cubrid | jaydebeapi + JPype1 (CUBRID JDBC) | cursor on updated_at | Smoke-tested only: small incremental sync, row counts match (2026-11-21) |
| mimer | mimerpy | cursor on updated_at | Smoke-tested only: small incremental sync, row counts match (2026-11-21) |
| h2 | jaydebeapi + JPype1 | cursor on updated_at | Smoke-tested only: small incremental sync, row counts match (2026-11-21) |

`fivetran debug` has been run on the VM for `iris`, `exasol`, `mimer`, `geode`, `cubrid` and `h2` (incremental from a late cursor: 9,989 rows each for the SQL databases; Geode's smaller dataset gives 985). Geode also had a full initial sync (100,000 customers / 1,000,000 orders, about 6 minutes, peak 393 MB). `vertica` has not run (no instance) and `voltdb` is skipped.

**What "smoke-tested" does and does not mean.** Each connector connects, builds its schema and upserts a small slice (rows updated after late Dec 2024) with counts matching the database. Not yet tested for any connector: a full initial sync of the 11M-row tables (only Geode's 1.1M had one), content/type fidelity beyond row counts, resume after a mid-sync failure, deletes, schema drift (schemas are hardcoded), concurrent writes. Do not call a connector "working" until it passes a full initial sync with a content comparison and the test scenarios. Open question: CUBRID's first run reported 9,548 upserts instead of 9,989; not reproducible, cause unknown.

Running `fivetran debug` on the VM:
- The `fivetran` CLI uses system Python 3.11 and does not install connector requirements. Install JDBC deps without root: `/usr/bin/python3 -m pip install --target ~/connector-deps jaydebeapi JPype1`, then run with `PYTHONPATH=~/connector-deps`.
- Use a scratch config/state file instead of editing `configuration.json`, so real passwords stay out of the repo. `debug` writes the final checkpoint back to the `--state` file.
- For long runs use `PYTHONUNBUFFERED=1` when redirecting output to a file (otherwise the log lags by minutes) and `--naming SOURCE`: the local debug runner re-normalizes every column name on every row, which was ~40% of sync time in a profile and does not happen in production. Column names that are already lowercase snake_case come out the same either way.
- Connectors log a timing breakdown every 250k rows and per table (`query`, `fetch`, `convert`, `upsert`, `checkpoint`). IRIS full sync of orders (10M rows): upsert 38%, query 22% (no index on `updated_at`, so each 50k batch re-scans and sorts the remaining rows, ~1.8 s), fetch 13%, convert 13%, checkpoint 13%. Fetch and convert time per row doubled over the run while process memory grew to ~2 GB. Cause confirmed with `gc.callbacks` timing (now in the log as "of which gc"): about 16 full collections per 250k rows throughout, each slower as the heap grows, so GC time per 250k rows rose from 1.3 s to 6.2 s by 4M rows. The connector holds at most one batch; the growth is most likely the debug runner (it keeps a set of every upserted key for the whole sync), so production may not see it. Not mitigated in the connector (no `gc.freeze()` / threshold changes).
- Pass timestamps and dates to `op.upsert` as `datetime`/`date` objects, not ISO strings: the SDK re-parses strings with `strptime` (~13% of sync time in a profile).
- Seeded data is dated 2022-2024. To test without syncing 11M rows, start from a late cursor, e.g. `{"customers_cursor": "2024-12-30 12:00:00", "orders_cursor": "2024-12-31 12:00:00"}`.
- The SDK requires `DECIMAL` columns to declare `precision`/`scale` in `schema()`, `DECIMAL` values as `str`/`int`, and `UTC_DATETIME` values as ISO 8601 with a zone. `schema()` returns a list of dicts (`{"table", "primary_key", "columns": {name: type}}`); there is no `SchemaBuilder` or `DataType`. `op.upsert` takes one record dict, not a list. `vertica` and `voltdb` were written against the same wrong API and will need the same treatment.
- Connector-specific gotchas found while debugging:
  - IRIS: selecting `updated_at` while also `ORDER BY updated_at` returns an internal sort-encoded value (e.g. `'1154657064703846976'`), so the connector selects `CAST(updated_at AS TIMESTAMP)`.
  - Exasol: unquoted identifiers are upper-cased, so the schema is `TESTDB`; pyexasol returns timestamps as strings unless `fetch_mapper=pyexasol.exasol_mapper`; set `validate_certificate` to `false` for the self-signed test cert.
  - Mimer: needs `MIMER_SQLHOSTS=/opt/sdk-test-dbs/sqlhosts` and `libmimerapi.so` on the loader path; `connect(dsn=...)` takes the database name (`mimerdb`), not host:port.
  - Geode: OQL has no `OFFSET` and the dev REST API's `bind-values` does not work, so the connector pages by keyset on `updated_at` and validates the cursor before putting it in the query. The old key-scan fallback used a wrong URL and silently returned zero rows, so it was removed (failures now surface).
- All connectors read with `updated_at >= cursor` (not `>`): rows sharing the cursor timestamp are never skipped on resume or with concurrent writes, and primary-key upserts make the re-read harmless.

## Test Scenarios

12 scenarios in `test_scenarios.sql` (this repo):
1. Initial sync baseline
2. Incremental — new rows
3. Incremental — updates
4. Soft deletes
5. Hard deletes
6. Prolonged outage
7. Mid-sync failure
8. Schema drift — add column
9. Schema drift — drop column
10. Large payload rows
11. Cursor boundary fence-post
12. Concurrent writes during sync

## Infrastructure Reference

Full setup guide (gcloud commands, docker-compose.yml, startup script):
https://claude.ai/artifact/GhkQhLXsQFxPXWyd3A4NjR

## Priority Order (top 10 from Gong demand + build feasibility)

1. Vertica — highest demand (57 Gong calls), lowest friction
2. InterSystems IRIS / Caché
3. PICK/MultiValue (UniVerse, UniData, jBASE)
4. IBM TM1 / Planning Analytics
5. Netezza (IBM)
6. IBM Domino / Lotus Notes
7. CA IDMS
8. Progress OpenEdge
9. VSAM (IBM Mainframe)
10. BAAN / Infor LN / M3

## Environment Variables Needed

| Variable | Purpose |
|----------|---------|
| `GCLOUD_SERVICE_ACCOUNT_KEY` | GCP service account JSON for `sdk-test-deploy@internal-sales.iam.gserviceaccount.com` (cloud sessions) |
| `DOCKER_HUB_USER` | Docker Hub username (for pulling `vertica/vertica-ce`) |
| `DOCKER_HUB_TOKEN` | Docker Hub personal access token |

## Handoff / current state (2026-11-22)

- Work is on branch `fix-db-seeding` (pushed; several commits on top of `main`). **The PR is not open yet**: the GitHub token on the VM can push but lacks Pull requests write. Open it at https://github.com/fivetran-clgritton/autogenerate_csdk/pull/new/fix-db-seeding. `seed_all.sh` resets to `origin/main`, so use `SEED_SKIP_UPDATE=1` until it is merged.
- VM state outside the repo: `gh` installed and logged in; git identity set in this checkout; JDBC/driver deps for `fivetran debug` in `~/connector-deps` (use `PYTHONPATH=~/connector-deps`); VM compose backup at `/opt/sdk-test-dbs/docker-compose.yml.bak-20261120233821`; Geode PDX persistence configured in its cluster config; Mimer `SEEDDB` databank created; CUBRID `dba` password set.
- Scratch `fivetran debug` config/state files live in each session's scratchpad and are lost between sessions. Recreate them from the password table above, outside the repo. IRIS state is `{"tables": {"<schema>.<table>": {...}}}` (see the porting guide); the old connectors use Geode `{"<region>": {"last_updated_at": "<ISO ts>"}}`, Exasol `{"<table>": {"updated_at": "..."}}`, the rest `{"<table>_cursor": "..."}`. Mimer also needs `MIMER_SQLHOSTS=/opt/sdk-test-dbs/sqlhosts`.
- IRIS has a test schema `FTTest` (tables `typesdemo`, `compositepk`, `nopk`) used for the edge-case tests; it is not part of the seeded data.
- Two GitHub PATs were pasted into the previous session's transcript; the user was asked to revoke them.

## Next Steps

1. Port the IRIS template to Exasol, Mimer, CUBRID and H2 (one agent per database, see **Parallel work**), then Geode.
2. Full initial sync per connector (one at a time; the 10M-row orders table is where memory/speed problems will show), with a content comparison against the source, not just counts
3. Run the 12 test scenarios against each connector (7, 11 and 12 exercise the `>=` cursor change)
4. Get Vertica running (Docker Hub login), then port its connector to the current SDK API (it uses the same nonexistent APIs the others did)
5. Move credentials out of the repo and rotate them
6. `fivetran deploy` to a Fivetran workspace for end-to-end testing
