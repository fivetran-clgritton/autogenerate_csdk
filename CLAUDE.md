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

## Connector Code

One directory per connector in this repo (`<name>/connector.py`, `configuration.json`, `requirements.txt`). Set the host in `configuration.json` (`localhost` when running `fivetran debug` on the VM, `35.202.214.246` from elsewhere).

| Connector | Driver | Sync strategy | Status |
|-----------|--------|---------------|--------|
| vertica | vertica-python | cursor on updated_at | Not yet run against a real DB |
| iris | sqlalchemy-iris | cursor on updated_at | Incremental sync verified on the VM (2026-11-21) |
| exasol | pyexasol | cursor on updated_at | Incremental sync verified on the VM (2026-11-21) |
| voltdb | requests (REST) | cursor on updated_at | SKIPPED |
| geode | requests (REST + OQL, port 8081) | cursor on updated_at | Incremental and full initial sync verified on the VM (2026-11-21) |
| cubrid | jaydebeapi + JPype1 (CUBRID JDBC) | cursor on updated_at | Incremental sync verified on the VM (2026-11-21) |
| mimer | mimerpy | cursor on updated_at | Incremental sync verified on the VM (2026-11-21) |
| h2 | jaydebeapi + JPype1 | cursor on updated_at | Incremental sync verified on the VM (2026-11-21) |

`fivetran debug` has been run on the VM for `iris`, `exasol`, `mimer`, `geode`, `cubrid` and `h2` (incremental from a late cursor: 9,989 rows each for the SQL databases; Geode's smaller dataset gives 985). Geode also had a full initial sync (100,000 customers / 1,000,000 orders, about 6 minutes, peak 393 MB). `vertica` has not run (no instance) and `voltdb` is skipped.

Running `fivetran debug` on the VM:
- The `fivetran` CLI uses system Python 3.11 and does not install connector requirements. Install JDBC deps without root: `/usr/bin/python3 -m pip install --target ~/connector-deps jaydebeapi JPype1`, then run with `PYTHONPATH=~/connector-deps`.
- Use a scratch config/state file instead of editing `configuration.json`, so real passwords stay out of the repo. `debug` writes the final checkpoint back to the `--state` file.
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

## Next Steps

1. Run the test scenarios against each working connector (iris, exasol, mimer, geode, cubrid, h2)
2. Get Vertica running (Docker Hub login), then port its connector to the current SDK API and debug it
3. Move credentials out of the repo and rotate them
4. `fivetran deploy` to a Fivetran workspace for end-to-end testing
