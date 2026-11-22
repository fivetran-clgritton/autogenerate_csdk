# Fivetran SDK Connector Project

## What this is
Building Fivetran Connector SDK connectors for legacy databases that have no managed Fivetran connector and are not in `fivetran/community_connectors`. Priority ordered by Gong call demand + build feasibility.

History of what was tried and why things are set up this way: `LESSONS_LEARNED.md`.

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
| Apache Geode | 10334 locator / 40404 server / 7070 locator HTTP / 8081 dev REST (pending, see below) | Running |
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
- Only Exasol truncates before loading. The other SQL seeders have primary keys, so after a partial failure truncate the tables before re-running.

### Seeding status (2026-11-20)

| DB | Status | Notes |
|----|--------|-------|
| Exasol | Seeded, **probably twice** | It ran twice before the TRUNCATE fix and has no primary key. Re-run `seed_all.sh exasol` to reload cleanly. |
| IRIS | Failed: Access Denied | The failing run probably used an older `seed_all.sh` with the pre-portal password. Retry first. |
| CUBRID | Failed: Invalid connection handle | The newest `CUBRID-Python` (9.3.0.2) predates Python 3.14. Options: a separate Python 3.11 venv for CUBRID, or the CUBRID JDBC driver via jaydebeapi (Java is installed for H2). The connection-string fix (`CUBRID:host:33000:testdb:::`) has not run yet. |
| Mimer | Failed: `libmimerapi.so` not found | `seed_all.sh` copies it from `/usr/lib` or `/usr/local/lib` in the container — unverified; locate it with `docker exec mimer find / -name 'libmimerapi*'`. `CREATE TABLE IF NOT EXISTS` support in Mimer is also unverified. |
| Geode | Failed: connection refused on 7070 | The dev REST API was moved to 8081 because the locator in the same container owns 7070. Apply the new `geode` command and port mapping from the repo compose to the VM compose file, then `docker compose up -d geode`. Regions are created with gfsh via `docker exec`; the multi-key PUT loader has only been tested against a fake server. |
| H2 | Failed: EOFException on 9092 | The JDBC URL fix (`jdbc:h2:tcp://host:9092//opt/h2-data/test`) has not run yet. Check that the container's H2 version matches the 2.2.224 client jar; H2 2.x also refuses remote database creation unless started with `-ifNotExists`. |
| Vertica | Not running | Needs Docker Hub login |
| VoltDB | Skipped | No free license |

## Connector Code

One directory per connector in this repo (`<name>/connector.py`, `configuration.json`, `requirements.txt`). Set the host in `configuration.json` (`localhost` when running `fivetran debug` on the VM, `35.202.214.246` from elsewhere).

| Connector | Driver | Sync strategy | Status |
|-----------|--------|---------------|--------|
| vertica | vertica-python | cursor on updated_at | Not yet run against a real DB |
| iris | sqlalchemy-iris | cursor on updated_at | Not yet run against a real DB |
| exasol | pyexasol | cursor on updated_at | Not yet run against a real DB |
| voltdb | requests (REST) | cursor on updated_at | SKIPPED |
| geode | requests (REST, port 8081) | cursor on updated_at | Not yet run against a real DB |
| cubrid | CUBRID-Python | cursor on updated_at | Not yet run against a real DB |
| mimer | mimerpy | cursor on updated_at | Not yet run against a real DB |
| h2 | jaydebeapi + JPype1 | cursor on updated_at | Not yet run against a real DB |

`fivetran debug` has so far only been run from cloud sessions, which cannot reach the databases.

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

1. Reload Exasol cleanly, then get IRIS, CUBRID, Mimer, Geode and H2 seeded (see the status table)
2. Run `fivetran debug` for each connector on the VM against `localhost`
3. Run the test scenarios against each connector
4. Get Vertica running (Docker Hub login)
5. Move credentials out of the repo and rotate them
6. `fivetran deploy` to a Fivetran workspace for end-to-end testing
