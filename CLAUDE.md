# Fivetran SDK Connector Project

## What this is
Building Fivetran Connector SDK connectors for legacy databases that have no managed Fivetran connector and are not in `fivetran/community_connectors`. Priority ordered by Gong call demand + build feasibility.

## GCP Test Infrastructure

| Item | Value |
|------|-------|
| VM | `sdk-test-dbs` |
| IP | `35.202.214.246` |
| Zone | `us-central1-a` |
| Project | `internal-sales` |
| Machine | `n2-standard-8` (8 vCPU, 32GB RAM) |
| Data disk | `sdk-test-data` — 500GB SSD at `/mnt/data` |
| Compose dir | `/opt/sdk-test-dbs/` on the VM |

SSH into the VM:
```bash
unset CLOUDSDK_AUTH_ACCESS_TOKEN
echo "${GCLOUD_SERVICE_ACCOUNT_KEY}" > /tmp/sa-key.json
gcloud auth activate-service-account --key-file=/tmp/sa-key.json
gcloud config set project internal-sales
gcloud compute ssh sdk-test-dbs --zone=us-central1-a --project=internal-sales --tunnel-through-iap --command="<cmd>"
```

Note: `CLOUDSDK_AUTH_ACCESS_TOKEN` is set to `proxy-injected` by the environment — always `unset` it before running gcloud commands.

**IMPORTANT — IAP tunnel is blocked from Claude Code cloud sessions:**
`gcloud compute ssh/scp --tunnel-through-iap` uses WebSocket upgrades to `tunnel.cloudproxy.app`, which the egress proxy explicitly blocks. Claude Code cloud sessions CANNOT SSH to the VM. The user must SSH from their own laptop. Direct TCP to database ports (e.g., 9563, 5433) is also blocked.

## VM Database Passwords

| Database | Password |
|----------|----------|
| Exasol | `exasol` (default) |
| Vertica | `ISn7D4o7psvJJ9WAxqPIm8H` |
| CUBRID | `TapSnlMKuWhuiGirAWlTCquZ` |
| Mimer | `cHGH1buf8XGnLS5CeuI8xjZ` |
| IRIS | `SYS` (default `_SYSTEM`/`SYS`) |
| H2 | no password (`sa`/``) |

## Running Databases (Docker Compose on VM)

| Database | Port | Status |
|----------|------|--------|
| InterSystems IRIS Community | 1972 | Running |
| Exasol | 9563 | Running |
| Apache Geode | 10334 / 40404 | Running |
| CUBRID | 33000 | Running |
| Mimer SQL | 1360 | Running |
| H2 | 9092 | Running |
| VoltDB | 21212 | Image removed from Docker Hub — needs new image (`voltactivedata/volt-ce` or similar) |
| Vertica | 5433 | Needs Docker Hub login to pull `vertica/vertica-ce` — TODO |

To start Vertica once Docker Hub credentials are available:
```bash
gcloud compute ssh sdk-test-dbs --zone=us-central1-a --project=internal-sales --tunnel-through-iap \
  --command="echo \$DOCKER_HUB_TOKEN | docker login --username \$DOCKER_HUB_USER --password-stdin && cd /opt/sdk-test-dbs && docker compose pull vertica && docker compose up -d vertica"
```

## Connector Code

All connectors at `/home/user/fivetran/connectors/<name>/`:

| Connector | Driver | Sync strategy | Status |
|-----------|--------|---------------|--------|
| vertica | vertica-python | cursor on updated_at | Written, not tested |
| iris | sqlalchemy-iris | cursor on updated_at | Written, not tested |
| exasol | pyexasol | cursor on updated_at | Written, not tested |
| voltdb | requests (REST) | cursor on updated_at | Written, not tested |
| geode | requests (REST) | cursor on updated_at | Written, not tested |
| cubrid | CUBRID-Python | cursor on updated_at | Written, not tested |
| mimer | mimerpy | cursor on updated_at | Written, not tested |
| h2 | jaydebeapi + JPype1 | cursor on updated_at | Written, not tested |

Each connector directory has:
- `connector.py` — main connector logic
- `configuration.json` — connection config (update `<VM_STATIC_IP>` → `35.202.214.246`)
- `requirements.txt` — Python dependencies

## Test Data Schema

Each database gets seeded with:
- `customers` — 1M rows (id, name, email, created_at, updated_at, status)
- `orders` — 10M rows (id, customer_id, amount, status, created_at, updated_at)

Seed script: `seed_data.py` (in this directory)
Test scenarios SQL: `test_scenarios.sql` (in this directory)
Docker Compose: `docker-compose.yml` (in this directory)

The seed script must be run ON the VM (not from a cloud session) because database ports are not reachable from Claude Code cloud sessions. From your laptop:
```bash
gcloud compute ssh sdk-test-dbs --zone=us-central1-a --project=internal-sales --tunnel-through-iap
# then inside the VM:
pip3 install faker pyexasol vertica-python
python3 /tmp/seed_data.py --db exasol --host localhost --password exasol
python3 /tmp/seed_data.py --db vertica --host localhost --password ISn7D4o7psvJJ9WAxqPIm8H
```

## Test Scenarios

12 scenarios in `/tmp/claude-0/-home-user/5fc22e03-c98c-5c31-829d-7109aed7c2d0/scratchpad/test_scenarios.sql`:
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
| `GCLOUD_SERVICE_ACCOUNT_KEY` | GCP service account JSON for `sdk-test-deploy@internal-sales.iam.gserviceaccount.com` |
| `DOCKER_HUB_USER` | Docker Hub username (for pulling `vertica/vertica-ce`) |
| `DOCKER_HUB_TOKEN` | Docker Hub personal access token |

## Next Steps

1. Get Vertica container running (needs Docker Hub login on VM)
2. Investigate VoltDB replacement image
3. Seed data into each running database (`seed_data.py`)
4. Run `fivetran debug` for each connector to test connections
5. Run test scenarios against each connector
6. `fivetran deploy` to Fivetran workspace for end-to-end testing
