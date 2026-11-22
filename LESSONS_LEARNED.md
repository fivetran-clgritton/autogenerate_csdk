# Lessons learned — SDK test database setup (Nov 19–22, 2026)

What we tried while building the test VM and seeding the databases, what broke, and why. Current state, credentials and the runbook are in `CLAUDE.md`; this file is the history behind it. Start with the "Seeding status" table in `CLAUDE.md` for what is still open.

## 1. Where Claude Code runs

**Run Claude Code on the VM for anything that touches the databases.** Most of the lost time came from a cloud session that could edit code but could not reach the VM, so every command was relayed by hand.

- **Cloud sessions can't reach the VM.** `gcloud compute ssh --tunnel-through-iap` needs a WebSocket to `tunnel.cloudproxy.app`, which the egress proxy blocks. Direct TCP to DB ports is blocked too: traffic leaves through the proxy, so allowlisting the session's IP in the GCP firewall doesn't help.
- **What cloud sessions *can* do:** edit and push code, call GCP APIs with the service account (e.g. firewall rules), query BigQuery.
- **Firewall gotcha:** `gcloud compute firewall-rules update --allow=...` replaces the whole allow list. Pass every port, not just the new one.
- **Remote Control was unreliable as a relay:**
  - `claude remote-control` on the VM connected (despite a one-off "CCR v2 worker registration failed (404)" message), but the cloud session's folder list never showed it, so it couldn't attach directly.
  - A child session created on the VM's Remote Control environment could run commands. But it treated multi-step `sudo` instructions relayed from another session as prompt injection and refused them. The same instructions typed by the user in that chat worked.
  - It also ran a narrower command than asked (`seed_data.py --db exasol` instead of `seed_all.sh`), so check what a relayed session actually ran.
  - The connection dies with the terminal running `claude remote-control`. Use tmux.
  - It can't answer `sudo` password prompts.
- **Running everything from a desktop instead** was considered and rejected. Seeding 11M rows per DB over the internet is slow. Container logs and `docker exec` still need the VM. And several images (e.g. Exasol) are x86-only, so running them locally on a Mac is risky.

## 2. VM and shell gotchas

- **Two compose files.** The containers run from `/opt/sdk-test-dbs/docker-compose.yml`, a separate copy with fixed container names. Running `docker compose` from the repo directory (`/opt/sdk-test-dbs/connectors`) created a second project (`connectors-exasol-1`) with blank passwords, because `.env` lives one level up. It then failed on port conflicts. Editing the repo's compose file does nothing to the running containers until it is applied to the VM copy.
- **Python 3.14** had to be built from source (Debian only ships 3.11; deadsnakes is Ubuntu-only). The first venv was accidentally created with system 3.11, so `python` was the wrong interpreter. It was recreated with `python3.14 -m venv venv`.
- **Ownership:** the repo clone was root-owned; `seed_all.sh` now `chown`s it back to the login user.
- **Docker data dir:** after `systemctl restart docker`, `/mnt/data/docker` was missing and had to be recreated.
- **`set -e` doesn't catch a failure in the middle of an `&&` list.** `cd connectors && git pull && cd ..` failed at `git pull`, the script kept going in the wrong directory, and it ran `docker compose` against the repo's compose file (see above). Use absolute paths.
- **Bash keeps executing the old copy of a script that git replaces mid-run.** The first run after an update executed stale code: one run still used the old IRIS password, and a "fixed" `git pull` step never ran. `seed_all.sh` now resets the checkout and then `exec`s the fresh copy.
- **Local edits on the VM block `git pull`** ("would be overwritten by merge"). `seed_all.sh` hard-resets to `origin/main`, so the VM must not hold uncommitted fixes. Push them (needs credentials on the VM) or run with `SEED_SKIP_UPDATE=1`.
- **`… | tee log` hides the script's exit code.** Read the final summary line.

## 3. Per-database findings

| DB | What we learned |
|----|-----------------|
| **Exasol** | Docker CE has no TLS, but pyexasol 4.x defaults to TLS, which gave an SSL EOF error. Use the unencrypted port 8563 with `websocket_sslopt={"cert_reqs": 0}`. DDL needs `CREATE SCHEMA` + `OPEN SCHEMA` first. `import_from_iterable` appends and the tables have no PK, so a re-run doubled the data; the seeder now truncates first. 1M + 10M rows load in about 220 s. |
| **IRIS Community** | Forces a `_SYSTEM` password change on first login ("Password change required"). `##class(Security.Users).ChangePassword` doesn't exist in this version, so it was changed in the management portal on port 52773 (opened in the firewall for that). A later "Access Denied" probably came from a stale script using the old password — not yet confirmed. |
| **CUBRID** | The PyPI package is `CUBRID-Python` (imports as `CUBRIDdb`; `pip install CUBRIDdb` fails). Its latest release, 9.3.0.2, predates Python 3.14, and connections fail with `Invalid connection handle (-20002)`. Likely fixes: a separate 3.11 venv, or the JDBC driver via jaydebeapi. The connection string is `CUBRID:host:33000:testdb:::` (an f-string `{{}}` once added a stray `{}`). |
| **Mimer** | `mimerpy` needs the native `libmimerapi.so`, which pip doesn't install. `seed_all.sh` tries to copy it out of the container; the path inside the container is unverified. |
| **Geode** | The developer REST API is off by default. The locator in the same container already uses 7070, so the REST API moved to 8081. The REST API can't create regions (the original `POST /regions` was wrong); regions are created with gfsh via `docker exec`. One PUT per row for 11M rows was far too slow; it now uses multi-key PUTs of 500 and 100K/1M rows (tested only against a fake server). |
| **H2** | jaydebeapi needs a JVM (`default-jre-headless`). `EOFException` on 9092 is still unresolved. Likely causes: a client/server version mismatch with the 2.2.224 jar, or the database path. H2 2.x also refuses to create a database remotely unless the server runs with `-ifNotExists`. |
| **VoltDB** | The Developer Edition image exits 0 with no logs when no license file is provided. Skipped. |
| **Vertica** | Needs a Docker Hub login to pull `vertica/vertica-ce`. Not started. |

## 4. Process mistakes not to repeat

- **Untested fixes were reported as fixed.** Several were pushed without ever running against the real DB: the CUBRID string, the H2 path, the first Geode flags, and the IRIS reset via `docker exec`. Each cost the user a full VM round-trip. Run a fix against the database (small `--customers/--orders` counts first) before calling it done.
- **"`CREATE TABLE IF NOT EXISTS`" was mistaken for an idempotent load.** It only protects the DDL; inserts still duplicate.
- **Generic docs were applied to the specific setup.** The user was once told to run Remote Control on their laptop when the VM was the target.
- **Secrets ended up in a public repo.** DB passwords are in `CLAUDE.md`, `seed_all.sh` and `vertica/configuration.json`. Don't add more; moving them to a gitignored `.env` and rotating is pending the user's decision.
- **Firewall policy:** the user prefers adding their current IP per session over opening ports to wide ranges.
