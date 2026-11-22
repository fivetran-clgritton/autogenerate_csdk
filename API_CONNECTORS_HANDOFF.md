# API connectors handoff (Nov 22, 2026)

Notes for continuing the API connector work in a local Claude Code session. This is separate from the legacy-database test VM work described in `CLAUDE.md` and `LESSONS_LEARNED.md`.

## Where things stand

Eight Connector SDK connectors are refactored to the plugin standard, each with a setup form and connection test, and pushed to its own branch. Only AfterShip and EasyPost have run against a live API. The other six (Rillet, Omnia, Jobber, Moz, Degreed, Lazada) are checked against mocks only and need credentials.

AfterShip and EasyPost are deployed to Fivetran as custom connections, but both were redeployed before the setup form was added, and no sync has been started from the cloud session. 13 of the 19 sources originally built already have a native Fivetran connector, so the 11 connectors not yet refactored are on hold until someone decides whether the native one covers the need.

## Where everything lives

- **Branches:** each connector is on its own branch named `<source>-connector` (for example `aftership-connector`), with code under `connectors/<source>/`. All 19 branches exist. `main` also has AfterShip, merged by pull request before its setup form was added.
- **Config files:** `configuration.json` is gitignored and the committed copies hold placeholders only. This repo is public, so never commit a credential.
- **Deployed connections:** named `auto_` plus the API name (`auto_aftership`, `auto_easypost`).
- **Plugin repo:** `fivetran/connector_sdk_tools`. Install in Claude Code with `claude plugin marketplace add fivetran/connector_sdk_tools`, then `claude plugin install fivetran-connector-sdk@fivetran-connector-sdk-ai`. Skills load when a session starts, so install before opening the local session.

## Native connector coverage

13 of the 19 sources already have a native Fivetran connector; only Rillet, Omnia Retail, Jobber, Moz, Degreed and Lazada do not. The build skill says to show this to the user before building custom code. AfterShip, Semrush, Clio, Gusto and DrChrono are missing from Fivetran's category listing pages but have their own connector pages, so the listings are incomplete. Whether any of these is a Lite connector was not determined.

| Source | Native Fivetran connector | Refactored here |
| --- | --- | --- |
| AfterShip | [Yes](https://fivetran.com/docs/connectors/applications/aftership) | Yes |
| EasyPost | [Yes](https://fivetran.com/docs/connectors/applications/easypost) | Yes |
| Rillet | None | Yes |
| Omnia Retail | None | Yes |
| Jobber | None | Yes |
| Moz | None | Yes |
| Degreed | None | Yes |
| Lazada | None | Yes |
| Gladly | [Yes](https://fivetran.com/docs/connectors/applications/gladly) | No |
| Semrush | [Yes](https://fivetran.com/docs/connectors/applications/semrush) | No |
| Acumatica | [Yes](https://fivetran.com/docs/connectors/applications/acumatica) | No |
| Dayforce | [Yes](https://fivetran.com/docs/connectors/applications/ceridian-dayforce) | No |
| Clio | [Yes](https://fivetran.com/docs/connectors/applications/clio) | No |
| Gorgias | [Yes](https://fivetran.com/docs/connectors/applications/gorgias) | No |
| Gusto | [Yes](https://fivetran.com/docs/connectors/applications/gusto) | No |
| Sage Intacct | [Yes](https://fivetran.com/docs/connectors/applications/sage-intacct) | No |
| FreshBooks | [Yes](https://fivetran.com/docs/connectors/applications/freshbooks) | No |
| DrChrono | [Yes](https://fivetran.com/docs/connectors/applications/drchrono) | No |
| Navan | [Yes](https://fivetran.com/docs/connectors/applications/navan) | No |

## Plugin standards the connectors follow

The first versions were hand-written from general knowledge and broke several plugin rules. All eight refactored connectors now follow the rules below, which come from `fivetran/connector_sdk_tools`.

| Rule | Where it comes from |
| --- | --- |
| Check for a native or Lite connector before building custom code | build-connector skill, Phase 0 |
| Scaffold with `fivetran init`, never hand-write (not done here; the cloud container could not reach the template) | build-connector skill, Phase 2 |
| Call `op.upsert` and `op.checkpoint` directly, with no `yield` | `canonical/workflows/generator.md`, Operations |
| Call `validate_configuration()` at the top of `update()` | `generator.md`, connector.py |
| Declare `table` and `primary_key`; declare a column type only to force one | `sdk-reference.md`, Schema Definition |
| `configuration.json` is flat and every value is a string (booleans become "true"/"false") | `sdk-reference.md`, configuration.json Rules |
| Do not list `requests` or `fivetran-connector-sdk` in `requirements.txt`; delete the file if empty | `sdk-reference.md`, Dependency Declaration |
| Retry network errors, 429 and 5xx with backoff and honor `Retry-After`; fail fast on 401 and 403 | `sdk-reference.md`, Error Handling |
| Never log per record; log per page or table | `sdk-reference.md`, Logging |
| First two lines are the AI build marker | `generator.md`, connector.py |
| Offer a setup form and add it only with the user's agreement | `generator.md` lines 68-83; `sdk-reference.md` line 705 |
| Do not ask users to paste secrets into chat | build-connector skill, Phase 5 |

## API behavior found by testing live

A sync that succeeds with zero rows is not a pass. EasyPost's first run reported success with 0 rows, and only a direct API query showed the connector was wrong. Compare row counts against the API before trusting a sync.

**AfterShip**

- The Tracking API moved to a dated path, `/tracking/2024-10/...`. The old `/v4/` path returns 404. Auth is the `as-api-key` header.
- Pagination is by cursor (`pagination.has_next_page` and `next_cursor`), not page numbers.
- The trial is 7 days. It exposed trackings and couriers only; notifications, estimated-deliveries, courier-connections, webhooks, organizations and users all returned 404. The couriers list had 9 entries.
- Estimated delivery objects and `next_couriers` are nested in each tracking, so the connector writes them to their own child tables.

**EasyPost**

- Auth is HTTP Basic with the API key as the username and an empty password. Test keys start with `EZTK` and production keys with `EZAK`. Lists page with `before_id` and `has_more`, up to 100 per page.
- List endpoints silently return nothing for a date window longer than about 30 days unless `end_datetime` is also sent. This hit both shipments and trackers.
- The shipments list returns only purchased shipments.
- The tracker `created_after` filter is ignored. A value in 2030 still returned every tracker, so the original incremental state never filtered anything. The fix is an explicit start and end window plus a 30-day lookback, so records older than 30 days are not refreshed.
- The first version declared an `addresses` table but never synced it.
- The test account was empty. One test shipment, a purchased rate and a tracker were created in test mode, and they remain in that account.

## Test, configure and deploy

Run these from a connector's folder, on its branch.

```bash
pip install fivetran-connector-sdk
fivetran configuration --test      # runs the setup test with the values in configuration.json
fivetran debug                     # full local sync into files/warehouse.db (DuckDB, schema tester)
fivetran debug --configuration /path/outside/repo/config.json
```

Use `fivetran debug`, not `python connector.py debug`; the second failed to find the configuration. Deploy with:

```bash
fivetran deploy . --api-key <base64 key> --destination <destination> \
  --connection auto_<api> --configuration /path/outside/repo/config.json --python 3.13 --yes
```

- There is no `--api-secret` flag. The API key is one base64 string, and `--connection` is required.
- `--yes` and `--non-interactive` cannot be combined.
- A redeploy needs `--python`, and it keeps the stored configuration if `--configuration` is left out.
- The first deploy copied the settings of 247 existing connections into `~/.fivetran/connector_sdk/options.toml` in the cloud container.

**Environment quirks**

- The cloud container blocks outbound hosts that are not allowed. `api.aftership.com` returned a proxy 403, and browser sessions could not reach signup pages. Fix it under the environment's Network access settings by allowing the host.
- The first SDK install failed building `docopt`. A partial install with `--no-deps` then left modules missing. A plain `pip install fivetran-connector-sdk` worked once setuptools had been upgraded.
- `fivetran init` could not fetch its template because `api.github.com` was blocked, so the connectors were not scaffolded.
- `fivetran debug` deletes an empty `requirements.txt`, which is fine.
- A stop hook that reports unpushed commits is usually a stale remote ref in the shallow clone. Run `git fetch origin refs/heads/<branch>:refs/remotes/origin/<branch>`.
- Before each commit, restore placeholders with `git checkout -- configuration.json` and delete `files/`, `crash-reports/` and `__pycache__`.

## Status and what is unverified

All eight have a setup form and a connection test on their branch. "Mocked" means a patched `requests` layer, which cannot confirm endpoint paths or field names.

| Connector | Sync test | Setup test | Deployed | Still unverified |
| --- | --- | --- | --- | --- |
| AfterShip | Live, 15 rows across 4 tables | Live, pass and fail | `auto_aftership` | Nothing known |
| EasyPost | Live, run twice with lookback | Live, pass and fail | `auto_easypost` | Refunds table is empty; no refunds exist in the test account |
| Rillet | Mocked | Mocked; a real call failed cleanly | No | `/accounts` and other paths, `next_cursor` pagination, `updated_at_min` filter, field name casing |
| Omnia Retail | Mocked | Mocked | No | `/products` endpoint, response envelope and field names, all assumed |
| Jobber | Mocked | Mocked; a real call returned 401 | No | GraphQL field names and the `updatedAt` filter, no API version header sent, throttle detection is a guess, `account { id name }` query |
| Moz | Mocked | Mocked; a real call returned 401 | No | Response field names, 50-item page cap |
| Degreed | Mocked | Mocked; a real call was rejected (HTTP 400) | No | Endpoint paths, field names, `next` cursor format, `users?limit=1` |
| Lazada | Mocked | Mocked; a real call returned `InvalidAppKey`, so signing works | No | `ApiCallLimit` as the rate-limit code, timestamp format (no longer typed) |

Behavior changes made while refactoring: Moz now fails fast on empty `targets`, Lazada streams orders per page, and Omnia, Degreed and Rillet keep a value of 0 instead of turning it into null.

## Next steps and open decisions

- [ ] Get credentials for the six untested connectors. For each, run `fivetran configuration --test` first, then `fivetran debug`, and compare row counts with the API.
- [ ] Redeploy `auto_aftership` and `auto_easypost` so the deployed code includes the setup form.
- [ ] Start the initial syncs from the Fivetran dashboard. None has been started from the cloud session.
- [ ] Decide whether to refactor the 11 connectors that have a native Fivetran equivalent. Build them only if the native connector misses what the Gong request asked for.
- [ ] Rotate the Fivetran API key and the AfterShip and EasyPost keys. They were pasted into a chat session.
- [ ] Optional: scaffold with `fivetran init` and run the `evaluate-connector` skill on each connector from a local session with the plugin installed.

Open question: the Gong requests behind the 13 sources with native connectors may have been for something the native connector lacks, such as extra tables.
