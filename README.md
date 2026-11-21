# autogenerate_csdk

[Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk) connectors for legacy databases that have no managed Fivetran connector and aren't in [`fivetran/community_connectors`](https://github.com/fivetran/community_connectors). Databases are prioritized by how often they come up on sales calls (Gong) and how feasible they are to build.

## Connectors

One directory per connector, each with `connector.py`, `configuration.json` and `requirements.txt`.

| Connector | Driver | Sync strategy | Status |
|-----------|--------|---------------|--------|
| `vertica` | vertica-python | cursor on `updated_at` | Not yet tested (no test instance running; likely needs the same SDK-API fixes as the others) |
| `iris` | sqlalchemy-iris | cursor on `updated_at` | Smoke-tested (small incremental sync only) |
| `exasol` | pyexasol | cursor on `updated_at` | Smoke-tested (small incremental sync only) |
| `geode` | requests (dev REST API + OQL) | cursor on `updated_at` | Smoke-tested (small incremental sync and one full initial sync) |
| `mimer` | mimerpy | cursor on `updated_at` | Smoke-tested (small incremental sync only) |
| `cubrid` | jaydebeapi + JPype1 (JDBC) | cursor on `updated_at` | Smoke-tested (small incremental sync only) |
| `h2` | jaydebeapi + JPype1 (JDBC) | cursor on `updated_at` | Smoke-tested (small incremental sync only) |
| `voltdb` | requests (REST) | cursor on `updated_at` | Skipped (the free image needs a license file) |

"Smoke-tested" means the connector ran a small incremental sync under `fivetran debug` with row counts matching the source. Full-volume syncs, deletes, schema drift and failure recovery are not tested yet (see `test_scenarios.sql`).

All connectors sync two tables, `customers` and `orders`, incrementally using an `updated_at` cursor.

## Running a connector

Install the [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk/setup-guide) and the connector's requirements, put real connection details in a **local copy** of the configuration, and run:

```bash
pip install -r <connector>/requirements.txt
fivetran debug <connector> --configuration my-config.json --state my-state.json
```

Notes:

- Keep real credentials in a config file outside the repo. The checked-in `configuration.json` files only hold placeholders.
- `fivetran debug` writes the final checkpoint back to the `--state` file, and creates `files/` (a DuckDB warehouse you can open with `duckdb -ui files/warehouse.db`) and `crash-reports/`. Both are gitignored.
- The CUBRID and H2 connectors talk JDBC through `jaydebeapi`, which needs Java and the vendor's JDBC jar. Point to it with `jdbc_jar` (CUBRID) or `h2_jar` (H2) in the configuration. The jar should match the server version.
- The SDK requires `DECIMAL` columns to declare `precision` and `scale` in `schema()` (and `DECIMAL` values as strings), and `UTC_DATETIME` values as ISO 8601 with a time zone.
- All connectors read with `updated_at >= cursor`, so rows sharing the cursor timestamp are never skipped. Re-read rows are harmless because they are upserted by primary key.
- The Mimer connector needs `MIMER_SQLHOSTS` pointing at an SQLHOSTS file that maps `mimerdb` to the server.

## Test data

`seed_data.py` loads deterministic test data (fixed Faker and random seeds) into each database:

| Table | Columns | Rows |
|-------|---------|------|
| `customers` | `id`, `name`, `email`, `created_at`, `updated_at`, `status` | 1,000,000 |
| `orders` | `id`, `customer_id`, `amount`, `status`, `created_at`, `updated_at` | 10,000,000 |

Geode is loaded over REST, so it gets 100,000 customers and 1,000,000 orders. Timestamps fall between 2022 and 2024.

```bash
# Try one database with small counts first
python seed_data.py --db h2 --host localhost --customers 1000 --orders 10000

# Seed any subset of exasol, iris, cubrid, mimer, geode, h2 (no arguments = all six)
sudo bash seed_all.sh iris cubrid
```

Every seeder clears its tables first, so re-running after a partial failure is safe. `seed_all.sh` resets the checkout to `origin/main` before running; set `SEED_SKIP_UPDATE=1` to run local edits as-is.

To test a connector without syncing the full 11M rows, start from a late cursor, for example `{"customers_cursor": "2024-12-30 12:00:00", "orders_cursor": "2024-12-31 12:00:00"}`.

`docker-compose.yml` describes the test databases. The seeders and `seed_all.sh` expect the containers to be named after the service (`iris`, `cubrid`, `mimer`, `h2`, `geode`, `exasol`).

## Test scenarios

`test_scenarios.sql` holds 12 scenarios to run against each connector:

1. Initial sync baseline
2. Incremental: new rows
3. Incremental: updates
4. Soft deletes
5. Hard deletes
6. Prolonged outage
7. Mid-sync failure
8. Schema drift: add column
9. Schema drift: drop column
10. Large payload rows
11. Cursor boundary fence-post
12. Concurrent writes during sync

## Planned connectors

In priority order: Vertica, InterSystems IRIS / Caché, PICK/MultiValue (UniVerse, UniData, jBASE), IBM TM1 / Planning Analytics, Netezza, IBM Domino / Lotus Notes, CA IDMS, Progress OpenEdge, VSAM, and BAAN / Infor LN / M3.
