# Geode connector: porting notes (2026-11-22)

`geode/connector.py` replaces the old hardcoded connector (fixed `customers` / `orders` regions,
`id` / `updated_at`, one sampled entry, `_at`-suffix timestamp guessing) with a port of the IRIS
template. Geode is a key-value data grid, not a SQL database, so the requirements map onto it as
described below. Geode-specific parts are marked `Geode:` in the code.

Tested against Apache Geode 1.15.1 (docker `apachegeode/geode`, one locator, one server, dev REST
API on 8081) with fivetran-connector-sdk 3.1.2 and requests 2.34.2 (system Python 3.11 of the
`fivetran` CLI).

## How the requirements map to Geode

| Requirement | Geode |
|---|---|
| 1. Discover tables and columns | Regions from the dev REST API (`GET /geode/v1`). Fields by **sampling** entries: `sample_size` (default 1,000) entries in the region's own order, plus, for incremental regions, the `sample_size` entries with the highest cursor values (new fields show up in recent writes, which an arbitrary sample of a large region can miss). A value that is not an object (e.g. a plain string) goes into a column `value`. |
| 2. Scope the sync | `include_tables` / `exclude_tables` (region names, case-insensitive). No schemas: Geode has none, so every region goes to the connection's destination schema and no operation passes `schema=`. Internal regions (`__*`) are excluded unless named in `include_tables`. |
| 3. Primary keys | The **region key**, delivered in its own column (`key_column`, default `_key`; `STRING` or `LONG`). Only string and integer keys are supported; regions with other key types (e.g. `Double`, objects, mixed) are skipped with one `op.warning` listing them. Every Geode entry has a key, so no region is skipped for lacking one. |
| 4. Sync strategy | Incremental on the first `cursor_columns` field (default `updated_at`) whose sampled values are all ISO-8601 timestamp/date strings of one form, or all integers; `cursor_overrides` (`region=field`, `region=` for a full reload). Otherwise full reload with a deferred truncate. Entries without the cursor field, or with it NULL, are re-read every sync. |
| 5. Bounded reads | Two-step keyset reads (below). |
| 6. Checkpoint cadence | Unchanged: 60 s or 100k rows (scaled down for wide regions; 1,000 rows when sampled values reach 100 KB), plus at the end of each region; mid-region checkpoints store the keyset position. |
| 7. Schema drift | New fields picked up (from the samples; fields not sampled are still delivered and inferred by the SDK, and declared on the next sync). Fields that disappear from the samples are logged; the destination keeps the column. |
| 8. SDK reference | `op.*` called directly; `/evaluate-connector` run (see the end). Pure Python (`requests`, pre-installed), so no custom driver is needed. |

State layout is the template's, keyed by region name instead of `schema.table`:
`{"tables": {"<region>": {"mode", "cursor_column", "cursor", "after", "columns"}}}`.

## Design

- **Two-step reads.** OQL has no `OFFSET`, no server-side cursor, and the REST API returns each query
  result as one JSON document. Each batch is
  `SELECT e.key AS k, e.value."<cursor>" AS c FROM /<region>.entrySet e WHERE ... ORDER BY e.value."<cursor>", e.key LIMIT 100000`
  (keys only for a full reload), then the values are fetched by key with `e.key IN SET(...)`, which
  Geode answers as a direct lookup (500 keys in about 40 ms on the 1M-entry region). Measured on the
  seeded `orders` (1M entries): an `ORDER BY` query that returns whole values took 15 s per batch
  regardless of `LIMIT` (it sorts the values); returning only key and cursor took 5.7 s; a filtered
  window of 28k entries took 1.7 s.
- **Value chunks** are bounded by URL length (Jetty rejects request lines over 8 KB with HTTP 414,
  so the encoded query is kept under 7,000 bytes) and by an estimate of the response size
  (16 MB, from the largest sampled value and the largest average seen per response).
- **Keyset conditions** are expanded into OR/AND form. Integer keys and cursors use `nL` literals
  (OQL comparisons promote Integer/Long); `IN SET` compares with `equals()`, so integer keys are
  listed as both `n` and `nL`.
- **No bind parameters.** The REST API binds parameters only for named queries, which are stored on
  the server (`POST /queries`); a sync should not write to the cluster. Literals are escaped
  (`'` doubled) and field names are double-quoted identifiers (`e.value."updated_at"`).
- **The REST API URL-decodes the query a second time** (`java.net.URLDecoder`). Found when a key
  `%25` silently went missing (it was looked up as `%`); a lone `%` fails the request and `+`
  becomes a space (which would break a `+02:00` cursor). The client escapes `%` and `+` before
  sending. Keys that vanish between the ordered query and the value fetch are counted and logged
  (normally entries deleted during the sync).
- **Partitioned regions** require every `ORDER BY` expression in the projection (found by the
  scenario fixture, which uses a PARTITION region for `fttest_orders`).
- **Missing vs NULL cursor.** `IS_DEFINED(f) AND f != null` selects entries with a cursor;
  `IS_UNDEFINED(f) OR f = null` the rest (`!= null` alone includes entries without the field, and
  they sort first).
- **Samples** are profiled as they stream in (field kinds, largest value size); the values are not
  kept. Profiles are cached between `schema()` and `update()` of one sync, so the recent-entries
  sort runs once per sync.
- **Subregions** are listed by the REST API under their leaf name only and cannot be queried by it;
  they are skipped with a warning. Regions whose names collide case-insensitively, regions whose
  values have fields that differ only in case, and regions with a field named like `key_column` are
  skipped with a warning.
- **Connection:** `host`, `rest_port`, optional `username`/`password` (HTTP basic auth, for a
  cluster with a security manager; needs `DATA:READ`), `use_https`, `validate_certificate`.
  Connection failures and HTTP 429/502/503/504 are retried 3 times (5 s, 10 s); other HTTP errors fail
  at once with Geode's message. Error and retry messages never include the request URL (it holds key
  values). Auth and HTTPS are **untested** (the test cluster has neither).

## Type mapping

Inferred per field from the sampled values; every type is declared, because the SDK infers an
undeclared float as 32-bit `FLOAT` and guesses types from string content (found on the Exasol and
H2 ports).

| Sampled values | Declared | Delivered |
|---|---|---|
| booleans | `BOOLEAN` | bool |
| integers within 64 bits | `LONG` | int |
| floats, or floats and integers | `DOUBLE` | float |
| ISO-8601 timestamps without a zone (`T` or space, optional fraction) | `NAIVE_DATETIME` | `datetime` (fraction truncated to microseconds) |
| ISO-8601 timestamps with `Z` or an offset | `UTC_DATETIME` | `datetime` converted to UTC |
| `YYYY-MM-DD` | `NAIVE_DATE` | `date` |
| objects or arrays (nested PDX) | `JSON` | dict / list |
| other strings, integers beyond 64 bits, or mixed kinds | `STRING` | str (non-strings as JSON text) |
| only NULL | not declared | |

`detect_timestamps=false` keeps timestamp and date strings as `STRING` (they can still be cursors).
A value that does not fit its field's declared type (the sample did not see that kind of value) is
delivered as NULL, and one `op.warning` per region lists the fields and counts, suggesting a larger
`sample_size` or `detect_timestamps=false`.

Seeded `orders` lands as `_key VARCHAR, id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR,
created_at TIMESTAMP, updated_at TIMESTAMP`.

## Edge-case tests (test regions `fttest_*`)

Regions created with gfsh (all `REPLICATE`): `fttest_types` (every JSON value kind: ints of each
width, a 30-digit integer, doubles incl. `1e-300`/`-1e300`, booleans, strings that look like
numbers/booleans/dates, empty and unicode strings, naive/fractional/`Z`/offset timestamps, dates,
nested object, array, explicit null; entries without `updated_at` and with it NULL),
`fttest_strkey` (string keys with quote, comma, double quote, slash, space, braces, `;`, `&`, `%`,
`%25`, `%41`, `+`, Cyrillic; tied cursor values), `fttest_intkey` (`Integer` keys, string values),
`fttest_longkey` (`Long` keys incl. 9,000,000,000), `fttest_scalar` (string values),
`fttest_dblkey` (`Double` key: must be skipped), `fttest-hyphen` (empty, hyphenated name),
`fttest_parent` + subregion `child` (must be skipped).

| Test | Result |
|---|---|
| Initial sync; type fidelity | Pass: types as in the table above; ambiguous strings stay `VARCHAR`; offset timestamp converted to UTC; doubles exact |
| Unsupported key type, subregion, unknown `include_tables` name | Skipped / warned with one `op.warning` each |
| NULL and missing cursor | Re-sent every sync |
| Second sync: drop a field, add a field, update, hard delete (incremental), delete in a full-reload region, insert at exactly the saved cursor | Pass: drift warning for the new field, dropped field logged and NULL in re-synced rows; `>=` picked up the 4 tied rows and the new row; incremental hard delete not seen (expected); full-reload delete flagged `_fivetran_deleted` |
| `cursor_overrides`: `fttest_strkey=` (full), `fttest_types=id` (integer cursor), an unknown region | Both strategy changes reset and re-synced; warning for the unknown name; an override on a string field fails with a clear message |
| Resume from hand-written `after`: integer cursor `[1, "1"]`, full reload `["it's"]` | Exact: 2 and 9 rows; no second truncate |
| `KEY_BATCH_SIZE = 2`, `MAX_URL_BYTES = 260` (scratch copy) with tied cursors | 9 ordered queries for 17 rows; destination identical to the default run |
| Value of an unexpected kind with `sample_size=1` | Delivered NULL, one warning with counts |
| Closed port | 3 attempts with backoff, clear failure, state unchanged; no URL in the messages |

## Test scenarios (small fixture)

Run with `geode/scenarios.py` (usage in its docstring; about 2 minutes, no lock). It recreates
`fttest_customers` (REPLICATE, seeded entries with id <= 1,000) and `fttest_orders` (**PARTITION**, id
<= 10,000) with gfsh and copies the seeded entries with the same keys; the seeded regions are only
read. After every scenario it compares every entry and field of the source with the destination and
checks the upsert count against the entries `cursor >= saved cursor` selects. Scenarios 7 and 12 run
patched copies of the connector. "Add/drop column" means adding a field to entries / removing it from
them.

| # | Scenario | Result |
|---|---|---|
| 1 | Initial sync | Pass: 11,000 upserts; every field equal. Largest scenario sync peaked under 50 MB |
| 2 | 500 new orders | Pass: 502 upserts (500 + 1 boundary entry per region) |
| 3 | 100 updates, all with the same new `updated_at` | Pass: 601 upserts (the 500 entries tied at the saved cursor are re-read, as `>=` intends); new amounts in the destination |
| 4 | 50 soft deletes (`status = 'cancelled'`) | Pass: 151 upserts |
| 5 | 10 hard deletes | Known gap: the entries stay in the destination, not flagged deleted |
| 6 | Outage: closed port, then a 200-entry backlog | Pass: clear failure after 3 attempts, state file unchanged; next sync picked up all 200 |
| 7 | Mid-sync failure: crash injected after 5 checkpoints of 1,000 rows (3,000 orders in), 300 entries inserted, then resume | Pass: `fivetran debug` kept the last checkpoint (mid-region `after` position); resume upserted 7,991; destination equals source, no duplicates |
| 8 | Add `discount_pct` (float) to 100 entries | Pass: column appears as `DOUBLE`, values on those rows, NULL elsewhere; drift warning |
| 9 | Add `notes` to 10 entries, sync, remove it from them (bumping `updated_at` on 20), sync | Pass: removal logged, sync continues; the destination keeps the column; the 20 re-synced rows have `notes` NULL |
| 10 | 5 entries with an 850,000-character `payload` (multi-byte characters, about 1 MB of JSON each) | Pass: compared in full, sha256 equal; value chunks shrank to fit the response-size target |
| 11 | 20 entries at exactly the saved cursor value | Pass: all 20 picked up (`>=`) |
| 12 | Writes from a second client between the 2nd and 3rd ordered query of a 3,000-entry sync (scratch copy, `KEY_BATCH_SIZE = 1000`) | Pass, with a known gap: entries updated after being read were re-read later in the same sync; entries updated before being read were read once with new values; new entries arrived. Entries updated with an **old** `updated_at` (a late commit below the cursor) are never picked up |

The first full run failed at scenario 1: the recent-entries sample ordered by a field it did not
project, which a PARTITION region rejects (`Query contains atleast one order by field which is not
present in projected fields`). Fixed; the table above is the run after the `/evaluate-connector`
fixes.

Known gaps (cursor-based sync, by design): hard deletes; entries written with a cursor value below
the saved cursor; a new field added to existing entries without bumping the cursor.

## Full initial sync (seeded `customers` 100k, `orders` 1M)

Run under the shared lock with `--naming SOURCE` and `PYTHONUNBUFFERED=1` (2026-11-22), from empty
state, `include_tables=customers,orders`:

- **SYNC SUCCEEDED: 1,100,000 upserts in 3 min 45 s, 13 checkpoints. Peak memory 571 MB** (SDK report;
  average 438 MB). The old hardcoded connector took about 6 minutes and 393 MB for the same data.
- `customers` (100k): 14.9 s = query 1.0 s, fetch 4.6 s, convert 0.4 s, upsert 6.3 s, checkpoint 2.0 s;
  2 ordered queries.
- `orders` (1M): 199.7 s = query 57.6 s (11 ordered queries of 100k, avg 5.2 s, slowest 8.6 s), fetch
  46.6 s (about 2,000 value queries), convert 4.5 s, upsert 62.7 s, checkpoint 20.1 s; GC 9.9 s. About
  50 s per 250k rows throughout; no slowdown over the run.
- Content comparison (`geode/compare_full_sync.py`): **all checks passed** for both regions.
  - Counts, `_fivetran_deleted` all false, distinct ids, distinct keys.
  - Sums: `id`, `customer_id`, `amount` (2,499,456,336.75, summed exactly on both sides).
  - Min, max and non-null count of `created_at` and `updated_at`.
  - Per-status counts.
  - A 2,000-row field-by-field sample per region, 0 mismatches.
- Geode's own OQL `sum()` over the `amount` doubles returns 2,499,456,260.00, which is wrong by 76.75
  (it loses precision). The exact sum of every source value equals the destination's, and the sorted
  list of all 1M source amounts equals the destination's. The comparison script sums doubles in
  Python for that reason.
- A follow-up sync from the saved state re-read only the entry at each cursor value (2 upserts) in
  11.5 s, most of it the two sampling queries (the recent-entries sample sorts `orders`).

## Open questions / not tested

- **Java domain objects.** All test data was written through the REST API as JSON, so values are PDX
  instances of `__GEMFIRE_JSON`. Customers mostly store Java classes (PDX or Java-serialized). How the
  REST API renders those (e.g. `java.util.Date` fields, which would not be ISO strings, and so not
  usable as cursors) is untested. A Java-serialized value the server cannot turn into JSON would fail
  the query.
- **Sampling limits.** Types come from at most 2 × `sample_size` entries. A rare field or rare value
  kind can be missed: an unseen field is still delivered (SDK-inferred, declared on the next sync); an
  unseen kind becomes NULL with a warning. If a later sync widens a type (e.g. `NAIVE_DATETIME` to
  `STRING` once an odd value is sampled), the declared type changes; not tested against a real
  destination.
- **Cost on large regions.** Every ordered query scans the region and sorts the matching entries
  (no index is used for `ORDER BY`); the recent-entries sample adds one sort per incremental region per
  sync. On 1M entries that is about 6 s per query. Regions of tens of millions of entries would pay
  minutes per batch. Windowing the cursor range (as the Redshift reference does) would bound each sort;
  not implemented.
- **String cursors** are compared by Geode as strings: correct for one consistent ISO-8601 format,
  wrong if writers mix formats (e.g. `...30Z` vs `...30.5Z`, or different offsets).
- **op.warning** is limited to 10 per sync by the SDK; with many regions, per-region drift or
  mismatch warnings beyond 10 are dropped (they are also in the logs).
- Expiration / eviction: entries removed by expiration look like hard deletes (not captured by
  incremental regions).
- Auth (`username`/`password`) and HTTPS are implemented but untested.
- No `configuration_form` (none of the ports has one).

## Changes made outside the connector

- Created test regions in the Geode cluster (gfsh, REPLICATE unless noted): `fttest_types`,
  `fttest_strkey`, `fttest_intkey`, `fttest_longkey`, `fttest_scalar`, `fttest_dblkey`, `fttest-hyphen`,
  `fttest_parent` with subregion `child`, and the scenario fixture `fttest_customers` / `fttest_orders`
  (PARTITION). They are in the cluster configuration, so they survive restarts. The seeded
  `customers` / `orders` were only read.
- While probing, one named query (`fttest_probe`) was created through `POST /queries` and deleted again.

## /evaluate-connector

Run after the first passing scenario run (static review against the plugin's SDK reference).
Fixed, then the edge cases and all 12 scenarios were re-run:

- **Required, memory:** samples held up to `sample_size` whole values per region in memory (and in
  the cache across regions); with values of about 1 MB that is about 1 GB per region. Now values are
  fetched in size-bounded chunks (the first chunk assumes 1 MB values) and only a profile is kept.
- **Required, security:** connection-retry warnings and errors included the request URL, whose query
  string holds region keys (possibly personal data). The URL is now replaced by `<query>`.
- **Good to have:** `requirements.txt` listed `requests` (pre-installed); now empty. Transient HTTP
  statuses (429, 502, 503, 504) are now retried with backoff; 500 is not, because Geode uses it for
  OQL errors.

Remaining good-to-have items, deliberate or open: every column type is declared (needed, see
**Type mapping**); no `configuration_form`; `_discover` and `_build_spec` are long; string cursor
comparison without parsing (Geode compares the strings itself, see **Open questions**).
