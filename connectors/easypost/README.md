# EasyPost Connector

Syncs shipments, trackers, tracking details, addresses and refunds from the [EasyPost API](https://docs.easypost.com/) using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `api_key` | EasyPost API key. A test key (starts with `EZTK`) reads test-mode data, a production key (starts with `EZAK`) reads live data. |

Keep real keys out of version control. `configuration.json` is gitignored in this repository.

The connector defines a setup form, so the key can be entered in the Fivetran dashboard instead. A setup test checks the key with a one-row request when the connection is saved. Locally, `fivetran configuration` collects the values and `fivetran configuration --test` runs the setup test.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `shipments` | `id` | Incremental window with lookback |
| `trackers` | `id` | Incremental window with lookback |
| `tracking_details` | `tracker_id`, `datetime`, `source` | Child rows of each synced tracker |
| `addresses` | `id` | Full refresh |
| `refunds` | `id` | Full refresh |

## Behavior notes

- The API has no "updated since" filter. Shipments and trackers are read by creation date, and each sync starts 30 days before the previous sync started so recent status changes are picked up. Records older than that window are not refreshed.
- `end_datetime` is always sent. Without it the API silently returns nothing for windows longer than about 30 days.
- The shipments list only returns purchased shipments.
- State is `last_sync_start`, saved in one checkpoint at the end of a sync.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the API key.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
