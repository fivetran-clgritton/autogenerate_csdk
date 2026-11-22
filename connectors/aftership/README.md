# AfterShip Connector

Syncs shipment tracking data from the [AfterShip Tracking API](https://www.aftership.com/docs/tracking) (version 2024-10) using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `api_key` | AfterShip API key. Create one under Settings, API Keys in the AfterShip dashboard. |

Keep real keys out of version control. `configuration.json` is gitignored in this repository.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `trackings` | `id` | Incremental on `updated_at` |
| `checkpoints` | `tracking_id`, `checkpoint_time`, `slug` | Child rows of each synced tracking |
| `estimated_deliveries` | `tracking_id`, `source` | Child rows of each synced tracking |
| `next_couriers` | `tracking_id`, `slug` | Child rows of each synced tracking |
| `couriers` | `slug` | Full refresh |

## Behavior notes

- Trackings use cursor pagination (200 per page) and are written page by page.
- State is `updated_at_min`, the latest `updated_at` seen, saved in one checkpoint at the end of a sync.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the API key.
- Some endpoints (notification settings, courier connections) are not available on trial plans and are not synced.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
