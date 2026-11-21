# Degreed Connector

Syncs learning data from the Degreed API (v2) using the Fivetran Connector SDK. The connector authenticates with OAuth 2.0 client credentials and reads JSON:API responses.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `client_id` | Degreed API client ID. |
| `client_secret` | Degreed API client secret. |
| `region` | `us`, `eu` or `ca`. Defaults to `us`. Selects the Degreed host. |

Keep real credentials out of version control.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `users` | `id` | Full refresh |
| `completions` | `id` | Incremental: `start_date` is the latest `completed_at` seen |
| `learning_paths` | `id` | Full refresh (`pathways` endpoint) |
| `skills` | `id` | Full refresh |
| `user_skills` | `user_id`, `skill_id` | Read in full each sync; latest `evaluated_at` is kept in state |
| `content` | `id` | Full refresh |

## Behavior notes

- A token is requested once per sync and refreshed once if an API call returns HTTP 401.
- Pages (100 records) are written as they arrive, following the `links.next` cursor.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. Rejected credentials or HTTP 403 fail the sync with a message.
- Checkpoints are saved after users, completions and user skills, and once more at the end.
- Endpoint paths, field names and the cursor format follow Degreed's public API conventions and have not been verified against a live tenant.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
