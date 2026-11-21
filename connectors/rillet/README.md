# Rillet Connector

Syncs accounting data from the Rillet API using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `api_key` | Rillet API key, sent as a Bearer token. |
| `use_sandbox` | `"true"` reads from `sandbox.api.rillet.com`, `"false"` from `api.rillet.com`. Defaults to `"false"` if omitted. |

Keep real keys out of version control. `configuration.json` is gitignored in this repository.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `accounts` | `id` | Full refresh |
| `transactions` | `id` | Incremental on `updated_at` |
| `journal_entries` | `id` | Incremental on `updated_at` |
| `journal_entry_lines` | `id` | Child rows of each synced journal entry |

## Behavior notes

- Endpoints, cursor pagination (`next_cursor`) and the `updated_at_min` filter are assumptions that have not been checked against a live Rillet account. Verify them with a sandbox key.
- Field names are accepted in both snake_case and camelCase because the API's naming is not confirmed.
- Pages are written as they arrive. State holds `transactions_updated_at` and `journal_entries_updated_at`, checkpointed after each incremental table.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the API key and sandbox setting.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
