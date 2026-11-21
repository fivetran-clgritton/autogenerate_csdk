# Moz Connector

Syncs SEO link metrics from the [Moz Links API v2](https://moz.com/products/api) using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `access_id` | Moz API Access ID, from Account, API Access in Moz. |
| `secret_key` | Moz API Secret Key. |
| `targets` | Comma-separated domains or URLs to track, for example `example.com,example2.com`. |

Keep real credentials out of version control. `configuration.json` is gitignored in this repository.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `url_metrics` | `page`, `target` | Full refresh |
| `linking_domains` | `target`, `source_domain` | Full refresh |
| `top_pages` | `target`, `page` | Full refresh |
| `anchor_text` | `target`, `anchor_text` | Full refresh |

## Behavior notes

- Authentication is HTTP Basic with `access_id` and `secret_key`. All Moz Links endpoints are called with POST.
- `url_metrics` requests targets in batches of 10. The other tables page with `limit` 50 and `offset` per target.
- All tables are full refreshes, so state stays empty and the checkpoint marks the end of a successful sync.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the credentials.
- A sync fails before any request if `access_id`, `secret_key` or `targets` is missing or empty.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
