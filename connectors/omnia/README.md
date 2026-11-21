# Omnia Retail Connector

Syncs pricing data from the Omnia Retail API using the Fivetran Connector SDK.

The endpoints, response envelope and field names in this connector are assumed from API conventions. They have not been verified against a live account, so confirm them with a real API key before relying on the output.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `api_key` | Omnia Retail API key, sent as `Authorization: Bearer <api_key>`. |

Keep real keys out of version control.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `products` | `id` | Full refresh (`GET /products`) |
| `pricing_strategies` | `id` | Full refresh (`GET /strategies`) |
| `price_history` | `product_id`, `timestamp` | Incremental from the latest timestamp seen (`GET /price-history?from=`) |

## Behavior notes

- Pagination uses `page` and `pageSize=500`. A response can be a bare list or an envelope with `data`, `items`, `products` or `strategies`, and may report `totalPages`. Each page is written as it arrives.
- Fields are read as camelCase first, then snake_case. The first value that is not null is used.
- State is `price_history_from`, saved in one checkpoint at the end of a sync.
- Requests retry on network errors, HTTP 429 and 5xx with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the API key.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
