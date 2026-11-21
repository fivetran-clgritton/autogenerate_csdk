# Lazada Connector

Syncs orders, order items, products and product SKUs from the [Lazada Open Platform](https://open.lazada.com/) API using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `app_key` | App key from the Lazada Open Platform console. |
| `app_secret` | App secret, used to sign every request. |
| `access_token` | Seller access token obtained through the OAuth authorization flow. |
| `region` | One of `my`, `th`, `sg`, `ph`, `id`, `vn`. Defaults to `my`. |

Keep real credentials out of version control. `configuration.json` is gitignored in this repository.

The connector defines a setup form, so the values can be entered in the Fivetran dashboard instead. A setup test checks the credentials with a one-row signed request when the connection is saved. Locally, `fivetran configuration` collects the values and `fivetran configuration --test` runs the setup test.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `orders` | `order_id` | Incremental on `created_at` |
| `order_items` | `order_item_id` | Fetched for each synced order |
| `products` | `item_id` | Incremental on `updated_time` |
| `product_skus` | `shop_sku` | Child rows of each synced product |

## Behavior notes

- Requests are signed with HMAC-SHA256 (endpoint path plus sorted parameters). Each retry is re-signed with a fresh timestamp.
- Orders and products use offset pagination (100 per page) and are written page by page.
- State holds `created_after` and `update_after`, saved in one checkpoint at the end of a sync.
- Requests retry on network errors, HTTP 429 and 5xx, and on the `ApiCallLimit` error code, with exponential backoff. HTTP 401 and 403, and any other non-zero API `code` in a response body, fail the sync with the API message.
- Column types are inferred. Timestamps are delivered as the strings Lazada returns.
- This connector has not been run against a live Lazada account.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
