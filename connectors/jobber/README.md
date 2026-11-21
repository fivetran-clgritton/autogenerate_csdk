# Jobber Connector

Syncs clients, jobs, quotes, invoices and expenses from the [Jobber GraphQL API](https://developer.getjobber.com/docs) using the Fivetran Connector SDK.

## Configuration

`configuration.json` holds flat string values:

| Key | Description |
|---|---|
| `access_token` | Jobber OAuth access token. Create an app at developer.getjobber.com and complete the OAuth flow to get one. |

Keep real tokens out of version control. `configuration.json` is gitignored in the main repository.

## Tables

| Table | Primary key | Sync |
|---|---|---|
| `clients` | `id` | Incremental on `updatedAt` |
| `jobs` | `id` | Incremental on `updatedAt` |
| `quotes` | `id` | Incremental on `updatedAt` |
| `invoices` | `id` | Incremental on `updatedAt` |
| `expenses` | `id` | Incremental on `updatedAt` |

## Behavior notes

- Queries use cursor pagination (`first: 100`, `pageInfo.endCursor`) and are written page by page.
- State is `updated_after`, the latest `updatedAt` seen across all tables, saved in one checkpoint at the end of a sync.
- Requests retry on network errors, HTTP 429, HTTP 5xx and GraphQL throttling errors with exponential backoff, honoring `Retry-After`. HTTP 401 and 403 fail the sync with a message about the access token. Other GraphQL errors fail the sync with the error text.
- Field names in the queries (for example `jobStatus`, `quoteStatus`, `invoiceNumber`) follow the Jobber schema as understood at build time and have not been run against a live account. The connector sends no `X-JOBBER-GRAPHQL-VERSION` header, so the API default version applies.

## Run locally

```bash
pip install fivetran-connector-sdk
fivetran debug
```
