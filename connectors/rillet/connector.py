import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


# Assumptions (verify against docs.api.rillet.com):
# - Base URL: https://api.rillet.com/v1
# - Auth: Bearer token via Authorization header
# - Pagination: cursor-based; response contains "next_cursor"; pass as "cursor" query param
# - Rate limit: 60 req/min
# - Incremental field: "updated_at" for transactions and journal_entries
# - Sandbox base URL: https://sandbox.api.rillet.com/v1 (set use_sandbox=true in config)

PROD_BASE_URL = "https://api.rillet.com/v1"
SANDBOX_BASE_URL = "https://sandbox.api.rillet.com/v1"
PAGE_LIMIT = 100


def schema(configuration: dict):
    return [
        {
            "table": "accounts",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "name": "STRING",
                "code": "STRING",
                "type": "STRING",
                "subtype": "STRING",
                "currency": "STRING",
                "is_active": "BOOLEAN",
                "description": "STRING",
                "parent_id": "STRING",
            },
        },
        {
            "table": "transactions",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "date": "NAIVE_DATE",
                "amount": "FLOAT",
                "currency": "STRING",
                "description": "STRING",
                "status": "STRING",
                "type": "STRING",
                "account_id": "STRING",
                "journal_entry_id": "STRING",
                "reference": "STRING",
                "memo": "STRING",
                "is_reconciled": "BOOLEAN",
            },
        },
        {
            "table": "journal_entries",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "date": "NAIVE_DATE",
                "memo": "STRING",
                "status": "STRING",
                "currency": "STRING",
                "total_debits": "FLOAT",
                "total_credits": "FLOAT",
                "reference": "STRING",
                "source": "STRING",
                "source_id": "STRING",
            },
        },
        {
            "table": "journal_entry_lines",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "journal_entry_id": "STRING",
                "account_id": "STRING",
                "debit": "FLOAT",
                "credit": "FLOAT",
                "description": "STRING",
                "currency": "STRING",
                "entity_type": "STRING",
                "entity_id": "STRING",
            },
        },
    ]


def _base_url(configuration: dict) -> str:
    if configuration.get("use_sandbox", False):
        return SANDBOX_BASE_URL
    return PROD_BASE_URL


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _paginate(api_key: str, base_url: str, path: str, params: dict):
    """Yield all items across cursor-paginated pages."""
    url = f"{base_url}{path}"
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(url, headers=_headers(api_key), params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        items = body.get("data", body.get("items", body.get("results", [])))
        if isinstance(items, list):
            yield from items
        elif isinstance(items, dict):
            # Some endpoints wrap in a named key; try common patterns
            for key in items:
                if isinstance(items[key], list):
                    yield from items[key]
                    break

        cursor = body.get("next_cursor") or body.get("nextCursor")
        if not cursor:
            break


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]
    base_url = _base_url(configuration)

    transactions_cursor = state.get("transactions_updated_at", "2010-01-01T00:00:00Z")
    journal_entries_cursor = state.get("journal_entries_updated_at", "2010-01-01T00:00:00Z")

    latest_txn_updated = transactions_cursor
    latest_je_updated = journal_entries_cursor

    # --- Accounts (full refresh) ---
    log.info("Fetching accounts (full refresh)")
    count = 0
    for account in _paginate(api_key, base_url, "/accounts", {"limit": PAGE_LIMIT}):
        yield op.upsert("accounts", {
            "id": account.get("id"),
            "created_at": account.get("created_at") or account.get("createdAt"),
            "updated_at": account.get("updated_at") or account.get("updatedAt"),
            "name": account.get("name"),
            "code": account.get("code") or account.get("account_code"),
            "type": account.get("type"),
            "subtype": account.get("subtype") or account.get("sub_type"),
            "currency": account.get("currency"),
            "is_active": account.get("is_active") if account.get("is_active") is not None else account.get("active"),
            "description": account.get("description"),
            "parent_id": account.get("parent_id") or account.get("parentId"),
        })
        count += 1
    log.info(f"Synced {count} accounts")

    # --- Transactions (incremental) ---
    log.info(f"Fetching transactions updated since {transactions_cursor}")
    params = {"limit": PAGE_LIMIT, "updated_at_min": transactions_cursor}
    count = 0
    for txn in _paginate(api_key, base_url, "/transactions", params):
        updated = txn.get("updated_at") or txn.get("updatedAt", "")
        yield op.upsert("transactions", {
            "id": txn.get("id"),
            "created_at": txn.get("created_at") or txn.get("createdAt"),
            "updated_at": updated,
            "date": txn.get("date"),
            "amount": txn.get("amount"),
            "currency": txn.get("currency"),
            "description": txn.get("description"),
            "status": txn.get("status"),
            "type": txn.get("type"),
            "account_id": txn.get("account_id") or txn.get("accountId"),
            "journal_entry_id": txn.get("journal_entry_id") or txn.get("journalEntryId"),
            "reference": txn.get("reference"),
            "memo": txn.get("memo"),
            "is_reconciled": txn.get("is_reconciled") if txn.get("is_reconciled") is not None else txn.get("reconciled"),
        })
        if updated and updated > latest_txn_updated:
            latest_txn_updated = updated
        count += 1
    log.info(f"Synced {count} transactions")

    # --- Journal Entries (incremental, with lines as child table) ---
    log.info(f"Fetching journal entries updated since {journal_entries_cursor}")
    params = {"limit": PAGE_LIMIT, "updated_at_min": journal_entries_cursor}
    count = 0
    for je in _paginate(api_key, base_url, "/journal-entries", params):
        updated = je.get("updated_at") or je.get("updatedAt", "")
        je_id = je.get("id")
        yield op.upsert("journal_entries", {
            "id": je_id,
            "created_at": je.get("created_at") or je.get("createdAt"),
            "updated_at": updated,
            "date": je.get("date"),
            "memo": je.get("memo"),
            "status": je.get("status"),
            "currency": je.get("currency"),
            "total_debits": je.get("total_debits") or je.get("totalDebits"),
            "total_credits": je.get("total_credits") or je.get("totalCredits"),
            "reference": je.get("reference"),
            "source": je.get("source"),
            "source_id": je.get("source_id") or je.get("sourceId"),
        })

        for line in je.get("lines", je.get("line_items", [])):
            yield op.upsert("journal_entry_lines", {
                "id": line.get("id"),
                "journal_entry_id": je_id,
                "account_id": line.get("account_id") or line.get("accountId"),
                "debit": line.get("debit"),
                "credit": line.get("credit"),
                "description": line.get("description"),
                "currency": line.get("currency"),
                "entity_type": line.get("entity_type") or line.get("entityType"),
                "entity_id": line.get("entity_id") or line.get("entityId"),
            })

        if updated and updated > latest_je_updated:
            latest_je_updated = updated
        count += 1
    log.info(f"Synced {count} journal entries")

    yield op.checkpoint(state={
        "transactions_updated_at": latest_txn_updated,
        "journal_entries_updated_at": latest_je_updated,
    })


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
