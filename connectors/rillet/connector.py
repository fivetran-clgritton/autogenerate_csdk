# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs accounting data from the Rillet API: accounts, transactions, journal entries
and journal entry lines. Set use_sandbox to "true" to read from the Rillet sandbox instead of production.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For adding a delay between retries
import time

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Assumptions (verify against docs.api.rillet.com):
# - Auth: Bearer token via Authorization header
# - Pagination: cursor-based; response contains "next_cursor"; pass as "cursor" query param
# - Rate limit: 60 req/min (handled by retrying on HTTP 429)
# - Incremental field: "updated_at" for transactions and journal_entries
__PROD_BASE_URL = "https://api.rillet.com/v1"
__SANDBOX_BASE_URL = "https://sandbox.api.rillet.com/v1"
__PAGE_LIMIT = 100
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START = "2010-01-01T00:00:00Z"

# Destination column -> candidate source keys, tried in order. The API field naming (snake_case vs
# camelCase) is not confirmed, so both spellings are accepted.
__ACCOUNT_FIELDS = {
    "id": ["id"],
    "created_at": ["created_at", "createdAt"],
    "updated_at": ["updated_at", "updatedAt"],
    "name": ["name"],
    "code": ["code", "account_code"],
    "type": ["type"],
    "subtype": ["subtype", "sub_type"],
    "currency": ["currency"],
    "is_active": ["is_active", "active"],
    "description": ["description"],
    "parent_id": ["parent_id", "parentId"],
}

__TRANSACTION_FIELDS = {
    "id": ["id"],
    "created_at": ["created_at", "createdAt"],
    "updated_at": ["updated_at", "updatedAt"],
    "date": ["date"],
    "amount": ["amount"],
    "currency": ["currency"],
    "description": ["description"],
    "status": ["status"],
    "type": ["type"],
    "account_id": ["account_id", "accountId"],
    "journal_entry_id": ["journal_entry_id", "journalEntryId"],
    "reference": ["reference"],
    "memo": ["memo"],
    "is_reconciled": ["is_reconciled", "reconciled"],
}

__JOURNAL_ENTRY_FIELDS = {
    "id": ["id"],
    "created_at": ["created_at", "createdAt"],
    "updated_at": ["updated_at", "updatedAt"],
    "date": ["date"],
    "memo": ["memo"],
    "status": ["status"],
    "currency": ["currency"],
    "total_debits": ["total_debits", "totalDebits"],
    "total_credits": ["total_credits", "totalCredits"],
    "reference": ["reference"],
    "source": ["source"],
    "source_id": ["source_id", "sourceId"],
}

__JOURNAL_ENTRY_LINE_FIELDS = {
    "id": ["id"],
    "account_id": ["account_id", "accountId"],
    "debit": ["debit"],
    "credit": ["credit"],
    "description": ["description"],
    "currency": ["currency"],
    "entity_type": ["entity_type", "entityType"],
    "entity_id": ["entity_id", "entityId"],
}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required value is missing or use_sandbox is not "true" or "false".
    """
    for key in ["api_key"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if str(configuration.get("use_sandbox", "false")).lower() not in ("true", "false"):
        raise ValueError('use_sandbox must be "true" or "false"')


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the columns whose type must be forced are declared, so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "accounts",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"},
        },
        {
            "table": "transactions",
            "primary_key": ["id"],
            "columns": {
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "date": "NAIVE_DATE",
                "amount": "FLOAT",
            },
        },
        {
            "table": "journal_entries",
            "primary_key": ["id"],
            "columns": {
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "date": "NAIVE_DATE",
                "total_debits": "FLOAT",
                "total_credits": "FLOAT",
            },
        },
        {
            "table": "journal_entry_lines",
            "primary_key": ["id"],
            "columns": {"debit": "FLOAT", "credit": "FLOAT"},
        },
    ]


def base_url(configuration: dict):
    """
    Pick the production or sandbox base URL from the use_sandbox setting.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The API base URL.
    """
    if str(configuration.get("use_sandbox", "false")).lower() == "true":
        return __SANDBOX_BASE_URL
    return __PROD_BASE_URL


def make_request(api_key: str, url: str, params: dict):
    """
    Send a GET request to the Rillet API, retrying transient failures with exponential backoff.
    Args:
        api_key: the Rillet API key, sent as a Bearer token.
        url: the full endpoint URL.
        params: query parameters.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, or when retries are exhausted.
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT_SECONDS
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"Rillet request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {url}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"Rillet rejected the API key (HTTP {response.status_code}). "
                "Check api_key and whether use_sandbox matches the key's environment."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} on {url}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"Rillet request to {url} failed: HTTP {response.status_code}")
        return response.json()
    raise RuntimeError(f"Rillet request to {url} still failing after {__MAX_RETRIES} retries")


def extract_items(body: dict):
    """
    Pull the list of records out of a response body, accepting the wrapper keys the API may use.
    Args:
        body: the parsed JSON response.
    Returns:
        A list of record dictionaries.
    """
    items = body.get("data", body.get("items", body.get("results", [])))
    if isinstance(items, list):
        return items
    if isinstance(items, dict):
        # Some endpoints wrap the list in a named key
        for value in items.values():
            if isinstance(value, list):
                return value
    return []


def pick(record: dict, keys: list):
    """
    Return the first value among the candidate keys that is present and not None.
    Args:
        record: the source record.
        keys: candidate keys in priority order.
    Returns:
        The value, or None if no candidate key has one.
    """
    for key in keys:
        if record.get(key) is not None:
            return record[key]
    return None


def map_fields(record: dict, fields: dict):
    """
    Build a destination row from a source record using a column to candidate-keys mapping.
    Args:
        record: the source record.
        fields: destination column name to list of candidate source keys.
    Returns:
        The row dictionary.
    """
    return {column: pick(record, keys) for column, keys in fields.items()}


def sync_endpoint(api_key: str, url: str, params: dict, handle_item, label: str):
    """
    Page through a cursor-paginated endpoint and pass each record to handle_item as pages arrive,
    so the full result set is never held in memory.
    Args:
        api_key: the Rillet API key.
        url: the full endpoint URL.
        params: query parameters; the cursor is added on each following page.
        handle_item: callable invoked once per record.
        label: name used in progress logs.
    """
    params = dict(params)
    total = 0
    while True:
        body = make_request(api_key, url, params)
        items = extract_items(body)
        for item in items:
            handle_item(item)
        total += len(items)
        log.info(f"Synced {total} {label} so far")

        cursor = body.get("next_cursor") or body.get("nextCursor")
        if not cursor:
            return
        params["cursor"] = cursor


def sync_accounts(api_key: str, root: str):
    """
    Upsert all accounts (full refresh each sync).
    Args:
        api_key: the Rillet API key.
        root: the API base URL.
    """

    def handle(account):
        # The 'upsert' operation inserts or updates a row by primary key.
        op.upsert(table="accounts", data=map_fields(account, __ACCOUNT_FIELDS))

    sync_endpoint(api_key, f"{root}/accounts", {"limit": __PAGE_LIMIT}, handle, "accounts")


def sync_transactions(api_key: str, root: str, updated_since: str):
    """
    Upsert transactions updated since the given timestamp.
    Args:
        api_key: the Rillet API key.
        root: the API base URL.
        updated_since: timestamp passed as updated_at_min.
    Returns:
        The latest updated_at value seen, or updated_since if nothing newer was returned.
    """
    latest = [updated_since]

    def handle(transaction):
        row = map_fields(transaction, __TRANSACTION_FIELDS)
        op.upsert(table="transactions", data=row)
        if row["updated_at"] and row["updated_at"] > latest[0]:
            latest[0] = row["updated_at"]

    params = {"limit": __PAGE_LIMIT, "updated_at_min": updated_since}
    sync_endpoint(api_key, f"{root}/transactions", params, handle, "transactions")
    return latest[0]


def sync_journal_entries(api_key: str, root: str, updated_since: str):
    """
    Upsert journal entries updated since the given timestamp, plus their lines as a child table.
    Args:
        api_key: the Rillet API key.
        root: the API base URL.
        updated_since: timestamp passed as updated_at_min.
    Returns:
        The latest updated_at value seen, or updated_since if nothing newer was returned.
    """
    latest = [updated_since]

    def handle(entry):
        row = map_fields(entry, __JOURNAL_ENTRY_FIELDS)
        op.upsert(table="journal_entries", data=row)
        for line in entry.get("lines", entry.get("line_items", [])):
            line_row = map_fields(line, __JOURNAL_ENTRY_LINE_FIELDS)
            line_row["journal_entry_id"] = row["id"]
            op.upsert(table="journal_entry_lines", data=line_row)
        if row["updated_at"] and row["updated_at"] > latest[0]:
            latest[0] = row["updated_at"]

    params = {"limit": __PAGE_LIMIT, "updated_at_min": updated_since}
    sync_endpoint(api_key, f"{root}/journal-entries", params, handle, "journal entries")
    return latest[0]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    validate_configuration(configuration=configuration)
    api_key = configuration["api_key"]
    root = base_url(configuration)

    new_state = {
        "transactions_updated_at": state.get("transactions_updated_at", __DEFAULT_START),
        "journal_entries_updated_at": state.get("journal_entries_updated_at", __DEFAULT_START),
    }

    sync_accounts(api_key, root)

    new_state["transactions_updated_at"] = sync_transactions(
        api_key, root, new_state["transactions_updated_at"]
    )
    # Checkpoint after each incremental table so completed progress survives a later failure.
    op.checkpoint(state=new_state)

    new_state["journal_entries_updated_at"] = sync_journal_entries(
        api_key, root, new_state["journal_entries_updated_at"]
    )
    op.checkpoint(state=new_state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
