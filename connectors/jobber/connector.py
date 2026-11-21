# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs clients, jobs, quotes, invoices and expenses from the Jobber GraphQL API.
All tables are incremental on updatedAt and share one state value.
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

__GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START = "2010-01-01T00:00:00Z"

__CLIENTS_QUERY = """
query GetClients($cursor: String, $updatedAfter: DateTime) {
  clients(first: 100, after: $cursor, filter: {updatedAt: {greaterThan: $updatedAfter}}) {
    nodes {
      id
      name
      emails { address }
      phones { number }
      billingAddress { street city province postalCode country }
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

__JOBS_QUERY = """
query GetJobs($cursor: String, $updatedAfter: DateTime) {
  jobs(first: 100, after: $cursor, filter: {updatedAt: {greaterThan: $updatedAfter}}) {
    nodes {
      id
      title
      jobStatus
      client { id }
      startAt
      endAt
      total
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

__QUOTES_QUERY = """
query GetQuotes($cursor: String, $updatedAfter: DateTime) {
  quotes(first: 100, after: $cursor, filter: {updatedAt: {greaterThan: $updatedAfter}}) {
    nodes {
      id
      title
      quoteStatus
      client { id }
      total
      createdAt
      updatedAt
      approvedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

__INVOICES_QUERY = """
query GetInvoices($cursor: String, $updatedAfter: DateTime) {
  invoices(first: 100, after: $cursor, filter: {updatedAt: {greaterThan: $updatedAfter}}) {
    nodes {
      id
      invoiceNumber
      invoiceStatus
      client { id }
      total
      dueDate
      paidAt
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

__EXPENSES_QUERY = """
query GetExpenses($cursor: String, $updatedAfter: DateTime) {
  expenses(first: 100, after: $cursor, filter: {updatedAt: {greaterThan: $updatedAfter}}) {
    nodes {
      id
      title
      total
      job { id }
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    for key in ["access_token"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the date and timestamp columns that must be typed are declared,
    so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "clients",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"},
        },
        {
            "table": "jobs",
            "primary_key": ["id"],
            "columns": {
                "start_at": "UTC_DATETIME",
                "end_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "quotes",
            "primary_key": ["id"],
            "columns": {
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "approved_at": "UTC_DATETIME",
            },
        },
        {
            "table": "invoices",
            "primary_key": ["id"],
            "columns": {
                "due_date": "NAIVE_DATE",
                "paid_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "expenses",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"},
        },
    ]


def is_throttled(errors: list):
    """
    Check whether a GraphQL errors array reports rate limiting.
    Args:
        errors: the `errors` list from a GraphQL response.
    Returns:
        True if any error looks like a throttling error.
    """
    for error in errors:
        code = str((error.get("extensions") or {}).get("code", "")).upper()
        message = str(error.get("message", "")).lower()
        if code == "THROTTLED" or "throttled" in message:
            return True
    return False


def make_request(access_token: str, query: str, variables: dict):
    """
    Send a GraphQL query to the Jobber API, retrying transient failures with exponential backoff.
    Retries network errors, HTTP 429 and 5xx, and GraphQL throttling errors.
    Args:
        access_token: the Jobber OAuth access token.
        query: the GraphQL query text.
        variables: the GraphQL query variables.
    Returns:
        The `data` object of the GraphQL response.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, non-throttling GraphQL
            errors, or when retries are exhausted.
    """
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(
                __GRAPHQL_URL,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"Jobber request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"Jobber rejected the access token (HTTP {response.status_code}). "
                "Check access_token."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} from Jobber, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"Jobber request failed: HTTP {response.status_code}")

        body = response.json()
        errors = body.get("errors")
        if errors:
            if is_throttled(errors):
                delay = __BACKOFF_BASE_SECONDS ** (attempt + 1)
                log.warning(f"Jobber GraphQL query throttled, retrying in {delay}s")
                time.sleep(delay)
                continue
            raise RuntimeError(f"Jobber GraphQL errors: {errors}")
        return body["data"]
    raise RuntimeError(f"Jobber request still failing after {__MAX_RETRIES} retries")


def address_string(address: dict):
    """
    Join the parts of a billing address into a single string.
    Args:
        address: the billingAddress object from the API, or None.
    Returns:
        A comma-separated address, or None when there is no address.
    """
    if not address:
        return None
    parts = [address.get("street"), address.get("city"), address.get("province"),
             address.get("postalCode"), address.get("country")]
    return ", ".join(part for part in parts if part)


def client_row(node: dict):
    """
    Build a clients row from a GraphQL node.
    Args:
        node: the client node returned by the API.
    Returns:
        A row dictionary.
    """
    return {
        "id": node["id"],
        "name": node.get("name"),
        "email": (node.get("emails") or [{}])[0].get("address"),
        "phone": (node.get("phones") or [{}])[0].get("number"),
        "billing_address": address_string(node.get("billingAddress")),
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt"),
    }


def job_row(node: dict):
    """
    Build a jobs row from a GraphQL node.
    Args:
        node: the job node returned by the API.
    Returns:
        A row dictionary.
    """
    return {
        "id": node["id"],
        "title": node.get("title"),
        "status": node.get("jobStatus"),
        "client_id": (node.get("client") or {}).get("id"),
        "start_at": node.get("startAt"),
        "end_at": node.get("endAt"),
        "total": node.get("total"),
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt"),
    }


def quote_row(node: dict):
    """
    Build a quotes row from a GraphQL node.
    Args:
        node: the quote node returned by the API.
    Returns:
        A row dictionary.
    """
    return {
        "id": node["id"],
        "title": node.get("title"),
        "status": node.get("quoteStatus"),
        "client_id": (node.get("client") or {}).get("id"),
        "total": node.get("total"),
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt"),
        "approved_at": node.get("approvedAt"),
    }


def invoice_row(node: dict):
    """
    Build an invoices row from a GraphQL node.
    Args:
        node: the invoice node returned by the API.
    Returns:
        A row dictionary.
    """
    return {
        "id": node["id"],
        "number": node.get("invoiceNumber"),
        "status": node.get("invoiceStatus"),
        "client_id": (node.get("client") or {}).get("id"),
        "total": node.get("total"),
        "due_date": node.get("dueDate"),
        "paid_at": node.get("paidAt"),
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt"),
    }


def expense_row(node: dict):
    """
    Build an expenses row from a GraphQL node.
    Args:
        node: the expense node returned by the API.
    Returns:
        A row dictionary.
    """
    return {
        "id": node["id"],
        "title": node.get("title"),
        "total": node.get("total"),
        "job_id": (node.get("job") or {}).get("id"),
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt"),
    }


def sync_entity(access_token: str, table: str, query: str, root_key: str, build_row,
                updated_after: str):
    """
    Page through one GraphQL connection and upsert every node into its table.
    Uses cursor pagination (pageInfo.endCursor) and writes each page as it arrives, so the full
    result set is never held in memory.
    Args:
        access_token: the Jobber OAuth access token.
        table: the destination table name.
        query: the GraphQL query text for this entity.
        root_key: the top-level field of the query result that holds the connection.
        build_row: a function that turns one node into a row dictionary.
        updated_after: ISO 8601 timestamp; only records updated after it are fetched.
    Returns:
        The latest updatedAt value seen, or updated_after if nothing newer was returned.
    """
    latest = updated_after
    cursor = None
    total = 0
    while True:
        data = make_request(
            access_token, query, {"cursor": cursor, "updatedAfter": updated_after}
        )
        connection = data[root_key]
        for node in connection.get("nodes", []):
            # The 'upsert' operation inserts or updates a row by primary key.
            op.upsert(table=table, data=build_row(node))
            updated_at = node.get("updatedAt")
            if updated_at and updated_at > latest:
                latest = updated_at
            total += 1

        page_info = connection.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]

    log.info(f"Synced {total} rows into {table}")
    return latest


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
    access_token = configuration["access_token"]

    updated_after = state.get("updated_after", __DEFAULT_START)
    latest = updated_after

    entities = [
        ("clients", __CLIENTS_QUERY, "clients", client_row),
        ("jobs", __JOBS_QUERY, "jobs", job_row),
        ("quotes", __QUOTES_QUERY, "quotes", quote_row),
        ("invoices", __INVOICES_QUERY, "invoices", invoice_row),
        ("expenses", __EXPENSES_QUERY, "expenses", expense_row),
    ]
    for table, query, root_key, build_row in entities:
        log.info(f"Syncing {table} updated after {updated_after}")
        entity_latest = sync_entity(
            access_token, table, query, root_key, build_row, updated_after
        )
        if entity_latest > latest:
            latest = entity_latest

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state={"updated_after": latest})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
