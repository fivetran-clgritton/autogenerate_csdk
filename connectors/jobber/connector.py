import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "clients",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "billing_address": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "jobs",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "title": "STRING",
                "status": "STRING",
                "client_id": "STRING",
                "start_at": "UTC_DATETIME",
                "end_at": "UTC_DATETIME",
                "total": "FLOAT",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "quotes",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "title": "STRING",
                "status": "STRING",
                "client_id": "STRING",
                "total": "FLOAT",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "approved_at": "UTC_DATETIME",
            },
        },
        {
            "table": "invoices",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "number": "STRING",
                "status": "STRING",
                "client_id": "STRING",
                "total": "FLOAT",
                "due_date": "NAIVE_DATE",
                "paid_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "expenses",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "title": "STRING",
                "total": "FLOAT",
                "job_id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
    ]


def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _run_query(access_token: str, query: str, variables: dict) -> dict:
    resp = requests.post(
        GRAPHQL_URL,
        headers=_headers(access_token),
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data["data"]


def _paginate(access_token: str, query: str, root_key: str, updated_after: str):
    cursor = None
    while True:
        variables = {"cursor": cursor, "updatedAfter": updated_after}
        data = _run_query(access_token, query, variables)
        connection = data[root_key]
        nodes = connection.get("nodes", [])
        yield from nodes
        page_info = connection.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]


CLIENTS_QUERY = """
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

JOBS_QUERY = """
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

QUOTES_QUERY = """
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

INVOICES_QUERY = """
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

EXPENSES_QUERY = """
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


def _address_str(addr: dict) -> str:
    if not addr:
        return None
    parts = [addr.get("street"), addr.get("city"), addr.get("province"),
             addr.get("postalCode"), addr.get("country")]
    return ", ".join(p for p in parts if p)


def update(configuration: dict, state: dict):
    access_token = configuration["access_token"]
    updated_after = state.get("updated_after", "2010-01-01T00:00:00Z")
    latest = updated_after

    def track(val):
        nonlocal latest
        if val and val > latest:
            latest = val

    # Clients
    log.info(f"Syncing clients updated after {updated_after}")
    for node in _paginate(access_token, CLIENTS_QUERY, "clients", updated_after):
        email = (node.get("emails") or [{}])[0].get("address")
        phone = (node.get("phones") or [{}])[0].get("number")
        yield op.upsert("clients", {
            "id": node["id"],
            "name": node.get("name"),
            "email": email,
            "phone": phone,
            "billing_address": _address_str(node.get("billingAddress")),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
        })
        track(node.get("updatedAt"))

    # Jobs
    log.info(f"Syncing jobs updated after {updated_after}")
    for node in _paginate(access_token, JOBS_QUERY, "jobs", updated_after):
        yield op.upsert("jobs", {
            "id": node["id"],
            "title": node.get("title"),
            "status": node.get("jobStatus"),
            "client_id": (node.get("client") or {}).get("id"),
            "start_at": node.get("startAt"),
            "end_at": node.get("endAt"),
            "total": node.get("total"),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
        })
        track(node.get("updatedAt"))

    # Quotes
    log.info(f"Syncing quotes updated after {updated_after}")
    for node in _paginate(access_token, QUOTES_QUERY, "quotes", updated_after):
        yield op.upsert("quotes", {
            "id": node["id"],
            "title": node.get("title"),
            "status": node.get("quoteStatus"),
            "client_id": (node.get("client") or {}).get("id"),
            "total": node.get("total"),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
            "approved_at": node.get("approvedAt"),
        })
        track(node.get("updatedAt"))

    # Invoices
    log.info(f"Syncing invoices updated after {updated_after}")
    for node in _paginate(access_token, INVOICES_QUERY, "invoices", updated_after):
        yield op.upsert("invoices", {
            "id": node["id"],
            "number": node.get("invoiceNumber"),
            "status": node.get("invoiceStatus"),
            "client_id": (node.get("client") or {}).get("id"),
            "total": node.get("total"),
            "due_date": node.get("dueDate"),
            "paid_at": node.get("paidAt"),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
        })
        track(node.get("updatedAt"))

    # Expenses
    log.info(f"Syncing expenses updated after {updated_after}")
    for node in _paginate(access_token, EXPENSES_QUERY, "expenses", updated_after):
        yield op.upsert("expenses", {
            "id": node["id"],
            "title": node.get("title"),
            "total": node.get("total"),
            "job_id": (node.get("job") or {}).get("id"),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
        })
        track(node.get("updatedAt"))

    yield op.checkpoint(state={"updated_after": latest})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
