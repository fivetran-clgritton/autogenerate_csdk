import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

AUTH_URL = "https://api.freshbooks.com/auth/api/v1/users/me"
BASE_ACCOUNTING = "https://api.freshbooks.com/accounting/account/{account_id}"
BASE_PROJECTS = "https://api.freshbooks.com/projects/business/{business_id}"
BASE_TIMETRACKING = "https://api.freshbooks.com/timetracking/business/{business_id}"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "clients",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "fname": "STRING", "lname": "STRING",
                "email": "STRING", "organization": "STRING", "phone": "STRING",
                "currency_code": "STRING", "language": "STRING",
                "created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "invoices",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "invoice_number": "STRING",
                "status": "INTEGER", "client_id": "STRING", "amount": "FLOAT",
                "currency_code": "STRING", "create_date": "NAIVE_DATE",
                "due_date": "NAIVE_DATE", "paid_date": "NAIVE_DATE", "notes": "STRING",
            },
        },
        {
            "table": "payments",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "invoice_id": "STRING",
                "client_id": "STRING", "amount": "FLOAT", "currency_code": "STRING",
                "date": "NAIVE_DATE", "type": "STRING", "note": "STRING",
            },
        },
        {
            "table": "expenses",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "staff_id": "STRING",
                "client_id": "STRING", "amount": "FLOAT", "currency_code": "STRING",
                "date": "NAIVE_DATE", "category_id": "STRING", "notes": "STRING",
                "status": "INTEGER",
            },
        },
        {
            "table": "time_entries",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "client_id": "STRING",
                "project_id": "STRING", "task_id": "STRING", "duration": "INTEGER",
                "started_at": "UTC_DATETIME", "note": "STRING",
                "billed": "BOOLEAN", "billable": "BOOLEAN",
            },
        },
        {
            "table": "projects",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING", "account_id": "STRING", "title": "STRING",
                "description": "STRING", "client_id": "STRING", "fixed_price": "FLOAT",
                "rate": "FLOAT", "budget": "FLOAT", "billed_amount": "FLOAT",
                "billed_status": "STRING",
            },
        },
    ]


def _headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}


def _get_me(access_token: str) -> dict:
    resp = requests.get(AUTH_URL, headers=_headers(access_token), timeout=30)
    resp.raise_for_status()
    return resp.json().get("response", {})


def _paginate_accounting(access_token: str, account_id: str, resource: str, params: dict):
    page = 1
    while True:
        params.update({"page": page, "per_page": PAGE_SIZE})
        url = f"{BASE_ACCOUNTING.format(account_id=account_id)}/{resource}"
        resp = requests.get(url, headers=_headers(access_token), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("result", {})
        # Resource key is typically the last path segment
        key = resource.split("/")[-1]
        items = data.get(key, [])
        for item in items:
            yield item
        pages = data.get("pages", {})
        total_pages = pages.get("pages", 1) if isinstance(pages, dict) else 1
        if page >= total_pages or not items:
            break
        page += 1


def _paginate_projects(access_token: str, business_id: str, resource: str, params: dict):
    page = 1
    while True:
        params.update({"page": page, "per_page": PAGE_SIZE})
        url = f"{BASE_PROJECTS.format(business_id=business_id)}/{resource}"
        resp = requests.get(url, headers=_headers(access_token), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get(resource.split("/")[-1], data.get("projects", []))
        for item in items:
            yield item
        meta = data.get("meta", {})
        total = meta.get("total", 0)
        if page * PAGE_SIZE >= total or not items:
            break
        page += 1


def _paginate_timetracking(access_token: str, business_id: str, resource: str, params: dict):
    page = 1
    while True:
        params.update({"page": page, "per_page": PAGE_SIZE})
        url = f"{BASE_TIMETRACKING.format(business_id=business_id)}/{resource}"
        resp = requests.get(url, headers=_headers(access_token), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("time_entries", data.get(resource, []))
        for item in items:
            yield item
        meta = data.get("meta", {})
        total = meta.get("total", 0)
        if page * PAGE_SIZE >= total or not items:
            break
        page += 1


def update(configuration: dict, state: dict):
    access_token = configuration["access_token"]
    updated_since = state.get("updated_since", "2010-01-01T00:00:00Z")

    me = _get_me(access_token)
    # Get first business
    business_memberships = me.get("business_memberships", [])
    if not business_memberships:
        log.warning("No business memberships found")
        return
    business = business_memberships[0].get("business", {})
    business_id = str(business.get("id", ""))
    account_id = business.get("account_id", "")

    log.info(f"Syncing account_id={account_id} business_id={business_id}")

    # Clients
    log.info("Syncing clients")
    for c in _paginate_accounting(access_token, account_id, "clients/clients",
                                   {"updated_since": updated_since, "include_deleted": True}):
        yield op.upsert("clients", {
            "id": str(c.get("id")), "account_id": account_id,
            "fname": c.get("fname"), "lname": c.get("lname"),
            "email": c.get("email"), "organization": c.get("organization"),
            "phone": c.get("bus_phone"), "currency_code": c.get("currency_code"),
            "language": c.get("language"),
            "created_at": c.get("signup_date"), "updated_at": c.get("updated"),
        })

    # Invoices
    log.info("Syncing invoices")
    for inv in _paginate_accounting(access_token, account_id, "invoices/invoices",
                                     {"date_modified[min]": updated_since[:10]}):
        amount_data = inv.get("amount", {})
        yield op.upsert("invoices", {
            "id": str(inv.get("id")), "account_id": account_id,
            "invoice_number": inv.get("invoice_number"),
            "status": inv.get("v3_status"),
            "client_id": str(inv.get("customerid")),
            "amount": float(amount_data.get("amount", 0)) if isinstance(amount_data, dict) else None,
            "currency_code": inv.get("currency_code"),
            "create_date": inv.get("create_date"),
            "due_date": inv.get("due_date"),
            "paid_date": inv.get("paid"),
            "notes": inv.get("notes"),
        })

    # Payments
    log.info("Syncing payments")
    for p in _paginate_accounting(access_token, account_id, "payments/payments",
                                   {"date_modified[min]": updated_since[:10]}):
        amount_data = p.get("amount", {})
        yield op.upsert("payments", {
            "id": str(p.get("id")), "account_id": account_id,
            "invoice_id": str(p.get("invoiceid")),
            "client_id": str(p.get("clientid")),
            "amount": float(amount_data.get("amount", 0)) if isinstance(amount_data, dict) else None,
            "currency_code": amount_data.get("code") if isinstance(amount_data, dict) else None,
            "date": p.get("date"), "type": p.get("type"), "note": p.get("note"),
        })

    # Expenses
    log.info("Syncing expenses")
    for e in _paginate_accounting(access_token, account_id, "expenses/expenses",
                                   {"date_modified[min]": updated_since[:10]}):
        amount_data = e.get("amount", {})
        yield op.upsert("expenses", {
            "id": str(e.get("id")), "account_id": account_id,
            "staff_id": str(e.get("staffid")),
            "client_id": str(e.get("clientid")),
            "amount": float(amount_data.get("amount", 0)) if isinstance(amount_data, dict) else None,
            "currency_code": amount_data.get("code") if isinstance(amount_data, dict) else None,
            "date": e.get("date"),
            "category_id": str(e.get("categoryid")),
            "notes": e.get("notes"), "status": e.get("status"),
        })

    # Projects
    if business_id:
        log.info("Syncing projects")
        for proj in _paginate_projects(access_token, business_id, "projects", {}):
            yield op.upsert("projects", {
                "id": str(proj.get("id")), "account_id": account_id,
                "title": proj.get("title"), "description": proj.get("description"),
                "client_id": str(proj.get("client_id")),
                "fixed_price": proj.get("fixed_price"),
                "rate": proj.get("rate"), "budget": proj.get("budget"),
                "billed_amount": proj.get("billed_amount"),
                "billed_status": proj.get("billed_status"),
            })

        # Time entries
        log.info("Syncing time entries")
        for te in _paginate_timetracking(access_token, business_id, "time_entries",
                                          {"updated_since": updated_since}):
            yield op.upsert("time_entries", {
                "id": str(te.get("id")), "account_id": account_id,
                "client_id": str(te.get("client_id")),
                "project_id": str(te.get("project_id")),
                "task_id": str(te.get("task_id")),
                "duration": te.get("duration"),
                "started_at": te.get("started_at"),
                "note": te.get("note"),
                "billed": te.get("billed"), "billable": te.get("billable"),
            })

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    yield op.checkpoint(state={"updated_since": now})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
