import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


API_VERSION = "24.200.001"
PAGE_SIZE = 100

INCREMENTAL_ENTITIES = [
    ("contacts", "Contact", "ContactID"),
    ("customers", "Customer", "CustomerID"),
    ("vendors", "Vendor", "VendorID"),
    ("invoices", "Invoice", "ReferenceNbr"),
    ("bills", "Bill", "ReferenceNbr"),
    ("payments", "Payment", "ReferenceNbr"),
    ("sales_orders", "SalesOrder", "OrderNbr"),
    ("purchase_orders", "PurchaseOrder", "OrderNbr"),
    ("employees", "Employee", "EmployeeID"),
    ("projects", "Project", "ProjectID"),
]


def schema(configuration: dict):
    return [
        {
            "table": "accounts",
            "primary_key": ["AccountID"],
            "columns": {
                "AccountID": "STRING",
                "AccountCD": "STRING",
                "Description": "STRING",
                "Type": "STRING",
                "Active": "BOOLEAN",
                "CashAccount": "BOOLEAN",
                "CurrencyID": "STRING",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "contacts",
            "primary_key": ["ContactID"],
            "columns": {
                "ContactID": "STRING",
                "FirstName": "STRING",
                "LastName": "STRING",
                "Email": "STRING",
                "Phone1": "STRING",
                "ContactType": "STRING",
                "Status": "STRING",
                "AccountID": "STRING",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "customers",
            "primary_key": ["CustomerID"],
            "columns": {
                "CustomerID": "STRING",
                "CustomerName": "STRING",
                "Status": "STRING",
                "CurrencyID": "STRING",
                "CustomerClass": "STRING",
                "MainContact_Email": "STRING",
                "MainContact_Phone1": "STRING",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "vendors",
            "primary_key": ["VendorID"],
            "columns": {
                "VendorID": "STRING",
                "VendorName": "STRING",
                "Status": "STRING",
                "CurrencyID": "STRING",
                "VendorClass": "STRING",
                "MainContact_Email": "STRING",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "invoices",
            "primary_key": ["ReferenceNbr"],
            "columns": {
                "ReferenceNbr": "STRING",
                "Type": "STRING",
                "Status": "STRING",
                "CustomerID": "STRING",
                "CustomerName": "STRING",
                "Date": "NAIVE_DATE",
                "DueDate": "NAIVE_DATE",
                "CurrencyID": "STRING",
                "Amount": "DECIMAL",
                "BalanceAmount": "DECIMAL",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "bills",
            "primary_key": ["ReferenceNbr"],
            "columns": {
                "ReferenceNbr": "STRING",
                "Type": "STRING",
                "Status": "STRING",
                "VendorID": "STRING",
                "VendorName": "STRING",
                "Date": "NAIVE_DATE",
                "DueDate": "NAIVE_DATE",
                "CurrencyID": "STRING",
                "Amount": "DECIMAL",
                "BalanceAmount": "DECIMAL",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "payments",
            "primary_key": ["ReferenceNbr"],
            "columns": {
                "ReferenceNbr": "STRING",
                "Type": "STRING",
                "Status": "STRING",
                "CustomerID": "STRING",
                "CurrencyID": "STRING",
                "PaymentAmount": "DECIMAL",
                "Date": "NAIVE_DATE",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "sales_orders",
            "primary_key": ["OrderNbr"],
            "columns": {
                "OrderNbr": "STRING",
                "OrderType": "STRING",
                "Status": "STRING",
                "CustomerID": "STRING",
                "CustomerName": "STRING",
                "Date": "NAIVE_DATE",
                "RequestedOn": "NAIVE_DATE",
                "CurrencyID": "STRING",
                "OrderTotal": "DECIMAL",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "purchase_orders",
            "primary_key": ["OrderNbr"],
            "columns": {
                "OrderNbr": "STRING",
                "OrderType": "STRING",
                "Status": "STRING",
                "VendorID": "STRING",
                "VendorName": "STRING",
                "Date": "NAIVE_DATE",
                "CurrencyID": "STRING",
                "OrderTotal": "DECIMAL",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "employees",
            "primary_key": ["EmployeeID"],
            "columns": {
                "EmployeeID": "STRING",
                "EmployeeName": "STRING",
                "Status": "STRING",
                "DepartmentID": "STRING",
                "PositionID": "STRING",
                "StartDate": "NAIVE_DATE",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "projects",
            "primary_key": ["ProjectID"],
            "columns": {
                "ProjectID": "STRING",
                "Description": "STRING",
                "Status": "STRING",
                "CustomerID": "STRING",
                "StartDate": "NAIVE_DATE",
                "EndDate": "NAIVE_DATE",
                "BudgetedAmount": "DECIMAL",
                "LastModified": "UTC_DATETIME",
            },
        },
        {
            "table": "time_entries",
            "primary_key": ["TimeEntryID"],
            "columns": {
                "TimeEntryID": "STRING",
                "Date": "NAIVE_DATE",
                "ProjectID": "STRING",
                "ProjectTaskID": "STRING",
                "EmployeeID": "STRING",
                "TimeSpent": "STRING",
                "Description": "STRING",
                "LastModified": "UTC_DATETIME",
            },
        },
    ]


def _login(session: requests.Session, tenant_url: str, username: str, password: str, company: str, branch: str):
    url = f"{tenant_url.rstrip('/')}/entity/auth/login"
    resp = session.post(url, json={
        "name": username,
        "password": password,
        "company": company,
        "branch": branch,
    }, timeout=30)
    resp.raise_for_status()


def _logout(session: requests.Session, tenant_url: str):
    try:
        session.post(f"{tenant_url.rstrip('/')}/entity/auth/logout", timeout=10)
    except Exception:
        pass


def _base_url(tenant_url: str) -> str:
    return f"{tenant_url.rstrip('/')}/entity/Default/{API_VERSION}"


def _get_entities(session: requests.Session, base_url: str, entity: str, filter_expr: str | None = None) -> list:
    rows = []
    skip = 0
    while True:
        params = {"$top": PAGE_SIZE, "$skip": skip}
        if filter_expr:
            params["$filter"] = filter_expr
        resp = session.get(f"{base_url}/{entity}", params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return rows


def _extract(record: dict, field: str):
    """Acumatica wraps values: {"value": ...}. Handle both wrapped and plain."""
    val = record.get(field)
    if isinstance(val, dict):
        return val.get("value")
    return val


def _flatten(record: dict) -> dict:
    return {k: _extract(record, k) for k in record}


def update(configuration: dict, state: dict):
    tenant_url = configuration["tenant_url"]
    username = configuration["username"]
    password = configuration["password"]
    company = configuration.get("company", "")
    branch = configuration.get("branch", "MAIN")

    default_ts = "2000-01-01T00:00:00"
    base_url = _base_url(tenant_url)

    session = requests.Session()
    _login(session, tenant_url, username, password, company, branch)

    try:
        # --- GL Accounts (full refresh) ---
        log.info("Fetching GL accounts")
        for rec in _get_entities(session, base_url, "Account"):
            flat = _flatten(rec)
            yield op.upsert("accounts", flat)

        # --- Incremental entities ---
        new_state = dict(state)
        for table, entity, pk in INCREMENTAL_ENTITIES:
            last_ts = state.get(f"{table}_last_modified", default_ts)
            log.info(f"Fetching {entity} modified since {last_ts}")
            filter_expr = f"LastModified gt datetimeoffset'{last_ts}'"
            records = _get_entities(session, base_url, entity, filter_expr)
            latest = last_ts
            for rec in records:
                flat = _flatten(rec)
                yield op.upsert(table, flat)
                lm = flat.get("LastModified", "")
                if lm and lm > latest:
                    latest = lm
            new_state[f"{table}_last_modified"] = latest
            log.info(f"Synced {len(records)} {entity} records")

        # --- Time entries (incremental by Date) ---
        last_date = state.get("time_entries_last_date", "2000-01-01")
        log.info(f"Fetching time entries since {last_date}")
        filter_expr = f"Date gt '{last_date}'"
        time_records = _get_entities(session, base_url, "TimeEntry", filter_expr)
        latest_date = last_date
        for rec in time_records:
            flat = _flatten(rec)
            yield op.upsert("time_entries", flat)
            d = flat.get("Date", "")
            if d and d > latest_date:
                latest_date = d
        new_state["time_entries_last_date"] = latest_date
        log.info(f"Synced {len(time_records)} time entries")

    finally:
        _logout(session, tenant_url)

    yield op.checkpoint(state=new_state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
