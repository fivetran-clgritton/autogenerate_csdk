import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://api.gusto.com/v1"
PER_PAGE = 100


def schema(configuration: dict):
    return [
        {
            "table": "companies",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "trade_name": "STRING",
                "ein": "STRING",
                "entity_type": "STRING",
                "company_status": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "employees",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "job_title": "STRING",
                "department": "STRING",
                "start_date": "STRING",
                "termination_date": "STRING",
                "is_terminated": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "departments",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "title": "STRING",
                "version": "STRING",
            },
        },
        {
            "table": "contractors",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "type": "STRING",
                "start_date": "STRING",
                "is_active": "BOOLEAN",
            },
        },
        {
            "table": "pay_schedules",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "frequency": "STRING",
                "anchor_pay_date": "STRING",
                "day_1": "INT",
                "day_2": "INT",
                "name": "STRING",
                "active": "BOOLEAN",
            },
        },
        {
            "table": "payrolls",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "pay_period_start_date": "STRING",
                "pay_period_end_date": "STRING",
                "processed": "BOOLEAN",
                "processed_date": "STRING",
                "gross_pay": "FLOAT",
                "net_pay": "FLOAT",
                "check_date": "STRING",
            },
        },
        {
            "table": "payroll_employee_compensations",
            "primary_key": ["payroll_id", "employee_id"],
            "columns": {
                "payroll_id": "STRING",
                "employee_id": "STRING",
                "gross_pay": "FLOAT",
                "net_pay": "FLOAT",
                "payment_method": "STRING",
                "fixed_compensations": "STRING",
                "hourly_compensations": "STRING",
            },
        },
    ]


def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _paginate(url: str, headers: dict, params: dict = None) -> list:
    results = []
    page = 1
    while True:
        p = {**(params or {}), "page": page, "per": PER_PAGE}
        resp = requests.get(url, headers=headers, params=p, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            results.extend(data)
            if len(data) < PER_PAGE:
                break
        else:
            # Some endpoints wrap in a key; return raw for caller to handle
            return data
        page += 1
    return results


def _get_company_ids(access_token: str) -> list:
    resp = requests.get(f"{BASE_URL}/me", headers=_headers(access_token), timeout=30)
    resp.raise_for_status()
    me = resp.json()
    roles = me.get("roles", {})
    payroll = roles.get("payroll", {})
    companies = payroll.get("companies", [])
    return [str(c["id"]) for c in companies if c.get("id")]


def update(configuration: dict, state: dict):
    access_token = configuration["access_token"]
    hdrs = _headers(access_token)

    company_ids = _get_company_ids(access_token)
    if not company_ids:
        log.warning("No companies found for this access token")
        return

    # Payrolls: sync from last check_date seen
    payrolls_start = state.get("payrolls_start_date", "2010-01-01")

    for company_id in company_ids:
        log.info(f"Syncing company {company_id}")

        # --- Company ---
        resp = requests.get(f"{BASE_URL}/companies/{company_id}", headers=hdrs, timeout=30)
        resp.raise_for_status()
        c = resp.json()
        yield op.upsert("companies", {
            "id": str(c.get("id", "")),
            "name": c.get("name"),
            "trade_name": c.get("trade_name"),
            "ein": c.get("ein"),
            "entity_type": c.get("entity_type"),
            "company_status": c.get("company_status"),
            "created_at": c.get("created_at"),
        })

        # --- Employees ---
        employees = _paginate(f"{BASE_URL}/companies/{company_id}/employees", hdrs)
        for e in employees:
            yield op.upsert("employees", {
                "id": str(e.get("id", "")),
                "company_id": company_id,
                "first_name": e.get("first_name"),
                "last_name": e.get("last_name"),
                "email": e.get("email"),
                "job_title": e.get("job_title"),
                "department": e.get("department"),
                "start_date": e.get("start_date"),
                "termination_date": e.get("termination_date"),
                "is_terminated": e.get("terminated", False),
                "created_at": e.get("created_at"),
                "updated_at": e.get("updated_at"),
            })

        # --- Departments ---
        depts = _paginate(f"{BASE_URL}/companies/{company_id}/departments", hdrs)
        for d in depts:
            yield op.upsert("departments", {
                "id": str(d.get("id", "")),
                "company_id": company_id,
                "title": d.get("title"),
                "version": d.get("version"),
            })

        # --- Contractors ---
        contractors = _paginate(f"{BASE_URL}/companies/{company_id}/contractors", hdrs)
        for ct in contractors:
            yield op.upsert("contractors", {
                "id": str(ct.get("id", "")),
                "company_id": company_id,
                "first_name": ct.get("first_name"),
                "last_name": ct.get("last_name"),
                "email": ct.get("email"),
                "type": ct.get("type"),
                "start_date": ct.get("start_date"),
                "is_active": ct.get("is_active", True),
            })

        # --- Pay schedules ---
        schedules = _paginate(f"{BASE_URL}/companies/{company_id}/pay_schedules", hdrs)
        for ps in schedules:
            yield op.upsert("pay_schedules", {
                "id": str(ps.get("id", "")),
                "company_id": company_id,
                "frequency": ps.get("frequency"),
                "anchor_pay_date": ps.get("anchor_pay_date"),
                "day_1": ps.get("day_1"),
                "day_2": ps.get("day_2"),
                "name": ps.get("name"),
                "active": ps.get("active", True),
            })

        # --- Payrolls (incremental) ---
        import datetime
        end_date = datetime.date.today().isoformat()
        resp = requests.get(
            f"{BASE_URL}/companies/{company_id}/payrolls",
            headers=hdrs,
            params={
                "start_date": payrolls_start,
                "end_date": end_date,
                "processed": "true",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payrolls = resp.json() if isinstance(resp.json(), list) else []

        latest_check_date = payrolls_start
        for pr in payrolls:
            pr_id = str(pr.get("payroll_id") or pr.get("id", ""))
            totals = pr.get("totals", {})
            yield op.upsert("payrolls", {
                "id": pr_id,
                "company_id": company_id,
                "pay_period_start_date": pr.get("pay_period", {}).get("start_date"),
                "pay_period_end_date": pr.get("pay_period", {}).get("end_date"),
                "processed": pr.get("processed", False),
                "processed_date": pr.get("processed_date"),
                "gross_pay": float(totals.get("gross_pay", 0) or 0),
                "net_pay": float(totals.get("net_pay", 0) or 0),
                "check_date": pr.get("check_date"),
            })

            check_date = pr.get("check_date", "")
            if check_date and check_date > latest_check_date:
                latest_check_date = check_date

            for ec in pr.get("employee_compensations", []):
                emp_id = str(ec.get("employee_id", ""))
                import json
                yield op.upsert("payroll_employee_compensations", {
                    "payroll_id": pr_id,
                    "employee_id": emp_id,
                    "gross_pay": float(ec.get("gross_pay", 0) or 0),
                    "net_pay": float(ec.get("net_pay", 0) or 0),
                    "payment_method": ec.get("payment_method"),
                    "fixed_compensations": json.dumps(ec.get("fixed_compensations", [])),
                    "hourly_compensations": json.dumps(ec.get("hourly_compensations", [])),
                })

    yield op.checkpoint(state={"payrolls_start_date": latest_check_date})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
