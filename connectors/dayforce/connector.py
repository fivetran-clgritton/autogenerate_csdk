import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


PAGE_SIZE = 200


def _base_url(configuration: dict) -> str:
    ns = configuration["client_namespace"]
    host = "ustest.dayforcehcm.com" if configuration.get("environment", "test") == "test" else "www.dayforcehcm.com"
    return f"https://{host}/Api/{ns}/V1"


def _get_token(configuration: dict) -> str:
    base = _base_url(configuration)
    resp = requests.post(
        f"{base}/Token",
        data={
            "grant_type": "client_credentials",
            "client_id": configuration["client_id"],
            "client_secret": configuration["client_secret"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_paged(token: str, url: str, params: dict) -> list:
    results = []
    skip = 0
    while True:
        params.update({"pageSize": PAGE_SIZE, "pageNumber": skip // PAGE_SIZE + 1})
        resp = requests.get(url, headers=_headers(token), params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json().get("Data", []) or []
        results.extend(data)
        if len(data) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return results


def schema(configuration: dict):
    return [
        {
            "table": "employees",
            "primary_key": ["xrefcode"],
            "columns": {
                "xrefcode": "STRING",
                "display_name": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "common_name": "STRING",
                "employee_number": "STRING",
                "hire_date": "NAIVE_DATE",
                "original_hire_date": "NAIVE_DATE",
                "termination_date": "NAIVE_DATE",
                "birth_date": "NAIVE_DATE",
                "gender": "STRING",
                "marital_status": "STRING",
                "employee_status": "STRING",
                "pay_type": "STRING",
                "pay_group": "STRING",
                "pay_policy": "STRING",
                "home_org_unit": "STRING",
                "job": "STRING",
                "position": "STRING",
                "work_location": "STRING",
                "manager_xrefcode": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "last_modified_timestamp": "UTC_DATETIME",
            },
        },
        {
            "table": "employee_employment_statuses",
            "primary_key": ["employee_xrefcode", "effective_start"],
            "columns": {
                "employee_xrefcode": "STRING",
                "employment_status": "STRING",
                "employment_status_reason": "STRING",
                "effective_start": "NAIVE_DATE",
                "effective_end": "NAIVE_DATE",
                "pay_type": "STRING",
                "pay_group": "STRING",
                "is_active": "BOOLEAN",
            },
        },
        {
            "table": "employee_work_assignments",
            "primary_key": ["employee_xrefcode", "effective_start", "position_xrefcode"],
            "columns": {
                "employee_xrefcode": "STRING",
                "position_xrefcode": "STRING",
                "position_name": "STRING",
                "job_xrefcode": "STRING",
                "job_name": "STRING",
                "org_unit_xrefcode": "STRING",
                "org_unit_name": "STRING",
                "work_location_xrefcode": "STRING",
                "work_location_name": "STRING",
                "manager_xrefcode": "STRING",
                "is_primary": "BOOLEAN",
                "effective_start": "NAIVE_DATE",
                "effective_end": "NAIVE_DATE",
            },
        },
        {
            "table": "employee_pay_summaries",
            "primary_key": ["employee_xrefcode", "pay_period_start"],
            "columns": {
                "employee_xrefcode": "STRING",
                "pay_period_start": "NAIVE_DATE",
                "pay_period_end": "NAIVE_DATE",
                "pay_date": "NAIVE_DATE",
                "pay_group": "STRING",
                "gross_pay": "FLOAT",
                "net_pay": "FLOAT",
                "total_hours": "FLOAT",
                "regular_hours": "FLOAT",
                "overtime_hours": "FLOAT",
                "currency": "STRING",
            },
        },
        {
            "table": "time_off_policies",
            "primary_key": ["xrefcode"],
            "columns": {
                "xrefcode": "STRING",
                "name": "STRING",
                "short_name": "STRING",
                "is_active": "BOOLEAN",
                "policy_type": "STRING",
                "accrual_frequency": "STRING",
                "carryover_policy": "STRING",
            },
        },
        {
            "table": "time_off_requests",
            "primary_key": ["time_off_request_id"],
            "columns": {
                "time_off_request_id": "STRING",
                "employee_xrefcode": "STRING",
                "time_off_policy_xrefcode": "STRING",
                "time_off_policy_name": "STRING",
                "status": "STRING",
                "start_date": "NAIVE_DATE",
                "end_date": "NAIVE_DATE",
                "total_days": "FLOAT",
                "total_hours": "FLOAT",
                "submitted_date": "UTC_DATETIME",
                "last_modified_timestamp": "UTC_DATETIME",
                "manager_comment": "STRING",
                "employee_comment": "STRING",
            },
        },
        {
            "table": "org_units",
            "primary_key": ["xrefcode"],
            "columns": {
                "xrefcode": "STRING",
                "name": "STRING",
                "short_name": "STRING",
                "org_level": "STRING",
                "parent_xrefcode": "STRING",
                "is_active": "BOOLEAN",
                "effective_start": "NAIVE_DATE",
                "effective_end": "NAIVE_DATE",
            },
        },
        {
            "table": "pay_groups",
            "primary_key": ["xrefcode"],
            "columns": {
                "xrefcode": "STRING",
                "name": "STRING",
                "short_name": "STRING",
                "pay_frequency": "STRING",
                "is_active": "BOOLEAN",
            },
        },
    ]


def _flatten_employee(e: dict) -> dict:
    status = (e.get("EmployeeStatus") or {})
    pay = (e.get("PayGroup") or {})
    job = (e.get("Job") or {})
    position = (e.get("Position") or {})
    location = (e.get("WorkLocation") or {})
    org = (e.get("HomeOrganization") or {})
    manager = (e.get("ManagerXRefCode") or {})
    return {
        "xrefcode": e.get("XRefCode"),
        "display_name": e.get("DisplayName"),
        "first_name": e.get("FirstName"),
        "last_name": e.get("LastName"),
        "common_name": e.get("CommonName"),
        "employee_number": e.get("EmployeeNumber"),
        "hire_date": e.get("HireDate"),
        "original_hire_date": e.get("OriginalHireDate"),
        "termination_date": e.get("TerminationDate"),
        "birth_date": e.get("BirthDate"),
        "gender": (e.get("Gender") or {}).get("ShortName"),
        "marital_status": (e.get("MaritalStatus") or {}).get("ShortName"),
        "employee_status": status.get("ShortName"),
        "pay_type": (e.get("PayType") or {}).get("ShortName"),
        "pay_group": pay.get("ShortName"),
        "pay_policy": (e.get("PayPolicy") or {}).get("ShortName"),
        "home_org_unit": org.get("ShortName"),
        "job": job.get("ShortName"),
        "position": position.get("ShortName"),
        "work_location": location.get("ShortName"),
        "manager_xrefcode": e.get("ManagerXRefCode"),
        "email": e.get("ContactEmail"),
        "phone": e.get("ContactPhone"),
        "last_modified_timestamp": e.get("LastModifiedTimestamp"),
    }


def update(configuration: dict, state: dict):
    token = _get_token(configuration)
    base = _base_url(configuration)
    updated_since = state.get("updated_since", "2000-01-01T00:00:00")
    latest_ts = updated_since

    # --- Employees (incremental) ---
    log.info(f"Fetching employees updated since {updated_since}")
    employees = _get_paged(token, f"{base}/Employees", {"filterUpdatedDateFrom": updated_since})
    log.info(f"Fetched {len(employees)} employees")

    for e in employees:
        xrefcode = e.get("XRefCode")
        if not xrefcode:
            continue

        yield op.upsert("employees", _flatten_employee(e))

        # Employment statuses (nested)
        for status in (e.get("EmploymentStatuses") or {}).get("Items", []):
            yield op.upsert("employee_employment_statuses", {
                "employee_xrefcode": xrefcode,
                "employment_status": (status.get("EmploymentStatus") or {}).get("ShortName"),
                "employment_status_reason": (status.get("EmploymentStatusReason") or {}).get("ShortName"),
                "effective_start": status.get("EffectiveStart"),
                "effective_end": status.get("EffectiveEnd"),
                "pay_type": (status.get("PayType") or {}).get("ShortName"),
                "pay_group": (status.get("PayGroup") or {}).get("ShortName"),
                "is_active": status.get("IsActive"),
            })

        # Work assignments (nested)
        for wa in (e.get("WorkAssignments") or {}).get("Items", []):
            yield op.upsert("employee_work_assignments", {
                "employee_xrefcode": xrefcode,
                "position_xrefcode": (wa.get("Position") or {}).get("XRefCode"),
                "position_name": (wa.get("Position") or {}).get("ShortName"),
                "job_xrefcode": (wa.get("Job") or {}).get("XRefCode"),
                "job_name": (wa.get("Job") or {}).get("ShortName"),
                "org_unit_xrefcode": (wa.get("OrgUnit") or {}).get("XRefCode"),
                "org_unit_name": (wa.get("OrgUnit") or {}).get("ShortName"),
                "work_location_xrefcode": (wa.get("WorkLocation") or {}).get("XRefCode"),
                "work_location_name": (wa.get("WorkLocation") or {}).get("ShortName"),
                "manager_xrefcode": wa.get("ManagerXRefCode"),
                "is_primary": wa.get("IsPrimary"),
                "effective_start": wa.get("EffectiveStart"),
                "effective_end": wa.get("EffectiveEnd"),
            })

        ts = e.get("LastModifiedTimestamp", "")
        if ts and ts > latest_ts:
            latest_ts = ts

    # --- Time off requests (incremental) ---
    log.info("Fetching time off requests")
    tor_rows = _get_paged(token, f"{base}/EmployeeTimeAwayFromWork", {
        "filterModifiedDateFrom": updated_since,
    })
    for r in tor_rows:
        yield op.upsert("time_off_requests", {
            "time_off_request_id": r.get("TimeAwayFromWorkXRefCode"),
            "employee_xrefcode": r.get("EmployeeXRefCode"),
            "time_off_policy_xrefcode": (r.get("TimeOffPolicy") or {}).get("XRefCode"),
            "time_off_policy_name": (r.get("TimeOffPolicy") or {}).get("ShortName"),
            "status": (r.get("StatusName") or {}).get("ShortName") if isinstance(r.get("StatusName"), dict) else r.get("StatusName"),
            "start_date": r.get("StartDate"),
            "end_date": r.get("EndDate"),
            "total_days": r.get("NetDays"),
            "total_hours": r.get("NetHours"),
            "submitted_date": r.get("SubmittedDate"),
            "last_modified_timestamp": r.get("LastModifiedTimestamp"),
            "manager_comment": r.get("ManagerComment"),
            "employee_comment": r.get("EmployeeComment"),
        })

    # --- Pay summaries (incremental) ---
    log.info("Fetching pay summaries")
    pay_rows = _get_paged(token, f"{base}/EmployeePaySummary", {
        "filterPayDate": updated_since[:10],
    })
    for p in pay_rows:
        yield op.upsert("employee_pay_summaries", {
            "employee_xrefcode": p.get("EmployeeXRefCode"),
            "pay_period_start": p.get("PayPeriodStartDate"),
            "pay_period_end": p.get("PayPeriodEndDate"),
            "pay_date": p.get("PayDate"),
            "pay_group": (p.get("PayGroup") or {}).get("ShortName"),
            "gross_pay": p.get("GrossPay"),
            "net_pay": p.get("NetPay"),
            "total_hours": p.get("TotalHours"),
            "regular_hours": p.get("RegularHours"),
            "overtime_hours": p.get("OvertimeHours"),
            "currency": p.get("Currency"),
        })

    # --- Org units (full refresh) ---
    log.info("Fetching org units")
    orgs = _get_paged(token, f"{base}/OrgUnits", {})
    for o in orgs:
        yield op.upsert("org_units", {
            "xrefcode": o.get("XRefCode"),
            "name": o.get("LongName"),
            "short_name": o.get("ShortName"),
            "org_level": (o.get("OrgLevel") or {}).get("ShortName"),
            "parent_xrefcode": (o.get("ParentOrgUnit") or {}).get("XRefCode"),
            "is_active": o.get("IsActive"),
            "effective_start": o.get("EffectiveStart"),
            "effective_end": o.get("EffectiveEnd"),
        })

    # --- Pay groups (full refresh) ---
    log.info("Fetching pay groups")
    pgs = _get_paged(token, f"{base}/PayGroups", {})
    for pg in pgs:
        yield op.upsert("pay_groups", {
            "xrefcode": pg.get("XRefCode"),
            "name": pg.get("LongName"),
            "short_name": pg.get("ShortName"),
            "pay_frequency": (pg.get("PayFrequency") or {}).get("ShortName"),
            "is_active": pg.get("IsActive"),
        })

    # --- Time off policies (full refresh) ---
    log.info("Fetching time off policies")
    policies = _get_paged(token, f"{base}/TimeOffPolicies", {})
    for pol in policies:
        yield op.upsert("time_off_policies", {
            "xrefcode": pol.get("XRefCode"),
            "name": pol.get("LongName"),
            "short_name": pol.get("ShortName"),
            "is_active": pol.get("IsActive"),
            "policy_type": (pol.get("PolicyType") or {}).get("ShortName"),
            "accrual_frequency": (pol.get("AccrualFrequency") or {}).get("ShortName"),
            "carryover_policy": (pol.get("CarryoverPolicy") or {}).get("ShortName"),
        })

    yield op.checkpoint(state={"updated_since": latest_ts})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
