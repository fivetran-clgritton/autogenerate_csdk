import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://app.clio.com/api/v4"
PAGE_SIZE = 200


def schema(configuration: dict):
    return [
        {
            "table": "matters",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "display_number": "STRING",
                "description": "STRING",
                "status": "STRING",
                "client_id": "INT",
                "practice_area_id": "INT",
                "responsible_attorney_id": "INT",
                "open_date": "NAIVE_DATE",
                "close_date": "NAIVE_DATE",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "contacts",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "type": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "time_entries",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "matter_id": "INT",
                "contact_id": "INT",
                "user_id": "INT",
                "date": "NAIVE_DATE",
                "quantity": "FLOAT",
                "rate": "FLOAT",
                "total": "FLOAT",
                "billed": "BOOLEAN",
                "description": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "bills",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "number": "STRING",
                "matter_id": "INT",
                "client_id": "INT",
                "issued_at": "NAIVE_DATE",
                "due_at": "NAIVE_DATE",
                "total": "FLOAT",
                "paid_total": "FLOAT",
                "balance": "FLOAT",
                "state": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "tasks",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "matter_id": "INT",
                "assignee_id": "INT",
                "name": "STRING",
                "status": "STRING",
                "due_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "email": "STRING",
                "role": "STRING",
                "enabled": "BOOLEAN",
            },
        },
    ]


def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _get_page(access_token: str, endpoint: str, params: dict) -> dict:
    resp = requests.get(
        f"{BASE_URL}/{endpoint}.json",
        headers=_headers(access_token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _sync_incremental(access_token: str, endpoint: str, table: str, fields: str,
                       updated_since: str, flatten_fn):
    page = 1
    latest = updated_since
    while True:
        log.info(f"Fetching {endpoint} page {page} updated since {updated_since}")
        data = _get_page(access_token, endpoint, {
            "updated_since": updated_since,
            "page[size]": PAGE_SIZE,
            "page[number]": page,
            "fields": fields,
        })
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            yield op.upsert(table, flatten_fn(r))
            updated_at = r.get("updated_at", "")
            if updated_at and updated_at > latest:
                latest = updated_at
        meta = data.get("meta", {})
        if not meta.get("paging", {}).get("next"):
            break
        page += 1
    return latest


def _flatten_matter(r: dict) -> dict:
    return {
        "id": r.get("id"),
        "display_number": r.get("display_number"),
        "description": r.get("description"),
        "status": r.get("status"),
        "client_id": (r.get("client") or {}).get("id"),
        "practice_area_id": (r.get("practice_area") or {}).get("id"),
        "responsible_attorney_id": (r.get("responsible_attorney") or {}).get("id"),
        "open_date": r.get("open_date"),
        "close_date": r.get("close_date"),
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }


def _flatten_contact(r: dict) -> dict:
    emails = r.get("email_addresses") or []
    phones = r.get("phone_numbers") or []
    return {
        "id": r.get("id"),
        "name": r.get("name"),
        "type": r.get("type"),
        "email": emails[0].get("address") if emails else None,
        "phone": phones[0].get("number") if phones else None,
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }


def _flatten_time_entry(r: dict) -> dict:
    return {
        "id": r.get("id"),
        "matter_id": (r.get("matter") or {}).get("id"),
        "contact_id": (r.get("contact") or {}).get("id"),
        "user_id": (r.get("user") or {}).get("id"),
        "date": r.get("date"),
        "quantity": r.get("quantity"),
        "rate": r.get("rate"),
        "total": r.get("total"),
        "billed": r.get("billed"),
        "description": r.get("note"),
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }


def _flatten_bill(r: dict) -> dict:
    return {
        "id": r.get("id"),
        "number": r.get("number"),
        "matter_id": (r.get("matter") or {}).get("id"),
        "client_id": (r.get("client") or {}).get("id"),
        "issued_at": r.get("issued_at"),
        "due_at": r.get("due_at"),
        "total": r.get("total"),
        "paid_total": r.get("paid_total"),
        "balance": r.get("balance"),
        "state": r.get("state"),
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }


def _flatten_task(r: dict) -> dict:
    return {
        "id": r.get("id"),
        "matter_id": (r.get("matter") or {}).get("id"),
        "assignee_id": (r.get("assignee") or {}).get("id"),
        "name": r.get("name"),
        "status": r.get("status"),
        "due_at": r.get("due_at"),
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }


def update(configuration: dict, state: dict):
    access_token = configuration["access_token"]
    since = state.get("updated_since", "2010-01-01T00:00:00+00:00")
    latest = since

    for table, endpoint, fields, flatten_fn in [
        ("matters", "matters",
         "id,display_number,description,status,client,practice_area,responsible_attorney,open_date,close_date,created_at,updated_at",
         _flatten_matter),
        ("contacts", "contacts",
         "id,name,type,email_addresses,phone_numbers,created_at,updated_at",
         _flatten_contact),
        ("time_entries", "time_entries",
         "id,matter,contact,user,date,quantity,rate,total,billed,note,created_at,updated_at",
         _flatten_time_entry),
        ("bills", "bills",
         "id,number,matter,client,issued_at,due_at,total,paid_total,balance,state,created_at,updated_at",
         _flatten_bill),
        ("tasks", "tasks",
         "id,matter,assignee,name,status,due_at,created_at,updated_at",
         _flatten_task),
    ]:
        page = 1
        while True:
            log.info(f"Fetching {endpoint} page {page} updated since {since}")
            data = _get_page(access_token, endpoint, {
                "updated_since": since,
                "page[size]": PAGE_SIZE,
                "page[number]": page,
                "fields": fields,
            })
            records = data.get("data", [])
            if not records:
                break
            for r in records:
                yield op.upsert(table, flatten_fn(r))
                updated_at = r.get("updated_at", "")
                if updated_at and updated_at > latest:
                    latest = updated_at
            if not (data.get("meta") or {}).get("paging", {}).get("next"):
                break
            page += 1

    # Users — full refresh
    log.info("Fetching users (full refresh)")
    page = 1
    while True:
        data = _get_page(access_token, "users", {
            "page[size]": PAGE_SIZE,
            "page[number]": page,
            "fields": "id,name,email,role,enabled",
        })
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            yield op.upsert("users", {
                "id": r.get("id"),
                "name": r.get("name"),
                "email": r.get("email"),
                "role": r.get("role"),
                "enabled": r.get("enabled"),
            })
        if not (data.get("meta") or {}).get("paging", {}).get("next"):
            break
        page += 1

    yield op.checkpoint(state={"updated_since": latest})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
