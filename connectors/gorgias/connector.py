import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


PAGE_LIMIT = 100


def schema(configuration: dict):
    return [
        {
            "table": "tickets",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "subject": "STRING",
                "status": "STRING",
                "channel": "STRING",
                "created_datetime": "UTC_DATETIME",
                "updated_datetime": "UTC_DATETIME",
                "opened_datetime": "UTC_DATETIME",
                "last_received_message_datetime": "UTC_DATETIME",
                "assignee_user_id": "INT",
                "assignee_team_id": "INT",
                "requester_id": "INT",
                "is_spam": "BOOLEAN",
                "spam_score": "FLOAT",
                "language": "STRING",
            },
        },
        {
            "table": "ticket_messages",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "ticket_id": "INT",
                "created_datetime": "UTC_DATETIME",
                "channel": "STRING",
                "source_type": "STRING",
                "body_text": "STRING",
                "sender_id": "INT",
                "receiver_id": "INT",
                "is_from_agent": "BOOLEAN",
                "attachments_count": "INT",
            },
        },
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "email": "STRING",
                "created_datetime": "UTC_DATETIME",
                "updated_datetime": "UTC_DATETIME",
                "order_count": "INT",
                "order_value": "FLOAT",
                "note": "STRING",
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
                "active": "BOOLEAN",
                "created_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "teams",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "description": "STRING",
                "created_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "satisfaction_surveys",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "ticket_id": "INT",
                "created_datetime": "UTC_DATETIME",
                "score": "INT",
                "body_text": "STRING",
                "nps": "INT",
            },
        },
    ]


def _base_url(domain: str) -> str:
    return f"https://{domain}.gorgias.com/api"


def _auth(configuration: dict):
    return (configuration["email"], configuration["api_token"])


def _paginate(url: str, auth, params: dict = None):
    """Yield pages of items using cursor pagination."""
    params = dict(params or {})
    params["limit"] = PAGE_LIMIT
    while True:
        resp = requests.get(url, auth=auth, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        yield items
        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor or not items:
            break
        params = {"limit": PAGE_LIMIT, "cursor": cursor}


def _get_ticket_messages(base_url: str, auth, ticket_id: int) -> list:
    url = f"{base_url}/tickets/{ticket_id}/messages"
    resp = requests.get(url, auth=auth, params={"limit": PAGE_LIMIT}, timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json().get("data", [])


def update(configuration: dict, state: dict):
    domain = configuration["domain"]
    base = _base_url(domain)
    auth = _auth(configuration)
    updated_since = state.get("updated_datetime_after", "2010-01-01T00:00:00.000000")
    latest = updated_since

    # --- Tickets + messages (incremental) ---
    log.info(f"Fetching tickets updated after {updated_since}")
    ticket_params = {"updated_datetime_after": updated_since, "order_by": "updated_datetime", "order_dir": "asc"}
    total = 0
    for page in _paginate(f"{base}/tickets", auth, ticket_params):
        for t in page:
            yield op.upsert("tickets", {
                "id": t.get("id"),
                "subject": t.get("subject"),
                "status": t.get("status"),
                "channel": t.get("channel"),
                "created_datetime": t.get("created_datetime"),
                "updated_datetime": t.get("updated_datetime"),
                "opened_datetime": t.get("opened_datetime"),
                "last_received_message_datetime": t.get("last_received_message_datetime"),
                "assignee_user_id": (t.get("assignee") or {}).get("id"),
                "assignee_team_id": (t.get("team") or {}).get("id"),
                "requester_id": (t.get("requester") or {}).get("id"),
                "is_spam": t.get("is_spam"),
                "spam_score": t.get("spam_score"),
                "language": t.get("language"),
            })

            for msg in _get_ticket_messages(base, auth, t["id"]):
                yield op.upsert("ticket_messages", {
                    "id": msg.get("id"),
                    "ticket_id": t["id"],
                    "created_datetime": msg.get("created_datetime"),
                    "channel": msg.get("channel"),
                    "source_type": (msg.get("source") or {}).get("type"),
                    "body_text": msg.get("body_text"),
                    "sender_id": (msg.get("sender") or {}).get("id"),
                    "receiver_id": (msg.get("receiver") or {}).get("id"),
                    "is_from_agent": msg.get("is_from_agent"),
                    "attachments_count": len(msg.get("attachments") or []),
                })

            t_updated = t.get("updated_datetime", "")
            if t_updated and t_updated > latest:
                latest = t_updated

        total += len(page)
        log.info(f"Synced {total} tickets so far")

    # --- Customers (incremental) ---
    log.info("Fetching customers")
    cust_params = {"updated_datetime_after": updated_since, "order_by": "updated_datetime", "order_dir": "asc"}
    for page in _paginate(f"{base}/customers", auth, cust_params):
        for c in page:
            yield op.upsert("customers", {
                "id": c.get("id"),
                "name": c.get("name"),
                "email": c.get("email"),
                "created_datetime": c.get("created_datetime"),
                "updated_datetime": c.get("updated_datetime"),
                "order_count": c.get("order_count"),
                "order_value": c.get("order_value"),
                "note": c.get("note"),
            })

    # --- Users (full refresh) ---
    log.info("Fetching users")
    for page in _paginate(f"{base}/users", auth):
        for u in page:
            yield op.upsert("users", {
                "id": u.get("id"),
                "name": u.get("name"),
                "email": u.get("email"),
                "role": u.get("role"),
                "active": u.get("active"),
                "created_datetime": u.get("created_datetime"),
            })

    # --- Teams (full refresh) ---
    log.info("Fetching teams")
    for page in _paginate(f"{base}/teams", auth):
        for t in page:
            yield op.upsert("teams", {
                "id": t.get("id"),
                "name": t.get("name"),
                "description": t.get("description"),
                "created_datetime": t.get("created_datetime"),
            })

    # --- Satisfaction surveys (incremental) ---
    log.info("Fetching satisfaction surveys")
    survey_params = {"created_datetime_after": updated_since}
    for page in _paginate(f"{base}/satisfaction-surveys", auth, survey_params):
        for s in page:
            yield op.upsert("satisfaction_surveys", {
                "id": s.get("id"),
                "ticket_id": s.get("ticket_id"),
                "created_datetime": s.get("created_datetime"),
                "score": s.get("score"),
                "body_text": s.get("body_text"),
                "nps": s.get("nps"),
            })

    yield op.checkpoint(state={"updated_datetime_after": latest})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
