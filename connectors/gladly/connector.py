import base64
import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


PAGE_LIMIT = 100


def _base_url(configuration: dict) -> str:
    org = configuration["organization"]
    # Support sandbox via organization ending in .gladly.qa
    if ".gladly.qa" in org:
        return f"https://{org}/api/v1"
    return f"https://{org}.gladly.com/api/v1"


def _headers(configuration: dict) -> dict:
    token = base64.b64encode(
        f"{configuration['email']}:{configuration['api_token']}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def schema(configuration: dict):
    return [
        {
            "table": "conversations",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "customer_id": "STRING",
                "assignee_id": "STRING",
                "assignee_type": "STRING",
                "inbox_id": "STRING",
                "status": "STRING",
                "topic_ids": "STRING",
                "external_id": "STRING",
                "note": "STRING",
            },
        },
        {
            "table": "conversation_items",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "conversation_id": "STRING",
                "created_at": "UTC_DATETIME",
                "type": "STRING",
                "initiator": "STRING",
                "author_type": "STRING",
                "author_id": "STRING",
                "content": "STRING",
                "channel": "STRING",
            },
        },
        {
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "name": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "external_customer_id": "STRING",
                "custom_attributes": "STRING",
            },
        },
        {
            "table": "agents",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "role": "STRING",
                "status": "STRING",
                "routing_enabled": "BOOLEAN",
                "max_concurrency": "INT",
            },
        },
        {
            "table": "topics",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "parent_topic_id": "STRING",
            },
        },
    ]


def _get_paginated(url: str, headers: dict, params: dict) -> list:
    """Fetch all pages from a cursor-paginated endpoint."""
    results = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Gladly returns a list directly or wraps in a data key
        if isinstance(data, list):
            items = data
            next_cursor = None
        else:
            items = data.get("items") or data.get("data") or []
            next_cursor = data.get("nextCursor") or data.get("cursor")

        results.extend(items)

        if not next_cursor or not items:
            break
        params = {**params, "cursor": next_cursor}

    return results


def update(configuration: dict, state: dict):
    base_url = _base_url(configuration)
    headers = _headers(configuration)
    updated_at_min = state.get("updated_at_min", "2010-01-01T00:00:00Z")
    latest_updated_at = updated_at_min

    # --- Agents (full refresh) ---
    log.info("Fetching agents")
    agents = _get_paginated(f"{base_url}/agents", headers, {"limit": PAGE_LIMIT})
    for a in agents:
        yield op.upsert("agents", {
            "id": a.get("id"),
            "name": a.get("name"),
            "email": a.get("email"),
            "role": a.get("role"),
            "status": a.get("status"),
            "routing_enabled": a.get("routingEnabled"),
            "max_concurrency": a.get("maxConcurrency"),
        })
    log.info(f"Synced {len(agents)} agents")

    # --- Topics (full refresh) ---
    log.info("Fetching topics")
    topics = _get_paginated(f"{base_url}/topics", headers, {"limit": PAGE_LIMIT})
    for t in topics:
        yield op.upsert("topics", {
            "id": t.get("id"),
            "name": t.get("name"),
            "description": t.get("description"),
            "parent_topic_id": t.get("parentTopicId"),
        })
    log.info(f"Synced {len(topics)} topics")

    # --- Customers (full refresh; Gladly doesn't expose updated_at filter on customers) ---
    log.info("Fetching customers")
    customers = _get_paginated(f"{base_url}/customers", headers, {"limit": PAGE_LIMIT})
    for c in customers:
        yield op.upsert("customers", {
            "id": c.get("id"),
            "created_at": c.get("createdAt"),
            "updated_at": c.get("updatedAt"),
            "name": c.get("name"),
            "email": _first_email(c),
            "phone": _first_phone(c),
            "external_customer_id": c.get("externalCustomerId"),
            "custom_attributes": str(c.get("customAttributes") or {}),
        })
    log.info(f"Synced {len(customers)} customers")

    # --- Conversations (incremental via updatedAt) ---
    log.info(f"Fetching conversations updated since {updated_at_min}")
    params = {"limit": PAGE_LIMIT, "updatedAfter": updated_at_min}
    conversations = _get_paginated(f"{base_url}/conversations", headers, params)

    for conv in conversations:
        conv_id = conv.get("id")
        updated_at = conv.get("updatedAt", "")

        yield op.upsert("conversations", {
            "id": conv_id,
            "created_at": conv.get("createdAt"),
            "updated_at": updated_at,
            "customer_id": conv.get("customerId"),
            "assignee_id": _nested(conv, "assignee", "id"),
            "assignee_type": _nested(conv, "assignee", "type"),
            "inbox_id": conv.get("inboxId"),
            "status": conv.get("status"),
            "topic_ids": ",".join(conv.get("topicIds") or []),
            "external_id": conv.get("externalId"),
            "note": conv.get("note"),
        })

        # Conversation items (messages/actions) nested in response
        for item in conv.get("items") or []:
            yield op.upsert("conversation_items", {
                "id": item.get("id"),
                "conversation_id": conv_id,
                "created_at": item.get("createdAt"),
                "type": item.get("type"),
                "initiator": item.get("initiator"),
                "author_type": _nested(item, "author", "type"),
                "author_id": _nested(item, "author", "id"),
                "content": item.get("content"),
                "channel": item.get("channel"),
            })

        if updated_at and updated_at > latest_updated_at:
            latest_updated_at = updated_at

    log.info(f"Synced {len(conversations)} conversations")
    yield op.checkpoint(state={"updated_at_min": latest_updated_at})


def _first_email(customer: dict) -> str | None:
    emails = customer.get("emails") or []
    return emails[0].get("normalized") if emails else customer.get("email")


def _first_phone(customer: dict) -> str | None:
    phones = customer.get("phones") or []
    return phones[0].get("normalized") if phones else customer.get("phone")


def _nested(obj: dict, key: str, subkey: str):
    inner = obj.get(key) or {}
    return inner.get(subkey) if isinstance(inner, dict) else None


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
