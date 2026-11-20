import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

REGION_URLS = {
    "us": "https://degreed.com",
    "eu": "https://eu.degreed.com",
    "ca": "https://ca.degreed.com",
}
PAGE_LIMIT = 100


def schema(configuration: dict):
    return [
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "email": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "profile_visibility": "STRING",
                "role": "STRING",
                "restricted": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "modified_at": "UTC_DATETIME",
            },
        },
        {
            "table": "completions",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "user_id": "STRING",
                "content_id": "STRING",
                "content_type": "STRING",
                "completed_at": "UTC_DATETIME",
                "points_earned": "FLOAT",
                "duration_in_minutes": "FLOAT",
            },
        },
        {
            "table": "learning_paths",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "title": "STRING",
                "description": "STRING",
                "owner_id": "STRING",
                "created_at": "UTC_DATETIME",
                "modified_at": "UTC_DATETIME",
                "published_at": "UTC_DATETIME",
            },
        },
        {
            "table": "skills",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "owner_id": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "user_skills",
            "primary_key": ["user_id", "skill_id"],
            "columns": {
                "user_id": "STRING",
                "skill_id": "STRING",
                "score": "FLOAT",
                "level": "STRING",
                "evaluated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "content",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "title": "STRING",
                "content_type": "STRING",
                "provider": "STRING",
                "url": "STRING",
                "duration_in_minutes": "FLOAT",
                "language": "STRING",
                "obsolete": "BOOLEAN",
            },
        },
    ]


def _get_token(base_url: str, client_id: str, client_secret: str) -> str:
    resp = requests.post(
        f"{base_url}/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _get_page(base_url: str, token: str, path: str, params: dict) -> dict:
    resp = requests.get(
        f"{base_url}/api/v2/{path}",
        headers=_headers(token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _attr(record: dict, key: str):
    """Degreed API returns JSON:API format: data[].attributes + data[].id"""
    attrs = record.get("attributes", record)
    return attrs.get(key)


def update(configuration: dict, state: dict):
    region = configuration.get("region", "us")
    base_url = REGION_URLS.get(region, REGION_URLS["us"])
    client_id = configuration["client_id"]
    client_secret = configuration["client_secret"]

    token = _get_token(base_url, client_id, client_secret)

    # --- Users (cursor-based incremental) ---
    log.info("Syncing users")
    cursor = state.get("users_cursor")
    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "users", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            rid = r.get("id")
            yield op.upsert("users", {
                "id": rid,
                "email": _attr(r, "email"),
                "first_name": _attr(r, "firstName") or _attr(r, "first_name"),
                "last_name": _attr(r, "lastName") or _attr(r, "last_name"),
                "profile_visibility": _attr(r, "profileVisibility") or _attr(r, "profile_visibility"),
                "role": _attr(r, "role"),
                "restricted": _attr(r, "restricted"),
                "created_at": _attr(r, "createdAt") or _attr(r, "created_at"),
                "modified_at": _attr(r, "modifiedAt") or _attr(r, "modified_at"),
            })
        links = data.get("links", {})
        next_cursor = links.get("next")
        if not next_cursor or len(records) < PAGE_LIMIT:
            cursor = None
            break
        # Extract cursor value from next URL if it's a full URL
        if "next=" in str(next_cursor):
            import urllib.parse
            parsed = urllib.parse.urlparse(next_cursor)
            cursor = urllib.parse.parse_qs(parsed.query).get("next", [None])[0]
        else:
            cursor = next_cursor
    yield op.checkpoint(state={**state, "users_cursor": None})

    # --- Completions (incremental by date window) ---
    log.info("Syncing completions")
    completions_since = state.get("completions_since", "2010-01-01")
    latest_completion = completions_since
    cursor = None
    while True:
        params = {"limit": PAGE_LIMIT, "start_date": completions_since}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "completions", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            completed_at = _attr(r, "completedAt") or _attr(r, "completed_at") or ""
            if completed_at and completed_at > latest_completion:
                latest_completion = completed_at
            yield op.upsert("completions", {
                "id": r.get("id"),
                "user_id": _attr(r, "userId") or _attr(r, "user_id"),
                "content_id": _attr(r, "contentId") or _attr(r, "content_id"),
                "content_type": _attr(r, "contentType") or _attr(r, "content_type"),
                "completed_at": completed_at,
                "points_earned": _attr(r, "pointsEarned") or _attr(r, "points_earned"),
                "duration_in_minutes": _attr(r, "durationInMinutes") or _attr(r, "duration_in_minutes"),
            })
        links = data.get("links", {})
        next_cursor = links.get("next")
        if not next_cursor or len(records) < PAGE_LIMIT:
            break
        cursor = next_cursor
    yield op.checkpoint(state={**state, "completions_since": latest_completion})

    # --- Learning Paths (full refresh) ---
    log.info("Syncing learning_paths")
    cursor = None
    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "pathways", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            yield op.upsert("learning_paths", {
                "id": r.get("id"),
                "title": _attr(r, "title"),
                "description": _attr(r, "description"),
                "owner_id": _attr(r, "ownerId") or _attr(r, "owner_id"),
                "created_at": _attr(r, "createdAt") or _attr(r, "created_at"),
                "modified_at": _attr(r, "modifiedAt") or _attr(r, "modified_at"),
                "published_at": _attr(r, "publishedAt") or _attr(r, "published_at"),
            })
        links = data.get("links", {})
        if not links.get("next") or len(records) < PAGE_LIMIT:
            break
        cursor = links["next"]

    # --- Skills (full refresh) ---
    log.info("Syncing skills")
    cursor = None
    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "skills", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            yield op.upsert("skills", {
                "id": r.get("id"),
                "name": _attr(r, "name"),
                "description": _attr(r, "description"),
                "owner_id": _attr(r, "ownerId") or _attr(r, "owner_id"),
                "created_at": _attr(r, "createdAt") or _attr(r, "created_at"),
            })
        links = data.get("links", {})
        if not links.get("next") or len(records) < PAGE_LIMIT:
            break
        cursor = links["next"]

    # --- User Skills (incremental) ---
    log.info("Syncing user_skills")
    user_skills_since = state.get("user_skills_since", "2010-01-01")
    latest_eval = user_skills_since
    cursor = None
    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "user-skills", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            evaluated_at = _attr(r, "evaluatedAt") or _attr(r, "evaluated_at") or ""
            if evaluated_at and evaluated_at > latest_eval:
                latest_eval = evaluated_at
            yield op.upsert("user_skills", {
                "user_id": _attr(r, "userId") or _attr(r, "user_id"),
                "skill_id": _attr(r, "skillId") or _attr(r, "skill_id"),
                "score": _attr(r, "score"),
                "level": _attr(r, "level"),
                "evaluated_at": evaluated_at,
            })
        links = data.get("links", {})
        if not links.get("next") or len(records) < PAGE_LIMIT:
            break
        cursor = links["next"]
    yield op.checkpoint(state={**state, "user_skills_since": latest_eval})

    # --- Content (full refresh) ---
    log.info("Syncing content")
    cursor = None
    while True:
        params = {"limit": PAGE_LIMIT}
        if cursor:
            params["next"] = cursor
        data = _get_page(base_url, token, "content", params)
        records = data.get("data", [])
        if not records:
            break
        for r in records:
            yield op.upsert("content", {
                "id": r.get("id"),
                "title": _attr(r, "title"),
                "content_type": _attr(r, "contentType") or _attr(r, "content_type"),
                "provider": _attr(r, "provider"),
                "url": _attr(r, "url"),
                "duration_in_minutes": _attr(r, "durationInMinutes") or _attr(r, "duration_in_minutes"),
                "language": _attr(r, "language"),
                "obsolete": _attr(r, "obsolete"),
            })
        links = data.get("links", {})
        if not links.get("next") or len(records) < PAGE_LIMIT:
            break
        cursor = links["next"]

    yield op.checkpoint(state={
        "users_cursor": None,
        "completions_since": latest_completion,
        "user_skills_since": latest_eval,
    })


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
