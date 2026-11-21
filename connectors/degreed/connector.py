# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs learning data from the Degreed API (v2): users, completions, learning
paths, skills, user skills and content. It authenticates with OAuth 2.0 client credentials.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For adding a delay between retries
import time

# For parsing the cursor out of a pagination link
import urllib.parse

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

__REGION_URLS = {
    "us": "https://degreed.com",
    "eu": "https://eu.degreed.com",
    "ca": "https://ca.degreed.com",
}
__DEFAULT_REGION = "us"
__PAGE_LIMIT = 100
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START_DATE = "2010-01-01"

# Column -> source attribute names. Degreed returns JSON:API records (an id plus attributes);
# the first attribute name that has a value is used, so camelCase and snake_case both work.
__USERS_FIELDS = {
    "email": ("email",),
    "first_name": ("firstName", "first_name"),
    "last_name": ("lastName", "last_name"),
    "profile_visibility": ("profileVisibility", "profile_visibility"),
    "role": ("role",),
    "restricted": ("restricted",),
    "created_at": ("createdAt", "created_at"),
    "modified_at": ("modifiedAt", "modified_at"),
}
__COMPLETIONS_FIELDS = {
    "user_id": ("userId", "user_id"),
    "content_id": ("contentId", "content_id"),
    "content_type": ("contentType", "content_type"),
    "completed_at": ("completedAt", "completed_at"),
    "points_earned": ("pointsEarned", "points_earned"),
    "duration_in_minutes": ("durationInMinutes", "duration_in_minutes"),
}
__LEARNING_PATHS_FIELDS = {
    "title": ("title",),
    "description": ("description",),
    "owner_id": ("ownerId", "owner_id"),
    "created_at": ("createdAt", "created_at"),
    "modified_at": ("modifiedAt", "modified_at"),
    "published_at": ("publishedAt", "published_at"),
}
__SKILLS_FIELDS = {
    "name": ("name",),
    "description": ("description",),
    "owner_id": ("ownerId", "owner_id"),
    "created_at": ("createdAt", "created_at"),
}
__USER_SKILLS_FIELDS = {
    "user_id": ("userId", "user_id"),
    "skill_id": ("skillId", "skill_id"),
    "score": ("score",),
    "level": ("level",),
    "evaluated_at": ("evaluatedAt", "evaluated_at"),
}
__CONTENT_FIELDS = {
    "title": ("title",),
    "content_type": ("contentType", "content_type"),
    "provider": ("provider",),
    "url": ("url",),
    "duration_in_minutes": ("durationInMinutes", "duration_in_minutes"),
    "language": ("language",),
    "obsolete": ("obsolete",),
}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required value is missing or the region is not supported.
    """
    for key in ["client_id", "client_secret"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    region = configuration.get("region") or __DEFAULT_REGION
    if region not in __REGION_URLS:
        raise ValueError(f"Unsupported region '{region}'. Use one of: {', '.join(__REGION_URLS)}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the columns whose type must be forced are declared, so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "modified_at": "UTC_DATETIME"},
        },
        {
            "table": "completions",
            "primary_key": ["id"],
            "columns": {
                "completed_at": "UTC_DATETIME",
                "points_earned": "FLOAT",
                "duration_in_minutes": "FLOAT",
            },
        },
        {
            "table": "learning_paths",
            "primary_key": ["id"],
            "columns": {
                "created_at": "UTC_DATETIME",
                "modified_at": "UTC_DATETIME",
                "published_at": "UTC_DATETIME",
            },
        },
        {
            "table": "skills",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME"},
        },
        {
            "table": "user_skills",
            "primary_key": ["user_id", "skill_id"],
            "columns": {"score": "FLOAT", "evaluated_at": "UTC_DATETIME"},
        },
        {
            "table": "content",
            "primary_key": ["id"],
            "columns": {"duration_in_minutes": "FLOAT"},
        },
    ]


def send_request(method: str, url: str, **kwargs):
    """
    Send an HTTP request, retrying network errors, HTTP 429 and 5xx with exponential backoff.
    Any other status, including 401, is returned to the caller to handle.
    Args:
        method: the HTTP method.
        url: the full request URL.
        kwargs: extra arguments passed to requests.request (headers, params, data).
    Returns:
        The requests.Response object.
    Raises:
        RuntimeError: when the request still fails after all retries.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.request(method, url, timeout=__REQUEST_TIMEOUT_SECONDS, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"Degreed request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} from Degreed, retrying in {delay}s")
            time.sleep(delay)
            continue
        return response
    raise RuntimeError(f"Degreed request still failing after {__MAX_RETRIES} retries")


def get_token(auth: dict):
    """
    Request an OAuth 2.0 access token with the client credentials grant and cache it in auth.
    Args:
        auth: dictionary holding base_url, client_id, client_secret and the cached token.
    Raises:
        RuntimeError: if Degreed rejects the credentials.
    """
    response = send_request(
        "POST",
        f"{auth['base_url']}/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": auth["client_id"],
            "client_secret": auth["client_secret"],
        },
    )
    if not response.ok:
        raise RuntimeError(
            f"Degreed rejected the client credentials (HTTP {response.status_code}). "
            "Check client_id, client_secret and region."
        )
    auth["token"] = response.json()["access_token"]


def api_get(auth: dict, path: str, params: dict):
    """
    GET an endpoint under /api/v2 with the cached token, refreshing the token once on a 401.
    Args:
        auth: dictionary holding base_url, client_id, client_secret and the cached token.
        path: the endpoint path, such as "users".
        params: query parameters.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures or other non-successful responses.
    """
    url = f"{auth['base_url']}/api/v2/{path}"
    response = send_request("GET", url, headers={"Authorization": f"Bearer {auth['token']}"}, params=params)
    if response.status_code == 401:
        log.info("Access token rejected, requesting a new one")
        get_token(auth)
        response = send_request("GET", url, headers={"Authorization": f"Bearer {auth['token']}"}, params=params)
    if response.status_code in (401, 403):
        raise RuntimeError(f"Degreed denied access to {path} (HTTP {response.status_code}). Check API permissions.")
    if not response.ok:
        raise RuntimeError(f"Degreed request to {path} failed: HTTP {response.status_code}")
    return response.json()


def attribute(record: dict, names: tuple):
    """
    Read a JSON:API attribute, falling back to the plain record when there is no attributes object.
    Args:
        record: one record from the API response.
        names: candidate attribute names, in priority order.
    Returns:
        The first value that is not None, or None.
    """
    attributes = record.get("attributes", record)
    for name in names:
        value = attributes.get(name)
        if value is not None:
            return value
    return None


def next_cursor(links: dict):
    """
    Extract the pagination cursor from the "next" link, which may be a full URL or a bare cursor.
    Args:
        links: the links object of an API response.
    Returns:
        The cursor value, or None when there is no next page.
    """
    link = links.get("next")
    if not link:
        return None
    if "next=" in str(link):
        query = urllib.parse.urlparse(link).query
        return urllib.parse.parse_qs(query).get("next", [None])[0]
    return link


def sync_endpoint(auth: dict, path: str, table: str, fields: dict, with_id: bool = True,
                  params: dict = None, track_column: str = None):
    """
    Page through an endpoint and upsert every record, writing each page as it arrives.
    Args:
        auth: dictionary holding base_url, client_id, client_secret and the cached token.
        path: the endpoint path under /api/v2.
        table: the destination table.
        fields: column -> source attribute names mapping for the table.
        with_id: whether to copy the record id into the row (false for user_skills, keyed by attributes).
        params: extra query parameters sent with every page request.
        track_column: optional column whose maximum value is returned, for incremental state.
    Returns:
        The largest value seen in track_column, or None.
    """
    cursor = None
    total = 0
    latest = None
    while True:
        page_params = {"limit": __PAGE_LIMIT, **(params or {})}
        if cursor:
            page_params["next"] = cursor
        records = api_get(auth, path, page_params)
        page = records.get("data", [])
        if not page:
            break

        for record in page:
            row = {"id": record.get("id")} if with_id else {}
            for column, names in fields.items():
                row[column] = attribute(record, names)
            # The 'upsert' operation inserts or updates a row by primary key.
            op.upsert(table=table, data=row)
            tracked = row.get(track_column) if track_column else None
            if tracked and (latest is None or tracked > latest):
                latest = tracked

        total += len(page)
        log.info(f"{table}: synced {total} records so far")

        cursor = next_cursor(records.get("links", {}))
        if not cursor or len(page) < __PAGE_LIMIT:
            break
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

    region = configuration.get("region") or __DEFAULT_REGION
    auth = {
        "base_url": __REGION_URLS[region],
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
        "token": None,
    }
    get_token(auth)

    # Users: full refresh each sync.
    sync_endpoint(auth, "users", "users", __USERS_FIELDS)
    op.checkpoint(state=state)

    # Completions: incremental from the latest completed_at, sent as start_date.
    completions_since = state.get("completions_since", __DEFAULT_START_DATE)
    latest_completion = sync_endpoint(
        auth, "completions", "completions", __COMPLETIONS_FIELDS,
        params={"start_date": completions_since}, track_column="completed_at",
    )
    state = {**state, "completions_since": latest_completion or completions_since}
    op.checkpoint(state=state)

    # Learning paths and skills: full refresh each sync.
    sync_endpoint(auth, "pathways", "learning_paths", __LEARNING_PATHS_FIELDS)
    sync_endpoint(auth, "skills", "skills", __SKILLS_FIELDS)

    # User skills: the latest evaluated_at is tracked in state; the endpoint is read in full each sync.
    user_skills_since = state.get("user_skills_since", __DEFAULT_START_DATE)
    latest_evaluation = sync_endpoint(
        auth, "user-skills", "user_skills", __USER_SKILLS_FIELDS,
        with_id=False, track_column="evaluated_at",
    )
    state = {**state, "user_skills_since": latest_evaluation or user_skills_since}
    op.checkpoint(state=state)

    # Content: full refresh each sync.
    sync_endpoint(auth, "content", "content", __CONTENT_FIELDS)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state=state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
