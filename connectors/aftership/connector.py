# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs shipment tracking data from the AfterShip Tracking API (version 2024-10).
It delivers trackings, their checkpoints, estimated deliveries, next couriers and the courier list.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For adding a delay between retries
import time

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, ConfigurationForm, Test, form_field

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.aftership.com/tracking/2024-10"
__PAGE_LIMIT = 200
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START = "2010-01-01T00:00:00+00:00"

# Top-level tracking fields copied to the trackings table as-is.
# Nested objects (checkpoints, estimated deliveries, next couriers) go to their own tables.
__TRACKING_FIELDS = [
    "id", "legacy_id", "created_at", "updated_at", "last_updated_at", "tracking_number", "slug",
    "active", "title", "note", "tag", "subtag", "subtag_message", "source", "signed_by",
    "shipment_type", "shipment_weight", "shipment_delivery_date", "shipment_pickup_date",
    "origin_country_iso3", "origin_city", "origin_state", "origin_postal_code",
    "origin_raw_location", "destination_country_iso3", "destination_city", "destination_state",
    "destination_postal_code", "destination_raw_location", "customer_name",
    "tracking_postal_code", "tracking_ship_date", "tracking_account_number",
    "tracking_destination_country", "on_time_status", "on_time_difference", "transit_time",
    "tracked_count", "failed_delivery_attempts", "return_to_sender", "first_attempted_at",
    "order_id", "order_number", "order_date", "courier_tracking_link",
]

__CHECKPOINT_FIELDS = [
    "slug", "city", "state", "zip", "location", "country_name", "country_iso3", "message", "tag",
    "subtag", "subtag_message", "checkpoint_time", "created_at", "raw_tag",
]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    for key in ["api_key"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")


def connection_test(configuration: dict):
    """
    Setup test that checks the API key with a one-row request to the trackings endpoint.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A success or failure Test result shown on the Fivetran setup page.
    """
    test = Test()
    try:
        validate_configuration(configuration=configuration)
        make_request(configuration["api_key"], "/trackings", {"limit": 1})
    except (ValueError, RuntimeError) as error:
        return test.failure(str(error))
    return test.success()


def configuration_form():
    """
    Declare the setup form Fivetran renders for this connector, and its setup test.
    Returns:
        A ConfigurationForm with one field per configuration key and one setup test.
    """
    form = ConfigurationForm()
    form.add_field(
        form_field.TextField(
            "api_key",
            label="AfterShip API key",
            required=True,
            field_type=form_field.TextField.password,
            description="Create one under Settings, API Keys in the AfterShip dashboard.",
        )
    )
    form.add_test(label="Check the API key against AfterShip", func=connection_test)
    return form


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the timestamps that must be typed are declared, so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "trackings",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"},
        },
        {
            "table": "checkpoints",
            "primary_key": ["tracking_id", "checkpoint_time", "slug"],
            "columns": {"checkpoint_time": "UTC_DATETIME"},
        },
        {"table": "estimated_deliveries", "primary_key": ["tracking_id", "source"]},
        {"table": "next_couriers", "primary_key": ["tracking_id", "slug"]},
        {"table": "couriers", "primary_key": ["slug"]},
    ]


def make_request(api_key: str, path: str, params: dict = None):
    """
    Send a GET request to the AfterShip API, retrying transient failures with exponential backoff.
    Args:
        api_key: the AfterShip API key.
        path: the endpoint path under the base URL.
        params: optional query parameters.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, or when retries are exhausted.
    """
    headers = {"as-api-key": api_key, "Content-Type": "application/json"}
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(
                f"{__BASE_URL}{path}",
                headers=headers,
                params=params,
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"AfterShip request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {path}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"AfterShip rejected the API key (HTTP {response.status_code}). Check api_key."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} on {path}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"AfterShip request to {path} failed: HTTP {response.status_code}")
        return response.json()
    raise RuntimeError(f"AfterShip request to {path} still failing after {__MAX_RETRIES} retries")


def estimated_delivery_rows(tracking_id: str, tracking: dict):
    """
    Build estimated_deliveries rows from the nested estimated delivery objects of a tracking.
    Args:
        tracking_id: the id of the parent tracking.
        tracking: the tracking object returned by the API.
    Returns:
        A list of row dictionaries.
    """
    rows = []
    first = tracking.get("first_estimated_delivery") or {}
    if first:
        rows.append({
            "tracking_id": tracking_id,
            "source": first.get("source") or "first",
            "type": first.get("type"),
            "datetime": first.get("datetime"),
            "datetime_min": first.get("datetime_min"),
            "datetime_max": first.get("datetime_max"),
        })
    courier = tracking.get("courier_estimated_delivery_date") or {}
    if courier:
        rows.append({
            "tracking_id": tracking_id,
            "source": "courier",
            "estimated_delivery_date": courier.get("estimated_delivery_date"),
            "estimated_delivery_date_min": courier.get("estimated_delivery_date_min"),
            "estimated_delivery_date_max": courier.get("estimated_delivery_date_max"),
        })
    return rows


def sync_tracking(tracking: dict):
    """
    Upsert one tracking and its child records (checkpoints, estimated deliveries, next couriers).
    Args:
        tracking: the tracking object returned by the API.
    """
    tracking_id = tracking["id"]

    # The 'upsert' operation inserts or updates a row by primary key.
    op.upsert(table="trackings", data={f: tracking.get(f) for f in __TRACKING_FIELDS})

    for checkpoint in tracking.get("checkpoints") or []:
        row = {f: checkpoint.get(f) for f in __CHECKPOINT_FIELDS}
        coordinate = checkpoint.get("coordinate") or {}
        row["tracking_id"] = tracking_id
        row["coordinate_lat"] = coordinate.get("latitude")
        row["coordinate_lng"] = coordinate.get("longitude")
        op.upsert(table="checkpoints", data=row)

    for row in estimated_delivery_rows(tracking_id, tracking):
        op.upsert(table="estimated_deliveries", data=row)

    for next_courier in tracking.get("next_couriers") or []:
        op.upsert(table="next_couriers", data={
            "tracking_id": tracking_id,
            "slug": next_courier.get("slug"),
            "tracking_number": next_courier.get("tracking_number"),
            "source": next_courier.get("source"),
        })


def sync_trackings(api_key: str, updated_at_min: str):
    """
    Page through trackings updated since the given timestamp, using cursor pagination.
    Each page is written as it arrives so the full result set is never held in memory.
    Args:
        api_key: the AfterShip API key.
        updated_at_min: ISO 8601 timestamp; only trackings updated at or after it are fetched.
    Returns:
        The latest updated_at value seen, or updated_at_min if nothing newer was returned.
    """
    latest_updated_at = updated_at_min
    cursor = ""
    total = 0
    while True:
        params = {"limit": __PAGE_LIMIT, "updated_at_min": updated_at_min}
        if cursor:
            params["cursor"] = cursor
        data = make_request(api_key, "/trackings", params).get("data", {})

        for tracking in data.get("trackings", []):
            sync_tracking(tracking)
            updated_at = tracking.get("updated_at") or ""
            if updated_at > latest_updated_at:
                latest_updated_at = updated_at
            total += 1

        log.info(f"Synced {total} trackings so far")

        pagination = data.get("pagination", {})
        if not pagination.get("has_next_page"):
            return latest_updated_at
        cursor = pagination.get("next_cursor", "")


def sync_couriers(api_key: str):
    """
    Upsert the full list of couriers supported by AfterShip (full refresh each sync).
    Args:
        api_key: the AfterShip API key.
    """
    couriers = make_request(api_key, "/couriers").get("data", {}).get("couriers", [])
    for courier in couriers:
        op.upsert(table="couriers", data={
            "slug": courier.get("slug"),
            "name": courier.get("name"),
            "phone": courier.get("phone"),
            "other_name": courier.get("other_name"),
            "web_url": courier.get("web_url"),
            "required_fields": ",".join(courier.get("required_fields") or []),
            "optional_fields": ",".join(courier.get("optional_fields") or []),
            "default_language": courier.get("default_language"),
            "support_languages": ",".join(courier.get("support_languages") or []),
        })
    log.info(f"Synced {len(couriers)} couriers")


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
    api_key = configuration["api_key"]

    updated_at_min = state.get("updated_at_min", __DEFAULT_START)
    latest_updated_at = sync_trackings(api_key, updated_at_min)
    sync_couriers(api_key)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state={"updated_at_min": latest_updated_at})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
