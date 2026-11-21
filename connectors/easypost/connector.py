# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs shipments, trackers, tracking details, addresses and refunds from the EasyPost API.
Shipments and trackers are synced incrementally with a lookback window so status changes are picked up.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For adding a delay between retries and computing sync windows
import time
from datetime import datetime, timedelta, timezone

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.easypost.com/v2"
__PAGE_SIZE = 100
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START = "2010-01-01T00:00:00Z"
# Shipments and trackers keep changing status after they are created, and the API has no
# "updated since" filter. Each sync re-reads this many days before the previous sync start.
__LOOKBACK_DAYS = 30
__TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

__SHIPMENT_FIELDS = ["id", "tracking_code", "status", "batch_id", "reference", "created_at", "updated_at"]
__TRACKER_FIELDS = [
    "id", "tracking_code", "carrier", "status", "weight", "est_delivery_date", "shipment_id",
    "created_at", "updated_at",
]
__TRACKING_DETAIL_FIELDS = ["message", "status", "datetime", "source", "carrier_code"]
__ADDRESS_FIELDS = [
    "id", "name", "company", "street1", "street2", "city", "state", "zip", "country", "phone",
    "email", "created_at", "updated_at",
]
__REFUND_FIELDS = [
    "id", "tracking_code", "confirmation_number", "carrier", "shipment_id", "status", "created_at",
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


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the timestamps that must be typed are declared, so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    timestamps = {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"}
    return [
        {"table": "shipments", "primary_key": ["id"], "columns": timestamps},
        {"table": "trackers", "primary_key": ["id"], "columns": timestamps},
        {
            "table": "tracking_details",
            "primary_key": ["tracker_id", "datetime", "source"],
            "columns": {"datetime": "UTC_DATETIME"},
        },
        {"table": "addresses", "primary_key": ["id"], "columns": timestamps},
        {"table": "refunds", "primary_key": ["id"], "columns": {"created_at": "UTC_DATETIME"}},
    ]


def make_request(api_key: str, endpoint: str, params: dict):
    """
    Send a GET request to the EasyPost API, retrying transient failures with exponential backoff.
    Args:
        api_key: the EasyPost API key, sent as the HTTP Basic username with an empty password.
        endpoint: the endpoint path under the base URL, for example "shipments".
        params: query parameters.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, or when retries are exhausted.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(
                f"{__BASE_URL}/{endpoint}",
                auth=(api_key, ""),
                params=params,
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"EasyPost request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {endpoint}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"EasyPost rejected the API key (HTTP {response.status_code}). Check api_key."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} on {endpoint}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"EasyPost request to {endpoint} failed: HTTP {response.status_code}")
        return response.json()
    raise RuntimeError(f"EasyPost request to {endpoint} still failing after {__MAX_RETRIES} retries")


def paginate(api_key: str, endpoint: str, extra_params: dict = None):
    """
    Yield every record of a list endpoint, one page at a time, using before_id pagination.
    Pages are consumed as they arrive so the full result set is never held in memory.
    Args:
        api_key: the EasyPost API key.
        endpoint: the list endpoint, which is also the response key holding the records.
        extra_params: additional query parameters such as a date window.
    """
    params = {"page_size": __PAGE_SIZE, **(extra_params or {})}
    total = 0
    while True:
        data = make_request(api_key, endpoint, params)
        records = data.get(endpoint, [])
        for record in records:
            yield record
        total += len(records)
        log.info(f"{endpoint}: {total} records read")
        if not records or not data.get("has_more"):
            return
        params["before_id"] = records[-1]["id"]


def sync_shipments(api_key: str, window: dict):
    """
    Upsert shipments created inside the date window. Only purchased shipments are listed by the API.
    Args:
        api_key: the EasyPost API key.
        window: start_datetime and end_datetime query parameters.
    """
    for shipment in paginate(api_key, "shipments", window):
        selected_rate = shipment.get("selected_rate") or {}
        row = {f: shipment.get(f) for f in __SHIPMENT_FIELDS}
        row["carrier"] = selected_rate.get("carrier")
        row["service"] = selected_rate.get("service")
        row["rate_id"] = selected_rate.get("id")
        row["from_address_id"] = (shipment.get("from_address") or {}).get("id")
        row["to_address_id"] = (shipment.get("to_address") or {}).get("id")
        row["parcel_id"] = (shipment.get("parcel") or {}).get("id")
        # The 'upsert' operation inserts or updates a row by primary key.
        op.upsert(table="shipments", data=row)


def sync_trackers(api_key: str, window: dict):
    """
    Upsert trackers created inside the date window, plus their tracking details.
    Args:
        api_key: the EasyPost API key.
        window: start_datetime and end_datetime query parameters.
    """
    for tracker in paginate(api_key, "trackers", window):
        op.upsert(table="trackers", data={f: tracker.get(f) for f in __TRACKER_FIELDS})
        for detail in tracker.get("tracking_details") or []:
            location = detail.get("tracking_location") or {}
            row = {f: detail.get(f) for f in __TRACKING_DETAIL_FIELDS}
            row["tracker_id"] = tracker.get("id")
            row["tracking_location_city"] = location.get("city")
            row["tracking_location_state"] = location.get("state")
            row["tracking_location_country"] = location.get("country")
            row["tracking_location_zip"] = location.get("zip")
            op.upsert(table="tracking_details", data=row)


def sync_simple_table(api_key: str, endpoint: str, fields: list):
    """
    Upsert every record of a list endpoint that has no date filter (addresses, refunds).
    Args:
        api_key: the EasyPost API key.
        endpoint: the list endpoint, which is also the destination table name.
        fields: the record fields to copy into the table.
    """
    for record in paginate(api_key, endpoint):
        op.upsert(table=endpoint, data={f: record.get(f) for f in fields})


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

    sync_started_at = datetime.now(timezone.utc)
    last_sync_start = state.get("last_sync_start")
    if last_sync_start:
        previous = datetime.strptime(last_sync_start, __TIME_FORMAT).replace(tzinfo=timezone.utc)
        start = (previous - timedelta(days=__LOOKBACK_DAYS)).strftime(__TIME_FORMAT)
    else:
        start = __DEFAULT_START

    # The API silently returns nothing for windows longer than ~30 days unless end_datetime is set.
    window = {"start_datetime": start, "end_datetime": sync_started_at.strftime(__TIME_FORMAT)}
    log.info(f"Syncing shipments and trackers created between {window['start_datetime']} and {window['end_datetime']}")

    sync_shipments(api_key, window)
    sync_trackers(api_key, window)
    sync_simple_table(api_key, "addresses", __ADDRESS_FIELDS)
    sync_simple_table(api_key, "refunds", __REFUND_FIELDS)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state={"last_sync_start": sync_started_at.strftime(__TIME_FORMAT)})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
