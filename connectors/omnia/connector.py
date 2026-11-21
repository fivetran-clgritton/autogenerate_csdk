# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs pricing data from the Omnia Retail API: products, pricing strategies and price history.
The endpoints, response envelope and field names are assumed from API conventions and are not yet verified
against a live account (see README.md).
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

__BASE_URL = "https://api.omniaretail.com"
__PAGE_SIZE = 500
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_HISTORY_START = "2020-01-01T00:00:00Z"

# Output column -> accepted source keys (camelCase first, then snake_case). The first value that is not None wins.
__STRATEGY_FIELDS = {
    "name": ("name",),
    "type": ("type",),
    "description": ("description",),
    "currency": ("currency",),
    "is_active": ("isActive", "is_active"),
    "created_at": ("createdAt", "created_at"),
    "updated_at": ("updatedAt", "updated_at"),
    "rule_count": ("ruleCount", "rule_count"),
    "product_count": ("productCount", "product_count"),
}

__PRODUCT_FIELDS = {
    "ean": ("ean",),
    "sku": ("sku",),
    "name": ("name",),
    "brand": ("brand",),
    "category": ("category",),
    "current_price": ("currentPrice", "current_price"),
    "cost_price": ("costPrice", "cost_price"),
    "currency": ("currency",),
    "strategy_name": ("strategyName", "strategy_name"),
    "calculated_price": ("calculatedPrice", "calculated_price"),
    "min_price": ("minPrice", "min_price"),
    "max_price": ("maxPrice", "max_price"),
    "stock": ("stock",),
    "active": ("active",),
    "updated_at": ("updatedAt", "updated_at"),
}

__HISTORY_FIELDS = {
    "ean": ("ean",),
    "price": ("price",),
    "cost_price": ("costPrice", "cost_price"),
    "currency": ("currency",),
    "strategy_name": ("strategyName", "strategy_name"),
    "source": ("source",),
    "competitor_name": ("competitorName", "competitor_name"),
    "competitor_price": ("competitorPrice", "competitor_price"),
}


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
    Setup test that checks the API key with a one-row request to the products endpoint.
    A single attempt is made so a bad key or unreachable host fails within seconds.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A success or failure Test result shown on the Fivetran setup page.
    """
    test = Test()
    try:
        validate_configuration(configuration=configuration)
        make_request(configuration["api_key"], "/products", {"page": 1, "pageSize": 1}, max_retries=1)
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
            label="Omnia Retail API key",
            required=True,
            field_type=form_field.TextField.password,
            description="API key for your Omnia Retail account, sent as a Bearer token.",
        )
    )
    form.add_test(label="Check the API key against Omnia Retail", func=connection_test)
    return form


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
            "table": "products",
            "primary_key": ["id"],
            "columns": {
                "current_price": "FLOAT",
                "cost_price": "FLOAT",
                "calculated_price": "FLOAT",
                "min_price": "FLOAT",
                "max_price": "FLOAT",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "pricing_strategies",
            "primary_key": ["id"],
            "columns": {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"},
        },
        {
            "table": "price_history",
            "primary_key": ["product_id", "timestamp"],
            "columns": {"price": "FLOAT", "cost_price": "FLOAT", "competitor_price": "FLOAT"},
        },
    ]


def make_request(api_key: str, path: str, params: dict, max_retries: int = __MAX_RETRIES):
    """
    Send a GET request to the Omnia Retail API, retrying transient failures with exponential backoff.
    Args:
        api_key: the Omnia Retail API key, sent as a Bearer token.
        path: the endpoint path under the base URL.
        params: query parameters.
        max_retries: number of attempts before giving up.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, or when retries are exhausted.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"{__BASE_URL}{path}",
                headers=headers,
                params=params,
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Omnia request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {path}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"Omnia rejected the API key (HTTP {response.status_code}). Check api_key."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            if attempt < max_retries - 1:
                log.warning(f"HTTP {response.status_code} on {path}, retrying in {delay}s")
                time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"Omnia request to {path} failed: HTTP {response.status_code}")
        return response.json()
    raise RuntimeError(f"Omnia request to {path} still failing after {max_retries} attempts")


def extract_items(body):
    """
    Return the list of records from a response that is either a bare list or a {data|items|...} envelope.
    Args:
        body: the parsed JSON response body.
    Returns:
        A list of records.
    """
    if isinstance(body, list):
        return body
    return (
        body.get("data")
        or body.get("items")
        or body.get("products")
        or body.get("strategies")
        or []
    )


def for_each_page(api_key: str, path: str, handle_page, params: dict = None):
    """
    Page through an endpoint (page and pageSize parameters) and pass each page of records to a handler,
    so the full result set is never held in memory.
    Args:
        api_key: the Omnia Retail API key.
        path: the endpoint path under the base URL.
        handle_page: callable that receives the list of records of one page.
        params: optional extra query parameters.
    Returns:
        The total number of records processed.
    """
    query = dict(params or {})
    query["pageSize"] = __PAGE_SIZE
    page = 1
    total = 0
    while True:
        query["page"] = page
        body = make_request(api_key, path, query)
        items = extract_items(body)
        if items:
            handle_page(items)
            total += len(items)
        total_pages = (body.get("totalPages") or body.get("total_pages")) if isinstance(body, dict) else None
        if not items or (total_pages is not None and page >= total_pages):
            return total
        page += 1


def first_value(record: dict, keys: tuple):
    """
    Return the first value among the given keys that is not None.
    Args:
        record: the source record.
        keys: candidate keys in priority order.
    Returns:
        The value, or None if every key is missing or None.
    """
    for key in keys:
        if record.get(key) is not None:
            return record[key]
    return None


def map_record(record: dict, fields: dict):
    """
    Build an output row from a source record using an output-column to source-keys mapping.
    Args:
        record: the source record.
        fields: mapping of output column to accepted source keys.
    Returns:
        A row dictionary.
    """
    return {column: first_value(record, keys) for column, keys in fields.items()}


def as_text(value):
    """
    Convert an identifier to a string, using an empty string when it is missing.
    Args:
        value: the identifier value or None.
    Returns:
        The identifier as a string.
    """
    return "" if value is None else str(value)


def sync_strategies(api_key: str):
    """
    Upsert all pricing strategies (full refresh each sync).
    Args:
        api_key: the Omnia Retail API key.
    """

    def handle_page(items):
        for strategy in items:
            row = map_record(strategy, __STRATEGY_FIELDS)
            row["id"] = as_text(strategy.get("id"))
            # The 'upsert' operation inserts or updates a row by primary key.
            op.upsert(table="pricing_strategies", data=row)

    total = for_each_page(api_key, "/strategies", handle_page)
    log.info(f"Synced {total} pricing strategies")


def sync_products(api_key: str):
    """
    Upsert all products (full refresh each sync).
    Args:
        api_key: the Omnia Retail API key.
    """

    def handle_page(items):
        for product in items:
            row = map_record(product, __PRODUCT_FIELDS)
            row["id"] = as_text(product.get("id"))
            row["strategy_id"] = as_text(first_value(product, ("strategyId", "strategy_id")))
            op.upsert(table="products", data=row)

    total = for_each_page(api_key, "/products", handle_page)
    log.info(f"Synced {total} products")


def sync_price_history(api_key: str, history_from: str):
    """
    Upsert price history records since the given timestamp, tracking the latest timestamp seen.
    Args:
        api_key: the Omnia Retail API key.
        history_from: ISO 8601 timestamp sent as the `from` query parameter.
    Returns:
        The latest record timestamp seen, or history_from if nothing newer was returned.
    """
    latest = {"timestamp": history_from}

    def handle_page(items):
        for record in items:
            timestamp = first_value(record, ("timestamp", "date", "createdAt")) or ""
            if timestamp and timestamp > latest["timestamp"]:
                latest["timestamp"] = timestamp
            row = map_record(record, __HISTORY_FIELDS)
            row["product_id"] = as_text(first_value(record, ("productId", "product_id")))
            row["timestamp"] = timestamp
            row["strategy_id"] = as_text(first_value(record, ("strategyId", "strategy_id")))
            op.upsert(table="price_history", data=row)

    total = for_each_page(api_key, "/price-history", handle_page, params={"from": history_from})
    log.info(f"Synced {total} price history records since {history_from}")
    return latest["timestamp"]


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

    sync_strategies(api_key)
    sync_products(api_key)
    latest_timestamp = sync_price_history(
        api_key, state.get("price_history_from", __DEFAULT_HISTORY_START)
    )

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state={"price_history_from": latest_timestamp})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
