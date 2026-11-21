# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs orders, order items, products and product SKUs from the Lazada Open Platform API.
Each request is signed with HMAC-SHA256 and sent to the regional endpoint selected in the configuration.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For signing requests
import hashlib
import hmac

# For request timestamps and retry delays
import time

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

__REGION_URLS = {
    "my": "https://api.lazada.com.my/rest",
    "th": "https://api.lazada.co.th/rest",
    "sg": "https://api.lazada.sg/rest",
    "ph": "https://api.lazada.com.ph/rest",
    "id": "https://api.lazada.co.id/rest",
    "vn": "https://api.lazada.vn/rest",
}
__DEFAULT_REGION = "my"
__PAGE_SIZE = 100
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30
__DEFAULT_START = "2020-01-01T00:00:00+08:00"
# Lazada returns this code inside an HTTP 200 body when the app exceeds its call quota.
__RATE_LIMIT_CODE = "ApiCallLimit"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required parameter is missing or the region is not supported.
    """
    for key in ["app_key", "app_secret", "access_token"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    region = configuration.get("region", __DEFAULT_REGION).lower()
    if region not in __REGION_URLS:
        raise ValueError(f"Unsupported region '{region}'. Use one of: {', '.join(__REGION_URLS)}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys are declared, so the SDK infers column types and the schema can evolve.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "orders", "primary_key": ["order_id"]},
        {"table": "order_items", "primary_key": ["order_item_id"]},
        {"table": "products", "primary_key": ["item_id"]},
        {"table": "product_skus", "primary_key": ["shop_sku"]},
    ]


def sign(app_secret: str, endpoint: str, params: dict):
    """
    Compute the Lazada HMAC-SHA256 signature: the endpoint path followed by the sorted key/value pairs.
    Args:
        app_secret: the app secret used as the HMAC key.
        endpoint: the endpoint path, for example /orders/get.
        params: all request parameters except the signature itself.
    Returns:
        The upper-case hex signature.
    """
    sign_str = endpoint + "".join(f"{k}{v}" for k, v in sorted(params.items()))
    return hmac.new(
        app_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256
    ).hexdigest().upper()


def make_request(configuration: dict, endpoint: str, extra: dict):
    """
    Send a signed GET request to the Lazada API, retrying transient failures with exponential backoff.
    The request is re-signed with a fresh timestamp on every attempt.
    Args:
        configuration: the connector configuration holding the credentials and region.
        endpoint: the endpoint path, for example /orders/get.
        extra: endpoint-specific query parameters.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, API errors, other 4xx responses, or exhausted retries.
    """
    region = configuration.get("region", __DEFAULT_REGION).lower()
    base_url = __REGION_URLS[region]
    for attempt in range(__MAX_RETRIES):
        params = {
            "app_key": configuration["app_key"],
            "access_token": configuration["access_token"],
            "timestamp": str(int(time.time() * 1000)),
            "sign_method": "sha256",
            **extra,
        }
        params["sign"] = sign(configuration["app_secret"], endpoint, params)
        try:
            response = requests.get(
                f"{base_url}{endpoint}", params=params, timeout=__REQUEST_TIMEOUT_SECONDS
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"Lazada request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {endpoint}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"Lazada rejected the credentials (HTTP {response.status_code}). "
                "Check app_key, app_secret and access_token."
            )
        if response.status_code == 429 or response.status_code >= 500:
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} on {endpoint}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"Lazada request to {endpoint} failed: HTTP {response.status_code}")

        data = response.json()
        code = data.get("code")
        if code and code != "0":
            if code == __RATE_LIMIT_CODE:
                delay = __BACKOFF_BASE_SECONDS**attempt
                log.warning(f"Lazada call limit reached on {endpoint}, retrying in {delay}s")
                time.sleep(delay)
                continue
            raise RuntimeError(f"Lazada API error {code}: {data.get('message')}")
        return data
    raise RuntimeError(f"Lazada request to {endpoint} still failing after {__MAX_RETRIES} retries")


def order_row(order: dict):
    """
    Build an orders row from an API order object.
    Args:
        order: the order object returned by the API.
    Returns:
        A row dictionary.
    """
    statuses = order.get("statuses")
    return {
        "order_id": str(order.get("order_id", "")),
        "order_number": str(order.get("order_number", "")),
        "payment_method": order.get("payment_method"),
        "price": float(order.get("price", 0) or 0),
        "status": statuses[0] if isinstance(statuses, list) and statuses else order.get("status"),
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "address_billing_name": (order.get("address_billing") or {}).get("name"),
        "address_shipping_name": (order.get("address_shipping") or {}).get("name"),
        "remarks": order.get("remarks"),
    }


def sync_order_items(configuration: dict, order_id: str):
    """
    Fetch and upsert the line items of one order.
    Args:
        configuration: the connector configuration.
        order_id: the id of the order whose items are fetched.
    """
    data = make_request(configuration, "/order/items/get", {"order_id": order_id})
    for item in data.get("data") or []:
        op.upsert(table="order_items", data={
            "order_item_id": str(item.get("order_item_id", "")),
            "order_id": str(order_id),
            "sku": item.get("sku"),
            "name": item.get("name"),
            "shop_sku": item.get("shop_sku"),
            "item_price": float(item.get("item_price", 0) or 0),
            "paid_price": float(item.get("paid_price", 0) or 0),
            "status": item.get("status"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            "variation": item.get("variation"),
            "warehouse_code": item.get("warehouse_code"),
        })


def sync_orders(configuration: dict, created_after: str):
    """
    Page through orders created after the given timestamp and upsert each order with its items.
    Orders are processed one page at a time so the full list is never held in memory.
    Args:
        configuration: the connector configuration.
        created_after: ISO 8601 timestamp; only orders created after it are fetched.
    Returns:
        The latest created_at value seen, or created_after if nothing newer was returned.
    """
    latest = created_after
    offset = 0
    total = 0
    while True:
        data = make_request(configuration, "/orders/get", {
            "created_after": created_after,
            "limit": __PAGE_SIZE,
            "offset": offset,
            "sort_by": "created_at",
            "sort_direction": "ASC",
        })
        orders = data.get("data", {}).get("orders", [])
        if not orders:
            return latest
        for order in orders:
            row = order_row(order)
            op.upsert(table="orders", data=row)
            sync_order_items(configuration, row["order_id"])
            if (row.get("created_at") or "") > latest:
                latest = row["created_at"]
        total += len(orders)
        log.info(f"Synced {total} orders so far")
        if len(orders) < __PAGE_SIZE:
            return latest
        offset += __PAGE_SIZE


def sync_products(configuration: dict, update_after: str):
    """
    Page through products updated after the given timestamp and upsert each product with its SKUs.
    Args:
        configuration: the connector configuration.
        update_after: ISO 8601 timestamp; only products updated after it are fetched.
    Returns:
        The latest updated_time value seen, or update_after if nothing newer was returned.
    """
    latest = update_after
    offset = 0
    total = 0
    while True:
        data = make_request(configuration, "/products/get", {
            "update_after": update_after,
            "limit": __PAGE_SIZE,
            "offset": offset,
            "filter": "all",
        })
        products = data.get("data", {}).get("products", [])
        if not products:
            return latest
        for product in products:
            item_id = str(product.get("item_id", ""))
            attributes = product.get("attributes") or {}
            op.upsert(table="products", data={
                "item_id": item_id,
                "name": attributes.get("name"),
                "primary_category": str(product.get("primary_category", "")),
                "status": product.get("status"),
                "created_time": product.get("created_time"),
                "updated_time": product.get("updated_time"),
                "description": attributes.get("description"),
                "brand": attributes.get("brand"),
            })
            for sku in product.get("skus") or []:
                op.upsert(table="product_skus", data={
                    "shop_sku": sku.get("ShopSku") or sku.get("shop_sku"),
                    "seller_sku": sku.get("SellerSku") or sku.get("seller_sku"),
                    "item_id": item_id,
                    "status": sku.get("Status") or sku.get("status"),
                    "quantity": int(sku.get("quantity", 0) or 0),
                    "special_price": float(sku.get("special_price", 0) or 0),
                    "price": float(sku.get("price", 0) or 0),
                    "url": sku.get("Url") or sku.get("url"),
                })
            if (product.get("updated_time") or "") > latest:
                latest = product["updated_time"]
        total += len(products)
        log.info(f"Synced {total} products so far")
        if len(products) < __PAGE_SIZE:
            return latest
        offset += __PAGE_SIZE


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

    created_after = state.get("created_after", __DEFAULT_START)
    update_after = state.get("update_after", __DEFAULT_START)

    log.info(f"Syncing orders created after {created_after}")
    latest_order_ts = sync_orders(configuration, created_after)

    log.info(f"Syncing products updated after {update_after}")
    latest_product_ts = sync_products(configuration, update_after)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state={"created_after": latest_order_ts, "update_after": latest_product_ts})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
