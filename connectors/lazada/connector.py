import hashlib
import hmac
import time
import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

REGION_URLS = {
    "my": "https://api.lazada.com.my/rest",
    "th": "https://api.lazada.co.th/rest",
    "sg": "https://api.lazada.sg/rest",
    "ph": "https://api.lazada.com.ph/rest",
    "id": "https://api.lazada.co.id/rest",
    "vn": "https://api.lazada.vn/rest",
}

PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "orders",
            "primary_key": ["order_id"],
            "columns": {
                "order_id": "STRING",
                "order_number": "STRING",
                "payment_method": "STRING",
                "price": "FLOAT",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "address_billing_name": "STRING",
                "address_shipping_name": "STRING",
                "remarks": "STRING",
            },
        },
        {
            "table": "order_items",
            "primary_key": ["order_item_id"],
            "columns": {
                "order_item_id": "STRING",
                "order_id": "STRING",
                "sku": "STRING",
                "name": "STRING",
                "shop_sku": "STRING",
                "item_price": "FLOAT",
                "paid_price": "FLOAT",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "variation": "STRING",
                "warehouse_code": "STRING",
            },
        },
        {
            "table": "products",
            "primary_key": ["item_id"],
            "columns": {
                "item_id": "STRING",
                "name": "STRING",
                "primary_category": "STRING",
                "status": "STRING",
                "created_time": "UTC_DATETIME",
                "updated_time": "UTC_DATETIME",
                "description": "STRING",
                "brand": "STRING",
            },
        },
        {
            "table": "product_skus",
            "primary_key": ["shop_sku"],
            "columns": {
                "shop_sku": "STRING",
                "seller_sku": "STRING",
                "item_id": "STRING",
                "status": "STRING",
                "quantity": "INT",
                "special_price": "FLOAT",
                "price": "FLOAT",
                "url": "STRING",
            },
        },
    ]


def _sign(app_secret: str, endpoint: str, params: dict) -> str:
    sorted_params = sorted(params.items())
    sign_str = endpoint + "".join(f"{k}{v}" for k, v in sorted_params)
    return hmac.new(
        app_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().upper()


def _base_params(app_key: str, access_token: str) -> dict:
    return {
        "app_key": app_key,
        "access_token": access_token,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
    }


def _get(base_url: str, endpoint: str, app_key: str, app_secret: str, access_token: str, extra: dict) -> dict:
    params = {**_base_params(app_key, access_token), **extra}
    params["sign"] = _sign(app_secret, endpoint, params)
    resp = requests.get(f"{base_url}{endpoint}", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") and data["code"] != "0":
        raise RuntimeError(f"Lazada API error {data.get('code')}: {data.get('message')}")
    return data


def _sync_orders(base_url, app_key, app_secret, access_token, created_after):
    offset = 0
    while True:
        data = _get(base_url, "/orders/get", app_key, app_secret, access_token, {
            "created_after": created_after,
            "limit": PAGE_SIZE,
            "offset": offset,
            "sort_by": "created_at",
            "sort_direction": "ASC",
        })
        orders = data.get("data", {}).get("orders", [])
        if not orders:
            break
        for o in orders:
            yield "orders", {
                "order_id": str(o.get("order_id", "")),
                "order_number": str(o.get("order_number", "")),
                "payment_method": o.get("payment_method"),
                "price": float(o.get("price", 0) or 0),
                "status": o.get("statuses", [None])[0] if isinstance(o.get("statuses"), list) else o.get("status"),
                "created_at": o.get("created_at"),
                "updated_at": o.get("updated_at"),
                "address_billing_name": (o.get("address_billing") or {}).get("name"),
                "address_shipping_name": (o.get("address_shipping") or {}).get("name"),
                "remarks": o.get("remarks"),
            }
        if len(orders) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


def _sync_order_items(base_url, app_key, app_secret, access_token, order_id):
    data = _get(base_url, "/order/items/get", app_key, app_secret, access_token, {
        "order_id": order_id,
    })
    items = data.get("data", [])
    for item in items:
        yield {
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
        }


def _sync_products(base_url, app_key, app_secret, access_token, update_after):
    offset = 0
    while True:
        data = _get(base_url, "/products/get", app_key, app_secret, access_token, {
            "update_after": update_after,
            "limit": PAGE_SIZE,
            "offset": offset,
            "filter": "all",
        })
        products = data.get("data", {}).get("products", [])
        if not products:
            break
        for p in products:
            item_id = str(p.get("item_id", ""))
            yield "products", {
                "item_id": item_id,
                "name": p.get("attributes", {}).get("name"),
                "primary_category": str(p.get("primary_category", "")),
                "status": p.get("status"),
                "created_time": p.get("created_time"),
                "updated_time": p.get("updated_time"),
                "description": p.get("attributes", {}).get("description"),
                "brand": p.get("attributes", {}).get("brand"),
            }
            for sku in p.get("skus", []):
                yield "product_skus", {
                    "shop_sku": sku.get("ShopSku") or sku.get("shop_sku"),
                    "seller_sku": sku.get("SellerSku") or sku.get("seller_sku"),
                    "item_id": item_id,
                    "status": sku.get("Status") or sku.get("status"),
                    "quantity": int(sku.get("quantity", 0) or 0),
                    "special_price": float(sku.get("special_price", 0) or 0),
                    "price": float(sku.get("price", 0) or 0),
                    "url": sku.get("Url") or sku.get("url"),
                }
        if len(products) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


def update(configuration: dict, state: dict):
    app_key = configuration["app_key"]
    app_secret = configuration["app_secret"]
    access_token = configuration["access_token"]
    region = configuration.get("region", "my").lower()
    base_url = REGION_URLS.get(region, REGION_URLS["my"])

    created_after = state.get("created_after", "2020-01-01T00:00:00+08:00")
    update_after = state.get("update_after", "2020-01-01T00:00:00+08:00")
    latest_order_ts = created_after
    latest_product_ts = update_after

    # Sync orders + order items
    log.info(f"Syncing orders from {created_after} via {region} API")
    order_ids = []
    for table, row in _sync_orders(base_url, app_key, app_secret, access_token, created_after):
        yield op.upsert(table, row)
        order_ids.append(row["order_id"])
        if row.get("created_at", "") > latest_order_ts:
            latest_order_ts = row["created_at"]

    log.info(f"Fetching items for {len(order_ids)} orders")
    for order_id in order_ids:
        for item in _sync_order_items(base_url, app_key, app_secret, access_token, order_id):
            yield op.upsert("order_items", item)

    # Sync products + SKUs
    log.info(f"Syncing products updated after {update_after}")
    for table, row in _sync_products(base_url, app_key, app_secret, access_token, update_after):
        yield op.upsert(table, row)
        if table == "products" and row.get("updated_time", "") > latest_product_ts:
            latest_product_ts = row["updated_time"]

    yield op.checkpoint(state={
        "created_after": latest_order_ts,
        "update_after": latest_product_ts,
    })


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
