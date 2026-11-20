# Omnia Retail connector — dynamic pricing API
# API docs: api-docs.omniaretail.dev
# Auth: Bearer token (api_key from configuration)
# Base URL: https://api.omniaretail.com
#
# ASSUMPTIONS (verify against live API):
#   - GET /products           — product catalog with current prices
#   - GET /strategies         — pricing strategies/rules
#   - GET /price-history      — historical prices; supports ?from=ISO8601&to=ISO8601
#   - Pagination via ?page=N&pageSize=N; response includes totalPages or similar
#   - Bearer token auth: Authorization: Bearer <api_key>
#
# Update configuration.json with a real api_key before running.

import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

BASE_URL = "https://api.omniaretail.com"
PAGE_SIZE = 500


def schema(configuration: dict):
    return [
        {
            "table": "products",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "ean": "STRING",
                "sku": "STRING",
                "name": "STRING",
                "brand": "STRING",
                "category": "STRING",
                "current_price": "FLOAT",
                "cost_price": "FLOAT",
                "currency": "STRING",
                "strategy_id": "STRING",
                "strategy_name": "STRING",
                "calculated_price": "FLOAT",
                "min_price": "FLOAT",
                "max_price": "FLOAT",
                "stock": "INT",
                "active": "BOOLEAN",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "pricing_strategies",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "type": "STRING",
                "description": "STRING",
                "currency": "STRING",
                "is_active": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "rule_count": "INT",
                "product_count": "INT",
            },
        },
        {
            "table": "price_history",
            "primary_key": ["product_id", "timestamp"],
            "columns": {
                "product_id": "STRING",
                "ean": "STRING",
                "timestamp": "UTC_DATETIME",
                "price": "FLOAT",
                "cost_price": "FLOAT",
                "currency": "STRING",
                "strategy_id": "STRING",
                "strategy_name": "STRING",
                "source": "STRING",
                "competitor_name": "STRING",
                "competitor_price": "FLOAT",
            },
        },
    ]


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _get_all_pages(api_key: str, path: str, params: dict = None) -> list:
    """Fetch all pages for a paginated endpoint."""
    params = params or {}
    params["pageSize"] = PAGE_SIZE
    page = 1
    results = []
    while True:
        params["page"] = page
        resp = requests.get(
            f"{BASE_URL}{path}",
            headers=_headers(api_key),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        # Support both list response and {data: [...], totalPages: N} envelope
        if isinstance(body, list):
            items = body
        else:
            items = body.get("data") or body.get("items") or body.get("products") or body.get("strategies") or []
        results.extend(items)
        total_pages = body.get("totalPages") or body.get("total_pages") if isinstance(body, dict) else None
        if not items or (total_pages is not None and page >= total_pages):
            break
        page += 1
    return results


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]

    # --- Pricing strategies (full refresh) ---
    log.info("Fetching pricing strategies")
    strategies = _get_all_pages(api_key, "/strategies")
    for s in strategies:
        yield op.upsert("pricing_strategies", {
            "id": str(s.get("id", "")),
            "name": s.get("name"),
            "type": s.get("type"),
            "description": s.get("description"),
            "currency": s.get("currency"),
            "is_active": s.get("isActive") if s.get("isActive") is not None else s.get("is_active"),
            "created_at": s.get("createdAt") or s.get("created_at"),
            "updated_at": s.get("updatedAt") or s.get("updated_at"),
            "rule_count": s.get("ruleCount") or s.get("rule_count"),
            "product_count": s.get("productCount") or s.get("product_count"),
        })
    log.info(f"Synced {len(strategies)} strategies")

    # --- Products (full refresh) ---
    log.info("Fetching products")
    products = _get_all_pages(api_key, "/products")
    for p in products:
        yield op.upsert("products", {
            "id": str(p.get("id", "")),
            "ean": p.get("ean"),
            "sku": p.get("sku"),
            "name": p.get("name"),
            "brand": p.get("brand"),
            "category": p.get("category"),
            "current_price": p.get("currentPrice") or p.get("current_price"),
            "cost_price": p.get("costPrice") or p.get("cost_price"),
            "currency": p.get("currency"),
            "strategy_id": str(p.get("strategyId") or p.get("strategy_id") or ""),
            "strategy_name": p.get("strategyName") or p.get("strategy_name"),
            "calculated_price": p.get("calculatedPrice") or p.get("calculated_price"),
            "min_price": p.get("minPrice") or p.get("min_price"),
            "max_price": p.get("maxPrice") or p.get("max_price"),
            "stock": p.get("stock"),
            "active": p.get("active"),
            "updated_at": p.get("updatedAt") or p.get("updated_at"),
        })
    log.info(f"Synced {len(products)} products")

    # --- Price history (incremental by date) ---
    history_from = state.get("price_history_from", "2020-01-01T00:00:00Z")
    log.info(f"Fetching price history from {history_from}")
    history = _get_all_pages(api_key, "/price-history", params={"from": history_from})
    latest_ts = history_from
    for h in history:
        ts = h.get("timestamp") or h.get("date") or h.get("createdAt") or ""
        if ts and ts > latest_ts:
            latest_ts = ts
        yield op.upsert("price_history", {
            "product_id": str(h.get("productId") or h.get("product_id") or ""),
            "ean": h.get("ean"),
            "timestamp": ts,
            "price": h.get("price"),
            "cost_price": h.get("costPrice") or h.get("cost_price"),
            "currency": h.get("currency"),
            "strategy_id": str(h.get("strategyId") or h.get("strategy_id") or ""),
            "strategy_name": h.get("strategyName") or h.get("strategy_name"),
            "source": h.get("source"),
            "competitor_name": h.get("competitorName") or h.get("competitor_name"),
            "competitor_price": h.get("competitorPrice") or h.get("competitor_price"),
        })
    log.info(f"Synced {len(history)} price history records")

    yield op.checkpoint(state={"price_history_from": latest_ts})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
