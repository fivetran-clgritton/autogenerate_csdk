import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://api.aftership.com/v4"
PAGE_LIMIT = 200  # max per AfterShip v4 docs


def schema(configuration: dict):
    return [
        {
            "table": "trackings",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "last_updated_at": "UTC_DATETIME",
                "tracking_number": "STRING",
                "slug": "STRING",
                "active": "BOOLEAN",
                "title": "STRING",
                "note": "STRING",
                "tag": "STRING",
                "subtag": "STRING",
                "subtag_message": "STRING",
                "expected_delivery": "STRING",
                "signed_by": "STRING",
                "source": "STRING",
                "shipment_type": "STRING",
                "shipment_weight": "FLOAT",
                "shipment_weight_unit": "STRING",
                "origin_country_iso3": "STRING",
                "destination_country_iso3": "STRING",
                "destination_raw_location": "STRING",
                "customer_name": "STRING",
                "customer_email": "STRING",
                "customer_phone": "STRING",
                "tracking_postal_code": "STRING",
                "tracking_ship_date": "STRING",
                "tracking_account_number": "STRING",
                "tracking_destination_country": "STRING",
            },
        },
        {
            "table": "checkpoints",
            "primary_key": ["tracking_id", "checkpoint_time", "slug"],
            "columns": {
                "tracking_id": "STRING",
                "slug": "STRING",
                "city": "STRING",
                "state": "STRING",
                "zip": "STRING",
                "country_name": "STRING",
                "country_iso3": "STRING",
                "message": "STRING",
                "tag": "STRING",
                "subtag": "STRING",
                "subtag_message": "STRING",
                "checkpoint_time": "UTC_DATETIME",
                "raw_tag": "STRING",
                "coordinates_lat": "FLOAT",
                "coordinates_lng": "FLOAT",
            },
        },
        {
            "table": "couriers",
            "primary_key": ["slug"],
            "columns": {
                "slug": "STRING",
                "name": "STRING",
                "phone": "STRING",
                "other_name": "STRING",
                "web_url": "STRING",
                "required_fields": "STRING",
                "optional_fields": "STRING",
                "default_language": "STRING",
                "support_languages": "STRING",
            },
        },
    ]


def _headers(api_key: str) -> dict:
    return {
        "as-api-key": api_key,
        "Content-Type": "application/json",
    }


def _get_trackings(api_key: str, updated_at_min: str, page: int) -> dict:
    params = {
        "limit": PAGE_LIMIT,
        "page": page,
        "updated_at_min": updated_at_min,
    }
    resp = requests.get(
        f"{BASE_URL}/trackings",
        headers=_headers(api_key),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_couriers(api_key: str) -> list:
    resp = requests.get(
        f"{BASE_URL}/couriers/all",
        headers=_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("couriers", [])


def _flatten_tracking(t: dict) -> dict:
    coords = None
    return {
        "id": t.get("id"),
        "created_at": t.get("created_at"),
        "updated_at": t.get("updated_at"),
        "last_updated_at": t.get("last_updated_at"),
        "tracking_number": t.get("tracking_number"),
        "slug": t.get("slug"),
        "active": t.get("active"),
        "title": t.get("title"),
        "note": t.get("note"),
        "tag": t.get("tag"),
        "subtag": t.get("subtag"),
        "subtag_message": t.get("subtag_message"),
        "expected_delivery": t.get("expected_delivery"),
        "signed_by": t.get("signed_by"),
        "source": t.get("source"),
        "shipment_type": t.get("shipment_type"),
        "shipment_weight": t.get("shipment_weight"),
        "shipment_weight_unit": t.get("shipment_weight_unit"),
        "origin_country_iso3": t.get("origin_country_iso3"),
        "destination_country_iso3": t.get("destination_country_iso3"),
        "destination_raw_location": t.get("destination_raw_location"),
        "customer_name": t.get("customer_name"),
        "customer_email": t.get("customer_email"),
        "customer_phone": t.get("customer_phone"),
        "tracking_postal_code": t.get("tracking_postal_code"),
        "tracking_ship_date": t.get("tracking_ship_date"),
        "tracking_account_number": t.get("tracking_account_number"),
        "tracking_destination_country": t.get("tracking_destination_country"),
    }


def _flatten_checkpoint(tracking_id: str, cp: dict) -> dict:
    coords = cp.get("coordinates") or {}
    return {
        "tracking_id": tracking_id,
        "slug": cp.get("slug"),
        "city": cp.get("city"),
        "state": cp.get("state"),
        "zip": cp.get("zip"),
        "country_name": cp.get("country_name"),
        "country_iso3": cp.get("country_iso3"),
        "message": cp.get("message"),
        "tag": cp.get("tag"),
        "subtag": cp.get("subtag"),
        "subtag_message": cp.get("subtag_message"),
        "checkpoint_time": cp.get("checkpoint_time"),
        "raw_tag": cp.get("raw_tag"),
        "coordinates_lat": coords.get("lat") if isinstance(coords, dict) else None,
        "coordinates_lng": coords.get("lng") if isinstance(coords, dict) else None,
    }


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]
    # Default to epoch start on first run; AfterShip accepts ISO 8601
    updated_at_min = state.get("updated_at_min", "2010-01-01T00:00:00+00:00")
    latest_updated_at = updated_at_min

    # --- Trackings (incremental) ---
    page = 1
    total_synced = 0
    while True:
        log.info(f"Fetching trackings page {page} updated since {updated_at_min}")
        data = _get_trackings(api_key, updated_at_min, page)
        trackings = data.get("data", {}).get("trackings", [])

        if not trackings:
            break

        for t in trackings:
            yield op.upsert("trackings", _flatten_tracking(t))

            for cp in t.get("checkpoints", []):
                yield op.upsert("checkpoints", _flatten_checkpoint(t["id"], cp))

            # Track the most recent updated_at seen
            t_updated = t.get("updated_at", "")
            if t_updated and t_updated > latest_updated_at:
                latest_updated_at = t_updated

        total_synced += len(trackings)
        log.info(f"Synced {total_synced} trackings so far")

        if len(trackings) < PAGE_LIMIT:
            break
        page += 1

    # --- Couriers (full refresh) ---
    log.info("Fetching all couriers")
    couriers = _get_couriers(api_key)
    for c in couriers:
        yield op.upsert("couriers", {
            "slug": c.get("slug"),
            "name": c.get("name"),
            "phone": c.get("phone"),
            "other_name": c.get("other_name"),
            "web_url": c.get("web_url"),
            "required_fields": ",".join(c.get("required_fields", [])),
            "optional_fields": ",".join(c.get("optional_fields", [])),
            "default_language": c.get("default_language"),
            "support_languages": ",".join(c.get("support_languages", [])),
        })

    yield op.checkpoint(state={"updated_at_min": latest_updated_at})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
