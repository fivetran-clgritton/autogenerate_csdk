import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://api.aftership.com/tracking/2024-10"
PAGE_LIMIT = 200


def schema(configuration: dict):
    return [
        {
            "table": "trackings",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "legacy_id": "STRING",
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
                "source": "STRING",
                "signed_by": "STRING",
                "shipment_type": "STRING",
                "shipment_weight": "FLOAT",
                "shipment_delivery_date": "STRING",
                "shipment_pickup_date": "STRING",
                "origin_country_iso3": "STRING",
                "origin_city": "STRING",
                "origin_state": "STRING",
                "origin_postal_code": "STRING",
                "origin_raw_location": "STRING",
                "destination_country_iso3": "STRING",
                "destination_city": "STRING",
                "destination_state": "STRING",
                "destination_postal_code": "STRING",
                "destination_raw_location": "STRING",
                "customer_name": "STRING",
                "tracking_postal_code": "STRING",
                "tracking_ship_date": "STRING",
                "tracking_account_number": "STRING",
                "tracking_destination_country": "STRING",
                "on_time_status": "STRING",
                "on_time_difference": "INT",
                "transit_time": "INT",
                "tracked_count": "INT",
                "failed_delivery_attempts": "INT",
                "return_to_sender": "BOOLEAN",
                "first_attempted_at": "UTC_DATETIME",
                "order_id": "STRING",
                "order_number": "STRING",
                "order_date": "STRING",
                "courier_tracking_link": "STRING",
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
                "location": "STRING",
                "country_name": "STRING",
                "country_iso3": "STRING",
                "message": "STRING",
                "tag": "STRING",
                "subtag": "STRING",
                "subtag_message": "STRING",
                "checkpoint_time": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "raw_tag": "STRING",
                "coordinate_lat": "FLOAT",
                "coordinate_lng": "FLOAT",
            },
        },
        {
            "table": "estimated_deliveries",
            "primary_key": ["tracking_id", "source"],
            "columns": {
                "tracking_id": "STRING",
                "source": "STRING",
                "type": "STRING",
                "datetime": "UTC_DATETIME",
                "datetime_min": "UTC_DATETIME",
                "datetime_max": "UTC_DATETIME",
                "estimated_delivery_date": "UTC_DATETIME",
                "estimated_delivery_date_min": "UTC_DATETIME",
                "estimated_delivery_date_max": "UTC_DATETIME",
            },
        },
        {
            "table": "next_couriers",
            "primary_key": ["tracking_id", "slug"],
            "columns": {
                "tracking_id": "STRING",
                "slug": "STRING",
                "tracking_number": "STRING",
                "source": "STRING",
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


def _get_trackings(api_key: str, updated_at_min: str, cursor: str = "") -> dict:
    params = {"limit": PAGE_LIMIT, "updated_at_min": updated_at_min}
    if cursor:
        params["cursor"] = cursor
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
        f"{BASE_URL}/couriers",
        headers=_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("couriers", [])


def _flatten_tracking(t: dict) -> dict:
    return {
        "id": t.get("id"),
        "legacy_id": t.get("legacy_id"),
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
        "source": t.get("source"),
        "signed_by": t.get("signed_by"),
        "shipment_type": t.get("shipment_type"),
        "shipment_weight": t.get("shipment_weight"),
        "shipment_delivery_date": t.get("shipment_delivery_date"),
        "shipment_pickup_date": t.get("shipment_pickup_date"),
        "origin_country_iso3": t.get("origin_country_iso3"),
        "origin_city": t.get("origin_city"),
        "origin_state": t.get("origin_state"),
        "origin_postal_code": t.get("origin_postal_code"),
        "origin_raw_location": t.get("origin_raw_location"),
        "destination_country_iso3": t.get("destination_country_iso3"),
        "destination_city": t.get("destination_city"),
        "destination_state": t.get("destination_state"),
        "destination_postal_code": t.get("destination_postal_code"),
        "destination_raw_location": t.get("destination_raw_location"),
        "customer_name": t.get("customer_name"),
        "tracking_postal_code": t.get("tracking_postal_code"),
        "tracking_ship_date": t.get("tracking_ship_date"),
        "tracking_account_number": t.get("tracking_account_number"),
        "tracking_destination_country": t.get("tracking_destination_country"),
        "on_time_status": t.get("on_time_status"),
        "on_time_difference": t.get("on_time_difference"),
        "transit_time": t.get("transit_time"),
        "tracked_count": t.get("tracked_count"),
        "failed_delivery_attempts": t.get("failed_delivery_attempts"),
        "return_to_sender": t.get("return_to_sender"),
        "first_attempted_at": t.get("first_attempted_at"),
        "order_id": t.get("order_id"),
        "order_number": t.get("order_number"),
        "order_date": t.get("order_date"),
        "courier_tracking_link": t.get("courier_tracking_link"),
    }


def _flatten_checkpoint(tracking_id: str, cp: dict) -> dict:
    coord = cp.get("coordinate") or {}
    return {
        "tracking_id": tracking_id,
        "slug": cp.get("slug"),
        "city": cp.get("city"),
        "state": cp.get("state"),
        "zip": cp.get("zip"),
        "location": cp.get("location"),
        "country_name": cp.get("country_name"),
        "country_iso3": cp.get("country_iso3"),
        "message": cp.get("message"),
        "tag": cp.get("tag"),
        "subtag": cp.get("subtag"),
        "subtag_message": cp.get("subtag_message"),
        "checkpoint_time": cp.get("checkpoint_time"),
        "created_at": cp.get("created_at"),
        "raw_tag": cp.get("raw_tag"),
        "coordinate_lat": coord.get("latitude") if isinstance(coord, dict) else None,
        "coordinate_lng": coord.get("longitude") if isinstance(coord, dict) else None,
    }


def _estimated_delivery_rows(tracking_id: str, t: dict) -> list:
    rows = []
    fed = t.get("first_estimated_delivery") or {}
    if fed:
        rows.append({
            "tracking_id": tracking_id,
            "source": fed.get("source", "first"),
            "type": fed.get("type"),
            "datetime": fed.get("datetime"),
            "datetime_min": fed.get("datetime_min"),
            "datetime_max": fed.get("datetime_max"),
            "estimated_delivery_date": None,
            "estimated_delivery_date_min": None,
            "estimated_delivery_date_max": None,
        })
    ced = t.get("courier_estimated_delivery_date") or {}
    if ced:
        rows.append({
            "tracking_id": tracking_id,
            "source": "courier",
            "type": None,
            "datetime": None,
            "datetime_min": None,
            "datetime_max": None,
            "estimated_delivery_date": ced.get("estimated_delivery_date"),
            "estimated_delivery_date_min": ced.get("estimated_delivery_date_min"),
            "estimated_delivery_date_max": ced.get("estimated_delivery_date_max"),
        })
    return rows


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]
    # Default to epoch start on first run; AfterShip accepts ISO 8601
    updated_at_min = state.get("updated_at_min", "2010-01-01T00:00:00+00:00")
    latest_updated_at = updated_at_min

    # --- Trackings (incremental) ---
    cursor = ""
    total_synced = 0
    while True:
        log.info(f"Fetching trackings updated since {updated_at_min} cursor={cursor!r}")
        data = _get_trackings(api_key, updated_at_min, cursor)
        pagination = data.get("data", {}).get("pagination", {})
        trackings = data.get("data", {}).get("trackings", [])

        for t in trackings:
            tid = t["id"]
            yield op.upsert("trackings", _flatten_tracking(t))

            for cp in t.get("checkpoints", []):
                yield op.upsert("checkpoints", _flatten_checkpoint(tid, cp))

            for row in _estimated_delivery_rows(tid, t):
                yield op.upsert("estimated_deliveries", row)

            for nc in t.get("next_couriers") or []:
                yield op.upsert("next_couriers", {
                    "tracking_id": tid,
                    "slug": nc.get("slug"),
                    "tracking_number": nc.get("tracking_number"),
                    "source": nc.get("source"),
                })

            t_updated = t.get("updated_at", "")
            if t_updated and t_updated > latest_updated_at:
                latest_updated_at = t_updated

        total_synced += len(trackings)
        log.info(f"Synced {total_synced} trackings so far")

        if not pagination.get("has_next_page"):
            break
        cursor = pagination.get("next_cursor", "")

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
