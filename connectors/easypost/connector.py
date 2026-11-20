import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://api.easypost.com/v2"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "shipments",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "tracking_code": "STRING",
                "status": "STRING",
                "carrier": "STRING",
                "service": "STRING",
                "from_address_id": "STRING",
                "to_address_id": "STRING",
                "parcel_id": "STRING",
                "rate_id": "STRING",
                "batch_id": "STRING",
                "reference": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "trackers",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "tracking_code": "STRING",
                "carrier": "STRING",
                "status": "STRING",
                "weight": "FLOAT",
                "est_delivery_date": "STRING",
                "shipment_id": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "tracking_details",
            "primary_key": ["tracker_id", "datetime", "source"],
            "columns": {
                "tracker_id": "STRING",
                "message": "STRING",
                "status": "STRING",
                "datetime": "UTC_DATETIME",
                "source": "STRING",
                "carrier_code": "STRING",
                "tracking_location_city": "STRING",
                "tracking_location_state": "STRING",
                "tracking_location_country": "STRING",
                "tracking_location_zip": "STRING",
            },
        },
        {
            "table": "addresses",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "company": "STRING",
                "street1": "STRING",
                "street2": "STRING",
                "city": "STRING",
                "state": "STRING",
                "zip": "STRING",
                "country": "STRING",
                "phone": "STRING",
                "email": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "refunds",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "tracking_code": "STRING",
                "confirmation_number": "STRING",
                "carrier": "STRING",
                "shipment_id": "STRING",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
    ]


def _auth(api_key: str):
    return (api_key, "")


def _get_page(api_key: str, endpoint: str, params: dict) -> dict:
    resp = requests.get(
        f"{BASE_URL}/{endpoint}",
        auth=_auth(api_key),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _sync_shipments(api_key: str, state: dict):
    start_datetime = state.get("shipments_start_datetime", "2010-01-01T00:00:00Z")
    before_id = None
    total = 0

    while True:
        params = {
            "page_size": PAGE_SIZE,
            "start_datetime": start_datetime,
        }
        if before_id:
            params["before_id"] = before_id

        data = _get_page(api_key, "shipments", params)
        shipments = data.get("shipments", [])
        if not shipments:
            break

        for s in shipments:
            selected_rate = s.get("selected_rate") or {}
            yield op.upsert("shipments", {
                "id": s.get("id"),
                "tracking_code": s.get("tracking_code"),
                "status": s.get("status"),
                "carrier": selected_rate.get("carrier"),
                "service": selected_rate.get("service"),
                "from_address_id": (s.get("from_address") or {}).get("id"),
                "to_address_id": (s.get("to_address") or {}).get("id"),
                "parcel_id": (s.get("parcel") or {}).get("id"),
                "rate_id": selected_rate.get("id"),
                "batch_id": s.get("batch_id"),
                "reference": s.get("reference"),
                "created_at": s.get("created_at"),
                "updated_at": s.get("updated_at"),
            })

        total += len(shipments)
        log.info(f"Synced {total} shipments")

        if len(shipments) < PAGE_SIZE:
            break
        before_id = shipments[-1]["id"]


def _sync_trackers(api_key: str, state: dict):
    created_after = state.get("trackers_created_after", "2010-01-01T00:00:00Z")
    before_id = None
    total = 0
    latest = created_after

    while True:
        params = {
            "page_size": PAGE_SIZE,
            "created_after": created_after,
        }
        if before_id:
            params["before_id"] = before_id

        data = _get_page(api_key, "trackers", params)
        trackers = data.get("trackers", [])
        if not trackers:
            break

        for t in trackers:
            yield op.upsert("trackers", {
                "id": t.get("id"),
                "tracking_code": t.get("tracking_code"),
                "carrier": t.get("carrier"),
                "status": t.get("status"),
                "weight": t.get("weight"),
                "est_delivery_date": t.get("est_delivery_date"),
                "shipment_id": t.get("shipment_id"),
                "created_at": t.get("created_at"),
                "updated_at": t.get("updated_at"),
            })

            for td in t.get("tracking_details", []):
                loc = td.get("tracking_location") or {}
                yield op.upsert("tracking_details", {
                    "tracker_id": t.get("id"),
                    "message": td.get("message"),
                    "status": td.get("status"),
                    "datetime": td.get("datetime"),
                    "source": td.get("source"),
                    "carrier_code": td.get("carrier_code"),
                    "tracking_location_city": loc.get("city"),
                    "tracking_location_state": loc.get("state"),
                    "tracking_location_country": loc.get("country"),
                    "tracking_location_zip": loc.get("zip"),
                })

            if t.get("created_at", "") > latest:
                latest = t["created_at"]

        total += len(trackers)
        log.info(f"Synced {total} trackers")

        if len(trackers) < PAGE_SIZE:
            break
        before_id = trackers[-1]["id"]

    return latest


def _sync_refunds(api_key: str, state: dict):
    before_id = None
    total = 0

    while True:
        params = {"page_size": PAGE_SIZE}
        if before_id:
            params["before_id"] = before_id

        data = _get_page(api_key, "refunds", params)
        refunds = data.get("refunds", [])
        if not refunds:
            break

        for r in refunds:
            yield op.upsert("refunds", {
                "id": r.get("id"),
                "tracking_code": r.get("tracking_code"),
                "confirmation_number": r.get("confirmation_number"),
                "carrier": r.get("carrier"),
                "shipment_id": r.get("shipment_id"),
                "status": r.get("status"),
                "created_at": r.get("created_at"),
            })

        total += len(refunds)
        log.info(f"Synced {total} refunds")

        if len(refunds) < PAGE_SIZE:
            break
        before_id = refunds[-1]["id"]


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]

    yield from _sync_shipments(api_key, state)
    latest_tracker_ts = yield from _sync_trackers(api_key, state)
    yield from _sync_refunds(api_key, state)

    new_state = dict(state)
    if latest_tracker_ts:
        new_state["trackers_created_after"] = latest_tracker_ts
    yield op.checkpoint(state=new_state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
