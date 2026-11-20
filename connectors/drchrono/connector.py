import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://drchrono.com/api"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "patients",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "first_name": "STRING",
                "last_name": "STRING",
                "date_of_birth": "NAIVE_DATE",
                "gender": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "address": "STRING",
                "doctor": "INT",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "appointments",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "patient": "INT",
                "doctor": "INT",
                "office": "INT",
                "exam_room": "INT",
                "scheduled_time": "UTC_DATETIME",
                "duration": "INT",
                "status": "STRING",
                "reason": "STRING",
                "notes": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "doctors",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "specialty": "STRING",
                "npi": "STRING",
                "group_practice_id": "INT",
            },
        },
        {
            "table": "offices",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "address": "STRING",
                "phone": "STRING",
                "fax": "STRING",
                "doctor": "INT",
            },
        },
        {
            "table": "clinical_notes",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "patient": "INT",
                "doctor": "INT",
                "appointment": "INT",
                "text": "STRING",
                "locked": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "medications",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "patient": "INT",
                "doctor": "INT",
                "appointment": "INT",
                "name": "STRING",
                "dosage_quantity": "STRING",
                "dosage_units": "STRING",
                "route": "STRING",
                "frequency": "STRING",
                "notes": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
    ]


def _headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def _paginate(access_token: str, endpoint: str, params: dict):
    """Yield all results from a paginated DrChrono endpoint."""
    url = f"{BASE_URL}/{endpoint}"
    params = {**params, "page_size": PAGE_SIZE}
    while url:
        resp = requests.get(url, headers=_headers(access_token), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("results", []):
            yield item
        # 'next' is a full URL; clear params after first request to avoid duplication
        url = data.get("next")
        params = {}


def update(configuration: dict, state: dict):
    token = configuration["access_token"]

    # --- Patients (incremental) ---
    since = state.get("patients_updated_since", "2010-01-01T00:00:00")
    latest = since
    log.info(f"Syncing patients updated since {since}")
    for p in _paginate(token, "patients", {"date_updated_since": since}):
        yield op.upsert("patients", {
            "id": p.get("id"),
            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "date_of_birth": p.get("date_of_birth"),
            "gender": p.get("gender"),
            "email": p.get("email"),
            "phone": p.get("cell_phone") or p.get("home_phone") or p.get("office_phone"),
            "address": p.get("address"),
            "doctor": p.get("doctor"),
            "created_at": p.get("created_at") or p.get("date_of_first_appointment"),
            "updated_at": p.get("updated_at") or p.get("date_of_last_appointment"),
        })
        updated = p.get("updated_at", "")
        if updated and updated > latest:
            latest = updated

    # --- Appointments (incremental by date window) ---
    appt_since = state.get("appointments_since", "2010-01-01")
    log.info(f"Syncing appointments since {appt_since}")
    for a in _paginate(token, "appointments", {"date_range": f"{appt_since}/3000-01-01"}):
        yield op.upsert("appointments", {
            "id": a.get("id"),
            "patient": a.get("patient"),
            "doctor": a.get("doctor"),
            "office": a.get("office"),
            "exam_room": a.get("exam_room"),
            "scheduled_time": a.get("scheduled_time"),
            "duration": a.get("duration"),
            "status": a.get("status"),
            "reason": a.get("reason"),
            "notes": a.get("notes"),
            "created_at": a.get("created_at"),
            "updated_at": a.get("updated_at"),
        })

    # --- Clinical Notes (incremental) ---
    notes_since = state.get("notes_since", "2010-01-01T00:00:00")
    log.info(f"Syncing clinical notes since {notes_since}")
    for n in _paginate(token, "clinical_notes", {"date_updated_since": notes_since}):
        yield op.upsert("clinical_notes", {
            "id": n.get("id"),
            "patient": n.get("patient"),
            "doctor": n.get("doctor"),
            "appointment": n.get("appointment"),
            "text": n.get("text"),
            "locked": n.get("locked"),
            "created_at": n.get("created_at"),
            "updated_at": n.get("updated_at"),
        })

    # --- Medications (incremental) ---
    meds_since = state.get("medications_since", "2010-01-01T00:00:00")
    log.info(f"Syncing medications since {meds_since}")
    for m in _paginate(token, "medications", {"date_updated_since": meds_since}):
        yield op.upsert("medications", {
            "id": m.get("id"),
            "patient": m.get("patient"),
            "doctor": m.get("doctor"),
            "appointment": m.get("appointment"),
            "name": m.get("name"),
            "dosage_quantity": m.get("dosage_quantity"),
            "dosage_units": m.get("dosage_units"),
            "route": m.get("route"),
            "frequency": m.get("frequency"),
            "notes": m.get("notes"),
            "created_at": m.get("created_at"),
            "updated_at": m.get("updated_at"),
        })

    # --- Doctors (full refresh) ---
    log.info("Syncing doctors")
    for d in _paginate(token, "doctors", {}):
        yield op.upsert("doctors", {
            "id": d.get("id"),
            "first_name": d.get("first_name"),
            "last_name": d.get("last_name"),
            "email": d.get("email"),
            "specialty": d.get("specialty"),
            "npi": d.get("npi"),
            "group_practice_id": d.get("group_practice"),
        })

    # --- Offices (full refresh) ---
    log.info("Syncing offices")
    for o in _paginate(token, "offices", {}):
        yield op.upsert("offices", {
            "id": o.get("id"),
            "name": o.get("name"),
            "address": o.get("address"),
            "phone": o.get("phone"),
            "fax": o.get("fax"),
            "doctor": o.get("doctor"),
        })

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    yield op.checkpoint(state={
        "patients_updated_since": latest,
        "appointments_since": now[:10],
        "notes_since": now,
        "medications_since": now,
    })


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
