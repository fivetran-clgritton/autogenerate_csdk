import requests
from datetime import datetime, timedelta, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://app.tripactions.com/app/developer-api/v1"
TOKEN_URL = "https://app.tripactions.com/app/developer-api/oauth/token"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "trips",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "booking_ref": "STRING",
                "traveler_id": "STRING",
                "traveler_email": "STRING",
                "trip_name": "STRING",
                "start_date": "NAIVE_DATE",
                "end_date": "NAIVE_DATE",
                "purpose": "STRING",
                "status": "STRING",
                "total_cost": "FLOAT",
                "currency": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
            },
        },
        {
            "table": "bookings",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "trip_id": "STRING",
                "booking_type": "STRING",
                "vendor": "STRING",
                "booking_ref": "STRING",
                "check_in": "UTC_DATETIME",
                "check_out": "UTC_DATETIME",
                "traveler_id": "STRING",
                "cost": "FLOAT",
                "currency": "STRING",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "expenses",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "traveler_id": "STRING",
                "report_name": "STRING",
                "status": "STRING",
                "submitted_date": "NAIVE_DATE",
                "approved_date": "NAIVE_DATE",
                "total_amount": "FLOAT",
                "currency": "STRING",
                "trip_id": "STRING",
            },
        },
        {
            "table": "expense_items",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "expense_id": "STRING",
                "category": "STRING",
                "merchant": "STRING",
                "amount": "FLOAT",
                "currency": "STRING",
                "expense_date": "NAIVE_DATE",
                "receipt_url": "STRING",
                "description": "STRING",
            },
        },
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "email": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "department": "STRING",
                "manager_id": "STRING",
                "role": "STRING",
                "active": "BOOLEAN",
            },
        },
    ]


def _get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _paginate(token: str, path: str, params: dict):
    params = {**params, "limit": PAGE_SIZE, "offset": 0}
    while True:
        resp = requests.get(
            f"{BASE_URL}{path}",
            headers=_headers(token),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data") or data.get("items") or data.get("results") or []
        if not items:
            break
        yield from items
        if len(items) < PAGE_SIZE:
            break
        params["offset"] += PAGE_SIZE


def update(configuration: dict, state: dict):
    client_id = configuration["client_id"]
    client_secret = configuration["client_secret"]
    token = _get_token(client_id, client_secret)

    # Use a 90-day rolling window for trips/bookings; track last run date for expenses
    last_sync = state.get("last_sync_date", "2020-01-01")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # --- Trips ---
    log.info(f"Syncing trips from {last_sync}")
    for trip in _paginate(token, "/trips", {"startDate": last_sync, "endDate": today}):
        yield op.upsert("trips", {
            "id": trip.get("id"),
            "booking_ref": trip.get("bookingRef") or trip.get("booking_ref"),
            "traveler_id": trip.get("travelerId") or trip.get("traveler_id"),
            "traveler_email": trip.get("travelerEmail") or trip.get("traveler_email"),
            "trip_name": trip.get("tripName") or trip.get("name"),
            "start_date": trip.get("startDate") or trip.get("start_date"),
            "end_date": trip.get("endDate") or trip.get("end_date"),
            "purpose": trip.get("purpose"),
            "status": trip.get("status"),
            "total_cost": trip.get("totalCost") or trip.get("total_cost"),
            "currency": trip.get("currency"),
            "created_at": trip.get("createdAt") or trip.get("created_at"),
            "updated_at": trip.get("updatedAt") or trip.get("updated_at"),
        })

        # --- Bookings (nested or separate endpoint) ---
        bookings = trip.get("bookings", [])
        for booking in bookings:
            yield op.upsert("bookings", {
                "id": booking.get("id"),
                "trip_id": trip.get("id"),
                "booking_type": booking.get("bookingType") or booking.get("type"),
                "vendor": booking.get("vendor") or booking.get("vendorName"),
                "booking_ref": booking.get("bookingRef") or booking.get("pnr"),
                "check_in": booking.get("checkIn") or booking.get("departureDate"),
                "check_out": booking.get("checkOut") or booking.get("arrivalDate"),
                "traveler_id": booking.get("travelerId") or trip.get("travelerId"),
                "cost": booking.get("cost") or booking.get("totalCost"),
                "currency": booking.get("currency") or trip.get("currency"),
                "status": booking.get("status"),
                "created_at": booking.get("createdAt") or booking.get("created_at"),
            })

    # --- Expenses ---
    log.info(f"Syncing expenses from {last_sync}")
    for expense in _paginate(token, "/expenses", {"submittedDateFrom": last_sync, "submittedDateTo": today}):
        expense_id = expense.get("id")
        yield op.upsert("expenses", {
            "id": expense_id,
            "traveler_id": expense.get("travelerId") or expense.get("traveler_id"),
            "report_name": expense.get("reportName") or expense.get("name"),
            "status": expense.get("status"),
            "submitted_date": expense.get("submittedDate") or expense.get("submitted_date"),
            "approved_date": expense.get("approvedDate") or expense.get("approved_date"),
            "total_amount": expense.get("totalAmount") or expense.get("total_amount"),
            "currency": expense.get("currency"),
            "trip_id": expense.get("tripId") or expense.get("trip_id"),
        })

        for item in expense.get("items", []):
            yield op.upsert("expense_items", {
                "id": item.get("id"),
                "expense_id": expense_id,
                "category": item.get("category"),
                "merchant": item.get("merchant") or item.get("merchantName"),
                "amount": item.get("amount"),
                "currency": item.get("currency"),
                "expense_date": item.get("expenseDate") or item.get("date"),
                "receipt_url": item.get("receiptUrl") or item.get("receipt_url"),
                "description": item.get("description") or item.get("note"),
            })

    # --- Users (full refresh) ---
    log.info("Syncing users")
    for user in _paginate(token, "/users", {}):
        yield op.upsert("users", {
            "id": user.get("id"),
            "email": user.get("email"),
            "first_name": user.get("firstName") or user.get("first_name"),
            "last_name": user.get("lastName") or user.get("last_name"),
            "department": user.get("department"),
            "manager_id": user.get("managerId") or user.get("manager_id"),
            "role": user.get("role"),
            "active": user.get("active"),
        })

    yield op.checkpoint(state={"last_sync_date": today})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
