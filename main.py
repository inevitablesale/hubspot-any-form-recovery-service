import os
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, Body, Header, HTTPException, Depends
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# App Metadata
# ---------------------------------------------------------------------------

APP_VERSION = "2.1.0"

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------

HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
FORM_PROPERTY_MAP_RAW = os.getenv("HUBSPOT_FORM_PROPERTY_MAP")
HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DRY_RUN_FORCE = os.getenv("DRY_RUN_FORCE", "false").lower() == "true"
APP_AUTH_TOKEN = os.getenv("APP_AUTH_TOKEN")

if not HUBSPOT_TOKEN:
    raise Exception("HUBSPOT_PRIVATE_APP_TOKEN is required.")
if not FORM_PROPERTY_MAP_RAW:
    raise Exception("HUBSPOT_FORM_PROPERTY_MAP is required.")
if not APP_AUTH_TOKEN:
    raise Exception("APP_AUTH_TOKEN is required for API security.")

FORM_PROPERTY_MAP: Dict[str, Dict[str, str]] = json.loads(FORM_PROPERTY_MAP_RAW)

# ---------------------------------------------------------------------------
# Logging Setup — One-line JSON logs
# ---------------------------------------------------------------------------

logger = logging.getLogger("recovery")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler("recovery.log", maxBytes=2_000_000, backupCount=3)
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def log_json(event: str, **kwargs) -> None:
    record = {"event": event, **kwargs}
    logger.info(json.dumps(record))


log_json(
    "service_start",
    version=APP_VERSION,
    dry_run_force=DRY_RUN_FORCE,
    forms_loaded=list(FORM_PROPERTY_MAP.keys()),
)

# ---------------------------------------------------------------------------
# Auth Helper
# ---------------------------------------------------------------------------


def require_auth(authorization: str = Header(None)) -> bool:
    """Validates Authorization: Bearer <token> header."""
    if not authorization:
        log_json("auth_missing")
        raise HTTPException(status_code=403, detail="Missing Authorization header.")

    try:
        scheme, token = authorization.split(" ", 1)
    except ValueError:
        log_json("auth_invalid_format", header=authorization)
        raise HTTPException(
            status_code=403, detail="Invalid Authorization header format."
        )

    if scheme.lower() != "bearer" or token != APP_AUTH_TOKEN:
        log_json("auth_invalid_token", provided=token)
        raise HTTPException(status_code=403, detail="Invalid authentication token.")

    return True


# ---------------------------------------------------------------------------
# Kill Switch (in-memory)
# ---------------------------------------------------------------------------

KILLED = False

# ---------------------------------------------------------------------------
# HubSpot API Helpers
# ---------------------------------------------------------------------------


def hubspot_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def apply_rate_limit_heuristics(resp_headers: Dict[str, Any]) -> None:
    """Simple backoff when approaching HubSpot rate limits."""
    remaining = resp_headers.get("X-HubSpot-RateLimit-Remaining")
    if remaining is not None:
        try:
            remaining_int = int(remaining)
            if remaining_int < 5:
                time.sleep(4)
            elif remaining_int < 10:
                time.sleep(2)
        except Exception:
            # If header isn't an int, just ignore
            pass

    retry_after = resp_headers.get("Retry-After")
    if retry_after:
        try:
            time.sleep(int(retry_after))
        except Exception:
            pass

    # Small general delay to be gentle on the API
    time.sleep(0.15)


def fetch_form_submissions(form_id: str, after: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch submissions for a given HubSpot form using the form-integrations API.

    This endpoint works for regular HubSpot forms (the IDs you see in the UI):
      /form-integrations/v1/submissions/forms/{formId}
    """
    url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}"
    params: Dict[str, Any] = {"limit": 1000}
    if after:
        params["after"] = after

    resp = requests.get(url, headers=hubspot_headers(), params=params)
    apply_rate_limit_heuristics(resp.headers)

    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        log_json(
            "form_submissions_error",
            form=form_id,
            status=status,
            error=str(exc),
        )
        # If a form was deleted or never existed, just log and return no results
        if status == 404:
            return {"results": [], "paging": {}}
        raise

    data = resp.json()
    log_json(
        "form_page_fetched",
        form=form_id,
        after=after,
        results_count=len(data.get("results", [])),
    )
    return data


def get_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"],
        "limit": 1,
    }

    resp = requests.post(url, headers=hubspot_headers(), json=body)
    resp.raise_for_status()
    data = resp.json()

    if "results" in data and len(data["results"]) > 0:
        return data["results"][0]

    return None


def update_contact(contact_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    resp = requests.patch(
        url, headers=hubspot_headers(), json={"properties": properties}
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Process One Submission
# ---------------------------------------------------------------------------


def process_submission(form_id: str, submission: Dict[str, Any], mode: str) -> None:
    submitted_values = submission.get("values", [])

    email: Optional[str] = None
    submission_fields: Dict[str, Any] = {}

    for field in submitted_values:
        field_name = field.get("name")
        field_value = field.get("value")
        if field_name is None:
            continue
        submission_fields[field_name] = field_value
        if field_name == "email":
            email = field_value

    if not email:
        log_json("skip_no_email", form=form_id)
        return

    contact = get_contact_by_email(email)
    if not contact:
        log_json("contact_not_found", email=email, form=form_id)
        return

    contact_id = contact["id"]
    mapped_fields = FORM_PROPERTY_MAP.get(form_id, {})
    updates: Dict[str, Any] = {}

    for form_field, hubspot_property in mapped_fields.items():
        submission_value = submission_fields.get(form_field)
        if submission_value is None:
            continue

        existing_value = contact.get("properties", {}).get(hubspot_property)
        if existing_value not in (None, "", " "):
            # Don't overwrite existing data
            continue

        updates[hubspot_property] = submission_value

    log_json(
        "submission_processed",
        form=form_id,
        email=email,
        contact_id=contact_id,
        updates_count=len(updates),
        updates=updates,
        mode=mode,
    )

    if DRY_RUN_FORCE:
        log_json("dry_run_forced", email=email, form=form_id)
        return

    if mode == "write" and updates:
        update_contact(contact_id, updates)
        log_json(
            "contact_updated",
            form=form_id,
            email=email,
            contact_id=contact_id,
            updated_properties=list(updates.keys()),
        )


# ---------------------------------------------------------------------------
# Worker Runner
# ---------------------------------------------------------------------------


def run_recovery(mode: str) -> None:
    """Iterate through all configured forms and repair contact data."""
    effective_mode = "smoke" if DRY_RUN_FORCE else mode

    for form_id in FORM_PROPERTY_MAP.keys():
        log_json("start_form", form=form_id, requested_mode=mode, mode=effective_mode)

        after: Optional[str] = None

        while True:
            page_data = fetch_form_submissions(form_id, after=after)
            results = page_data.get("results", [])

            if not results:
                # No submissions or form not found / empty
                break

            for submission in results:
                process_submission(form_id, submission, effective_mode)

            page = page_data.get("paging", {})
            next_page = page.get("next") if page else None
            if not next_page or "after" not in next_page:
                break

            after = next_page["after"]

        log_json("end_form", form=form_id, mode=effective_mode)


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(title="HubSpot Form Submission Recovery Service", version=APP_VERSION)


@app.get("/health")
def health():
    """Unauthenticated health check for Render and uptime monitors."""
    return {
        "status": "ok",
        "version": APP_VERSION,
        "forms": list(FORM_PROPERTY_MAP.keys()),
        "dry_run_force": DRY_RUN_FORCE,
    }


@app.get("/status")
def status(_: bool = Depends(require_auth)):
    return {
        "status": "alive" if not KILLED else "killed",
        "kill_switch": KILLED,
        "dry_run_force": DRY_RUN_FORCE,
        "forms_loaded": list(FORM_PROPERTY_MAP.keys()),
        "version": APP_VERSION,
    }


@app.post("/kill")
def kill(_: bool = Depends(require_auth)):
    global KILLED
    KILLED = True
    log_json("kill_switch_activated")
    return {"status": "killed"}


@app.post("/unkill")
def unkill(_: bool = Depends(require_auth)):
    global KILLED
    KILLED = False
    log_json("kill_switch_deactivated")
    return {"status": "alive"}


@app.post("/run-all")
def run_all(
    body: Dict[str, Any] = Body(...),
    _: bool = Depends(require_auth),
):
    if KILLED:
        log_json("run_blocked_killed")
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."},
        )

    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(status_code=400, content={"error": "Invalid mode"})

    log_json("run_all_start", mode=mode, dry_run_force=DRY_RUN_FORCE)
    run_recovery(mode)
    return {"status": "complete", "mode": "smoke" if DRY_RUN_FORCE else mode}
