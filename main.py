import os
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, Body, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

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
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def log_json(event: str, **kwargs):
    record = {"event": event, **kwargs}
    logger.info(json.dumps(record))


# ---------------------------------------------------------------------------
# Auth Helper
# ---------------------------------------------------------------------------

def require_auth(authorization: str = Header(None)):
    """Validates Authorization: Bearer <token> header."""
    if not authorization:
        log_json("auth_missing")
        raise HTTPException(status_code=403, detail="Missing Authorization header.")

    try:
        scheme, token = authorization.split(" ")
    except ValueError:
        log_json("auth_invalid_format", header=authorization)
        raise HTTPException(status_code=403, detail="Invalid Authorization header format.")

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

def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }


def fetch_form_submissions(form_id: str, after: Optional[str] = None):
    url = f"{HUBSPOT_BASE_URL}/marketing/v3/forms/{form_id}/submissions"
    params = {"limit": 100}
    if after:
        params["after"] = after

    resp = requests.get(url, headers=hubspot_headers(), params=params)
    resp.raise_for_status()
    return resp.json()


def get_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"],
        "limit": 1
    }

    resp = requests.post(url, headers=hubspot_headers(), json=body)
    resp.raise_for_status()
    data = resp.json()

    if "results" in data and len(data["results"]) > 0:
        return data["results"][0]

    return None


def update_contact(contact_id: str, properties: Dict[str, Any]):
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    resp = requests.patch(url, headers=hubspot_headers(), json={"properties": properties})
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Rate Limit Heuristics
# ---------------------------------------------------------------------------

def apply_rate_limit_heuristics(resp_headers):
    remaining = resp_headers.get("X-HubSpot-RateLimit-Remaining")
    if remaining is not None:
        try:
            remaining = int(remaining)
            if remaining < 5:
                time.sleep(4)
            elif remaining < 10:
                time.sleep(2)
        except:
            pass

    if resp_headers.get("Retry-After"):
        time.sleep(int(resp_headers["Retry-After"]))

    time.sleep(0.15)


# ---------------------------------------------------------------------------
# Process One Submission
# ---------------------------------------------------------------------------

def process_submission(form_id: str, submission: Dict[str, Any], mode: str):
    submitted_values = submission.get("values", [])

    email = None
    submission_fields = {}

    for field in submitted_values:
        field_name = field.get("name")
        field_value = field.get("value")
        submission_fields[field_name] = field_value
        if field_name == "email":
            email = field_value

    if not email:
        log_json("skip_no_email", form=form_id)
        return

    contact = get_contact_by_email(email)
    if not contact:
        log_json("contact_not_found", email=email)
        return

    contact_id = contact["id"]
    mapped_fields = FORM_PROPERTY_MAP.get(form_id, {})
    updates = {}

    for form_field, hubspot_property in mapped_fields.items():
        submission_value = submission_fields.get(form_field)

        if submission_value is None:
            continue

        existing_value = contact.get("properties", {}).get(hubspot_property)
        if existing_value not in (None, "", " "):
            continue

        updates[hubspot_property] = submission_value

    log_json(
        "submission_processed",
        form=form_id,
        email=email,
        contact_id=contact_id,
        updates_count=len(updates),
        updates=updates,
        mode=mode
    )

    if DRY_RUN_FORCE:
        log_json("dry_run_forced", email=email)
        return

    if mode == "write" and updates:
        update_contact(contact_id, updates)


# ---------------------------------------------------------------------------
# Worker Runner
# ---------------------------------------------------------------------------

def run_recovery(mode: str):
    if DRY_RUN_FORCE:
        mode = "smoke"

    for form_id in FORM_PROPERTY_MAP.keys():
        log_json("start_form", form=form_id, mode=mode)

        after = None

        while True:
            resp = requests.get(
                f"{HUBSPOT_BASE_URL}/marketing/v3/forms/{form_id}/submissions",
                headers=hubspot_headers(),
                params={"limit": 100, **({"after": after} if after else {})}
            )
            resp.raise_for_status()

            data = resp.json()
            results = data.get("results", [])

            apply_rate_limit_heuristics(resp.headers)

            for submission in results:
                process_submission(form_id, submission, mode)

            page = data.get("paging", {})
            if not page or "next" not in page:
                break

            after = page["next"]["after"]

        log_json("end_form", form=form_id)


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI()


@app.get("/health")
def health():
    """Unauthenticated for Render health checks."""
    return {"status": "ok", "forms": list(FORM_PROPERTY_MAP.keys())}


@app.get("/status")
def status(authorization: str = Header(None)):
    require_auth(authorization)
    return {
        "status": "alive" if not KILLED else "killed",
        "kill_switch": KILLED,
        "dry_run_force": DRY_RUN_FORCE,
        "forms_loaded": list(FORM_PROPERTY_MAP.keys())
    }


@app.post("/kill")
def kill(authorization: str = Header(None)):
    require_auth(authorization)
    global KILLED
    KILLED = True
    log_json("kill_switch_activated")
    return {"status": "killed"}


@app.post("/unkill")
def unkill(authorization: str = Header(None)):
    require_auth(authorization)
    global KILLED
    KILLED = False
    log_json("kill_switch_deactivated")
    return {"status": "alive"}


@app.post("/run-all")
def run_all(body: Dict[str, Any] = Body(...), authorization: str = Header(None)):
    require_auth(authorization)

    if KILLED:
        log_json("run_blocked_killed")
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."}
        )

    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(status_code=400, content={"error": "Invalid mode"})

    log_json("run_all_start", mode=mode)
    run_recovery(mode)
    return {"status": "complete", "mode": mode}
