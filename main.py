import os
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, Body
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

if not HUBSPOT_TOKEN:
    raise Exception("HUBSPOT_PRIVATE_APP_TOKEN is required.")
if not FORM_PROPERTY_MAP_RAW:
    raise Exception("HUBSPOT_FORM_PROPERTY_MAP is required.")

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
# HubSpot API Helpers
# ---------------------------------------------------------------------------

def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }


# Fetch form submissions w/ pagination
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
    """
    Uses HubSpot response headers to throttle calls safely.

    Uses the 4 heuristic rules you approved:
    1. If remaining under 10 → sleep 2 seconds
    2. If remaining under 5  → sleep 4 seconds
    3. If "too many requests" → sleep 10 seconds
    4. Add a tiny jitter (0.1–0.3s) for smoothing burst patterns
    """

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

    time.sleep(0.15)  # jitter


# ---------------------------------------------------------------------------
# Process One Submission (core logic)
# ---------------------------------------------------------------------------

def process_submission(
    form_id: str,
    submission: Dict[str, Any],
    mode: str
):
    submitted_values = submission.get("values", [])

    # Extract email from submission
    email = None
    submission_fields = {}

    for field in submitted_values:
        field_name = field.get("name")
        field_value = field.get("value")
        submission_fields[field_name] = field_value
        if field_name == "email":
            email = field_value

    # EMAIL REQUIRED
    if not email:
        log_json("skip_no_email", form=form_id, submissionId=submission.get("submittedAt"))
        return

    # Find contact once
    contact = get_contact_by_email(email)
    if not contact:
        log_json("contact_not_found", form=form_id, email=email)
        return

    contact_id = contact["id"]

    # Start building changes
    mapped_fields = FORM_PROPERTY_MAP.get(form_id, {})
    updates = {}

    # Loop mapped fields ONLY
    for form_field, hubspot_property in mapped_fields.items():
        submission_value = submission_fields.get(form_field)

        # Per-field rule 1: Must be in submission
        if submission_value is None:
            continue

        # Per-field rule 2: Must be mapped (guaranteed)
        # Per-field rule 3: Don't overwrite existing values
        existing_value = contact.get("properties", {}).get(hubspot_property)
        if existing_value not in (None, "", " "):
            continue

        updates[hubspot_property] = submission_value

    # Emit summary log
    log_json(
        "submission_processed",
        form=form_id,
        email=email,
        contact_id=contact_id,
        updates_count=len(updates),
        updates=updates,
        mode=mode
    )

    # Dry-Run Override (env-level)
    if DRY_RUN_FORCE:
        log_json("dry_run_forced", form=form_id, email=email)
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
    return {"status": "ok", "forms": list(FORM_PROPERTY_MAP.keys())}


@app.post("/run-all")
def run_all(body: Dict[str, Any] = Body(...)):
    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(status_code=400, content={"error": "Invalid mode"})

    run_recovery(mode)
    return {"status": "complete", "mode": mode}
