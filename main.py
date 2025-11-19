"""
HubSpot Form Recovery – Multi-Form Edition
------------------------------------------

Modes:
1) Smoke Test Mode (dry run):
    → Reads submissions
    → Finds contacts in HubSpot
    → Prints a simple summary
    → No HubSpot updates

2) Write Mode:
    → Same as smoke test
    → PLUS updates only missing fields defined in HUBSPOT_FORM_PROPERTY_MAP
"""

from __future__ import annotations
import json, logging, os, time
from datetime import datetime
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------

load_dotenv()
os.makedirs("data", exist_ok=True)

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"

logger = logging.getLogger("hubspot_recovery")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (logging.StreamHandler(), RotatingFileHandler(LOG_FILE, maxBytes=3_000_000, backupCount=3)):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

app = FastAPI(title="HubSpot Form Recovery")

HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# MULTI-FORM MAP (in Render env)
HUBSPOT_FORM_PROPERTY_MAP = json.loads(os.getenv("HUBSPOT_FORM_PROPERTY_MAP", "{}"))

if not HUBSPOT_FORM_PROPERTY_MAP:
    raise RuntimeError("Missing HUBSPOT_FORM_PROPERTY_MAP env var — cannot continue.")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def hubspot_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

def find_contact_by_email(email: str) -> Optional[str]:
    """Search HubSpot contact by email."""
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email
            }]
        }],
        "limit": 1,
        "properties": ["email"],
    }

    r = requests.post(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
        headers=hubspot_headers(),
        json=payload,
        timeout=30,
    )

    if not r.ok:
        logger.error(f"Search failed for {email}: {r.text}")
        return None

    results = r.json().get("results", [])
    return results[0]["id"] if results else None


def fetch_form_submissions(form_id: str) -> List[Dict]:
    """Fetch ALL submissions for a given form ID."""
    logger.info(f"Fetching submissions for form: {form_id}")

    after = None
    results = []
    total = 0

    while True:
        params = {"limit": 50}
        if after:
            params["after"] = after

        r = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
            params=params,
            timeout=30,
        )

        if not r.ok:
            logger.error(f"Failed to fetch form {form_id}: {r.text}")
            break

        data = r.json()
        batch = data.get("results", [])
        if not batch:
            break

        results.extend(batch)
        total += len(batch)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        time.sleep(0.3)

    logger.info(f"Fetched {total} submissions for form {form_id}")
    return results


def extract_submission_values(sub: Dict) -> Dict[str, str]:
    """Turns HubSpot's weird structure into a simple dict."""
    flat = {}
    for item in sub.get("values", []):
        name = item.get("name")
        value = item.get("value")
        if isinstance(name, str):
            flat[name] = value
    return flat


# ---------------------------------------------------------------------
# Smoke Test & Write Logic
# ---------------------------------------------------------------------

def run_smoke_test(form_id: str, mapping: Dict[str, str], submissions: List[Dict]):
    """Print a clean, simple summary with NO writes."""
    logger.info(f"--- Smoke Test for Form {form_id} ({len(submissions)} submissions) ---")

    for idx, sub in enumerate(submissions, start=1):
        vals = extract_submission_values(sub)

        email = vals.get("email", "").strip()
        cid = find_contact_by_email(email) if email else None

        logger.info(f"[SMOKE] {form_id} | #{idx}")
        logger.info(f"   email: {email or 'MISSING'}")
        logger.info(f"   contact_id: {cid or 'NOT FOUND'}")

        # Show only mapped fields present
        mapped_found = {k: vals.get(k) for k in mapping.keys() if k in vals}
        logger.info(f"   fields: {json.dumps(mapped_found, indent=4)}")


def run_write_mode(form_id: str, mapping: Dict[str, str], submissions: List[Dict]):
    """Updates ONLY the fields defined in the map and present in the submission."""
    logger.info(f"--- Write Mode for Form {form_id} ({len(submissions)} submissions) ---")

    for idx, sub in enumerate(submissions, start=1):
        vals = extract_submission_values(sub)
        email = vals.get("email", "").strip()
        if not email:
            continue

        cid = find_contact_by_email(email)
        if not cid:
            logger.info(f"[WRITE] #{idx} email={email} → Contact NOT FOUND")
            continue

        # Build only properties that appear in this submission
        props_to_update = {
            hs_prop: vals.get(form_field)
            for form_field, hs_prop in mapping.items()
            if form_field in vals
        }

        if not props_to_update:
            logger.info(f"[WRITE] #{idx} email={email} → No mapped fields in submission")
            continue

        payload = {"properties": props_to_update}

        r = requests.patch(
            f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
            headers=hubspot_headers(),
            json=payload,
            timeout=30,
        )

        if r.ok:
            logger.info(f"[WRITE] Updated {email} → {json.dumps(props_to_update)}")
        else:
            logger.error(f"[WRITE] FAILED {email}: {r.text}")

        time.sleep(0.3)


# ---------------------------------------------------------------------
# Main Endpoint
# ---------------------------------------------------------------------

@app.post("/run-all")
async def run_all(request: Request):
    """
    Body:
    {
        "mode": "smoke" | "write"
    }
    """
    try:
        body = await request.json()
    except:
        body = {}

    mode = body.get("mode", "smoke").lower()

    if mode not in ("smoke", "write"):
        return {"error": "mode must be 'smoke' or 'write'"}

    logger.info(f"▶️ Starting run-all in mode={mode}")

    # Iterate form → mapping → submissions
    for form_id, mapping in HUBSPOT_FORM_PROPERTY_MAP.items():
        submissions = fetch_form_submissions(form_id)

        if mode == "smoke":
            run_smoke_test(form_id, mapping, submissions)
        else:
            run_write_mode(form_id, mapping, submissions)

    return {"status": "complete", "mode": mode}


@app.get("/health")
def health():
    return {"status": "ok", "forms": list(HUBSPOT_FORM_PROPERTY_MAP.keys())}


# ---------------------------------------------------------------------
# Local run
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
