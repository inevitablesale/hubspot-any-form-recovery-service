"""FastAPI service to recover and log HubSpot consent preferences (no updates, enhanced logging, kill switch, submission ID)."""

from __future__ import annotations
import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# --- Environment Setup ---
load_dotenv()

REQUIRED_ENV_VARS = [
    "HUBSPOT_BASE_URL",
    "HUBSPOT_PRIVATE_APP_TOKEN",
    "HUBSPOT_FORM_ID",
]

for key in REQUIRED_ENV_VARS:
    if not os.getenv(key):
        raise RuntimeError(f"Missing required environment variable: {key}")

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"

logger = logging.getLogger("hubspot_form_recovery")
logger.setLevel(logging.INFO)
logger.handlers = []

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(stream_handler)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(file_handler)

DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")
logger.info("Starting HubSpot Recovery Service (v1 – read-only)")
logger.info("DRY_RUN=%s | LOG_FILE=%s", DRY_RUN, LOG_FILE)

# --- FastAPI app ---
app = FastAPI(title="HubSpot Form Recovery – v1 Read-Only Version")

# --- HubSpot configuration ---
HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "")

CHECKBOX_PROPERTIES = [
    prop.strip()
    for prop in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if prop.strip()
]

logger.info("Configured checkbox fields: %s", ", ".join(CHECKBOX_PROPERTIES))

FORM_PAGE_SIZE = 50  # v1 max limit
FETCH_DELAY = 0.2

# --- Kill flag ---
RUNNING = True


@app.get("/kill")
def kill_job():
    """Stops a currently running recovery process."""
    global RUNNING
    RUNNING = False
    logger.warning("Kill signal received — active process will stop shortly.")
    return {"status": "terminating", "message": "Active recovery job will stop gracefully."}


# --- Helpers ---
def hubspot_headers(include_content_type: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN environment variable is required")
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "HubSpotConsentRecovery/1.0",
    }
    if include_content_type:
        headers["Content-Type"] = "application/json"
    return headers


# --- Models ---
class RunRequest(BaseModel):
    dry_run: Optional[bool] = None
    form_id: Optional[str] = None


class RunSummary(BaseModel):
    dry_run: bool
    processed: int
    parsed: int
    skipped: int
    errors: int


# --- Main Recovery Logic ---
def execute_recovery(form_id: str, dry_run: bool) -> RunSummary:
    global RUNNING
    RUNNING = True  # reset kill flag each run

    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID env variable required")

    logger.info("Running recovery for form: %s | dry_run=%s", form_id, dry_run)
    start_time = time.time()

    submissions = fetch_all_submissions(form_id)
    stats = process_submissions(submissions)

    elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info("✅ Run finished | Total processed: %s | Runtime: %.1f sec", stats["processed"], elapsed)
    logger.info("=" * 80)

    return RunSummary(dry_run=dry_run, **stats)


# --- POST trigger ---
@app.post("/run", response_model=RunSummary)
def run_recovery_post(request: Optional[RunRequest] = None) -> RunSummary:
    dry_run = DRY_RUN if not request or request.dry_run is None else bool(request.dry_run)
    form_id = (request.form_id or DEFAULT_FORM_ID).strip()
    return execute_recovery(form_id, dry_run)


# --- GET trigger ---
@app.get("/run", response_model=RunSummary)
def run_recovery_get(
    form_id: Optional[str] = Query(None, description="HubSpot form ID"),
    dry_run: Optional[bool] = Query(False, description="Dry run mode"),
) -> RunSummary:
    form_id = form_id or DEFAULT_FORM_ID
    return execute_recovery(form_id, bool(dry_run))


# --- Fetch submissions (v1 version) ---
def fetch_all_submissions(form_id: str) -> List[Dict]:
    global RUNNING
    submissions: List[Dict] = []
    after: Optional[str] = None

    logger.info("Fetching submissions from HubSpot API (/form-integrations/v1)...")
    url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}"

    while RUNNING:
        params: Dict[str, object] = {"limit": FORM_PAGE_SIZE}
        if after:
            params["after"] = after

        response = requests.get(url, headers=hubspot_headers(False), params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        page_results = payload.get("results") or []
        submissions.extend(page_results)
        logger.info("Fetched %s new submissions (total=%s)", len(page_results), len(submissions))

        paging = payload.get("paging", {})
        after = paging.get("next", {}).get("after")
        time.sleep(FETCH_DELAY)

        if not after:
            break

    if not RUNNING:
        logger.warning("🟥 Fetch interrupted by kill signal. Returning partial data (%s submissions).", len(submissions))
    else:
        logger.info("Total submissions fetched: %s", len(submissions))

    return submissions


# --- Parse & Log Consent Values (enhanced per-record output) ---
def process_submissions(submissions: List[Dict]) -> Dict[str, int]:
    global RUNNING
    stats = {"processed": 0, "parsed": 0, "skipped": 0, "errors": 0}

    for i, submission in enumerate(submissions, start=1):
        if not RUNNING:
            logger.warning("🟥 Run stopped manually after %s submissions processed.", stats["processed"])
            break

        stats["processed"] += 1
        try:
            email, checkboxes = parse_submission(submission)
            submission_id = (
                submission.get("conversionId")
                or submission.get("submissionGuid")
                or submission.get("id")
                or "N/A"
            )

            if not email:
                stats["skipped"] += 1
                continue

            portal_terms = checkboxes.get(
                "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", "N/A"
            )
            marketing_opt_in = checkboxes.get(
                "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
                "N/A",
            )

            logger.info(
                "[%04d] Submission ID: %s | Email: %-40s | Portal Terms: %-10s | Marketing Opt-In: %-10s",
                i, submission_id, email, portal_terms, marketing_opt_in
            )

            stats["parsed"] += 1
        except Exception as e:
            stats["errors"] += 1
            logger.error("Error processing submission %s: %s", i, e)

    if RUNNING:
        logger.info("✅ Completed full run (%s submissions).", stats["processed"])
    else:
        logger.warning("🟥 Run terminated early by user.")

    return stats


def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    email, states = None, {}
    values = submission.get("values", submission.get("formValues", []))
    for item in values:
        name, value = item.get("name"), item.get("value")
        if not name or not isinstance(value, str):
            continue
        if name == "email":
            email = value.strip() or None
        elif name in CHECKBOX_PROPERTIES and value.strip() in ("Checked", "Not Checked"):
            states[name] = value.strip()
    return email, states


# --- Health check ---
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": time.time(), "dry_run": DRY_RUN}
