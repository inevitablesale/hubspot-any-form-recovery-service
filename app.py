import logging
import os
from typing import Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="HubSpot Registration Recovery")

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
HUBSPOT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

CHECKBOX_FIELDS = {
    "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy": "portal_terms_accepted",
    "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information": "marketing_opt_in_vrm_properties",
}

DEFAULT_STATE = "Not Checked"


def hubspot_headers() -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN environment variable is required")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


class RunRequest(BaseModel):
    limit: int = Field(500, gt=0, le=1000, description="Maximum number of submissions to fetch.")


class RunResponse(BaseModel):
    processed: int
    updated: int
    skipped: int
    errors: int


@app.post("/run", response_model=RunResponse)
def run_sync(payload: RunRequest) -> RunResponse:
    try:
        stats = process_submissions(limit=payload.limit)
    except RuntimeError as exc:
        logger.exception("Configuration error while running recovery job")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except requests.HTTPError as exc:
        logger.exception("HubSpot API returned an error")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive catch-all
        logger.exception("Unexpected error while running recovery job")
        raise HTTPException(status_code=500, detail="Unexpected error") from exc

    return RunResponse(**stats)


def process_submissions(limit: int) -> Dict[str, int]:
    submissions = fetch_form_submissions(limit=limit)
    stats = {"processed": len(submissions), "updated": 0, "skipped": 0, "errors": 0}

    for submission in submissions:
        try:
            email, checkbox_values = parse_submission(submission)
        except ValueError as exc:
            logger.warning("Skipping submission due to parsing error: %s", exc)
            stats["skipped"] += 1
            continue

        if not email:
            logger.info("Skipping submission without an email address")
            stats["skipped"] += 1
            continue

        try:
            contact_id = find_contact_id(email)
        except requests.HTTPError as exc:
            logger.error("Failed to find contact for %s: %s", email, exc)
            stats["errors"] += 1
            continue

        if not contact_id:
            logger.info("No contact found for email %s", email)
            stats["skipped"] += 1
            continue

        try:
            update_contact(contact_id, checkbox_values)
        except requests.HTTPError as exc:
            logger.error("Failed to update contact %s: %s", contact_id, exc)
            stats["errors"] += 1
            continue

        stats["updated"] += 1

    return stats


def fetch_form_submissions(limit: int) -> List[Dict]:
    url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{HUBSPOT_FORM_ID}"
    response = requests.get(url, headers=hubspot_headers(), params={"limit": limit}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results", [])

    logger.info("Fetched %s submissions", len(results))
    return results


def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    values = submission.get("values", [])
    email = None
    consent_states = {v: DEFAULT_STATE for v in CHECKBOX_FIELDS.values()}

    for item in values:
        name = item.get("name")
        value = item.get("value")
        if name == "email" and isinstance(value, str):
            email = value.strip() or None
        elif name in CHECKBOX_FIELDS:
            label = CHECKBOX_FIELDS[name]
            consent_states[label] = "Checked" if value == "Checked" else DEFAULT_STATE

    return email, consent_states


def find_contact_id(email: str) -> Optional[str]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email,
                    }
                ]
            }
        ],
        "limit": 1,
        "properties": list(CHECKBOX_FIELDS.values()),
    }
    response = requests.post(url, headers=hubspot_headers(), json=payload, timeout=30)
    response.raise_for_status()
    results = response.json().get("results", [])

    if not results:
        return None

    return results[0].get("id")


def update_contact(contact_id: str, consent_states: Dict[str, str]) -> None:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    payload = {"properties": consent_states}
    response = requests.patch(url, headers=hubspot_headers(), json=payload, timeout=30)
    response.raise_for_status()
    logger.info("Updated contact %s", contact_id)


if __name__ == "__main__":
    stats = process_submissions(limit=500)
    logger.info("Run finished: %s", stats)
