"""
HubSpot Form Recovery Service – Scalable Edition

Includes:
✅ Smoke test & trace routes
✅ Batch fetch, dedupe, recovery, and reporting for >17k submissions
✅ Fully safe — no writes to system-managed properties like hs_marketable_status
✅ Enhanced logging of form checkbox values and payloads for each record
"""

from __future__ import annotations
import json, logging, os, time, glob
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

# ---------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------

load_dotenv()
os.makedirs("data", exist_ok=True)

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logger = logging.getLogger("hubspot_form_recovery")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (logging.StreamHandler(), RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

logger.info("Starting HubSpot Form Recovery Service")

app = FastAPI(title="HubSpot Form Recovery API")

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

CHECKBOX_PROPERTIES = [
    p.strip()
    for p in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if p.strip()
]


def hubspot_headers(ct: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN")
    h = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if ct:
        h["Content-Type"] = "application/json"
    return h


# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "mode": "scalable"}


# ---------------------------------------------------------------------
# Fetch ALL submissions (paged)
# ---------------------------------------------------------------------

@app.api_route("/run-fetch", methods=["GET", "POST"])
def run_fetch(form_id: str = DEFAULT_FORM_ID, max_pages: int = 9999):
    """Fetch all form submissions and write paginated JSONL files to /data."""
    logger.info("🚀 Starting full form fetch for %s", form_id)
    after, total = None, 0
    page_idx = 1

    while page_idx <= max_pages:
        params = {"limit": 50}
        if after:
            params["after"] = after

        r = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers=hubspot_headers(False),
            params=params,
            timeout=30,
        )
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="Form not found or deleted.")
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        file_path = f"data/submissions_page_{page_idx:04d}.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            for s in results:
                f.write(json.dumps(s) + "\n")
        total += len(results)
        logger.info("📄 Saved page %s (%s submissions)", page_idx, len(results))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        page_idx += 1
        time.sleep(0.3)

    logger.info("✅ Fetch complete — %s total submissions across %s pages", total, page_idx)
    return {"pages": page_idx, "total_submissions": total}


# ---------------------------------------------------------------------
# Deduplicate all local snapshots by latest email submission
# ---------------------------------------------------------------------

@app.post("/run-dedupe")
def run_dedupe():
    """Deduplicate downloaded submissions into deduped_submissions.jsonl."""
    files = sorted(glob.glob("data/submissions_page_*.jsonl"))
    if not files:
        raise HTTPException(status_code=404, detail="No snapshot files found.")

    latest: Dict[str, Dict] = {}
    total = 0
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                s = json.loads(line)
                email, _ = parse_submission(s)
                if not email:
                    continue
                t = s.get("submittedAt") or s.get("timestamp") or 0
                if email not in latest or t > (latest[email].get("submittedAt") or 0):
                    latest[email] = s
                total += 1

    out_path = "data/deduped_submissions.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for s in latest.values():
            f.write(json.dumps(s) + "\n")

    logger.info("✅ Deduplicated %s total → %s unique emails", total, len(latest))
    return {"input_submissions": total, "unique_emails": len(latest), "output_file": out_path}


# ---------------------------------------------------------------------
# Recovery runner – safe consent updates with resume + detailed logging
# ---------------------------------------------------------------------

@app.post("/run-recover")
def run_recover(batch_size: int = 1000):
    """
    Apply safe consent updates for all deduped submissions.
    Resumable: tracks progress in cursor.txt.
    Now logs checkbox values and payload details for every record.
    """
    deduped_path = "data/deduped_submissions.jsonl"
    if not os.path.exists(deduped_path):
        raise HTTPException(status_code=404, detail="deduped_submissions.jsonl not found")

    with open(deduped_path, "r", encoding="utf-8") as f:
        subs = [json.loads(line) for line in f]

    cursor_file = "data/cursor.txt"
    start_idx = 0
    if os.path.exists(cursor_file):
        start_idx = int(open(cursor_file).read().strip() or 0)
        logger.info("⏩ Resuming from index %s", start_idx)

    success, skipped, errors = 0, 0, 0

    for i, s in enumerate(subs[start_idx:], start=start_idx):
        try:
            email, boxes = parse_submission(s)
            if not email:
                skipped += 1
                continue

            cid = find_contact_by_email(email)
            if not cid:
                skipped += 1
                continue

            opt_in = boxes.get("select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information")
            tos = boxes.get("i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy")

            if opt_in != "Checked":
                logger.info(
                    "⏭️ [%s/%s] Skipped %s — Opt-In: %s, Terms: %s",
                    i + 1,
                    len(subs),
                    email,
                    opt_in or "—",
                    tos or "—",
                )
                skipped += 1
                continue

            payload = {
                "properties": {
                    "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information": "Checked",
                    "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy": "Checked",
                }
            }

            r = requests.patch(
                f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
                headers=hubspot_headers(),
                json=payload,
                timeout=30,
            )

            if not r.ok:
                logger.error("❌ Update failed for %s: %s", email, r.text)
                errors += 1
                continue

            success += 1
            logger.info(
                "✅ [%s/%s] Updated %s\n"
                "    → Form Values: %s\n"
                "    → Payload Sent: %s",
                i + 1,
                len(subs),
                email,
                json.dumps(boxes, indent=2),
                json.dumps(payload['properties'], indent=2),
            )

        except Exception as e:
            logger.error("⚠️ Error on record %s: %s", i, e)
            errors += 1

        if (i + 1) % batch_size == 0:
            with open(cursor_file, "w") as f:
                f.write(str(i + 1))
            logger.info("💾 Progress saved (%s processed)", i + 1)

        time.sleep(0.6)

    with open(cursor_file, "w") as f:
        f.write(str(len(subs)))

    logger.info("🏁 Recovery completed — Success: %s, Skipped: %s, Errors: %s", success, skipped, errors)
    return {"success": success, "skipped": skipped, "errors": errors, "total": len(subs)}


# ---------------------------------------------------------------------
# Generate a simple summary report
# ---------------------------------------------------------------------

@app.get("/run-report")
def run_report():
    """Summarize progress and show final metrics."""
    cursor = 0
    if os.path.exists("data/cursor.txt"):
        cursor = int(open("data/cursor.txt").read().strip() or 0)
    deduped = sum(1 for _ in open("data/deduped_submissions.jsonl", "r")) if os.path.exists("data/deduped_submissions.jsonl") else 0
    return {"records_total": deduped, "records_processed": cursor, "records_remaining": max(deduped - cursor, 0)}


# ---------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------

def parse_submission(s: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    vals = s.get("values", [])
    email, consent = None, {}
    for v in vals:
        name, val = v.get("name"), v.get("value")
        if not isinstance(name, str) or not isinstance(val, str):
            continue
        if name == "email":
            email = val.strip()
        elif name in CHECKBOX_PROPERTIES and val.strip() in ("Checked", "Not Checked"):
            consent[name] = val.strip()
    return email, consent


def find_contact_by_email(email: str) -> Optional[str]:
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "limit": 1,
        "properties": ["email"],
    }
    r = requests.post(f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search", headers=hubspot_headers(), json=payload, timeout=30)
    r.raise_for_status()
    res = r.json().get("results", [])
    return res[0].get("id") if res else None


# ---------------------------------------------------------------------
# Run directly
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
