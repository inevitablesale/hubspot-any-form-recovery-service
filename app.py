"""
HubSpot Form Recovery Service – Unified Edition (Optimized)

Includes:
✅ Single /run-all endpoint
✅ Two modes:
   - Prep Mode (no start_email): Fetch → Dedupe → Export CSV
   - Resume Mode (with start_email): Use uploaded deduped file → Recover
✅ Full contact-level logging
✅ Safe background execution (Render-friendly)
✅ Real-time reporting via /run-report
"""

from __future__ import annotations
import json, logging, os, time, glob, threading, csv
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

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

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
UPLOADED_DEDUPED_PATH = "/etc/secrets/deduped_submissions.jsonl"
LATEST_EXPORT = "/tmp/deduped_submissions.csv"

CHECKBOX_PROPERTIES = [
    p.strip()
    for p in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if p.strip()
]

JOB_STATUS_FILE = "data/job_status.json"

# ---------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------

def update_job_status(**kwargs):
    status = {"timestamp": datetime.utcnow().isoformat(), **kwargs}
    with open(JOB_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)
    logger.info(f"💾 Job status updated: {kwargs}")

def read_job_status():
    if not os.path.exists(JOB_STATUS_FILE):
        return {"status": "idle"}
    with open(JOB_STATUS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------------------------------------------------------------------
# HubSpot helpers
# ---------------------------------------------------------------------

def hubspot_headers(ct: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN")
    h = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if ct:
        h["Content-Type"] = "application/json"
    return h

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
    r = requests.post(f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
                      headers=hubspot_headers(), json=payload, timeout=30)
    r.raise_for_status()
    res = r.json().get("results", [])
    return res[0].get("id") if res else None

# ---------------------------------------------------------------------
# Fetch & dedupe
# ---------------------------------------------------------------------

def fetch_submissions(form_id: str = DEFAULT_FORM_ID, max_pages: int = 9999):
    logger.info(f"🚀 Starting full form fetch for {form_id}")
    after, total, page_idx = None, 0, 1
    while page_idx <= max_pages:
        params = {"limit": 50}
        if after:
            params["after"] = after
        r = requests.get(f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
                         headers=hubspot_headers(False), params=params, timeout=30)
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
        logger.info(f"📄 Saved page {page_idx} ({len(results)} submissions)")
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        page_idx += 1
        time.sleep(0.3)
    logger.info(f"✅ Fetch complete — {total} submissions across {page_idx} pages")
    return total

def dedupe_submissions() -> List[Dict]:
    files = sorted(glob.glob("data/submissions_page_*.jsonl"))
    if not files:
        raise HTTPException(status_code=404, detail="No submission snapshots found.")
    latest: Dict[str, Dict] = {}
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
    out_path = "data/deduped_submissions.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for s in latest.values():
            f.write(json.dumps(s) + "\n")
    logger.info(f"✅ Deduplicated {len(files)} pages → {len(latest)} unique emails")
    return list(latest.values())

# ---------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------

def recover_contacts(start_email: Optional[str], limit: int, subs: List[Dict]):
    """Perform updates on provided submission list starting after given email."""
    start_idx = 0
    if start_email:
        for i, s in enumerate(subs):
            email, _ = parse_submission(s)
            if email and email.lower().strip() == start_email.lower().strip():
                start_idx = i + 1
                logger.info(f"📧 Starting from email {start_email} (index {start_idx})")
                break
        else:
            logger.warning(f"⚠️ Email {start_email} not found, starting from beginning.")

    total = len(subs)
    end_idx = min(start_idx + limit, total)
    success, errors = 0, 0
    update_job_status(status="running", current=start_idx, total=total)
    logger.info(f"🚀 Processing records {start_idx+1}–{end_idx} of {total}")

    for i, s in enumerate(subs[start_idx:end_idx], start=start_idx):
        try:
            email, boxes = parse_submission(s)
            if not email:
                continue
            cid = find_contact_by_email(email)
            if not cid:
                logger.info(f"🚫 [{i+1}/{total}] No HubSpot contact for {email}")
                continue

            payload = {"properties": {
                "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information":
                    boxes.get("select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information", "Not Checked"),
                "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy":
                    boxes.get("i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", "Not Checked"),
            }}

            r = requests.patch(f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
                               headers=hubspot_headers(), json=payload, timeout=30)
            if not r.ok:
                logger.error(f"❌ [{i+1}/{total}] Update failed for {email}: {r.text}")
                errors += 1
                continue

            success += 1
            logger.info(
                f"✅ [{i+1}/{total}] Updated {email}\n"
                f"    → Form Values: {json.dumps(boxes, indent=2)}\n"
                f"    → Payload Sent: {json.dumps(payload['properties'], indent=2)}"
            )
        except Exception as e:
            logger.error(f"⚠️ Error on record {i}: {e}")
            errors += 1
        if (i + 1) % 100 == 0:
            update_job_status(status="running", current=i + 1, total=total, success=success, errors=errors)
            logger.info(f"💾 Progress saved ({i+1} processed)")
        time.sleep(0.6)

    update_job_status(status="complete", success=success, errors=errors, total=total)
    logger.info(f"🏁 Run complete — Success: {success}, Errors: {errors}")

# ---------------------------------------------------------------------
# Unified /run-all
# ---------------------------------------------------------------------

@app.post("/run-all")
def run_all(form_id: str = DEFAULT_FORM_ID, start_email: Optional[str] = None, limit: int = 700):
    """Two-mode runner:
    - No start_email → Fetch + Dedupe + Export CSV (no HubSpot updates)
    - With start_email → Load uploaded deduped file + Recover
    """
    def background_job():
        try:
            if start_email:
                logger.info(f"🔁 Resume mode using uploaded file {UPLOADED_DEDUPED_PATH}")
                if not os.path.exists(UPLOADED_DEDUPED_PATH):
                    raise FileNotFoundError("Uploaded deduped file not found in /etc/secrets/")
                with open(UPLOADED_DEDUPED_PATH, "r", encoding="utf-8") as f:
                    subs = [json.loads(line) for line in f]
                recover_contacts(start_email, limit, subs)
            else:
                logger.info("🧾 Prep mode: fetching and deduping submissions...")
                fetch_submissions(form_id)
                subs = dedupe_submissions()
                logger.info("📤 Exporting deduped CSV for download...")
                with open(LATEST_EXPORT, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["email", "consent_terms", "consent_marketing"])
                    for s in subs:
                        email, consent = parse_submission(s)
                        writer.writerow([
                            email or "",
                            consent.get("i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", ""),
                            consent.get("select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information", "")
                        ])
                logger.info(f"✅ CSV exported to {LATEST_EXPORT}")
        except Exception as e:
            logger.error(f"💥 Background job failed: {e}")
            update_job_status(status="error", message=str(e))

    threading.Thread(target=background_job, daemon=True).start()

    if not start_email:
        return {
            "status": "started",
            "mode": "prep",
            "message": "Fetch + dedupe + export running in background.",
            "download_link": "/download-latest"
        }
    return {
        "status": "started",
        "mode": "resume",
        "message": f"Recovery job running from {start_email}"
    }

@app.get("/download-latest")
def download_latest():
    if not os.path.exists(LATEST_EXPORT):
        raise HTTPException(status_code=404, detail="No deduped CSV found yet.")
    return FileResponse(LATEST_EXPORT, filename="deduped_submissions.csv", media_type="text/csv")

@app.get("/run-report")
def run_report():
    job = read_job_status()
    return {"job_status": job}

@app.get("/health")
def health():
    return {"status": "ok", "mode": "unified"}

# ---------------------------------------------------------------------
# Run locally
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
