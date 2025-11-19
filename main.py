import os
import json
import time
import csv
import logging
from typing import Dict, Any, Optional, List, Tuple

import requests
from fastapi import FastAPI, Body, Header, HTTPException, Depends
from fastapi.responses import JSONResponse, FileResponse
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# App Metadata
# ---------------------------------------------------------------------------

APP_VERSION = "3.0.0"

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------

HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
FORM_PROPERTY_MAP_RAW = os.getenv("HUBSPOT_FORM_PROPERTY_MAP")
HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DRY_RUN_FORCE = os.getenv("DRY_RUN_FORCE", "false").lower() == "true"
APP_AUTH_TOKEN = os.getenv("APP_AUTH_TOKEN")
PREPARED_DIR = os.getenv("PREPARED_DIR", "/data/prepared")

if not HUBSPOT_TOKEN:
    raise Exception("HUBSPOT_PRIVATE_APP_TOKEN is required.")
if not FORM_PROPERTY_MAP_RAW:
    raise Exception("HUBSPOT_FORM_PROPERTY_MAP is required.")
if not APP_AUTH_TOKEN:
    raise Exception("APP_AUTH_TOKEN is required for API security.")

FORM_PROPERTY_MAP: Dict[str, Dict[str, str]] = json.loads(FORM_PROPERTY_MAP_RAW)

# ---------------------------------------------------------------------------
# Logging Setup — One-line JSON logs (stdout for Render)
# ---------------------------------------------------------------------------

logger = logging.getLogger("recovery")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()             # send logs to stdout
formatter = logging.Formatter("%(message)s")  # one-line JSON logs
handler.setFormatter(formatter)

logger.handlers = [handler]  # ensure no duplicate handlers


def log_json(event: str, **kwargs) -> None:
    record = {"event": event, **kwargs}
    logger.info(json.dumps(record))


os.makedirs(PREPARED_DIR, exist_ok=True)

log_json(
    "service_start",
    version=APP_VERSION,
    dry_run_force=DRY_RUN_FORCE,
    forms_loaded=list(FORM_PROPERTY_MAP.keys()),
    prepared_dir=PREPARED_DIR,
)

# ---------------------------------------------------------------------------
# Auth Helper (for API headers, not CSV download)
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
            status_code=403,
            detail="Invalid Authorization header format.",
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
    """Dynamic slowdown when HubSpot warns of rate limits."""
    remaining = resp_headers.get("X-HubSpot-RateLimit-Remaining")
    retry_after = resp_headers.get("Retry-After")

    if retry_after:
        try:
            sleep_for = int(retry_after)
            log_json("retry_after_header", retry_after=sleep_for)
            time.sleep(sleep_for)
        except Exception:
            pass

    try:
        if remaining is not None:
            r = int(remaining)
            if r < 5:
                time.sleep(4)
            elif r < 10:
                time.sleep(2)
    except Exception:
        pass

    # small jitter for safety
    time.sleep(0.15)


def safe_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    Wrapper around requests.request with basic 429 handling and logging.
    """
    max_retries = 5
    attempt = 0

    while True:
        attempt += 1
        resp = requests.request(method.upper(), url, **kwargs)

        # Explicit rate-limit hit
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "3")
            try:
                retry_secs = int(retry_after)
            except ValueError:
                retry_secs = 3

            log_json(
                "rate_limit_hit",
                url=url,
                attempt=attempt,
                retry_after=retry_secs,
            )
            if attempt > max_retries:
                log_json(
                    "rate_limit_give_up",
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                )
                resp.raise_for_status()
                return resp

            time.sleep(retry_secs)
            continue

        # Other errors
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            log_json(
                "request_error",
                url=url,
                status=resp.status_code,
                error=str(exc),
                body=getattr(resp, "text", None),
            )
            raise

        # Successful
        apply_rate_limit_heuristics(resp.headers)
        return resp


def fetch_form_submissions(
    form_id: str,
    after: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Fetch submissions using the form-integrations API.

    NOTE: HubSpot's max limit for this endpoint is 50.
    We hard-code limit=50 to avoid 400 Bad Request responses.
    """
    url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}"

    hubspot_limit = 50
    params: Dict[str, Any] = {"limit": hubspot_limit}
    if after:
        params["after"] = after

    resp = safe_request("get", url, headers=hubspot_headers(), params=params)
    data = resp.json()
    results = data.get("results", [])

    # Normalize after token across all HubSpot paging formats
    next_after = None

    # structure #1 { "paging": { "next": { "after": "abc" } } }
    if isinstance(data.get("paging"), dict):
        next_after = data.get("paging", {}).get("next", {}).get("after")

    # structure #2 { "next": { "after": "abc" } }
    if not next_after and isinstance(data.get("next"), dict):
        next_after = data.get("next", {}).get("after")

    # structure #3 { "next": "abc" }
    if not next_after and isinstance(data.get("next"), str):
        next_after = data.get("next")

    log_json(
        "form_page_fetched",
        form=form_id,
        after=after,
        next_after=next_after,
        results_count=len(results),
    )

    return {"results": results, "after": next_after}


def fetch_all_submissions_for_form(form_id: str) -> List[Dict[str, Any]]:
    """
    Fetch ALL submissions for a form (newest → oldest) by paging through the API.
    """
    all_results: List[Dict[str, Any]] = []
    after: Optional[str] = None

    while True:
        page = fetch_form_submissions(form_id, after=after)
        results = page.get("results", [])
        next_after = page.get("after")

        if not results:
            break

        all_results.extend(results)

        if not next_after:
            break

        after = next_after

    log_json(
        "form_all_submissions_fetched",
        form=form_id,
        total=len(all_results),
    )
    return all_results


def get_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"],
        "limit": 1,
    }

    resp = safe_request("post", url, headers=hubspot_headers(), json=body)
    data = resp.json()

    return data["results"][0] if data.get("results") else None


def update_contact(contact_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    resp = safe_request(
        "patch",
        url,
        headers=hubspot_headers(),
        json={"properties": properties},
    )
    return resp.json()


# ---------------------------------------------------------------------------
# Core Helpers
# ---------------------------------------------------------------------------


def extract_submission_email_and_fields(
    submission: Dict[str, Any]
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Extract email and form field values from a raw submission object.
    We only care about the 'values' array for your case.
    """
    submitted_values = submission.get("values", [])
    email = None
    submission_fields: Dict[str, Any] = {}

    for f in submitted_values:
        name = f.get("name")
        val = f.get("value")
        if name:
            submission_fields[name] = val
        if name == "email":
            email = val

    return email, submission_fields


def compute_updates_for_submission(
    form_id: str,
    submission_fields: Dict[str, Any],
    contact: Dict[str, Any],
) -> Dict[str, Any]:
    existing_props = contact.get("properties", {}) or {}
    map_for_form = FORM_PROPERTY_MAP.get(form_id, {})
    updates: Dict[str, Any] = {}

    for form_field, hubspot_prop in map_for_form.items():
        val = submission_fields.get(form_field)
        if val is None:
            continue
        existing_val = existing_props.get(hubspot_prop)
        if existing_val not in (None, "", " "):
            continue
        updates[hubspot_prop] = val

    return updates


def dedupe_submissions_newest_first(
    form_id: str,
    submissions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Given a list of submissions (newest → oldest), keep only the newest
    submission per email address.
    Returns a list of dicts:
      { "email": str, "submission_fields": { ... } }
    ordered newest → oldest (by first-seen).
    """
    seen_emails = set()
    deduped: List[Dict[str, Any]] = []

    for submission in submissions:
        email, fields = extract_submission_email_and_fields(submission)
        if not email:
            continue
        email_lower = email.lower()
        if email_lower in seen_emails:
            continue
        seen_emails.add(email_lower)

        deduped.append(
            {
                "email": email,
                "submission_fields": fields,
            }
        )

    log_json(
        "dedupe_complete",
        form=form_id,
        total=len(submissions),
        deduped=len(deduped),
    )
    return deduped


def save_prepared_json(form_id: str, deduped_list: List[Dict[str, Any]]) -> str:
    os.makedirs(PREPARED_DIR, exist_ok=True)
    json_path = os.path.join(PREPARED_DIR, f"{form_id}.json")
    payload = {
        "form_id": form_id,
        "count": len(deduped_list),
        "items": deduped_list,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    log_json("prepared_json_saved", form=form_id, path=json_path, count=len(deduped_list))
    return json_path


def load_prepared_json(form_id: str) -> Dict[str, Any]:
    json_path = os.path.join(PREPARED_DIR, f"{form_id}.json")
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"No prepared JSON found for form {form_id}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def write_csv(form_id: str, deduped_list: List[Dict[str, Any]]) -> str:
    os.makedirs(PREPARED_DIR, exist_ok=True)
    csv_path = os.path.join(PREPARED_DIR, f"{form_id}.csv")

    # Dynamically collect all submission fields
    fieldnames = ["email"]
    for item in deduped_list:
        for key in item["submission_fields"].keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for item in deduped_list:
            row = {"email": item["email"]}
            row.update(item["submission_fields"])
            writer.writerow(row)

    log_json("csv_written", form=form_id, path=csv_path, rows=len(deduped_list))
    return csv_path


def process_deduped_item(
    form_id: str,
    email: str,
    submission_fields: Dict[str, Any],
    mode: str,
) -> Dict[str, Any]:
    """
    Run the update logic for a single deduped row (email + submission_fields).
    """
    effective_mode = "smoke" if DRY_RUN_FORCE else mode

    contact = get_contact_by_email(email)
    if not contact:
        log_json("contact_not_found", form=form_id, email=email)
        return {
            "email": email,
            "status": "contact_not_found",
            "updates_count": 0,
        }

    contact_id = contact["id"]
    updates = compute_updates_for_submission(form_id, submission_fields, contact)

    log_json(
        "submission_processed",
        form=form_id,
        email=email,
        contact_id=contact_id,
        updates_count=len(updates),
        updates=updates,
        mode=effective_mode,
    )

    if DRY_RUN_FORCE or effective_mode != "write" or not updates:
        if DRY_RUN_FORCE:
            log_json("dry_run_forced", email=email, form=form_id)
        return {
            "email": email,
            "status": "dry_run" if effective_mode == "write" else effective_mode,
            "updates_count": len(updates),
        }

    update_contact(contact_id, updates)
    log_json(
        "contact_updated",
        form=form_id,
        email=email,
        contact_id=contact_id,
        updated_properties=list(updates.keys()),
    )
    return {
        "email": email,
        "status": "updated",
        "updates_count": len(updates),
    }


# ---------------------------------------------------------------------------
# Batch Runner (using prepared JSON)
# ---------------------------------------------------------------------------


def run_batch_for_form(
    form_id: str,
    mode: str,
    offset: int,
    limit: int,
) -> Dict[str, Any]:
    effective_mode = "smoke" if DRY_RUN_FORCE else mode

    data = load_prepared_json(form_id)
    items: List[Dict[str, Any]] = data.get("items", [])
    total = len(items)

    if offset < 0:
        offset = 0
    if limit <= 0:
        limit = 100

    if offset >= total:
        return {
            "status": "complete",
            "form": form_id,
            "mode": effective_mode,
            "processed_count": 0,
            "updated_count": 0,
            "offset": offset,
            "next_offset": None,
            "remaining": 0,
            "total": total,
        }

    end = min(offset + limit, total)
    batch = items[offset:end]

    log_json(
        "batch_start",
        form=form_id,
        mode=effective_mode,
        offset=offset,
        limit=limit,
        batch_size=len(batch),
        total=total,
    )

    processed_count = 0
    updated_count = 0
    not_found_count = 0

    for row in batch:
        email = row["email"]
        submission_fields = row["submission_fields"]

        result = process_deduped_item(form_id, email, submission_fields, mode)
        processed_count += 1

        if result["status"] == "updated":
            updated_count += 1
        elif result["status"] == "contact_not_found":
            not_found_count += 1

    next_offset = end if end < total else None
    remaining = max(total - end, 0)

    log_json(
        "batch_complete",
        form=form_id,
        mode=effective_mode,
        processed_count=processed_count,
        updated_count=updated_count,
        not_found_count=not_found_count,
        offset=offset,
        next_offset=next_offset,
        remaining=remaining,
        total=total,
    )

    return {
        "status": "complete",
        "form": form_id,
        "mode": effective_mode,
        "processed_count": processed_count,
        "updated_count": updated_count,
        "not_found_count": not_found_count,
        "offset": offset,
        "next_offset": next_offset,
        "remaining": remaining,
        "total": total,
    }


def run_latest_for_email_live(
    form_id: str,
    email: str,
    mode: str,
) -> Dict[str, Any]:
    """
    For testing: scan the form submissions live, find the *latest* submission
    for the given email (newest → oldest), and process exactly that one.
    Stops scanning as soon as the first match is found.
    """
    effective_mode = "smoke" if DRY_RUN_FORCE else mode
    email_lower = email.lower()

    log_json(
        "run_email_live_start",
        form=form_id,
        email=email,
        mode=effective_mode,
    )

    after: Optional[str] = None
    latest_fields: Optional[Dict[str, Any]] = None

    while True:
        page = fetch_form_submissions(form_id, after=after)
        results = page.get("results", [])
        next_after = page.get("after")

        if not results:
            break

        for submission in results:
            sub_email, fields = extract_submission_email_and_fields(submission)
            if not sub_email:
                continue
            if sub_email.lower() == email_lower:
                latest_fields = fields
                break

        if latest_fields is not None:
            break

        if not next_after:
            break

        after = next_after

    if latest_fields is None:
        log_json("run_email_live_no_submission_found", form=form_id, email=email)
        return {
            "status": "no_submission_found",
            "form": form_id,
            "email": email,
            "mode": effective_mode,
        }

    result = process_deduped_item(form_id, email, latest_fields, mode)
    log_json(
        "run_email_live_complete",
        form=form_id,
        email=email,
        mode=effective_mode,
        result=result,
    )
    return {
        "status": "complete",
        "form": form_id,
        "email": email,
        "mode": effective_mode,
        "item": result,
    }


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="HubSpot Form Submission Recovery Service",
    version=APP_VERSION,
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "forms": list(FORM_PROPERTY_MAP.keys()),
        "dry_run_force": DRY_RUN_FORCE,
        "prepared_dir": PREPARED_DIR,
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


# ---------------------------------------------------------------------------
# Prepare Run (fetch all, dedupe, save JSON + CSV)
# ---------------------------------------------------------------------------


@app.post("/prepare-run/{form_id}")
def prepare_run(
    form_id: str,
    _: bool = Depends(require_auth),
):
    """
    Fetch all submissions for a form, dedupe by email (keeping newest),
    save to JSON + CSV (newest-first).
    """
    if KILLED:
        log_json("prepare_run_blocked_killed", form=form_id)
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."},
        )

    if form_id not in FORM_PROPERTY_MAP:
        log_json("prepare_run_form_not_found", form=form_id)
        return JSONResponse(
            status_code=404,
            content={"error": f"Form '{form_id}' not found in HUBSPOT_FORM_PROPERTY_MAP."},
        )

    log_json("prepare_run_start", form=form_id)

    submissions = fetch_all_submissions_for_form(form_id)
    deduped = dedupe_submissions_newest_first(form_id, submissions)

    json_path = save_prepared_json(form_id, deduped)
    csv_path = write_csv(form_id, deduped)

    log_json(
        "prepare_run_complete",
        form=form_id,
        json_path=json_path,
        csv_path=csv_path,
        count=len(deduped),
    )

    return {
        "status": "prepared",
        "form": form_id,
        "count": len(deduped),
        "json_path": json_path,
        "csv_available": True,
    }


# ---------------------------------------------------------------------------
# CSV Download (browser-friendly, token query required)
# ---------------------------------------------------------------------------


@app.get("/download/{form_id}.csv")
def download_csv(form_id: str, token: Optional[str] = None):
    """
    Public-ish endpoint: Returns the deduped CSV for this form.
    Requires ?token=<APP_AUTH_TOKEN> query param.
    No Authorization header needed so it works in the browser.
    """
    if token != APP_AUTH_TOKEN:
        log_json("csv_download_auth_failed", form=form_id, provided_token=token)
        return JSONResponse(
            status_code=403,
            content={"error": "Invalid or missing token"},
        )

    csv_path = os.path.join(PREPARED_DIR, f"{form_id}.csv")

    if not os.path.exists(csv_path):
        log_json("csv_download_not_found", form=form_id, path=csv_path)
        return JSONResponse(
            status_code=404,
            content={"error": f"No CSV found for form {form_id}. Run /prepare-run first."},
        )

    log_json("csv_download_success", form=form_id, path=csv_path)
    return FileResponse(
        path=csv_path,
        media_type="text/csv",
        filename=f"{form_id}.csv",
    )


# ---------------------------------------------------------------------------
# Batch Runner Endpoint (using prepared JSON)
# ---------------------------------------------------------------------------


@app.post("/run-form/{form_id}/batch")
def run_form_batch(
    form_id: str,
    body: Dict[str, Any] = Body(...),
    offset: int = 0,
    limit: int = 200,
    _: bool = Depends(require_auth),
):
    """
    Run a batch for a single form using the PREPARED deduped data.
    Example: POST /run-form/{form_id}/batch?offset=0&limit=200
    Body: { "mode": "smoke" | "write" }
    """
    if KILLED:
        log_json("run_batch_blocked_killed", form=form_id)
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."},
        )

    if form_id not in FORM_PROPERTY_MAP:
        log_json("run_batch_form_not_found", form=form_id)
        return JSONResponse(
            status_code=404,
            content={"error": f"Form '{form_id}' not found in HUBSPOT_FORM_PROPERTY_MAP."},
        )

    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid mode. Use 'smoke' or 'write'."},
        )

    try:
        summary = run_batch_for_form(
            form_id=form_id,
            mode=mode,
            offset=offset,
            limit=limit,
        )
    except FileNotFoundError:
        return JSONResponse(
            status_code=404,
            content={
                "error": f"No prepared data for form {form_id}. Run /prepare-run/{form_id} first."
            },
        )

    return summary


# ---------------------------------------------------------------------------
# Single Email Runner (live, latest submission only)
# ---------------------------------------------------------------------------


@app.post("/run-form/{form_id}/email/{email}")
def run_form_for_email(
    form_id: str,
    email: str,
    body: Dict[str, Any] = Body(...),
    _: bool = Depends(require_auth),
):
    """
    Run recovery for a single contact (by email) within a form.
    Fetches form submissions live, finds the *latest* submission for that email,
    and uses that to compute updates.
    Respects DRY_RUN_FORCE.
    """
    if KILLED:
        log_json("run_email_blocked_killed", form=form_id, email=email)
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."},
        )

    if form_id not in FORM_PROPERTY_MAP:
        log_json("run_email_form_not_found", form=form_id, email=email)
        return JSONResponse(
            status_code=404,
            content={"error": f"Form '{form_id}' not found in HUBSPOT_FORM_PROPERTY_MAP."},
        )

    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid mode. Use 'smoke' or 'write'."},
        )

    summary = run_latest_for_email_live(
        form_id=form_id,
        email=email,
        mode=mode,
    )
    return summary


# ---------------------------------------------------------------------------
# Legacy "run-all" (not deduped, best for small forms only)
# ---------------------------------------------------------------------------


def process_submission_streaming(form_id: str, submission: Dict[str, Any], mode: str) -> None:
    """
    Streaming processor used only by /run-all for smaller forms.
    Not deduped; kept for backwards compatibility / diagnostics.
    """
    email, submission_fields = extract_submission_email_and_fields(submission)
    if not email:
        log_json("skip_no_email", form=form_id)
        return

    # Reuse deduped-item logic for consistency
    process_deduped_item(form_id, email, submission_fields, mode)


def run_recovery_streaming(mode: str) -> None:
    """Iterate through all configured forms and repair contact data (no dedupe)."""
    effective_mode = "smoke" if DRY_RUN_FORCE else mode

    for form_id in FORM_PROPERTY_MAP.keys():
        log_json("run_all_start_form", form=form_id, requested_mode=mode, mode=effective_mode)

        after = None
        while True:
            page = fetch_form_submissions(form_id, after=after)
            results = page.get("results", [])
            next_after = page.get("after")

            if not results:
                break

            for submission in results:
                process_submission_streaming(form_id, submission, effective_mode)

            if not next_after:
                break

            after = next_after

        log_json("run_all_end_form", form=form_id, mode=effective_mode)


@app.post("/run-all")
def run_all(body: Dict[str, Any] = Body(...), _: bool = Depends(require_auth)):
    """
    Legacy "run-all" utility. Streams all submissions for all forms
    without dedupe. For large forms, prefer:
      1) POST /prepare-run/{form_id}
      2) POST /run-form/{form_id}/batch?offset=0&limit=200 (repeat)
    """
    if KILLED:
        log_json("run_all_blocked_killed")
        return JSONResponse(
            status_code=403,
            content={"error": "Kill switch active — execution blocked."},
        )

    mode = body.get("mode", "smoke")
    if mode not in ("smoke", "write"):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid mode"},
        )

    log_json("run_all_start", mode=mode, dry_run_force=DRY_RUN_FORCE)
    run_recovery_streaming(mode)

    return {
        "status": "complete",
        "mode": "smoke" if DRY_RUN_FORCE else mode,
    }
