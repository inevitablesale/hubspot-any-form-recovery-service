# HubSpot Any-Form Recovery Service

This service replays historical HubSpot form submissions back into CRM contacts using a safe, controlled, overwrite-protected process.

It is built for **data repair**, **backfills**, and **consent/property recovery** when:

- HubSpot forms previously had unmapped fields  
- A private app or Zapier integration failed  
- A form migration caused older submissions to be ignored  
- Multiple forms feed different HubSpot properties  

The service can run safely in **smoke mode** (dry-run) or **write mode**, and always protects existing CRM data.

---

## 🔑 Key Features

### **✔ Multi-form property recovery**
The worker is controlled by a single environment variable:

`HUBSPOT_FORM_PROPERTY_MAP=<json>`

Structure:

```json
{
  "form_id_1": {
    "formFieldA": "hubspot_property_x",
    "formFieldB": "hubspot_property_y"
  },
  "form_id_2": {
    "another_form_field": "another_hubspot_property"
  }
}
```

The app automatically:

- Iterates each form ID
- Fetches submissions with correct HubSpot pagination
- Attempts updates only for mapped fields

### ✔ Smoke Mode (dry-run)
Reads submissions → looks up contacts → logs results.

No CRM changes are made.

Useful for:

- Validating mapping accuracy
- Reviewing what would be updated
- Confirming contact matches

### ✔ Write Mode (safe recovery)
Performs everything smoke mode does, plus applies updates—but only when safe.

Write mode:

- Updates only mapped fields
- Updates only fields present in the submission
- Does not overwrite any existing HubSpot property
- Requires that the HubSpot contact is found
- Applies per-field overwrite-protection

### ✔ Mandatory Overwrite Protection
A property is updated only if all are true:

1. The form submission includes the field
2. The field is mapped in the JSON
3. The HubSpot contact’s matching property is empty (null, empty string, or whitespace)
4. The contact exists in CRM

This ensures a no overwrite guarantee.

### ✔ Global DRY RUN override
Even if someone posts:

```json
{"mode":"write"}
```

You can force the API to always run smoke mode using:

```ini
DRY_RUN_FORCE=true
```

This provides production-grade safety.

### ✔ HubSpot Rate-Limit Heuristics
The worker dynamically throttles itself based on HubSpot response headers:

| Remaining calls | Behavior |
| --- | --- |
| < 10 | Sleep 2s |
| < 5 | Sleep 4s |
| `Retry-After` | Sleep per header |
| Always | Add jitter (0.1–0.3s) |

This ensures reliable long-running backfills.

### ✔ One-line JSON logs
All logs are emitted as valid single-line JSON records:

```json
{"event":"submission_processed","form":"abc","email":"john@example.com","updates_count":2}
```

Ideal for:

- Datadog
- ELK
- Cloud logging
- `grep` / `jq`
- Render logs

### ✔ Contact Lookup Once Per Submission
The contact lookup (by email) is performed only once per submission, not per field.

This reduces API load and keeps logs clean.

## 🧩 Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `HUBSPOT_PRIVATE_APP_TOKEN` | ✅ | Token w/ forms + contacts scope |
| `HUBSPOT_FORM_PROPERTY_MAP` | ✅ | JSON mapping of form → fields |
| `DRY_RUN_FORCE` | ❌ | "true" forces smoke mode |
| `HUBSPOT_BASE_URL` | ❌ | Defaults to HubSpot API |

Example:

```bash
export HUBSPOT_PRIVATE_APP_TOKEN="xxx"
export HUBSPOT_FORM_PROPERTY_MAP='{"formA":{"fieldA":"propA"}}'
export DRY_RUN_FORCE=false
```

## 🚀 Running Locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the service:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Run smoke mode:

```bash
curl -X POST http://localhost:8000/run-all \
    -H "Content-Type: application/json" \
    -d '{"mode":"smoke"}'
```

Run write mode:

```bash
curl -X POST http://localhost:8000/run-all \
    -H "Content-Type: application/json" \
    -d '{"mode":"write"}'
```

(If `DRY_RUN_FORCE=true`, it still runs smoke mode.)

## 🌡 Health Check

```bash
curl http://localhost:8000/health
```

Example output:

```json
{
  "status": "ok",
  "forms": ["form_id_1", "form_id_2"]
}
```

## 🧪 Example Log Output

```json
{"event":"submission_processed","form":"xyz","email":"jane@example.com","contact_id":"123","updates_count":1}
```

Other events include:

- `start_form`
- `end_form`
- `contact_not_found`
- `skip_no_email`
- `dry_run_forced`

## 📦 Deploy Anywhere

Compatible with:

- Render
- Fly.io
- Railway
- Docker containers
- Bare metal servers
- Cloud VMs

Start command:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Trigger `/run-all` manually or with a scheduled job.

## 🧱 File Structure

```bash
main.py            # Full service
requirements.txt   # Minimal deps
README.md          # This document
```

## 🎯 Summary

This worker provides:

- Zero-overwrite safety
- Multiple-form recovery
- Dynamic rate-limit control
- Structured JSON logs
- Dry-run protection
- Predictable, linear processing

Perfect for historical HubSpot form backfills and data repair.
