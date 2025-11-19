HubSpot Form Recovery Service
=============================

A lightweight FastAPI tool for replaying historical HubSpot form submissions into CRM contacts.

This service is designed for situations where older forms were missing mappings, properties were added later, or submissions were collected before CRM syncing was fully configured. It fetches historical submissions, extracts only the fields you explicitly allow, and writes them back to matching contacts safely and predictably.

The worker runs only when triggered via `/run-all`, making it suitable for controlled, manual repair jobs or automated maintenance tasks on platforms like Render, Fly.io, or any container host.

Features
--------

✔ **Multi-form support**

Pass a single JSON object mapping `formId → {formField: hubspotProperty}`. The service automatically loops through each form and uses only the fields you define.

✔ **Two modes**

| Mode | Description |
| --- | --- |
| `smoke` | Fetch submissions, find matching contacts, log a short summary. No updates are made. |
| `write` | Same as smoke, but updates the mapped HubSpot properties for each contact. |

✔ **Safe by design**

- Only explicitly mapped fields are ever updated.
- Only contacts with an exact email match are touched.
- Missing contacts are skipped without errors.
- No workflow triggers or automations required.

✔ **Clear, structured logging**

All activity is written to both stdout and a rotating file log for easy review.

✔ **Simple health check**

`/health` returns the parsed form IDs to confirm configuration is correct.

Configuration
-------------

| Environment Variable | Required | Description |
| --- | --- | --- |
| `HUBSPOT_PRIVATE_APP_TOKEN` | ✅ | Token for reading submissions and updating contacts. |
| `HUBSPOT_FORM_PROPERTY_MAP` | ✅ | JSON object mapping form IDs → field/property pairs. |
| `HUBSPOT_BASE_URL` | ❌ | Override HubSpot API base URL (optional). |
| `LOG_FILE` | ❌ | Custom log path (defaults to `recovery.log`). |
| `PORT` | ❌ | Local port (defaults to `8000`). |

Example (generic):

```json
{
  "form-id-1": {
    "email": "email",
    "custom_field": "hs_custom_property"
  },
  "form-id-2": {
    "email": "email",
    "another_field": "another_property"
  }
}
```

Running Locally
---------------

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**

   Example `.env`:

   ```bash
   HUBSPOT_PRIVATE_APP_TOKEN=xxxx
   HUBSPOT_FORM_PROPERTY_MAP={"form-id":{"email":"email"}}
   ```

3. **Start the server**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

4. **Trigger a run**

   Smoke test:
   ```bash
   curl -X POST http://localhost:8000/run-all -d '{"mode":"smoke"}' -H "Content-Type: application/json"
   ```

   Write mode:
   ```bash
   curl -X POST http://localhost:8000/run-all -d '{"mode":"write"}' -H "Content-Type: application/json"
   ```

5. **Check health**
   ```bash
   curl http://localhost:8000/health
   ```

Endpoints
---------

### `POST /run-all`

Body:

```json
{
  "mode": "smoke" | "write"
}
```

- `smoke` → fetch, inspect, log.
- `write` → fetch, inspect, update mapped properties.

### `GET /health`

Returns:

```json
{
  "status": "ok",
  "forms": ["form-id-1", "form-id-2", ...]
}
```

Project Structure
-----------------

```
main.py          # FastAPI app
requirements.txt # Dependencies
README.md        # This file
```
