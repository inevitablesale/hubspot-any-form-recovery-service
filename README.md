# HubSpot Any Form Recovery Service

This FastAPI service fetches historical HubSpot form submissions and replays the field values back to CRM contacts. It was built to rescue submissions from multiple legacy marketing forms where contacts failed to receive the proper lifecycle or consent updates.

The worker only performs actions when you call the `/run-all` webhook, making it ideal for one-off data repair jobs that you can host on [Render](https://render.com/), Fly.io, or any platform that supports containers and cron jobs.

---

## Features

- **Multi-form property mapping** – Supply a JSON map of `formId → {formField: hubspotProperty}` via the `HUBSPOT_FORM_PROPERTY_MAP` environment variable and the service will iterate each form automatically.
- **Two execution modes** – `smoke` mode reads submissions, finds contacts, and logs the data without performing any CRM updates. `write` mode repeats the smoke flow but only patches the mapped properties that exist in each submission.
- **Contact safety** – Updates are scoped to the mapped fields and require an exact email match. Missing contacts are skipped and noted in the logs.
- **Structured logging** – Stream + rotating-file logging make it easy to watch progress locally or when deployed to a host like Render.
- **Health endpoint** – `/health` returns the configured form IDs so you can verify the service booted with the expected configuration.

---

## Prerequisites & Configuration

- Python 3.10+
- HubSpot private app token that can read form submissions and update contacts

| Variable | Required | Description |
| --- | --- | --- |
| `HUBSPOT_PRIVATE_APP_TOKEN` | ✅ | Private app token used for all HubSpot API calls. |
| `HUBSPOT_FORM_PROPERTY_MAP` | ✅ | JSON string that defines which form fields map to which HubSpot contact properties. |
| `HUBSPOT_BASE_URL` | ❌ | Override HubSpot's base URL (defaults to `https://api.hubapi.com`). |
| `LOG_FILE` | ❌ | Custom log file path for the rotating file handler (defaults to `recovery.log`). |
| `PORT` | ❌ | Port used when running `uvicorn` locally (defaults to `8000`). |

Example `HUBSPOT_FORM_PROPERTY_MAP` payload:

```json
{
  "123abc-form-id": {
    "email": "email",
    "lifecyclestage": "lifecyclestage",
    "custom_checkbox": "custom_checkbox_property"
  },
  "another-form": {
    "email": "email",
    "company": "company",
    "job_title": "jobtitle"
  }
}
```

Place these values in a `.env` file for local development (the app loads it with `python-dotenv`).

---

## Running Locally

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file or export the required environment variables:

   ```bash
   export HUBSPOT_PRIVATE_APP_TOKEN="your-private-app-token"
   export HUBSPOT_FORM_PROPERTY_MAP='{"form_id":{"email":"email"}}'
   ```

3. Start the FastAPI app with Uvicorn:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

4. Trigger the worker by calling `/run-all`:

   ```bash
   curl -X POST http://localhost:8000/run-all -H "Content-Type: application/json" -d '{"mode":"smoke"}'
   curl -X POST http://localhost:8000/run-all -H "Content-Type: application/json" -d '{"mode":"write"}'
   ```

   Smoke mode logs the submissions and the contact IDs it found. Write mode repeats the smoke flow but also patches the mapped properties for each matching contact.

5. Check `/health` to ensure the service is online and has read the form IDs:

   ```bash
   curl http://localhost:8000/health
   ```

---

## API Endpoints

### `POST /run-all`

Body:

```json
{
  "mode": "smoke" | "write"
}
```

- `smoke` (default) fetches each configured form, prints a submission summary, and never calls the CRM update endpoint.
- `write` performs the same iteration but patches only the mapped properties that are present in each submission.

The endpoint returns `{ "status": "complete", "mode": "smoke" }` (or `write`) when the run finishes.

### `GET /health`

Returns `{ "status": "ok", "forms": ["form-id", ...] }`.

---

## Deployment Notes

1. Create a new web service in your preferred host (Render, Fly.io, etc.).
2. Set the start command to:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port $PORT
   ```

3. Add the required environment variables in the hosting dashboard.
4. Trigger the job manually (curl, workflow automation, cron) whenever you need to re-sync the historic submissions.

---

## Repository Structure

```
main.py          # FastAPI app entry point and HubSpot recovery logic
requirements.txt # Python dependencies
README.md        # Documentation for running and deploying the worker
```

This lightweight setup lets you replay multiple form submissions safely without relying on Zapier or other automation tools.
