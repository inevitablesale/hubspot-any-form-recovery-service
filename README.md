# HubSpot Registration Form Recovery Automation

This guide walks through building a Zapier automation that recovers consent preferences from HubSpot form submissions and syncs them to the corresponding contact records. The automation runs every weekday morning, fetches the most recent submissions from the `#registerForm`, parses the consent checkboxes, and updates the matching HubSpot contacts.

---

## Prerequisites

- A Zapier account with access to the **Schedule by Zapier**, **Looping by Zapier**, **Formatter by Zapier**, and **Code by Zapier** apps
- HubSpot account credentials with API access and permission to read form submissions and update contacts
- The HubSpot form ID `4750ad3c-bf26-4378-80f6-e7937821533f` (the `#registerForm`)

---

## High-Level Flow

1. Schedule the Zap to run every weekday at 9:00 AM.
2. Retrieve up to 500 submissions for the registration form via the HubSpot Forms API.
3. Loop over each submission returned.
4. Extract the submitter’s email address.
5. Parse the checkbox fields to determine consent values.
6. Locate the HubSpot contact that matches the extracted email.
7. Update the contact’s consent properties with the parsed values.

The Zap never creates new contacts; it only updates consent fields on existing records.

---

## Step-by-Step Build Instructions

### 1. Schedule Trigger
- **App**: Schedule by Zapier
- **Event**: *Every Day*
- **Configuration**:
  - Time: `9:00 AM`
  - Choose "*Weekdays*" so the Zap runs Monday–Friday only

Purpose: ensures the recovery process runs once per business day during working hours.

### 2. Fetch Form Submissions
- **App**: HubSpot API Request (Beta)
- **Event**: *Custom Request*
- **Configuration**:
  - Method: `GET`
  - URL: `/form-integrations/v1/submissions/forms/4750ad3c-bf26-4378-80f6-e7937821533f`
  - Query String Parameters: `limit = 500`

Purpose: retrieves up to 500 submissions for the registration form in a single run. The response body contains the `results` array used in later steps.

### 3. Loop Through Submissions
- **App**: Looping by Zapier
- **Event**: *Create Loop From Line Items*
- **Configuration**:
  - Loop Values: `{{HubSpot API Request → results}}`
  - Max Iterations: `500`

Purpose: processes each submission individually to keep contact lookups and updates targeted.

### 4. Extract the Email Address
- **App**: Formatter by Zapier
- **Event**: *Text → Extract Email Address*
- **Input**: `{{Loop → submission}}`

Purpose: isolates the email string from the submission payload so it can be used in the HubSpot contact search.

### 5. Parse Checkbox Values (Python Code)
- **App**: Code by Zapier
- **Event**: *Run Python*
- **Input**:
  - Pass the entire loop item JSON (e.g., `submission`) as an input variable.
- **Sample Code**:

  ```python
  import json

  submission = json.loads(input_data["submission"])

  checkbox_fields = {
      "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy": "Portal Terms Accepted",
      "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information": "Marketing Opt-In (VRM Properties)",
  }

  output = {
      "Portal Terms Accepted": "Not Checked",
      "Marketing Opt-In (VRM Properties)": "Not Checked",
  }

  for value in submission.get("values", []):
      name = value.get("name")
      if name in checkbox_fields:
          label = checkbox_fields[name]
          output[label] = "Checked" if value.get("value") == "Checked" else "Not Checked"

  return output
  ```

Purpose: converts the checkbox data from the submission into clean "Checked" / "Not Checked" strings without hard-coded defaults beyond the two relevant fields.

### 6. Find the Contact by Email
- **App**: HubSpot
- **Event**: *Find Contact*
- **Configuration**:
  - Search Property: `email`
  - Search Value: `{{Formatter → Email Address}}`
  - Success on Miss: `False`
  - Multiple Matches: `First`

Purpose: ensures the Zap only proceeds for existing contacts. If no match is found the loop iteration ends without attempting an update.

### 7. Update Contact Consent Fields
- **App**: HubSpot
- **Event**: *Update Contact*
- **Configuration**:
  - Contact ID: `{{Find Contact → Record ID}}`
  - Properties:
    - `portal_terms_accepted`: `{{Code → Portal Terms Accepted}}`
    - `marketing_opt_in_vrm_properties`: `{{Code → Marketing Opt-In (VRM Properties)}}`

Purpose: writes the parsed checkbox states back to the contact record while leaving all other properties untouched.

---

## Safeguards and Best Practices

- **No new contacts**: The Zap updates only if a matching contact exists, preventing duplicate records.
- **Minimal scope updates**: Only the consent fields are updated to avoid overwriting other profile data.
- **Daily batch window**: Capping the loop at 500 items respects HubSpot rate limits while covering the day’s submissions.
- **Error visibility**: Failed API calls stop the Zap, making it clear when manual intervention is needed.

---

## Form Response Structure Reference

HubSpot returns each submission in the following format:

```json
{
  "values": [
    {"name": "email", "value": "contact@example.com"},
    {"name": "firstname", "value": "John"},
    {"name": "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", "value": "Checked"},
    {"name": "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information", "value": "Not Checked"}
  ]
}
```

Use this structure to confirm the correct field names and values in the parsing step.

---

## Maintenance Tips

- Periodically review submission volume to confirm the 500-item limit is sufficient.
- Monitor HubSpot property API names in case they change during portal configuration updates.
- Log Zap runs and review error notifications to catch issues quickly.
- Test the Zap after any HubSpot form or property changes to ensure the parsing logic still aligns with the payload.

This automation offers a reliable recovery mechanism to keep HubSpot consent preferences aligned with the latest form submissions.
