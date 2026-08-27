# Synthetic data dictionary

The source contains fictional customer-experience survey events. One response may appear more than once because the API can receive a later revision. A small number of intentionally invalid records is included to make the validation and quarantine steps observable.

| Field | Type | Description |
| --- | --- | --- |
| `response_id` | string | Stable identifier of the survey response |
| `customer_id` | string | Fictional customer identifier |
| `journey_stage` | string | Journey stage: onboarding, service, support or renewal |
| `responded_at` | timestamp | Time when the survey was answered |
| `updated_at` | timestamp | Time when that event version became available in the source |
| `event_version` | integer | Version of the source event |
| `nps_score` | integer | NPS response on the valid 0 to 10 scale |
| `csat_score` | integer | CSAT response on the valid 1 to 5 scale |
| `category` | string | Fictional reason category associated with the journey stage |
| `comment` | string | Synthetic free-text response |

The generation seed is fixed at `20260826`. The source has 6,000 unique response IDs, later revisions for every seventeenth response and an invalid NPS event for every 233rd response.
