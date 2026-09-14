# Output dictionary

See [the data contract](../docs/DATA_CONTRACT.md) for source fields, keys and semantics.

| Field | Type | Meaning |
| --- | --- | --- |
| `survey_id`, `response_id` | string | Composite response key |
| `customer_id` | string | Customer; repeat responses allowed |
| `source_version` | integer | Authoritative synthetic revision number |
| `updated_at` | UTC ISO timestamp | Source update time, normalized to microseconds |
| `answered_at`, `created_at` | UTC ISO timestamp | Answer and creation times |
| `metric` | NPS / CSAT | Main survey metric |
| `score` | integer | 0–10 for NPS; 1–5 for CSAT |
| `journey_stage` | string | Fictional stage; background uses four stages |
| `feedback` | string or null | Synthetic comment |
| `event_id` | string | Representative source event lineage, not response key |
| `payload_hash` | SHA-256 string | Normalized modeled business content |
| `quality_status` | string | `ok` or `has_rejected_history` |
| `category`, `subcategory` | string | Cleaned classification pair; absent subcategory is empty string |
| `question_id`, `question_text` | string | Stable question ID and display text |
| `value_json` | JSON string or null | Typed scalar, array or object answer; null means unanswered |
| `answer_wait` | JSON string or null | Pivot of `q_wait` |
| `answer_resolution` | JSON string or null | Pivot of `q_resolution` |
| `answer_channels` | JSON string or null | Pivot of `q_channels`; may be an array |
| `answer_wait_alternative` | JSON string or null | Pivot of separate ID `q_wait_alt` |
| `answer_detail` | JSON string or null | Pivot of `q_detail`; may be a struct |
| `category_count` | integer | Number of distinct current classification pairs |

`reports/receipt_audit.csv` assigns exactly one status to every retained receipt. Possible statuses:
`quarantined`, `repeated_snapshot`, `superseded_version`, `selected_current`, `deleted_current`.
`reports/quarantine.jsonl` keeps the raw synthetic payload and rejection reason.

The full-fixture diagnostics use only the latest valid active snapshots, deliberately isolating
join fan-out from replays, old versions and rejected events. `legacy_join_latest.csv` has 4,479
rows for 1,215 active responses. The LinkedIn example is the explicit three-response subset
`survey-demo / R001–R003`, with eight joined rows. These are different samples.
