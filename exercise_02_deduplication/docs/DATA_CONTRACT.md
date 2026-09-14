# Data contract and resolution rules

This is an educational contract for a synthetic source, not a claim about a commercial API.
Every source event is a **complete response snapshot**. An omitted child in a later complete
snapshot was removed; an omitted/null child array is unknown and causes quarantine.

## Grains and keys

| Dataset | One row represents | Key |
| --- | --- | --- |
| Bronze delivery | One receipt of an event | `receipt_id` |
| Candidate snapshot | One semantic response version | `survey_id, response_id, source_version` |
| Current response | Latest accepted version of one survey response | `survey_id, response_id` |
| Response–category bridge | One current classification pair assigned to a response | `survey_id, response_id, category, subcategory` |
| Question answers | One current additional question within a response | `survey_id, response_id, question_id` |
| Wide output | One current response, with selected question columns | `survey_id, response_id` |

The exercise assumes one source and tenant. A real multi-source ingestion must add source/tenant
to every key. Customer ID is never a response key: one person can answer different invitations.
Two categories sharing a main label but having different subcategories remain distinct.
The labels are cleaned but case-sensitive; synonym mapping requires a separately governed taxonomy.

## Source-to-model mapping

| Synthetic payload | Model | Interpretation |
| --- | --- | --- |
| `_id` | `response_id` | Response identifier |
| `actionId` | `survey_id` | Survey scope in this exercise |
| `clientId` | `customer_id` | Customer identifier; may repeat |
| `review`, `metric` | `score`, `metric` | Main question score; NPS 0–10, CSAT 1–5 |
| `answerDate`, `createdAt`, `updatedAt` | UTC timestamp columns | Answer, creation and source update time |
| `version` | `source_version` | **Synthetic authoritative revision counter**, not present in the supplied historical scripts |
| `eventId` | `event_id` | **Synthetic event identity**, used to demonstrate transport consistency |
| `categories[]` | `response_categories` | All classification/subclassification pairs |
| `additionalQuestions[]._id` | `question_id` | Stable question identity; wording is descriptive |
| Question `review`, `answerText`, `multipleValues` | `value_json` | First nonempty value, preserving type |
| `deleted` | Current tombstone | Suppresses the response and its children from active outputs |
| `treatments`, `indicators`, `inviteId`, `text`, `date` | Bronze only | Context retained; not part of the analytical state in this exercise |

Transport fields (`receipt_id`, `batch_id`, `page`, `position`, `received_at`) are generated
outside the payload. Pages are provenance labels, not an HTTP pagination test. Source setup
and consumption remain separate, but Exercise 02 starts from a saved API-shaped JSONL fixture.
Exercise 01 covers HTTP pagination, watermarks and extraction.

## Resolution order

1. Validate identifiers, numeric scale, timestamps, child-array presence and question identities.
2. Normalize Unicode, whitespace, timestamps and typed question values. Zero/false are valid.
   Empty strings, nulls, empty arrays and empty objects do not block the fallback chain.
   Arrays preserve order and item boundaries; they are not concatenated into ambiguous strings.
3. Remove repeated identical children **inside each snapshot**. Different values for the same
   question ID within one snapshot reject that entire snapshot.
4. Hash the normalized modeled business payload, excluding transport and event identity.
   The hash covers the modeled response, questions and classifications, not Bronze-only metadata.
5. If a valid event ID refers to different modeled payloads, quarantine all valid candidates with
   that event ID. If the same response/version has different modeled payloads, quarantine that
   whole version. Invalid payloads are individually quarantined before these comparisons.
6. Collapse equivalent deliveries of the same version. The smallest receipt ID supplies only
   reproducible lineage for equivalent copies; it is never used to settle a business conflict.
7. Choose the highest accepted `source_version` for each response. Arrival time cannot overwrite
   a newer version. A source version can break an `updatedAt` tie under this synthetic contract.
8. Preserve a selected tombstone in retained history, but exclude it and its children from active
   outputs. Otherwise derive **all** current response/child tables from that one chosen snapshot.
9. A response with any rejected history receives `quality_status=has_rejected_history`. If its
   latest revision is invalid/ambiguous, the previous valid snapshot is displayed with this flag.
   This is a chosen last-valid policy, not a guarantee that the source's newest truth is known.

No arbitrary `first`, maximum score, alphabetical category or hash decides conflicting values.
`max(value_json)` in the Spark pivot runs only after uniqueness is established, so it is a
single-value aggregation rather than a conflict resolution rule.

## When adapting a real source

The historical code provides update timestamps but does not establish an authoritative revision
counter or event ID. Do not fabricate their ordering semantics from ingestion time. If only
`updatedAt` is reliable, use it as the version rank and quarantine different payloads tied at the
same source update time; document timestamp precision and late-arrival behavior. An event hash
can identify equal modeled content, but cannot prove chronology. Missing historical event IDs
can be handled using a documented content-deduplication contract instead of inventing source IDs.

Confirm whether the source supplies full snapshots, patches or individually versioned children.
Patches require explicit operation/tombstone semantics and state application. Independently
versioned classifications need their own identity and history. This implementation deliberately
rejects unknown arrays and does not interpret a patch as a full snapshot.

## Pivot policy

The long table is authoritative for questions. `QUESTION_COLUMNS` is a reviewed mapping from
stable IDs to English column names; new IDs remain in the long table until explicitly added to
the wide schema. Text changes never create a new identity. Two question IDs with labels that
sanitize to the same name remain two questions. In this fixture IDs have consistent meanings
across surveys; otherwise the registry must also be survey-scoped.

Wide answer cells contain JSON scalar/array/object strings, with SQL null for no answer. This
keeps arrays and structs reversible. For a later Power BI serving layer, parse selected scalar
questions into typed columns; do not flatten multi-select answers by concatenation. That serving
model belongs to Exercise 05.

## Persistence and publication

The local pipeline ingests only the selected delivery batch into persistent SQLite Bronze, then
recomputes the current model from all retained receipts. A single SQLite transaction publishes
the four related tables. Keys and foreign keys are enforced there. CSV files are exports, not a
transactional serving interface. Exact job retries reuse receipt IDs and do not append; a distinct
source re-delivery gets a new receipt ID and stays visible in Bronze.

The Databricks notebook uses a Bronze Delta MERGE and publishes six datasets into one Delta
snapshot-bundle table with persistent views. One snapshot-table overwrite prevents partial child
replacement across separately written output tables. Bronze and the bundle are not one cross-table
transaction: if publication fails, rerun from retained Bronze. Schema/view changes and concurrent
writers require additional coordination; run one writer for this exercise.

Both implementations recompute retained history for clarity. They do not claim production-scale
incremental transformations. A scalable extension would validate and resolve all retained versions
only for affected response keys, including late conflicts and child deletions.
