# Exercise 1: full load versus incremental API load

This is the first exercise in the **Customer Experience Analytics and Pipeline** repository. It demonstrates how a pipeline moves from an initial historical extraction to a safe incremental load while preserving validation, replay protection and an analytical export.

Later releases will reuse the resulting dataset for response-grain analysis, NPS and CSAT, claims-ratio exercises, outlier treatment and a Power BI-ready model.

## What belongs to the source simulation

`setup_synthetic_api.py` combines two preparation tasks:

- it generates the deterministic synthetic events when the source file does not exist;
- it exposes those events through an authenticated and paginated local API.

This file stands in for an external source system. Once it is running, the person completing the exercise receives records from the API and does not need to know the data-generation logic to execute the pipeline.

## What belongs to the exercise

Both pipeline versions cover the same sequence without depending on separate project modules:

1. Read the last successful watermark
2. Apply a five-minute overlap
3. Extract every API page
4. Preserve the raw batch
5. Validate required fields and score ranges
6. Quarantine invalid events
7. Retain the newest valid event per response in the batch
8. Upsert the current response state
9. Export or publish the analytical layer
10. Commit the new watermark only after success

`run_pipeline_local.py` implements the sequence with Python, SQLite and CSV. `run_pipeline_databricks.py` implements it with Python, Spark, Delta tables and `MERGE`.

## Local reproduction

Python 3.11 or newer is sufficient. No third-party package is required.

From this folder, run the tests:

```bash
python -m unittest discover -s tests -v
```

Open a first terminal and start the synthetic source:

```bash
python setup_synthetic_api.py
```

Keep that process running. In a second terminal, execute the two cutoffs:

```bash
python run_pipeline_local.py --reset --as-of 2025-09-30T23:59:59Z
python run_pipeline_local.py --as-of 2026-01-15T23:59:59Z
```

Expected results:

| Run | Full-scan rows at cutoff | Rows extracted | Quarantined | Final analytical rows |
| --- | ---: | ---: | ---: | ---: |
| Historical load | 4,573 | 4,573 | 18 | 4,315 |
| Incremental load | 6,365 | 1,793 | 7 | 6,000 |

The second request avoids reading 4,572 events relative to another full scan, a reduction of 71.8%.

Generated local intermediates are written under `runtime/` and excluded from Git. The final portable output remains at `data/gold/customer_experience_responses.csv`.

## Databricks reproduction

1. Import `setup_synthetic_api.py` and `run_pipeline_databricks.py` into the same workspace folder.
2. Open `run_pipeline_databricks.py` as a notebook.
3. Attach available compute.
4. Select **Run all**.

The first `%run` cell prepares the synthetic source and starts its local API on the notebook driver. The notebook then executes the historical and incremental cutoffs in order.

The exercise creates only tables prefixed with `cx_ex01_` in the active catalog and schema:

| Table | Purpose |
| --- | --- |
| `cx_ex01_bronze_responses` | Raw events received in each API batch |
| `cx_ex01_quarantine_responses` | Events that fail validation |
| `cx_ex01_silver_responses` | Current valid version of each response |
| `cx_ex01_control_watermarks` | Last successfully committed extraction state |
| `cx_ex01_gold_responses` | Analytical fields for reuse in later exercises |
| `cx_ex01_load_comparison` | Full and incremental load comparison |

The local implementation is covered by automated tests. The Databricks notebook follows the same rules, but it should be run in the target workspace to verify the available compute, catalog permissions and Delta behavior.

## Files in this exercise

| File | Purpose |
| --- | --- |
| `setup_synthetic_api.py` | Generates the source and simulates the paginated API |
| `run_pipeline_local.py` | Runs the whole exercise outside Databricks |
| `run_pipeline_databricks.py` | Runs the whole exercise with Spark and Delta |
| `data/source/responses.jsonl` | Frozen synthetic source served by the API |
| `data/gold/customer_experience_responses.csv` | Current analytical state after both local runs |
| `data/DATA_DICTIONARY.md` | Defines each synthetic field |
| `reports/*.json` | Preserves detailed evidence from each local run |
| `reports/load_comparison.csv` | Summarizes the main load comparison |
| `tests/test_exercise_01.py` | Tests deterministic generation, validation and idempotency |
