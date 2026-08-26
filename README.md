# Customer Experience Analytics and Pipeline

This repository recreates, with entirely synthetic data, a customer-experience pipeline from API ingestion to an analytical dataset. It documents the type of data problem, the technical decisions and the learning process without reproducing any confidential data, code, endpoint, credential or business information.

The repository will be released as a sequence of exercises. This first release focuses on a mistake that is easy to make when moving from fixed research samples to operational data: requesting the entire history from an API every time a job runs.

## Exercise releases

| Exercise | Scope | Main output | Status |
| :---: | --- | --- | :---: |
| **01** | Full load versus incremental API load | Validated and incrementally updated analytical table | **Available** |
| **02** | Response grain, repeated events and deduplication | Documented grain and version-resolution rules | Planned |
| **03** | NPS and CSAT across the customer journey | Reproducible customer-experience indicators | Planned |
| **04** | Claims ratio and outlier treatment | Robust analytical comparison | Planned |
| **05** | Analytical model for Power BI | Dashboard-ready Gold layer | Planned |

### How the data were obtained and validated

The repository separates the source simulation from the analytical exercise:

1. `setup_synthetic_api.py` creates a deterministic JSON Lines source and exposes it through a paginated HTTP API.
2. The API delivers the records as if they came from an external operational system.
3. `run_pipeline_local.py` or `run_pipeline_databricks.py` then performs the actual exercise: extraction, validation, loading and export.

- The synthetic source contains 6,000 unique responses, 352 later revisions and 25 intentionally invalid events. 
- The values represent four fictional journey stages and use valid NPS and CSAT scales except for the records deliberately created for quarantine testing. 
- The fixed seed makes every run reproducible.

Validation checks required fields, NPS scores from 0 to 10 and CSAT scores from 1 to 5. Invalid events are quarantined. Among valid events, only the most recent version of each `response_id` is sent to the analytical upsert.

### Limitations

This is a controlled educational simulation. The local API does not reproduce every production concern, such as OAuth flows, token renewal, strict rate limits, schema drift, distributed failures or source-side deletions. SQLite is used for portability and is not presented as a replacement for an enterprise warehouse.

The Databricks notebook mirrors the tested local logic with Spark and Delta Lake, but managed-table behavior must still be verified in the user's own Databricks workspace. Later exercises will extend the analytical layer rather than claim that this first pipeline covers the whole customer-experience domain.

### How to reproduce

Requirements for the local version:

- Python 3.11 or newer
- No third-party Python packages

Open two terminals in `exercise_01_incremental_load`.

In the first terminal, prepare the synthetic source and keep the API running:

```bash
python setup_synthetic_api.py
```

In the second terminal, run the historical load and then the incremental load:

```bash
python run_pipeline_local.py --reset --as-of 2025-09-30T23:59:59Z
python run_pipeline_local.py --as-of 2026-01-15T23:59:59Z
```

The local analytical output is written to:

```text
exercise_01_incremental_load/data/gold/customer_experience_responses.csv
```

To run the platform version, import `setup_synthetic_api.py` and `run_pipeline_databricks.py` into the same Databricks workspace folder, open the pipeline notebook and select **Run all**. The `%run` command prepares the source automatically, and the notebook performs both cutoffs and displays the comparison and Gold tables.

Detailed instructions and expected results are available in [`exercise_01_incremental_load/README.md`](exercise_01_incremental_load/README.md).

## Repository organization

Each exercise is stored in its own numbered folder and contains its specific documentation, data, scripts, tests and outputs.

```text
README.md                       Project overview and release roadmap
CITATION.cff                    Citation metadata
LICENSE                         Repository license
assets/              		Figures used in the project documentation

exercise_XX_topic/
  README.md                     Exercise documentation and reproduction instructions
  assets/                       Figures specific to the exercise
  data/                         Synthetic inputs and analytical outputs
  reports/                      Execution evidence and comparisons
  tests/                        Automated validation
  setup_*.py                    Synthetic source preparation, when required
  run_*.py                      Local and platform-specific implementations
```

## Privacy and scope

Every record, identifier, comment, endpoint, token, table name and business rule in this repository was created specifically for this public exercise. 
The project illustrates a general technical pattern and should not be read as documentation of any particular organization or production environment.

## License

The code is available under the MIT License. The synthetic dataset may be reused with attribution to this repository.
