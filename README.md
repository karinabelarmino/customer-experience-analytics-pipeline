# Customer Experience Analytics and Pipeline

> A reproducible, exercise-based project connecting data pipelines, customer journey measurement and CX analytics.

This repository recreates, with entirely synthetic data, a customer-experience pipeline from API ingestion to an analytical dataset. It documents the type of data problem, the technical decisions and the learning process without reproducing any confidential data, code, endpoint, credential or business information.

The repository will be released as a sequence of exercises.

## 🚀 Exercise releases

| Exercise | Scope | Main output | Status |
| :---: | --- | --- | :---: |
| **01** | Full load versus incremental API load | Validated and incrementally updated analytical table | Planned |
| **02** | Response grain, repeated events and deduplication | Documented grain and version-resolution rules | Planned |
| **03** | NPS and CSAT across the customer journey | Reproducible customer-experience indicators | Planned |
| **04** | Claims ratio and outlier treatment | Robust analytical comparison | Planned |
| **05** | Analytical model for Power BI | Dashboard-ready Gold layer | Planned |

### How the data were obtained and validated

The repository separates the source simulation from the analytical exercise:

1. `setup_synthetic_api.py` creates a deterministic JSON Lines source and exposes it through a paginated HTTP API.
2. The API delivers the records as if they came from an external operational system.
3. `run_pipeline_local.py` or `run_pipeline_databricks.py` then performs the actual exercise: extraction, validation, loading and export.

The synthetic source contains 6,000 unique responses, 352 later revisions and 25 intentionally invalid events. The values represent four fictional journey stages and use valid NPS and CSAT scales except for the records deliberately created for quarantine testing. The fixed seed makes every run reproducible.

Validation checks required fields, NPS scores from 0 to 10 and CSAT scores from 1 to 5. Invalid events are quarantined. Among valid events, only the most recent version of each `response_id` is sent to the analytical upsert.

### Limitations

This is a controlled educational simulation. The local API does not reproduce every production concern, such as OAuth flows, token renewal, strict rate limits, schema drift, distributed failures or source-side deletions. SQLite is used for portability and is not presented as a replacement for an enterprise warehouse.

The Databricks notebook mirrors the tested local logic with Spark and Delta Lake, but managed-table behavior must still be verified in the user's own Databricks workspace. Later exercises will extend the analytical layer rather than claim that this first pipeline covers the whole customer-experience domain.

## 🗂️ Repository structure

```text
README.md                       Project overview and release roadmap
CITATION.cff                    Citation metadata
LICENSE                         Repository license
.github/workflows/             Automated validation
assets/                         Figures and visual references used in the documentation

exercise_XX_topic/
  README.md                     Exercise documentation and reproduction instructions
  assets/                       Exercise-specific figures, when required
  data/                         Synthetic inputs and analytical outputs
  reports/                      Execution evidence and comparisons
  tests/                        Automated validation
  setup_*.py                    Synthetic source preparation, when required
  run_*.py                      Local and platform-specific implementations
```

The `runtime/` directory is intentionally excluded from Git because it contains reproducible intermediate files, the local database and the committed watermark.

## 📚 Learning resources

This repository is primarily a technical project, but its analytical choices were also informed by concepts from Customer Experience and Customer Success. The resources below helped connect the data pipeline to the customer journey and to the business questions that the data are intended to answer.

### Books

<details>
<summary><strong>Customer Experience Management: Gestão Prática da Experiência do Cliente — Carlos Caldeira</strong></summary>

<br>

<table>
  <tr>
    <td width="125" align="center" valign="top">
      <a href="https://www.amazon.com.br/dp/6555202513">
        <img
          src="assets/reading/customer-experience-management.jpg"
          width="105"
          alt="Cover of Customer Experience Management by Carlos Caldeira"
        >
      </a>
    </td>
    <td valign="top">
      The book's discussion of customer journey mapping and experience measurement was particularly relevant to this project. It shows why a journey map should be more than a visual representation of touchpoints: each stage should be connected to observable evidence and meaningful indicators.
      <br><br>
      Journey mapping helps identify <strong>where</strong> the experience should be observed, while data and measurement help evaluate <strong>what is actually happening</strong> at each stage. This relationship influenced the organization of the synthetic responses by journey stage and will be explored more directly in the exercise on NPS and CSAT.
      <br><br>
      <a href="https://www.amazon.com.br/dp/6555202513">View the book</a>
    </td>
  </tr>
</table>

</details>

<details>
<summary><strong>Be Our Guest: Perfecting the Art of Customer Service — Disney Institute</strong></summary>

<br>

<table>
  <tr>
    <td width="125" align="center" valign="top">
      <a href="https://www.amazon.com.br/dp/6558101246">
        <img
          src="assets/reading/o-jeito-disney-de-encantar-os-clientes.jpg"
          width="105"
          alt="Cover of the Portuguese edition of Be Our Guest by Disney Institute"
        >
      </a>
    </td>
    <td valign="top">
      This book offers a complementary operational perspective. Memorable experiences are not produced only through individual friendliness or spontaneous gestures: they depend on intentionally designed service standards, processes and attention to detail.
      <br><br>
      This perspective helped frame customer experience as something built across multiple touchpoints, not merely measured through a survey at the end of the journey.
      <br><br>
      <a href="https://www.amazon.com.br/dp/6558101246">View the Portuguese edition</a>
    </td>
  </tr>
</table>

</details>

### Platform and professional content

<p>
  <a href="https://csacademy.com.br/">
    <img
      src="https://img.shields.io/badge/CS%20Academy-CX%20%26%20CS%20Learning-263238?style=for-the-badge"
      alt="CS Academy — CX and CS Learning"
    >
  </a>
</p>

CS Academy helped connect the technical side of the project to the professional vocabulary and practical challenges of CX and CS. Its content covers topics such as Customer Data Analytics, customer research, NPS, Voice of the Customer, journey management and customer engagement.

These references were useful for thinking about how data extraction and validation eventually support indicators, diagnoses and decisions about the customer experience.

[Visit CS Academy](https://csacademy.com.br/)

> These are personal learning references. This project is not affiliated with or sponsored by the authors, publishers or platforms listed above. Book cover images are displayed for identification and recommendation purposes and remain the property of their respective publishers. They are not covered by this repository's MIT License.

## 🔐 Privacy and scope

Every record, identifier, comment, endpoint, token, table name and business rule in this repository was created specifically for this public exercise. The project illustrates a general technical pattern and should not be read as documentation of any particular organization or production environment.

## 📄 License

The code is available under the MIT License. The synthetic dataset may be reused with attribution to this repository.
