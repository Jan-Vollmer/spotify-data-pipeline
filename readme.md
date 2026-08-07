# Spotify Data Engineering Pipeline

## Overview

Data engineering pipeline for collecting, structuring, and analyzing personal Spotify listening data.

The project implements a layered data architecture (**Bronze → Silver → Gold**) to ingest data from the Spotify Web API, process it into structured datasets, and store them in an analytical warehouse for querying, transformation, and ML-ready feature generation.

The primary goal is to demonstrate core data engineering concepts, including:

- data ingestion
- layered transformations
- reproducible pipelines
- automated testing and data quality checks
- infrastructure as code
- cloud-native orchestration on Azure
- feature engineering for downstream ML use cases

---

## Business Case

Spotify insights are heavily aggregated and only available periodically.
Regular API snapshots enable:

- trend analysis
- custom KPIs
- exploratory analytics

The pipeline started as a local prototype to validate the architecture and data model. Bronze, Silver, and Gold layers now run on Azure (Azure Functions + Blob Storage). The DuckDB warehouse and dbt transformation layer run locally, but read directly from Azure Blob Storage (Gold layer) — execution migration (running the warehouse build and dbt inside an Azure Function/Container instead of locally) is the one remaining "local" piece, tracked under [Planned Next Steps](#planned-next-steps).

---

# Architecture

The pipeline follows a simplified **Medallion Architecture**, extended with a transformation and ML layer on top of the warehouse.

```text
Spotify API
    ↓
Bronze (raw ingestion, Azure Blob Storage)
    ↓
Silver (cleaned & normalized datasets, Azure Blob Storage)
    ↓
Gold (analytics-ready tables, Azure Blob Storage)
    ↓
DuckDB Warehouse — local execution, reads directly from Azure Blob
    ↓
dbt (staging + feature engineering, local against DuckDB warehouse)
    ↓
ML Layer (artist clustering, local against DuckDB warehouse)
```

Infrastructure (Storage Account, Function App, Service Plan) is managed via Terraform — see [Infrastructure as Code](#infrastructure-as-code-terraform).

## Layer Responsibilities

### Bronze on Azure

- raw API responses
- minimal transformation
- append-only ingestion
- snapshot-based storage
- runs on Azure Functions (timer-triggered)

### Silver on Azure

- data cleaning
- schema normalization
- field extraction
- deduplication
- validation
- runs immediately after Bronze within the same Function execution

### Gold on Azure

- analytics-ready tables, partitioned per scope/time_range
- runs after Silver within the same Function execution (top_*, per time_range) or on a separate weekly trigger (recent_tracks)
- writes Parquet to Azure Blob Storage (`gold` container)

---

# Data Sources

The pipeline collects data from the **Spotify Web API**:

- Top Artists (short / medium / long term)
- Top Tracks (short / medium / long term)
- Recently Played Tracks

These endpoints enable the creation of a structured historical dataset of listening behavior.

---

# Technology Stack

| Component | Technology |
|---|---|
| Language | Python 3 |
| Data Processing | Pandas |
| Cloud Storage | Azure Blob Storage (Bronze, Silver, Gold) |
| Compute / Orchestration | Azure Functions (timer triggers, Flex Consumption plan) |
| Infrastructure as Code | Terraform (`azurerm` provider) |
| Authentication | Service Principal (OIDC, federated credentials) |
| Monitoring | Application Insights |
| Analytical Database | DuckDB (local execution, reads Gold layer from Azure Blob) |
| Transformation / Modeling | dbt (dbt-core + dbt-duckdb adapter) |
| ML | scikit-learn, scikit-learn-extra alternative (`kmedoids`), `gower` |
| Storage Formats | JSON (Bronze), Parquet (Silver / Gold) |
| API Integration | Spotify Web API |
| Testing | Pytest, dbt tests |
| CI/CD | GitHub Actions |
| Environment Management | Python Virtual Environments (separate envs per component — pipeline, ML) |

---

# Project Structure

```text
spotify-data-pipeline/

├── spotify_data_pipeline/       # Bronze/Silver/Gold ingestion pipeline (Azure Functions)
│   ├── Bronze/
│   ├── Silver/
│   ├── Gold/
│   ├── helpers/
│   └── ddl/
│
├── azure_function/               # Azure Function App deployment (function_app.py, requirements.txt)
│
├── spotify_analytics/            # dbt project (staging + marts models)
│   └── models/
│       ├── staging/
│       └── marts/
│
├── spotify_ml/                   # ML layer — artist clustering
│   ├── notebooks/
│   └── src/
│
├── infra/                        # Terraform IaC
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars          # gitignored
│
├── tests/
├── requirements.txt
├── pytest.ini
└── README.md
```

## Key Components

### Bronze Layer

- API ingestion
- raw data storage in Azure Blob Storage (`bronze` container)
- retry and error handling

### Silver Layer

- data normalization
- schema enforcement
- structured dataset generation
- writes Parquet to Azure Blob Storage (`silver` container)
- archives processed Bronze files

### Gold Layer

- analytics-ready, partitioned Parquet tables in Azure Blob Storage (`gold` container)

### DDL / Warehouse Population

- warehouse schema definition (`ddl/warehouse.sql`, `ddl/reset_warehouse.sql`)
- `populate_warehouse.py` reads Gold-layer Parquet directly from Azure Blob (via DuckDB's `azure` extension) into the local DuckDB star schema

### Tests

- unit tests for ingestion, transformations, and utilities
- Silver tests mock Blob Storage interactions

---

# Pipeline Execution

## Production (Azure)

Bronze and Silver run automatically via Azure Functions timer triggers. Silver executes immediately after Bronze within the same job — see [Orchestration](#orchestration) and [Scheduling](#scheduling).

## Local Development / Testing

### Data Ingestion (Bronze)

```bash
python3 -m spotify_data_pipeline.Bronze.fill_bronze
```

### Silver — Data Transformation

```bash
python3 -m spotify_data_pipeline.Silver.fill_silver
```

Requires `AZURE_CONNECTION_STRING` set (e.g. via `.env` with `python-dotenv`).

### Populate Warehouse (reads Gold layer from Azure Blob)

```bash
python3 -m spotify_data_pipeline.ddl.populate_warehouse
```

Requires `AZURE_CONNECTION_STRING` set. Reads Parquet directly from the `gold` container via DuckDB's `azure` extension.

---

# Orchestration

Bronze and Silver are coupled per job: each Azure Function executes its Bronze ingestion, then immediately runs the corresponding Silver transformation (`silver_func` in the `JOBS` dict). This guarantees Silver always processes the data Bronze just wrote, without needing a separate queue or trigger.

```text
execute(job_name):
    1. fetch token
    2. call Spotify API
    3. write Bronze blob
    4. run Silver transformation for this job
    5. archive processed Bronze file(s)
```

---

# Scheduling

| Job | Frequency | Reason |
|-----|-----------|--------|
| recent_tracks | daily | Spotify caps recently played at 50 tracks. Daily pull is sufficient for personal listening volume. |
| top_*_short | monthly | Matches Spotify's short-term window (~4 weeks). |
| top_*_medium | every 6 months | Matches Spotify's medium-term window (~6 months). |
| top_*_long | yearly | Matches Spotify's long-term window (all time). |

All triggers run at 00:00 UTC. On Jan 1st all 7 jobs run simultaneously — no issue since each runs in a separate Function instance. Spotify rate limits are covered by retry logic.

---

# Monitoring

- **Application Insights** is connected via `APPLICATIONINSIGHTS_CONNECTION_STRING`
- Structured logging per pipeline step (token load, API call, items received, Blob write, Silver run)
- **dbt source freshness checks** on the warehouse fact tables catch silent pipeline staleness that Azure-level monitoring alone would miss (see [Data Quality](#data-quality) — a real incident of this kind occurred during development)

---

# Cost Considerations

The project runs on Azure's consumption-based pricing, chosen deliberately to keep a private/portfolio project near-zero cost:

- **Azure Functions (Flex Consumption Plan)**: pay-per-execution, no idle cost between the weekly/monthly/yearly triggers.
- **Blob Storage**: last month's actual cost was €0.43, forecast €0.93 for the current month — the increase reflects the Gold layer migration adding more pipelines and files. Current blob capacity: ~460 MB.
- **Transactions**: ~10,370/month, average end-to-end latency 27.88 ms, average server latency 18.09 ms — well within normal range for this data volume, no optimization needed at this scale.
- **Application Insights**: negligible — 0.00 USD estimated (no web tests configured, custom metrics usage at 120 bytes/month).
- **Monitoring Alert**: a dedicated "no data processed" Azure Monitor alert was evaluated at ~$0.50/month and rejected for this private project in favor of dbt source freshness checks, which cover the same failure mode within the existing tooling at no extra cost.

Design choices made explicitly to minimize cost:
- Trigger frequency matched to actual data-consumption need (see [Scheduling](#scheduling)), not maximal freshness.
- No dedicated orchestration service (Airflow) — avoided to skip the operational/infrastructure cost of running it on Azure (see [Architecture Decisions](#architecture-decisions)).
- Gold uses per-scope/time_range writes instead of full rebuilds, reducing compute per run.
- At current volume (~460 MB, ~10K transactions/month), cost is dominated by the Gold-layer expansion rather than storage capacity — worth revisiting tiering (Hot vs. Cool) only if data volume grows significantly.

---

# Data Warehouse

The project uses **DuckDB** as an embedded analytical database, running locally and reading Gold-layer Parquet directly from Azure Blob Storage.

## Benefits

- columnar storage
- fast analytical queries
- zero infrastructure overhead for the warehouse itself
- reads cloud data directly (no separate download/sync step)

The warehouse schema is defined using SQL (`ddl/warehouse.sql`) and can be recreated or reset using the provided scripts (`ddl/reset_warehouse.sql`).

---

# Transformation Layer (dbt)

The warehouse's star schema is transformed into analytics- and ML-ready features using **dbt** (dbt-core with the `dbt-duckdb` adapter), running locally against the DuckDB warehouse. Kept as a separate project (`spotify_analytics/`) with its own dependencies, distinct from both the pipeline and the ML layer.

## Structure

- **Staging models** (`stg_*`): 1:1 mappings onto warehouse tables via `source()`, no aggregation. Isolates all downstream models from raw table names.
- **Marts model** (`fct_artist_features`): grain change from "one row per ranking snapshot" to "one row per artist + term + month", with aggregated metrics (average position, position volatility, linear trend slope, genre array).

## Data Quality

- Generic tests (`not_null`, `unique`, `relationships`) on all staging models, enforcing referential integrity between the bridge and dimension tables.
- `dbt_utils.accepted_range` on the marts model to catch aggregation errors before they reach downstream analysis.
- **Source freshness checks** on the three fact tables, flagging silent pipeline staleness. A real incident occurred during development: an Azure Function silently produced no Gold output for `top_artists` due to a naming mismatch (`top_artist` vs. `top_artists`) between the write and read paths, while still reporting `Success` — the function completed without exception, so no Azure-level alert would have caught it. Freshness checks are the intended safety net for this class of failure going forward.

## Running

```bash
cd spotify_analytics
dbt run
dbt test
dbt source freshness
dbt docs generate && dbt docs serve
```

---

# ML Layer (Artist Clustering)

Artists are clustered based on listening popularity/stability and genre similarity, using `fct_artist_features` as input. Implemented in `spotify_ml/`, kept separate from the dbt layer — different tooling (Python ML stack vs. SQL transformations) and a dependency scope that shouldn't bleed into the Function App deployment or the dbt project's environment.

## Approach

- **Distance metric**: combines Gower distance (on `avg_position`, `position_stddev`) with Jaccard distance (genre set similarity) into a single weighted distance matrix.
- **Algorithm**: K-Medoids (PAM), not K-Means — K-Means requires a Euclidean mean, which isn't defined on a precomputed distance matrix or on mixed categorical data.
- **k selection**: k=8, chosen via silhouette score marginal gain across k=2..14 (largest single jump, followed by a plateau with no further meaningful improvement) rather than the global maximum score, which tends to keep improving as k grows without reflecting genuine structure.

## Design Iteration

An initial version reduced each artist's genre list to a single "primary genre" for Gower distance. This caused genre to dominate the distance almost entirely — resulting clusters were >90% single-genre, effectively reproducing a group-by rather than finding meaningful structure. Switching to Jaccard distance over the full genre list per artist, combined with Gower distance on the numeric features, produced more balanced, mixed-genre clusters.

## Notable Finding

One resulting cluster groups artists with no genre overlap (children's music, Christmas music, medieval metal) — not because they're musically related, but because they co-occur in the same listening context. This illustrates a key advantage of data-driven clustering over fixed genre taxonomies: it surfaces behavioral listening patterns rather than musical similarity.

## Running

```bash
python -m spotify_ml.src.clustering
```

Reads `fct_artist_features` from the DuckDB warehouse, writes results to a new `artist_clusters` table.

---

# Infrastructure as Code (Terraform)

Storage Account, Service Plan, Storage Container, and Function App are managed via Terraform (`infra/`), imported from the pre-existing, manually-created Azure resources rather than provisioned from scratch — reflecting how the project actually evolved (manual setup first, IaC retrofitted afterward), which is also a realistic scenario in existing organizations.

## Structure

```text
infra/
├── main.tf              # provider config + resource definitions
├── variables.tf          # input variable declarations
├── outputs.tf
└── terraform.tfvars      # actual values (secrets, resource names) — gitignored
```

## Resource Group Scope

A single Service Principal, scoped to the existing resource group (`Contributor` at RG level, not subscription level), is used for Terraform. The resource group itself already exists and is referenced read-only via a `data` block rather than managed by Terraform — so no separate, broader-scoped bootstrap identity is needed to create it. In an organization where Terraform were also responsible for creating the resource group itself, a separate, more privileged bootstrap SP (subscription- or management-group-scoped) would be used for that one-time step, kept distinct from the narrowly-scoped workload SP used for day-to-day `plan`/`apply` runs. That separation isn't necessary here, since the bootstrap step doesn't happen through Terraform.

## Function App: Flex Consumption

The Function App runs on Azure's newer Flex Consumption plan, which required the dedicated `azurerm_function_app_flex_consumption` resource (available from `azurerm` provider v4.21+) rather than the more commonly documented `azurerm_linux_function_app` — the latter doesn't correctly represent several Flex Consumption-specific settings (deployment storage container, instance memory/count), which surfaced as import drift before switching resource types.

## Import Workflow

Since all four resources pre-existed in Azure, each was written as a `resource` block matching the real configuration, then brought into Terraform's state via `terraform import`, then verified with `terraform plan` until it reported no changes. This surfaced several real discrepancies between assumed and actual configuration — including security-relevant settings (`https_only`, `client_certificate_mode`, FTP/WebDeploy basic auth, CORS) that would have been silently weakened had `terraform apply` been run against an incomplete resource definition. Catching these via `plan` before `apply`, rather than after, was the main practical lesson from this part of the project.

## CI/CD

`terraform plan` is intended to run in CI on pull requests touching `infra/`, surfacing infrastructure changes as part of code review. `terraform apply` is deliberately kept manual: for a single-person project without a staging environment or approval gates, automated apply trades a marginal convenience gain for real risk against the only existing environment.

---

# Design Principles

The pipeline follows several core data engineering principles:

- layered data architecture
- separation of ingestion and transformation
- reproducible environments
- modular pipeline components
- automated testing and data quality checks
- infrastructure as code
- cloud-native, serverless execution

The implementation intentionally favors **simple and transparent tooling** to prioritize pipeline design over infrastructure complexity — see [Architecture Decisions](#architecture-decisions) for the Airflow trade-off specifically.

---

# Architecture Decisions

### Bronze → Silver Orchestration
Silver runs directly after Bronze within the same Azure Function execution (`execute()`), rather than via a separate queue trigger or Airflow DAG. Simpler, no extra infrastructure, and sufficient for the project's dependency needs.

### Blob Naming Structure
`top_artists_{time_range}/` and `top_tracks_{time_range}/` (flat per time_range) instead of `top_artists/{time_range}/` (nested). Chosen for direct prefix access matching how the pipeline is partitioned (per time_range, not per scope).

### Monitoring Alert Cost
A dedicated "no data processed" Azure Monitor alert was evaluated at ~$0.50/month. Decided against it for this private project — dbt source freshness checks cover the same failure mode within existing tooling, at no additional cost.

### Airflow
Considered for orchestration. Deemed overkill — 7 independent jobs with a simple linear Bronze→Silver dependency don't justify the operational overhead of running Airflow on Azure (no managed offering on Azure equivalent to AWS MWAA).

### Terraform Resource Import (vs. Greenfield)
Resources were created manually first, then retrofitted into Terraform via `import`, rather than starting from a clean `terraform apply`. Chosen deliberately to reflect a realistic scenario (adopting IaC for existing infrastructure) rather than the easier greenfield case — see [Infrastructure as Code](#infrastructure-as-code-terraform).

---

# Testing

The project includes automated tests using **pytest**, alongside dbt's own test suite for the transformation layer (see [Data Quality](#data-quality)).

## Current Test Coverage

- API authentication
- error handling and retry logic
- transformation helpers
- ingestion logic
- Silver pipeline logic (mocked Blob Storage)
- utility functions
- dbt: referential integrity, not-null/unique constraints, accepted-range checks, source freshness

Run tests with:

```bash
pytest
```

## Known Gaps

- no fully isolated end-to-end tests using controlled datasets across the whole Bronze→Silver→Gold→warehouse chain
- limited validation of final analytical outputs beyond dbt's generic tests
- no automated alerting on Function failures beyond dbt source freshness (no Azure-level "function failed" alert)

## Planned Improvements

- isolated end-to-end tests using temporary file systems / mocked Blob Storage
- reduce mocking in critical transformation paths
- extend dbt tests to the ML layer's input assumptions (e.g. genre data completeness)

---

# Learning Goals

## Focus Areas

- building end-to-end data pipelines
- applying layered data architectures
- handling real-world API ingestion
- understanding trade-offs between simplicity and robustness
- cloud-native serverless architecture on Azure
- infrastructure as code, including retrofitting it onto pre-existing resources
- feature engineering and translating a warehouse table into a working ML model

## Planned Next Steps

- migrate warehouse build + dbt execution to run inside Azure (Function or Container) instead of locally, reading and writing Gold-layer data without a local machine in the loop
- `terraform plan` in CI (GitHub Actions) on changes to `infra/`
- extend end-to-end test coverage (see [Testing](#testing))

# Future Extensions

Potential future extensions include:

- feature generation pipelines for additional ML use cases (e.g. popularity trend classification)
- training dataset generation from curated data, with explicit point-in-time correctness
- Hierarchical Namespace (HNS) for future Spark/Synapse workloads if data volume grows

The architecture keeps these options open without introducing unnecessary complexity for the current project scope.
