# Spotify Data Engineering Pipeline

## Overview

Data engineering pipeline for collecting, structuring, and analyzing personal Spotify listening data.

The project implements a layered data architecture (**Bronze → Silver → Gold**) to ingest data from the Spotify Web API, process it into structured datasets, and store them in an analytical warehouse for querying and exploration.

The primary goal is to demonstrate core data engineering concepts, including:

- data ingestion
- layered transformations
- reproducible pipelines
- automated testing
- cloud-native orchestration on Azure

---

## Business Case

Spotify insights are heavily aggregated and only available periodically.
Regular API snapshots enable:

- trend analysis
- custom KPIs
- exploratory analytics

The pipeline started as a local prototype to validate the architecture and data model.
~~With a working Bronze → Silver → Gold pipeline in place, the next step is migrating to Azure~~ — **Bronze, Silver, and Gold layers are now running on Azure** (Azure Functions + Blob Storage).
DuckDB warehouse and Analytics layer remain local — migration pending.

---

# Architecture

The pipeline follows a simplified **Medallion Architecture**.

```text
Spotify API
    ↓
Bronze (raw ingestion, Azure Blob Storage)
    ↓
Silver (cleaned & normalized datasets, Azure Blob Storage)
    ↓
Gold (analytics-ready tables, Azure Blob Storage)
    ↓
DuckDB Warehouse — local, migration pending
```

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
| Cloud Storage | Azure Blob Storage (Bronze, Silver) |
| Compute / Orchestration | Azure Functions (timer triggers) |
| Authentication | Service Principal (OIDC, federated credentials) |
| Monitoring | Application Insights |
| Analytical Database | DuckDB *(Gold, local — migration pending)* |
| Storage Formats | JSON (Bronze), Parquet (Silver / Gold) |
| API Integration | Spotify Web API |
| Testing | Pytest |
| CI/CD | GitHub Actions |
| Environment Management | Python Virtual Environment |

---

# Project Structure

```text
spotify_data_pipeline/

├── spotify_data_pipeline/
│   ├── Bronze/
│   ├── Silver/
│   ├── Gold/
│   ├── Analytics/
│   ├── helpers/
│   └── ddl/
│
├── function_app.py
├── tests/
│
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

- analytics-ready tables (local, migration pending)

### DDL

- warehouse schema definition
- reproducible database setup (local, migration pending)

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

### Gold — Analytics Tables (local, migration pending)

```bash
python3 -m spotify_data_pipeline.Gold.fill_gold
```

### Populate Warehouse Tables (local, migration pending)

```bash
python3 -m spotify_data_pipeline.ddl.populate_warehouse
```

### Analytics (local, migration pending)

```bash
python3 -m spotify_data_pipeline.Analytics.analyze_gold
```

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
- Alert configured for `"No new JSON blobs found"` to detect silent no-op Silver runs

---

# Cost Considerations

The project runs on Azure's consumption-based pricing, chosen deliberately to keep a private/portfolio project near-zero cost:

- **Azure Functions (Consumption Plan)**: pay-per-execution, no idle cost between the weekly/monthly/yearly triggers.
- **Blob Storage**: last month's actual cost was €0.43, forecast €0.93 for the current month — the increase reflects the Gold layer migration adding more pipelines and files. Current blob capacity: ~460 MB (file share capacity separately reports 530 B, effectively unused).
- **Transactions**: ~10,370/month, average end-to-end latency 27.88 ms, average server latency 18.09 ms — well within normal range for this data volume, no optimization needed at this scale.
- **Application Insights**: negligible — 0.00 USD estimated (no web tests configured, custom metrics usage at 120 bytes/month). Log ingestion/retention costs are tracked separately via the linked Log Analytics workspace, not yet itemized here.
- **Monitoring Alert**: a "no data processed" alert was evaluated at ~$0.50/month and rejected for this private project — manual log review via Application Insights is sufficient at this scale.

Design choices made explicitly to minimize cost:
- Trigger frequency matched to actual data-consumption need (see Scheduling), not maximal freshness — e.g. weekly Gold rebuild for recent_tracks instead of daily.
- No dedicated orchestration service (Airflow) — avoided to skip the operational/infrastructure cost of running it on Azure.
- Gold uses per-scope/time_range writes instead of full rebuilds, reducing compute per run.
- At current volume (~460 MB, ~10K transactions/month), cost is dominated by the Gold-layer expansion rather than storage capacity — worth revisiting tiering (Hot vs. Cool) only if data volume grows significantly.

---

# Data Warehouse

The project uses **DuckDB** as an embedded analytical database for the Gold layer *(local, migration pending)*.

## Benefits

- columnar storage
- fast analytical queries
- zero infrastructure overhead
- reproducible local environment

The warehouse schema is defined using SQL and can be recreated or reset using the provided scripts.

---

# Testing

The project includes automated tests using **pytest**.

## Current Test Coverage

- API authentication
- error handling and retry logic
- transformation helpers
- ingestion logic
- Silver pipeline logic (mocked Blob Storage)
- utility functions

Run tests with:

```bash
pytest
```

---

# Design Principles

The pipeline follows several core data engineering principles:

- layered data architecture
- separation of ingestion and transformation
- reproducible environments
- modular pipeline components
- automated testing
- cloud-native, serverless execution

The implementation intentionally favors **simple and transparent tooling** to prioritize pipeline design over infrastructure complexity. ~~Airflow was considered for orchestration but deemed overkill for this project's scope — the coupled Bronze→Silver execution within Azure Functions covers the dependency requirements without additional infrastructure.~~

---

# Architecture Decisions

### Bronze → Silver Orchestration
Silver runs directly after Bronze within the same Azure Function execution (`execute()`), rather than via a separate queue trigger or Airflow DAG. Simpler, no extra infrastructure, and sufficient for the project's dependency needs.

### Blob Naming Structure
`top_artists_{time_range}/` and `top_tracks_{time_range}/` (flat per time_range) instead of `top_artists/{time_range}/` (nested). Chosen for direct prefix access matching how the pipeline is partitioned (per time_range, not per scope).

### Monitoring Alert Cost
A "no data processed" alert was evaluated at ~$0.50/month. Decided against it for this private project on cost grounds — manual log review via Application Insights is sufficient at this scale.

### Airflow
Considered for orchestration. Deemed overkill — 7 independent jobs with a simple linear Bronze→Silver dependency don't justify the operational overhead of running Airflow on Azure (no managed offering on Azure equivalent to AWS MWAA).

---

# Data Quality & Testing Limitations

## Current Limitations

- End-to-end pipeline tests are partially mocked, especially within orchestration layers
- Mocking ensures isolation and prevents unintended side effects (e.g. persistent writes)
- Full data correctness across the entire pipeline is therefore not yet fully guaranteed
- Gold layer and DuckDB warehouse are not yet migrated to Azure

## Known Gaps

- no fully isolated end-to-end tests using controlled datasets
- limited validation of final analytical outputs
- no formal data quality framework (expectations, constraints, validations)
- DuckDB warehouse and Analytics layer not yet migrated to Azure
- no automated alerting on Function failures (only the "no data processed" log pattern is monitored)


## Planned Improvements

- introduce isolated end-to-end tests using temporary file systems / mocked Blob Storage
- reduce mocking in critical transformation paths
- add data quality checks:
  - completeness
  - uniqueness
  - consistency
- implement validation rules for analytical outputs

---

# Learning Goals

## Focus Areas

- building end-to-end data pipelines
- applying layered data architectures
- handling real-world API ingestion
- understanding trade-offs between simplicity and robustness
- cloud-native serverless architecture on Azure

## Planned Next Steps

- ~~migrating pipeline execution to Azure (in progress)~~
  - ~~Azure Blob Storage as storage backend (replaces local file system)~~ done (Bronze, Silver)
  - ~~Azure Functions for scheduled, serverless pipeline execution~~ done
  - ~~Service Principal for secure, non-interactive authentication~~ done (OIDC, federated credentials)
    - ~~migrate Gold layer to Azure~~ done
- migrate DuckDB warehouse and Analytics layer to Azure
- data quality frameworks
- ML-ready dataset preparation
- introduce Terraform for infrastructure-as-code

# Future Extensions

Potential future extensions include:

- data quality validation
- feature generation pipelines for ML use cases
- training dataset generation from curated data
- Hierarchical Namespace (HNS) for future Spark/Synapse workloads if data volume grows

The architecture keeps these options open without introducing unnecessary complexity for the current project scope.