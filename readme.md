# Spotify Data Engineering Pipeline

## Overview

Local data engineering pipeline for collecting, structuring, and analyzing personal Spotify listening data.

The project implements a layered data architecture (**Bronze → Silver → Gold**) to ingest data from the Spotify Web API, process it into structured datasets, and store them in an analytical warehouse for querying and exploration.

The primary goal is to demonstrate core data engineering concepts, including:

- data ingestion
- layered transformations
- reproducible pipelines
- automated testing

All within a lightweight local environment.

---

## Business Case

Spotify insights are heavily aggregated and only available periodically.  
Regular API snapshots enable:

- trend analysis
- custom KPIs
- exploratory analytics

The pipeline started as a local prototype to validate the architecture and data model.  
With a working Bronze → Silver → Gold pipeline in place, the next step is migrating to Azure —  
moving from a local environment to a production-grade cloud pipeline.

---

# Architecture

The pipeline follows a simplified **Medallion Architecture**.

```text
Spotify API
    ↓
Bronze (raw ingestion)
    ↓
Silver (cleaned & normalized datasets)
    ↓
Gold (analytics-ready tables)
    ↓
DuckDB Warehouse
```

## Layer Responsibilities

### Bronze

- raw API responses
- minimal transformation
- append-only ingestion
- snapshot-based storage

### Silver

- data cleaning
- schema normalization
- field extraction
- deduplication
- validation

### Gold

- analytics-ready tables
- curated datasets optimized for querying
- aggregation and analytical modeling

---

# Data Sources

The pipeline collects data from the **Spotify Web API**:

- Top Artists
- Top Tracks
- Recently Played Tracks

These endpoints enable the creation of a structured historical dataset of listening behavior.

---

# Technology Stack

| Component | Technology |
|---|---|
| Language | Python 3 |
| Data Processing | Pandas |
| Analytical Database | DuckDB |
| Storage Formats | CSV (Bronze), Parquet (Silver / Gold) |
| API Integration | Spotify Web API |
| Testing | Pytest |
| Environment Management | Python Virtual Environment |
| Containerization | Docker *(planned)* |

---

# Project Structure

```text
spotify_data_pipeline/

├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── spotify_data_pipeline/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── analytics/
│   ├── helpers/
│   └── ddl/
│
├── tests/
│
├── requirements.txt
├── pytest.ini
└── README.md
```

## Key Components

### Bronze Layer

- API ingestion
- raw data storage
- retry and error handling

### Silver Layer

- data normalization
- schema enforcement
- structured dataset generation

### Gold Layer

- analytics-ready tables
- curated datasets

### DDL

- warehouse schema definition
- reproducible database setup

### Tests

- unit tests for ingestion, transformations, and utilities

---

# Pipeline Execution

The pipeline can either be executed end-to-end or stage by stage.

## Full Pipeline Execution

```bash
python -m spotify_data_pipeline.main
```

This performs the following steps:

1. Fetch data from the Spotify API
2. Store raw data in the Bronze layer
3. Transform and clean data into the Silver layer
4. Build analytics-ready datasets in the Gold layer
5. Populate the DuckDB analytical warehouse

---

## Stage-Level Execution

Each pipeline stage can also be executed independently for targeted debugging and development.

### Data Ingestion

Fetch data from the Spotify API and store raw snapshots.

```bash
python3 -m spotify_data_pipeline.Bronze.fill_bronze
```

### Silver — Data Transformation

Clean and normalize Bronze data into structured datasets.

```bash
python3 -m spotify_data_pipeline.Silver.fill_silver
```

### Gold — Analytics Tables

Build analytics-ready tables from the Silver layer.

```bash
python3 -m spotify_data_pipeline.Gold.fill_gold
```

### Populate Warehouse Tables

```bash
python3 -m spotify_data_pipeline.ddl.populate_warehouse
```

### Analytics

Run analytical queries on curated Gold datasets.

```bash
python3 -m spotify_data_pipeline.Analytics.analyze_gold
```

---

# Data Warehouse

The project uses **DuckDB** as an embedded analytical database.

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
- utility functions

Run tests with:

```bash
pytest
```

Coverage reports can additionally be generated to validate code quality.

---

# Design Principles

The pipeline follows several core data engineering principles:

- layered data architecture
- separation of ingestion and transformation
- reproducible environments
- modular pipeline components
- automated testing

The implementation intentionally favors **simple and transparent tooling** to prioritize pipeline design over infrastructure complexity.

---

# Data Quality & Testing Limitations

The project includes a comprehensive test suite covering core components such as API integration, error handling, and data transformations.

However, several limitations currently exist regarding full pipeline validation.

## Current Limitations

- End-to-end pipeline tests are partially mocked, especially within orchestration layers
- Mocking ensures isolation and prevents unintended side effects (e.g. persistent writes)
- Full data correctness across the entire pipeline is therefore not yet fully guaranteed

## Known Gaps

- no fully isolated end-to-end tests using controlled datasets
- limited validation of final analytical outputs
- no formal data quality framework (expectations, constraints, validations)

## Planned Improvements

- introduce isolated end-to-end tests using temporary file systems
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

## Planned Next Steps

- migrating pipeline execution to Azure (in progress)
  - Azure Data Lake Storage Gen2 as storage backend (replaces local file system)
  - Azure Functions for scheduled, serverless pipeline execution
  - Service Principal for secure, non-interactive authentication
- data quality frameworks
- ML-ready dataset preparation

# Future Extensions

Potential future extensions include:

- workflow orchestration
- scheduled pipeline execution
- data quality validation
- cloud storage and compute integration
- feature generation pipelines for ML use cases
- training dataset generation from curated data

The architecture keeps these options open without introducing unnecessary complexity for the current project scope.