# Spotify Data Engineering Pipeline

## Overview
Local data engineering pipeline for collecting, structuring, and analyzing personal Spotify listening data.

The project implements a layered data architecture (Bronze → Silver → Gold) to ingest data from the Spotify Web API, process it into structured datasets, and store it in an analytical warehouse for querying and exploration.

The goal is to demonstrate core data engineering concepts including data ingestion, layered transformations, reproducible pipelines, and automated testing in a lightweight local environment.

## Business Case
- Spotify insights are highly aggregated and only available annually
- Regular snapshots enable:
  - trend analysis
  - custom KPIs
  - exploratory analytics
- This project is intentionally local-only:
  - cloud infrastructure would add unnecessary complexity
  - operational overhead outweighs current benefits

---

# Architecture

The pipeline follows a simplified **Medallion Architecture**.

Spotify API
↓
Bronze (raw ingestion)
↓
Silver (cleaned and normalized datasets)
↓
Gold (analytics-ready tables)
↓
DuckDB Warehouse

### Layer Responsibilities

**Bronze**
- Raw API responses
- Minimal transformation
- Append-only ingestion
- Snapshot-based storage

**Silver**
- Data cleaning
- Schema normalization
- Field extraction
- Deduplication and validation

**Gold**
- Analytics-ready tables
- Structured datasets optimized for querying
- Aggregation and modeling for downstream analysis

---

# Data Sources

The pipeline collects data from the **Spotify Web API**:

- Top Artists
- Top Tracks
- Recently Played Tracks

These endpoints allow building a structured historical dataset of listening behavior.

---

# Technology Stack

| Component | Technology |
|---|---|
Language | Python 3 |
Data Processing | Pandas |
Analytical Database | DuckDB |
Storage Formats | CSV (Bronze), Parquet (Silver / Gold) |
API Integration | Spotify Web API |
Testing | Pytest |
Environment Management | Python Virtual Environment |
Containerization | Docker (planned) |

---

# Project Structure
spotify_data_pipeline/

data/
bronze/
silver/
gold/

spotify_data_pipeline/
bronze/
silver/
gold/
analytics/
helpers/
ddl/

tests/

requirements.txt
pytest.ini
README.md

### Key Components

**Bronze Layer**
- API ingestion
- raw data storage
- retry and error handling

**Silver Layer**
- data normalization
- schema enforcement
- structured dataset generation

**Gold Layer**
- analytics-ready tables
- curated datasets

**DDL**
- warehouse schema definition
- reproducible database setup

**Tests**
- unit tests for ingestion, transformations, and utilities

---

# Pipeline Execution
The pipeline can either be executed end-to-end or stage by stage.

Full execution of the pipeline locally:
python -m spotify_data_pipeline.main

Execution performs the following steps:

1. Fetch data from the Spotify API  
2. Store raw data in the Bronze layer  
3. Transform and clean data into the Silver layer  
4. Build analytics-ready datasets in the Gold layer  
5. Populate the DuckDB analytical warehouse  

## Stage-Level Execution

Each pipeline stage can also be executed independently.  
This allows targeted debugging, development, and partial pipeline runs.

### Data Ingestion
Fetch data from the Spotify API and store raw snapshots.

python3 -m spotify_data_pipeline.Bronze.fill_bronze

### Silver – Data Transformation
Clean and normalize Bronze data into structured datasets.

python3 -m spotify_data_pipeline.Silver.fill_silver

### Analytics Tables
Build analytics-ready tables from the Silver layer.

python3 -m spotify_data_pipeline.Gold.fill_gold

### Populate the Warehouse Tables
python3 -m spotify_data_pipeline.ddl.populate_warehouse

### Analytics
Run analytical queries on the curated Gold datasets.

python3 -m spotify_data_pipeline.Analytics.analyze_gold

---

# Data Warehouse

The project uses **DuckDB** as an embedded analytical database.

Benefits:

- columnar storage  
- fast analytical queries  
- zero infrastructure overhead  
- reproducible local environment  

The warehouse schema is defined using SQL and can be reset or recreated using the provided scripts.

---

# Testing

The project includes automated tests using **pytest**.

Test coverage includes:

- API authentication  
- error handling and retry logic  
- transformation helpers  
- ingestion logic  
- utility functions  

Tests can be executed with:
pytest
Coverage reports can be generated to validate code quality.

---

# Design Principles

The pipeline follows several data engineering principles:

- layered data architecture  
- separation of ingestion and transformation  
- reproducible environments  
- modular pipeline components  
- automated testing  

The implementation intentionally favors **simple, transparent tooling** to focus on pipeline design rather than infrastructure complexity.

---

## Future Extensions

Possible extensions if requirements grow:

- containerized execution with Docker
- workflow orchestration
- scheduled pipeline execution
- data quality validation
- cloud storage and compute integration
- feature generation pipelines for machine learning use cases
- training dataset generation from curated data

The architecture keeps these options open without introducing unnecessary complexity for the current scope.