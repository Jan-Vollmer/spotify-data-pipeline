# Spotify Data Engineering & Machine Learning Pipeline

## Executive Summary
Local data pipeline for collecting, structuring, and enabling analysis of personal Spotify usage data.

## Business Case
- Spotify insights are highly aggregated and only available annually
- Regular snapshots enable:
  - trend analysis
  - custom KPIs
  - exploratory analytics
- This project is intentionally local-only:
  - cloud infrastructure would add unnecessary complexity
  - operational overhead outweighs current benefits

## Target State

### Ingestion
- Spotify Web API
- Manual or lightweight scheduled execution

### Storage
- CSV (raw)
- Parquet (processed)
- Local filesystem

### Transformation
- Python / Pandas
- Explicit schemas and basic data quality checks

### Analytics Enablement
- Jupyter Notebooks
- Exploratory Data Analysis (EDA)
- Feature engineering for downstream use
- Model building optional

### Operations
- Docker for reproducibility and environment isolation
- Containerization keeps a future cloud move technically simple, if needed


## Methodology (CRISP-DM)

### 1. Business Understanding
Define the problem, KPIs, and value proposition (regular insights, full data ownership).

### 2. Data Understanding
Initial data exploration, structure checks, and understanding of Spotify fields.

### 3. Data Preparation
Schema definition, data cleaning, Parquet conversion, and initial validations.

### 4. Modeling
EDA, feature engineering, and prototyping of clustering and NLP models.

### 5. Evaluation
interpretability and analytical usefulness

### 6. Deployment
reproducible local execution

## Deliverables
- Local, reproducible ETL pipeline
- Structured Parquet datasets
- Analytics-ready data models
- Clean and maintainable project structure

## Optional Future Extensions
- Increased automation if requirements grow
- Cloud deployment only if complexity becomes justified