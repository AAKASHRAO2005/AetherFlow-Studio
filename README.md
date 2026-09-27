# 🌊 AetherFlow
### Production-Grade Weather & CSV Data ETL Pipeline

**AetherFlow** is a modern, educational, and robust **ETL (Extract, Transform, Load)** data engineering engine built in Python using **Pandas**, **Requests**, **SQLAlchemy**, and **SQLite / PostgreSQL**.

AetherFlow bridges real-time meteorological API streams and messy flat-file datasets into an idempotent relational database with data quality gates, anomaly handling, and full audit lineage.

---

## ✨ Features at a Glance

- **Extract**: Pull live weather data from the public [Open-Meteo REST API](https://open-meteo.com/) (no API key needed) and ingest raw, dirty CSV datasets with custom null tokens (`-999`, `N/A`, `missing`).
- **Transform**: Schema standardization, whitespace and casing normalization, mixed-format datetime resolution, sensor outlier detection (filtering $>60^\circ\text{C}$ or $<-60^\circ\text{C}$ glitches), city-grouped median imputation, and meteorological feature engineering (°F, Heat Index, Wind Chill, Comfort Category, WMO weather interpretation).
- **Load**: Idempotent upsert (`ON CONFLICT (city, observation_time) DO UPDATE`) ensuring zero duplication across re-runs.
- **Audit Lineage**: Automated tracking of rows extracted, rows transformed, rows loaded, execution duration, and errors in the `etl_pipeline_runs` table.
- **Interactive Studio**: Modern **Streamlit Web Dashboard** (`app.py`) with visual Plotly analytics, raw vs. cleaned comparison tables, and a live SQL query console.
- **Test-Driven**: Comprehensive `pytest` test suite verifying all transformation and idempotency rules.

---

## 🏗️ Architecture Overview

```
                      ┌──────────────────────────────────────┐
                      │            EXTRACT STAGE             │
                      ├──────────────────┬───────────────────┤
                      │  Open-Meteo API  │  Dirty CSV Data   │
                      │  (JSON Response) │  (Delimited Text) │
                      └────────┬─────────┴─────────┬─────────┘
                               │                   │
                               ▼                   ▼
                      ┌──────────────────────────────────────┐
                      │         Raw Landing Zone             │
                      │         (data/raw/*.json)            │
                      └──────────────────┬───────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │           TRANSFORM STAGE            │
                      ├──────────────────────────────────────┤
                      │  • Schema Standardization            │
                      │  • Casing & Whitespace Normalization │
                      │  • Mixed-Format Datetime Parsing     │
                      │  • Outlier Detection (-60°C to 60°C) │
                      │  • City-Level Median Imputation      │
                      │  • Deduplication on Composite Key    │
                      │  • Feature Engineering:              │
                      │    (°F, Heat Index, Wind Chill,      │
                      │     Comfort Category, WMO Code)      │
                      │  • Data Quality Gate Validation      │
                      └──────────────────┬───────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │              LOAD STAGE              │
                      ├──────────────────────────────────────┤
                      │  • Idempotent UPSERT into DB         │
                      │    (SQLite / PostgreSQL)             │
                      │  • Lineage & Audit Run Logging       │
                      │    (etl_pipeline_runs table)         │
                      └──────────────────┬───────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │             CONSUMPTION              │
                      ├──────────────────┬───────────────────┤
                      │  Streamlit App   │  Analytical SQL   │
                      │  (Live Dashboard)│  (Ad-hoc Queries) │
                      └──────────────────┴───────────────────┘
```

---

## 📁 Project Structure

```
E:\CSV\
│
├── config.py                 # File paths, DB connection string, target cities, WMO codes
├── pipeline.py               # Main CLI orchestrator (Extract -> Transform -> Load)
├── generate_dirty_data.py    # Synthetic generator creating realistic dirty CSVs
├── app.py                    # Interactive Streamlit Web Dashboard & SQL console
│
├── extractors/
│   ├── __init__.py
│   ├── api_extractor.py      # Pulls current & hourly data from Open-Meteo REST API
│   └── csv_extractor.py      # Resilient CSV reader (encodings, custom nulls)
│
├── transformers/
│   ├── __init__.py
│   ├── cleaner.py            # Normalization, regex unit stripping, outlier filtering, imputation
│   └── enricher.py           # Feature engineering, Heat Index, comfort index, quality gates
│
├── loaders/
│   ├── __init__.py
│   └── db_loader.py          # SQLAlchemy loader with idempotent UPSERT & run audit logging
│
├── models/
│   ├── __init__.py
│   └── schema.py             # SQLAlchemy models (WeatherObservation, EtlPipelineRun)
│
├── tests/
│   └── test_pipeline.py      # Pytest test suite testing Extract, Transform, and Load phases
│
└── data/
    ├── raw/                  # Landing zone for raw API dumps and raw CSVs
    ├── processed/            # Cleaned artifacts (if cached)
    └── database/             # SQLite database file (etl_pipeline.db)
```

---

## 🚀 Quickstart Guide

### 1. Run the Pipeline from the Command Line

Run an end-to-end pipeline ingestion extracting from **both** API and CSV:
```bash
python pipeline.py --source both
```

Or ingest specifically from either source:
```bash
# Ingest only from the Open-Meteo REST API
python pipeline.py --source api

# Ingest only from the CSV dataset
python pipeline.py --source csv
```

### 2. View Database Inventory & Summary
```bash
python pipeline.py --summary
```

### 3. Run Custom SQL Queries Directly
```bash
python pipeline.py --query "SELECT city, AVG(temperature_c) FROM weather_observations GROUP BY city;"
```

### 4. Launch the Interactive Web Dashboard
Run the Streamlit studio to inspect data visually, run queries, and compare raw vs cleaned data:
```bash
streamlit run app.py
```
Open your browser at `http://localhost:8501`.

### 5. Run the Test Suite
```bash
python -m pytest tests/test_pipeline.py -v
```

---

## 🧠 Core ETL Concepts Explained

### 1. Extract (E)
- **API Extraction (`api_extractor.py`)**: Demonstrates fetching data from public REST APIs, handling HTTP query parameters, retry timeouts, and saving the raw JSON payload to a **landing zone** (`data/raw/`) before touching the data.
- **CSV Ingestion (`csv_extractor.py`)**: Demonstrates handling messy real-world CSV files with unknown encodings (UTF-8 vs Latin-1), custom null representations (`-999`, `N/A`, `missing`), and delimiter sniffing.

### 2. Transform (T)
- **Schema Standardization**: Normalizes column names (e.g. `City` and `city`, `Observed_At` and `timestamp`) into a unified canonical schema before merging.
- **Numeric Stripping**: Strips embedded units (`"24.5 C"` -> `24.5`, `"65%"` -> `65.0`) using regular expressions.
- **Mixed Datetime Parsing**: Uses Pandas `pd.to_datetime(format='mixed', errors='coerce')` to parse disparate formats (`YYYY/MM/DD`, `DD-MM-YYYY`, ISO8601) while safely flagging genuinely corrupted dates.
- **Outlier Sanitization**: Identifies sensor failures (e.g. `999.0°C` or `-888.0°C`) and replaces them with NaN.
- **Grouped Imputation**: Imputes missing values by computing the median per city (`df.groupby('city')['temperature_c'].transform(...)`).
- **Feature Engineering (`enricher.py`)**:
  - Computes temperature in Fahrenheit: $T_F = T_C \times 1.8 + 32$
  - Computes simplified **Heat Index** and **Wind Chill**
  - Categorizes into comfort levels (`Freezing`, `Cold`, `Mild`, `Warm`, `Hot`)
  - Translates numeric WMO codes into human-readable conditions (`Clear sky`, `Moderate rain`)
- **Data Quality Gates**: Automated checks verify non-null primary keys, correct data types, and valid physical ranges before database loading.

### 3. Load (L)
- **Idempotence & Upsert**: Re-running a pipeline should never duplicate records or fail on primary key collisions. In `loaders/db_loader.py`, we implement:
  - **SQLite**: `sqlite_insert(WeatherObservation).on_conflict_do_update(...)`
  - **PostgreSQL**: `pg_insert(WeatherObservation).on_conflict_do_update(...)`
- **Audit Logging**: Every pipeline run is logged in the `etl_pipeline_runs` table with rows extracted, rows transformed, rows loaded, execution time, and any error tracebacks.

---

## 🐘 Switching from SQLite to PostgreSQL

The pipeline uses SQLAlchemy ORM, meaning switching databases requires only setting the `DATABASE_URL` environment variable.

### In PowerShell (Windows):
```powershell
$env:DATABASE_URL="postgresql+psycopg://myuser:mypassword@localhost:5432/weather_db"
python pipeline.py --source both
```

### In Bash (Linux/macOS):
```bash
export DATABASE_URL="postgresql+psycopg://myuser:mypassword@localhost:5432/weather_db"
python pipeline.py --source both
```

The database tables and indexes will be created automatically on the target PostgreSQL database upon first connection!

---

## 📊 Analytical SQL Query Examples

You can run these in `pipeline.py --query "..."` or inside the Streamlit SQL Console:

```sql
-- Average, Min, and Max Temperature by City
SELECT 
    city,
    country,
    COUNT(*) as readings,
    ROUND(AVG(temperature_c), 1) as avg_temp_c,
    ROUND(MIN(temperature_c), 1) as min_temp_c,
    ROUND(MAX(temperature_c), 1) as max_temp_c
FROM weather_observations
GROUP BY city, country
ORDER BY avg_temp_c DESC;

-- Cities with precipitation recorded
SELECT city, observation_time, precipitation_mm, weather_condition, wind_speed_kmh
FROM weather_observations
WHERE is_precipitation = 1
ORDER BY precipitation_mm DESC;

-- Pipeline execution performance and throughput
SELECT id, pipeline_name, source_name, status, rows_loaded, duration_seconds, started_at
FROM etl_pipeline_runs
ORDER BY id DESC;
```
