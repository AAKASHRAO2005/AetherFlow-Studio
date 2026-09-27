"""
Interactive Streamlit Dashboard for the ETL Pipeline.
Features:
- Interactive Pipeline Execution (API, CSV, or Both)
- Raw vs Transformed Data Inspection (before & after data cleaning)
- Database Analytics & Interactive Visual Charts
- Live SQL Query Console (with pre-built query templates)
- Audit & Pipeline Lineage Logs
- Synthetic Data Flaw Generator
"""

import time
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

from config import SQLITE_DB_PATH, DATABASE_URL, RAW_DATA_DIR, TARGET_CITIES, PROJECT_NAME, PROJECT_TAGLINE
from extractors.api_extractor import ApiExtractor
from extractors.csv_extractor import CsvExtractor
from transformers.cleaner import DataCleaner
from transformers.enricher import DataEnricher
from loaders.db_loader import DatabaseLoader
from pipeline import EtlPipeline
from generate_dirty_data import generate_dirty_weather_csv

st.set_page_config(
    page_title=f"{PROJECT_NAME} Studio | CSV/API to Database",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.3rem;
        font-weight: 800;
        background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        color: #94a3b8;
        font-size: 1rem;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #1e293b;
        border-radius: 8px;
        padding: 16px;
        border: 1px solid #334155;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 600;
        border-radius: 6px;
        padding: 8px 16px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_loader():
    return DatabaseLoader(db_url=DATABASE_URL)


loader = get_loader()

# Sidebar: Controls & Architecture Guide
with st.sidebar:
    st.markdown(f"### 🌊 {PROJECT_NAME}")
    st.caption("Atmospheric Data Ingestion & Transformation Engine")

    source_option = st.selectbox(
        "Ingestion Source",
        options=["both", "api", "csv"],
        format_func=lambda x: {
            "both": "🌐 Both (API + CSV Dataset)",
            "api": "☁️ Public Weather API (Open-Meteo)",
            "csv": "📄 Dirty CSV Dataset"
        }[x]
    )

    st.markdown("---")
    st.markdown("#### ⚙️ Pipeline Execution")
    
    if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
        pipeline = EtlPipeline(db_url=DATABASE_URL)
        with st.spinner(f"Running ETL Pipeline from {source_option.upper()}..."):
            result = pipeline.run_pipeline(source=source_option)
            if result["status"] == "SUCCESS":
                st.success(f"Pipeline Run #{result['run_id']} Completed! ({result['duration']:.2f}s)")
            else:
                st.error(f"Pipeline Failed: {result.get('error')}")

    if st.button("🎲 Generate Fresh Dirty CSV", use_container_width=True):
        with st.spinner("Generating dirty synthetic data..."):
            path = generate_dirty_weather_csv(num_rows=100)
            st.info(f"Generated new dirty dataset with 100 rows!")

    st.markdown("---")
    st.markdown("#### 📚 Core ETL Concepts")
    with st.expander("1. Extract (E)", expanded=False):
        st.write("""
        - **REST API Extraction**: Queries Open-Meteo public endpoints with geo-coordinates.
        - **CSV Ingestion**: Handles delimiters, encodings, and missing tokens (`-999`, `N/A`).
        - **Landing Zone**: Saves raw JSON snapshots in `data/raw/` for lineage.
        """)
    with st.expander("2. Transform (T)", expanded=False):
        st.write("""
        - **Data Cleaning**: Strips whitespace, normalizes casing, cleans units (`24°C` -> `24.0`).
        - **Timestamp Normalization**: Parses mixed formats (`YYYY/MM/DD`, `DD-MM-YYYY`).
        - **Outlier Filtering**: Cleans sensor errors (>60°C or < -60°C).
        - **Imputation**: Fills missing values with city-specific medians.
        - **Enrichment**: Computes Heat Index, Wind Chill, Comfort Index, WMO descriptions.
        """)
    with st.expander("3. Load (L)", expanded=False):
        st.write("""
        - **Idempotent Upsert**: `ON CONFLICT (city, observation_time) DO UPDATE`.
        - **Audit Logging**: Logs every execution to `etl_pipeline_runs`.
        - **Dual Support**: Works with SQLite or PostgreSQL seamlessly.
        """)


# Main Header
st.markdown(f'<div class="main-header">🌊 {PROJECT_NAME} Studio</div>', unsafe_allow_html=True)
st.markdown(f'<div class="sub-header">{PROJECT_TAGLINE} &middot; Pulling public API & CSV records, performing Pandas cleaning & enrichment, and loading into SQLite/PostgreSQL with idempotent upserts.</div>', unsafe_allow_html=True)

# Overview KPI Metrics
metrics = loader.get_summary_metrics()
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total DB Records", f"{metrics['total_observations']:,}")
with col2:
    st.metric("Active Cities", f"{metrics['distinct_cities']}")
with col3:
    st.metric("Total Pipeline Runs", f"{metrics['total_runs']}")
with col4:
    success_rate = (metrics['successful_runs'] / metrics['total_runs'] * 100) if metrics['total_runs'] > 0 else 0
    st.metric("Pipeline Success Rate", f"{success_rate:.0f}%")

st.markdown("---")

# Navigation Tabs
tab_data, tab_diff, tab_sql, tab_runs, tab_arch = st.tabs([
    "📊 Database & Analytics",
    "🔍 Raw vs Cleaned Diff",
    "💻 SQL Query Console",
    "📜 Pipeline Audit Runs",
    "🏗️ Pipeline Architecture"
])

# TAB 1: Database & Analytics
with tab_data:
    st.subheader("Database Inventory & Visualizations")
    df_obs = loader.fetch_recent_observations(limit=500)

    if df_obs.empty:
        st.warning("Database currently has 0 observations. Click '🚀 Run Pipeline' in the sidebar to populate data!")
    else:
        # Visual Analytics
        c1, c2 = st.columns([1, 1])

        with c1:
            fig_temp = px.box(
                df_obs,
                x="city",
                y="temperature_c",
                color="city",
                title="Temperature Distribution by City (°C)",
                points="all"
            )
            fig_temp.update_layout(showlegend=False, margin=dict(t=40, b=20, l=20, r=20))
            st.plotly_chart(fig_temp, use_container_width=True)

        with c2:
            comfort_counts = df_obs["comfort_index"].value_counts().reset_index()
            comfort_counts.columns = ["comfort_index", "count"]
            fig_comfort = px.pie(
                comfort_counts,
                names="comfort_index",
                values="count",
                title="Comfort Index Distribution",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_comfort.update_layout(margin=dict(t=40, b=20, l=20, r=20))
            st.plotly_chart(fig_comfort, use_container_width=True)

        c3, c4 = st.columns([1, 1])
        with c3:
            fig_scatter = px.scatter(
                df_obs,
                x="temperature_c",
                y="humidity_percent",
                color="comfort_index",
                hover_data=["city", "weather_condition"],
                title="Temperature vs Humidity Correlation"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        with c4:
            source_counts = df_obs["source_type"].value_counts().reset_index()
            source_counts.columns = ["source_type", "records"]
            fig_source = px.bar(
                source_counts,
                x="source_type",
                y="records",
                color="source_type",
                title="Observations Ingested by Source"
            )
            st.plotly_chart(fig_source, use_container_width=True)

        st.markdown("#### 📋 Latest Database Records")
        st.dataframe(df_obs, use_container_width=True)


# TAB 2: Raw vs Cleaned Diff
with tab_diff:
    st.subheader("Data Cleaning Inspection: Raw vs Cleaned")
    st.caption("See how real-world data flaws (outliers, mixed date formats, casing, sentinels) are transformed.")

    try:
        raw_csv_df = CsvExtractor().extract()
        cleaner = DataCleaner()
        cleaned_csv_df, stats = cleaner.clean(raw_csv_df)
        enricher = DataEnricher()
        enriched_csv_df = enricher.enrich(cleaned_csv_df)

        stat_col1, stat_col2, stat_col3, stat_col4, stat_col5 = st.columns(5)
        stat_col1.metric("Raw Rows", stats["initial_rows"])
        stat_col2.metric("Dropped Corrupted Dates", stats["dropped_invalid_dates"])
        stat_col3.metric("Outliers Sanitized", stats["outliers_cleaned"])
        stat_col4.metric("Nulls Imputed", stats["nulls_imputed"])
        stat_col5.metric("Duplicates Dropped", stats["duplicates_removed"])

        col_left, col_right = st.columns(2)
        with col_left:
            st.markdown("##### 🔴 Raw Dataset (with flaws)")
            st.dataframe(raw_csv_df.head(20), use_container_width=True)

        with col_right:
            st.markdown("##### 🟢 Cleaned & Enriched Dataset")
            st.dataframe(enriched_csv_df.head(20), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading raw CSV diff: {e}")


# TAB 3: SQL Query Console
with tab_sql:
    st.subheader("Interactive SQL Query Console")
    st.caption("Query the SQLite database directly using analytical SQL.")

    templates = {
        "City Weather Summary": """
SELECT 
    city,
    country,
    COUNT(*) as observation_count,
    ROUND(AVG(temperature_c), 1) as avg_temp_c,
    ROUND(MIN(temperature_c), 1) as min_temp_c,
    ROUND(MAX(temperature_c), 1) as max_temp_c,
    ROUND(AVG(humidity_percent), 1) as avg_humidity
FROM weather_observations
GROUP BY city, country
ORDER BY avg_temp_c DESC;
""",
        "Hottest Observations Recorded": """
SELECT 
    city, observation_time, temperature_c, temperature_f,
    humidity_percent, weather_condition, comfort_index
FROM weather_observations
ORDER BY temperature_c DESC
LIMIT 10;
""",
        "Precipitation & Rainy Conditions": """
SELECT 
    city, observation_time, precipitation_mm, weather_condition, wind_speed_kmh
FROM weather_observations
WHERE is_precipitation = 1
ORDER BY precipitation_mm DESC
LIMIT 15;
""",
        "Recent Pipeline Audit Runs": """
SELECT 
    id, pipeline_name, source_name, status,
    rows_extracted, rows_loaded, duration_seconds, started_at
FROM etl_pipeline_runs
ORDER BY id DESC
LIMIT 10;
"""
    }

    selected_template = st.selectbox("Select SQL Query Template", list(templates.keys()))
    query_input = st.text_area("SQL Statement", value=templates[selected_template].strip(), height=160)

    if st.button("▶ Run SQL Query", type="primary"):
        try:
            query_res = loader.execute_custom_sql(query_input)
            st.success(f"Query returned {len(query_res)} rows.")
            st.dataframe(query_res, use_container_width=True)
        except Exception as e:
            st.error(f"SQL Execution Error: {e}")


# TAB 4: Pipeline Audit Runs
with tab_runs:
    st.subheader("Data Lineage & Pipeline Audit Log")
    st.caption("Every pipeline run tracks row counts, duration, and status to ensure observability.")

    runs_df = loader.fetch_pipeline_runs(limit=30)
    if runs_df.empty:
        st.info("No pipeline executions logged yet.")
    else:
        styler = runs_df.style
        style_fn = lambda val: "color: #22c55e; font-weight: bold;" if val == "SUCCESS" else ("color: #ef4444; font-weight: bold;" if val == "FAILED" else "")
        if hasattr(styler, "map"):
            styled_df = styler.map(style_fn, subset=["status"])
        else:
            styled_df = styler.applymap(style_fn, subset=["status"])
        st.dataframe(styled_df, use_container_width=True)


# TAB 5: Pipeline Architecture
with tab_arch:
    st.subheader("End-to-End ETL Architecture")
    
    st.markdown("""
```mermaid
graph LR
    subgraph Extract [Extract Phase]
        API[Open-Meteo REST API] --> |JSON Payloads| Extractor1[ApiExtractor]
        CSV[Dirty CSV Datasets] --> |Raw Delimited| Extractor2[CsvExtractor]
        Extractor1 --> LandingZone[(data/raw/ Landing Zone)]
    end

    subgraph Transform [Transform Phase]
        Extractor1 & Extractor2 --> Standardize[Schema Standardization]
        Standardize --> Cleaner[DataCleaner: Impute, Outliers, Dedup]
        Cleaner --> Enricher[DataEnricher: Heat Index, Comfort, WMO]
        Enricher --> QualityGate{Data Quality Gate}
    end

    subgraph Load [Load Phase]
        QualityGate --> |Passed| Upsert[DatabaseLoader: Idempotent Upsert]
        Upsert --> DB[(SQLite / PostgreSQL)]
        Upsert --> Audit[(etl_pipeline_runs Audit Table)]
    end
```
    """, unsafe_allow_html=False)

    st.markdown("""
### 🧠 Key Engineering Principles Implemented:
1. **Separation of Concerns**: Dedicated packages for `extractors`, `transformers`, `loaders`, and `models`.
2. **Idempotence**: Re-running the pipeline on the same data never creates duplicate rows; it safely updates records using composite key `(city, observation_time)`.
3. **Landing Zone Raw Preservation**: Saves verbatim JSON responses to `data/raw/` for reproducibility and data auditing.
4. **Data Quality Gates**: Automated validation checks on temperature ranges, null keys, and valid types before database loading.
5. **Observability & Auditing**: Tracks start/end timestamps, row throughput, and exceptions in `etl_pipeline_runs`.
6. **Portability**: Works out of the box with zero-setup SQLite, with immediate switch to PostgreSQL via `DATABASE_URL` environment variable.
""")
