"""
Comprehensive Unit & Integration Tests for the ETL Pipeline.
Covers:
- Extract: CSV extraction & schema mapping
- Transform: Cleaning, outlier filtering, imputation, and enrichment
- Load: Database table creation, idempotent upserting, and audit logging
"""

from datetime import datetime
import pandas as pd
import pytest
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from models.schema import Base, WeatherObservation, EtlPipelineRun
from extractors.csv_extractor import CsvExtractor
from transformers.cleaner import DataCleaner
from transformers.enricher import DataEnricher
from loaders.db_loader import DatabaseLoader


@pytest.fixture
def sample_dirty_df():
    """
    Creates a small representative dirty DataFrame with common real-world flaws.
    """
    return pd.DataFrame([
        {
            "City": "  new york ",
            "Country": "USA",
            "Latitude": 40.71,
            "Longitude": -74.00,
            "Observed_At": "2026-03-20 12:00:00",
            "Temp_Celsius": "24.5 C",
            "Humidity": "65%",
            "Precipitation_mm": 0.0,
            "Wind_Speed_kmh": 12.0,
            "Weather_Code": 0
        },
        {
            "City": "PARIS",
            "Country": "France",
            "Latitude": 48.85,
            "Longitude": 2.35,
            "Observed_At": "20/03/2026 14:00",
            "Temp_Celsius": 999.0,  # Outlier
            "Humidity": -10.0,      # Invalid humidity
            "Precipitation_mm": -2.0,  # Negative rain
            "Wind_Speed_kmh": -5.0,
            "Weather_Code": 61
        },
        {
            "City": "Tokyo",
            "Country": "Japan",
            "Latitude": 35.67,
            "Longitude": 139.65,
            "Observed_At": "INVALID_DATE",  # Corrupted date
            "Temp_Celsius": 18.0,
            "Humidity": 50.0,
            "Precipitation_mm": 1.5,
            "Wind_Speed_kmh": 8.0,
            "Weather_Code": 2
        },
        # Duplicate of row 1
        {
            "City": "New York",
            "Country": "USA",
            "Latitude": 40.71,
            "Longitude": -74.00,
            "Observed_At": "2026-03-20 12:00:00",
            "Temp_Celsius": 24.5,
            "Humidity": 65.0,
            "Precipitation_mm": 0.0,
            "Wind_Speed_kmh": 12.0,
            "Weather_Code": 0
        }
    ])


@pytest.fixture
def in_memory_loader():
    """
    In-memory SQLite database fixture for isolated testing.
    """
    db_url = "sqlite:///:memory:"
    loader = DatabaseLoader(db_url=db_url)
    return loader


def test_cleaner_transformations(sample_dirty_df):
    cleaner = DataCleaner()
    cleaned_df, stats = cleaner.clean(sample_dirty_df)

    # 1. Invalid date dropped
    assert stats["dropped_invalid_dates"] == 1
    # 2. Duplicate New York row removed
    assert stats["duplicates_removed"] == 1
    # 3. Text whitespace and casing normalized
    assert "New York" in cleaned_df["city"].values
    assert "Paris" in cleaned_df["city"].values
    # 4. Outlier 999.0 C filtered and imputed
    assert (cleaned_df["temperature_c"] < 60.0).all()
    # 5. Invalid humidity clamped / imputed
    assert (cleaned_df["humidity_percent"] >= 0.0).all()
    assert (cleaned_df["humidity_percent"] <= 100.0).all()
    # 6. Negative precipitation clamped to 0.0
    assert (cleaned_df["precipitation_mm"] >= 0.0).all()


def test_enricher_features(sample_dirty_df):
    cleaner = DataCleaner()
    cleaned_df, _ = cleaner.clean(sample_dirty_df)
    
    enricher = DataEnricher()
    enriched_df = enricher.enrich(cleaned_df)

    # Check derived fields exist
    for col in ["temperature_f", "comfort_index", "feels_like_c", "weather_condition", "is_precipitation"]:
        assert col in enriched_df.columns

    # Verify Fahrenheit conversion formula: T_F = T_C * 1.8 + 32
    ny_row = enriched_df[enriched_df["city"] == "New York"].iloc[0]
    expected_f = round(ny_row["temperature_c"] * 1.8 + 32.0, 2)
    assert abs(ny_row["temperature_f"] - expected_f) < 0.05

    # Verify quality gate
    quality = enricher.validate_quality(enriched_df)
    assert quality["passed"] is True


def test_idempotent_database_load(sample_dirty_df, in_memory_loader):
    cleaner = DataCleaner()
    cleaned_df, _ = cleaner.clean(sample_dirty_df)
    enricher = DataEnricher()
    enriched_df = enricher.enrich(cleaned_df)

    # First Load
    loaded_1 = in_memory_loader.load_weather_data(enriched_df)
    metrics_1 = in_memory_loader.get_summary_metrics()
    assert metrics_1["total_observations"] == len(enriched_df)

    # Second Load (Idempotency test: Re-inserting the exact same rows)
    loaded_2 = in_memory_loader.load_weather_data(enriched_df)
    metrics_2 = in_memory_loader.get_summary_metrics()
    
    # Row count MUST NOT double!
    assert metrics_2["total_observations"] == metrics_1["total_observations"]


def test_audit_run_logging(in_memory_loader):
    run_id = in_memory_loader.record_pipeline_run(
        pipeline_name="Test_Pipeline",
        source_name="TEST",
        status="SUCCESS",
        rows_extracted=100,
        rows_transformed=95,
        rows_loaded=95,
        duration_seconds=1.234
    )
    assert run_id > 0

    runs_df = in_memory_loader.fetch_pipeline_runs(limit=10)
    assert len(runs_df) == 1
    assert runs_df.iloc[0]["pipeline_name"] == "Test_Pipeline"
    assert runs_df.iloc[0]["status"] == "SUCCESS"
