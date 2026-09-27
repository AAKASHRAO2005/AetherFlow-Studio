"""
Database Schema definitions using SQLAlchemy.
Defines tables for Weather Data, CSV Datasets, and ETL Execution Audit Runs.
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, Text, Index, UniqueConstraint
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)



class WeatherObservation(Base):
    """
    Stores cleaned and enriched weather observations from API or CSV sources.
    Uses composite uniqueness (city + observation_time) to enable idempotent upserts.
    """
    __tablename__ = "weather_observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False, index=True)
    country = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    observation_time = Column(DateTime, nullable=False, index=True)
    
    # Core Meteorological Metrics
    temperature_c = Column(Float, nullable=False)
    feels_like_c = Column(Float, nullable=True)
    temperature_f = Column(Float, nullable=False)
    humidity_percent = Column(Float, nullable=False)
    precipitation_mm = Column(Float, default=0.0)
    wind_speed_kmh = Column(Float, nullable=False)
    weather_code = Column(Integer, nullable=True)
    weather_condition = Column(String(100), nullable=False)
    
    # Derived / Enriched Features
    comfort_index = Column(String(50), nullable=True)  # Freezing, Cold, Mild, Warm, Hot
    heat_index_c = Column(Float, nullable=True)
    wind_chill_c = Column(Float, nullable=True)
    is_precipitation = Column(Integer, default=0)       # 1 if precip > 0 else 0
    
    # Audit & Lineage Tracking
    source_type = Column(String(50), default="API")     # "API" or "CSV"
    ingestion_timestamp = Column(DateTime, default=utc_now, nullable=False)

    __table_args__ = (
        UniqueConstraint("city", "observation_time", name="uq_city_observation_time"),
        Index("idx_city_time", "city", "observation_time"),
    )

    def __repr__(self):
        return f"<WeatherObservation(city='{self.city}', time='{self.observation_time}', temp={self.temperature_c}°C)>"


class EtlPipelineRun(Base):
    """
    Audit log table tracking every ETL pipeline execution.
    Teaches observability, data lineage, and failure logging.
    """
    __tablename__ = "etl_pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(100), nullable=False)
    source_name = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)          # SUCCESS, FAILED, RUNNING
    rows_extracted = Column(Integer, default=0)
    rows_transformed = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    duration_seconds = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=utc_now)
    completed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<EtlPipelineRun(pipeline='{self.pipeline_name}', status='{self.status}', loaded={self.rows_loaded})>"
