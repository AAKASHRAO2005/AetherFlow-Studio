"""
Database Loader Module.
Part of the Load phase in ETL.
Handles:
- Database schema initialization
- Batch loading with Idempotent Upsert (ON CONFLICT DO UPDATE) for both SQLite and PostgreSQL
- Pipeline Run metadata auditing
- Querying and analytical inspection helpers
"""

from datetime import datetime, timezone
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import DATABASE_URL
from models.schema import Base, WeatherObservation, EtlPipelineRun

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Loads transformed data into SQLite or PostgreSQL databases.
    Implements idempotent upsert to ensure re-running the pipeline is safe.
    """

    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.is_sqlite = db_url.startswith("sqlite")
        self.engine = create_engine(
            self.db_url,
            echo=False,
            # SQLite connection thread check disabled for multithreaded apps like Streamlit
            connect_args={"check_same_thread": False} if self.is_sqlite else {}
        )
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.init_database()

    def init_database(self) -> None:
        """
        Creates all tables defined in models.schema if they do not exist.
        """
        logger.info(f"Initializing database tables on {self.db_url}...")
        Base.metadata.create_all(self.engine)
        logger.info("Database schema initialized successfully.")

    def load_weather_data(self, df: pd.DataFrame, batch_size: int = 500) -> int:
        """
        Loads cleaned weather records using an idempotent UPSERT pattern.
        If a record with (city, observation_time) exists, update metrics; otherwise insert.
        """
        if df.empty:
            logger.warning("Empty dataframe provided to load_weather_data.")
            return 0

        # Filter down to matching model columns
        valid_cols = [c.name for c in WeatherObservation.__table__.columns if c.name != "id"]
        available_cols = [c for c in valid_cols if c in df.columns]
        
        subset_df = df[available_cols].copy()
        # Convert NaN to None for proper SQL NULL handling
        records = subset_df.replace({pd.NA: None, float("nan"): None}).to_dict(orient="records")

        total_loaded = 0
        with self.engine.begin() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                if not batch:
                    continue

                if self.is_sqlite:
                    stmt = sqlite_insert(WeatherObservation).values(batch)
                    update_dict = {
                        c.name: c for c in stmt.excluded
                        if c.name not in ["id", "city", "observation_time"]
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["city", "observation_time"],
                        set_=update_dict
                    )
                    conn.execute(stmt)
                else:
                    # PostgreSQL dialect
                    stmt = pg_insert(WeatherObservation).values(batch)
                    update_dict = {
                        c.name: c for c in stmt.excluded
                        if c.name not in ["id", "city", "observation_time"]
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["city", "observation_time"],
                        set_=update_dict
                    )
                    conn.execute(stmt)

                total_loaded += len(batch)

        logger.info(f"Successfully upserted {total_loaded} records into {WeatherObservation.__tablename__}.")
        return total_loaded

    def record_pipeline_run(
        self,
        pipeline_name: str,
        source_name: str,
        status: str,
        rows_extracted: int,
        rows_transformed: int,
        rows_loaded: int,
        duration_seconds: float,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None
    ) -> int:
        """
        Records an audit entry in the etl_pipeline_runs table.
        """
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.SessionFactory() as session:
            run_entry = EtlPipelineRun(
                pipeline_name=pipeline_name,
                source_name=source_name,
                status=status,
                rows_extracted=rows_extracted,
                rows_transformed=rows_transformed,
                rows_loaded=rows_loaded,
                duration_seconds=round(duration_seconds, 3),
                error_message=error_msg if (error_msg := error_message) else None,
                started_at=started_at or now_utc,
                completed_at=now_utc
            )
            session.add(run_entry)
            session.commit()
            return run_entry.id

    def get_summary_metrics(self) -> Dict[str, Any]:
        """
        Returns high-level statistics for reporting and dashboard views.
        """
        with self.SessionFactory() as session:
            total_observations = session.query(func.count(WeatherObservation.id)).scalar() or 0
            distinct_cities = session.query(func.count(func.distinct(WeatherObservation.city))).scalar() or 0
            total_runs = session.query(func.count(EtlPipelineRun.id)).scalar() or 0
            successful_runs = session.query(func.count(EtlPipelineRun.id)).filter(EtlPipelineRun.status == "SUCCESS").scalar() or 0

            return {
                "total_observations": total_observations,
                "distinct_cities": distinct_cities,
                "total_runs": total_runs,
                "successful_runs": successful_runs
            }

    def fetch_recent_observations(self, limit: int = 100) -> pd.DataFrame:
        """
        Fetches the latest loaded observations as a pandas DataFrame.
        """
        query = f"""
        SELECT 
            city, country, observation_time, temperature_c, temperature_f,
            humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition,
            comfort_index, source_type, ingestion_timestamp
        FROM {WeatherObservation.__tablename__}
        ORDER BY observation_time DESC
        LIMIT {limit};
        """
        return pd.read_sql_query(query, con=self.engine)

    def fetch_pipeline_runs(self, limit: int = 20) -> pd.DataFrame:
        """
        Fetches recent pipeline audit run logs.
        """
        query = f"""
        SELECT 
            id, pipeline_name, source_name, status,
            rows_extracted, rows_transformed, rows_loaded,
            duration_seconds, started_at, completed_at, error_message
        FROM {EtlPipelineRun.__tablename__}
        ORDER BY id DESC
        LIMIT {limit};
        """
        return pd.read_sql_query(query, con=self.engine)

    def execute_custom_sql(self, sql_query: str) -> pd.DataFrame:
        """
        Executes an arbitrary analytical SQL query for education and exploration.
        """
        return pd.read_sql_query(sql_query, con=self.engine)
