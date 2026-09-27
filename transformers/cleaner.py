"""
Data Cleaning & Normalization Module.
Core component of the Transform phase in ETL.
Handles:
- Column standardization & mapping
- Whitespace stripping and string casing normalization
- Regex-based numeric extraction (stripping units like 'C', 'km/h', '%')
- Resilient datetime parsing and timezone normalization
- Outlier filtering (extreme sensor readings)
- Null value imputation (median grouping by city)
- Deduplication across primary keys
"""

import logging
import re
from typing import Tuple, Dict, Any
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Cleans raw DataFrame extracted from either API or CSV.
    """

    COLUMN_MAPPING = {
        # CSV variations
        "city": "city",
        "country": "country",
        "latitude": "latitude",
        "longitude": "longitude",
        "observed_at": "observation_time",
        "temp_celsius": "temperature_c",
        "humidity": "humidity_percent",
        "precipitation_mm": "precipitation_mm",
        "wind_speed_kmh": "wind_speed_kmh",
        "weather_code": "weather_code",
        # API variations
        "timestamp": "observation_time",
        "temperature": "temperature_c",
        "apparent_temperature": "feels_like_c",
        "precipitation": "precipitation_mm",
        "wind_speed": "wind_speed_kmh",
    }

    def __init__(self):
        self.stats: Dict[str, Any] = {
            "initial_rows": 0,
            "dropped_invalid_dates": 0,
            "duplicates_removed": 0,
            "outliers_cleaned": 0,
            "nulls_imputed": 0,
            "final_rows": 0
        }

    @staticmethod
    def _clean_numeric_string(val: Any) -> Any:
        """
        Strips unit symbols (%, C, km/h, etc.) and extracts pure float.
        """
        if pd.isna(val):
            return np.nan
        if isinstance(val, (int, float)):
            return float(val)
        
        # String cleanup
        text = str(val).strip()
        # Match leading or embedded numeric pattern e.g. '24.5 C', '-5.2', '75%'
        match = re.search(r"[-+]?\d*\.?\d+", text)
        if match:
            try:
                return float(match.group())
            except ValueError:
                return np.nan
        return np.nan

    @classmethod
    def standardize_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes column names to lowercase and standardizes to target schema names.
        """
        if df.empty:
            return df
        res = df.copy()
        res.columns = [str(c).strip().lower() for c in res.columns]
        res.rename(columns=cls.COLUMN_MAPPING, inplace=True)
        return res

    def clean(self, df: pd.DataFrame, source_type: str = "CSV") -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Executes full data cleaning pipeline on raw DataFrame.
        Returns cleaned DataFrame and transformation audit statistics.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to DataCleaner.")
            return df, self.stats

        cleaned = self.standardize_columns(df)
        self.stats["initial_rows"] = len(cleaned)

        # Ensure required columns exist
        expected_cols = [
            "city", "country", "latitude", "longitude",
            "observation_time", "temperature_c", "humidity_percent",
            "precipitation_mm", "wind_speed_kmh", "weather_code"
        ]
        for col in expected_cols:
            if col not in cleaned.columns:
                cleaned[col] = np.nan

        # 2. Text standardization: strip whitespace and normalize casing
        cleaned["city"] = cleaned["city"].astype(str).str.strip().str.title()
        cleaned["country"] = cleaned["country"].astype(str).str.strip().str.title()

        # 3. Numeric string sanitization
        numeric_columns = [
            "latitude", "longitude", "temperature_c", "humidity_percent",
            "precipitation_mm", "wind_speed_kmh", "weather_code"
        ]
        if "feels_like_c" in cleaned.columns:
            numeric_columns.append("feels_like_c")

        for col in numeric_columns:
            cleaned[col] = cleaned[col].apply(self._clean_numeric_string)

        # 4. Datetime parsing with mixed format resilience
        # Coerce invalid dates (e.g. 'INVALID_TIMESTAMP') to NaT
        cleaned["observation_time"] = pd.to_datetime(cleaned["observation_time"], format="mixed", errors="coerce", utc=True)
        # Convert to naive UTC datetime format for database compatibility
        cleaned["observation_time"] = cleaned["observation_time"].dt.tz_convert(None)

        # Drop rows where observation_time or city is null (these violate DB primary key integrity)
        invalid_time_mask = cleaned["observation_time"].isna() | (cleaned["city"] == "Nan") | (cleaned["city"] == "")
        dropped_count = invalid_time_mask.sum()
        self.stats["dropped_invalid_dates"] = int(dropped_count)
        if dropped_count > 0:
            logger.info(f"Dropping {dropped_count} rows with invalid dates or missing city names.")
            cleaned = cleaned[~invalid_time_mask].copy()

        # 5. Outlier Detection & Range Validation
        # Valid Earth surface temperature: -60°C to +60°C
        temp_outlier_mask = (cleaned["temperature_c"] < -60.0) | (cleaned["temperature_c"] > 60.0)
        self.stats["outliers_cleaned"] += int(temp_outlier_mask.sum())
        cleaned.loc[temp_outlier_mask, "temperature_c"] = np.nan

        # Valid humidity: 0% to 100%
        hum_outlier_mask = (cleaned["humidity_percent"] < 0.0) | (cleaned["humidity_percent"] > 100.0)
        self.stats["outliers_cleaned"] += int(hum_outlier_mask.sum())
        cleaned.loc[hum_outlier_mask, "humidity_percent"] = np.nan

        # Precipitation & Wind cannot be negative
        cleaned["precipitation_mm"] = cleaned["precipitation_mm"].apply(lambda x: 0.0 if pd.isna(x) or x < 0 else round(x, 2))
        cleaned["wind_speed_kmh"] = cleaned["wind_speed_kmh"].apply(lambda x: 0.0 if pd.isna(x) or x < 0 else round(x, 1))

        # 6. Null Handling / Imputation
        # For missing temperatures, impute using city median, then fallback to global median
        null_temps_before = cleaned["temperature_c"].isna().sum()
        cleaned["temperature_c"] = cleaned.groupby("city")["temperature_c"].transform(lambda g: g.fillna(g.median()))
        cleaned["temperature_c"] = cleaned["temperature_c"].fillna(cleaned["temperature_c"].median()).fillna(20.0)
        cleaned["temperature_c"] = cleaned["temperature_c"].round(2)

        # For missing humidity, impute using city median, then fallback to global median
        cleaned["humidity_percent"] = cleaned.groupby("city")["humidity_percent"].transform(lambda g: g.fillna(g.median()))
        cleaned["humidity_percent"] = cleaned["humidity_percent"].fillna(cleaned["humidity_percent"].median()).fillna(55.0)
        cleaned["humidity_percent"] = cleaned["humidity_percent"].round(1)

        nulls_fixed = int(null_temps_before - cleaned["temperature_c"].isna().sum())
        self.stats["nulls_imputed"] = max(0, nulls_fixed)

        # 7. Deduplication
        # Sort by observation_time descending so we retain the latest record for each city & time
        cleaned.sort_values(by=["city", "observation_time"], ascending=[True, False], inplace=True)
        initial_before_dedup = len(cleaned)
        cleaned.drop_duplicates(subset=["city", "observation_time"], keep="first", inplace=True)
        self.stats["duplicates_removed"] = int(initial_before_dedup - len(cleaned))

        # Add source tag
        cleaned["source_type"] = source_type

        self.stats["final_rows"] = len(cleaned)
        logger.info(
            f"Data Cleaning finished: {self.stats['initial_rows']} initial rows -> "
            f"{self.stats['final_rows']} clean rows ({self.stats['duplicates_removed']} duplicates removed, "
            f"{self.stats['outliers_cleaned']} outliers sanitized)."
        )

        return cleaned, self.stats
