"""
Data Enrichment & Validation Module.
Part of the Transform phase.
Adds business logic, derived features, descriptive labels,
and runs data quality validation checks before loading into the database.
"""

from datetime import datetime, timezone
import logging
from typing import Dict, Any, List
import pandas as pd

from config import WMO_WEATHER_CODES

logger = logging.getLogger(__name__)


class DataEnricher:
    """
    Enriches cleaned weather dataset with derived meteorological metrics
    and validates schema integrity.
    """

    @staticmethod
    def _classify_comfort(temp_c: float) -> str:
        """
        Categorizes temperature into human-readable comfort levels.
        """
        if pd.isna(temp_c):
            return "Unknown"
        if temp_c < 0.0:
            return "Freezing"
        elif temp_c < 12.0:
            return "Cold"
        elif temp_c < 20.0:
            return "Mild"
        elif temp_c < 28.0:
            return "Warm"
        else:
            return "Hot"

    @staticmethod
    def _calculate_heat_index(temp_c: float, humidity: float) -> float:
        """
        Simplified heat index approximation for warm, humid conditions.
        """
        if pd.isna(temp_c) or pd.isna(humidity) or temp_c < 20.0:
            return round(temp_c, 2) if not pd.isna(temp_c) else None
        
        # Simplified formula
        hi = temp_c + 0.5555 * ((6.11 * (10 ** (7.5 * temp_c / (237.3 + temp_c))) * (humidity / 100)) - 10)
        return round(hi, 2)

    @staticmethod
    def _calculate_wind_chill(temp_c: float, wind_kmh: float) -> float:
        """
        Calculates wind chill temperature for cold, windy conditions.
        Valid for temperatures <= 10°C and wind speed >= 4.8 km/h.
        """
        if pd.isna(temp_c) or pd.isna(wind_kmh) or temp_c > 10.0 or wind_kmh < 4.8:
            return round(temp_c, 2) if not pd.isna(temp_c) else None
        
        # Standard Environment Canada / NOAA formula in Celsius and km/h
        wc = 13.12 + 0.6215 * temp_c - 11.37 * (wind_kmh ** 0.16) + 0.3965 * temp_c * (wind_kmh ** 0.16)
        return round(wc, 2)

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds derived metrics and feature engineering columns.
        """
        if df.empty:
            return df

        enriched = df.copy()

        # 1. Temperature in Fahrenheit
        enriched["temperature_f"] = (enriched["temperature_c"] * 9.0 / 5.0 + 32.0).round(2)

        # 2. Comfort categorization
        enriched["comfort_index"] = enriched["temperature_c"].apply(self._classify_comfort)

        # 3. Apparent / Feels Like temperature if missing
        if "feels_like_c" not in enriched.columns or enriched["feels_like_c"].isna().all():
            enriched["feels_like_c"] = enriched.apply(
                lambda row: self._calculate_heat_index(row["temperature_c"], row["humidity_percent"])
                if row["temperature_c"] >= 20.0
                else self._calculate_wind_chill(row["temperature_c"], row["wind_speed_kmh"]),
                axis=1
            )
        enriched["feels_like_c"] = enriched["feels_like_c"].fillna(enriched["temperature_c"]).round(2)

        # 4. Heat Index and Wind Chill
        enriched["heat_index_c"] = enriched.apply(
            lambda r: self._calculate_heat_index(r["temperature_c"], r["humidity_percent"]), axis=1
        )
        enriched["wind_chill_c"] = enriched.apply(
            lambda r: self._calculate_wind_chill(r["temperature_c"], r["wind_speed_kmh"]), axis=1
        )

        # 5. Weather Condition Text from WMO Code
        def map_weather_code(code):
            if pd.isna(code):
                return "Partly cloudy"
            code_int = int(code)
            return WMO_WEATHER_CODES.get(code_int, "Variable weather")

        enriched["weather_condition"] = enriched["weather_code"].apply(map_weather_code)

        # 6. Precipitation Flag (binary indicator)
        enriched["is_precipitation"] = (enriched["precipitation_mm"] > 0.0).astype(int)

        # 7. Ingestion timestamp for data lineage
        enriched["ingestion_timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None)

        logger.info(f"Data enrichment complete. Added 6 derived analytical features.")
        return enriched

    def validate_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Runs automated data quality gates.
        Returns a dictionary with quality checks and boolean pass/fail status.
        """
        results: Dict[str, Any] = {
            "passed": True,
            "checks": [],
            "warnings": []
        }

        # Check 1: Non-empty dataset
        if df.empty:
            results["passed"] = False
            results["checks"].append({"check": "Non-empty check", "status": "FAIL", "message": "DataFrame has 0 rows"})
            return results
        else:
            results["checks"].append({"check": "Non-empty check", "status": "PASS", "message": f"{len(df)} rows"})

        # Check 2: Primary Key Null Check (city, observation_time)
        pk_nulls = df["city"].isna().sum() + df["observation_time"].isna().sum()
        if pk_nulls > 0:
            results["passed"] = False
            results["checks"].append({"check": "Primary Key Null check", "status": "FAIL", "message": f"{pk_nulls} nulls in PK"})
        else:
            results["checks"].append({"check": "Primary Key Null check", "status": "PASS", "message": "0 nulls in PKs"})

        # Check 3: Temperature Range Check
        invalid_temps = ((df["temperature_c"] < -60) | (df["temperature_c"] > 60)).sum()
        if invalid_temps > 0:
            results["passed"] = False
            results["checks"].append({"check": "Temperature Bounds", "status": "FAIL", "message": f"{invalid_temps} invalid values"})
        else:
            results["checks"].append({"check": "Temperature Bounds", "status": "PASS", "message": "All temperatures valid"})

        # Check 4: Humidity Range Check
        invalid_hum = ((df["humidity_percent"] < 0) | (df["humidity_percent"] > 100)).sum()
        if invalid_hum > 0:
            results["passed"] = False
            results["checks"].append({"check": "Humidity Bounds", "status": "FAIL", "message": f"{invalid_hum} invalid values"})
        else:
            results["checks"].append({"check": "Humidity Bounds", "status": "PASS", "message": "All humidities valid"})

        return results
