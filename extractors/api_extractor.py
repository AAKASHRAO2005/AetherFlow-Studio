"""
API Extractor Module.
Fetches meteorological weather data from the public Open-Meteo REST API.
Demonstrates HTTP request handling, response parsing, error handling with timeouts,
and saving raw landing-zone payloads for data lineage.
"""

from datetime import datetime, timezone
import json
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
import requests

from config import API_BASE_URL, TARGET_CITIES, RAW_DATA_DIR

logger = logging.getLogger(__name__)


class ApiExtractor:
    """
    Extracts weather data from Open-Meteo public REST API.
    """

    def __init__(self, base_url: str = API_BASE_URL, timeout: int = 15):
        self.base_url = base_url
        self.timeout = timeout

    def fetch_city_weather(self, city_meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch current and hourly weather for a single city.
        """
        params = {
            "latitude": city_meta["latitude"],
            "longitude": city_meta["longitude"],
            "current": [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "weather_code",
                "wind_speed_10m"
            ],
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "weather_code",
                "wind_speed_10m"
            ],
            "timezone": "auto",
            "forecast_days": 2,
            "past_days": 1
        }

        try:
            logger.info(f"Extracting weather from API for {city_meta['city']} ({city_meta['country']})...")
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            # Enrich raw response with city/country metadata
            data["city"] = city_meta["city"]
            data["country"] = city_meta["country"]
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for {city_meta['city']}: {str(e)}")
            return None

    def extract(self, cities: Optional[List[Dict[str, Any]]] = None, save_raw: bool = True) -> pd.DataFrame:
        """
        Extract weather records across multiple locations.
        Saves raw landing zone JSON snapshots and returns an uncleaned raw DataFrame.
        """
        cities_to_fetch = cities or TARGET_CITIES
        all_records = []
        raw_dump = []

        for city_meta in cities_to_fetch:
            payload = self.fetch_city_weather(city_meta)
            if not payload:
                continue

            raw_dump.append(payload)

            # 1. Parse current reading
            current = payload.get("current", {})
            if current:
                all_records.append({
                    "city": payload["city"],
                    "country": payload["country"],
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "timestamp": current.get("time"),
                    "temperature": current.get("temperature_2m"),
                    "apparent_temperature": current.get("apparent_temperature"),
                    "humidity": current.get("relative_humidity_2m"),
                    "precipitation": current.get("precipitation"),
                    "weather_code": current.get("weather_code"),
                    "wind_speed": current.get("wind_speed_10m"),
                    "is_current": True
                })

            # 2. Parse hourly readings (giving us rich historical & forecast data points)
            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            feels = hourly.get("apparent_temperature", [])
            humids = hourly.get("relative_humidity_2m", [])
            precips = hourly.get("precipitation", [])
            codes = hourly.get("weather_code", [])
            winds = hourly.get("wind_speed_10m", [])

            for i in range(len(times)):
                all_records.append({
                    "city": payload["city"],
                    "country": payload["country"],
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "timestamp": times[i],
                    "temperature": temps[i] if i < len(temps) else None,
                    "apparent_temperature": feels[i] if i < len(feels) else None,
                    "humidity": humids[i] if i < len(humids) else None,
                    "precipitation": precips[i] if i < len(precips) else None,
                    "weather_code": codes[i] if i < len(codes) else None,
                    "wind_speed": winds[i] if i < len(winds) else None,
                    "is_current": False
                })

        # Save landing zone raw data
        if save_raw and raw_dump:
            timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            raw_filepath = RAW_DATA_DIR / f"raw_api_weather_{timestamp_str}.json"
            with open(raw_filepath, "w", encoding="utf-8") as f:
                json.dump(raw_dump, f, indent=2)
            logger.info(f"Raw API payload saved to {raw_filepath}")

        df = pd.DataFrame(all_records)
        logger.info(f"API Extraction complete: extracted {len(df)} total raw rows.")
        return df
