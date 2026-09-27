"""
Configuration settings for AetherFlow ETL Pipeline.
Centralizes file paths, database connection strings, API endpoints, and pipeline defaults.
"""

from pathlib import Path
import os

PROJECT_NAME = "AetherFlow"
PROJECT_TAGLINE = "Production-Grade Weather & CSV Data ETL Pipeline"

# Base Directories
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
DB_DIR = DATA_DIR / "database"

# Ensure directories exist
for folder in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, DB_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# Database Configuration (SQLite by default, easily switchable to PostgreSQL)
SQLITE_DB_PATH = DB_DIR / "etl_pipeline.db"
DEFAULT_DB_URL = f"sqlite:///{SQLITE_DB_PATH}"

# Can be overridden by DATABASE_URL environment variable (e.g. for PostgreSQL: postgresql+psycopg://user:pass@localhost:5432/dbname)
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Public Weather API Settings (Open-Meteo: free, no API key required)
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Target cities for multi-city API extraction
TARGET_CITIES = [
    {"city": "New York", "country": "USA", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "London", "country": "UK", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "Tokyo", "country": "Japan", "latitude": 35.6762, "longitude": 139.6503},
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Sydney", "country": "Australia", "latitude": -33.8688, "longitude": 151.2093},
    {"city": "Mumbai", "country": "India", "latitude": 19.0760, "longitude": 72.8777},
    {"city": "São Paulo", "country": "Brazil", "latitude": -23.5505, "longitude": -46.6333},
]

# Weather code mapping (WMO Weather interpretation codes)
WMO_WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
}
