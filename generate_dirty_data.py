"""
Synthetic Dirty Data Generator.
Generates a realistic, imperfect CSV dataset containing real-world data issues:
- Missing values and custom null tokens ('-999', 'N/A', 'missing')
- Whitespace and casing inconsistencies in text columns
- Faulty sensor readings / extreme outliers (e.g. 999.0°C)
- Inconsistent and corrupted timestamp formats
- Duplicate records
- Corrupted numeric strings (e.g. '24.5 C', '45%')

Use this to test and demonstrate the 'Transform' stage of the ETL pipeline.
"""

from pathlib import Path
import random
import pandas as pd
from datetime import datetime, timedelta

from config import RAW_DATA_DIR, TARGET_CITIES


def generate_dirty_weather_csv(num_rows: int = 120, output_path: Path = None) -> Path:
    if output_path is None:
        output_path = RAW_DATA_DIR / "dirty_weather_dataset.csv"

    records = []
    base_time = datetime(2026, 3, 20, 0, 0, 0)

    for i in range(num_rows):
        city_meta = random.choice(TARGET_CITIES)
        time_offset = timedelta(hours=random.randint(0, 48), minutes=random.choice([0, 15, 30, 45]))
        row_time = base_time + time_offset

        # 1. Inconsistent city naming & casing
        city_raw = city_meta["city"]
        casing_choice = random.random()
        if casing_choice < 0.2:
            city_str = f"  {city_raw.lower()} "
        elif casing_choice < 0.35:
            city_str = city_raw.upper()
        elif casing_choice < 0.45:
            city_str = f"{city_raw}   "
        else:
            city_str = city_raw

        # 2. Timestamp formatting variation & occasional corrupted date
        ts_choice = random.random()
        if ts_choice < 0.05:
            timestamp_str = "INVALID_TIMESTAMP"
        elif ts_choice < 0.3:
            timestamp_str = row_time.strftime("%Y/%m/%d %H:%M:%S")
        elif ts_choice < 0.5:
            timestamp_str = row_time.strftime("%d-%m-%Y %H:%M")
        else:
            timestamp_str = row_time.isoformat()

        # 3. Temperature with occasional outliers and nulls
        temp_choice = random.random()
        if temp_choice < 0.08:
            temp_val = "-999"  # Sentinal null
        elif temp_choice < 0.12:
            temp_val = None    # True null
        elif temp_choice < 0.15:
            temp_val = 999.0   # Extreme sensor malfunction outlier
        elif temp_choice < 0.22:
            temp_val = f"{round(random.uniform(5.0, 35.0), 1)} C"  # String with unit
        else:
            temp_val = round(random.uniform(8.0, 32.0), 2)

        # 4. Humidity (should be 0 - 100%)
        hum_choice = random.random()
        if hum_choice < 0.06:
            humidity_val = "missing"
        elif hum_choice < 0.10:
            humidity_val = -15.0  # Invalid negative
        elif hum_choice < 0.15:
            humidity_val = 185.0  # Invalid > 100%
        elif hum_choice < 0.22:
            humidity_val = f"{random.randint(40, 90)}%"  # Percentage string
        else:
            humidity_val = round(random.uniform(30.0, 95.0), 1)

        # 5. Precipitation
        precip_choice = random.random()
        if precip_choice < 0.05:
            precipitation_val = "N/A"
        elif precip_choice < 0.08:
            precipitation_val = -5.0  # Invalid negative rain
        else:
            precipitation_val = round(max(0.0, random.uniform(-1.0, 15.0)), 2)

        # 6. Wind speed
        wind_speed_val = round(random.uniform(0.0, 45.0), 1) if random.random() > 0.05 else None

        # 7. Weather code
        weather_code_val = random.choice([0, 1, 2, 3, 51, 61, 80, 95, 999])

        records.append({
            "City": city_str,
            "Country": city_meta["country"],
            "Latitude": city_meta["latitude"],
            "Longitude": city_meta["longitude"],
            "Observed_At": timestamp_str,
            "Temp_Celsius": temp_val,
            "Humidity": humidity_val,
            "Precipitation_mm": precipitation_val,
            "Wind_Speed_kmh": wind_speed_val,
            "Weather_Code": weather_code_val
        })

    # Add deliberate duplicate rows for deduplication teaching
    if len(records) > 5:
        duplicates = records[:4]
        records.extend(duplicates)

    df = pd.DataFrame(records)
    # Shuffle slightly
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"Generated dirty CSV dataset with {len(df)} rows at: {output_path}")
    return output_path


if __name__ == "__main__":
    generate_dirty_weather_csv()
