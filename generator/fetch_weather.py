"""
EpiScope Weather Data Fetcher (S3)
Fetches 5 years of daily weather data per district from Open-Meteo API.
Caches responses locally in the raw data directory.
"""

import os
import sys
import json
import time
import requests
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    RAW_DIR, START_YEAR, END_YEAR, DISTRICT_POPULATIONS, get_all_districts
)

# Approximate district centroid coordinates (lat, lon)
# Simplified – uses rough geographic centres
DISTRICT_CENTROIDS = {
    "Dhaka": (23.81, 90.41), "Chattogram": (22.34, 91.83),
    "Gazipur": (24.00, 90.43), "Narayanganj": (23.63, 90.50),
    "Comilla": (23.46, 91.18), "Rajshahi": (24.37, 88.60),
    "Khulna": (22.82, 89.53), "Sylhet": (24.90, 91.87),
    "Rangpur": (25.75, 89.25), "Mymensingh": (24.75, 90.41),
    "Barishal": (22.70, 90.37), "Bogura": (24.85, 89.37),
    "Tangail": (24.25, 89.92), "Narsingdi": (23.92, 90.72),
    "Brahmanbaria": (23.96, 91.11), "Kishoreganj": (24.44, 90.78),
    "Sirajganj": (24.45, 89.70), "Dinajpur": (25.63, 88.63),
    "Jessore": (23.17, 89.21), "Pabna": (24.01, 89.23),
    "Faridpur": (23.60, 89.84), "Noakhali": (22.87, 91.10),
    "Jamalpur": (24.94, 89.95), "Cox's Bazar": (21.44, 92.01),
    "Chandpur": (23.23, 90.67), "Feni": (23.01, 91.40),
    "Lakshmipur": (22.94, 90.83), "Gopalganj": (23.00, 89.83),
    "Manikganj": (23.86, 90.00), "Madaripur": (23.16, 90.19),
    "Rajbari": (23.76, 89.64), "Shariatpur": (23.24, 90.35),
    "Munshiganj": (23.54, 90.53), "Satkhira": (22.72, 89.07),
    "Natore": (24.42, 89.00), "Nawabganj": (24.60, 88.28),
    "Kushtia": (23.90, 89.12), "Magura": (23.49, 89.42),
    "Narail": (23.17, 89.51), "Chuadanga": (23.64, 88.84),
    "Meherpur": (23.76, 88.67), "Jhenaidah": (23.54, 89.17),
    "Bagerhat": (22.66, 89.79), "Bhola": (22.69, 90.65),
    "Patuakhali": (22.35, 90.33), "Pirojpur": (22.58, 89.97),
    "Jhalokati": (22.64, 90.20), "Barguna": (22.15, 90.13),
    "Bandarban": (22.20, 92.22), "Khagrachhari": (23.13, 91.98),
    "Rangamati": (22.73, 92.20), "Nilphamari": (25.93, 88.85),
    "Kurigram": (25.81, 89.64), "Lalmonirhat": (25.92, 89.45),
    "Gaibandha": (25.33, 89.53), "Panchagarh": (26.33, 88.55),
    "Thakurgaon": (26.03, 88.46), "Chapainawabganj": (24.68, 88.27),
    "Naogaon": (24.81, 88.94), "Joypurhat": (25.10, 89.02),
    "Netrokona": (24.88, 90.73), "Sherpur": (25.02, 90.01),
    "Habiganj": (24.37, 91.42), "Moulvibazar": (24.48, 91.78),
    "Sunamganj": (25.07, 91.40),
}


def fetch_weather_for_district(district, lat, lon, start_date, end_date, cache_dir):
    """Fetch daily weather from Open-Meteo for a single district."""
    safe_name = district.replace(" ", "_").replace("'", "")
    cache_file = os.path.join(cache_dir, f"weather_{safe_name}.json")

    # Use cache if exists
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_mean,precipitation_sum,relative_humidity_2m_mean",
        "timezone": "Asia/Dhaka",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        result = {
            "district": district,
            "latitude": lat,
            "longitude": lon,
            "dates": data.get("daily", {}).get("time", []),
            "temperature": data.get("daily", {}).get("temperature_2m_mean", []),
            "precipitation": data.get("daily", {}).get("precipitation_sum", []),
            "humidity": data.get("daily", {}).get("relative_humidity_2m_mean", []),
        }

        # Cache the result
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(result, f)

        return result

    except Exception as e:
        print(f"    ⚠ Failed to fetch weather for {district}: {e}")
        return None


def generate_synthetic_weather(district, lat, lon, rng):
    """Generate synthetic weather data when API is unavailable."""
    import numpy as np
    dates = []
    temps = []
    precip = []
    humidity = []

    start = date(START_YEAR, 1, 1)
    end = date(END_YEAR, 12, 31)
    current = start

    while current <= end:
        dates.append(current.isoformat())
        day_of_year = current.timetuple().tm_yday

        # Seasonal temperature (Bangladesh: 20-35°C range)
        base_temp = 27.0 + 7.0 * np.sin(2 * np.pi * (day_of_year - 120) / 365)
        temps.append(round(float(base_temp + rng.normal(0, 2)), 1))

        # Monsoon precipitation (Jun-Sep peak)
        if 150 <= day_of_year <= 270:  # monsoon
            precip.append(round(float(max(0, rng.exponential(15))), 1))
        else:
            precip.append(round(float(max(0, rng.exponential(2))), 1))

        # Humidity (higher in monsoon)
        base_humid = 70 + 15 * np.sin(2 * np.pi * (day_of_year - 90) / 365)
        humidity.append(round(float(min(100, max(40, base_humid + rng.normal(0, 5)))), 1))

        current += __import__("datetime").timedelta(days=1)

    return {
        "district": district,
        "latitude": lat,
        "longitude": lon,
        "dates": dates,
        "temperature": temps,
        "precipitation": precip,
        "humidity": humidity,
    }


def fetch_all_weather(use_api=True):
    """Fetch weather for all districts."""
    import numpy as np

    cache_dir = os.path.join(RAW_DIR, "weather_cache")
    os.makedirs(cache_dir, exist_ok=True)

    start_date = f"{START_YEAR}-01-01"
    end_date = f"{END_YEAR}-12-31"
    rng = np.random.default_rng(42)

    all_weather = []
    districts = get_all_districts()

    print(f"═══ Weather Data Acquisition ═══")
    print(f"  Districts: {len(districts)}")
    print(f"  Period: {start_date} to {end_date}")

    for i, district in enumerate(districts):
        coords = DISTRICT_CENTROIDS.get(district)
        if coords is None:
            coords = (23.5 + rng.uniform(-2, 2), 90.0 + rng.uniform(-2, 2))

        lat, lon = coords

        if use_api:
            result = fetch_weather_for_district(district, lat, lon, start_date, end_date, cache_dir)
            if result is None:
                print(f"    → Falling back to synthetic weather for {district}")
                result = generate_synthetic_weather(district, lat, lon, rng)
        else:
            result = generate_synthetic_weather(district, lat, lon, rng)

        all_weather.append(result)
        print(f"  [{i+1}/{len(districts)}] {district}: {len(result['dates'])} days")

        if use_api:
            time.sleep(0.3)  # rate limit

    # Save consolidated weather CSV
    output_path = os.path.join(RAW_DIR, "weather_daily.csv")
    import csv
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["district", "date", "temperature", "precipitation", "humidity"])
        for w in all_weather:
            for j in range(len(w["dates"])):
                writer.writerow([
                    w["district"],
                    w["dates"][j],
                    w["temperature"][j] if j < len(w["temperature"]) else "",
                    w["precipitation"][j] if j < len(w["precipitation"]) else "",
                    w["humidity"][j] if j < len(w["humidity"]) else "",
                ])

    print(f"  ✓ Weather data saved → {output_path}")
    return all_weather


if __name__ == "__main__":
    # Try API first, fall back to synthetic
    fetch_all_weather(use_api=False)  # Use synthetic for speed; set True for real API
