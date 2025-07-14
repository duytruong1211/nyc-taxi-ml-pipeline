import os
import requests
import json
import time
from pathlib import Path
from paths import YELLOW_TAXI_DIR, WEATHER_DIR


for d in [YELLOW_TAXI_DIR, WEATHER_DIR]:
    d.mkdir(parents=True, exist_ok=True)

TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
def download_nyc_taxi_parquet(year: int, month: int) -> Path:
    """
    Downloads a NYC Yellow Taxi .parquet file for a given month/year.
    Returns the local file path.
    """
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    file_path = YELLOW_TAXI_DIR / filename

    if file_path.exists():
        print(f"✓ Already exists: {file_path.name}")
        return file_path

    url = TAXI_BASE_URL + filename
    print(f"→ Downloading: {filename}")
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✓ Saved: {file_path}")
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        if file_path.exists():
            file_path.unlink()  # remove corrupted file
        raise e

    return file_path


BOROUGH_COORDS = {
    "bronx": (40.8448, -73.8648),
    "brooklyn": (40.6782, -73.9442),
    "manhattan": (40.7831, -73.9712),
    "queens": (40.7282, -73.7949),
    "staten_island": (40.5795, -74.1502),
}

def download_weather_alltime():
    """
    Download full historical weather data (2023-01-01 to 2024-12-31)
    for each NYC borough in a single JSON file.
    """
    start_date = "2023-01-01"
    end_date = "2024-12-31"

    for borough, (lat, lon) in BOROUGH_COORDS.items():
        filename = f"{borough}.json"
        path = WEATHER_DIR / filename

        if path.exists():
            print(f"✓ Skipped: {filename}")
            continue

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join([
                "temperature_2m",
                "precipitation",
                "snowfall",
                "windspeed_10m",
                "cloudcover"
            ]),
            "timezone": "America/New_York"
        }

        print(f"⬇ Downloading: {filename}")
        r = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
        if r.status_code == 200:
            with open(path, "w") as f:
                json.dump(r.json(), f)
            time.sleep(0.5)
        else:
            print(f"✗ Error {r.status_code} on {filename}")


def download_weather_month(year: int, month: int):
    """
    Download hourly weather data for all NYC boroughs for a specific month.
    Saves each borough as a separate JSON file inside WEATHER_DIR.
    Skips download if file already exists.
    """
    start = f"{year}-{month:02d}-01"
    end = f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"

    for borough, (lat, lon) in BOROUGH_COORDS.items():
        fname = f"{borough}_{year}_{month:02d}.json"
        fpath = WEATHER_DIR / fname

        if fpath.exists():
            print(f"✓ Skipping existing: {fname}")
            continue

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "hourly": ",".join([
                "temperature_2m", "precipitation", "snowfall", "windspeed_10m", "cloudcover"
            ]),
            "timezone": "America/New_York"
        }

        res = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
        if res.status_code == 200:
            with open(fpath, "w") as f:
                json.dump(res.json(), f)
            print(f"⬇️  Downloaded: {fname}")
            time.sleep(0.5)
        else:
            print(f"✗ Error {res.status_code} on {fname}")