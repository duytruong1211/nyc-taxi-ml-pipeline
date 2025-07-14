from paths import YELLOW_TAXI_DIR, WEATHER_DIR, SILVER_DIR,SILVER_WEATHER_CSV
# from etl.silver import silver_filter_weather
import os
import pandas as pd
from pathlib import Path
import pandas as pd
import json
from tqdm import tqdm


def append_to_silver_csv(df: pd.DataFrame, output_path: str, existing_months=None) -> None:
    import os

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    file_exists = os.path.isfile(output_path)

    if df.empty:
        print("⚠️ Input DataFrame is empty — nothing to write.")
        return

    new_months = pd.to_datetime(df["snapshot_month"]).dt.to_period("M").unique()

    # If not passed in, read from disk
    if existing_months is None and file_exists:
        existing = pd.read_csv(output_path, usecols=["snapshot_month"])
        existing_months = pd.to_datetime(existing["snapshot_month"]).dt.to_period("M").unique()
    elif existing_months is None:
        existing_months = []

    # Check overlap
    overlap = set(new_months) & set(existing_months)
    if overlap:
        print(f"⚠️ Skipping months already in {output_path}: {sorted(overlap)}")
        return

    df.to_csv(output_path, mode='a', index=False, header=not file_exists)
    print(f"✅ Appended {len(df)} rows to {output_path}")


def cleanup_raw_parquet(year: int, month: int):
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    file_path = YELLOW_TAXI_DIR / filename

    if file_path.exists():
        file_path.unlink()
        print(f"🗑 Deleted: {file_path}")
    else:
        print(f"⚠️ File not found: {file_path}")

import re

def cleanup_weather_jsons(input_dir: Path, mode: str = "bulk") -> None:
    """
    Deletes weather JSON files from the input directory.

    Args:
        input_dir (Path): Directory containing JSON files.
        mode (str): Either "bulk" or "monthly".
            - "bulk": Deletes files like bronx.json
            - "monthly": Deletes files like bronx_2025_07.json
    """
    assert mode in {"bulk", "monthly"}, "mode must be 'bulk' or 'monthly'"
    deleted = 0

    monthly_pattern = re.compile(r"^[a-z_]+_\d{4}_\d{2}\.json$")

    for file in input_dir.glob("*.json"):
        fname = file.name

        is_monthly = bool(monthly_pattern.match(fname))

        if (mode == "bulk" and is_monthly) or (mode == "monthly" and not is_monthly):
            continue

        file.unlink()
        print(f"🗑 Deleted: {fname}")
        deleted += 1

    if deleted == 0:
        print(f"⚠️ No {mode} JSON files found to delete.")



BOROUGH_COORDS = {
    "bronx": (40.8448, -73.8648),
    "brooklyn": (40.6782, -73.9442),
    "manhattan": (40.7831, -73.9712),
    "queens": (40.7282, -73.7949),
    "staten_island": (40.5795, -74.1502),
}
import json
import pandas as pd

def flatten_raw_weather(mode: str = "bulk") -> pd.DataFrame:
    """
    Flattens JSON into raw weather DataFrame without saving.

    Args:
        mode (str): "bulk" or "monthly"
    Returns:
        pd.DataFrame
    """
    assert mode in {"bulk", "monthly"}
    rows = []

    paths = [
        p for p in WEATHER_DIR.glob("*.json")
        if (mode == "bulk" and "_" not in p.stem) or
           (mode == "monthly" and "_" in p.stem)
    ]

    for path in tqdm(paths, desc=f"📦 Flattening weather ({mode})"):
        borough = path.stem.split("_")[0]  # Safe for both formats

        with open(path, "r") as f:
            data = json.load(f)

        hourly = data.get("hourly", {})
        time_series = hourly.get("time", [])

        if not time_series:
            print(f"⚠️ Missing time series in {path.name}")
            continue

        for i, time in enumerate(time_series):
            rows.append({
                "borough": borough,
                "time": time,
                "temperature_2m": hourly["temperature_2m"][i],
                "precipitation": hourly["precipitation"][i],
                "snowfall": hourly["snowfall"][i],
                "windspeed_10m": hourly["windspeed_10m"][i],
                "cloudcover": hourly["cloudcover"][i],
            })

    df = pd.DataFrame(rows)
    print(f"✅ Flattened total: {len(df):,} rows from {len(paths)} files")
    return df






