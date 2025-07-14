from dagster import asset, AssetExecutionContext
import os
import pandas as pd
from dagster_ingestion.utilities.download_utils import download_file_if_not_exists

borough_coords = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502),
}

weather_vars = [
    "temperature_2m",
    "precipitation",
    "windspeed_10m",
    "cloudcover",
    "rain",
    "snowfall",
]

timezone = "America/New_York"
base_url = "https://archive-api.open-meteo.com/v1/archive"

@asset(name="nyc_weather_monthly_bulk")
def nyc_weather_monthly_bulk(context: AssetExecutionContext) -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, "..", "..", ".."))
    raw_folder = os.path.join(project_root, "raw", "weather")
    os.makedirs(raw_folder, exist_ok=True)

    years = [2023, 2024]
    months = range(1, 13)
    total_files = 0

    for year in years:
        for month in months:
            for borough, (lat, lon) in borough_coords.items():
                json_filename = f"{borough}_{year}_{month:02d}.json"
                json_path = os.path.join(raw_folder, json_filename)

                if os.path.exists(json_path):
                    context.log.info(f"Skipping existing raw JSON: {json_filename}")
                    continue

                start_date = f"{year}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year+1}-01-01"
                else:
                    end_date = f"{year}-{month+1:02d}-01"
                # End date is day before next month
                end_date = (pd.to_datetime(end_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

                params = {
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": start_date,
                    "end_date": end_date,
                    "hourly": ",".join(weather_vars),
                    "timezone": timezone,
                }
                url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

                result_path = download_file_if_not_exists(context, url, "weather", json_filename)
                if result_path:
                    total_files += 1

                    try:
                        # Load and flatten JSON
                        with open(result_path, "r") as f:
                            json_data = pd.read_json(f)

                        if "hourly" not in json_data:
                            context.log.warning(f"Missing 'hourly' section in {json_filename}, skipping.")
                            continue

                        hourly_df = pd.DataFrame(json_data["hourly"])
                        hourly_df["borough"] = borough

                        csv_filename = json_filename.replace(".json", ".csv")
                        csv_path = os.path.join(raw_folder, csv_filename)
                        hourly_df.to_csv(csv_path, index=False)
                        context.log.info(f"Saved CSV: {csv_filename}")

                    except Exception as e:
                        context.log.warning(f"Error processing {json_filename}: {str(e)}")
                else:
                    context.log.warning(f"Failed to download {json_filename}")


