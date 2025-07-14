from dagster import Definitions
from dagster_ingestion.assets.nyc_yellow_taxi_assets import nyc_yellow_taxi_assets
from dagster_ingestion.assets.nyc_weather_assets import nyc_weather_monthly_bulk  # import the weather asset

# Combine all assets in one list
all_assets = [*nyc_yellow_taxi_assets, nyc_weather_monthly_bulk]

defs = Definitions(
    assets=all_assets,
)
