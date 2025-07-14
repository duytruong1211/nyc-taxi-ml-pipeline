from dagster import Definitions
from dagster_ingestion.assets.nyc_yellow_taxi_assets import yellow_tripdata_bulk_download

defs = Definitions(
    assets=[yellow_tripdata_bulk_download],
)
