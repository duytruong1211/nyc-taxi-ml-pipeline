from dagster import asset, AssetExecutionContext, MetadataValue
from dagster_ingestion.utilities.download_utils import download_file_if_not_exists
from dagster_ingestion.utilities.file_paths import get_raw_folder


@asset(name="yellow_tripdata_bulk_download")
def yellow_tripdata_bulk_download(context: AssetExecutionContext) -> None:
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    subfolder = "nyc_taxi_yellow"
    raw_folder = get_raw_folder(subfolder)
    years = ["2023", "2024"]
    months = [f"{i:02d}" for i in range(1, 13)]
    count = 0

    for year in years:
        for month in months:
            filename = f"yellow_tripdata_{year}-{month}.parquet"
            url = f"{base_url}/{filename}"
            file_path = download_file_if_not_exists(context, url, subfolder, filename)
            if file_path:
                count += 1

    context.log.info(f"{count} yellow tripdata files downloaded or confirmed.")

# Required for Dagster registration
nyc_yellow_taxi_assets = [yellow_tripdata_bulk_download]
