import requests
from pathlib import Path
from dagster import MetadataValue

from dagster_ingestion.utilities.file_paths import get_raw_folder


def download_file_if_not_exists(context, url: str, subfolder: str, filename: str):
    """
    Downloads a file to raw/subfolder/filename if it doesn't already exist.
    Logs download status and attaches Dagster metadata.
    """
    raw_folder = get_raw_folder(subfolder)  # returns a Path object
    file_path = raw_folder / filename       # also a Path object

    if file_path.exists():
        context.log.info(f"File already exists: {file_path}")
    else:
        context.log.info(f"Downloading file from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            file_path.write_bytes(response.content)
            context.log.info(f"Download complete: {file_path}")
        except Exception as e:
            context.log.warning(f"Download failed: {e}")
            return None

    try:
        context.add_output_metadata({
            "Download Path": MetadataValue.path(str(file_path)),
            "File Size (MB)": round(file_path.stat().st_size / (1024 ** 2), 2)
        })
    except Exception as e:
        context.log.warning(f"Failed to attach metadata: {e}")

    return str(file_path)


