
import os
from dagster_ingestion.utilities.download_utils import download_file_if_not_exists

def test_files_exist_in_yellow_taxi_folder():
    folder_path = os.path.join("raw", "nyc_taxi_yellow")
    
    assert os.path.exists(folder_path), f"Folder does not exist: {folder_path}"
    
    files = os.listdir(folder_path)
    assert len(files) > 0, "No files found in nyc_taxi_yellow folder"

    print(f"✅ {len(files)} files found:")
    for f in sorted(files):
        print(" -", f)
def test_sanity():
    assert True
