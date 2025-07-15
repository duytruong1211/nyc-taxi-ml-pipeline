from pathlib import Path

# Hardcode your actual root path — make it work first
PROJECT_ROOT = Path("/Users/duytruong/Documents/Projects/nyc_taxi_pipeline")
DATA_DIR_NAME = "data"


def get_data_folder(layer: str, subfolder: str = "") -> Path:
    """
    Returns the full path to /<PROJECT_ROOT>/data/<layer>/<subfolder>
    """
    valid_layers = {"raw", "bronze", "silver", "gold"}
    if layer not in valid_layers:
        raise ValueError(f"Invalid layer: {layer}. Must be one of {valid_layers}")

    path = PROJECT_ROOT / DATA_DIR_NAME / layer
    if subfolder:
        path = path / subfolder

    path.mkdir(parents=True, exist_ok=True)
    return path


def get_raw_folder(subfolder: str = "") -> Path:
    return get_data_folder("raw", subfolder)

def get_bronze_folder(subfolder: str = "") -> Path:
    return get_data_folder("bronze", subfolder)

def get_silver_folder(subfolder: str = "") -> Path:
    return get_data_folder("silver", subfolder)

def get_gold_folder(subfolder: str = "") -> Path:
    return get_data_folder("gold", subfolder)
