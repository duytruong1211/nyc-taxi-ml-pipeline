# config.py
import os
from pathlib import Path

# Dynamically locate the project root (assuming this file lives in root or a subfolder)
PROJECT_ROOT = Path(__file__).resolve().parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_YELLOW_DIR = DATA_DIR / "raw" / "nyc_taxi_yellow"
BRONZE_OUTPUT_PATH = DATA_DIR / "nyc_taxi" / "bronze" / "yellow_tripdata"

# Warehouse for Spark (only used if not in-memory)
WAREHOUSE_DIR = PROJECT_ROOT / "spark-warehouse"

# Convert to str for Spark compatibility
RAW_YELLOW_DIR = str(RAW_YELLOW_DIR)
BRONZE_OUTPUT_PATH = str(BRONZE_OUTPUT_PATH)
WAREHOUSE_DIR = str(WAREHOUSE_DIR)
