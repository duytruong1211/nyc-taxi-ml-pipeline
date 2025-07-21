# paths.py

from pathlib import Path

# Root project directory (you can adjust if nested)
PROJECT_ROOT = Path(__file__).resolve().parent

# Bronze layer (raw ingested files)
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
YELLOW_TAXI_DIR = BRONZE_DIR / "nyc_taxi_yellow"
WEATHER_DIR = BRONZE_DIR / "weather"
DIM_DATE_CSV = BRONZE_DIR /  "dim_date.csv"

# Silver 
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_NYC_CSV = PROJECT_ROOT / "data" / "silver" / "silver_nyc.csv"
SILVER_WEATHER_CSV = PROJECT_ROOT / "data" / "silver" / "silver_weather.csv"
# Gold layer
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
ZONE_FEATURES_PARQUET = GOLD_DIR / "zone_avg_rolling_features.parquet"
MAIN_FEATURES_PARQUET = GOLD_DIR / "main_trip_features.parquet"  

# DuckDB path
DUCKDB_PATH = PROJECT_ROOT / "data" / "duck" / "nyc_taxi.duckdb"

# Evaluation Data
EVAL_RESULTS_DIR = "data/eval_results"


# Artifacts for models
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

# MLflow (optional, if logging locally)
MLRUNS_DIR = PROJECT_ROOT / "mlruns"
