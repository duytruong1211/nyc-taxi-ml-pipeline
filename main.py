import argparse
import os
from pathlib import Path
import shutil
import pandas as pd

import os
os.environ["GIT_PYTHON_REFRESH"] = "quiet"


from paths import (
    SILVER_NYC_CSV,
    SILVER_WEATHER_CSV,
    ZONE_FEATURES_PARQUET,
    MAIN_FEATURES_PARQUET,
    EVAL_RESULTS_DIR,
    DUCKDB_PATH
)

from etl.gold import build_zone_rolling_features, build_main_trip_features
from pipeline.pipeline import run_taxi_pipeline, run_ingestion_loop, run_weather_pipeline
from pipeline.ml_pipeline import run_ml_pipeline



def main_bulk():
    
    print("Starting NYC taxi data ingestion pipeline...")
    year_month_list = [(y, m) for y in [2023, 2024] for m in range(1, 13)]
    run_ingestion_loop(year_month_list, end_date="2024-12-31")

    print("Starting weather data ingestion pipeline...")
    run_weather_pipeline(mode="bulk", force=True, cleanup=True)

    print("Building Zone Rolling Average table...")

    build_zone_rolling_features(end_date="2024-12-31")

    print("Building Main Feature table...")

    build_main_trip_features()

    print("Building ML Pipeline for production...")
    for m in range(1, 13):
        run_ml_pipeline(
            test_month=pd.to_datetime(f"2024-{m:02d}-01"),
            log_to_mlflow_flag=True,
            run_name_prefix="prod_xgb"
        )
    print("\n🧹 Cleaning up gold outputs...\n")
    cleanup_targets = [
        Path(ZONE_FEATURES_PARQUET),
        Path(MAIN_FEATURES_PARQUET)
    ]

    for target in cleanup_targets:
        try:
            target.unlink()
            print(f"🧹 Deleted file: {target}")
        except FileNotFoundError:
            print(f"✅ Already cleaned: {target}")
        except IsADirectoryError:
            print(f"⚠️ Skipped directory (not a file): {target}")


def main_test():

    print("Starting NYC taxi data test ingestion pipeline...")

    year_month_list = [(2024, m) for m in range(1, 5)]
    run_ingestion_loop(year_month_list, end_date="2024-04-30")

    print("Starting weather data test ingestion pipeline...")

    run_weather_pipeline(mode="bulk", force=True, cleanup=True)

    print("Building Zone Rolling Average table for test...")
    build_zone_rolling_features(end_date="2024-04-30")

    print("Building Main Feature table for test...")
    build_main_trip_features()

    print("Building ML Pipeline for test...")
    run_ml_pipeline(
        test_month=pd.to_datetime("2024-04-01"),
        log_to_mlflow_flag=True,
        run_name_prefix="test_xgb"
    )

    print("\n🧹 Cleaning up test outputs...\n")
    cleanup_targets = [
        Path(SILVER_NYC_CSV),
        Path(SILVER_WEATHER_CSV),
        Path(ZONE_FEATURES_PARQUET),
        Path(MAIN_FEATURES_PARQUET),
        Path(DUCKDB_PATH),
    ]

    for target in cleanup_targets:
        try:
            target.unlink()
            print(f"🧹 Deleted file: {target}")
        except FileNotFoundError:
            print(f"✅ Already cleaned: {target}")
        except IsADirectoryError:
            print(f"⚠️ Skipped directory (not a file): {target}")

    eval_dir = Path(EVAL_RESULTS_DIR)
    if eval_dir.exists() and eval_dir.is_dir():
        shutil.rmtree(eval_dir)
        print(f"🧹 Deleted directory: {eval_dir}")
    else:
        print(f"✅ Eval results already cleaned: {eval_dir}")


def main_incremental(year: int, month: int):

    print(f"Running incremental pipeline for {year}-{month:02d}...")
    run_taxi_pipeline(
        year=year,
        month=month,
        n_rows=100_000,
        power=0.8,
        existing_months=set()
    )

    print(f"Running weather pipeline for {year}-{month:02d}...")
    run_weather_pipeline(
        year=year,
        month=month,
        mode="monthly",
        force=True,
        cleanup=True
    )

    print("Building Zone Rolling Average table for incremental...")
    end_date = pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
    build_zone_rolling_features(end_date=end_date.strftime("%Y-%m-%d"))

    print("Building Main Feature table for incremental..."  ) 
    build_main_trip_features()

    print("Building ML Pipeline for incremental...")
    run_ml_pipeline(
        test_month=pd.to_datetime(f"{year}-{month:02d}-01"),
        log_to_mlflow_flag=True,
        run_name_prefix="prod_xgb"
    )
    print("\n🧹 Cleaning up gold outputs...\n")
    cleanup_targets = [
        Path(ZONE_FEATURES_PARQUET),
        Path(MAIN_FEATURES_PARQUET)
    ]

    for target in cleanup_targets:
        try:
            target.unlink()
            print(f"🧹 Deleted file: {target}")
        except FileNotFoundError:
            print(f"✅ Already cleaned: {target}")
        except IsADirectoryError:
            print(f"⚠️ Skipped directory (not a file): {target}")



if __name__ == "__main__":
    import sys

    parser = argparse.ArgumentParser(description="Run NYC Taxi pipeline.")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["bulk", "incremental", "test"],
        help="Pipeline mode to run"
    )
    parser.add_argument("--year", type=int, help="Year for incremental mode (required if mode=incremental)")
    parser.add_argument("--month", type=int, help="Month for incremental mode (required if mode=incremental)")

    try:
        args = parser.parse_args()
    except SystemExit as e:
        # Gracefully exit inside Docker with useful help text
        print("\n❌ Invalid arguments.")
        parser.print_help()
        sys.exit(e.code)

    if args.mode == "bulk":
        main_bulk()
    elif args.mode == "test":
        main_test()
    elif args.mode == "incremental":
        if args.year is None or args.month is None:
            print("\n❌ For incremental mode, --year and --month must both be provided.")
            parser.print_help()
            sys.exit(1)
        main_incremental(year=args.year, month=args.month)

