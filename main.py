from paths import SILVER_NYC_CSV
import pandas as pd
from etl.gold import build_zone_rolling_features,build_main_trip_features
from pipeline.pipeline import run_weather_pipeline
import argparse
from pipeline.pipeline import run_nyc_ingestion, run_weather_pipeline
from pipeline.ml_pipeline import run_ml_pipeline

def main_bulk():
    year_month_list = [
        (year, month) for year in [2023, 2024] for month in range(1, 13)
    ]


    run_nyc_ingestion(
        year_month_list=year_month_list,
        output_path=SILVER_NYC_CSV,
        max_workers=8,
        n_rows=100_000,
        power=0.8,
        force=True
    )

    run_weather_pipeline(mode="bulk", force=True, cleanup=True)
    build_zone_rolling_features(end_date="2024-12-31")
    build_main_trip_features()

    for m in range(1, 13):
        run_ml_pipeline(
            test_month=pd.to_datetime(f"2024-{m:02d}-01"),
            log_to_mlflow_flag=True
        )


def main_incremental(year: int, month: int):
    run_nyc_ingestion(
        year_month_list=[(year, month)],
        output_path=SILVER_NYC_CSV,
        max_workers=1,
        n_rows=100_000,
        power=0.8,
        force=True
    )

    run_weather_pipeline(
        year=year,
        month=month,
        mode="monthly",
        force=True,
        cleanup=True
    )

    end_date = pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)

    build_zone_rolling_features(end_date=end_date.strftime("%Y-%m-%d"))
    build_main_trip_features()

    run_ml_pipeline(
        test_month=pd.to_datetime(f"{year}-{month:02d}-01"),
        log_to_mlflow_flag=True
    )



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, choices=["bulk", "incremental"])
    parser.add_argument("--year", type=int, help="Year for incremental mode")
    parser.add_argument("--month", type=int, help="Month for incremental mode (1–12)")
    args = parser.parse_args()

    if args.mode == "bulk":
        main_bulk()
    elif args.mode == "incremental":
        if args.year is None or args.month is None:
            raise ValueError("Must provide --year and --month for incremental mode.")
        main_incremental(year=args.year, month=args.month)

