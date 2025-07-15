import pandas as pd
from pathlib import Path
from etl.silver import silver_filter,silver_filter_weather,stratified_zone_sample
from utils.other_utils import cleanup_raw_parquet, append_to_silver_csv,flatten_raw_weather,cleanup_weather_jsons
from etl.bronze import download_weather_alltime, download_weather_month,download_nyc_taxi_parquet
from paths import BRONZE_DIR, SILVER_NYC_CSV,SILVER_WEATHER_CSV,WEATHER_DIR
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from tqdm import tqdm
import os 


# ProcessPoolExecutor

def run_taxi_pipeline(
    year: int,
    month: int,
    n_rows: int = 100_000,
    power: float = 0.8,
    existing_months=None
) -> None:
    try:
        parquet_path = download_nyc_taxi_parquet(year, month)
    except Exception:
        print(f"❌ Skipping {year}-{month:02d} due to download failure")
        return

    print(f"📥 Loading: {parquet_path}")
    df_raw = pd.read_parquet(parquet_path)
    df_filtered = silver_filter(df_raw, snapshot_month=f"{year}-{month:02d}")
    df_sampled = stratified_zone_sample(df_filtered, n_rows=n_rows, power=power)
    append_to_silver_csv(df_sampled, output_path=SILVER_NYC_CSV, existing_months=existing_months)
    cleanup_raw_parquet(year, month)


def run_taxi_pipeline_wrapper(
    ym: tuple[int, int],
    n_rows: int,
    power: float,
    existing_months: set
) -> None:
    year, month = ym
    run_taxi_pipeline(year=year, month=month, n_rows=n_rows, power=power, existing_months=existing_months)

def is_docker():
    return Path("/.dockerenv").exists() or os.getenv("IS_DOCKER") == "1"

def wrapped(args):
    ym, n_rows, power, existing_months = args
    return run_taxi_pipeline_wrapper(
        ym=ym,
        n_rows=n_rows,
        power=power,
        existing_months=existing_months
    )

def run_nyc_ingestion(
    year_month_list: list[tuple[int, int]],
    output_path: Path = SILVER_NYC_CSV,
    max_workers: int = 8,
    n_rows: int = 100_000,
    power: float = 0.8,
    force: bool = False,
) -> None:
    """
    Ingest NYC taxi data for multiple (year, month) pairs.
    Uses dynamic executor: ThreadPool in Docker, ProcessPool locally.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Caching previously processed months
    if output_path.exists():
        existing_df = pd.read_csv(output_path, usecols=["snapshot_month"])
        existing_months = set(pd.to_datetime(existing_df["snapshot_month"]).dt.to_period("M"))
    else:
        existing_months = set()

    if not force:
        year_month_list = [
            (y, m) for (y, m) in year_month_list
            if pd.Period(f"{y}-{m:02d}") not in existing_months
        ]
        if not year_month_list:
            print("✅ All months already processed. Nothing to do.")
            return

    Executor = ThreadPoolExecutor if is_docker() else ProcessPoolExecutor

    args_list = [(ym, n_rows, power, existing_months) for ym in year_month_list]

    with Executor(max_workers=max_workers) as executor:
        list(tqdm(
            executor.map(wrapped, args_list),
            total=len(args_list),
            desc="Parallel ingestion"
        ))


def run_weather_pipeline(
    year: int = None,
    month: int = None,
    mode: str = "bulk",  # "bulk" or "monthly"
    force: bool = False,
    cleanup: bool = True
) -> None:
    """
    Full pipeline: download → flatten → transform → save/appends to silver_weather.csv.

    Args:
        year (int): Required if mode="monthly"
        month (int): Required if mode="monthly"
        mode (str): "bulk" or "monthly"
        force (bool): Overwrite silver CSV if bulk, or allow re-append for month
        cleanup (bool): Whether to delete raw JSONs after processing
    """
    assert mode in {"bulk", "monthly"}

    if mode == "bulk":
        download_weather_alltime()
    else:
        assert year and month, "year and month required for monthly mode"
        download_weather_month(year, month)

    df_raw = flatten_raw_weather(mode=mode)
    if df_raw.empty:
        print("⚠️ No data found after flattening.")
        return

    df_filtered = silver_filter_weather(df_raw)

    if mode == "bulk":
        if SILVER_WEATHER_CSV.exists() and not force:
            print(f"⚠️ silver_weather.csv exists. Use force=True to overwrite.")
            return
        SILVER_WEATHER_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_filtered.to_csv(SILVER_WEATHER_CSV, index=False)
        print(f"✅ Saved silver_weather.csv ({len(df_filtered):,} rows)")
    else:
        df_existing = pd.read_csv(SILVER_WEATHER_CSV) if SILVER_WEATHER_CSV.exists() else pd.DataFrame()
        df_combined = pd.concat([df_existing, df_filtered], ignore_index=True)
        df_combined.to_csv(SILVER_WEATHER_CSV, index=False)
        print(f"✅ Appended {len(df_filtered):,} rows to silver_weather.csv")

    if cleanup:
        cleanup_weather_jsons(WEATHER_DIR, mode=mode)

