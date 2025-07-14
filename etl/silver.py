import pandas as pd
from paths import BRONZE_DIR
import warnings




def silver_filter(df: pd.DataFrame, snapshot_month: str) -> pd.DataFrame:
    # Time boundaries
    month_start = pd.to_datetime(f"{snapshot_month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)

    df = df.copy()
    # Drop unexpected columns to enforce stable schema
    df = df.drop(columns=[col for col in df.columns if col == "cbd_congestion_fee"], errors="ignore")

    df.columns = df.columns.str.lower()

    # Add default columns if missing
    for col in ["airport_fee", "congestion_surcharge", "improvement_surcharge"]:
        if col not in df.columns:
            df[col] = 0

    # Add trip duration + snapshot month
    df["trip_duration_seconds"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    df["snapshot_month"] = month_start

    # Load zone lookup from centralized path
    zone_lookup_path = BRONZE_DIR / "taxi_zone_lookup.csv"
    zone_lookup = pd.read_csv(zone_lookup_path)

    # Apply filters
    df = df[
        (df["passenger_count"] < 7) &
        (df["trip_distance"].between(0.01, 100)) &
        (df["fare_amount"].between(0.01, 200)) &
        (df["payment_type"].between(1, 4)) &
        (df["tip_amount"].between(0, 500)) &
        (df["extra"] >= 0) &
        (df["mta_tax"].isin([0, 0.5])) &
        (df["tolls_amount"].between(0, 50)) &
        (df["improvement_surcharge"].isin([0, 1])) &
        (df["total_amount"].between(0.01, 200)) &
        (df["congestion_surcharge"] >= 0) &
        (df["airport_fee"] >= 0) &
        (df["tpep_pickup_datetime"] >= month_start) &
        (df["tpep_pickup_datetime"] < month_end) &
        (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]) &
        (df["trip_duration_seconds"].between(60, 3600))
    ]

    # Add zone + borough info
    pu_lookup = zone_lookup.rename(columns={
        "LocationID": "pulocationid",
        "Zone": "pu_zone",
        "Borough": "pu_borough"
    })
    do_lookup = zone_lookup.rename(columns={
        "LocationID": "dolocationid",
        "Zone": "do_zone",
        "Borough": "do_borough"
    })

    df = df.merge(pu_lookup, how="left", on="pulocationid")
    df = df.merge(do_lookup, how="left", on="dolocationid")
    return df


def stratified_zone_sample(df: pd.DataFrame, n_rows: int = 100_000, power: float = 0.8) -> pd.DataFrame:
    """
    Stratified downsampling by (PU, DO) zone pair, with controllable weighting.
    
    Args:
        df (pd.DataFrame): Input DataFrame with pulocationid and dolocationid.
        n_rows (int): Target number of rows in the final sample.
        power (float): Controls sampling sharpness; 1.0 = proportional, <1.0 = flatten.

    Returns:
        pd.DataFrame: Sampled dataframe with max n_rows rows.
    """
    df = df.copy()
    df["zone_pair"] = list(zip(df["pulocationid"], df["dolocationid"]))

    pair_counts = df["zone_pair"].value_counts()
    pair_weights = (pair_counts ** power) / (pair_counts ** power).sum()
    sample_sizes = (pair_weights * n_rows).round().astype(int)

    df["target_sample_size"] = df["zone_pair"].map(sample_sizes)

    def stratified_sample(group):
        n = int(group["target_sample_size"].iloc[0])
        return group if len(group) <= n else group.sample(n=n, random_state=42)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        df_sampled = df.groupby("zone_pair", group_keys=False).apply(stratified_sample)

    if len(df_sampled) > n_rows:
        df_sampled = df_sampled.sample(n=n_rows, random_state=42)

    return df_sampled.reset_index(drop=True)


def silver_filter_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add weather features and flags using pandas instead of SQL.

    Args:
        df (pd.DataFrame): Flattened weather data with columns:
            - 'time' (str or datetime)
            - 'borough'
            - 'temperature_2m', 'precipitation', 'snowfall', 'windspeed_10m', 'cloudcover'

    Returns:
        pd.DataFrame: Enhanced weather feature set.
    """
    df = df.copy()

    # Ensure datetime parsing
    df["time"] = pd.to_datetime(df["time"])
    df["weather_hour"] = df["time"].dt.hour
    df["weather_date"] = df["time"].dt.date

    # Rename columns
    df = df.rename(columns={
        "temperature_2m": "temperature",
        "snowfall": "snow",
        "windspeed_10m": "windspeed"
    })

    # Binary flags
    df["is_heavyrain"] = (df["precipitation"] >= 5).astype(int)
    df["is_snowfall"] = (df["snow"] >= 5).astype(int)
    df["is_highwind"] = (df["windspeed"] >= 30).astype(int)
    df["is_cold"] = (df["temperature"] < -5).astype(int)
    df["is_hot"] = (df["temperature"] >= 35).astype(int)

    # Weather condition (categorical)
    def classify_condition(row):
        if row["is_heavyrain"]:
            return "heavy_rain"
        if row["is_snowfall"]:
            return "snowfall"
        if row["is_highwind"]:
            return "high_wind"
        if row["is_cold"]:
            return "cold"
        if row["is_hot"]:
            return "hot"
        return "clear"

    df["weather_condition"] = df.apply(classify_condition, axis=1)

    # Composite flag
    df["is_bad_weather"] = (
        df[["is_heavyrain", "is_snowfall", "is_highwind", "is_cold", "is_hot"]].sum(axis=1) > 0
    ).astype(int)

    return df[[
        "weather_date", "weather_hour", "borough",
        "temperature", "precipitation", "snow", "windspeed", "cloudcover",
        "is_heavyrain", "is_snowfall", "is_highwind", "is_cold", "is_hot",
        "weather_condition", "is_bad_weather"
    ]]

