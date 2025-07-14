from paths import SILVER_NYC_CSV, ZONE_FEATURES_PARQUET, DUCKDB_PATH,DIM_DATE_CSV, MAIN_FEATURES_PARQUET,SILVER_WEATHER_CSV
from utils.duck_db import DuckDBClient


zone_rolling_sql = """
    WITH snapshots AS (
        SELECT DISTINCT DATE_TRUNC('month', tpep_pickup_datetime) AS snapshot_month
        FROM silver_taxi
        WHERE tpep_pickup_datetime BETWEEN DATE '2023-01-01' AND DATE '{end_date}'
    ),
    aggregated_snapshots AS (
        SELECT
            s.snapshot_month,
            t.pu_zone,
            t.do_zone,

            -- 3-month
            COUNT(*) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '3 months') AS trip_count_3mo,
            AVG(t.fare_amount) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '3 months') AS avg_fare_3mo,
            AVG(t.trip_distance) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '3 months') AS avg_distance_3mo,
            AVG(t.trip_duration_seconds / 60.0) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '3 months') AS avg_duration_min_3mo,
            AVG(t.trip_distance / NULLIF(t.trip_duration_seconds, 0)) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '3 months') AS avg_speed_3mo,

            -- 6-month
            COUNT(*) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '6 months') AS trip_count_6mo,
            AVG(t.fare_amount) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '6 months') AS avg_fare_6mo,
            AVG(t.trip_distance) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '6 months') AS avg_distance_6mo,
            AVG(t.trip_duration_seconds / 60.0) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '6 months') AS avg_duration_min_6mo,
            AVG(t.trip_distance / NULLIF(t.trip_duration_seconds, 0)) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '6 months') AS avg_speed_6mo,

            -- 9-month
            COUNT(*) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '9 months') AS trip_count_9mo,
            AVG(t.fare_amount) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '9 months') AS avg_fare_9mo,
            AVG(t.trip_distance) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '9 months') AS avg_distance_9mo,
            AVG(t.trip_duration_seconds / 60.0) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '9 months') AS avg_duration_min_9mo,
            AVG(t.trip_distance / NULLIF(t.trip_duration_seconds, 0)) FILTER (WHERE t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '9 months') AS avg_speed_9mo,

            -- 12-month
            COUNT(*) AS trip_count_12mo,
            AVG(t.fare_amount) AS avg_fare_12mo,
            AVG(t.trip_distance) AS avg_distance_12mo,
            AVG(t.trip_duration_seconds / 60.0) AS avg_duration_min_12mo,
            AVG(t.trip_distance / NULLIF(t.trip_duration_seconds, 0)) AS avg_speed_12mo

        FROM snapshots s
        JOIN silver_taxi t
          ON t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL '12 months'
         AND t.tpep_pickup_datetime <  s.snapshot_month
         AND t.pulocationid IS NOT NULL AND t.dolocationid IS NOT NULL
        GROUP BY
            s.snapshot_month,
            t.pu_zone,
            t.do_zone
    )
    SELECT * FROM aggregated_snapshots
"""

main_trip_sql = """
WITH dates AS (
    SELECT
        d.date,
        d.is_holiday,
        d.is_near_holiday,
        SIN(2 * PI() * d.day_of_week / 7) AS sin_dow,
        COS(2 * PI() * d.day_of_week / 7) AS cos_dow,
        d.is_weekend,
        SIN(2 * PI() * d.month / 12) AS sin_month,
        COS(2 * PI() * d.month / 12) AS cos_month,
        CASE 
            WHEN d.is_month_start = 1 THEN 1
            WHEN d.is_month_end = 1 THEN 1
            WHEN DATE_PART('day', d.date) BETWEEN 13 AND 17 THEN 1
            ELSE 0
        END AS is_payroll_window,
        d.month,
        d.year
    FROM dim_date d
),

summary AS (
    SELECT
        st.vendorid AS vendor_id,
        CASE st.vendorid
            WHEN 1 THEN 'CMT'
            WHEN 2 THEN 'VTS'
            ELSE 'other'
        END AS vendor_name,

        st.tpep_pickup_datetime,
        DATE(st.tpep_pickup_datetime) AS pickup_date,
        HOUR(st.tpep_pickup_datetime) AS pickup_hour,
        CASE WHEN HOUR(st.tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END AS is_rush_hour_morning,
        CASE WHEN HOUR(st.tpep_pickup_datetime) BETWEEN 16 AND 19 THEN 1 ELSE 0 END AS is_rush_hour_evening,
        SIN(2 * PI() * HOUR(st.tpep_pickup_datetime) / 24) AS sin_hour,
        COS(2 * PI() * HOUR(st.tpep_pickup_datetime) / 24) AS cos_hour,

        st.tpep_dropoff_datetime,
        st.passenger_count,
        st.trip_distance,
        st.RatecodeID AS rate_code_id,
        CASE st.ratecodeid
            WHEN 1 THEN 'standard'
            WHEN 2 THEN 'jfk'
            WHEN 3 THEN 'newark'
            WHEN 4 THEN 'outer_counties'
            WHEN 5 THEN 'negotiated'
            WHEN 6 THEN 'group_ride'
            ELSE 'unknown'
        END AS rate_code_type,

        st.store_and_fwd_flag,
        CASE st.store_and_fwd_flag WHEN 'Y' THEN 1 ELSE 0 END AS is_store_and_fwd,
        CASE WHEN st.pu_borough = st.do_borough THEN 1 ELSE 0 END AS is_same_borough,
        CASE WHEN st.pu_zone = st.do_zone THEN 1 ELSE 0 END AS is_same_zone,
        CASE WHEN st.pu_zone IN ('JFK Airport', 'LaGuardia Airport') THEN 1 ELSE 0 END AS is_airport_pu_trip,
        CASE WHEN st.do_zone IN ('JFK Airport', 'LaGuardia Airport') THEN 1 ELSE 0 END AS is_airport_do_trip,

        st.payment_type,
        CASE st.payment_type
            WHEN 1 THEN 'credit_card'
            WHEN 2 THEN 'cash'
            WHEN 3 THEN 'no_charge'
            WHEN 4 THEN 'dispute'
            WHEN 5 THEN 'unknown'
            WHEN 6 THEN 'voided'
            ELSE 'other'
        END AS payment_type_str,

        st.trip_duration_seconds,
        st.trip_duration_seconds / 60.0 AS trip_duration_minutes,
        st.fare_amount,
        st.extra,
        st.mta_tax,
        st.tip_amount,
        st.tolls_amount,
        st.improvement_surcharge,
        st.total_amount,
        st.congestion_surcharge,
        st.airport_fee,
        st.tip_amount / NULLIF(st.fare_amount, 0) AS tip_pct,
        st.pu_zone,
        st.do_zone,
        st.pu_borough,
        st.do_borough

    FROM silver_taxi st
),

final_summary AS (
    SELECT
        s.*,
        d.*,
        w.weather_condition,
        w.is_bad_weather,
        w.temperature,
        w.precipitation,
        w.snow,
        w.windspeed,
        w.cloudcover,
        -- 3 month
        z.trip_count_3mo,
        z.avg_fare_3mo,
        z.avg_duration_min_3mo,
        z.avg_speed_3mo,
        z.avg_distance_3mo,
        -- 6 month
        z.trip_count_6mo,
        z.avg_fare_6mo,
        z.avg_duration_min_6mo,
        z.avg_speed_6mo,
        z.avg_distance_6mo,
        -- 9 month
        z.trip_count_9mo,
        z.avg_fare_9mo,
        z.avg_duration_min_9mo,
        z.avg_speed_9mo,
        z.avg_distance_9mo,
        -- 12 month
        z.trip_count_12mo,
        z.avg_fare_12mo,
        z.avg_duration_min_12mo,
        z.avg_speed_12mo,
        z.avg_distance_12mo
    FROM summary s
    LEFT JOIN dates d
        ON s.pickup_date = d.date 
    LEFT JOIN weather_features w
        ON w.weather_date = s.pickup_date
        AND w.weather_hour = s.pickup_hour
        AND TRIM(LOWER(w.borough)) = TRIM(LOWER(s.pu_borough))
    LEFT JOIN zone_avg z
        ON z.pu_zone = s.pu_zone
        AND z.do_zone = s.do_zone
        AND z.snapshot_month = DATE_TRUNC('month', s.pickup_date)
)

SELECT * FROM final_summary;
"""



def build_zone_rolling_features(
    silver_csv_path: str = SILVER_NYC_CSV,
    output_parquet_path: str = ZONE_FEATURES_PARQUET,
    db_path: str = DUCKDB_PATH,
    end_date: str = "2024-12-31",
    overwrite: bool = False,
):
    duck = DuckDBClient(db_path=db_path)
    duck.load_csv_as_table(silver_csv_path, "silver_taxi")

    sql = zone_rolling_sql.format(end_date=end_date)
    df = duck.run_query(sql)

    duck.save_to_parquet(
        df=df,
        path=output_parquet_path,
        dedup_cols=["snapshot_month", "pu_zone", "do_zone"] if not overwrite else None
    )

    duck.close()
    print(f"✅ Rolling zone features saved to: {output_parquet_path}")


def build_main_trip_features(
    silver_csv_path: str = SILVER_NYC_CSV,
    dim_date_path: str = DIM_DATE_CSV,
    weather_path: str = SILVER_WEATHER_CSV,
    zone_avg_path: str = ZONE_FEATURES_PARQUET,
    output_parquet_path: str = MAIN_FEATURES_PARQUET,
    db_path: str = DUCKDB_PATH,
    overwrite: bool = False,
):
    duck = DuckDBClient(db_path=db_path)

    # Register all source tables
    duck.load_csv_as_table(silver_csv_path, "silver_taxi")
    duck.load_csv_as_table(dim_date_path, "dim_date")
    duck.load_csv_as_table(weather_path, "weather_features")
    duck.load_parquet_as_table(zone_avg_path, "zone_avg")

    # Run query
    df = duck.run_query(main_trip_sql)

    # Save
    duck.save_to_parquet(
        df=df,
        path=output_parquet_path,
        dedup_cols=["tpep_pickup_datetime"] if not overwrite else None
    )

    duck.close()
    print(f"✅ Main trip features saved to: {output_parquet_path}")