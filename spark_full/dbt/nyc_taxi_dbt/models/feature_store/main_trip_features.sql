{{ config(materialized='table') }}

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
    FROM {{ ref('dim_date') }} d
),

summary AS (
    SELECT
        VendorID AS vendor_id,
        CASE VendorID
            WHEN 1 THEN 'CMT'
            WHEN 2 THEN 'VTS'
            ELSE 'other'
        END AS vendor_name,

        tpep_pickup_datetime,
        DATE(tpep_pickup_datetime) AS pickup_date,
        HOUR(tpep_pickup_datetime) AS pickup_hour,
        CASE WHEN HOUR(tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END AS is_rush_hour_morning,
        CASE WHEN HOUR(tpep_pickup_datetime) BETWEEN 16 AND 19 THEN 1 ELSE 0 END AS is_rush_hour_evening,
        SIN(2 * PI() * HOUR(tpep_pickup_datetime) / 24) AS sin_hour,
        COS(2 * PI() * HOUR(tpep_pickup_datetime) / 24) AS cos_hour,

        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID AS rate_code_id,
        CASE RatecodeID
            WHEN 1 THEN 'standard'
            WHEN 2 THEN 'jfk'
            WHEN 3 THEN 'newark'
            WHEN 4 THEN 'outer_counties'
            WHEN 5 THEN 'negotiated'
            WHEN 6 THEN 'group_ride'
            ELSE 'unknown'
        END AS rate_code_type,

        store_and_fwd_flag,
        CASE store_and_fwd_flag
            WHEN 'Y' THEN 1 ELSE 0
        END AS is_store_and_fwd,

        PU_Zone AS pu_zone,
        PU_Borough AS pu_borough,
        DO_Zone AS do_zone,
        DO_Borough AS do_borough,
        CASE WHEN PU_Borough = DO_Borough THEN 1 ELSE 0 END AS is_same_borough,
        CASE WHEN PU_Zone = DO_Zone THEN 1 ELSE 0 END AS is_same_zone,
        CASE WHEN PU_Zone IN ('JFK Airport', 'LaGuardia Airport') THEN 1 ELSE 0 END AS is_airport_pu_trip,
        CASE WHEN DO_Zone IN ('JFK Airport', 'LaGuardia Airport') THEN 1 ELSE 0 END AS is_airport_do_trip,

        payment_type,
        CASE payment_type
            WHEN 1 THEN 'credit_card'
            WHEN 2 THEN 'cash'
            WHEN 3 THEN 'no_charge'
            WHEN 4 THEN 'dispute'
            WHEN 5 THEN 'unknown'
            WHEN 6 THEN 'voided'
            ELSE 'other'
        END AS payment_type_str,

        trip_duration_seconds,
        trip_duration_seconds / 60.0 AS trip_duration_minutes,
        is_speed_suspicious,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        tip_amount / NULLIF(fare_amount, 0) AS tip_pct

    FROM {{ ref('silver_taxi') }}
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
    LEFT JOIN {{ ref('weather_features_per_hour') }} w
        ON w.weather_date = s.pickup_date
        AND w.weather_hour = s.pickup_hour
        AND w.borough = s.pu_borough
    LEFT JOIN {{ ref('zone_avg_12mo') }} z
        ON z.PU_Zone = s.pu_zone
        AND z.DO_Zone = s.do_zone
        AND z.snapshot_month = DATE_TRUNC('month', s.pickup_date)
)

SELECT * FROM final_summary
