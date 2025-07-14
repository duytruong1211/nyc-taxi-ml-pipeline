{{ config(materialized='table') }}

WITH snapshots AS (
    SELECT DISTINCT DATE_TRUNC('month', tpep_pickup_datetime) AS snapshot_month
    FROM {{ ref('silver_taxi') }}
    WHERE tpep_pickup_datetime BETWEEN '2023-01-01' AND '2024-12-31'
),

aggregated_snapshots AS (
    SELECT
        s.snapshot_month,
        t.PU_Zone,
        t.DO_Zone,

        -- 3-month
        COUNT(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 3 MONTH THEN 1 END) AS trip_count_3mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 3 MONTH THEN t.fare_amount END) AS avg_fare_3mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 3 MONTH THEN t.trip_distance END) AS avg_distance_3mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 3 MONTH THEN t.trip_duration_seconds / 60.0 END) AS avg_duration_min_3mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 3 MONTH THEN t.trip_distance / NULLIF(t.trip_duration_seconds, 0) END) AS avg_speed_3mo,

        -- 6-month
        COUNT(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 6 MONTH THEN 1 END) AS trip_count_6mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 6 MONTH THEN t.fare_amount END) AS avg_fare_6mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 6 MONTH THEN t.trip_distance END) AS avg_distance_6mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 6 MONTH THEN t.trip_duration_seconds / 60.0 END) AS avg_duration_min_6mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 6 MONTH THEN t.trip_distance / NULLIF(t.trip_duration_seconds, 0) END) AS avg_speed_6mo,

        -- 9-month
        COUNT(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 9 MONTH THEN 1 END) AS trip_count_9mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 9 MONTH THEN t.fare_amount END) AS avg_fare_9mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 9 MONTH THEN t.trip_distance END) AS avg_distance_9mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 9 MONTH THEN t.trip_duration_seconds / 60.0 END) AS avg_duration_min_9mo,
        AVG(CASE WHEN t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 9 MONTH THEN t.trip_distance / NULLIF(t.trip_duration_seconds, 0) END) AS avg_speed_9mo,

        -- 12-month
        COUNT(*) AS trip_count_12mo,
        AVG(t.fare_amount) AS avg_fare_12mo,
        AVG(t.trip_distance) AS avg_distance_12mo,
        AVG(t.trip_duration_seconds / 60.0) AS avg_duration_min_12mo,
        AVG(t.trip_distance / NULLIF(t.trip_duration_seconds, 0)) AS avg_speed_12mo

    FROM snapshots s
    INNER JOIN {{ ref('silver_taxi') }} t
        ON t.tpep_pickup_datetime >= s.snapshot_month - INTERVAL 12 MONTH
       AND t.tpep_pickup_datetime <  s.snapshot_month
       AND t.PU_Zone IS NOT NULL AND t.DO_Zone IS NOT NULL
    GROUP BY
        s.snapshot_month,
        t.PU_Zone,
        t.DO_Zone
)

SELECT * FROM aggregated_snapshots
