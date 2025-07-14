{{ config(materialized="table", schema="bronze_weather") }}

{{ load_weather_parquet("/Users/duytruong/Documents/Projects/nyc_taxi_pipeline/data/raw/weather") }}
