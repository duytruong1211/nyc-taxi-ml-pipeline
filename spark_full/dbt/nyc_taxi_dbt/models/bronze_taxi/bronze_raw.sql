{{ config(materialized='table') }}

{{ load_yellow_tripdata(2023, 2024) }}
