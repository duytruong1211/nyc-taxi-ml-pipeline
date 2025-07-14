{% macro load_yellow_tripdata(start_year=2023, end_year=2024) %}
{% set columns = [
    ("VendorID", "BIGINT"),
    ("tpep_pickup_datetime", "TIMESTAMP"),
    ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "DOUBLE"),
    ("trip_distance", "DOUBLE"),
    ("RatecodeID", "DOUBLE"),
    ("store_and_fwd_flag", "STRING"),
    ("PULocationID", "BIGINT"),
    ("DOLocationID", "BIGINT"),
    ("payment_type", "BIGINT"),
    ("fare_amount", "DOUBLE"),
    ("extra", "DOUBLE"),
    ("mta_tax", "DOUBLE"),
    ("tip_amount", "DOUBLE"),
    ("tolls_amount", "DOUBLE"),
    ("improvement_surcharge", "DOUBLE"),
    ("total_amount", "DOUBLE"),
    ("congestion_surcharge", "DOUBLE"),
    ("airport_fee", "DOUBLE")
] %}

{% set queries = [] %}
{% for year in range(start_year, end_year + 1) %}
  {% for month in range(1, 13) %}
    {% set path = "/Users/duytruong/Documents/Projects/nyc_taxi_pipeline/data/raw/nyc_taxi_yellow/yellow_tripdata_" ~ year ~ "-" ~ "{:02d}".format(month) ~ ".parquet" %}
    {% set col_exprs = [] %}
    {% for col, dtype in columns %}
      {% do col_exprs.append("CAST(" ~ col ~ " AS " ~ dtype ~ ") AS " ~ col) %}
    {% endfor %}
    {% set query %}
      SELECT
        {{ col_exprs | join(",\n        ") }}
      FROM parquet.`{{ path }}`
    {% endset %}
    {% do queries.append(query) %}
  {% endfor %}
{% endfor %}

{{ queries | join("\nUNION ALL\n") }}
{% endmacro %}
