{% macro load_weather_parquet(base_path, years=[2023, 2024]) %}
{% set boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"] %}
{% set months = range(1, 13) %}
{% set queries = [] %}

{% for borough in boroughs %}
  {% for year in years %}
    {% for month in months %}
      {% set file_path = base_path ~ "/" ~ borough ~ "_" ~ year ~ "_" ~ "{:02d}".format(month) ~ ".parquet" %}
      {% set query %}
        SELECT * FROM parquet.`{{ file_path }}`
      {% endset %}
      {% do queries.append(query) %}
    {% endfor %}
  {% endfor %}
{% endfor %}

{{ queries | join("\nUNION ALL\n") }}
{% endmacro %}
