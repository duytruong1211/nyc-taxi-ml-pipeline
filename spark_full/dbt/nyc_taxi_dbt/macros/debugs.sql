{% macro preview_bronze_raw() %}
  {% set query %}
    SELECT * FROM {{ ref('bronze_raw') }} LIMIT 10
  {% endset %}
  {{ return(run_query(query)) }}
{% endmacro %}
