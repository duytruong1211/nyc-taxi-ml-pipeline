WITH weather_features AS (
SELECT
  DATE(time) AS weather_date,
  HOUR(time) AS weather_hour,
  borough,

  -- Raw values
  temperature_2m AS temperature,
  precipitation,
  snowfall AS snow,
  windspeed_10m AS windspeed,
  cloudcover,

  -- Binary flags
  CASE WHEN precipitation >= 5 THEN 1 ELSE 0 END AS is_heavyrain,
  CASE WHEN snowfall >= 5 THEN 1 ELSE 0 END AS is_snowfall,
  CASE WHEN windspeed >= 30 THEN 1 ELSE 0 END AS is_highwind,
  CASE WHEN temperature_2m < -5 THEN 1 ELSE 0 END AS is_cold,
  CASE WHEN temperature_2m >= 35 THEN 1 ELSE 0 END AS is_hot,

  -- Categorical label (priority order)
  CASE
    WHEN precipitation >= 5 THEN 'heavy_rain'
    WHEN snowfall >= 5 THEN 'snowfall'
    WHEN windspeed >= 30 THEN 'high_wind'
    WHEN temperature_2m < -5 THEN 'cold'
    WHEN temperature_2m >= 35 THEN 'hot'
    ELSE 'clear'
  END AS weather_condition,

  -- Composite flag
  CASE
    WHEN precipitation >= 5 OR snowfall >= 5 OR windspeed >= 30 OR temperature_2m < -5 OR temperature_2m >= 35
    THEN 1 ELSE 0
  END AS is_bad_weather

FROM {{ ref('bronze_weather') }}
)

SELECT * FROM weather_features;
