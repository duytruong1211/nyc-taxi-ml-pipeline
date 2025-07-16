# 🗽 NYC Taxi Trip Data – dbt Transformations

This dbt project builds a clean, modular transformation pipeline for NYC Yellow Taxi data (2023–2024), enriched with weather and zone-level context. It follows the bronze → silver → gold modeling structure.

---

## 🔧 Project Structure

```text
dbt/
├── models/
│   ├── bronze_taxi/        # Combine raw taxi parquets
│   ├── bronze_weather/     # Combine raw weather json
│   ├── silver_taxi/        # Light transformation, filter, join with taxi_zone_lookup
│   └── feature_store/      # ML ready feature tables
├── macros/                 # Reusable SQL logic, used for clean and appending NYC taxi monthly data & weather
├── seeds/                  # Helper csv files, including dim_date.csv and taxi_zone_lookup.csv(zone detailed names)
├── dbt_project.yml         # Project config

