---
title: "NYC Taxi ML Pipeline"
output: github_document
---

# 🗽 NYC Taxi ML Pipeline

A full-stack machine learning pipeline to predict NYC taxi trip duration.

**Designed for:** clarity, reproducibility, and real-world ML infra readiness — with lean setup for cloning.

---

## 🧭 Project Evolution

> **Prototyped in Spark. Delivered in pandas.**

The project began with a full **Spark-based workflow** for processing large-scale NYC TLC data, using:

- 🔹 Spark for prototyping data transformations and rolling aggregations  
- 🔹 dbt for SQL-style modeling  
- 🔹 Dagster for orchestration (optional)

However, for simplicity, speed, and **ease of cloning**, we **ported the full pipeline to Python + pandas**.

### ✅ Why pandas for final version?

- ⚡ Lightweight: no Spark setup or cluster needed
- 💻 Local-friendly: works out-of-the-box via `python main.py`
- 🔁 Same logic: All Spark SQL logic reimplemented using pandas
- 📦 Portable: Easy to fork, run, and adapt


Both versions are maintained:

- **`/spark_full/`** folder — full Spark/dbt version
- **Root folder** — lean pandas version for reproducibility and demo


## 🚀 Quickstart


```bash
pip install -r requirements.txt
python main.py
```

---

## 📂 Project Structure

```text
nyc-taxi-ml-pipeline/
├── data/                # Data snapshots (bronze/silver/gold)
├── models/              # Saved XGBoost models
├── notebooks/           # EDA + diagnostics
├── pipeline/            # Data ingestion and transformation scripts
├── weather/             # Open-Meteo ingestion and precompute logic
├── main.py              # Pipeline entry point
├── Dockerfile
└── README.md
```

---

## 🔍 Model Performance

| Month     | R²   | RMSE (min) |
|-----------|------|------------|
| Jan 2025  | 0.78 | 5.02       |
| Feb 2025  | 0.80 | 4.88       |
| Mar 2025  | 0.77 | 5.20       |
| Peak (Aug)| **0.81** | **4.80** |

> 🧠 Drop in December likely caused by holiday route disruptions and erratic demand.

---

## 🧱 Feature Store Summary

- **Temporal**:
  - Hour of day (raw + sin/cos), day-of-week, weekend flag, holiday/payroll flag
  - Rush hour (morning/evening)
- **Spatial**:
  - Pickup/dropoff zone & borough, same zone/borough, airport flags
- **Rolling Aggregates**:
  - `trip_count`, `avg_duration`, `avg_speed`, `avg_distance`, `avg_fare`
  - Precomputed for 3, 6, 12 months per PU_DO zone pair
- **Weather**:
  - Hourly temperature, precipitation, snow, windspeed, cloudcover
  - Joined by borough and timestamp

---

## 🔗 MLflow Demo

MLflow tracks:

- Run parameters: model type, feature set
- Metrics: RMSE, MAE, R²
- Feature importance (split gain + gain weight)
- Versioned artifact logs (models, inputs)

👉 Access locally at: [http://localhost:5000](http://localhost:5000)

---

## 🧠 Key Insights

- 🛫 **Airport trips**: Longer and more variable — peak morning traffic inflates duration
- 🌧️ **Heavy rain & snow**: Strong predictors of delay — shown via gain importance
- 💰 **Fare vs duration**: Weak correlation in December — potential surge fare anomaly
- ⌛ **Rush hours**: Morning > Evening in impact on average trip duration

---

## 📘 Reports & Demos

- 🗒️ [Notion Summary Report](#) *(stakeholder view – business insights)*
- 📓 [Notebook HTML Walkthrough](#) *(Spark ML version – deep dive)*
- 🛠️ [GitHub Repo](https://github.com/duytruong1211/nyc-taxi-ml-pipeline) *(lean pandas version – forkable)*

---

## 🧰 Tech Stack

- **Compute**: PySpark 3.x, Docker
- **ETL**: Python, dbt
- **ML**: XGBoost, scikit-learn
- **Tracking**: MLflow
- **Data**: NYC TLC, Open-Meteo Archive API
- **Orchestration**: Dagster *(optional, not required for pandas version)*

---



---

Built by [@duytruong1211](https://github.com/duytruong1211) — tailored for clarity, portability, and real-world data science workflow.

