# 🗽 NYC Taxi ML Pipeline

A full-stack machine learning pipeline to predict NYC taxi trip duration.

**Designed for:** clarity, reproducibility, and real-world ML infra readiness — with lean setup for cloning.

---
## Project Summary

This is a solo project built as a demonstration of end-to-end ML workflow using NYC Taxi data.  
All data processing, modeling, and documentation were done by me.

Data Source: NYC Taxi Dataset (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Weather Data: Open-Meteo API (https://open-meteo.com)  


## 📘 Reports

- 🗒️ [Notion Summary Report](https://bitter-aster-788.notion.site/NYC-Taxi-ETA-Forecasting-Modeling-Meets-Real-World-Chaos-2248b64123bf805991cae210535ab3c7) *(stakeholder view – business insights)*
- 📓 [Notebook Spark Walkthrough](spark_full/spark_eta_model.ipynb) *(Spark ML version – deep dive)*


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

## 🚧 Prerequisites

Before you run this project, make sure you have the following installed:

## 🐳 Docker

- [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)  
  (Required for building and running containers)

You can test Docker is working with:

```bash
docker --version
docker-compose --version
```
## 🚀 Quickstart
```bash
### ✅ Clone the repo
git clone https://github.com/duytruong1211/nyc-taxi-ml-pipeline.git
cd nyc-taxi-ml-pipeline

### 🔨 One-time build (first-time users)
make build

### 🧪 Run test pipeline (quick demo)
make test
### → Ingests Jan–Apr 2024
### → Trains + logs model for April 2024

### 🛠️ Run full historical pipeline (bulk mode)
make bulk
### → Ingests + processes all 2023–2024 trips
### → Builds rolling features per PU/DO pair
### → Trains + logs models per month

### 📅 Ingest new month (incremental mode)
make incremental YEAR=2025 MONTH=1
### → Ingests new trip data
### → Updates zone-pair aggregates
### → Retrains model on latest month

### 📊 Launch MLflow UI to track model runs
make ui
### → Open http://localhost:5001 in browser

### 🧼 Cleanup (optional)
make stop            # Stop all containers
make clean           # Stop + remove volumes
make clean-orphans   # Delete leftover run containers
```
---

## 📂 Project Structure

```text
nyc-taxi-ml-pipeline/
├── data/                            # Raw and processed datasets
│   ├── bronze/                      # Raw ingested files
│   ├── silver/                      # Cleaned + enriched datasets
│   └── gold/                        # Final ML-ready feature tables
│
├── etl/                             # Scripts for data ingestion and transformation 
│   ├── bronze.py                    # Data ingestion
│   └── silver.py                    # Ligh transform, downsizing 
│   └── gold.py                      # Enriched, ultilized DUCKDB
│
├── models/                          # Scripts for model building
│   ├── configs.py                   # Constant variables, Tuned hyperparameters
│   └── evaluates.py                 # Metrics, Feature Importances
│   └── prepare.py                   # Train test split, Encoding
│
├── pipeline/                        # Combine everything together
│   ├── ml_pipeline.py               # Build Model, save results to MLFLow
│   ├── pipeline.py                  # Bronze -> Silver -> Gold
│
├── spark_full/                      # Spark artifact, dbt, dagster
│
├── main.py                          # Pipeline orchestration entry point
├── Dockerfile                       # Reproducible container setup for pipeline
└── README.md                        # Project overview and usage guide
```

---

## 🔍 Model Overview

After running the pipeline, launch MLflow UI:

```bash
mlflow ui
```
You will see a dashboard like this. CLick on the Columns tab to track metrics( MAE, R2, feature gains/weight)
![MLflow Dashboard](utils/mlflow_output.png)


---

## 🧱 Feature Store Summary

- **Temporal**  
  `pickup_hour`, `is_rush_hour_morning`, `is_weekend`

- **Calendar**  
  `is_holiday`, `is_near_holiday`, `is_payroll_window`

- **Zone & Ride Info**  
  `is_same_zone`, `is_airport_pu_trip`, `passenger_count`

- **Weather** *(by borough & hour)*  
  `temperature`, `precipitation`, `cloudcover`

- **Rolling Aggregates** *(PU-DO pairs)*  
  `avg_duration_min_3mo`, `avg_fare_12mo`, `trip_count_12mo`


---




## 🧰 Tech Stack

- **Compute**: PySpark 3.x, Docker
- **ETL**: Python, dbt
- **DB**: DuckDB
- **ML**: XGBoost
- **Tracking**: MLflow
- **Data**: NYC TLC, Open-Meteo Archive API
- **Orchestration**: Dagster *(optional, not required for pandas version)*

---



---

Built by [@duytruong1211](https://github.com/duytruong1211) 

