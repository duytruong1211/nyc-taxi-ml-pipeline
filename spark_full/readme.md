# ⚡ spark_full – End-to-End ML Prototype with Spark, dbt, and Dagster

This folder contains a complete prototype of the NYC Taxi trip duration prediction pipeline — built with Apache Spark, integrated with dbt and Dagster for scalable transformation and orchestration.

It serves as a **production-style artifact** showcasing modular pipeline design, weather integration, feature store construction, and model evaluation using Spark ML.

---

## 🧱 Components

```text
spark_full/
├── spark_eta_model.ipynb       # Main prototype notebook: Spark ML training + evaluation
├── dbt/                         # dbt models for transforming taxi, weather, zone data
├── dagster_project/             # Dagster assets for orchestrating data + ML flows
└── README.md
