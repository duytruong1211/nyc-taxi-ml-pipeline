# scripts/bronze_ingestion.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from config import WAREHOUSE_DIR, RAW_YELLOW_DIR, BRONZE_OUTPUT_PATH


# scripts/bronze_ingestion.py
import sys
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import RAW_YELLOW_DIR, BRONZE_OUTPUT_PATH

def combine_parquet_to_delta(
    raw_dir=RAW_YELLOW_DIR,
    output_path=BRONZE_OUTPUT_PATH,
    table_name="spark_catalog.dbt_nyc.bronze_nyc_taxi"
):
    spark = (
        SparkSession.builder
        .appName("BronzeIngestion")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    input_files = sorted([
        os.path.join(raw_dir, f)
        for f in os.listdir(raw_dir)
        if f.startswith("yellow_tripdata_2023-") or f.startswith("yellow_tripdata_2024-")
    ])

    if not input_files:
        print("❌ No matching files found.")
        return

    print(f"📦 Combining {len(input_files)} Parquet files...")

    dfs = [
        spark.read.parquet(file).withColumn("VendorID", F.col("VendorID").cast("long"))
        for file in input_files
    ]

    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    combined_df = combined_df.repartition(6)
    combined_df.write.format("delta").mode("overwrite").save(output_path)
    print(f"✅ Delta table saved to: {output_path}")

    spark.sql("CREATE DATABASE IF NOT EXISTS dbt_nyc")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{os.path.abspath(output_path)}'")
    print(f"📌 Registered Delta table as: {table_name}")
    spark.sql("SHOW TABLES IN dbt_nyc").show()

if __name__ == "__main__":
    combine_parquet_to_delta()
