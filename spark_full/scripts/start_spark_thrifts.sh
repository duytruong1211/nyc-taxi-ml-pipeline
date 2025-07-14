#!/bin/bash
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Assumes script lives in scripts/ or a subdir
WAREHOUSE_DIR = str(PROJECT_ROOT / "spark-warehouse")
METASTORE_DB = str(PROJECT_ROOT / "metastore_db")

echo "📡 Starting Spark Thrift Server..."
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master local[*] \
  --driver-memory 4g \
  --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR \
  --conf javax.jdo.option.ConnectionURL=jdbc:derby:$METASTORE_DB;create=true \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
