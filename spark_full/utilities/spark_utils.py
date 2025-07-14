import os
import findspark
from pyspark.sql import SparkSession
from utilities.spark_config import SPARK_HOME, WAREHOUSE_DIR, METASTORE_PATH, XGBOOST_JAR,PROJECT_ROOT

def get_spark_session(app_name="NYC_Taxi_Model"):
    findspark.init(SPARK_HOME)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={METASTORE_PATH};create=true")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.jars", XGBOOST_JAR)
        .config("spark.pyspark.python", os.environ.get("PYSPARK_PYTHON", "python3"))
        .config("spark.pyspark.driver.python", os.environ.get("PYSPARK_PYTHON", "python3"))
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark session initialized (v{spark.version})")
    return spark