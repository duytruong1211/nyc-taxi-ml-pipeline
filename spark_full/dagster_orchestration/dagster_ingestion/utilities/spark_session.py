from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession configured for:
    - Delta Lake support
    - Hive Metastore integration
    - Local development (single session)
    """
    return (
        SparkSession.builder
        .appName("DeltaWithHiveSession")
        .master("local[*]")
        # Delta Lake configs
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Hive Metastore configs
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", "file:/app/hive/warehouse")  # Use absolute path if Dockerized
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:metastore_db;create=true")
        .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        # Optional tuning
        .config("spark.driver.memory", "8g")
        .enableHiveSupport()
        .getOrCreate()
    )
