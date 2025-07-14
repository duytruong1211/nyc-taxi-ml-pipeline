#!/bin/bash
# Suppress Spark/Hive logs when running dbt
export SPARK_SUBMIT_OPTIONS="--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./config/log4j.properties"

# Forward all arguments to dbt
dbt "$@"
