from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor
import mlflow
import mlflow.spark


# ✅ Label column
label_col = "trip_duration_minutes"

# ✅ Numeric and categorical features (same as RF)
numeric_features = [
    'pickup_hour', 'sin_hour', 'cos_hour',
    "is_rush_hour_morning", "is_rush_hour_evening",
    'passenger_count', 'is_airport_pu_trip', 'is_airport_do_trip',
    'is_store_and_fwd', 'is_same_borough', 'is_near_holiday', 'is_holiday','is_same_zone',
    'sin_dow', 'cos_dow', 'is_weekend',
    'sin_month', 'cos_month', 'is_payroll_window',
    'temperature', 'precipitation', 'snow', 'windspeed', 'cloudcover',

    # Rolling features
    "avg_duration_min_3mo",  "avg_duration_min_12mo",
    "avg_distance_3mo",        "avg_distance_12mo",
    "avg_speed_3mo",              "avg_speed_12mo",
    "avg_fare_3mo",                "avg_fare_12mo",
    "trip_count_3mo",            "trip_count_12mo"
]

cat_features = [
    'vendor_name', 'rate_code_type', 'payment_type_str',
    'pu_borough', 'do_borough'
]



def prepare_rolling_train_test(df, test_month, label_col, features):
    test_start = test_month
    test_end = test_start + relativedelta(months=1)

    train_start = test_start - relativedelta(months=3)
    train_end = test_start

    trace_cols = ["pickup_date", "pu_zone", "do_zone"]

    train_df = (
        df.filter((col("pickup_date") >= lit(train_start)) & (col("pickup_date") < lit(train_end)))
          .select([label_col] + features + trace_cols)
          .dropna()
          .withColumn("trip_id", monotonically_increasing_id())
    )

    test_df = (
        df.filter((col("pickup_date") >= lit(test_start)) & (col("pickup_date") < lit(test_end)))
          .select([label_col] + features + trace_cols)
          .dropna()
          .withColumn("trip_id", monotonically_increasing_id())
    )

    return train_df, test_df, train_start, train_end, test_start

def build_xgb_pipeline(numeric_features, cat_features, label_col):

    # --- Best tuned XGBoost parameters (Combo D) ---
    best_params = {
        "max_depth": 6,
        "eta": 0.1,
        "num_round": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.8
    }

    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in cat_features
    ]

    assembler = VectorAssembler(
        inputCols=numeric_features + [f"{col}_idx" for col in cat_features],
        outputCol="features"
    )

    xgb = SparkXGBRegressor(
        features_col="features",
        label_col=label_col,
        prediction_col="prediction",
        objective="reg:absoluteerror",  # Focus on MAE
        **best_params
    )

    return Pipeline(stages=indexers + [assembler, xgb])

def evaluate_predictions(predictions, label_col):
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")
    return (
        evaluator.setMetricName("rmse").evaluate(predictions),
        evaluator.setMetricName("mae").evaluate(predictions),
        evaluator.setMetricName("r2").evaluate(predictions)
    )
def log_xgb_metrics_to_mlflow(
    model,
    run_name,
    metrics_dict,
    params_dict,
    tag_dict=None,
    artifact_path="spark_model",
    tuned=False
):
    """
    Logs parameters, metrics, tags, and Spark model to MLflow.
    """
    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    with mlflow.start_run(run_name=run_name):
        for k, v in params_dict.items():
            mlflow.log_param(k, v)
        mlflow.set_tag("tuned", str(tuned).lower())
        for k, v in metrics_dict.items():
            mlflow.log_metric(k, v)
        if tag_dict:
            for k, v in tag_dict.items():
                mlflow.set_tag(k, v)
        mlflow.spark.log_model(model, artifact_path=artifact_path)


def run_and_log_single_month(
    df,
    test_month: datetime,
    run_name_prefix: str,
    label_col: str = "trip_duration_minutes",
    feature_tag: str = "rolling_3_pruned",
    log_to_ml_flow = True
):
    # Internal feature config
    numeric_features = [
        'pickup_hour', 'sin_hour', 'cos_hour',
        "is_rush_hour_morning", "is_rush_hour_evening",
        'passenger_count', 'is_airport_pu_trip', 'is_airport_do_trip',
        'is_store_and_fwd', 'is_same_borough', 'is_near_holiday', 'is_holiday','is_same_zone',
        'sin_dow', 'cos_dow', 'is_weekend',
        'sin_month', 'cos_month', 'is_payroll_window',
        'temperature', 'precipitation', 'snow', 'windspeed', 'cloudcover',
        "avg_duration_min_3mo",  "avg_duration_min_12mo",
        "avg_distance_3mo",        "avg_distance_12mo",
        "avg_speed_3mo",              "avg_speed_12mo",
        "avg_fare_3mo",                "avg_fare_12mo",
        "trip_count_3mo",            "trip_count_12mo"
    ]
    cat_features = [
        'vendor_name', 'rate_code_type', 'payment_type_str',
        'pu_borough', 'do_borough'
    ]

    best_params = {
        "max_depth": 6,
        "eta": 0.1,
        "num_round": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.8
    }

    features = numeric_features + cat_features
    train_df, test_df, train_start, train_end, test_start = prepare_rolling_train_test(
        df, test_month, label_col, features
    )

    test_month_str = test_start.strftime("%Y-%m")
    run_name = f"{run_name_prefix}_{test_month_str}"

    pipeline = build_xgb_pipeline(numeric_features, cat_features, label_col)
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    rmse, mae, r2 = evaluate_predictions(predictions, label_col)

    params_dict = {
        "model": "SparkXGBoost",
        **best_params,
        "num_features": len(features),
        "train_start": train_start.strftime("%Y-%m"),
        "train_end": (train_end - relativedelta(days=1)).strftime("%Y-%m"),
        "test_month": test_month_str
    }

    metrics_dict = {"rmse": rmse, "mae": mae, "r2": r2}
    tag_dict = {
        "features": feature_tag,
        "rolling": "3mo",
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "label_col": label_col
    }
    if log_to_ml_flow:

        log_xgb_metrics_to_mlflow(
            model=model,
            run_name=run_name,
            metrics_dict=metrics_dict,
            params_dict=params_dict,
            tag_dict=tag_dict,
            tuned=True
        )

    print(f"✅ {run_name}: RMSE={rmse:.4f}, MAE={mae:.4f}, R2={r2:.4f}")
    return predictions

def load_mae_eval_summary(
    experiment_name="Default",
    feature_tag="rolling_3_pruned",
    run_prefix="xgb_mae",
    tracking_uri="http://127.0.0.1:5000"
):
    """
    Load monthly MAE evaluation summary from MLflow runs.

    Args:
        experiment_name (str): Name of the MLflow experiment.
        feature_tag (str): Feature tag to filter on.
        run_prefix (str): Prefix of MLflow run names.
        tracking_uri (str): URI where MLflow is hosted.

    Returns:
        pd.DataFrame: Summary dataframe with test_month, rmse, mae, r2
    """
    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"Experiment '{experiment_name}' not found.")
    experiment_id = experiment.experiment_id

    runs_df = mlflow.search_runs(
        experiment_ids=[experiment_id],
        filter_string=f"""
            tags.features = '{feature_tag}'
            and tags.tuned = 'true'
            and tags.mlflow.runName LIKE '{run_prefix}%'
        """,
        output_format="pandas"
    )

    summary_df = runs_df[[
        "params.test_month", "metrics.rmse", "metrics.mae", "metrics.r2"
    ]].rename(columns={
        "params.test_month": "test_month",
        "metrics.rmse": "rmse",
        "metrics.mae": "mae",
        "metrics.r2": "r2"
    }).sort_values("test_month")

    summary_df.reset_index(drop=True, inplace=True)
    return summary_df
