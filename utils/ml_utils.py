import os
import pandas as pd
import mlflow
from models.configs import generate_metadata
import numpy as np

def save_evaluation_outputs(eval_dir, run_name_prefix, test_start, train_start, train_end, metrics, df_weight, df_gain):
    """
    Appends model evaluation outputs (metrics and feature importance) to CSV files in eval_dir.
    
    Args:
        eval_dir (str): Directory path to save evaluation outputs.
        run_name_prefix (str): Prefix for the run name.
        test_start (datetime): Start date of test period.
        train_start (datetime): Start date of train period.
        train_end (datetime): End date of train period.
        metrics (dict): Dictionary of evaluation metrics (e.g., mae, rmse, r2).
        df_weight (pd.DataFrame): Feature importance (split count).
        df_gain (pd.DataFrame): Feature importance (gain).
    """
    os.makedirs(eval_dir, exist_ok=True)

    test_month_str = test_start.strftime('%Y-%m')
    run_name = f"{run_name_prefix}_{test_month_str}"

    # --- Save metrics ---
    metrics_df = pd.DataFrame([{
        "run_name": run_name,
        "test_month": test_month_str,
        "train_start": train_start.strftime("%Y-%m"),
        "train_end": train_end.strftime("%Y-%m"),
        **metrics
    }])
    metrics_path = os.path.join(eval_dir, "metrics_log.csv")
    metrics_df.to_csv(metrics_path, mode='a', index=False, header=not os.path.exists(metrics_path))

    # --- Save feature importances ---
    weight_path = os.path.join(eval_dir, "feature_importance_weight.csv")
    df_weight["run_name"] = run_name
    df_weight.to_csv(weight_path, mode='a', index=False, header=not os.path.exists(weight_path))

    gain_path = os.path.join(eval_dir, "feature_importance_gain.csv")
    df_gain["run_name"] = run_name
    df_gain.to_csv(gain_path, mode='a', index=False, header=not os.path.exists(gain_path))


def log_pipeline_to_mlflow(
    model,
    run_name,
    metadata,
    metrics,
    df_gain,
    df_weight,
    X_test,
    artifact_path="pandas_model"
):
    """
    Log model, params, metrics, tags to MLflow for a single test month run.
    """

    # mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_tracking_uri("file:///app/mlruns")  


    with mlflow.start_run(run_name=run_name):
        for k, v in metadata["params"].items():
            mlflow.log_param(k, v)
        for k, v in metrics.items():
            mlflow.log_metric(k, v)
        for k, v in metadata["tags"].items():
            mlflow.set_tag(k, v)
        

                # Top 5 by GAIN
        top_gain = df_gain.sort_values("importance", ascending=False).head(5)["feature"].tolist()
        for i, feat in enumerate(top_gain, 1):
            mlflow.log_param(f"gain_feat_{i}", feat)

        # Top 5 by WEIGHT
        top_weight = df_weight.sort_values("importance", ascending=False).head(5)["feature"].tolist()
        for i, feat in enumerate(top_weight, 1):
            mlflow.log_param(f"weight_feat_{i}", feat)

 
        input_example = X_test.iloc[[0]].copy()
        input_example = input_example.astype(np.float64)  
        mlflow.sklearn.log_model(model, name=artifact_path,input_example=input_example)
  
  



def save_predictions_to_csv(test_df, preds, test_start, run_name_prefix, eval_dir):
    """
    Append predicted vs actual trip durations to a CSV file in eval_dir.

    Args:
        test_df (pd.DataFrame): Test set with actual labels and pickup times.
        preds (np.array): Model predictions.
        test_start (datetime): First day of test month.
        run_name_prefix (str): Prefix for this model run.
        eval_dir (str): Directory to save output CSV.
    """
    os.makedirs(eval_dir, exist_ok=True)

    run_name = f"{run_name_prefix}_{test_start.strftime('%Y-%m')}"
    test_month_str = test_start.strftime('%Y-%m')

    pred_df = pd.DataFrame({
        "pickup_date": test_df["pickup_date"].values,
        "actual_duration_min": test_df["trip_duration_minutes"].values,
        "predicted_duration_min": preds,
        "run_name": run_name,
        "test_month": test_month_str
    })

    path = os.path.join(eval_dir, "predictions_log.csv")
    pred_df.to_csv(path, mode='a', index=False, header=not os.path.exists(path))

