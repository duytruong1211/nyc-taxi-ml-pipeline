import os
import pandas as pd
from xgboost import XGBRegressor

from models.prepare import filter_and_split_data, encode_categoricals
from models.configs import get_feature_lists,generate_metadata, get_xgb_params


from models.evaluate import evaluate_model,extract_feature_importance
from utils.ml_utils import save_evaluation_outputs, log_pipeline_to_mlflow,save_predictions_to_csv
from paths import EVAL_RESULTS_DIR,MAIN_FEATURES_PARQUET
from pathlib import Path

def run_ml_pipeline(
    test_month,
    run_name_prefix,
    df=None,
    log_to_mlflow_flag=True
):
    eval_dir = EVAL_RESULTS_DIR
    os.makedirs(eval_dir, exist_ok=True)
    label_col="trip_duration_minutes"
    feature_tag="rolling_3_pruned"

    if df is None:
        feature_path = Path(MAIN_FEATURES_PARQUET)
        if not feature_path.exists():
            print(f"❌ Feature file not found: {feature_path}. Aborting pipeline.")
            return None
        df = pd.read_parquet(feature_path)

    # --- Features ---
    numeric_features, cat_features, all_features = get_feature_lists()

    # --- Split ---
    train_df, test_df, train_start, train_end, test_start, test_end = filter_and_split_data(
        df, test_month, label_col, all_features
    )

    # --- Encode categoricals ---
    train_df, test_df = encode_categoricals(train_df, test_df, cat_features)
    X_train, y_train = train_df[all_features], train_df[label_col]
    X_test, y_test = test_df[all_features], test_df[label_col]
    # --- Train model ---

    print(f"\n🚧 Building model for {test_start.strftime('%Y-%m')}...")

    model = XGBRegressor(**get_xgb_params())
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    # --- Evaluate ---
    metrics = evaluate_model(y_test, preds)

    df_weight, df_gain = extract_feature_importance(model, all_features,test_start)


    print("✅ Model built and evaluated.")

    # # --- Save results to CSV ---

    # save_evaluation_outputs(
    #     eval_dir=eval_dir,
    #     run_name_prefix=run_name_prefix,
    #     test_start=test_start,
    #     train_start=train_start,
    #     train_end=train_end,
    #     metrics=metrics,
    #     df_weight=df_weight,
    #     df_gain=df_gain
    # )

    save_predictions_to_csv(
        test_df=test_df,
        preds=preds,
        test_start=test_start,
        run_name_prefix=run_name_prefix,
        eval_dir=eval_dir
    )

    # --- MLflow logging ---
    if log_to_mlflow_flag:
        run_name = f"{run_name_prefix}_{test_start.strftime('%Y-%m')}"

        print(f"\n📌 Starting MLflow logging for: {run_name}")

        metadata = generate_metadata(test_start, train_start, train_end, feature_tag, label_col)
 
        log_pipeline_to_mlflow(
            model=model,
            run_name=run_name,
            metadata=metadata,
            metrics=metrics,
            artifact_path="pandas_model",
            df_gain=df_gain,
            df_weight=df_weight,
            X_test=X_test
        )

        print("✅ Finished MLflow logging.")


    print(f"\n✅ {test_start.strftime('%Y-%m')}: MAE={metrics['mae']:.4f}, RMSE={metrics['rmse']:.4f}, R2={metrics['r2']:.4f}")

