from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

def evaluate_model(y_true, y_pred):
    """
    Compute evaluation metrics for regression.

    Args:
        y_true (array-like): True target values.
        y_pred (array-like): Predicted values.

    Returns:
        dict: Dictionary with MAE, RMSE, R2.
    """
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)

    return {"mae": mae, "rmse": rmse, "r2": r2}

import pandas as pd

def importance_to_df(importance_dict, importance_type):
    if not importance_dict:
        return pd.DataFrame(columns=["feature", "importance", "rank", "type"])
    
    return (
        pd.DataFrame(importance_dict.items(), columns=["feature", "importance"])
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
        .assign(rank=lambda df_: df_.index + 1, type=importance_type)
    )

def extract_feature_importance(model, feature_names, test_start):
    booster = model.get_booster()
    booster.feature_names = feature_names

    df_weight = importance_to_df(booster.get_score(importance_type="weight"), "weight")
    df_weight["test_month"] = test_start.strftime("%Y-%m")
    df_gain = importance_to_df(booster.get_score(importance_type="gain"), "gain")
    df_gain["test_month"] = test_start.strftime("%Y-%m")


    return df_weight, df_gain
