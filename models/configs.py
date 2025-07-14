from datetime import datetime
import pandas as pd

def get_feature_lists():
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
    return numeric_features, cat_features, numeric_features + cat_features


# Load Tuned hyperparameters for XGB
def get_xgb_params():
    return {
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "objective": "reg:absoluteerror",
        "verbosity": 0
    }

def generate_metadata(test_start, train_start, train_end, feature_tag, label_col):
    """
    Returns params, metrics, tags dictionary stubs.
    """
    model_params = get_xgb_params()
    return {
        "params": {
            "model": "XGBRegressor",
            **model_params,
            "train_start": train_start.strftime("%Y-%m"),
            "train_end": (train_end - pd.Timedelta(days=1)).strftime("%Y-%m"),
            "test_month": test_start.strftime("%Y-%m")
        },
        "tags": {
            "features": feature_tag,
            "rolling": "3mo",
            "run_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "label_col": label_col
        }
    }
