from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import OrdinalEncoder

def filter_and_split_data(df, test_month, label_col, all_features):
    """
    Returns train_df, test_df after filtering by rolling 3-month window.
    """
    test_start = test_month
    test_end = test_start + relativedelta(months=1)
    train_start = test_start - relativedelta(months=3)
    train_end = test_start

    train_df = df[(df["pickup_date"] >= train_start) & (df["pickup_date"] < train_end)].dropna(subset=[label_col] + all_features)
    test_df = df[(df["pickup_date"] >= test_start) & (df["pickup_date"] < test_end)].dropna(subset=[label_col] + all_features)
    return train_df, test_df, train_start, train_end, test_start, test_end


def encode_categoricals(train_df, test_df, cat_features):
    """
    Applies ordinal encoding, safely handling unknown values.
    """
    encoder = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    train_df[cat_features] = encoder.fit_transform(train_df[cat_features])
    test_df[cat_features] = encoder.transform(test_df[cat_features])
    return train_df, test_df
