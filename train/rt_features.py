"""Real-time feature logic shared by the TXN_RISK feature view (online) and training (offline).

Only pandas is used so the function is valid as a Feature Store compute_fn.
"""
import pandas as pd


def compute_txn_risk(request_df, profile_df, velocity_df):
    amt = request_df["TXN_AMOUNT"].astype(float).reset_index(drop=True)
    n = len(amt)
    zeros = pd.Series([0.0] * n)
    # Source rows are omitted when a key has no stored value, so fall back to zeros
    # (no nested helpers allowed in compute_fn)
    if len(profile_df) == n:
        mean = profile_df["CARDID_MEAN_AMOUNT_60D"].astype(float).fillna(0.0).reset_index(drop=True)
        std = profile_df["CARDID_STD_AMOUNT_60D"].astype(float).fillna(0.0).reset_index(drop=True)
    else:
        mean, std = zeros, zeros
    if len(velocity_df) == n:
        cnt5m = velocity_df["CARDID_COUNT_5M"].astype(float).fillna(0.0).reset_index(drop=True)
    else:
        cnt5m = zeros
    safe_mean = mean.where(mean > 0, amt.where(amt > 0, 1.0))
    safe_std = std.where(std > 0, safe_mean)
    return pd.DataFrame({
        "AMOUNT_TO_MEAN_RATIO": amt / safe_mean,
        "AMOUNT_ZSCORE": (amt - mean) / safe_std,
        "IS_BURST": (cnt5m >= 4).astype(float),
    })
