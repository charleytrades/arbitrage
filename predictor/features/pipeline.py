"""Feature pipeline — combines all feature groups into a model-ready DataFrame.

Takes multi-timeframe OHLCV data and produces a single DataFrame with
all features aligned to the base timeframe.
"""

from __future__ import annotations

import pandas as pd

from predictor.constants import ALL_FEATURES
from predictor.features.microstructure import add_order_flow_proxy, add_volume_features
from predictor.features.technical import (
    add_bollinger,
    add_macd,
    add_returns,
    add_rsi,
    add_volatility,
)
from predictor.features.temporal import add_cyclical_time


def build_features(
    df_dict: dict[str, pd.DataFrame],
    base_interval: str = "5m",
) -> pd.DataFrame:
    """Build all features from multi-timeframe OHLCV data.

    Args:
        df_dict: {interval: DataFrame} for one symbol.
                 Must include base_interval and optionally higher TFs.
        base_interval: The interval to compute features on.

    Returns:
        DataFrame with all feature columns, NaN rows dropped.
    """
    if base_interval not in df_dict:
        raise ValueError(f"Base interval '{base_interval}' not in data")

    df = df_dict[base_interval].copy()

    # ── Base timeframe features ──────────────────────────────
    df = add_returns(df, periods=[1, 5, 15, 60])
    df = add_rsi(df, length=14)
    df = add_macd(df)
    df = add_bollinger(df, length=20)
    df = add_volatility(df, windows=[5, 15, 60])
    df = add_volume_features(df, window=20)
    df = add_order_flow_proxy(df)
    df = add_cyclical_time(df)

    # ── Multi-timeframe features ─────────────────────────────
    # Merge higher-TF returns onto base timeframe
    for tf_label, tf_key in [("15m", "15m"), ("1h", "1h")]:
        if tf_key in df_dict:
            htf = df_dict[tf_key][["close"]].copy()
            # Compute returns on the higher timeframe
            htf[f"ret_{tf_label}_1"] = htf["close"].pct_change(1)
            htf[f"ret_{tf_label}_5"] = htf["close"].pct_change(5)
            htf = htf.drop(columns=["close"])

            # Forward-fill higher TF values onto base TF timestamps
            df = pd.merge_asof(
                df,
                htf,
                left_index=True,
                right_index=True,
                direction="backward",
            )

    # ── Fill missing multi-TF columns with 0 ────────────────
    for col in ["ret_15m_1", "ret_15m_5", "ret_1h_1", "ret_1h_5"]:
        if col not in df.columns:
            df[col] = 0.0

    # ── Drop warmup NaN rows ─────────────────────────────────
    df = df.dropna(subset=_available_features(df))

    return df


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return the list of feature columns that exist in this DataFrame."""
    return _available_features(df)


def _available_features(df: pd.DataFrame) -> list[str]:
    """Return feature columns from ALL_FEATURES that exist in df."""
    return [f for f in ALL_FEATURES if f in df.columns]
