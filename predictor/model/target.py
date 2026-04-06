"""Target variable construction for the predictor.

Creates binary labels: "did price go up in the next N minutes?"
"""

from __future__ import annotations

import pandas as pd

from predictor.constants import INTERVAL_MINUTES


def add_target(
    df: pd.DataFrame,
    horizon: str = "5m",
    base_interval: str = "5m",
) -> pd.DataFrame:
    """Add binary target column: 1 if price went up, 0 if down.

    Args:
        df: DataFrame with 'close' column.
        horizon: Prediction horizon (e.g., "5m", "15m", "1h").
        base_interval: Base candle interval of the data.

    Returns:
        DataFrame with target column added. Last N rows dropped (no future).
    """
    base_minutes = INTERVAL_MINUTES[base_interval]
    horizon_minutes = INTERVAL_MINUTES[horizon]
    bars_ahead = horizon_minutes // base_minutes

    if bars_ahead < 1:
        bars_ahead = 1

    col_name = f"target_up_{horizon}"

    # Future return: close at t+N vs close at t
    future_close = df["close"].shift(-bars_ahead)
    df[col_name] = (future_close > df["close"]).astype(float)

    # Drop rows where we don't have future data
    df = df.dropna(subset=[col_name])
    df[col_name] = df[col_name].astype(int)

    return df
