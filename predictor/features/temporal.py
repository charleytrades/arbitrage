"""Temporal features — cyclical time encoding."""

from __future__ import annotations

import numpy as np
import pandas as pd


def add_cyclical_time(df: pd.DataFrame) -> pd.DataFrame:
    """Add sin/cos encoding for hour-of-day and day-of-week.

    Cyclical encoding avoids the discontinuity at midnight (23→0)
    and at week boundaries (Sunday→Monday).
    """
    idx = df.index
    if not hasattr(idx, "hour"):
        # Try to convert if not a DatetimeIndex
        idx = pd.DatetimeIndex(idx)

    hour = idx.hour + idx.minute / 60.0
    dow = idx.dayofweek

    df["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7.0)

    return df
