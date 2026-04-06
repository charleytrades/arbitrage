"""Microstructure features — volume and order flow proxies."""

from __future__ import annotations

import numpy as np
import pandas as pd


def add_volume_features(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Add volume ratio and taker buy ratio."""
    # Volume relative to rolling average
    vol_ma = df["volume"].rolling(window).mean()
    df["volume_ratio_20"] = np.where(vol_ma > 0, df["volume"] / vol_ma, 1.0)

    # Taker buy ratio — fraction of volume from aggressive buyers
    if "taker_buy_volume" in df.columns:
        df["taker_buy_ratio"] = np.where(
            df["volume"] > 0,
            df["taker_buy_volume"] / df["volume"],
            0.5,
        )
    else:
        df["taker_buy_ratio"] = 0.5

    return df


def add_order_flow_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """Add close-position-in-range and bar range features.

    close_position_in_range: where the close sits within the high-low range.
    Values near 1.0 = buying pressure (close near high).
    Values near 0.0 = selling pressure (close near low).

    bar_range: (high - low) / open as a volatility/range measure.
    """
    hl_range = df["high"] - df["low"]

    df["close_position_in_range"] = np.where(
        hl_range > 0,
        (df["close"] - df["low"]) / hl_range,
        0.5,
    )

    df["bar_range"] = np.where(
        df["open"] > 0,
        hl_range / df["open"],
        0.0,
    )

    return df
