"""Technical indicator features computed from OHLCV data.

Uses pandas-ta for standard indicators. All functions take a DataFrame
with OHLCV columns and return it with new columns appended.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pandas_ta as ta


def add_returns(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
    """Add log returns at various lookback periods."""
    periods = periods or [1, 5, 15, 60]
    for p in periods:
        if len(df) > p:
            df[f"ret_{p}"] = np.log(df["close"] / df["close"].shift(p))
    return df


def add_rsi(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    """Add RSI indicator, normalized to 0-1."""
    rsi = ta.rsi(df["close"], length=length)
    if rsi is not None:
        df["rsi_14"] = rsi / 100.0  # Normalize to 0-1
    return df


def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """Add MACD line, signal, and histogram."""
    macd = ta.macd(df["close"])
    if macd is not None:
        cols = macd.columns.tolist()
        # pandas-ta returns columns like MACD_12_26_9, MACDs_12_26_9, MACDh_12_26_9
        df["macd_line"] = macd[cols[0]]
        df["macd_signal"] = macd[cols[1]]
        df["macd_hist"] = macd[cols[2]]
    return df


def add_bollinger(df: pd.DataFrame, length: int = 20) -> pd.DataFrame:
    """Add Bollinger Band position (0-1) and width."""
    bb = ta.bbands(df["close"], length=length)
    if bb is not None:
        cols = bb.columns.tolist()
        # cols: BBL_20_2.0, BBM_20_2.0, BBU_20_2.0, BBB_20_2.0, BBP_20_2.0
        lower = bb[cols[0]]
        upper = bb[cols[2]]
        band_range = upper - lower
        df["bb_position"] = np.where(
            band_range > 0,
            (df["close"] - lower) / band_range,
            0.5,
        )
        df["bb_width"] = band_range / df["close"]
    return df


def add_volatility(df: pd.DataFrame, windows: list[int] | None = None) -> pd.DataFrame:
    """Add rolling volatility (std of log returns)."""
    windows = windows or [5, 15, 60]
    log_ret = np.log(df["close"] / df["close"].shift(1))
    for w in windows:
        if len(df) > w:
            df[f"vol_{w}"] = log_ret.rolling(w).std()
    return df
