"""Constants for the predictor module."""

# Binance kline intervals we fetch
INTERVALS = ["1m", "5m", "15m", "1h"]

# Map interval string to minutes
INTERVAL_MINUTES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

# Binance kline column names
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]

# Feature columns output by the pipeline (defined here so model and predictor agree)
FEATURE_GROUPS = {
    "returns": [
        "ret_1", "ret_5", "ret_15", "ret_60",
    ],
    "technical": [
        "rsi_14", "macd_line", "macd_signal", "macd_hist",
        "bb_position", "bb_width",
    ],
    "volatility": [
        "vol_5", "vol_15", "vol_60",
    ],
    "volume": [
        "volume_ratio_20", "taker_buy_ratio",
    ],
    "microstructure": [
        "close_position_in_range", "bar_range",
    ],
    "temporal": [
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    ],
    "multi_tf": [
        "ret_15m_1", "ret_15m_5",
        "ret_1h_1", "ret_1h_5",
    ],
}

# Flat list of all features
ALL_FEATURES: list[str] = []
for group in FEATURE_GROUPS.values():
    ALL_FEATURES.extend(group)
