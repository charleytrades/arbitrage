"""Constants for market discovery, slug generation, and trading parameters."""

from enum import Enum

# ── Supported symbols and bucket durations ──────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Map Binance symbol → Polymarket slug prefix
SYMBOL_TO_SLUG_PREFIX: dict[str, str] = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "SOLUSDT": "sol",
}

BUCKET_DURATIONS_SEC: dict[str, int] = {
    "5m": 300,
    "15m": 900,
}

# ── Polymarket CLOB constants ───────────────────────────────────────
CLOB_SIDE_BUY = "BUY"
CLOB_SIDE_SELL = "SELL"

# Outcome token indices on Polymarket binary markets
OUTCOME_YES = 0
OUTCOME_NO = 1


class TradingMode(str, Enum):
    BACKTEST = "backtest"
    PAPER_TRADE = "paper_trade"
    LIVE = "live"


class Outcome(str, Enum):
    YES = "Yes"
    NO = "No"


# ── Timing ──────────────────────────────────────────────────────────
WS_PING_INTERVAL_SEC = 20
WS_RECONNECT_DELAY_SEC = 2
MARKET_REFRESH_INTERVAL_SEC = 60

# ── Gamma API search tags ───────────────────────────────────────────
GAMMA_CRYPTO_TAG = "crypto"
GAMMA_UPDOWN_KEYWORD = "up/down"
