"""Pydantic models for markets, positions, orders, and signals."""

from __future__ import annotations

import time
from enum import Enum

from pydantic import BaseModel, Field


# ── Enums ───────────────────────────────────────────────────────────
class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Outcome(str, Enum):
    YES = "Yes"
    NO = "No"


class SignalType(str, Enum):
    MOMENTUM_LATENCY = "momentum_latency"
    CROSS_OUTCOME_ARB = "cross_outcome_arb"


# ── Market representation ───────────────────────────────────────────
class MarketInfo(BaseModel):
    """A discovered Polymarket binary micro-market."""

    condition_id: str
    question: str
    slug: str
    token_id_yes: str
    token_id_no: str
    symbol: str  # e.g. "BTCUSDT"
    bucket: str  # "5m" or "15m"
    start_ts: int  # UTC unix seconds – start of the bucket window
    end_ts: int  # UTC unix seconds – end of the bucket window
    active: bool = True

    @property
    def seconds_remaining(self) -> float:
        return max(0.0, self.end_ts - time.time())


# ── Order-book snapshot (simplified) ────────────────────────────────
class BookLevel(BaseModel):
    price: float
    size: float


class OrderBookSnapshot(BaseModel):
    market_id: str
    timestamp: float = Field(default_factory=time.time)
    bids: list[BookLevel] = Field(default_factory=list)
    asks: list[BookLevel] = Field(default_factory=list)

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 1.0


# ── Binance tick ────────────────────────────────────────────────────
class BinanceTick(BaseModel):
    symbol: str
    price: float
    timestamp_ms: int
    volume: float = 0.0


# ── Trading signal ──────────────────────────────────────────────────
class Signal(BaseModel):
    signal_type: SignalType
    market: MarketInfo
    side: Side
    outcome: Outcome
    confidence: float  # 0.0–1.0
    edge: float  # estimated edge in price terms
    timestamp: float = Field(default_factory=time.time)
    meta: dict = Field(default_factory=dict)


# ── Position tracking ──────────────────────────────────────────────
class Position(BaseModel):
    market: MarketInfo
    outcome: Outcome
    side: Side
    size: float
    entry_price: float
    entry_ts: float = Field(default_factory=time.time)
    exit_price: float | None = None
    exit_ts: float | None = None
    pnl: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.exit_ts is None


# ── Order result ────────────────────────────────────────────────────
class OrderResult(BaseModel):
    success: bool
    order_id: str = ""
    filled_size: float = 0.0
    avg_price: float = 0.0
    error: str = ""
    timestamp: float = Field(default_factory=time.time)
