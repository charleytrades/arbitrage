"""Centralised configuration loaded from environment / .env file.

All settings are validated via Pydantic so the bot fails fast on
misconfiguration rather than blowing up mid-trade.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# Load .env from project root (two levels up from this file)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


class Settings(BaseSettings):
    """Application settings – sourced from environment variables."""

    # ── Polymarket / Polygon ────────────────────────────────────────
    private_key: str = Field(..., alias="PRIVATE_KEY")
    polymarket_host: str = Field(
        "https://clob.polymarket.com", alias="POLYMARKET_HOST"
    )
    polymarket_ws_url: str = Field(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        alias="POLYMARKET_WS_URL",
    )
    chain_id: int = Field(137, alias="CHAIN_ID")

    # ── Gamma API ───────────────────────────────────────────────────
    gamma_api_url: str = Field(
        "https://gamma-api.polymarket.com", alias="GAMMA_API_URL"
    )

    # ── Binance ─────────────────────────────────────────────────────
    binance_ws_url: str = Field(
        "wss://stream.binance.com:9443/ws", alias="BINANCE_WS_URL"
    )

    # ── Telegram ────────────────────────────────────────────────────
    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field("", alias="TELEGRAM_CHAT_ID")

    # ── Risk ────────────────────────────────────────────────────────
    max_bankroll_percent: float = Field(2.0, alias="MAX_BANKROLL_PERCENT")
    max_daily_loss_percent: float = Field(5.0, alias="MAX_DAILY_LOSS_PERCENT")
    max_consecutive_losses: int = Field(5, alias="MAX_CONSECUTIVE_LOSSES")
    kelly_fraction: float = Field(0.25, alias="KELLY_FRACTION")

    # ── Trading mode ────────────────────────────────────────────────
    trading_mode: Literal["backtest", "paper_trade", "live"] = Field(
        "paper_trade", alias="TRADING_MODE"
    )

    # ── Symbols ─────────────────────────────────────────────────────
    symbols: str = Field("BTCUSDT,ETHUSDT,SOLUSDT", alias="SYMBOLS")

    # ── Strategy parameters ─────────────────────────────────────────
    momentum_threshold: float = Field(0.0015, alias="MOMENTUM_THRESHOLD")
    latency_edge_ms: int = Field(500, alias="LATENCY_EDGE_MS")
    cross_outcome_threshold: float = Field(0.99, alias="CROSS_OUTCOME_THRESHOLD")
    min_spread_profit: float = Field(0.005, alias="MIN_SPREAD_PROFIT")

    # ── Order sizing ────────────────────────────────────────────────
    min_order_size: float = Field(5.0, alias="MIN_ORDER_SIZE")
    max_order_size: float = Field(100.0, alias="MAX_ORDER_SIZE")

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols.split(",")]


# Singleton – import this everywhere
settings = Settings()  # type: ignore[call-arg]
