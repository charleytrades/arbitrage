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

    # ── Bybit (multi-venue confirmation) ────────────────────────────
    bybit_ws_url: str = Field(
        "wss://stream.bybit.com/v5/public/spot", alias="BYBIT_WS_URL"
    )

    # ── Telegram ────────────────────────────────────────────────────
    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field("", alias="TELEGRAM_CHAT_ID")

    # ── Risk ────────────────────────────────────────────────────────
    max_bankroll_percent: float = Field(4.0, alias="MAX_BANKROLL_PERCENT")
    max_daily_loss_percent: float = Field(8.0, alias="MAX_DAILY_LOSS_PERCENT")
    max_consecutive_losses: int = Field(3, alias="MAX_CONSECUTIVE_LOSSES")
    consecutive_loss_cooldown_sec: int = Field(
        1800, alias="CONSECUTIVE_LOSS_COOLDOWN_SEC"
    )
    kelly_fraction: float = Field(0.25, alias="KELLY_FRACTION")
    max_concurrent_buckets: int = Field(8, alias="MAX_CONCURRENT_BUCKETS")

    # ── Trading mode ────────────────────────────────────────────────
    trading_mode: Literal["backtest", "paper_trade", "live"] = Field(
        "paper_trade", alias="TRADING_MODE"
    )

    # ── Symbols ─────────────────────────────────────────────────────
    symbols: str = Field("BTCUSDT,ETHUSDT,SOLUSDT", alias="SYMBOLS")

    # ── Strategy parameters ─────────────────────────────────────────
    momentum_threshold: float = Field(0.0035, alias="MOMENTUM_THRESHOLD")
    momentum_window_start_sec: int = Field(15, alias="MOMENTUM_WINDOW_START_SEC")
    momentum_window_end_sec: int = Field(45, alias="MOMENTUM_WINDOW_END_SEC")
    latency_edge_ms: int = Field(500, alias="LATENCY_EDGE_MS")
    cross_outcome_threshold: float = Field(0.99, alias="CROSS_OUTCOME_THRESHOLD")
    min_spread_profit: float = Field(0.005, alias="MIN_SPREAD_PROFIT")
    volume_confirm_multiplier: float = Field(1.5, alias="VOLUME_CONFIRM_MULTIPLIER")

    # ── Order management ────────────────────────────────────────────
    min_order_size: float = Field(5.0, alias="MIN_ORDER_SIZE")
    max_order_size: float = Field(100.0, alias="MAX_ORDER_SIZE")
    order_ttl_sec: int = Field(60, alias="ORDER_TTL_SEC")

    # ── Drift BET (cross-platform arb) ────────────────────────────
    drift_enabled: bool = Field(False, alias="DRIFT_ENABLED")
    drift_mainnet_rpc: str = Field(
        "https://api.mainnet-beta.solana.com", alias="DRIFT_MAINNET_RPC"
    )
    drift_bet_api_url: str = Field(
        "https://data.api.drift.trade", alias="DRIFT_BET_API_URL"
    )
    drift_gateway_url: str = Field(
        "http://localhost:8080", alias="DRIFT_GATEWAY_URL"
    )
    solana_private_key: str = Field("", alias="SOLANA_PRIVATE_KEY")
    cross_platform_min_spread: float = Field(
        0.06, alias="CROSS_PLATFORM_MIN_SPREAD"
    )
    drift_poll_interval_sec: int = Field(5, alias="DRIFT_POLL_INTERVAL_SEC")

    # ── Market discovery ────────────────────────────────────────────
    market_refresh_interval_sec: int = Field(30, alias="MARKET_REFRESH_INTERVAL_SEC")

    # ── Broad market scan (cross-outcome arb on ALL binary markets) ─
    broad_scan_enabled: bool = Field(True, alias="BROAD_SCAN_ENABLED")
    broad_scan_refresh_sec: int = Field(300, alias="BROAD_SCAN_REFRESH_SEC")
    broad_scan_max_markets: int = Field(500, alias="BROAD_SCAN_MAX_MARKETS")

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols.split(",")]


# Singleton – import this everywhere
settings = Settings()  # type: ignore[call-arg]
