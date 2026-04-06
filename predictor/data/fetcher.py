"""Binance REST API kline fetcher.

Pulls historical OHLCV candles for BTC/ETH/SOL from Binance's
public API (no API key needed). Supports incremental updates —
only fetches candles newer than what's already on disk.

Rate limit: Binance allows 1200 req/min on public endpoints.
Each kline request returns up to 1000 candles (weight=1).
90 days of 1m data = ~130K candles = ~130 requests per symbol.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import httpx
import pandas as pd

from predictor.config import settings
from predictor.constants import INTERVAL_MINUTES, KLINE_COLUMNS
from predictor.utils.logger import logger


class BinanceKlineFetcher:
    """Fetches historical klines from Binance REST API."""

    def __init__(self) -> None:
        self.base_url = settings.binance_rest_base
        self._semaphore = asyncio.Semaphore(5)  # Max concurrent requests
        self._request_delay = 0.1  # 100ms between requests

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> pd.DataFrame:
        """Fetch klines from Binance, paginating through 1000-candle chunks."""
        all_rows: list[list] = []
        current_start = start_ms

        async with httpx.AsyncClient(timeout=30) as client:
            while current_start < end_ms:
                async with self._semaphore:
                    try:
                        resp = await client.get(
                            f"{self.base_url}/api/v3/klines",
                            params={
                                "symbol": symbol,
                                "interval": interval,
                                "startTime": current_start,
                                "endTime": end_ms,
                                "limit": 1000,
                            },
                        )
                        resp.raise_for_status()
                        data = resp.json()
                    except httpx.HTTPError as exc:
                        logger.error(
                            "Binance kline fetch failed",
                            symbol=symbol,
                            interval=interval,
                            error=str(exc),
                        )
                        break

                if not data:
                    break

                all_rows.extend(data)

                # Move start past the last candle's close_time
                last_close_time = int(data[-1][6])
                current_start = last_close_time + 1

                if len(data) < 1000:
                    break  # No more data

                await asyncio.sleep(self._request_delay)

        if not all_rows:
            return pd.DataFrame(columns=KLINE_COLUMNS)

        df = pd.DataFrame(all_rows, columns=KLINE_COLUMNS)

        # Convert types
        for col in ["open", "high", "low", "close", "volume", "quote_volume",
                     "taker_buy_volume", "taker_buy_quote_volume"]:
            df[col] = df[col].astype(float)
        for col in ["open_time", "close_time", "trades"]:
            df[col] = df[col].astype(int)
        df.drop(columns=["ignore"], inplace=True)

        # Add datetime index
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.set_index("timestamp").sort_index()

        # Remove duplicates
        df = df[~df.index.duplicated(keep="last")]

        return df

    async def fetch_symbol(
        self,
        symbol: str,
        interval: str,
        days: int | None = None,
    ) -> pd.DataFrame:
        """Fetch klines for a symbol/interval, with incremental update support."""
        days = days or settings.fetch_days
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (days * 24 * 60 * 60 * 1000)

        # Check existing data for incremental update
        existing = load_klines(symbol, interval)
        if existing is not None and len(existing) > 0:
            last_ts = int(existing.index[-1].timestamp() * 1000)
            if last_ts > start_ms:
                start_ms = last_ts + 1
                logger.info(
                    "Incremental fetch",
                    symbol=symbol,
                    interval=interval,
                    existing_rows=len(existing),
                    fetch_from=pd.Timestamp(start_ms, unit="ms", tz="UTC"),
                )

        new_data = await self.fetch_klines(symbol, interval, start_ms, end_ms)

        if existing is not None and len(existing) > 0 and len(new_data) > 0:
            combined = pd.concat([existing, new_data])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
        elif existing is not None and len(existing) > 0:
            combined = existing
        else:
            combined = new_data

        # Trim to requested window
        cutoff = pd.Timestamp(
            end_ms - (days * 24 * 60 * 60 * 1000), unit="ms", tz="UTC"
        )
        combined = combined[combined.index >= cutoff]

        return combined

    async def fetch_all(self, days: int | None = None) -> dict[str, dict[str, pd.DataFrame]]:
        """Fetch all symbols and intervals. Returns {symbol: {interval: df}}."""
        result: dict[str, dict[str, pd.DataFrame]] = {}
        days = days or settings.fetch_days

        for symbol in settings.symbol_list:
            result[symbol] = {}
            for interval in settings.timeframe_list:
                logger.info("Fetching", symbol=symbol, interval=interval, days=days)
                df = await self.fetch_symbol(symbol, interval, days)
                save_klines(df, symbol, interval)
                result[symbol][interval] = df
                logger.info(
                    "Fetched",
                    symbol=symbol,
                    interval=interval,
                    rows=len(df),
                    start=str(df.index[0]) if len(df) > 0 else "empty",
                    end=str(df.index[-1]) if len(df) > 0 else "empty",
                )

        return result


def _kline_path(symbol: str, interval: str) -> Path:
    """Path to the parquet file for a symbol/interval pair."""
    path = Path(settings.data_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{symbol}_{interval}.parquet"


def save_klines(df: pd.DataFrame, symbol: str, interval: str) -> None:
    """Save klines to parquet."""
    if df.empty:
        return
    path = _kline_path(symbol, interval)
    df.to_parquet(path, engine="pyarrow")
    logger.info("Saved", path=str(path), rows=len(df))


def load_klines(symbol: str, interval: str) -> pd.DataFrame | None:
    """Load klines from parquet. Returns None if file doesn't exist."""
    path = _kline_path(symbol, interval)
    if not path.exists():
        return None
    df = pd.read_parquet(path, engine="pyarrow")
    return df


def list_available() -> dict[str, list[str]]:
    """List available data files. Returns {symbol: [intervals]}."""
    data_dir = Path(settings.data_dir)
    if not data_dir.exists():
        return {}
    result: dict[str, list[str]] = {}
    for f in sorted(data_dir.glob("*.parquet")):
        parts = f.stem.split("_")
        if len(parts) == 2:
            symbol, interval = parts
            result.setdefault(symbol, []).append(interval)
    return result
