"""Async Binance + Bybit WebSocket client for real-time trade data.

Subscribes to multiple symbols simultaneously via multiplexed connections
on both Binance and Bybit for multi-venue price confirmation.
Feeds BinanceTick objects into an asyncio.Queue for downstream consumption.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque

import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import WS_PING_INTERVAL_SEC, WS_RECONNECT_DELAY_SEC
from polymarket_micro_arb.models import BinanceTick
from polymarket_micro_arb.utils.logger import logger


# ── Volume tracker for confirmation ─────────────────────────────────
class VolumeTracker:
    """Tracks rolling volume per symbol to confirm momentum signals."""

    def __init__(self, window_sec: float = 60.0) -> None:
        self._window_sec = window_sec
        # symbol -> deque of (timestamp_sec, volume)
        self._data: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=2000)
        )
        # symbol -> baseline volume per second (updated periodically)
        self._baseline: dict[str, float] = defaultdict(float)

    def record(self, symbol: str, ts: float, volume: float) -> None:
        self._data[symbol].append((ts, volume))

    def get_recent_volume(self, symbol: str, lookback_sec: float = 30.0) -> float:
        """Sum of volume in the last `lookback_sec` seconds."""
        cutoff = time.time() - lookback_sec
        return sum(v for t, v in self._data[symbol] if t >= cutoff)

    def get_baseline_volume(self, symbol: str) -> float:
        """Average volume per `lookback` period over the full window."""
        now = time.time()
        cutoff = now - self._window_sec
        entries = [(t, v) for t, v in self._data[symbol] if t >= cutoff]
        if not entries or self._window_sec <= 0:
            return 0.0
        total = sum(v for _, v in entries)
        # Normalize to a 30s baseline
        return total * 30.0 / self._window_sec

    def is_volume_confirmed(
        self, symbol: str, multiplier: float = 1.5, lookback_sec: float = 30.0
    ) -> bool:
        """True if recent volume exceeds baseline * multiplier."""
        recent = self.get_recent_volume(symbol, lookback_sec)
        baseline = self.get_baseline_volume(symbol)
        if baseline <= 0:
            return recent > 0  # No baseline yet, any volume counts
        return recent >= baseline * multiplier


class BinanceWSClient:
    """Multi-symbol Binance WebSocket consumer with volume tracking."""

    def __init__(self, tick_queue: asyncio.Queue[BinanceTick]) -> None:
        self.tick_queue = tick_queue
        self._running = False
        self._ws: websockets.WebSocketClientProtocol | None = None
        self.volume_tracker = VolumeTracker(window_sec=120.0)

    def _build_url(self) -> str:
        """Build the combined stream URL for all configured symbols."""
        streams = []
        for sym in settings.symbol_list:
            lower = sym.lower()
            streams.append(f"{lower}@aggTrade")
            streams.append(f"{lower}@kline_1m")
        combined = "/".join(streams)
        base = settings.binance_ws_url.rstrip("/")
        return f"{base}/{combined}"

    async def start(self) -> None:
        """Connect and begin consuming. Auto-reconnects on failure."""
        self._running = True
        url = self._build_url()

        while self._running:
            try:
                logger.info("Binance WS connecting", url=url)
                async with websockets.connect(
                    url,
                    ping_interval=WS_PING_INTERVAL_SEC,
                    ping_timeout=WS_PING_INTERVAL_SEC * 2,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    logger.info("Binance WS connected")
                    await self._consume(ws)
            except ConnectionClosed as exc:
                logger.warning("Binance WS closed", code=exc.code, reason=exc.reason)
            except Exception as exc:
                logger.error("Binance WS error", error=str(exc))

            if self._running:
                logger.info("Binance WS reconnecting", delay_sec=WS_RECONNECT_DELAY_SEC)
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw_msg in ws:
            if not self._running:
                break
            try:
                msg = json.loads(raw_msg)
                tick = self._parse_message(msg)
                if tick:
                    # Track volume
                    self.volume_tracker.record(
                        tick.symbol, tick.timestamp_ms / 1000.0, tick.volume
                    )
                    await self.tick_queue.put(tick)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.debug("Binance parse error", error=str(exc))

    @staticmethod
    def _parse_message(msg: dict) -> BinanceTick | None:
        if msg.get("e") == "aggTrade":
            return BinanceTick(
                symbol=msg["s"],
                price=float(msg["p"]),
                timestamp_ms=int(msg["T"]),
                volume=float(msg["q"]),
                source="binance",
            )
        if msg.get("e") == "kline":
            k = msg["k"]
            return BinanceTick(
                symbol=k["s"],
                price=float(k["c"]),
                timestamp_ms=int(k["T"]),
                volume=float(k["v"]),
                source="binance",
            )
        return None

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("Binance WS stopped")

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.open


class BybitWSClient:
    """Bybit WebSocket client for multi-venue price confirmation.

    Subscribes to Bybit spot trade streams for the same symbols.
    Ticks are tagged with source="bybit" and fed into the same queue.
    """

    def __init__(self, tick_queue: asyncio.Queue[BinanceTick]) -> None:
        self.tick_queue = tick_queue
        self._running = False
        self._ws: websockets.WebSocketClientProtocol | None = None
        # symbol -> latest price from Bybit
        self.latest_prices: dict[str, float] = {}

    async def start(self) -> None:
        self._running = True
        url = settings.bybit_ws_url

        while self._running:
            try:
                logger.info("Bybit WS connecting", url=url)
                async with websockets.connect(
                    url,
                    ping_interval=WS_PING_INTERVAL_SEC,
                    ping_timeout=WS_PING_INTERVAL_SEC * 2,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    logger.info("Bybit WS connected")
                    # Subscribe to trade streams
                    sub_msg = {
                        "op": "subscribe",
                        "args": [f"publicTrade.{sym}" for sym in settings.symbol_list],
                    }
                    await ws.send(json.dumps(sub_msg))
                    await self._consume(ws)
            except ConnectionClosed as exc:
                logger.warning("Bybit WS closed", code=exc.code, reason=exc.reason)
            except Exception as exc:
                logger.error("Bybit WS error", error=str(exc))

            if self._running:
                logger.info("Bybit WS reconnecting", delay_sec=WS_RECONNECT_DELAY_SEC)
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw_msg in ws:
            if not self._running:
                break
            try:
                msg = json.loads(raw_msg)
                # Bybit trade format: {"topic":"publicTrade.BTCUSDT","data":[...]}
                if "topic" not in msg or "data" not in msg:
                    continue

                topic = msg["topic"]
                # Extract symbol from "publicTrade.BTCUSDT"
                symbol = topic.split(".")[-1] if "." in topic else ""

                for trade in msg["data"]:
                    price = float(trade.get("p", 0))
                    volume = float(trade.get("v", 0))
                    ts_ms = int(trade.get("T", time.time() * 1000))

                    if price > 0:
                        self.latest_prices[symbol] = price
                        tick = BinanceTick(
                            symbol=symbol,
                            price=price,
                            timestamp_ms=ts_ms,
                            volume=volume,
                            source="bybit",
                        )
                        await self.tick_queue.put(tick)
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.debug("Bybit parse error", error=str(exc))

    def get_price(self, symbol: str) -> float | None:
        """Get latest Bybit price for a symbol."""
        return self.latest_prices.get(symbol)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("Bybit WS stopped")
