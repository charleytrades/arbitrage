"""Async Binance WebSocket client for real-time trade/kline data.

Subscribes to multiple symbols simultaneously via a single multiplexed
connection, feeding BinanceTick objects into an asyncio.Queue for
downstream strategy consumption.
"""

from __future__ import annotations

import asyncio
import json
import time

import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import WS_PING_INTERVAL_SEC, WS_RECONNECT_DELAY_SEC
from polymarket_micro_arb.models import BinanceTick
from polymarket_micro_arb.utils.logger import logger


class BinanceWSClient:
    """Multi-symbol Binance WebSocket consumer.

    Subscribes to aggTrade streams for low-latency price updates and
    kline_1m streams for OHLCV context.
    """

    def __init__(self, tick_queue: asyncio.Queue[BinanceTick]) -> None:
        self.tick_queue = tick_queue
        self._running = False
        self._ws: websockets.WebSocketClientProtocol | None = None

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
                logger.info(
                    "Binance WS reconnecting",
                    delay_sec=WS_RECONNECT_DELAY_SEC,
                )
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Read messages and enqueue parsed ticks."""
        async for raw_msg in ws:
            if not self._running:
                break

            try:
                msg = json.loads(raw_msg)
                tick = self._parse_message(msg)
                if tick:
                    await self.tick_queue.put(tick)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.debug("Binance parse error", error=str(exc))

    @staticmethod
    def _parse_message(msg: dict) -> BinanceTick | None:
        """Extract a BinanceTick from aggTrade or kline messages."""
        # aggTrade event
        if msg.get("e") == "aggTrade":
            return BinanceTick(
                symbol=msg["s"],
                price=float(msg["p"]),
                timestamp_ms=int(msg["T"]),
                volume=float(msg["q"]),
            )

        # Kline event – use close price
        if msg.get("e") == "kline":
            k = msg["k"]
            return BinanceTick(
                symbol=k["s"],
                price=float(k["c"]),
                timestamp_ms=int(k["T"]),
                volume=float(k["v"]),
            )

        return None

    async def stop(self) -> None:
        """Gracefully shut down the WebSocket."""
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("Binance WS stopped")

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.open
