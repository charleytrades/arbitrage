"""Polymarket CLOB WebSocket client for real-time order book data.

Subscribes to book updates for active micro-markets and maintains
a local snapshot of best bid/ask for each token pair.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict

import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import WS_PING_INTERVAL_SEC, WS_RECONNECT_DELAY_SEC
from polymarket_micro_arb.models import BookLevel, MarketInfo, OrderBookSnapshot
from polymarket_micro_arb.utils.logger import logger


class PolymarketWSClient:
    """Subscribes to Polymarket CLOB book updates for tracked markets."""

    def __init__(self) -> None:
        self._running = False
        self._ws: websockets.WebSocketClientProtocol | None = None
        # token_id -> latest snapshot
        self.books: dict[str, OrderBookSnapshot] = {}
        # Condition IDs we're currently subscribed to
        self._subscribed: set[str] = set()
        self._lock = asyncio.Lock()

    async def start(self, markets: list[MarketInfo]) -> None:
        """Connect and subscribe to the given markets."""
        self._running = True
        url = settings.polymarket_ws_url

        while self._running:
            try:
                logger.info("Polymarket WS connecting", url=url)
                async with websockets.connect(
                    url,
                    ping_interval=WS_PING_INTERVAL_SEC,
                    ping_timeout=WS_PING_INTERVAL_SEC * 2,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    logger.info("Polymarket WS connected")

                    # Subscribe to all current markets
                    await self._subscribe_markets(ws, markets)
                    await self._consume(ws)

            except ConnectionClosed as exc:
                logger.warning("Polymarket WS closed", code=exc.code, reason=exc.reason)
            except Exception as exc:
                logger.error("Polymarket WS error", error=str(exc))

            if self._running:
                logger.info(
                    "Polymarket WS reconnecting",
                    delay_sec=WS_RECONNECT_DELAY_SEC,
                )
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def _subscribe_markets(
        self, ws: websockets.WebSocketClientProtocol, markets: list[MarketInfo]
    ) -> None:
        """Send subscription messages for each market's token pairs."""
        for market in markets:
            if market.condition_id in self._subscribed:
                continue

            # Subscribe to both YES and NO token books
            for token_id in (market.token_id_yes, market.token_id_no):
                sub_msg = {
                    "type": "subscribe",
                    "channel": "book",
                    "markets": [token_id],
                }
                await ws.send(json.dumps(sub_msg))
                logger.debug("Subscribed to CLOB book", token_id=token_id)

            self._subscribed.add(market.condition_id)

    async def update_subscriptions(self, markets: list[MarketInfo]) -> None:
        """Dynamically add subscriptions for newly discovered markets."""
        if self._ws and self._ws.open:
            await self._subscribe_markets(self._ws, markets)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Process incoming book update messages."""
        async for raw_msg in ws:
            if not self._running:
                break

            try:
                msg = json.loads(raw_msg)
                self._handle_message(msg)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.debug("Polymarket WS parse error", error=str(exc))

    def _handle_message(self, msg: dict) -> None:
        """Parse a book snapshot or delta and update local state."""
        # The CLOB WS sends different message types
        msg_type = msg.get("type", "")

        if msg_type in ("book", "book_snapshot"):
            market_id = msg.get("market", msg.get("asset_id", ""))
            if not market_id:
                return

            bids = [
                BookLevel(price=float(b["price"]), size=float(b["size"]))
                for b in msg.get("bids", [])
            ]
            asks = [
                BookLevel(price=float(a["price"]), size=float(a["size"]))
                for a in msg.get("asks", [])
            ]

            # Sort: bids descending, asks ascending
            bids.sort(key=lambda x: x.price, reverse=True)
            asks.sort(key=lambda x: x.price)

            self.books[market_id] = OrderBookSnapshot(
                market_id=market_id,
                bids=bids,
                asks=asks,
            )

        elif msg_type == "book_delta":
            # Incremental update – apply to existing snapshot
            market_id = msg.get("market", msg.get("asset_id", ""))
            if market_id not in self.books:
                return

            book = self.books[market_id]

            # Apply bid changes
            for change in msg.get("bids", []):
                price, size = float(change["price"]), float(change["size"])
                if size == 0:
                    book.bids = [b for b in book.bids if b.price != price]
                else:
                    # Update or insert
                    updated = False
                    for b in book.bids:
                        if b.price == price:
                            b.size = size
                            updated = True
                            break
                    if not updated:
                        book.bids.append(BookLevel(price=price, size=size))
                    book.bids.sort(key=lambda x: x.price, reverse=True)

            # Apply ask changes
            for change in msg.get("asks", []):
                price, size = float(change["price"]), float(change["size"])
                if size == 0:
                    book.asks = [a for a in book.asks if a.price != price]
                else:
                    updated = False
                    for a in book.asks:
                        if a.price == price:
                            a.size = size
                            updated = True
                            break
                    if not updated:
                        book.asks.append(BookLevel(price=price, size=size))
                    book.asks.sort(key=lambda x: x.price)

    def get_book(self, token_id: str) -> OrderBookSnapshot | None:
        """Get the latest order book for a token."""
        return self.books.get(token_id)

    def get_best_prices(
        self, token_id: str
    ) -> tuple[float, float]:
        """Return (best_bid, best_ask) for a token. Defaults to (0, 1)."""
        book = self.books.get(token_id)
        if not book:
            return 0.0, 1.0
        return book.best_bid, book.best_ask

    async def stop(self) -> None:
        """Gracefully disconnect."""
        self._running = False
        if self._ws:
            await self._ws.close()
        self._subscribed.clear()
        logger.info("Polymarket WS stopped")
