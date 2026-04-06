"""Polymarket CLOB WebSocket client for real-time order book data.

Subscribes to book updates for active micro-markets and maintains
a local snapshot of best bid/ask for each token pair.

The Polymarket CLOB WS API sends three event types:
  - Initial snapshot: JSON array of book objects (event_type="book")
  - Book refresh: dict with asset_id, bids, asks (event_type="book")
  - Price changes: dict with price_changes array containing level updates
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
        self._ws = None
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
        self, ws, markets: list[MarketInfo]
    ) -> None:
        """Send a single subscription for all markets' token pairs.

        The CLOB WS API expects ``assets_ids`` (not ``markets``) in the
        subscription payload.  Sending ``markets`` causes a 1006 disconnect.
        """
        token_ids: list[str] = []
        for market in markets:
            if market.condition_id in self._subscribed:
                continue
            token_ids.extend([market.token_id_yes, market.token_id_no])
            self._subscribed.add(market.condition_id)

        if not token_ids:
            return

        sub_msg = {
            "type": "subscribe",
            "channel": "book",
            "assets_ids": token_ids,
        }
        await ws.send(json.dumps(sub_msg))
        logger.info("Subscribed to CLOB book", token_count=len(token_ids))

    async def subscribe_batch(
        self, markets: list[MarketInfo], chunk_size: int = 50
    ) -> None:
        """Subscribe to many markets in chunks to avoid overwhelming the WS.

        Used by the broad scanner which may add hundreds of markets at once.
        """
        ws_open = False
        if self._ws:
            try:
                ws_open = self._ws.state.name == "OPEN"
            except AttributeError:
                try:
                    ws_open = self._ws.open
                except AttributeError:
                    ws_open = False
        if not ws_open:
            return

        new_markets = [m for m in markets if m.condition_id not in self._subscribed]
        for i in range(0, len(new_markets), chunk_size):
            chunk = new_markets[i : i + chunk_size]
            await self._subscribe_markets(self._ws, chunk)
            if i + chunk_size < len(new_markets):
                await asyncio.sleep(0.1)  # Brief pause between chunks

        if new_markets:
            logger.info(
                "Batch subscribed to broad markets",
                count=len(new_markets),
                total_books=len(self.books),
            )

    async def update_subscriptions(self, markets: list[MarketInfo]) -> None:
        """Dynamically add subscriptions for newly discovered markets."""
        if self._ws and self.is_connected:
            await self._subscribe_markets(self._ws, markets)

    async def _consume(self, ws) -> None:
        """Process incoming book update messages."""
        async for raw_msg in ws:
            if not self._running:
                break

            try:
                msg = json.loads(raw_msg)
                if isinstance(msg, list):
                    # Initial snapshot: array of book objects
                    for book_obj in msg:
                        self._handle_book_snapshot(book_obj)
                elif isinstance(msg, dict):
                    self._handle_message(msg)
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.debug("Polymarket WS parse error", error=str(exc))

    def _handle_book_snapshot(self, book_obj: dict) -> None:
        """Parse a single book snapshot into local state."""
        asset_id = book_obj.get("asset_id", "")
        if not asset_id:
            return

        bids = [
            BookLevel(price=float(b["price"]), size=float(b["size"]))
            for b in book_obj.get("bids", [])
        ]
        asks = [
            BookLevel(price=float(a["price"]), size=float(a["size"]))
            for a in book_obj.get("asks", [])
        ]

        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        self.books[asset_id] = OrderBookSnapshot(
            market_id=asset_id,
            bids=bids,
            asks=asks,
        )

    def _handle_message(self, msg: dict) -> None:
        """Route dict messages by event_type."""
        event_type = msg.get("event_type", "")

        if event_type == "book":
            # Full book refresh for a single asset
            self._handle_book_snapshot(msg)

        elif "price_changes" in msg:
            # Incremental level updates
            for change in msg["price_changes"]:
                asset_id = change.get("asset_id", "")
                if not asset_id:
                    continue

                price = float(change["price"])
                size = float(change["size"])
                side = change.get("side", "").upper()

                book = self.books.get(asset_id)
                if not book:
                    continue

                levels = book.bids if side == "BUY" else book.asks

                if size == 0:
                    if side == "BUY":
                        book.bids = [b for b in book.bids if b.price != price]
                    else:
                        book.asks = [a for a in book.asks if a.price != price]
                else:
                    updated = False
                    for lvl in levels:
                        if lvl.price == price:
                            lvl.size = size
                            updated = True
                            break
                    if not updated:
                        levels.append(BookLevel(price=price, size=size))

                    if side == "BUY":
                        book.bids.sort(key=lambda x: x.price, reverse=True)
                    else:
                        book.asks.sort(key=lambda x: x.price)

        # Ignore last_trade_price and other event types

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

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        try:
            from websockets.protocol import State
            return self._ws.state == State.OPEN
        except (AttributeError, ImportError):
            # websockets v16+: state is an int (1 = OPEN)
            state = getattr(self._ws, "state", None)
            if isinstance(state, int):
                return state == 1
            return getattr(self._ws, "open", False)
