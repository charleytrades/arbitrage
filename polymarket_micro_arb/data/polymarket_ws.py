"""Polymarket CLOB order book client.

Polls the CLOB REST API for book snapshots and maintains
a local snapshot of best bid/ask for each token pair.
Falls back to WebSocket if available.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import WS_PING_INTERVAL_SEC, WS_RECONNECT_DELAY_SEC
from polymarket_micro_arb.models import BookLevel, MarketInfo, OrderBookSnapshot
from polymarket_micro_arb.utils.logger import logger

CLOB_BOOK_URL = "https://clob.polymarket.com/book"
BOOK_POLL_INTERVAL_SEC = 2


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
        """Start polling CLOB REST API for book data."""
        self._running = True
        self._markets = markets
        self._session: aiohttp.ClientSession | None = None

        # Collect all token IDs to poll
        self._tracked_tokens: list[str] = []
        for m in markets:
            self._tracked_tokens.append(m.token_id_yes)
            self._tracked_tokens.append(m.token_id_no)
            self._subscribed.add(m.condition_id)

        logger.info(
            "Polymarket book poller starting",
            tokens=len(self._tracked_tokens),
            interval=f"{BOOK_POLL_INTERVAL_SEC}s",
        )

        try:
            self._session = aiohttp.ClientSession()
            while self._running:
                await self._poll_all_books()
                await asyncio.sleep(BOOK_POLL_INTERVAL_SEC)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def _poll_all_books(self) -> None:
        """Fetch book snapshots for all tracked tokens via REST API."""
        if not self._session:
            return
        for token_id in self._tracked_tokens:
            if not self._running:
                break
            try:
                async with self._session.get(
                    CLOB_BOOK_URL,
                    params={"token_id": token_id},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    bids = [
                        BookLevel(price=float(b["price"]), size=float(b["size"]))
                        for b in data.get("bids", [])
                    ]
                    asks = [
                        BookLevel(price=float(a["price"]), size=float(a["size"]))
                        for a in data.get("asks", [])
                    ]
                    bids.sort(key=lambda x: x.price, reverse=True)
                    asks.sort(key=lambda x: x.price)
                    self.books[token_id] = OrderBookSnapshot(
                        market_id=token_id,
                        bids=bids,
                        asks=asks,
                    )
            except (aiohttp.ClientError, asyncio.TimeoutError, Exception):
                pass  # Skip this token, try next

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

            # Subscribe to both YES and NO token books
            for token_id in (market.token_id_yes, market.token_id_no):
                sub_msg = {
                    "type": "subscribe",
                    "channel": "book",
                    "market": token_id,
                }
                await ws.send(json.dumps(sub_msg))
                logger.debug("Subscribed to CLOB book", token_id=token_id)

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
        if not self.is_connected:
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

    async def subscribe_batch(
        self, markets: list[MarketInfo], chunk_size: int = 50
    ) -> None:
        """Add markets to the polling list."""
        new = [m for m in markets if m.condition_id not in self._subscribed]
        for m in new:
            self._tracked_tokens.append(m.token_id_yes)
            self._tracked_tokens.append(m.token_id_no)
            self._subscribed.add(m.condition_id)
        if new:
            logger.info("Added broad markets to book poller", count=len(new))

    async def update_subscriptions(self, markets: list[MarketInfo]) -> None:
        """Add newly discovered markets to the polling list."""
        for m in markets:
            if m.condition_id not in self._subscribed:
                self._tracked_tokens.append(m.token_id_yes)
                self._tracked_tokens.append(m.token_id_no)
                self._subscribed.add(m.condition_id)

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
        """Stop polling."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        self._subscribed.clear()
        logger.info("Polymarket book poller stopped")
