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
        self._ws: websockets.WebSocketClientProtocol | None = None
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
                    "market": token_id,
                }
                await ws.send(json.dumps(sub_msg))
                logger.debug("Subscribed to CLOB book", token_id=token_id)

            self._subscribed.add(market.condition_id)

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
        """Stop polling."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        self._subscribed.clear()
        logger.info("Polymarket book poller stopped")
