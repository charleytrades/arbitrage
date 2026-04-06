"""Drift BET prediction market client.

Polls Drift BET's API for active crypto price prediction markets
and tracks their prices. Used for cross-platform arbitrage against
Polymarket.

Drift BET uses Pyth oracle feeds for settlement — the same oracle
lag dynamic we exploit on Polymarket applies here too, but the
mispricing may differ between platforms, creating a spread.

Architecture:
  - REST polling (every 5s) for market discovery and price updates
  - No WebSocket needed — Drift BET markets update less frequently
    than Polymarket CLOB, so polling is sufficient
  - Matches Drift markets to Polymarket markets by symbol + timeframe
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict

import aiohttp

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.models import DriftMarket, Platform
from polymarket_micro_arb.utils.logger import logger

# Map Drift symbol naming to our internal format
DRIFT_SYMBOL_MAP: dict[str, str] = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
}

# Keywords that indicate short-term price direction markets
BUCKET_KEYWORDS = {
    "5 min": "5m",
    "5-min": "5m",
    "5m": "5m",
    "15 min": "15m",
    "15-min": "15m",
    "15m": "15m",
}


class DriftBetClient:
    """Async client for Drift BET prediction market data.

    Discovers and tracks crypto price prediction markets on Drift
    that overlap with Polymarket's 5m/15m micro-buckets.
    """

    def __init__(self) -> None:
        self.api_url = settings.drift_bet_api_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._running = False

        # market_index -> DriftMarket
        self.markets: dict[int, DriftMarket] = {}
        # symbol -> list of active DriftMarkets
        self.markets_by_symbol: dict[str, list[DriftMarket]] = defaultdict(list)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def start(self) -> None:
        """Start the polling loop for Drift BET markets."""
        self._running = True
        logger.info("Drift BET client starting", api_url=self.api_url)

        while self._running:
            try:
                await self._poll_markets()
                await asyncio.sleep(settings.drift_poll_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Drift BET poll error", error=str(exc))
                await asyncio.sleep(10)

    async def stop(self) -> None:
        self._running = False
        await self.close()
        logger.info("Drift BET client stopped")

    async def _poll_markets(self) -> None:
        """Fetch active markets from Drift BET API and update local state."""
        session = await self._get_session()

        # Try multiple API endpoint patterns that Drift BET may use
        markets_data = await self._fetch_markets(session)
        if not markets_data:
            return

        now = time.time()
        new_count = 0

        for raw in markets_data:
            market = self._parse_market(raw)
            if market and market.active and market.seconds_remaining > 10:
                if market.market_index not in self.markets:
                    new_count += 1
                self.markets[market.market_index] = market

        # Rebuild symbol index
        self.markets_by_symbol.clear()
        for m in self.markets.values():
            if m.active and m.seconds_remaining > 0:
                self.markets_by_symbol[m.symbol].append(m)

        # Clean up expired
        expired = [
            idx for idx, m in self.markets.items()
            if m.end_ts < now
        ]
        for idx in expired:
            self.markets[idx].active = False

        if new_count > 0:
            logger.info(
                "Drift BET markets updated",
                total=len(self.markets),
                new=new_count,
                active=sum(1 for m in self.markets.values() if m.active),
            )

    async def _fetch_markets(self, session: aiohttp.ClientSession) -> list[dict]:
        """Try to fetch markets from Drift BET API."""
        # Primary endpoint: /markets or /v1/markets
        for endpoint in ["/markets", "/v1/markets", "/prediction-markets"]:
            try:
                url = f"{self.api_url}{endpoint}"
                params = {"status": "active", "category": "crypto"}
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # API may return {"markets": [...]} or just [...]
                        if isinstance(data, list):
                            return data
                        if isinstance(data, dict):
                            return data.get("markets", data.get("data", []))
            except (aiohttp.ClientError, KeyError) as exc:
                logger.debug("Drift endpoint miss", endpoint=endpoint, error=str(exc))
                continue

        # Fallback: try to get all markets and filter client-side
        try:
            url = f"{self.api_url}/markets"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        return [m for m in data if self._is_crypto_bucket(m)]
                    if isinstance(data, dict):
                        items = data.get("markets", data.get("data", []))
                        return [m for m in items if self._is_crypto_bucket(m)]
        except (aiohttp.ClientError, KeyError):
            pass

        return []

    @staticmethod
    def _is_crypto_bucket(raw: dict) -> bool:
        """Check if a raw market entry is a crypto price bucket we care about."""
        question = (raw.get("question", "") or raw.get("title", "")).lower()
        # Must mention a crypto we track
        has_crypto = any(s in question for s in ["btc", "eth", "sol", "bitcoin", "ethereum", "solana"])
        # Must be a short-term direction market
        has_direction = any(w in question for w in ["up", "down", "above", "below", "higher", "lower"])
        return has_crypto and has_direction

    def _parse_market(self, raw: dict) -> DriftMarket | None:
        """Parse a raw API response into a DriftMarket."""
        try:
            question = raw.get("question", "") or raw.get("title", "")
            question_lower = question.lower()

            # Determine symbol
            symbol = ""
            for key, mapped in DRIFT_SYMBOL_MAP.items():
                if key.lower() in question_lower:
                    symbol = mapped
                    break
            if not symbol:
                return None

            # Determine bucket duration
            bucket = ""
            for keyword, bucket_label in BUCKET_KEYWORDS.items():
                if keyword in question_lower:
                    bucket = bucket_label
                    break
            if not bucket:
                # Default to 5m if it's clearly a short-term market
                if any(w in question_lower for w in ["minute", "min"]):
                    bucket = "5m"
                else:
                    return None

            # Extract market index
            market_index = raw.get("marketIndex", raw.get("market_index", raw.get("id", 0)))
            if isinstance(market_index, str):
                try:
                    market_index = int(market_index)
                except ValueError:
                    market_index = hash(market_index) % 100000

            # Timestamps
            start_ts = int(raw.get("startTs", raw.get("start_ts", raw.get("createdAt", 0))))
            end_ts = int(raw.get("endTs", raw.get("end_ts", raw.get("expiresAt", 0))))

            # Prices — Drift may report as probability or price
            yes_price = float(raw.get("yesPrice", raw.get("yes_price", raw.get("probability", 0.5))))
            no_price = 1.0 - yes_price
            if "noPrice" in raw or "no_price" in raw:
                no_price = float(raw.get("noPrice", raw.get("no_price", no_price)))

            # Liquidity
            yes_liq = float(raw.get("yesLiquidity", raw.get("yes_liquidity", 0)))
            no_liq = float(raw.get("noLiquidity", raw.get("no_liquidity", 0)))

            return DriftMarket(
                market_index=market_index,
                question=question,
                symbol=symbol,
                bucket=bucket,
                start_ts=start_ts,
                end_ts=end_ts,
                yes_price=yes_price,
                no_price=no_price,
                yes_liquidity=yes_liq,
                no_liquidity=no_liq,
            )

        except Exception as exc:
            logger.debug("Failed to parse Drift market", error=str(exc))
            return None

    def get_active_markets(self, symbol: str = "") -> list[DriftMarket]:
        """Get active Drift BET markets, optionally filtered by symbol."""
        if symbol:
            return [m for m in self.markets_by_symbol.get(symbol, []) if m.active]
        return [m for m in self.markets.values() if m.active and m.seconds_remaining > 0]

    def get_price(self, market_index: int) -> tuple[float, float]:
        """Get (yes_price, no_price) for a Drift market."""
        m = self.markets.get(market_index)
        if not m:
            return 0.5, 0.5
        return m.yes_price, m.no_price
