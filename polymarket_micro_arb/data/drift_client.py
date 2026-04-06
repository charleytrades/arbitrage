"""Drift BET prediction market client.

Polls Drift's public APIs for active crypto prediction (BET) markets
and tracks their prices. Used for cross-platform arbitrage against
Polymarket.

Drift BET markets are perp markets with contract_type=Prediction.
They use the -BET suffix naming convention (e.g., "BTC-5M-UP-BET").

Architecture:
  - REST polling via Drift Data API for market discovery
  - DLOB REST API for real-time L2 orderbook / best bid-ask
  - No auth needed for read-only data access
  - Matches Drift markets to Polymarket markets by symbol + timeframe
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import defaultdict

import aiohttp

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.models import DriftMarket, Platform
from polymarket_micro_arb.utils.logger import logger

# Drift Data API (market discovery, stats)
DRIFT_DATA_API = "https://data.api.drift.trade"
# Drift DLOB API (orderbook, best bid/ask)
DRIFT_DLOB_API = "https://dlob.drift.trade"

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
    "5m": "5m",
    "5-min": "5m",
    "5 min": "5m",
    "15m": "15m",
    "15-min": "15m",
    "15 min": "15m",
    "1h": "1h",
    "1-hour": "1h",
}

# Regex to detect BET markets by name pattern (e.g., "BTC-5M-UP-BET")
BET_NAME_PATTERN = re.compile(
    r"(BTC|ETH|SOL)[-_ ]?(\d+[MmHh])[-_ ]?(UP|DOWN|ABOVE|BELOW)[-_ ]?BET",
    re.IGNORECASE,
)


class DriftBetClient:
    """Async client for Drift BET prediction market data.

    Discovers and tracks crypto price prediction markets on Drift
    that overlap with Polymarket's 5m/15m micro-buckets.

    Uses Drift's public REST APIs (no auth needed for reads):
    - Data API: market discovery and stats
    - DLOB API: L2 orderbook for best bid/ask prices
    """

    def __init__(self) -> None:
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
        logger.info(
            "Drift BET client starting",
            data_api=DRIFT_DATA_API,
            dlob_api=DRIFT_DLOB_API,
        )

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
        """Fetch active BET markets from Drift Data API and update prices."""
        session = await self._get_session()

        # Step 1: Discover BET markets via Data API
        bet_markets = await self._fetch_bet_markets(session)
        if not bet_markets:
            return

        now = time.time()
        new_count = 0

        for raw in bet_markets:
            market = self._parse_market(raw)
            if market and market.active:
                if market.market_index not in self.markets:
                    new_count += 1
                self.markets[market.market_index] = market

        # Step 2: Fetch L2 orderbook prices for each BET market
        await self._update_prices(session)

        # Rebuild symbol index
        self.markets_by_symbol.clear()
        for m in self.markets.values():
            if m.active and m.seconds_remaining > 0:
                self.markets_by_symbol[m.symbol].append(m)

        # Clean up expired
        expired = [
            idx for idx, m in self.markets.items()
            if m.end_ts > 0 and m.end_ts < now
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

    async def _fetch_bet_markets(self, session: aiohttp.ClientSession) -> list[dict]:
        """Fetch all markets from Drift Data API and filter for BET prediction markets."""
        try:
            url = f"{DRIFT_DATA_API}/stats/markets"
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.debug("Drift Data API non-200", status=resp.status)
                    return []
                data = await resp.json()

                # Response is a list of market objects
                if isinstance(data, list):
                    markets = data
                elif isinstance(data, dict):
                    markets = data.get("markets", data.get("data", data.get("result", [])))
                else:
                    return []

                # Filter for BET prediction markets
                bet_markets = [
                    m for m in markets
                    if self._is_bet_market(m)
                ]

                return bet_markets

        except (aiohttp.ClientError, Exception) as exc:
            logger.debug("Drift Data API fetch error", error=str(exc))
            return []

    async def _update_prices(self, session: aiohttp.ClientSession) -> None:
        """Fetch L2 orderbook from DLOB API for each active BET market."""
        active_markets = [m for m in self.markets.values() if m.active]
        if not active_markets:
            return

        # Batch price fetches concurrently (max 10 at a time)
        semaphore = asyncio.Semaphore(10)

        async def fetch_price(market: DriftMarket) -> None:
            async with semaphore:
                try:
                    url = f"{DRIFT_DLOB_API}/l2"
                    params = {
                        "marketIndex": market.market_index,
                        "marketType": "perp",
                        "depth": 1,
                        "includeVamm": "true",
                    }
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return
                        data = await resp.json()

                        # L2 response: {"bids": [{"price": "0.55", "size": "100"}], "asks": [...]}
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])

                        if asks:
                            # Best ask = YES price (cost to buy YES)
                            yes_price = float(asks[0].get("price", 0.5))
                        elif "oracle" in data:
                            yes_price = float(data["oracle"])
                        else:
                            return

                        if bids:
                            # Best bid = price someone will pay for YES
                            # NO price ≈ 1 - best_bid (or from ask side of NO)
                            no_price = 1.0 - float(bids[0].get("price", 0.5))
                        else:
                            no_price = 1.0 - yes_price

                        market.yes_price = yes_price
                        market.no_price = max(0.01, min(0.99, no_price))

                        # Liquidity from depth
                        if asks:
                            market.yes_liquidity = float(asks[0].get("size", 0))
                        if bids:
                            market.no_liquidity = float(bids[0].get("size", 0))

                except (aiohttp.ClientError, Exception) as exc:
                    logger.debug(
                        "DLOB price fetch failed",
                        market_index=market.market_index,
                        error=str(exc),
                    )

        await asyncio.gather(*(fetch_price(m) for m in active_markets))

    @staticmethod
    def _is_bet_market(raw: dict) -> bool:
        """Check if a raw market entry is a crypto BET prediction market."""
        # Check for -BET suffix in market name
        market_name = str(raw.get("marketName", raw.get("name", raw.get("symbol", ""))))
        if "BET" in market_name.upper():
            # Must also reference a crypto we track
            name_upper = market_name.upper()
            has_crypto = any(s in name_upper for s in ["BTC", "ETH", "SOL"])
            if has_crypto:
                return True

        # Fallback: check contract_type field
        contract_type = raw.get("contractType", raw.get("contract_type", ""))
        if str(contract_type).lower() in ("prediction", "bet"):
            question = str(raw.get("question", raw.get("title", market_name))).lower()
            has_crypto = any(s in question for s in ["btc", "eth", "sol", "bitcoin", "ethereum", "solana"])
            if has_crypto:
                return True

        return False

    def _parse_market(self, raw: dict) -> DriftMarket | None:
        """Parse a raw API response into a DriftMarket."""
        try:
            market_name = str(raw.get("marketName", raw.get("name", raw.get("symbol", ""))))
            question = raw.get("question", raw.get("title", market_name))

            # Try regex pattern first (e.g., "BTC-5M-UP-BET")
            match = BET_NAME_PATTERN.search(market_name) or BET_NAME_PATTERN.search(question)

            if match:
                crypto = match.group(1).upper()
                symbol = DRIFT_SYMBOL_MAP.get(crypto, "")
                duration = match.group(2).lower()
                # Normalize bucket
                bucket = BUCKET_KEYWORDS.get(duration, "")
                if not bucket and duration.endswith("m"):
                    bucket = duration
            else:
                # Fallback: scan for symbol + bucket in name/question
                text = f"{market_name} {question}".lower()
                symbol = ""
                for key, mapped in DRIFT_SYMBOL_MAP.items():
                    if key.lower() in text:
                        symbol = mapped
                        break
                if not symbol:
                    return None

                bucket = ""
                for keyword, bucket_label in BUCKET_KEYWORDS.items():
                    if keyword in text:
                        bucket = bucket_label
                        break
                if not bucket:
                    if any(w in text for w in ["minute", "min"]):
                        bucket = "5m"
                    else:
                        return None

            if not symbol:
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

            # Initial price from stats (will be updated by DLOB L2 fetch)
            yes_price = float(raw.get("yesPrice", raw.get("yes_price",
                              raw.get("probability", raw.get("lastPrice", 0.5)))))
            no_price = 1.0 - yes_price
            if "noPrice" in raw or "no_price" in raw:
                no_price = float(raw.get("noPrice", raw.get("no_price", no_price)))

            # Liquidity
            yes_liq = float(raw.get("yesLiquidity", raw.get("yes_liquidity",
                            raw.get("baseAssetAmountLong", 0))))
            no_liq = float(raw.get("noLiquidity", raw.get("no_liquidity",
                           raw.get("baseAssetAmountShort", 0))))

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
