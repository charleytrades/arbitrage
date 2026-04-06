"""Gamma API client for Polymarket market discovery.

Discovers active 5m/15m BTC/ETH/SOL Up/Down micro-markets using both
the Gamma REST API and predictable slug generation based on floored
Unix timestamps.
"""

from __future__ import annotations

import math
import time

import aiohttp

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import (
    BUCKET_DURATIONS_SEC,
    SYMBOL_TO_SLUG_PREFIX,
)
from polymarket_micro_arb.models import MarketInfo
from polymarket_micro_arb.utils.logger import logger


def floor_timestamp(ts: float, duration_sec: int) -> int:
    """Floor a unix timestamp to the nearest bucket boundary."""
    return int(math.floor(ts / duration_sec) * duration_sec)


def generate_slug(symbol: str, bucket: str, floored_ts: int) -> str:
    """Build the predictable Polymarket slug for a micro-market.

    Pattern: {coin}-updown-{bucket}-{floored_unix_ts}
    e.g.  btc-updown-5m-1700000100
    """
    prefix = SYMBOL_TO_SLUG_PREFIX.get(symbol, symbol[:3].lower())
    return f"{prefix}-updown-{bucket}-{floored_ts}"


class GammaClient:
    """Async client for the Gamma API (market discovery)."""

    def __init__(self) -> None:
        self.base_url = settings.gamma_api_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Slug-based discovery (fast, predictable) ────────────────────
    async def discover_current_markets(self) -> list[MarketInfo]:
        """Generate slugs for the current + next bucket windows and
        look them up via the Gamma API."""
        now = time.time()
        markets: list[MarketInfo] = []

        for symbol in settings.symbol_list:
            for bucket_label, dur_sec in BUCKET_DURATIONS_SEC.items():
                # Current window
                current_floor = floor_timestamp(now, dur_sec)
                # Next window (pre-fetch for early positioning)
                next_floor = current_floor + dur_sec

                for floored_ts in (current_floor, next_floor):
                    slug = generate_slug(symbol, bucket_label, floored_ts)
                    market = await self._fetch_market_by_slug(slug)
                    if market:
                        market.symbol = symbol
                        market.bucket = bucket_label
                        market.start_ts = floored_ts
                        market.end_ts = floored_ts + dur_sec
                        markets.append(market)

        logger.info(
            "Market discovery complete",
            discovered=len(markets),
            symbols=settings.symbol_list,
        )
        return markets

    async def _fetch_market_by_slug(self, slug: str) -> MarketInfo | None:
        """Fetch a single market from Gamma by its slug."""
        session = await self._get_session()
        url = f"{self.base_url}/markets"
        params = {"slug": slug, "closed": "false"}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.debug("Gamma slug lookup miss", slug=slug, status=resp.status)
                    return None

                data = await resp.json()
                if not data:
                    return None

                # Gamma returns a list; take the first match
                m = data[0] if isinstance(data, list) else data
                return self._parse_market(m, slug)

        except (aiohttp.ClientError, KeyError, IndexError) as exc:
            logger.warning("Gamma API error", slug=slug, error=str(exc))
            return None

    # ── Full binary-market scan (paginated) ──────────────────────────
    async def discover_all_binary_markets(
        self,
        known_ids: set[str] | None = None,
        max_markets: int = 500,
    ) -> list[MarketInfo]:
        """Paginate through ALL open Polymarket markets and return
        every binary market (exactly 2 CLOB tokens) not in *known_ids*.

        Used by the broad cross-outcome arb scanner so it can find
        YES+NO < $0.99 opportunities across the entire platform.
        """
        known_ids = known_ids or set()
        session = await self._get_session()
        url = f"{self.base_url}/markets"
        markets: list[MarketInfo] = []
        offset = 0
        page_size = 100
        far_future = int(time.time()) + 86400 * 365  # 1 year from now

        while len(markets) < max_markets:
            params = {
                "closed": "false",
                "limit": str(page_size),
                "offset": str(offset),
            }
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "Broad scan page failed",
                            status=resp.status,
                            offset=offset,
                        )
                        break
                    data = await resp.json()
                    if not data:
                        break  # No more pages

                    for raw in data:
                        cid = raw.get("conditionId", raw.get("condition_id", ""))
                        if not cid or cid in known_ids:
                            continue

                        parsed = self._parse_market(raw, raw.get("slug", ""))
                        if parsed is None:
                            continue  # Not binary (< 2 tokens)

                        # Fill in broad-scan defaults
                        parsed.symbol = ""
                        parsed.bucket = "broad"
                        parsed.start_ts = 0
                        parsed.end_ts = far_future
                        markets.append(parsed)

                        if len(markets) >= max_markets:
                            break

                    # If page was smaller than page_size, we've hit the end
                    if len(data) < page_size:
                        break
                    offset += page_size
                    await asyncio.sleep(0.2)  # Rate limit API requests

            except (aiohttp.ClientError, Exception) as exc:
                logger.warning("Broad scan error", offset=offset, error=str(exc))
                break

        logger.info(
            "Broad market scan complete",
            discovered=len(markets),
            skipped_known=len(known_ids),
        )
        return markets

    # ── Broad search fallback ───────────────────────────────────────
    async def search_markets(
        self, query: str = "up/down", tag: str = "crypto", limit: int = 50
    ) -> list[MarketInfo]:
        """Broad Gamma search – useful for initial market discovery when
        slug patterns are unknown or changed."""
        session = await self._get_session()
        url = f"{self.base_url}/markets"
        params = {
            "tag": tag,
            "closed": "false",
            "limit": str(limit),
        }

        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                markets = []
                for m in data:
                    question = (m.get("question") or "").lower()
                    if query.lower() in question:
                        parsed = self._parse_market(m, m.get("slug", ""))
                        if parsed:
                            markets.append(parsed)
                return markets
        except aiohttp.ClientError as exc:
            logger.warning("Gamma search failed", error=str(exc))
            return []

    # ── Helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _parse_market(raw: dict, slug: str) -> MarketInfo | None:
        """Parse raw Gamma JSON into our MarketInfo model."""
        try:
            tokens = raw.get("clobTokenIds") or raw.get("tokens", [])
            if isinstance(tokens, str):
                # Sometimes returned as JSON string
                import json
                tokens = json.loads(tokens)

            # Ensure we have exactly 2 outcome tokens (binary market)
            if not tokens or len(tokens) < 2:
                return None

            # tokens[0] = YES, tokens[1] = NO
            if isinstance(tokens[0], dict):
                yes_id = tokens[0].get("token_id", "")
                no_id = tokens[1].get("token_id", "")
            else:
                yes_id = str(tokens[0])
                no_id = str(tokens[1])

            return MarketInfo(
                condition_id=raw.get("conditionId", raw.get("condition_id", "")),
                question=raw.get("question", ""),
                slug=slug or raw.get("slug", ""),
                token_id_yes=yes_id,
                token_id_no=no_id,
                symbol="",  # Caller fills in
                bucket="",  # Caller fills in
                start_ts=0,
                end_ts=0,
            )
        except Exception as exc:
            logger.debug("Failed to parse market", error=str(exc), slug=slug)
            return None
