"""Momentum + Oracle-Lag Latency Strategy.

Core idea: Binance spot price moves BEFORE Polymarket CLOB books
reprice. When we detect a strong directional move on Binance that
hasn't yet been reflected in Polymarket Yes/No prices, we buy the
underpriced outcome token before market makers adjust.

Signal flow:
  1. Track rolling price change over a short lookback (e.g. 30s).
  2. Compare Binance-implied fair value with Polymarket best ask (YES)
     or best bid (NO).
  3. If the gap exceeds our threshold → emit a Signal.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import SYMBOL_TO_SLUG_PREFIX
from polymarket_micro_arb.data.polymarket_ws import PolymarketWSClient
from polymarket_micro_arb.models import (
    BinanceTick,
    MarketInfo,
    Outcome,
    Side,
    Signal,
    SignalType,
)
from polymarket_micro_arb.utils.logger import logger


# Rolling price window per symbol
_PriceEntry = tuple[float, float]  # (timestamp_sec, price)


class MomentumLatencyStrategy:
    """Detects Binance momentum that hasn't repriced on Polymarket."""

    def __init__(
        self,
        polymarket_ws: PolymarketWSClient,
        lookback_sec: float = 30.0,
        momentum_threshold: float | None = None,
        latency_edge_ms: int | None = None,
    ) -> None:
        self.polymarket_ws = polymarket_ws
        self.lookback_sec = lookback_sec
        self.momentum_threshold = momentum_threshold or settings.momentum_threshold
        self.latency_edge_ms = latency_edge_ms or settings.latency_edge_ms

        # symbol -> deque of (ts, price)
        self._price_windows: dict[str, deque[_PriceEntry]] = defaultdict(
            lambda: deque(maxlen=500)
        )
        # Track last signal time per market to avoid spam
        self._last_signal_ts: dict[str, float] = {}
        # Minimum cooldown between signals on the same market (seconds)
        self._signal_cooldown = 10.0

    def on_tick(self, tick: BinanceTick) -> None:
        """Ingest a Binance tick into the rolling window."""
        ts = tick.timestamp_ms / 1000.0
        self._price_windows[tick.symbol].append((ts, tick.price))

    def evaluate(
        self, markets: list[MarketInfo]
    ) -> list[Signal]:
        """Check all active markets for momentum-latency signals."""
        signals: list[Signal] = []
        now = time.time()

        for market in markets:
            if not market.active or market.seconds_remaining < 15:
                # Too close to expiry – skip
                continue

            window = self._price_windows.get(market.symbol)
            if not window or len(window) < 5:
                continue

            # Compute momentum: price change over lookback window
            cutoff = now - self.lookback_sec
            recent = [(ts, p) for ts, p in window if ts >= cutoff]
            if len(recent) < 2:
                continue

            price_start = recent[0][1]
            price_now = recent[-1][1]
            pct_change = (price_now - price_start) / price_start

            # Determine direction – is price going UP or DOWN?
            going_up = pct_change > self.momentum_threshold
            going_down = pct_change < -self.momentum_threshold

            if not (going_up or going_down):
                continue

            # Compare with Polymarket book pricing
            signal = self._check_latency_edge(market, pct_change, going_up, now)
            if signal:
                signals.append(signal)

        return signals

    def _check_latency_edge(
        self,
        market: MarketInfo,
        pct_change: float,
        going_up: bool,
        now: float,
    ) -> Signal | None:
        """Check if the Polymarket book is lagging behind Binance."""
        # Cooldown check
        last = self._last_signal_ts.get(market.condition_id, 0)
        if now - last < self._signal_cooldown:
            return None

        # Get Polymarket prices for YES token
        yes_bid, yes_ask = self.polymarket_ws.get_best_prices(
            market.token_id_yes
        )
        no_bid, no_ask = self.polymarket_ws.get_best_prices(
            market.token_id_no
        )

        if going_up:
            # Price going up → YES should be worth more.
            # If YES ask is still cheap (< fair value), buy YES.
            # Rough fair value = 0.5 + |pct_change| * sensitivity
            # We use a simplified model here.
            implied_yes_fair = min(0.95, 0.5 + abs(pct_change) * 50)
            edge = implied_yes_fair - yes_ask

            if edge > settings.min_spread_profit and yes_ask > 0.01:
                self._last_signal_ts[market.condition_id] = now
                logger.info(
                    "Momentum signal: BUY YES",
                    market=market.slug,
                    pct_change=f"{pct_change:.4f}",
                    yes_ask=yes_ask,
                    implied_fair=f"{implied_yes_fair:.4f}",
                    edge=f"{edge:.4f}",
                )
                return Signal(
                    signal_type=SignalType.MOMENTUM_LATENCY,
                    market=market,
                    side=Side.BUY,
                    outcome=Outcome.YES,
                    confidence=min(1.0, abs(pct_change) / self.momentum_threshold),
                    edge=edge,
                    meta={
                        "pct_change": pct_change,
                        "yes_ask": yes_ask,
                        "implied_fair": implied_yes_fair,
                    },
                )
        else:
            # Price going down → NO should be worth more (i.e. YES drops).
            # Buy NO if it's underpriced.
            implied_no_fair = min(0.95, 0.5 + abs(pct_change) * 50)
            edge = implied_no_fair - no_ask

            if edge > settings.min_spread_profit and no_ask > 0.01:
                self._last_signal_ts[market.condition_id] = now
                logger.info(
                    "Momentum signal: BUY NO",
                    market=market.slug,
                    pct_change=f"{pct_change:.4f}",
                    no_ask=no_ask,
                    implied_fair=f"{implied_no_fair:.4f}",
                    edge=f"{edge:.4f}",
                )
                return Signal(
                    signal_type=SignalType.MOMENTUM_LATENCY,
                    market=market,
                    side=Side.BUY,
                    outcome=Outcome.NO,
                    confidence=min(1.0, abs(pct_change) / self.momentum_threshold),
                    edge=edge,
                    meta={
                        "pct_change": pct_change,
                        "no_ask": no_ask,
                        "implied_fair": implied_no_fair,
                    },
                )

        return None
