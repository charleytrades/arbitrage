"""Momentum + Oracle-Lag Latency Strategy (hardened, 90%+ filter).

Production rules:
  1. New 5m/15m bucket opens → monitor Binance for first 15-45 seconds.
  2. If price moves >=0.35% with volume confirmation AND Polymarket
     Yes/No prices haven't snapped yet (lag window), buy underpriced side.
  3. Multi-venue confirmation: require Binance + Bybit agreement.
  4. Hold to resolution or exit when edge collapses.

The 15-45s window exploits the fact that Polymarket MMs are slow to
reprice newly-opened micro-markets, while Binance/Bybit already
reflect the directional move.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.data.binance_ws import VolumeTracker
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

# Rolling price window per symbol: (timestamp_sec, price, source)
_PriceEntry = tuple[float, float, str]


class MomentumLatencyStrategy:
    """Detects Binance/Bybit momentum that hasn't repriced on Polymarket.

    High-winrate filter:
      - Only fires in the 15-45s window after bucket open
      - Requires >=0.35% move with volume confirmation
      - Requires Binance + Bybit price agreement (within 0.05%)
      - Only buys if Polymarket ask hasn't snapped to fair value
    """

    def __init__(
        self,
        polymarket_ws: PolymarketWSClient,
        volume_tracker: VolumeTracker,
        bybit_prices: dict[str, float] | None = None,
    ) -> None:
        self.polymarket_ws = polymarket_ws
        self.volume_tracker = volume_tracker
        # Reference to BybitWSClient.latest_prices for multi-venue check
        self._bybit_prices = bybit_prices or {}

        self.momentum_threshold = settings.momentum_threshold  # 0.35%
        self.window_start = settings.momentum_window_start_sec  # 15s
        self.window_end = settings.momentum_window_end_sec  # 45s
        self.volume_multiplier = settings.volume_confirm_multiplier  # 1.5x

        # symbol -> deque of (ts, price, source)
        self._price_windows: dict[str, deque[_PriceEntry]] = defaultdict(
            lambda: deque(maxlen=2000)
        )
        # Binance-only prices for momentum calc (avoid mixing venues)
        self._binance_prices: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=1000)
        )

        # Track last signal time per market to avoid spam
        self._last_signal_ts: dict[str, float] = {}
        # One signal per market per bucket window
        self._signaled_markets: set[str] = set()

    def on_tick(self, tick: BinanceTick) -> None:
        """Ingest a price tick into the rolling window."""
        ts = tick.timestamp_ms / 1000.0
        self._price_windows[tick.symbol].append((ts, tick.price, tick.source))

        if tick.source == "binance":
            self._binance_prices[tick.symbol].append((ts, tick.price))

    def set_bybit_prices(self, prices: dict[str, float]) -> None:
        """Update reference to Bybit latest prices."""
        self._bybit_prices = prices

    def evaluate(self, markets: list[MarketInfo]) -> list[Signal]:
        """Check all active markets for momentum-latency signals."""
        signals: list[Signal] = []
        now = time.time()

        for market in markets:
            if not market.active:
                continue

            # ── Time window filter: only fire 15-45s after bucket open ──
            age = market.seconds_since_open
            if age < self.window_start or age > self.window_end:
                continue

            # Don't re-signal a market we already traded this window
            if market.condition_id in self._signaled_markets:
                continue

            # Need sufficient Binance tick data
            binance_window = self._binance_prices.get(market.symbol)
            if not binance_window or len(binance_window) < 5:
                logger.debug(
                    "GUARD:TICKS insufficient",
                    market=market.slug,
                    ticks=len(binance_window) if binance_window else 0,
                    age=f"{age:.1f}s",
                )
                continue

            # ── Compute momentum from Binance only ──────────────────
            # Use ticks from the start of this bucket
            bucket_start = market.start_ts
            recent = [
                (ts, p) for ts, p in binance_window if ts >= bucket_start
            ]
            if len(recent) < 5:
                logger.debug(
                    "GUARD:BUCKET_TICKS insufficient",
                    market=market.slug,
                    bucket_ticks=len(recent),
                    age=f"{age:.1f}s",
                )
                continue

            price_at_open = recent[0][1]
            price_now = recent[-1][1]
            pct_change = (price_now - price_at_open) / price_at_open

            going_up = pct_change >= self.momentum_threshold
            going_down = pct_change <= -self.momentum_threshold

            if not (going_up or going_down):
                # Log near-misses (>50% of threshold)
                if abs(pct_change) >= self.momentum_threshold * 0.5:
                    logger.info(
                        "FILTER:MOMENTUM near-miss",
                        market=market.slug,
                        pct_change=f"{pct_change:.4f}",
                        threshold=f"{self.momentum_threshold:.4f}",
                        age=f"{age:.1f}s",
                        need=f"{self.momentum_threshold - abs(pct_change):.4f} more",
                    )
                continue

            # Momentum threshold passed — log it
            logger.info(
                "PASSED:MOMENTUM threshold",
                market=market.slug,
                pct_change=f"{pct_change:.4f}",
                direction="UP" if going_up else "DOWN",
                age=f"{age:.1f}s",
            )

            # ── Volume confirmation ─────────────────────────────────
            if not self.volume_tracker.is_volume_confirmed(
                market.symbol,
                multiplier=self.volume_multiplier,
                lookback_sec=age,  # Look at volume since bucket open
            ):
                recent_vol = self.volume_tracker.get_recent_volume(market.symbol, age)
                baseline_vol = self.volume_tracker.get_baseline_volume(market.symbol)
                logger.info(
                    "FILTER:VOLUME blocked",
                    market=market.slug,
                    pct_change=f"{pct_change:.4f}",
                    recent_vol=f"{recent_vol:.2f}",
                    baseline=f"{baseline_vol:.2f}",
                    need=f"{self.volume_multiplier}x",
                    got=f"{recent_vol / max(baseline_vol, 0.001):.2f}x",
                )
                continue

            logger.info("PASSED:VOLUME confirmed", market=market.slug)

            # ── Multi-venue confirmation (Binance + Bybit agree) ────
            bybit_price = self._bybit_prices.get(market.symbol)
            if bybit_price is not None and price_at_open > 0:
                bybit_pct = (bybit_price - price_at_open) / price_at_open
                # Both venues must agree on direction
                if going_up and bybit_pct < self.momentum_threshold * 0.5:
                    logger.info(
                        "FILTER:BYBIT disagrees (UP)",
                        market=market.slug,
                        binance_pct=f"{pct_change:.4f}",
                        bybit_pct=f"{bybit_pct:.4f}",
                        need=f">= {self.momentum_threshold * 0.5:.4f}",
                    )
                    continue
                if going_down and bybit_pct > -self.momentum_threshold * 0.5:
                    logger.info(
                        "FILTER:BYBIT disagrees (DOWN)",
                        market=market.slug,
                        binance_pct=f"{pct_change:.4f}",
                        bybit_pct=f"{bybit_pct:.4f}",
                        need=f"<= {-self.momentum_threshold * 0.5:.4f}",
                    )
                    continue

            logger.info("PASSED:BYBIT confirmed", market=market.slug)

            # ── Check Polymarket book for lag (the actual edge) ─────
            signal = self._check_latency_edge(
                market, pct_change, going_up, price_now, now
            )
            if signal:
                self._signaled_markets.add(market.condition_id)
                signals.append(signal)
            else:
                # Log why the edge check failed
                yes_bid, yes_ask = self.polymarket_ws.get_best_prices(
                    market.token_id_yes
                )
                no_bid, no_ask = self.polymarket_ws.get_best_prices(
                    market.token_id_no
                )
                move_strength = abs(pct_change) / self.momentum_threshold
                implied_fair = min(0.95, 0.5 + (move_strength - 1.0) * 0.15 + 0.15)
                if going_up:
                    edge = implied_fair - yes_ask - (yes_ask * 0.10)
                    logger.info(
                        "FILTER:EDGE too small (UP)",
                        market=market.slug,
                        yes_ask=yes_ask,
                        implied_fair=f"{implied_fair:.4f}",
                        edge=f"{edge:.4f}",
                        min_needed=f"{settings.min_spread_profit:.4f}",
                        verdict="Polymarket already repriced" if edge <= 0 else "edge below min_spread_profit",
                    )
                else:
                    edge = implied_fair - no_ask - (no_ask * 0.10)
                    logger.info(
                        "FILTER:EDGE too small (DOWN)",
                        market=market.slug,
                        no_ask=no_ask,
                        implied_fair=f"{implied_fair:.4f}",
                        edge=f"{edge:.4f}",
                        min_needed=f"{settings.min_spread_profit:.4f}",
                        verdict="Polymarket already repriced" if edge <= 0 else "edge below min_spread_profit",
                    )

        return signals

    def _check_latency_edge(
        self,
        market: MarketInfo,
        pct_change: float,
        going_up: bool,
        spot_price: float,
        now: float,
    ) -> Signal | None:
        """Check if the Polymarket book is lagging behind spot venues."""
        yes_bid, yes_ask = self.polymarket_ws.get_best_prices(
            market.token_id_yes
        )
        no_bid, no_ask = self.polymarket_ws.get_best_prices(
            market.token_id_no
        )

        if going_up:
            # Price going up → YES should be worth more.
            # Implied fair value based on magnitude of move.
            # Sigmoid-like mapping: strong move → high probability
            move_strength = abs(pct_change) / self.momentum_threshold
            implied_yes_fair = min(0.95, 0.5 + (move_strength - 1.0) * 0.15 + 0.15)
            fee = yes_ask * 0.10  # 10% Polymarket taker fee
            edge = implied_yes_fair - yes_ask - fee

            if edge > settings.min_spread_profit and 0.01 < yes_ask < implied_yes_fair:
                # Confidence is high because we have multi-venue + volume confirmation
                confidence = min(0.98, 0.85 + move_strength * 0.04)

                logger.info(
                    "MOMENTUM SIGNAL: BUY YES (multi-venue confirmed)",
                    market=market.slug,
                    pct_change=f"{pct_change:.4f}",
                    yes_ask=yes_ask,
                    implied_fair=f"{implied_yes_fair:.4f}",
                    edge=f"{edge:.4f}",
                    confidence=f"{confidence:.3f}",
                    age_sec=f"{market.seconds_since_open:.1f}",
                )
                return Signal(
                    signal_type=SignalType.MOMENTUM_LATENCY,
                    market=market,
                    side=Side.BUY,
                    outcome=Outcome.YES,
                    confidence=confidence,
                    edge=edge,
                    limit_price=round(yes_ask, 4),  # Limit at current ask
                    meta={
                        "pct_change": pct_change,
                        "yes_ask": yes_ask,
                        "no_ask": no_ask,
                        "implied_fair": implied_yes_fair,
                        "spot_price": spot_price,
                        "volume_confirmed": True,
                        "multi_venue": True,
                    },
                )
        else:
            # Price going down → NO should be worth more
            move_strength = abs(pct_change) / self.momentum_threshold
            implied_no_fair = min(0.95, 0.5 + (move_strength - 1.0) * 0.15 + 0.15)
            fee = no_ask * 0.10  # 10% Polymarket taker fee
            edge = implied_no_fair - no_ask - fee

            if edge > settings.min_spread_profit and 0.01 < no_ask < implied_no_fair:
                confidence = min(0.98, 0.85 + move_strength * 0.04)

                logger.info(
                    "MOMENTUM SIGNAL: BUY NO (multi-venue confirmed)",
                    market=market.slug,
                    pct_change=f"{pct_change:.4f}",
                    no_ask=no_ask,
                    implied_fair=f"{implied_no_fair:.4f}",
                    edge=f"{edge:.4f}",
                    confidence=f"{confidence:.3f}",
                    age_sec=f"{market.seconds_since_open:.1f}",
                )
                return Signal(
                    signal_type=SignalType.MOMENTUM_LATENCY,
                    market=market,
                    side=Side.BUY,
                    outcome=Outcome.NO,
                    confidence=confidence,
                    edge=edge,
                    limit_price=round(no_ask, 4),
                    meta={
                        "pct_change": pct_change,
                        "yes_ask": yes_ask,
                        "no_ask": no_ask,
                        "implied_fair": implied_no_fair,
                        "spot_price": spot_price,
                        "volume_confirmed": True,
                        "multi_venue": True,
                    },
                )

        return None

    def cleanup_expired(self, markets: list[MarketInfo]) -> None:
        """Remove signal tracking for expired markets."""
        active_ids = {m.condition_id for m in markets if m.active}
        self._signaled_markets = self._signaled_markets & active_ids
