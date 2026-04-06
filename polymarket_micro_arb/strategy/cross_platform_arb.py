"""Cross-Platform Arbitrage Strategy (Polymarket vs Drift BET).

Detects price discrepancies on the same directional outcome across
two prediction market platforms. When the spread exceeds fees,
we buy the cheaper side.

Two modes of operation:

1. DIRECTIONAL ARB: Same outcome is cheaper on one platform
   - Polymarket YES = $0.55, Drift YES = $0.42
   - Buy YES on Drift (cheaper) → expected profit = $0.13 minus fees

2. CROSS-PLATFORM LOCK: Buy opposite sides on different platforms
   - Polymarket YES = $0.55, Drift NO = $0.40
   - Total cost = $0.95 → one side pays $1.00 → guaranteed $0.05

The strategy matches markets by symbol + bucket timeframe, then
compares prices continuously.
"""

from __future__ import annotations

import time

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.data.drift_client import DriftBetClient
from polymarket_micro_arb.data.polymarket_ws import PolymarketWSClient
from polymarket_micro_arb.models import (
    CrossPlatformPair,
    DriftMarket,
    MarketInfo,
    Outcome,
    Platform,
    Side,
    Signal,
    SignalType,
)
from polymarket_micro_arb.utils.logger import logger


class CrossPlatformArbStrategy:
    """Detects and exploits price discrepancies between Polymarket and Drift BET."""

    def __init__(
        self,
        polymarket_ws: PolymarketWSClient,
        drift_client: DriftBetClient,
        min_spread: float | None = None,
    ) -> None:
        self.polymarket_ws = polymarket_ws
        self.drift_client = drift_client
        self.min_spread = min_spread or settings.cross_platform_min_spread

        # Cooldown per pair
        self._last_signal_ts: dict[str, float] = {}
        self._signal_cooldown = 10.0

    def evaluate(
        self,
        polymarket_markets: list[MarketInfo],
    ) -> list[Signal]:
        """Match Polymarket and Drift markets, scan for arb opportunities."""
        signals: list[Signal] = []
        now = time.time()

        # Get all active Drift markets
        drift_markets = self.drift_client.get_active_markets()
        if not drift_markets:
            return signals

        # Match markets across platforms by symbol + bucket
        pairs = self._match_markets(polymarket_markets, drift_markets)

        for pair in pairs:
            # Cooldown
            pair_key = f"{pair.polymarket.condition_id}_{pair.drift.market_index}"
            last = self._last_signal_ts.get(pair_key, 0)
            if now - last < self._signal_cooldown:
                continue

            # Get live Polymarket prices
            _, poly_yes_ask = self.polymarket_ws.get_best_prices(
                pair.polymarket.token_id_yes
            )
            _, poly_no_ask = self.polymarket_ws.get_best_prices(
                pair.polymarket.token_id_no
            )

            # Get Drift prices
            drift_yes = pair.drift.yes_price
            drift_no = pair.drift.no_price

            # Skip if we don't have real data
            if poly_yes_ask >= 1.0 or poly_no_ask >= 1.0:
                continue
            if drift_yes <= 0 or drift_no <= 0:
                continue

            # ── Check all four arb combinations ─────────────────────

            # 1. Buy YES on Drift, it's cheaper than Polymarket YES
            if drift_yes < poly_yes_ask:
                spread = poly_yes_ask - drift_yes
                if spread >= self.min_spread:
                    signals.extend(
                        self._emit_directional_arb(
                            pair, Outcome.YES, "drift",
                            drift_yes, poly_yes_ask, spread, now
                        )
                    )
                    self._last_signal_ts[pair_key] = now
                    continue

            # 2. Buy YES on Polymarket, it's cheaper than Drift YES
            if poly_yes_ask < drift_yes:
                spread = drift_yes - poly_yes_ask
                if spread >= self.min_spread:
                    signals.extend(
                        self._emit_directional_arb(
                            pair, Outcome.YES, "polymarket",
                            poly_yes_ask, drift_yes, spread, now
                        )
                    )
                    self._last_signal_ts[pair_key] = now
                    continue

            # 3. Cross-platform lock: Poly YES + Drift NO < 1.0
            cross_cost_1 = poly_yes_ask + drift_no
            if cross_cost_1 < (1.0 - self.min_spread):
                profit = 1.0 - cross_cost_1
                signals.extend(
                    self._emit_cross_lock(
                        pair, poly_yes_ask, drift_no,
                        "poly_yes_drift_no", profit, now
                    )
                )
                self._last_signal_ts[pair_key] = now
                continue

            # 4. Cross-platform lock: Drift YES + Poly NO < 1.0
            cross_cost_2 = drift_yes + poly_no_ask
            if cross_cost_2 < (1.0 - self.min_spread):
                profit = 1.0 - cross_cost_2
                signals.extend(
                    self._emit_cross_lock(
                        pair, drift_yes, poly_no_ask,
                        "drift_yes_poly_no", profit, now
                    )
                )
                self._last_signal_ts[pair_key] = now

        return signals

    def _match_markets(
        self,
        poly_markets: list[MarketInfo],
        drift_markets: list[DriftMarket],
    ) -> list[CrossPlatformPair]:
        """Match Polymarket and Drift markets by symbol + bucket."""
        pairs: list[CrossPlatformPair] = []

        # Index Drift markets by (symbol, bucket)
        drift_index: dict[tuple[str, str], list[DriftMarket]] = {}
        for dm in drift_markets:
            key = (dm.symbol, dm.bucket)
            drift_index.setdefault(key, []).append(dm)

        for pm in poly_markets:
            if not pm.active or pm.seconds_remaining < 30:
                continue

            key = (pm.symbol, pm.bucket)
            matching_drift = drift_index.get(key, [])

            for dm in matching_drift:
                if not dm.active or dm.seconds_remaining < 30:
                    continue
                # Time windows should overlap significantly
                overlap_start = max(pm.start_ts, dm.start_ts)
                overlap_end = min(pm.end_ts, dm.end_ts)
                if overlap_end - overlap_start < 60:
                    continue  # Less than 1 minute overlap — not the same event

                pairs.append(CrossPlatformPair(
                    polymarket=pm,
                    drift=dm,
                    symbol=pm.symbol,
                    bucket=pm.bucket,
                ))

        return pairs

    def _emit_directional_arb(
        self,
        pair: CrossPlatformPair,
        outcome: Outcome,
        cheap_platform: str,
        cheap_price: float,
        expensive_price: float,
        spread: float,
        now: float,
    ) -> list[Signal]:
        """Emit a signal to buy the cheaper side on one platform."""
        # We buy on the cheaper platform
        if cheap_platform == "drift":
            market = pair.polymarket  # Signal references Polymarket for execution routing
            meta_platform = "drift"
        else:
            market = pair.polymarket
            meta_platform = "polymarket"

        logger.info(
            "CROSS-PLATFORM ARB: directional",
            symbol=pair.symbol,
            bucket=pair.bucket,
            outcome=outcome.value,
            cheap_platform=cheap_platform,
            cheap_price=f"{cheap_price:.4f}",
            expensive_price=f"{expensive_price:.4f}",
            spread=f"{spread:.4f}",
        )

        return [Signal(
            signal_type=SignalType.CROSS_PLATFORM_ARB,
            market=market,
            side=Side.BUY,
            outcome=outcome,
            confidence=min(0.98, 0.90 + spread * 2),
            edge=spread,
            limit_price=round(cheap_price, 4),
            meta={
                "arb_type": "directional",
                "buy_platform": cheap_platform,
                "sell_platform": "drift" if cheap_platform == "polymarket" else "polymarket",
                "cheap_price": cheap_price,
                "expensive_price": expensive_price,
                "spread": spread,
                "drift_market_index": pair.drift.market_index,
                "poly_condition_id": pair.polymarket.condition_id,
            },
        )]

    def _emit_cross_lock(
        self,
        pair: CrossPlatformPair,
        leg1_price: float,
        leg2_price: float,
        lock_type: str,
        profit: float,
        now: float,
    ) -> list[Signal]:
        """Emit signals for a cross-platform lock (buy both sides across platforms)."""
        logger.info(
            "CROSS-PLATFORM ARB: lock",
            symbol=pair.symbol,
            bucket=pair.bucket,
            lock_type=lock_type,
            leg1_price=f"{leg1_price:.4f}",
            leg2_price=f"{leg2_price:.4f}",
            total_cost=f"{leg1_price + leg2_price:.4f}",
            profit=f"{profit:.4f}",
        )

        signals = []

        if lock_type == "poly_yes_drift_no":
            # Leg 1: Buy YES on Polymarket
            signals.append(Signal(
                signal_type=SignalType.CROSS_PLATFORM_ARB,
                market=pair.polymarket,
                side=Side.BUY,
                outcome=Outcome.YES,
                confidence=1.0,  # Risk-free lock
                edge=profit / 2,
                limit_price=round(leg1_price, 4),
                meta={
                    "arb_type": "cross_lock",
                    "lock_type": lock_type,
                    "buy_platform": "polymarket",
                    "leg": "yes",
                    "leg_price": leg1_price,
                    "other_leg_price": leg2_price,
                    "total_cost": leg1_price + leg2_price,
                    "profit": profit,
                    "pair_trade": True,
                    "drift_market_index": pair.drift.market_index,
                },
            ))
            # Leg 2: Buy NO on Drift
            signals.append(Signal(
                signal_type=SignalType.CROSS_PLATFORM_ARB,
                market=pair.polymarket,  # Reference market for tracking
                side=Side.BUY,
                outcome=Outcome.NO,
                confidence=1.0,
                edge=profit / 2,
                limit_price=round(leg2_price, 4),
                meta={
                    "arb_type": "cross_lock",
                    "lock_type": lock_type,
                    "buy_platform": "drift",
                    "leg": "no",
                    "leg_price": leg2_price,
                    "other_leg_price": leg1_price,
                    "total_cost": leg1_price + leg2_price,
                    "profit": profit,
                    "pair_trade": True,
                    "drift_market_index": pair.drift.market_index,
                },
            ))
        else:  # drift_yes_poly_no
            # Leg 1: Buy YES on Drift
            signals.append(Signal(
                signal_type=SignalType.CROSS_PLATFORM_ARB,
                market=pair.polymarket,
                side=Side.BUY,
                outcome=Outcome.YES,
                confidence=1.0,
                edge=profit / 2,
                limit_price=round(leg1_price, 4),
                meta={
                    "arb_type": "cross_lock",
                    "lock_type": lock_type,
                    "buy_platform": "drift",
                    "leg": "yes",
                    "leg_price": leg1_price,
                    "other_leg_price": leg2_price,
                    "total_cost": leg1_price + leg2_price,
                    "profit": profit,
                    "pair_trade": True,
                    "drift_market_index": pair.drift.market_index,
                },
            ))
            # Leg 2: Buy NO on Polymarket
            signals.append(Signal(
                signal_type=SignalType.CROSS_PLATFORM_ARB,
                market=pair.polymarket,
                side=Side.BUY,
                outcome=Outcome.NO,
                confidence=1.0,
                edge=profit / 2,
                limit_price=round(leg2_price, 4),
                meta={
                    "arb_type": "cross_lock",
                    "lock_type": lock_type,
                    "buy_platform": "polymarket",
                    "leg": "no",
                    "leg_price": leg2_price,
                    "other_leg_price": leg1_price,
                    "total_cost": leg1_price + leg2_price,
                    "profit": profit,
                    "pair_trade": True,
                    "drift_market_index": pair.drift.market_index,
                },
            ))

        return signals
