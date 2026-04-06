"""Cross-Outcome Arbitrage Strategy (always-on, risk-free).

For every active bucket: if (Yes ask + No ask) < 0.99, buy both
sides in exact dollar ratio for risk-free profit at settlement.

The only risk is execution: both legs must fill. We mitigate this
with limit orders and conservative sizing.
"""

from __future__ import annotations

import time

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.data.polymarket_ws import PolymarketWSClient
from polymarket_micro_arb.models import (
    MarketInfo,
    Outcome,
    Side,
    Signal,
    SignalType,
)
from polymarket_micro_arb.utils.logger import logger


class CrossOutcomeArbStrategy:
    """Detects YES + NO ask sum < 0.99 for guaranteed profit."""

    def __init__(
        self,
        polymarket_ws: PolymarketWSClient,
        threshold: float | None = None,
        min_profit: float | None = None,
    ) -> None:
        self.polymarket_ws = polymarket_ws
        self.threshold = threshold or settings.cross_outcome_threshold
        self.min_profit = min_profit or settings.min_spread_profit

        # Cooldown per market
        self._last_signal_ts: dict[str, float] = {}
        self._signal_cooldown = 3.0  # Fast re-scan for arb

    def evaluate(self, markets: list[MarketInfo]) -> list[Signal]:
        """Scan all markets for cross-outcome arbitrage opportunities."""
        signals: list[Signal] = []
        now = time.time()

        for market in markets:
            if not market.active or market.seconds_remaining < 10:
                continue

            # Cooldown
            last = self._last_signal_ts.get(market.condition_id, 0)
            if now - last < self._signal_cooldown:
                continue

            # Get best ask for both YES and NO
            _, yes_ask = self.polymarket_ws.get_best_prices(
                market.token_id_yes
            )
            _, no_ask = self.polymarket_ws.get_best_prices(
                market.token_id_no
            )

            # Skip if we don't have real book data yet
            if yes_ask >= 1.0 or no_ask >= 1.0:
                continue
            if yes_ask <= 0.0 or no_ask <= 0.0:
                continue

            total_cost = yes_ask + no_ask
            # Fee is 10% (1000 bps) on each leg's cost
            total_fee = (yes_ask + no_ask) * 0.10
            profit = 1.0 - total_cost - total_fee

            if total_cost < self.threshold and profit > self.min_profit:
                self._last_signal_ts[market.condition_id] = now

                # Get available liquidity to size properly
                yes_book = self.polymarket_ws.get_book(market.token_id_yes)
                no_book = self.polymarket_ws.get_book(market.token_id_no)
                yes_liq = yes_book.best_ask_size if yes_book else 0.0
                no_liq = no_book.best_ask_size if no_book else 0.0

                # Size to the minimum available liquidity on either side
                max_shares = min(yes_liq, no_liq)
                if max_shares <= 0:
                    continue

                logger.info(
                    "CROSS-OUTCOME ARB (fee-adjusted)",
                    market=market.slug,
                    yes_ask=f"{yes_ask:.4f}",
                    no_ask=f"{no_ask:.4f}",
                    total_cost=f"{total_cost:.4f}",
                    fee=f"{total_fee:.4f}",
                    net_profit=f"{profit:.4f}",
                    available_shares=f"{max_shares:.2f}",
                )

                # Emit paired signals – both must execute for arb to work
                # Confidence is 1.0 because this is a mathematical certainty
                pair_id = f"arb_{market.condition_id}_{int(now)}"

                signals.append(
                    Signal(
                        signal_type=SignalType.CROSS_OUTCOME_ARB,
                        market=market,
                        side=Side.BUY,
                        outcome=Outcome.YES,
                        confidence=1.0,
                        edge=profit / 2,
                        limit_price=round(yes_ask, 4),
                        meta={
                            "yes_ask": yes_ask,
                            "no_ask": no_ask,
                            "total_cost": total_cost,
                            "profit": profit,
                            "pair_trade": True,
                            "pair_id": pair_id,
                            "max_shares": max_shares,
                        },
                    )
                )
                signals.append(
                    Signal(
                        signal_type=SignalType.CROSS_OUTCOME_ARB,
                        market=market,
                        side=Side.BUY,
                        outcome=Outcome.NO,
                        confidence=1.0,
                        edge=profit / 2,
                        limit_price=round(no_ask, 4),
                        meta={
                            "yes_ask": yes_ask,
                            "no_ask": no_ask,
                            "total_cost": total_cost,
                            "profit": profit,
                            "pair_trade": True,
                            "pair_id": pair_id,
                            "max_shares": max_shares,
                        },
                    )
                )

        return signals
