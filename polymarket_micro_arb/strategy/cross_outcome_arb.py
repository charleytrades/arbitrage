"""Cross-Outcome Arbitrage Strategy.

On a binary market, YES + NO must sum to $1.00 at settlement.
If we can buy YES at ask_y and NO at ask_n where ask_y + ask_n < 1.0,
we lock in a risk-free profit of (1.0 - ask_y - ask_n) per dollar.

This is "free money" arbitrage – the only risk is execution slippage
and the spread not being fillable at the quoted prices.
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
    """Detects YES + NO ask sum < threshold (typically < 0.99)."""

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
        self._signal_cooldown = 5.0  # Seconds between arb signals

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

            total_cost = yes_ask + no_ask
            profit = 1.0 - total_cost  # Guaranteed profit per $1 of each

            if total_cost < self.threshold and profit > self.min_profit:
                self._last_signal_ts[market.condition_id] = now

                logger.info(
                    "Cross-outcome arb detected",
                    market=market.slug,
                    yes_ask=f"{yes_ask:.4f}",
                    no_ask=f"{no_ask:.4f}",
                    total_cost=f"{total_cost:.4f}",
                    profit=f"{profit:.4f}",
                )

                # Emit TWO signals – buy both YES and NO
                signals.append(
                    Signal(
                        signal_type=SignalType.CROSS_OUTCOME_ARB,
                        market=market,
                        side=Side.BUY,
                        outcome=Outcome.YES,
                        confidence=min(1.0, profit / self.min_profit),
                        edge=profit / 2,  # Split edge attribution
                        meta={
                            "yes_ask": yes_ask,
                            "no_ask": no_ask,
                            "total_cost": total_cost,
                            "profit": profit,
                            "pair_trade": True,
                        },
                    )
                )
                signals.append(
                    Signal(
                        signal_type=SignalType.CROSS_OUTCOME_ARB,
                        market=market,
                        side=Side.BUY,
                        outcome=Outcome.NO,
                        confidence=min(1.0, profit / self.min_profit),
                        edge=profit / 2,
                        meta={
                            "yes_ask": yes_ask,
                            "no_ask": no_ask,
                            "total_cost": total_cost,
                            "profit": profit,
                            "pair_trade": True,
                        },
                    )
                )

        return signals
