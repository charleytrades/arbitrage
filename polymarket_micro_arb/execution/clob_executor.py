"""CLOB order execution wrapper around py-clob-client.

Handles order placement, cancellation, and position tracking via
the AsyncClobClient. Supports paper_trade mode for dry runs.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import TradingMode
from polymarket_micro_arb.models import (
    OrderResult,
    Outcome,
    Position,
    Side,
    Signal,
)
from polymarket_micro_arb.utils.logger import logger
from polymarket_micro_arb.utils.telegram_alerts import send_trade_alert


class ClobExecutor:
    """Wraps the Polymarket CLOB client for order execution.

    In paper_trade mode, simulates fills without touching the chain.
    In live mode, places real orders via the CLOB API.
    """

    def __init__(self) -> None:
        self.mode = TradingMode(settings.trading_mode)
        self._client: ClobClient | None = None
        self.open_positions: list[Position] = []
        self._order_count = 0

    async def initialize(self) -> None:
        """Set up the CLOB client with credentials."""
        if self.mode == TradingMode.LIVE:
            self._client = ClobClient(
                settings.polymarket_host,
                key=settings.private_key,
                chain_id=settings.chain_id,
            )
            # Derive API credentials from the private key
            self._client.set_api_creds(self._client.create_or_derive_api_creds())
            logger.info("CLOB client initialized", mode="live")
        else:
            logger.info("CLOB executor initialized", mode=self.mode.value)

    async def execute_signal(self, signal: Signal, size: float) -> OrderResult:
        """Execute a trading signal. Routes to live or paper execution."""
        if self.mode == TradingMode.LIVE:
            return await self._execute_live(signal, size)
        else:
            return await self._execute_paper(signal, size)

    async def _execute_live(self, signal: Signal, size: float) -> OrderResult:
        """Place a real order on the Polymarket CLOB."""
        if not self._client:
            return OrderResult(success=False, error="CLOB client not initialized")

        token_id = (
            signal.market.token_id_yes
            if signal.outcome == Outcome.YES
            else signal.market.token_id_no
        )

        try:
            # Build the order
            order_args = OrderArgs(
                token_id=token_id,
                price=round(signal.edge + 0.5, 2),  # Limit price
                size=size,
                side=signal.side.value,
            )

            # Create and sign the order
            signed_order = self._client.create_order(order_args)
            # Post to CLOB
            response = self._client.post_order(signed_order, OrderType.GTC)

            order_id = response.get("orderID", "")
            success = bool(order_id)

            result = OrderResult(
                success=success,
                order_id=order_id,
                filled_size=size if success else 0.0,
                avg_price=order_args.price,
                error="" if success else str(response),
            )

            if success:
                self._track_position(signal, size, order_args.price)
                await send_trade_alert(
                    action="LIVE ORDER",
                    market_slug=signal.market.slug,
                    side=signal.side.value,
                    outcome=signal.outcome.value,
                    size=size,
                    price=order_args.price,
                    edge=signal.edge,
                )

            logger.info(
                "Live order placed",
                order_id=order_id,
                market=signal.market.slug,
                side=signal.side.value,
                outcome=signal.outcome.value,
                size=size,
                success=success,
            )
            return result

        except Exception as exc:
            logger.error("Live order failed", error=str(exc), market=signal.market.slug)
            return OrderResult(success=False, error=str(exc))

    async def _execute_paper(self, signal: Signal, size: float) -> OrderResult:
        """Simulate an order fill for paper trading."""
        self._order_count += 1

        # Simulate fill at the signal's implied price
        if signal.outcome == Outcome.YES:
            _, fill_price = signal.meta.get("yes_ask", 0.5), signal.meta.get(
                "yes_ask", 0.5
            )
        else:
            _, fill_price = signal.meta.get("no_ask", 0.5), signal.meta.get(
                "no_ask", 0.5
            )

        # Ensure fill_price is a float
        if not isinstance(fill_price, (int, float)):
            fill_price = 0.5

        result = OrderResult(
            success=True,
            order_id=f"paper_{self._order_count}",
            filled_size=size,
            avg_price=float(fill_price),
        )

        self._track_position(signal, size, float(fill_price))

        logger.info(
            "Paper order filled",
            order_id=result.order_id,
            market=signal.market.slug,
            side=signal.side.value,
            outcome=signal.outcome.value,
            size=size,
            price=float(fill_price),
            edge=signal.edge,
        )

        await send_trade_alert(
            action="PAPER TRADE",
            market_slug=signal.market.slug,
            side=signal.side.value,
            outcome=signal.outcome.value,
            size=size,
            price=float(fill_price),
            edge=signal.edge,
        )

        return result

    def _track_position(
        self, signal: Signal, size: float, price: float
    ) -> None:
        """Record an open position."""
        pos = Position(
            market=signal.market,
            outcome=signal.outcome,
            side=signal.side,
            size=size,
            entry_price=price,
        )
        self.open_positions.append(pos)

    async def cancel_all_orders(self) -> None:
        """Cancel all open orders (live mode only)."""
        if self.mode != TradingMode.LIVE or not self._client:
            return

        try:
            self._client.cancel_all()
            logger.info("All open orders cancelled")
        except Exception as exc:
            logger.error("Failed to cancel orders", error=str(exc))

    def get_open_positions(self) -> list[Position]:
        """Return currently open positions."""
        return [p for p in self.open_positions if p.is_open]

    def close_position(self, position: Position, exit_price: float) -> float:
        """Mark a position as closed and compute PnL."""
        if position.side == Side.BUY:
            pnl = (exit_price - position.entry_price) * position.size
        else:
            pnl = (position.entry_price - exit_price) * position.size

        position.pnl = pnl
        position.exit_price = exit_price
        position.exit_ts = time.time()

        logger.info(
            "Position closed",
            market=position.market.slug,
            pnl=f"{pnl:.4f}",
            entry=position.entry_price,
            exit=exit_price,
        )
        return pnl
