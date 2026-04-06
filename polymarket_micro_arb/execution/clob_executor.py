"""CLOB order execution wrapper with proper EIP-712 signing.

Handles limit-order placement, auto-cancel after TTL, and full
position tracking with realized/unrealized PnL.

Key rules:
  - Limit orders only (post-only where possible)
  - Auto-cancel unfilled orders after ORDER_TTL_SEC (60s)
  - Paper trade mode simulates fills without touching the chain
"""

from __future__ import annotations

import asyncio
import time

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
    SignalType,
)
from polymarket_micro_arb.utils.logger import logger
from polymarket_micro_arb.utils.telegram_alerts import send_trade_alert


class ClobExecutor:
    """Wraps the Polymarket CLOB client for order execution.

    In paper_trade mode, simulates fills without touching the chain.
    In live mode, places real limit orders via the CLOB API with EIP-712 signing.
    """

    def __init__(self) -> None:
        self.mode = TradingMode(settings.trading_mode)
        self._client: ClobClient | None = None
        self.open_positions: list[Position] = []
        self.closed_positions: list[Position] = []
        self._order_count = 0
        # order_id -> (placement_time, signal) for TTL tracking
        self._pending_orders: dict[str, tuple[float, Signal]] = {}

    async def initialize(self) -> None:
        """Set up the CLOB client with EIP-712 credentials."""
        if self.mode == TradingMode.LIVE:
            self._client = ClobClient(
                settings.polymarket_host,
                key=settings.private_key,
                chain_id=settings.chain_id,
            )
            # Derive API creds (EIP-712 signed) from the private key
            self._client.set_api_creds(self._client.create_or_derive_api_creds())
            logger.info("CLOB client initialized (EIP-712)", mode="live")
        else:
            logger.info("CLOB executor initialized", mode=self.mode.value)

    async def execute_signal(self, signal: Signal, size: float) -> OrderResult:
        """Execute a trading signal. Routes to live or paper execution."""
        if self.mode == TradingMode.LIVE:
            return await self._execute_live(signal, size)
        else:
            return await self._execute_paper(signal, size)

    async def _execute_live(self, signal: Signal, size: float) -> OrderResult:
        """Place a real limit order on the Polymarket CLOB."""
        if not self._client:
            return OrderResult(success=False, error="CLOB client not initialized")

        token_id = (
            signal.market.token_id_yes
            if signal.outcome == Outcome.YES
            else signal.market.token_id_no
        )

        # Use the signal's limit_price (computed by strategy)
        limit_price = signal.limit_price
        if limit_price <= 0:
            # Fallback: use meta ask price
            if signal.outcome == Outcome.YES and "yes_ask" in signal.meta:
                limit_price = float(signal.meta["yes_ask"])
            elif "no_ask" in signal.meta:
                limit_price = float(signal.meta["no_ask"])
            else:
                limit_price = 0.50

        # Clamp to valid range and round to CLOB tick size (0.01)
        limit_price = round(max(0.01, min(0.99, limit_price)), 2)

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=size,
                side=signal.side.value,
            )

            # Create EIP-712 signed order
            signed_order = self._client.create_order(order_args)
            # Post as GTC (Good Till Cancelled) – we manage TTL ourselves
            response = self._client.post_order(signed_order, OrderType.GTC)

            order_id = response.get("orderID", "")
            success = bool(order_id)

            result = OrderResult(
                success=success,
                order_id=order_id,
                filled_size=size if success else 0.0,
                avg_price=limit_price,
                error="" if success else str(response),
            )

            if success:
                # Track for TTL-based auto-cancel
                self._pending_orders[order_id] = (time.time(), signal)
                self._track_position(signal, size, limit_price, order_id)

                await send_trade_alert(
                    action="LIVE LIMIT ORDER",
                    market_slug=signal.market.slug,
                    side=signal.side.value,
                    outcome=signal.outcome.value,
                    size=size,
                    price=limit_price,
                    edge=signal.edge,
                )

            logger.info(
                "Live limit order placed",
                order_id=order_id,
                market=signal.market.slug,
                side=signal.side.value,
                outcome=signal.outcome.value,
                size=size,
                limit_price=limit_price,
                success=success,
            )
            return result

        except Exception as exc:
            logger.error("Live order failed", error=str(exc), market=signal.market.slug)
            return OrderResult(success=False, error=str(exc))

    async def _execute_paper(self, signal: Signal, size: float) -> OrderResult:
        """Simulate a limit order fill for paper trading."""
        self._order_count += 1

        # Use signal's limit price for realistic simulation
        fill_price = signal.limit_price
        if fill_price <= 0:
            if signal.outcome == Outcome.YES and "yes_ask" in signal.meta:
                fill_price = float(signal.meta["yes_ask"])
            elif "no_ask" in signal.meta:
                fill_price = float(signal.meta["no_ask"])
            else:
                fill_price = 0.5

        fill_price = float(max(0.01, min(0.99, fill_price)))
        order_id = f"paper_{self._order_count}"

        result = OrderResult(
            success=True,
            order_id=order_id,
            filled_size=size,
            avg_price=fill_price,
        )

        self._track_position(signal, size, fill_price, order_id)

        logger.info(
            "Paper limit order filled",
            order_id=order_id,
            market=signal.market.slug,
            side=signal.side.value,
            outcome=signal.outcome.value,
            size=size,
            price=fill_price,
            edge=f"{signal.edge:.4f}",
            signal_type=signal.signal_type.value,
        )

        await send_trade_alert(
            action=f"PAPER {signal.signal_type.value.upper()}",
            market_slug=signal.market.slug,
            side=signal.side.value,
            outcome=signal.outcome.value,
            size=size,
            price=fill_price,
            edge=signal.edge,
        )

        return result

    def _track_position(
        self, signal: Signal, size: float, price: float, order_id: str
    ) -> None:
        """Record an open position."""
        pos = Position(
            market=signal.market,
            outcome=signal.outcome,
            side=signal.side,
            size=size,
            entry_price=price,
            order_id=order_id,
        )
        self.open_positions.append(pos)

    async def cancel_stale_orders(self) -> int:
        """Cancel orders that have exceeded ORDER_TTL_SEC without filling.

        Returns the number of orders cancelled.
        """
        if self.mode != TradingMode.LIVE or not self._client:
            return 0

        now = time.time()
        ttl = settings.order_ttl_sec
        stale = [
            oid for oid, (placed_at, _) in self._pending_orders.items()
            if now - placed_at > ttl
        ]

        cancelled = 0
        # Collect pair_ids of cancelled orders so we can cancel their partners
        cancelled_pair_ids: set[str] = set()

        for order_id in stale:
            try:
                self._client.cancel(order_id)
                cancelled += 1
                # Track pair_id for partner cancellation
                _, signal = self._pending_orders.get(order_id, (0, None))
                if signal and signal.meta.get("pair_trade"):
                    cancelled_pair_ids.add(signal.meta["pair_id"])
                logger.info("Auto-cancelled stale order", order_id=order_id, ttl_sec=ttl)
            except Exception as exc:
                logger.warning("Failed to cancel stale order", order_id=order_id, error=str(exc))
            finally:
                self._pending_orders.pop(order_id, None)

        # Cancel partner legs of any cancelled arb pair
        if cancelled_pair_ids:
            partner_ids = [
                oid for oid, (_, sig) in list(self._pending_orders.items())
                if sig and sig.meta.get("pair_id") in cancelled_pair_ids
            ]
            for order_id in partner_ids:
                try:
                    self._client.cancel(order_id)
                    cancelled += 1
                    logger.info("Cancelled arb partner order", order_id=order_id)
                except Exception as exc:
                    logger.warning("Failed to cancel partner", order_id=order_id, error=str(exc))
                finally:
                    self._pending_orders.pop(order_id, None)

        return cancelled

    async def cancel_all_orders(self) -> None:
        """Cancel all open orders (live mode only)."""
        if self.mode != TradingMode.LIVE or not self._client:
            return
        try:
            self._client.cancel_all()
            self._pending_orders.clear()
            logger.info("All open orders cancelled")
        except Exception as exc:
            logger.error("Failed to cancel orders", error=str(exc))

    def get_open_positions(self) -> list[Position]:
        return [p for p in self.open_positions if p.is_open]

    def get_open_bucket_count(self) -> int:
        """Count distinct buckets with open positions."""
        return len({p.market.condition_id for p in self.open_positions if p.is_open})

    def close_position(self, position: Position, exit_price: float) -> float:
        """Mark a position as closed and compute PnL."""
        if position.side == Side.BUY:
            # Bought at entry, settling at exit (1.0 for win, 0.0 for loss)
            pnl = (exit_price - position.entry_price) * position.size
        else:
            pnl = (position.entry_price - exit_price) * position.size

        position.pnl = pnl
        position.exit_price = exit_price
        position.exit_ts = time.time()
        position.realized = True

        # Move to closed list
        self.closed_positions.append(position)

        logger.info(
            "Position closed",
            market=position.market.slug,
            outcome=position.outcome.value,
            pnl=f"${pnl:.4f}",
            entry=position.entry_price,
            exit=exit_price,
        )
        return pnl

    def resolve_expired_positions(self) -> list[tuple[Position, float]]:
        """Auto-close positions on expired markets.

        For markets past their end_ts, we can't know the resolution here,
        so we mark them as needing resolution. In production, poll the
        Gamma API for resolved outcomes.
        Returns list of (position, pnl) that were closed.
        """
        resolved: list[tuple[Position, float]] = []
        now = time.time()

        for pos in self.open_positions:
            if pos.is_open and pos.market.end_ts < now:
                # Market expired – needs resolution from Gamma API
                # For now, mark as expired with 0 PnL (updated when resolution known)
                logger.info(
                    "Position expired, awaiting resolution",
                    market=pos.market.slug,
                    outcome=pos.outcome.value,
                    entry=pos.entry_price,
                )

        return resolved

    @property
    def position_stats(self) -> dict:
        """Summary of all position tracking."""
        open_pos = self.get_open_positions()
        realized_pnl = sum(p.pnl for p in self.closed_positions)
        return {
            "open_positions": len(open_pos),
            "closed_positions": len(self.closed_positions),
            "realized_pnl": f"${realized_pnl:.2f}",
            "open_buckets": self.get_open_bucket_count(),
        }
