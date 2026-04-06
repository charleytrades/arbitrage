"""Drift BET order executor.

Places orders on Drift BET prediction markets.
In paper_trade mode, simulates fills. In live mode, posts real
orders via the Drift Gateway REST API (self-hosted).

Drift BET markets are perp markets with binary YES/NO outcomes:
  - Long (positive amount) = YES
  - Short (negative amount) = NO
  - Price range: 0 to 1 USDC per share
  - Settlement: winning side → $1, losing side → $0

Live trading requires the Drift Gateway running locally:
  docker run -e DRIFT_GATEWAY_KEY=<base58_key> -p 8080:8080 \
    ghcr.io/drift-labs/gateway https://api.mainnet-beta.solana.com

This is the second execution leg for cross-platform arb —
when the strategy identifies a cheaper price on Drift, the
DriftExecutor handles that side while ClobExecutor handles
the Polymarket side.
"""

from __future__ import annotations

import time

import aiohttp

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

# Default gateway URL (self-hosted Drift Gateway)
DRIFT_GATEWAY_URL = "http://localhost:8080"


class DriftExecutor:
    """Handles order placement on Drift BET.

    In paper_trade mode: simulates fills at the quoted price.
    In live mode: posts orders via Drift Gateway REST API.

    The Gateway handles Solana transaction signing and submission.
    See: https://github.com/drift-labs/gateway
    """

    def __init__(self) -> None:
        self.mode = TradingMode(settings.trading_mode)
        self.gateway_url = getattr(settings, "drift_gateway_url", DRIFT_GATEWAY_URL)
        self._session: aiohttp.ClientSession | None = None
        self.open_positions: list[Position] = []
        self.closed_positions: list[Position] = []
        self._order_count = 0

    async def initialize(self) -> None:
        """Set up the Drift Gateway session for live trading."""
        if self.mode == TradingMode.LIVE:
            if not settings.solana_private_key:
                logger.warning("SOLANA_PRIVATE_KEY not set — Drift live trading disabled")
                return
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
            # Verify gateway is reachable
            try:
                async with self._session.get(f"{self.gateway_url}/v2/positions") as resp:
                    if resp.status == 200:
                        logger.info(
                            "Drift Gateway connected",
                            gateway=self.gateway_url,
                            mode="live",
                        )
                    else:
                        logger.warning(
                            "Drift Gateway returned non-200 — check gateway is running",
                            status=resp.status,
                        )
            except aiohttp.ClientError as exc:
                logger.warning(
                    "Drift Gateway unreachable — live orders will fail",
                    gateway=self.gateway_url,
                    error=str(exc),
                )
        else:
            logger.info("Drift executor initialized", mode=self.mode.value)

    async def execute_signal(self, signal: Signal, size: float) -> OrderResult:
        """Execute a Drift-side trade. Routes to live or paper."""
        if self.mode == TradingMode.LIVE:
            return await self._execute_live(signal, size)
        return await self._execute_paper(signal, size)

    async def _execute_live(self, signal: Signal, size: float) -> OrderResult:
        """Place a real order on Drift BET via Gateway REST API.

        Gateway endpoint: POST /v2/orders
        Positive amount = Long (YES), Negative amount = Short (NO)
        """
        if not self._session:
            return OrderResult(success=False, error="Drift session not initialized")

        market_index = signal.meta.get("drift_market_index", 0)

        # Long = YES, Short = NO
        # Positive amount = buying YES, Negative = buying NO
        if signal.outcome == Outcome.YES:
            amount = size  # Positive = Long/YES
        else:
            amount = -size  # Negative = Short/NO

        try:
            url = f"{self.gateway_url}/v2/orders"
            payload = {
                "orders": [{
                    "marketIndex": market_index,
                    "marketType": "perp",
                    "amount": amount,
                    "price": signal.limit_price,
                    "orderType": "limit",
                    "postOnly": True,
                }]
            }

            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()

                if resp.status in (200, 201):
                    # Gateway returns order confirmation
                    order_id = str(data.get("orderId", data.get("ids", [""])[0] if isinstance(data.get("ids"), list) else ""))
                    if not order_id:
                        order_id = f"drift_live_{self._order_count + 1}"

                    result = OrderResult(
                        success=True,
                        order_id=f"drift_{order_id}",
                        filled_size=size,
                        avg_price=signal.limit_price,
                    )
                    self._order_count += 1
                    self._track_position(signal, size, signal.limit_price, result.order_id)

                    await send_trade_alert(
                        action="DRIFT LIVE ORDER",
                        market_slug=f"drift-{market_index}",
                        side=signal.side.value,
                        outcome=signal.outcome.value,
                        size=size,
                        price=signal.limit_price,
                        edge=signal.edge,
                    )
                    logger.info(
                        "Drift live order placed via Gateway",
                        order_id=result.order_id,
                        market_index=market_index,
                        outcome=signal.outcome.value,
                        amount=amount,
                        price=signal.limit_price,
                    )
                    return result
                else:
                    error = str(data)
                    logger.error("Drift Gateway order rejected", error=error, status=resp.status)
                    return OrderResult(success=False, error=error)

        except Exception as exc:
            logger.error("Drift order failed", error=str(exc))
            return OrderResult(success=False, error=str(exc))

    async def _execute_paper(self, signal: Signal, size: float) -> OrderResult:
        """Simulate a Drift fill for paper trading."""
        self._order_count += 1
        fill_price = signal.limit_price if signal.limit_price > 0 else 0.5
        fill_price = float(max(0.01, min(0.99, fill_price)))
        order_id = f"drift_paper_{self._order_count}"

        result = OrderResult(
            success=True,
            order_id=order_id,
            filled_size=size,
            avg_price=fill_price,
        )

        self._track_position(signal, size, fill_price, order_id)

        market_index = signal.meta.get("drift_market_index", 0)
        logger.info(
            "Drift paper order filled",
            order_id=order_id,
            market_index=market_index,
            outcome=signal.outcome.value,
            size=size,
            price=fill_price,
            edge=f"{signal.edge:.4f}",
        )

        await send_trade_alert(
            action="DRIFT PAPER",
            market_slug=f"drift-{market_index}",
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
        pos = Position(
            market=signal.market,
            outcome=signal.outcome,
            side=signal.side,
            size=size,
            entry_price=price,
            order_id=order_id,
        )
        self.open_positions.append(pos)

    def get_open_positions(self) -> list[Position]:
        return [p for p in self.open_positions if p.is_open]

    def close_position(self, position: Position, exit_price: float) -> float:
        if position.side == Side.BUY:
            pnl = (exit_price - position.entry_price) * position.size
        else:
            pnl = (position.entry_price - exit_price) * position.size

        position.pnl = pnl
        position.exit_price = exit_price
        position.exit_ts = time.time()
        position.realized = True
        self.closed_positions.append(position)
        return pnl

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Drift executor stopped")
