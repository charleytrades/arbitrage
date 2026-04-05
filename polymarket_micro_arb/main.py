"""Main entry point for the Polymarket Crypto Micro-Arbitrage Bot.

Orchestrates all components:
  1. Discover active micro-markets via Gamma API
  2. Stream Binance spot prices via WebSocket
  3. Stream Polymarket CLOB book data via WebSocket
  4. Run momentum-latency and cross-outcome-arb strategies
  5. Execute trades through the CLOB executor
  6. Enforce risk limits via the risk engine

Supports three modes: backtest, paper_trade, live.
Graceful shutdown on SIGTERM/SIGINT.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.constants import (
    MARKET_REFRESH_INTERVAL_SEC,
    TradingMode,
)
from polymarket_micro_arb.data.binance_ws import BinanceWSClient
from polymarket_micro_arb.data.gamma_client import GammaClient
from polymarket_micro_arb.data.polymarket_ws import PolymarketWSClient
from polymarket_micro_arb.execution.clob_executor import ClobExecutor
from polymarket_micro_arb.models import BinanceTick, MarketInfo, Signal
from polymarket_micro_arb.risk.risk_engine import RiskEngine
from polymarket_micro_arb.strategy.cross_outcome_arb import CrossOutcomeArbStrategy
from polymarket_micro_arb.strategy.momentum_latency import MomentumLatencyStrategy
from polymarket_micro_arb.utils.backtester import Backtester, BacktestConfig
from polymarket_micro_arb.utils.logger import logger
from polymarket_micro_arb.utils.telegram_alerts import send_alert


class Bot:
    """Top-level bot orchestrator."""

    def __init__(self) -> None:
        self.mode = TradingMode(settings.trading_mode)
        self._shutdown_event = asyncio.Event()
        self._markets: list[MarketInfo] = []

        # ── Data sources ────────────────────────────────────────────
        self._tick_queue: asyncio.Queue[BinanceTick] = asyncio.Queue(maxsize=5000)
        self._gamma = GammaClient()
        self._binance_ws = BinanceWSClient(self._tick_queue)
        self._polymarket_ws = PolymarketWSClient()

        # ── Strategies ──────────────────────────────────────────────
        self._momentum = MomentumLatencyStrategy(self._polymarket_ws)
        self._cross_arb = CrossOutcomeArbStrategy(self._polymarket_ws)

        # ── Execution & risk ────────────────────────────────────────
        self._executor = ClobExecutor()
        self._risk = RiskEngine(initial_bankroll=1_000.0)

        # ── Backtest (only used in backtest mode) ───────────────────
        self._backtester: Backtester | None = None

    async def run(self) -> None:
        """Main entry point – sets up signal handlers and starts all tasks."""
        self._install_signal_handlers()

        logger.info(
            "Bot starting",
            mode=self.mode.value,
            symbols=settings.symbol_list,
        )
        await send_alert(f"Bot starting in <b>{self.mode.value}</b> mode")

        if self.mode == TradingMode.BACKTEST:
            await self._run_backtest()
            return

        # Initialize executor
        await self._executor.initialize()

        # Discover initial markets
        self._markets = await self._gamma.discover_current_markets()
        if not self._markets:
            logger.warning("No markets discovered – will retry in market refresh loop")

        # Launch concurrent tasks
        tasks = [
            asyncio.create_task(self._binance_ws.start(), name="binance_ws"),
            asyncio.create_task(
                self._polymarket_ws.start(self._markets), name="polymarket_ws"
            ),
            asyncio.create_task(self._strategy_loop(), name="strategy_loop"),
            asyncio.create_task(self._market_refresh_loop(), name="market_refresh"),
        ]

        # Wait for shutdown signal
        await self._shutdown_event.wait()

        # Graceful shutdown
        logger.info("Shutting down...")
        await self._shutdown(tasks)

    async def _strategy_loop(self) -> None:
        """Core loop: consume Binance ticks, evaluate strategies, execute."""
        logger.info("Strategy loop started")

        while not self._shutdown_event.is_set():
            try:
                # Drain tick queue (non-blocking batch)
                ticks_processed = 0
                while not self._tick_queue.empty() and ticks_processed < 100:
                    try:
                        tick = self._tick_queue.get_nowait()
                        self._momentum.on_tick(tick)
                        ticks_processed += 1
                    except asyncio.QueueEmpty:
                        break

                if not self._markets:
                    await asyncio.sleep(1.0)
                    continue

                # Evaluate strategies
                signals: list[Signal] = []
                signals.extend(self._momentum.evaluate(self._markets))
                signals.extend(self._cross_arb.evaluate(self._markets))

                # Execute signals through risk engine
                for sig in signals:
                    allowed, size = self._risk.check_risk(sig)
                    if allowed and size > 0:
                        result = await self._executor.execute_signal(sig, size)
                        if result.success:
                            logger.info(
                                "Signal executed",
                                signal_type=sig.signal_type.value,
                                market=sig.market.slug,
                                size=f"${size:.2f}",
                                order_id=result.order_id,
                            )

                # Tight loop with small sleep to avoid CPU spin
                await asyncio.sleep(0.05)

            except Exception as exc:
                logger.error("Strategy loop error", error=str(exc))
                await asyncio.sleep(1.0)

    async def _market_refresh_loop(self) -> None:
        """Periodically refresh active markets from Gamma."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(MARKET_REFRESH_INTERVAL_SEC)
                new_markets = await self._gamma.discover_current_markets()

                if new_markets:
                    # Merge: keep existing, add new
                    existing_ids = {m.condition_id for m in self._markets}
                    added = [
                        m for m in new_markets if m.condition_id not in existing_ids
                    ]

                    # Mark expired markets as inactive
                    now = time.time()
                    for m in self._markets:
                        if m.end_ts < now:
                            m.active = False

                    # Remove inactive, add new
                    self._markets = [m for m in self._markets if m.active]
                    self._markets.extend(added)

                    if added:
                        # Subscribe to new markets on Polymarket WS
                        await self._polymarket_ws.update_subscriptions(added)
                        logger.info(
                            "Markets refreshed",
                            active=len(self._markets),
                            new=len(added),
                        )

            except Exception as exc:
                logger.error("Market refresh error", error=str(exc))

    async def _run_backtest(self) -> None:
        """Run historical backtest simulation."""
        logger.info("Starting backtest mode")
        self._backtester = Backtester(BacktestConfig())

        # In backtest mode, we'd normally load historical data from CSV/DB.
        # This is a skeleton – plug in your data source here.
        logger.warning(
            "Backtest mode requires historical data. "
            "Provide Binance kline CSVs in data/ directory. "
            "See backtester.py for the simulation API."
        )

        # Example: if you had historical signals, you'd loop through them:
        # for signal in historical_signals:
        #     result = self._backtester.simulate_fill(signal, size)
        #     ...resolve at expiry...
        #     self._backtester.resolve_position(position, won=True/False)

        result = self._backtester.compute_results()
        logger.info("Backtest results", **result.summary())
        await send_alert(
            f"Backtest complete\n<pre>{result.summary()}</pre>"
        )

    def _install_signal_handlers(self) -> None:
        """Register SIGTERM/SIGINT handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal, sig)

    def _handle_signal(self, sig: signal.Signals) -> None:
        logger.info("Received signal", signal=sig.name)
        self._shutdown_event.set()

    async def _shutdown(self, tasks: list[asyncio.Task]) -> None:
        """Gracefully shut down all components."""
        # Cancel orders first
        await self._executor.cancel_all_orders()

        # Stop data feeds
        await self._binance_ws.stop()
        await self._polymarket_ws.stop()
        await self._gamma.close()

        # Cancel remaining tasks
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        # Final stats
        logger.info("Final risk stats", **self._risk.stats)
        await send_alert(
            f"Bot shut down.\n<pre>{self._risk.stats}</pre>"
        )
        logger.info("Shutdown complete")


def main() -> None:
    """CLI entry point."""
    bot = Bot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as exc:
        logger.critical("Fatal error", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
