"""Historical simulation mode for strategy backtesting.

Replays saved Binance kline data + synthetic Polymarket book snapshots
to evaluate strategy performance without risking capital.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from polymarket_micro_arb.models import OrderResult, Position, Signal, Side, Outcome
from polymarket_micro_arb.utils.logger import logger


@dataclass
class BacktestConfig:
    initial_bankroll: float = 1_000.0
    maker_fee: float = 0.0  # Polymarket is 0 maker fee
    taker_fee: float = 0.002
    slippage_bps: float = 5.0  # 0.05% slippage assumption


@dataclass
class BacktestResult:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    total_fees: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    win_rate: float = 0.0
    equity_curve: list[float] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "win_rate": f"{self.win_rate:.2%}",
            "gross_pnl": f"${self.gross_pnl:.2f}",
            "net_pnl": f"${self.net_pnl:.2f}",
            "max_drawdown": f"{self.max_drawdown:.2%}",
            "sharpe_ratio": f"{self.sharpe_ratio:.3f}",
        }


class Backtester:
    """Simulates order fills against historical data."""

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()
        self.bankroll = self.config.initial_bankroll
        self.positions: list[Position] = []
        self.equity_history: list[float] = [self.bankroll]
        self.peak_equity = self.bankroll

    def simulate_fill(self, signal: Signal, size: float) -> OrderResult:
        """Simulate an order fill with slippage and fees."""
        slippage = signal.edge * (self.config.slippage_bps / 10_000)
        if signal.side == Side.BUY:
            fill_price = signal.market.token_id_yes and (
                signal.edge + slippage
            )
            # Use mid-market estimate for simulation
            fill_price = min(0.99, max(0.01, 0.5 + signal.edge + slippage))
        else:
            fill_price = max(0.01, 0.5 - signal.edge - slippage)

        fee = size * self.config.taker_fee
        cost = size * fill_price + fee

        if cost > self.bankroll:
            return OrderResult(
                success=False,
                error="Insufficient simulated bankroll",
            )

        self.bankroll -= cost

        position = Position(
            market=signal.market,
            outcome=signal.outcome,
            side=signal.side,
            size=size,
            entry_price=fill_price,
        )
        self.positions.append(position)

        return OrderResult(
            success=True,
            order_id=f"sim_{int(time.time()*1000)}",
            filled_size=size,
            avg_price=fill_price,
        )

    def resolve_position(self, position: Position, won: bool) -> float:
        """Resolve a position at market expiry. Returns PnL."""
        if won:
            # Binary market pays $1 per share on YES win
            payout = position.size * 1.0
            pnl = payout - (position.size * position.entry_price)
        else:
            pnl = -(position.size * position.entry_price)

        position.pnl = pnl
        position.exit_price = 1.0 if won else 0.0
        position.exit_ts = time.time()
        self.bankroll += position.size * position.entry_price + pnl

        self.equity_history.append(self.bankroll)
        self.peak_equity = max(self.peak_equity, self.bankroll)

        return pnl

    def compute_results(self) -> BacktestResult:
        """Compute final backtest statistics."""
        closed = [p for p in self.positions if not p.is_open]
        if not closed:
            return BacktestResult(equity_curve=self.equity_history)

        wins = [p for p in closed if p.pnl > 0]
        losses = [p for p in closed if p.pnl <= 0]
        gross_pnl = sum(p.pnl for p in closed)

        # Drawdown from equity curve
        eq = np.array(self.equity_history)
        running_max = np.maximum.accumulate(eq)
        drawdowns = (running_max - eq) / running_max
        max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

        # Simple Sharpe (daily returns proxy – each trade is a "period")
        returns = pd.Series([p.pnl / (p.size * p.entry_price) for p in closed])
        sharpe = (
            float(returns.mean() / returns.std() * np.sqrt(252))
            if len(returns) > 1 and returns.std() > 0
            else 0.0
        )

        total_fees = sum(
            p.size * self.config.taker_fee for p in closed
        )

        result = BacktestResult(
            total_trades=len(closed),
            winning_trades=len(wins),
            losing_trades=len(losses),
            gross_pnl=gross_pnl,
            net_pnl=gross_pnl - total_fees,
            total_fees=total_fees,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            win_rate=len(wins) / len(closed) if closed else 0.0,
            equity_curve=self.equity_history,
        )

        logger.info("Backtest complete", **result.summary())
        return result
