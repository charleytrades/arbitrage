"""Historical simulation mode for strategy backtesting.

Replays Binance kline CSV data + simulated Polymarket book snapshots.
Evaluates strategy P&L with realistic slippage, fees, and risk limits.

Data format (CSV):
  timestamp_ms,symbol,open,high,low,close,volume
  1700000100000,BTCUSDT,37500.0,37550.0,37480.0,37520.0,1.5
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from polymarket_micro_arb.models import (
    BinanceTick,
    MarketInfo,
    OrderResult,
    Position,
    Signal,
    Side,
    Outcome,
    SignalType,
)
from polymarket_micro_arb.risk.risk_engine import RiskEngine
from polymarket_micro_arb.utils.logger import logger


@dataclass
class BacktestConfig:
    initial_bankroll: float = 1_000.0
    maker_fee: float = 0.0  # Polymarket is 0 maker fee
    taker_fee: float = 0.002
    slippage_bps: float = 5.0  # 0.05% slippage
    data_dir: str = "data/backtest"  # Directory for CSV files


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
    avg_trade_pnl: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    profit_factor: float = 0.0
    equity_curve: list[float] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "win_rate": f"{self.win_rate:.2%}",
            "gross_pnl": f"${self.gross_pnl:.2f}",
            "net_pnl": f"${self.net_pnl:.2f}",
            "max_drawdown": f"{self.max_drawdown:.2%}",
            "sharpe_ratio": f"{self.sharpe_ratio:.3f}",
            "avg_trade_pnl": f"${self.avg_trade_pnl:.4f}",
            "best_trade": f"${self.best_trade:.4f}",
            "worst_trade": f"${self.worst_trade:.4f}",
            "profit_factor": f"{self.profit_factor:.2f}",
        }


class Backtester:
    """Replays historical data and simulates order fills with full risk engine."""

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()
        self.bankroll = self.config.initial_bankroll
        self.positions: list[Position] = []
        self.equity_history: list[float] = [self.bankroll]
        self.peak_equity = self.bankroll
        self._risk = RiskEngine(initial_bankroll=self.config.initial_bankroll)

    def load_kline_data(self, filepath: str) -> pd.DataFrame:
        """Load Binance kline CSV data for replay.

        Expected columns: timestamp_ms,symbol,open,high,low,close,volume
        """
        path = Path(filepath)
        if not path.exists():
            logger.warning("Backtest data file not found", path=str(path))
            return pd.DataFrame()

        df = pd.read_csv(
            path,
            dtype={
                "timestamp_ms": int,
                "symbol": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            },
        )
        df = df.sort_values("timestamp_ms").reset_index(drop=True)
        logger.info("Loaded backtest data", rows=len(df), file=str(path))
        return df

    def simulate_fill(
        self, signal: Signal, size: float, sim_time: float = 0.0
    ) -> OrderResult:
        """Simulate a limit order fill with slippage and fees.

        Uses the signal's limit_price and applies realistic slippage.
        """
        # Apply slippage to limit price
        slippage_pct = self.config.slippage_bps / 10_000
        base_price = signal.limit_price if signal.limit_price > 0 else 0.5

        if signal.side == Side.BUY:
            fill_price = min(0.99, base_price * (1 + slippage_pct))
        else:
            fill_price = max(0.01, base_price * (1 - slippage_pct))

        fee = size * self.config.taker_fee
        cost = size * fill_price + fee

        if cost > self.bankroll:
            return OrderResult(
                success=False,
                error=f"Insufficient bankroll: need ${cost:.2f}, have ${self.bankroll:.2f}",
            )

        self.bankroll -= cost

        position = Position(
            market=signal.market,
            outcome=signal.outcome,
            side=signal.side,
            size=size,
            entry_price=fill_price,
            entry_ts=sim_time or time.time(),
            order_id=f"sim_{len(self.positions)}",
        )
        self.positions.append(position)

        return OrderResult(
            success=True,
            order_id=position.order_id,
            filled_size=size,
            avg_price=fill_price,
        )

    def resolve_position(
        self, position: Position, won: bool, sim_time: float = 0.0
    ) -> float:
        """Resolve a position at market expiry.

        Binary market: pays $1 per share on win, $0 on loss.
        """
        if won:
            payout = position.size * 1.0
            pnl = payout - (position.size * position.entry_price)
        else:
            pnl = -(position.size * position.entry_price)

        fee = position.size * self.config.taker_fee
        pnl -= fee

        position.pnl = pnl
        position.exit_price = 1.0 if won else 0.0
        position.exit_ts = sim_time or time.time()
        position.realized = True

        # Return cost + pnl to bankroll
        self.bankroll += position.size * position.entry_price + pnl + fee

        self.equity_history.append(self.bankroll)
        self.peak_equity = max(self.peak_equity, self.bankroll)

        # Update risk engine
        self._risk.record_trade(pnl)

        return pnl

    def run_replay(self, df: pd.DataFrame, bucket_duration_sec: int = 300) -> BacktestResult:
        """Replay kline data and simulate the momentum strategy.

        This is a simplified replay that:
        1. Groups data into bucket windows
        2. Checks if price moved >=0.35% in first 15-45s of each bucket
        3. Simulates a buy on the underpriced side
        4. Resolves at bucket end based on whether the move held
        """
        if df.empty:
            return self.compute_results()

        # Group into bucket windows
        df["bucket_ts"] = (df["timestamp_ms"] // (bucket_duration_sec * 1000)) * (
            bucket_duration_sec * 1000
        )

        for bucket_ts, group in df.groupby("bucket_ts"):
            if len(group) < 5:
                continue

            symbol = group.iloc[0]["symbol"]
            rows = group.sort_values("timestamp_ms")

            # Get open and close of bucket
            bucket_open_price = rows.iloc[0]["open"]
            bucket_close_price = rows.iloc[-1]["close"]

            # Simulate: check price at ~30s mark (early in bucket)
            early_cutoff = int(bucket_ts) + 30_000  # 30s into bucket
            early_rows = rows[rows["timestamp_ms"] <= early_cutoff]
            if early_rows.empty:
                continue

            early_price = early_rows.iloc[-1]["close"]
            early_move = (early_price - bucket_open_price) / bucket_open_price

            # Check if move exceeds threshold (0.35%)
            threshold = 0.0035
            if abs(early_move) < threshold:
                continue

            # Determine direction and resolution
            going_up = early_move > 0
            bucket_resolved_up = bucket_close_price > bucket_open_price

            # Create synthetic market and signal
            bucket_start = int(bucket_ts) // 1000
            market = MarketInfo(
                condition_id=f"sim_{symbol}_{bucket_start}",
                question=f"Will {symbol} go up in this bucket?",
                slug=f"{symbol.lower()}-sim-{bucket_start}",
                token_id_yes=f"yes_{bucket_start}",
                token_id_no=f"no_{bucket_start}",
                symbol=symbol,
                bucket="5m" if bucket_duration_sec == 300 else "15m",
                start_ts=bucket_start,
                end_ts=bucket_start + bucket_duration_sec,
            )

            # Simulate Polymarket lag: assume ask is still at ~0.50
            if going_up:
                outcome = Outcome.YES
                limit_price = 0.52  # Slight premium over 0.50
                won = bucket_resolved_up
            else:
                outcome = Outcome.NO
                limit_price = 0.52
                won = not bucket_resolved_up

            signal = Signal(
                signal_type=SignalType.MOMENTUM_LATENCY,
                market=market,
                side=Side.BUY,
                outcome=outcome,
                confidence=min(0.95, 0.85 + abs(early_move) / threshold * 0.04),
                edge=abs(early_move),
                limit_price=limit_price,
                meta={"pct_change": early_move, "yes_ask": limit_price},
            )

            # Check risk before trading
            allowed, size = self._risk.check_risk(signal)
            if not allowed or size <= 0:
                continue

            # Execute
            result = self.simulate_fill(signal, size, sim_time=bucket_start)
            if result.success:
                pos = self.positions[-1]
                self.resolve_position(
                    pos, won=won, sim_time=bucket_start + bucket_duration_sec
                )

        return self.compute_results()

    def compute_results(self) -> BacktestResult:
        """Compute final backtest statistics."""
        closed = [p for p in self.positions if not p.is_open]
        if not closed:
            return BacktestResult(equity_curve=self.equity_history)

        wins = [p for p in closed if p.pnl > 0]
        losses = [p for p in closed if p.pnl <= 0]
        pnls = [p.pnl for p in closed]
        gross_pnl = sum(pnls)

        # Drawdown from equity curve
        eq = np.array(self.equity_history)
        running_max = np.maximum.accumulate(eq)
        drawdowns = (running_max - eq) / np.where(running_max > 0, running_max, 1)
        max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

        # Sharpe ratio (per-trade returns annualized)
        returns = pd.Series([p.pnl / (p.size * p.entry_price) for p in closed])
        sharpe = (
            float(returns.mean() / returns.std() * np.sqrt(252))
            if len(returns) > 1 and returns.std() > 0
            else 0.0
        )

        total_fees = sum(p.size * self.config.taker_fee for p in closed)

        # Profit factor
        gross_wins = sum(p.pnl for p in wins) if wins else 0.0
        gross_losses = abs(sum(p.pnl for p in losses)) if losses else 0.0
        profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

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
            avg_trade_pnl=gross_pnl / len(closed),
            best_trade=max(pnls),
            worst_trade=min(pnls),
            profit_factor=profit_factor,
            equity_curve=self.equity_history,
        )

        logger.info("Backtest complete", **result.summary())
        return result
