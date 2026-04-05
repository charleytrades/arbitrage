"""Risk management engine.

Implements:
  - Kelly criterion position sizing
  - Daily drawdown kill-switch
  - Consecutive loss pause
  - Per-trade max size enforcement
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.models import Signal
from polymarket_micro_arb.utils.logger import logger
from polymarket_micro_arb.utils.telegram_alerts import send_risk_alert


@dataclass
class RiskState:
    """Mutable risk tracking state."""

    bankroll: float = 1_000.0
    daily_pnl: float = 0.0
    consecutive_losses: int = 0
    total_trades: int = 0
    winning_trades: int = 0
    daily_reset_ts: float = field(default_factory=time.time)
    paused: bool = False
    pause_reason: str = ""


class RiskEngine:
    """Evaluates whether a signal should be traded and at what size."""

    def __init__(self, initial_bankroll: float = 1_000.0) -> None:
        self.state = RiskState(bankroll=initial_bankroll)

    def check_risk(self, signal: Signal) -> tuple[bool, float]:
        """Decide whether to trade a signal and compute position size.

        Returns:
            (allowed, size) – if not allowed, size is 0.
        """
        self._maybe_reset_daily()

        # ── Kill switch: paused ─────────────────────────────────────
        if self.state.paused:
            logger.debug("Risk engine paused", reason=self.state.pause_reason)
            return False, 0.0

        # ── Consecutive loss check ──────────────────────────────────
        if self.state.consecutive_losses >= settings.max_consecutive_losses:
            self.state.paused = True
            self.state.pause_reason = (
                f"Max consecutive losses ({settings.max_consecutive_losses}) hit"
            )
            logger.warning("RISK: Pausing bot", reason=self.state.pause_reason)
            _fire_risk_alert(self.state.pause_reason)
            return False, 0.0

        # ── Daily drawdown check ────────────────────────────────────
        max_daily_loss = self.state.bankroll * (settings.max_daily_loss_percent / 100)
        if self.state.daily_pnl < -max_daily_loss:
            self.state.paused = True
            self.state.pause_reason = (
                f"Daily loss limit (${max_daily_loss:.2f}) breached"
            )
            logger.warning("RISK: Daily loss limit hit", reason=self.state.pause_reason)
            _fire_risk_alert(self.state.pause_reason)
            return False, 0.0

        # ── Kelly sizing ────────────────────────────────────────────
        size = self._kelly_size(signal)
        if size < settings.min_order_size:
            logger.debug(
                "Kelly size below minimum",
                kelly_size=f"{size:.2f}",
                min_size=settings.min_order_size,
            )
            return False, 0.0

        return True, size

    def _kelly_size(self, signal: Signal) -> float:
        """Compute position size using fractional Kelly criterion.

        Kelly f* = (p * b - q) / b
        where p = win probability (confidence), b = odds, q = 1-p.

        We use fractional Kelly (settings.kelly_fraction) to reduce
        variance at the cost of slightly lower expected growth.
        """
        p = signal.confidence
        q = 1.0 - p

        # For binary markets, the payout on a correct bet is:
        # buy at `ask`, receive $1 → odds = (1 - ask) / ask
        ask_price = max(0.01, 0.5)  # Simplified; real price from signal.meta
        if "yes_ask" in signal.meta:
            ask_price = float(signal.meta["yes_ask"])
        elif "no_ask" in signal.meta:
            ask_price = float(signal.meta["no_ask"])

        b = (1.0 - ask_price) / ask_price if ask_price > 0 else 0.0

        if b <= 0:
            return 0.0

        kelly_f = (p * b - q) / b
        kelly_f = max(0.0, kelly_f)  # Never bet negative Kelly

        # Apply fractional Kelly and bankroll percentage cap
        fraction = kelly_f * settings.kelly_fraction
        max_pct = settings.max_bankroll_percent / 100.0

        size = self.state.bankroll * min(fraction, max_pct)
        size = min(size, settings.max_order_size)
        size = max(0.0, size)

        logger.debug(
            "Kelly sizing",
            confidence=f"{p:.3f}",
            odds=f"{b:.3f}",
            raw_kelly=f"{kelly_f:.4f}",
            fractional=f"{fraction:.4f}",
            size=f"${size:.2f}",
        )

        return round(size, 2)

    def record_trade(self, pnl: float) -> None:
        """Update risk state after a trade resolves."""
        self.state.daily_pnl += pnl
        self.state.total_trades += 1

        if pnl > 0:
            self.state.winning_trades += 1
            self.state.consecutive_losses = 0
        else:
            self.state.consecutive_losses += 1

        self.state.bankroll += pnl

        logger.info(
            "Trade recorded",
            pnl=f"{pnl:.4f}",
            daily_pnl=f"{self.state.daily_pnl:.4f}",
            bankroll=f"{self.state.bankroll:.2f}",
            consecutive_losses=self.state.consecutive_losses,
        )

    def _maybe_reset_daily(self) -> None:
        """Reset daily PnL counter at midnight UTC (approximation)."""
        now = time.time()
        # 86400 seconds in a day
        if now - self.state.daily_reset_ts > 86400:
            logger.info(
                "Daily risk reset",
                previous_daily_pnl=f"{self.state.daily_pnl:.4f}",
            )
            self.state.daily_pnl = 0.0
            self.state.daily_reset_ts = now
            # Unpause if paused for daily loss (but not for consecutive losses)
            if "Daily loss" in self.state.pause_reason:
                self.state.paused = False
                self.state.pause_reason = ""

    def force_unpause(self) -> None:
        """Manual override to unpause the bot."""
        self.state.paused = False
        self.state.pause_reason = ""
        self.state.consecutive_losses = 0
        logger.info("Risk engine manually unpaused")

    @property
    def stats(self) -> dict:
        return {
            "bankroll": f"${self.state.bankroll:.2f}",
            "daily_pnl": f"${self.state.daily_pnl:.2f}",
            "total_trades": self.state.total_trades,
            "win_rate": (
                f"{self.state.winning_trades / self.state.total_trades:.1%}"
                if self.state.total_trades > 0
                else "N/A"
            ),
            "consecutive_losses": self.state.consecutive_losses,
            "paused": self.state.paused,
        }


def _fire_risk_alert(reason: str) -> None:
    """Fire-and-forget risk alert (runs in background)."""
    import asyncio

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_risk_alert(reason))
    except RuntimeError:
        pass  # No event loop – skip alert
