"""Risk management engine (hardened, non-negotiable rules).

Implements:
  - Fractional Kelly (0.25x default)
  - Max 4% bankroll per trade
  - Daily drawdown kill-switch: -8% → full pause 24h
  - Max 3 consecutive losses → 30min cooldown
  - Hard position limit: max 8 concurrent buckets
  - Auto-adjusts sizing based on current bankroll
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.models import Position, Signal
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
    losing_trades: int = 0
    total_pnl: float = 0.0
    daily_reset_ts: float = field(default_factory=time.time)
    paused: bool = False
    pause_reason: str = ""
    pause_until_ts: float = 0.0  # Unix timestamp when pause expires
    peak_bankroll: float = 1_000.0


class RiskEngine:
    """Evaluates whether a signal should be traded and at what size.

    Non-negotiable rules enforced here – no overrides except force_unpause().
    """

    def __init__(self, initial_bankroll: float = 1_000.0) -> None:
        self.state = RiskState(
            bankroll=initial_bankroll,
            peak_bankroll=initial_bankroll,
        )

    def check_risk(
        self,
        signal: Signal,
        open_positions: list[Position] | None = None,
    ) -> tuple[bool, float]:
        """Decide whether to trade a signal and compute position size.

        Returns:
            (allowed, size) – if not allowed, size is 0.
        """
        self._maybe_reset_daily()
        self._maybe_unpause_cooldown()

        # ── Kill switch: paused ─────────────────────────────────────
        if self.state.paused:
            remaining = max(0, self.state.pause_until_ts - time.time())
            logger.debug(
                "Risk engine paused",
                reason=self.state.pause_reason,
                remaining_sec=f"{remaining:.0f}",
            )
            return False, 0.0

        # ── Consecutive loss check → 30min cooldown ─────────────────
        if self.state.consecutive_losses >= settings.max_consecutive_losses:
            cooldown = settings.consecutive_loss_cooldown_sec
            self.state.paused = True
            self.state.pause_until_ts = time.time() + cooldown
            self.state.pause_reason = (
                f"Max consecutive losses ({settings.max_consecutive_losses}) "
                f"→ {cooldown}s cooldown"
            )
            logger.warning("RISK: Consecutive loss cooldown", reason=self.state.pause_reason)
            _fire_risk_alert(self.state.pause_reason)
            return False, 0.0

        # ── Daily drawdown check → 24h pause ────────────────────────
        max_daily_loss = self.state.bankroll * (settings.max_daily_loss_percent / 100)
        if self.state.daily_pnl < -max_daily_loss:
            self.state.paused = True
            self.state.pause_until_ts = time.time() + 86400  # 24 hours
            self.state.pause_reason = (
                f"Daily loss limit -${max_daily_loss:.2f} breached "
                f"(actual: ${self.state.daily_pnl:.2f}) → 24h pause"
            )
            logger.warning("RISK: Daily drawdown kill-switch", reason=self.state.pause_reason)
            _fire_risk_alert(self.state.pause_reason)
            return False, 0.0

        # ── Concurrent bucket limit ─────────────────────────────────
        if open_positions is not None:
            active_buckets = {p.market.condition_id for p in open_positions if p.is_open}
            if len(active_buckets) >= settings.max_concurrent_buckets:
                logger.debug(
                    "Max concurrent buckets reached",
                    active=len(active_buckets),
                    max=settings.max_concurrent_buckets,
                )
                return False, 0.0

        # ── Low bankroll warning ────────────────────────────────────
        if self.state.bankroll < settings.min_order_size * 2:
            logger.warning(
                "RISK: Bankroll critically low",
                bankroll=f"${self.state.bankroll:.2f}",
            )
            _fire_risk_alert(
                f"Low balance warning: ${self.state.bankroll:.2f}",
            )
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

        Fractional Kelly (0.25x) reduces variance dramatically.
        Hard-capped at 4% of current bankroll.
        """
        p = signal.confidence
        q = 1.0 - p

        # Get the actual limit price for odds calculation
        ask_price = signal.limit_price
        if ask_price <= 0:
            # Fallback to meta
            if "yes_ask" in signal.meta:
                ask_price = float(signal.meta["yes_ask"])
            elif "no_ask" in signal.meta:
                ask_price = float(signal.meta["no_ask"])
            else:
                ask_price = 0.5

        ask_price = max(0.01, min(0.99, ask_price))

        # Binary market odds: buy at ask, receive $1 on win
        b = (1.0 - ask_price) / ask_price

        if b <= 0:
            return 0.0

        kelly_f = (p * b - q) / b
        kelly_f = max(0.0, kelly_f)

        # Apply fractional Kelly
        fraction = kelly_f * settings.kelly_fraction

        # Hard cap at max_bankroll_percent (4%)
        max_pct = settings.max_bankroll_percent / 100.0
        effective_fraction = min(fraction, max_pct)

        size = self.state.bankroll * effective_fraction
        size = min(size, settings.max_order_size)
        size = max(0.0, size)

        logger.debug(
            "Kelly sizing",
            confidence=f"{p:.3f}",
            ask_price=f"{ask_price:.4f}",
            odds=f"{b:.3f}",
            raw_kelly=f"{kelly_f:.4f}",
            fractional=f"{fraction:.4f}",
            capped_fraction=f"{effective_fraction:.4f}",
            size=f"${size:.2f}",
            bankroll=f"${self.state.bankroll:.2f}",
        )

        return round(size, 2)

    def record_trade(self, pnl: float) -> None:
        """Update risk state after a trade resolves."""
        self.state.daily_pnl += pnl
        self.state.total_pnl += pnl
        self.state.total_trades += 1

        if pnl > 0:
            self.state.winning_trades += 1
            self.state.consecutive_losses = 0
        else:
            self.state.losing_trades += 1
            self.state.consecutive_losses += 1

        self.state.bankroll += pnl
        self.state.peak_bankroll = max(self.state.peak_bankroll, self.state.bankroll)

        logger.info(
            "Trade recorded",
            pnl=f"${pnl:.4f}",
            daily_pnl=f"${self.state.daily_pnl:.4f}",
            bankroll=f"${self.state.bankroll:.2f}",
            consecutive_losses=self.state.consecutive_losses,
            win_rate=self.win_rate_str,
        )

    def update_bankroll(self, new_bankroll: float) -> None:
        """Sync bankroll from Polymarket balance query."""
        old = self.state.bankroll
        self.state.bankroll = new_bankroll
        self.state.peak_bankroll = max(self.state.peak_bankroll, new_bankroll)
        logger.info(
            "Bankroll synced",
            old=f"${old:.2f}",
            new=f"${new_bankroll:.2f}",
        )

    def _maybe_reset_daily(self) -> None:
        """Reset daily PnL counter at midnight UTC."""
        now = time.time()
        if now - self.state.daily_reset_ts > 86400:
            logger.info(
                "Daily risk reset",
                previous_daily_pnl=f"${self.state.daily_pnl:.4f}",
            )
            self.state.daily_pnl = 0.0
            self.state.daily_reset_ts = now

    def _maybe_unpause_cooldown(self) -> None:
        """Auto-unpause after cooldown expires."""
        if not self.state.paused:
            return
        if self.state.pause_until_ts > 0 and time.time() >= self.state.pause_until_ts:
            logger.info(
                "Cooldown expired, resuming trading",
                was_paused_for=self.state.pause_reason,
            )
            self.state.paused = False
            self.state.pause_reason = ""
            self.state.pause_until_ts = 0.0
            self.state.consecutive_losses = 0

    def force_unpause(self) -> None:
        """Manual override to unpause the bot."""
        self.state.paused = False
        self.state.pause_reason = ""
        self.state.pause_until_ts = 0.0
        self.state.consecutive_losses = 0
        logger.info("Risk engine manually unpaused")

    @property
    def win_rate_str(self) -> str:
        if self.state.total_trades > 0:
            return f"{self.state.winning_trades / self.state.total_trades:.1%}"
        return "N/A"

    @property
    def current_drawdown(self) -> float:
        if self.state.peak_bankroll <= 0:
            return 0.0
        return (self.state.peak_bankroll - self.state.bankroll) / self.state.peak_bankroll

    @property
    def stats(self) -> dict:
        return {
            "bankroll": f"${self.state.bankroll:.2f}",
            "daily_pnl": f"${self.state.daily_pnl:.2f}",
            "total_pnl": f"${self.state.total_pnl:.2f}",
            "total_trades": self.state.total_trades,
            "win_rate": self.win_rate_str,
            "wins": self.state.winning_trades,
            "losses": self.state.losing_trades,
            "consecutive_losses": self.state.consecutive_losses,
            "drawdown": f"{self.current_drawdown:.2%}",
            "paused": self.state.paused,
        }

    @property
    def daily_summary(self) -> str:
        """Formatted daily summary for Telegram."""
        s = self.state
        return (
            f"<b>Daily Summary</b>\n"
            f"Bankroll: <code>${s.bankroll:.2f}</code>\n"
            f"Daily P&L: <code>${s.daily_pnl:.2f}</code>\n"
            f"Total P&L: <code>${s.total_pnl:.2f}</code>\n"
            f"Trades: {s.total_trades} (W:{s.winning_trades} L:{s.losing_trades})\n"
            f"Win Rate: {self.win_rate_str}\n"
            f"Drawdown: {self.current_drawdown:.2%}\n"
            f"Status: {'PAUSED' if s.paused else 'ACTIVE'}"
        )


def _fire_risk_alert(reason: str) -> None:
    """Fire-and-forget risk alert (runs in background)."""
    import asyncio

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_risk_alert(reason))
    except RuntimeError:
        pass
