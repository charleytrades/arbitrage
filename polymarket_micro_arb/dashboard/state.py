"""Shared state writer for the Streamlit dashboard.

The bot calls `StateWriter.update()` every few seconds to dump its
live state into a JSON file. The Streamlit dashboard reads this file
with auto-refresh to render real-time charts and tables.

This avoids any IPC complexity — just a file on disk.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from polymarket_micro_arb.utils.logger import logger

# Default state file location (project root)
DEFAULT_STATE_PATH = Path(__file__).resolve().parent.parent.parent / "bot_state.json"


class StateWriter:
    """Serialises bot state to a JSON file for dashboard consumption."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else DEFAULT_STATE_PATH
        self._last_write = 0.0
        self._min_interval = 2.0  # Don't write more than every 2s

    def update(
        self,
        *,
        mode: str = "",
        uptime_sec: float = 0.0,
        # Risk state
        bankroll: float = 0.0,
        daily_pnl: float = 0.0,
        total_pnl: float = 0.0,
        total_trades: int = 0,
        winning_trades: int = 0,
        losing_trades: int = 0,
        consecutive_losses: int = 0,
        win_rate: str = "N/A",
        drawdown: float = 0.0,
        paused: bool = False,
        pause_reason: str = "",
        # Markets
        active_markets: list[dict] = [],
        broad_markets: int = 0,
        # Positions
        open_positions: list[dict] = [],
        closed_positions: list[dict] = [],
        # Signals
        recent_signals: list[dict] = [],
        # Connections
        binance_connected: bool = False,
        bybit_connected: bool = False,
        polymarket_books: int = 0,
        tick_queue_size: int = 0,
        # Equity curve
        equity_curve: list[float] = [],
        # Trades log (last N)
        trade_log: list[dict] = [],
    ) -> None:
        """Write current state to JSON file."""
        now = time.time()
        if now - self._last_write < self._min_interval:
            return

        state: dict[str, Any] = {
            "timestamp": now,
            "timestamp_human": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now)),
            "mode": mode,
            "uptime_sec": uptime_sec,
            "risk": {
                "bankroll": bankroll,
                "daily_pnl": daily_pnl,
                "total_pnl": total_pnl,
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "consecutive_losses": consecutive_losses,
                "win_rate": win_rate,
                "drawdown": drawdown,
                "paused": paused,
                "pause_reason": pause_reason,
            },
            "markets": {
                "active_count": len(active_markets),
                "broad_count": broad_markets,
                "markets": active_markets[:20],  # Cap to avoid huge files
            },
            "positions": {
                "open": open_positions,
                "closed_count": len(closed_positions),
                "recent_closed": closed_positions[-20:],  # Last 20
            },
            "signals": {
                "recent": recent_signals[-50:],  # Last 50
            },
            "connections": {
                "binance": binance_connected,
                "bybit": bybit_connected,
                "polymarket_books": polymarket_books,
                "tick_queue_size": tick_queue_size,
            },
            "equity_curve": equity_curve[-500:],  # Last 500 points
            "trade_log": trade_log[-100:],  # Last 100 trades
        }

        try:
            # Atomic write: write to tmp then rename
            tmp_path = self.path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(state, indent=2, default=str))
            tmp_path.rename(self.path)
            self._last_write = now
        except Exception as exc:
            logger.debug("State write failed", error=str(exc))


def read_state(path: Path | str | None = None) -> dict:
    """Read the current bot state from the JSON file."""
    p = Path(path) if path else DEFAULT_STATE_PATH
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
