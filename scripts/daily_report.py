#!/usr/bin/env python3
"""Daily report generator for the Polymarket Micro-Arb Bot.

Designed to be called by a Claude Code /schedule agent once per day.
Reads bot_state.json, compiles a full daily summary, and optionally
sends it via Telegram.

Can also be run manually: python scripts/daily_report.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

STATE_FILE = PROJECT_ROOT / "bot_state.json"
REPORT_DIR = PROJECT_ROOT / "reports"


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def generate_report(state: dict) -> str:
    """Generate a formatted daily report."""
    now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    risk = state.get("risk", {})
    conns = state.get("connections", {})
    positions = state.get("positions", {})
    markets = state.get("markets", {})
    trade_log = state.get("trade_log", [])
    equity = state.get("equity_curve", [])

    # Calculate additional stats from trade log
    today_trades = trade_log  # In production, filter by date
    today_pnls = [t.get("pnl", 0) for t in today_trades]
    best = max(today_pnls) if today_pnls else 0
    worst = min(today_pnls) if today_pnls else 0
    avg = sum(today_pnls) / len(today_pnls) if today_pnls else 0

    # Streak calculation
    streak = 0
    streak_type = ""
    for t in reversed(today_trades):
        pnl = t.get("pnl", 0)
        if streak == 0:
            streak_type = "W" if pnl > 0 else "L"
            streak = 1
        elif (pnl > 0 and streak_type == "W") or (pnl <= 0 and streak_type == "L"):
            streak += 1
        else:
            break

    uptime_h = state.get("uptime_sec", 0) / 3600

    report = f"""
{'='*60}
  DAILY REPORT — {now}
{'='*60}

MODE: {state.get('mode', 'unknown').upper()}
UPTIME: {uptime_h:.1f} hours

─── PERFORMANCE ────────────────────────────────────
  Bankroll:          ${risk.get('bankroll', 0):.2f}
  Daily P&L:         ${risk.get('daily_pnl', 0):.2f}
  Total P&L:         ${risk.get('total_pnl', 0):.2f}

  Total Trades:      {risk.get('total_trades', 0)}
  Wins:              {risk.get('winning_trades', 0)}
  Losses:            {risk.get('losing_trades', 0)}
  Win Rate:          {risk.get('win_rate', 'N/A')}

  Best Trade:        ${best:.4f}
  Worst Trade:       ${worst:.4f}
  Avg Trade:         ${avg:.4f}
  Current Streak:    {streak}{streak_type}

─── RISK STATUS ────────────────────────────────────
  Drawdown:          {risk.get('drawdown', 0):.2%}
  Consec Losses:     {risk.get('consecutive_losses', 0)}
  Paused:            {'YES — ' + risk.get('pause_reason', '') if risk.get('paused') else 'No'}

─── MARKET COVERAGE ────────────────────────────────
  Active Markets:    {markets.get('active_count', 0)}
  Open Positions:    {len(positions.get('open', []))}
  Closed Today:      {positions.get('closed_count', 0)}

─── CONNECTIONS ────────────────────────────────────
  Binance WS:        {'Connected' if conns.get('binance') else 'DISCONNECTED'}
  Bybit WS:          {'Connected' if conns.get('bybit') else 'DISCONNECTED'}
  Polymarket Books:  {conns.get('polymarket_books', 0)}
  Tick Queue:        {conns.get('tick_queue_size', 0)}

─── EQUITY CURVE ───────────────────────────────────"""

    # Simple ASCII equity curve
    if len(equity) > 2:
        # Downsample to ~40 points
        step = max(1, len(equity) // 40)
        sampled = equity[::step]
        if sampled:
            mn, mx = min(sampled), max(sampled)
            rng = mx - mn if mx > mn else 1
            chart_height = 8
            for row in range(chart_height, -1, -1):
                threshold = mn + (rng * row / chart_height)
                line = f"  ${threshold:>8.1f} │"
                for val in sampled:
                    if val >= threshold:
                        line += "█"
                    else:
                        line += " "
                report += f"\n{line}"
            report += f"\n           └{'─' * len(sampled)}"
    else:
        report += "\n  (Not enough data for chart)"

    # Recent trades
    if today_trades:
        report += f"\n\n─── LAST 10 TRADES ─────────────────────────────────"
        report += f"\n  {'Time':<10} {'Market':<30} {'Out':<4} {'P&L':>10}"
        report += f"\n  {'─'*10} {'─'*30} {'─'*4} {'─'*10}"
        for t in reversed(today_trades[-10:]):
            pnl = t.get('pnl', 0)
            marker = "+" if pnl >= 0 else ""
            report += (
                f"\n  {t.get('time', '?'):<10} "
                f"{t.get('market', '?'):<30} "
                f"{t.get('outcome', '?'):<4} "
                f"{marker}${pnl:.4f}"
            )

    report += f"\n\n{'='*60}\n"
    return report


def generate_telegram_report(state: dict) -> str:
    """Generate a compact HTML report for Telegram."""
    risk = state.get("risk", {})
    trade_log = state.get("trade_log", [])
    today_pnls = [t.get("pnl", 0) for t in trade_log]
    best = max(today_pnls) if today_pnls else 0
    worst = min(today_pnls) if today_pnls else 0

    pnl = risk.get("daily_pnl", 0)
    pnl_emoji = "📈" if pnl >= 0 else "📉"

    return (
        f"<b>{pnl_emoji} Daily Report</b>\n"
        f"\n"
        f"Bankroll: <code>${risk.get('bankroll', 0):.2f}</code>\n"
        f"Daily P&L: <code>${pnl:.2f}</code>\n"
        f"Total P&L: <code>${risk.get('total_pnl', 0):.2f}</code>\n"
        f"\n"
        f"Trades: {risk.get('total_trades', 0)} "
        f"(W:{risk.get('winning_trades', 0)} L:{risk.get('losing_trades', 0)})\n"
        f"Win Rate: {risk.get('win_rate', 'N/A')}\n"
        f"Best: <code>${best:.4f}</code> / Worst: <code>${worst:.4f}</code>\n"
        f"\n"
        f"Drawdown: {risk.get('drawdown', 0):.2%}\n"
        f"Status: {'⏸ PAUSED' if risk.get('paused') else '✅ Active'}\n"
        f"Uptime: {state.get('uptime_sec', 0) / 3600:.1f}h"
    )


async def send_telegram(message: str) -> bool:
    """Send report via Telegram if configured."""
    try:
        # Import here to avoid dependency if just running locally
        from polymarket_micro_arb.utils.telegram_alerts import send_alert
        return await send_alert(message)
    except ImportError:
        return False


def main():
    state = load_state()
    if not state:
        print("ERROR: No bot state found. Is the bot running?")
        print(f"Looking for: {STATE_FILE}")
        sys.exit(1)

    # Generate and print the full report
    report = generate_report(state)
    print(report)

    # Save to file
    REPORT_DIR.mkdir(exist_ok=True)
    date_str = time.strftime("%Y-%m-%d", time.gmtime())
    report_file = REPORT_DIR / f"daily_{date_str}.txt"
    report_file.write_text(report)
    print(f"Report saved to: {report_file}")

    # Send Telegram summary
    import asyncio
    tg_report = generate_telegram_report(state)
    try:
        sent = asyncio.run(send_telegram(tg_report))
        if sent:
            print("Telegram report sent.")
        else:
            print("Telegram not configured or send failed.")
    except Exception as e:
        print(f"Telegram send error: {e}")


if __name__ == "__main__":
    main()
