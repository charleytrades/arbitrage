#!/usr/bin/env python3
"""Daily report generator for the Polymarket Micro-Arb Bot.

Parses the structured JSON log file for full trade history,
generates a report with P&L, trade breakdown, equity curve,
saves to reports/, and optionally sends a Telegram digest.

Can be run manually: python scripts/daily_report.py
Or scheduled via Claude Code /schedule for daily automation.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LOG_DIR = PROJECT_ROOT / "logs"
REPORT_DIR = PROJECT_ROOT / "reports"
STATE_FILE = PROJECT_ROOT / "bot_state.json"


def find_log_file(date_str: str) -> Path | None:
    """Find the active log file for a given date."""
    # Active (unrotated) log
    active = LOG_DIR / f"bot_{date_str}.log"
    if active.exists():
        return active
    # Might only have rotated .gz files
    return None


def parse_log(log_path: Path) -> dict:
    """Parse a JSON log file and extract trades, heartbeats, signals, errors."""
    trades = []
    heartbeats = []
    signals = []
    errors = []
    filter_counts: Counter = Counter()

    with open(log_path) as f:
        for line in f:
            try:
                rec = json.loads(line)
                msg = rec["record"]["message"]
                ex = rec["record"]["extra"]
                ts = rec["record"]["time"]["repr"][:19]
                level = rec["record"]["level"]["name"]

                if "Signal executed" in msg:
                    signals.append({"time": ts, **ex})
                elif "HEARTBEAT" in msg:
                    heartbeats.append({"time": ts, **ex})
                elif level == "ERROR":
                    errors.append({"time": ts, "message": msg, **ex})
                elif msg.startswith("FILTER:") or msg.startswith("PASSED:"):
                    filter_counts[msg.split(",")[0].strip()] += 1
            except (json.JSONDecodeError, KeyError):
                continue

    # Get trade log from bot_state.json for resolved P&L
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    trade_log = state.get("trade_log", [])

    return {
        "signals": signals,
        "heartbeats": heartbeats,
        "errors": errors,
        "filter_counts": filter_counts,
        "trade_log": trade_log,
        "state": state,
    }


def generate_report(data: dict, date_str: str) -> str:
    """Generate a full daily report from parsed log data."""
    signals = data["signals"]
    heartbeats = data["heartbeats"]
    errors = data["errors"]
    filter_counts = data["filter_counts"]
    trade_log = data["trade_log"]
    state = data["state"]
    risk = state.get("risk", {})
    conns = state.get("connections", {})
    markets = state.get("markets", {})
    equity = state.get("equity_curve", [])

    # Signal stats
    sig_types = Counter(s.get("signal_type", "?") for s in signals)
    edges = [float(s["edge"]) for s in signals if "edge" in s]

    # Trade P&L stats
    pnls = [t.get("pnl", 0) for t in trade_log]
    best = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0
    avg = sum(pnls) / len(pnls) if pnls else 0

    # Streak
    streak = 0
    streak_type = ""
    for t in reversed(trade_log):
        pnl = t.get("pnl", 0)
        if streak == 0:
            streak_type = "W" if pnl > 0 else "L"
            streak = 1
        elif (pnl > 0 and streak_type == "W") or (pnl <= 0 and streak_type == "L"):
            streak += 1
        else:
            break

    # Uptime
    if heartbeats:
        first_hb = heartbeats[0].get("time", "")
        last_hb = heartbeats[-1].get("time", "")
    else:
        first_hb = last_hb = "?"
    uptime_h = state.get("uptime_sec", 0) / 3600

    report = f"""{'='*60}
  DAILY REPORT — {date_str}
{'='*60}

MODE: {state.get('mode', 'unknown').upper()}
UPTIME: {uptime_h:.1f} hours
HEARTBEATS: {len(heartbeats)} (first: {first_hb}, last: {last_hb})

─── PERFORMANCE ────────────────────────────────────
  Bankroll:          {risk.get('bankroll', '?')}
  Daily P&L:         {risk.get('daily_pnl', '?')}
  Total P&L:         {risk.get('total_pnl', '?')}

  Trades Settled:    {risk.get('total_trades', 0)}
  Wins:              {risk.get('winning_trades', 0)}
  Losses:            {risk.get('losing_trades', 0)}
  Win Rate:          {risk.get('win_rate', 'N/A')}

  Best Trade:        ${best:.4f}
  Worst Trade:       ${worst:.4f}
  Avg Trade:         ${avg:.4f}
  Current Streak:    {streak}{streak_type}

─── SIGNALS EXECUTED ───────────────────────────────
  Total:             {len(signals)}"""

    for typ, count in sig_types.most_common():
        report += f"\n  {typ + ':':25s}{count}"

    if edges:
        report += f"""

  Edge min/avg/max:  {min(edges):.4f} / {sum(edges)/len(edges):.4f} / {max(edges):.4f}"""

    # Per-market breakdown
    by_market = Counter(s.get("market", "?") for s in signals)
    report += "\n\n─── TRADES PER MARKET ──────────────────────────────"
    for m, c in by_market.most_common():
        report += f"\n  {c:4d}  {m}"

    # Filter pipeline
    if filter_counts:
        report += "\n\n─── FILTER PIPELINE ────────────────────────────────"
        for f, c in filter_counts.most_common():
            report += f"\n  {c:6d}  {f}"

    # Risk status
    report += f"""

─── RISK STATUS ────────────────────────────────────
  Drawdown:          {risk.get('drawdown', 0)}
  Consec Losses:     {risk.get('consecutive_losses', 0)}
  Paused:            {'YES — ' + risk.get('pause_reason', '') if risk.get('paused') else 'No'}

─── CONNECTIONS ────────────────────────────────────
  Binance WS:        {'Connected' if conns.get('binance') else 'DISCONNECTED'}
  Bybit WS:          {'Connected' if conns.get('bybit') else 'DISCONNECTED'}
  Polymarket Books:  {conns.get('polymarket_books', 0)}

─── ERRORS ({len(errors)}) ─────────────────────────────────"""

    if errors:
        err_msgs = Counter(e.get("message", "?") for e in errors)
        for msg, c in err_msgs.most_common(10):
            report += f"\n  {c:4d}x  {msg}"
    else:
        report += "\n  None"

    # Equity curve
    report += "\n\n─── EQUITY CURVE ───────────────────────────────────"
    if len(equity) > 2:
        step = max(1, len(equity) // 50)
        sampled = equity[::step]
        mn, mx = min(sampled), max(sampled)
        rng = mx - mn if mx > mn else 1
        chart_height = 10
        for row in range(chart_height, -1, -1):
            threshold = mn + (rng * row / chart_height)
            line = f"  ${threshold:>8.1f} │"
            for val in sampled:
                line += "█" if val >= threshold else " "
            report += f"\n{line}"
        report += f"\n           └{'─' * len(sampled)}"
    else:
        report += "\n  (Not enough data)"

    # Last 15 trades
    if trade_log:
        report += f"\n\n─── LAST 15 TRADES ─────────────────────────────────"
        report += f"\n  {'Time':<10} {'Type':<22} {'Market':<35} {'Out':<4} {'Entry':>6} {'Exit':>6} {'P&L':>10}"
        report += f"\n  {'─'*10} {'─'*22} {'─'*35} {'─'*4} {'─'*6} {'─'*6} {'─'*10}"
        for t in reversed(trade_log[-15:]):
            pnl = t.get("pnl", 0)
            marker = "+" if pnl >= 0 else ""
            report += (
                f"\n  {t.get('time', '?'):<10} "
                f"{t.get('signal_type', '?'):<22} "
                f"{t.get('market', '?'):<35} "
                f"{t.get('outcome', '?'):<4} "
                f"{t.get('entry', 0):>6.2f} "
                f"{t.get('exit', 0):>6.1f} "
                f"{marker}${pnl:.4f}"
            )

    report += f"\n\n{'='*60}\n"
    return report


def generate_telegram_report(data: dict) -> str:
    """Compact HTML report for Telegram."""
    state = data["state"]
    risk = state.get("risk", {})
    signals = data["signals"]
    trade_log = data["trade_log"]
    pnls = [t.get("pnl", 0) for t in trade_log]
    best = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0

    pnl = risk.get("daily_pnl", 0)
    pnl_str = f"${pnl}" if isinstance(pnl, str) else f"${pnl:.2f}"

    sig_types = Counter(s.get("signal_type", "?") for s in signals)
    type_str = ", ".join(f"{t}:{c}" for t, c in sig_types.most_common())

    return (
        f"<b>{'📈' if not isinstance(pnl, str) and pnl >= 0 else '📉'} Daily Report</b>\n"
        f"\n"
        f"Bankroll: <code>{risk.get('bankroll', '?')}</code>\n"
        f"Daily P&L: <code>{pnl_str}</code>\n"
        f"\n"
        f"Signals: {len(signals)} ({type_str})\n"
        f"Settled: {risk.get('total_trades', 0)} "
        f"(W:{risk.get('winning_trades', 0)} L:{risk.get('losing_trades', 0)})\n"
        f"Win Rate: {risk.get('win_rate', 'N/A')}\n"
        f"Best: <code>${best:.4f}</code> / Worst: <code>${worst:.4f}</code>\n"
        f"\n"
        f"Drawdown: {risk.get('drawdown', 0)}\n"
        f"Status: {'⏸ PAUSED' if risk.get('paused') else '✅ Active'}\n"
        f"Uptime: {state.get('uptime_sec', 0) / 3600:.1f}h"
    )


async def send_telegram(message: str) -> bool:
    try:
        from polymarket_micro_arb.utils.telegram_alerts import send_alert
        return await send_alert(message)
    except ImportError:
        return False


def main():
    date_str = time.strftime("%Y-%m-%d", time.gmtime())

    # Allow passing a date as argument
    if len(sys.argv) > 1:
        date_str = sys.argv[1]

    log_path = find_log_file(date_str)
    if not log_path:
        print(f"ERROR: No log file found for {date_str}")
        print(f"Looking in: {LOG_DIR}")
        sys.exit(1)

    print(f"Parsing {log_path} ...")
    data = parse_log(log_path)
    print(f"  Signals: {len(data['signals'])}, Heartbeats: {len(data['heartbeats'])}, Errors: {len(data['errors'])}")

    report = generate_report(data, date_str)
    print(report)

    # Save
    REPORT_DIR.mkdir(exist_ok=True)
    report_file = REPORT_DIR / f"daily_{date_str}.txt"
    report_file.write_text(report)
    print(f"Report saved to: {report_file}")

    # Send Telegram
    import asyncio
    tg_report = generate_telegram_report(data)
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
