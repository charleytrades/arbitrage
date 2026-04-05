#!/usr/bin/env python3
"""Health check script for the Polymarket Micro-Arb Bot.

Designed to be called by a Claude Code /loop agent every 5 minutes.
Reads bot_state.json and checks:
  - Is the bot process alive?
  - Is state fresh (updated within last 60s)?
  - Are WebSockets connected?
  - Is the risk engine paused?
  - Is the tick queue backing up?
  - Are there any error patterns in recent logs?

Exit codes:
  0 = healthy
  1 = warning (degraded but running)
  2 = critical (bot is down or unresponsive)
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

STATE_FILE = Path(__file__).resolve().parent.parent / "bot_state.json"
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
MAX_STATE_AGE_SEC = 120  # State older than this = bot is dead
TICK_QUEUE_WARNING = 5000  # Queue backing up threshold


def check_process() -> tuple[bool, str]:
    """Check if the bot Python process is running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "polymarket_micro_arb.main"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split("\n")
            return True, f"Running (PID: {', '.join(pids)})"
        return False, "NOT RUNNING"
    except Exception as e:
        return False, f"Check failed: {e}"


def check_state() -> dict:
    """Read and validate bot_state.json."""
    if not STATE_FILE.exists():
        return {"exists": False, "error": "bot_state.json not found"}

    try:
        state = json.loads(STATE_FILE.read_text())
        age = time.time() - state.get("timestamp", 0)
        state["_age_sec"] = age
        state["_fresh"] = age < MAX_STATE_AGE_SEC
        return state
    except (json.JSONDecodeError, OSError) as e:
        return {"exists": True, "error": f"Parse error: {e}"}


def check_recent_errors() -> list[str]:
    """Scan recent log file for error patterns."""
    errors = []
    if not LOG_DIR.exists():
        return ["No log directory found"]

    log_files = sorted(LOG_DIR.glob("bot_*.log"), reverse=True)
    if not log_files:
        return ["No log files found"]

    # Read last 200 lines of most recent log
    try:
        lines = log_files[0].read_text().strip().split("\n")[-200:]
        error_count = 0
        for line in lines:
            if '"level":"ERROR"' in line or '"level":"CRITICAL"' in line:
                error_count += 1
                try:
                    entry = json.loads(line)
                    msg = entry.get("text", "")[:100]
                    errors.append(msg)
                except json.JSONDecodeError:
                    errors.append(line[:100])

        if error_count > 10:
            errors.insert(0, f"HIGH ERROR RATE: {error_count} errors in last 200 log lines")
    except OSError:
        errors.append("Could not read log file")

    return errors[-5:]  # Return last 5 errors max


def run_health_check() -> int:
    """Run all checks and print a structured report."""
    print("=" * 60)
    print(f"HEALTH CHECK — {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print("=" * 60)

    issues = []
    warnings = []
    exit_code = 0

    # 1. Process check
    alive, proc_msg = check_process()
    status = "OK" if alive else "CRITICAL"
    print(f"\n[{status}] Process: {proc_msg}")
    if not alive:
        issues.append("Bot process is not running")
        exit_code = 2

    # 2. State file check
    state = check_state()
    if "error" in state:
        print(f"\n[CRITICAL] State: {state['error']}")
        issues.append(state["error"])
        exit_code = 2
    elif not state.get("_fresh", False):
        age = state.get("_age_sec", 0)
        print(f"\n[CRITICAL] State: STALE ({age:.0f}s old, max {MAX_STATE_AGE_SEC}s)")
        issues.append(f"State file is {age:.0f}s old — bot may be hung")
        exit_code = 2
    else:
        age = state.get("_age_sec", 0)
        print(f"\n[OK] State: Fresh ({age:.0f}s old)")

        # 3. Connection checks
        conns = state.get("connections", {})
        binance = conns.get("binance", False)
        bybit = conns.get("bybit", False)
        books = conns.get("polymarket_books", 0)
        queue = conns.get("tick_queue_size", 0)

        print(f"\n--- Connections ---")
        print(f"  Binance WS:       {'OK' if binance else 'DISCONNECTED'}")
        print(f"  Bybit WS:         {'OK' if bybit else 'DISCONNECTED'}")
        print(f"  Polymarket books:  {books}")
        print(f"  Tick queue:        {queue}")

        if not binance:
            warnings.append("Binance WS disconnected")
            exit_code = max(exit_code, 1)
        if queue > TICK_QUEUE_WARNING:
            warnings.append(f"Tick queue backing up: {queue}")
            exit_code = max(exit_code, 1)

        # 4. Risk engine
        risk = state.get("risk", {})
        paused = risk.get("paused", False)
        bankroll = risk.get("bankroll", 0)
        daily_pnl = risk.get("daily_pnl", 0)
        consec = risk.get("consecutive_losses", 0)

        print(f"\n--- Risk ---")
        print(f"  Bankroll:          ${bankroll:.2f}")
        print(f"  Daily P&L:         ${daily_pnl:.2f}")
        print(f"  Consec losses:     {consec}")
        print(f"  Paused:            {'YES — ' + risk.get('pause_reason', '') if paused else 'No'}")

        if paused:
            warnings.append(f"Risk engine PAUSED: {risk.get('pause_reason', 'unknown')}")
            exit_code = max(exit_code, 1)
        if bankroll < 50:
            warnings.append(f"LOW BANKROLL: ${bankroll:.2f}")
            exit_code = max(exit_code, 1)

        # 5. Trading activity
        print(f"\n--- Activity ---")
        print(f"  Mode:              {state.get('mode', '?')}")
        print(f"  Uptime:            {state.get('uptime_sec', 0) / 3600:.1f}h")
        print(f"  Total trades:      {risk.get('total_trades', 0)}")
        print(f"  Win rate:          {risk.get('win_rate', 'N/A')}")
        print(f"  Open positions:    {len(state.get('positions', {}).get('open', []))}")
        print(f"  Active markets:    {state.get('markets', {}).get('active_count', 0)}")

    # 6. Log errors
    errors = check_recent_errors()
    if errors:
        print(f"\n--- Recent Errors ({len(errors)}) ---")
        for e in errors:
            print(f"  ! {e}")
        if any("HIGH ERROR RATE" in e for e in errors):
            warnings.append("High error rate in logs")
            exit_code = max(exit_code, 1)

    # Summary
    print(f"\n{'=' * 60}")
    if exit_code == 0:
        print("RESULT: HEALTHY")
    elif exit_code == 1:
        print(f"RESULT: WARNING")
        for w in warnings:
            print(f"  ⚠ {w}")
    else:
        print(f"RESULT: CRITICAL")
        for i in issues:
            print(f"  🛑 {i}")
        for w in warnings:
            print(f"  ⚠ {w}")

    print("=" * 60)
    return exit_code


if __name__ == "__main__":
    sys.exit(run_health_check())
