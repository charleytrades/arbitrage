"""Streamlit real-time dashboard for the Polymarket Micro-Arb Bot.

Run with:
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0

Then open http://your-server-ip:8501 in your browser.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import streamlit as st
import pandas as pd

# ── Page config ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Polymarket Micro-Arb Bot",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

ENV_FILE = Path(__file__).parent / ".env"

STATE_FILE = Path(__file__).parent / "bot_state.json"
REFRESH_RATE = 3  # seconds


def load_state() -> dict:
    """Load bot state from the shared JSON file."""
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def format_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m}m {s}s"


def render_no_data():
    st.title("Polymarket Micro-Arb Bot")
    st.warning(
        "**No bot state found.** The bot is either not running or hasn't "
        "written its first state update yet.\n\n"
        "Start the bot with:\n```\npython -m polymarket_micro_arb.main\n```\n\n"
        f"Looking for state file at: `{STATE_FILE}`"
    )
    st.info("This page auto-refreshes every 3 seconds.")


def main():
    state = load_state()

    if not state:
        render_no_data()
        time.sleep(REFRESH_RATE)
        st.rerun()
        return

    # ── Header ──────────────────────────────────────────────────────
    risk = state.get("risk", {})
    conns = state.get("connections", {})
    mode = state.get("mode", "unknown")
    uptime = state.get("uptime_sec", 0)

    st.title("Polymarket Micro-Arb Bot")

    # Status bar
    col_mode, col_uptime, col_updated, col_status = st.columns(4)
    with col_mode:
        mode_colors = {"live": "🔴", "paper_trade": "🟡", "backtest": "🔵"}
        st.metric("Mode", f"{mode_colors.get(mode, '⚪')} {mode.upper()}")
    with col_uptime:
        st.metric("Uptime", format_uptime(uptime))
    with col_updated:
        ts = state.get("timestamp_human", "—")
        st.metric("Last Update", ts)
    with col_status:
        if risk.get("paused"):
            st.metric("Status", "⏸ PAUSED")
        else:
            st.metric("Status", "✅ ACTIVE")

    st.divider()

    # ── Row 1: Key metrics ──────────────────────────────────────────
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Bankroll", f"${risk.get('bankroll', 0):.2f}")
    with c2:
        dpnl = risk.get("daily_pnl", 0)
        st.metric("Daily P&L", f"${dpnl:.2f}", delta=f"${dpnl:.2f}")
    with c3:
        tpnl = risk.get("total_pnl", 0)
        st.metric("Total P&L", f"${tpnl:.2f}", delta=f"${tpnl:.2f}")
    with c4:
        st.metric("Win Rate", risk.get("win_rate", "N/A"))
    with c5:
        st.metric(
            "Trades",
            f"{risk.get('total_trades', 0)}",
            delta=f"W:{risk.get('winning_trades',0)} L:{risk.get('losing_trades',0)}",
        )
    with c6:
        dd = risk.get("drawdown", 0)
        st.metric("Drawdown", f"{dd:.2%}")

    st.divider()

    # ── Row 2: Connections + Risk ───────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("Connections")
        conn_data = {
            "Source": ["Binance WS", "Bybit WS", "Polymarket Books", "Tick Queue"],
            "Status": [
                "🟢 Connected" if conns.get("binance") else "🔴 Disconnected",
                "🟢 Connected" if conns.get("bybit") else "🔴 Disconnected",
                f"📖 {conns.get('polymarket_books', 0)} books",
                f"📬 {conns.get('tick_queue_size', 0)} pending",
            ],
        }
        st.dataframe(
            pd.DataFrame(conn_data),
            use_container_width=True,
            hide_index=True,
        )

    with right:
        st.subheader("Risk Engine")
        risk_data = {
            "Parameter": [
                "Consecutive Losses",
                "Paused",
                "Pause Reason",
            ],
            "Value": [
                str(risk.get("consecutive_losses", 0)),
                "YES" if risk.get("paused") else "No",
                risk.get("pause_reason", "—") or "—",
            ],
        }
        st.dataframe(
            pd.DataFrame(risk_data),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ── Row 3: Equity curve ─────────────────────────────────────────
    equity = state.get("equity_curve", [])
    if len(equity) > 1:
        st.subheader("Equity Curve")
        eq_df = pd.DataFrame({"Bankroll ($)": equity})
        st.line_chart(eq_df, use_container_width=True, height=300)

    # ── Row 4: Open Positions ───────────────────────────────────────
    positions = state.get("positions", {})
    open_pos = positions.get("open", [])

    st.subheader(f"Open Positions ({len(open_pos)})")
    if open_pos:
        pos_rows = []
        for p in open_pos:
            market = p.get("market", {})
            remaining = max(0, market.get("end_ts", 0) - time.time())
            pos_rows.append(
                {
                    "Market": market.get("slug", "—"),
                    "Symbol": market.get("symbol", "—"),
                    "Bucket": market.get("bucket", "—"),
                    "Outcome": p.get("outcome", "—"),
                    "Side": p.get("side", "—"),
                    "Size ($)": f"${p.get('size', 0):.2f}",
                    "Entry": f"{p.get('entry_price', 0):.4f}",
                    "Time Left": f"{int(remaining)}s",
                    "Order ID": p.get("order_id", "—"),
                }
            )
        st.dataframe(pd.DataFrame(pos_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No open positions.")

    # ── Row 5: Recent Signals ───────────────────────────────────────
    signals = state.get("signals", {}).get("recent", [])

    st.subheader(f"Recent Signals ({len(signals)})")
    if signals:
        sig_rows = []
        for s in reversed(signals[-15:]):  # Show last 15, newest first
            market = s.get("market", {})
            sig_rows.append(
                {
                    "Time": time.strftime(
                        "%H:%M:%S", time.gmtime(s.get("timestamp", 0))
                    ),
                    "Type": s.get("signal_type", "—"),
                    "Market": market.get("slug", "—"),
                    "Outcome": s.get("outcome", "—"),
                    "Confidence": f"{s.get('confidence', 0):.3f}",
                    "Edge": f"{s.get('edge', 0):.4f}",
                    "Limit $": f"{s.get('limit_price', 0):.4f}",
                }
            )
        st.dataframe(pd.DataFrame(sig_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No signals yet. Waiting for market activity...")

    # ── Row 6: Trade Log ────────────────────────────────────────────
    trade_log = state.get("trade_log", [])
    if trade_log:
        st.subheader(f"Trade Log (last {len(trade_log)})")
        log_rows = []
        for t in reversed(trade_log[-20:]):
            pnl = t.get("pnl", 0)
            log_rows.append(
                {
                    "Time": t.get("time", "—"),
                    "Market": t.get("market", "—"),
                    "Type": t.get("signal_type", "—"),
                    "Outcome": t.get("outcome", "—"),
                    "Size": f"${t.get('size', 0):.2f}",
                    "Entry": f"{t.get('entry', 0):.4f}",
                    "Exit": f"{t.get('exit', 0):.4f}",
                    "P&L": f"{'+'if pnl>=0 else ''}${pnl:.4f}",
                    "Result": "✅" if pnl > 0 else "❌",
                }
            )
        st.dataframe(pd.DataFrame(log_rows), use_container_width=True, hide_index=True)

    # ── Row 7: Active Markets ───────────────────────────────────────
    markets = state.get("markets", {})
    market_list = markets.get("markets", [])

    with st.expander(f"Active Markets ({markets.get('active_count', 0)})", expanded=False):
        if market_list:
            mkt_rows = []
            for m in market_list:
                remaining = max(0, m.get("end_ts", 0) - time.time())
                mkt_rows.append(
                    {
                        "Slug": m.get("slug", "—"),
                        "Symbol": m.get("symbol", "—"),
                        "Bucket": m.get("bucket", "—"),
                        "Time Left": f"{int(remaining)}s",
                        "Active": "✅" if m.get("active") else "❌",
                    }
                )
            st.dataframe(
                pd.DataFrame(mkt_rows), use_container_width=True, hide_index=True
            )
        else:
            st.info("No active markets discovered yet.")

    # ── Row 8: Closed Positions ─────────────────────────────────────
    closed = positions.get("recent_closed", [])
    if closed:
        with st.expander(
            f"Closed Positions ({positions.get('closed_count', 0)} total)", expanded=False
        ):
            cl_rows = []
            for p in reversed(closed[-20:]):
                market = p.get("market", {})
                pnl = p.get("pnl", 0)
                cl_rows.append(
                    {
                        "Market": market.get("slug", "—"),
                        "Outcome": p.get("outcome", "—"),
                        "Entry": f"{p.get('entry_price', 0):.4f}",
                        "Exit": f"{p.get('exit_price', 0):.4f}",
                        "Size": f"${p.get('size', 0):.2f}",
                        "P&L": f"{'+'if pnl>=0 else ''}${pnl:.4f}",
                    }
                )
            st.dataframe(
                pd.DataFrame(cl_rows), use_container_width=True, hide_index=True
            )

    # ── Sidebar ─────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Bot Info")
        st.write(f"**State file:** `{STATE_FILE}`")
        st.write(f"**Refresh rate:** {REFRESH_RATE}s")

        if st.button("Force Refresh"):
            st.rerun()

    # ── Auto-refresh ────────────────────────────────────────────────
    time.sleep(REFRESH_RATE)
    st.rerun()


# ── Settings Page ──────────────────────────────────────────────────
def load_env() -> dict[str, str]:
    """Parse .env file into a dict, preserving order."""
    values = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                values[key.strip()] = val.strip()
    return values


def save_env(values: dict[str, str]) -> None:
    """Write values back to .env, preserving comments and structure."""
    lines = ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []
    new_lines = []
    written_keys = set()
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in values:
                new_lines.append(f"{key}={values[key]}")
                written_keys.add(key)
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
    # Append any new keys
    for key, val in values.items():
        if key not in written_keys:
            new_lines.append(f"{key}={val}")
    ENV_FILE.write_text("\n".join(new_lines) + "\n")


def restart_bot() -> str:
    """Restart the bot via systemd."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "restart", "polymarket-bot"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return "Bot restarted successfully."
        return f"Restart failed: {result.stderr}"
    except Exception as e:
        return f"Error: {e}"


SETTINGS_GROUPS = {
    "Strategy": [
        ("MOMENTUM_THRESHOLD", "Momentum Threshold", "Min price move to trigger signal (e.g. 0.0015 = 0.15%)"),
        ("MOMENTUM_WINDOW_START_SEC", "Window Start (sec)", "Seconds after bucket open to start looking"),
        ("MOMENTUM_WINDOW_END_SEC", "Window End (sec)", "Seconds after bucket open to stop looking"),
        ("VOLUME_CONFIRM_MULTIPLIER", "Volume Multiplier", "1.0 = any volume, 1.5 = above average"),
        ("CROSS_OUTCOME_THRESHOLD", "Cross-Outcome Threshold", "Max YES+NO ask sum for arb (e.g. 0.99)"),
        ("MIN_SPREAD_PROFIT", "Min Spread Profit", "Minimum edge to take a trade"),
    ],
    "Risk": [
        ("MAX_BANKROLL_PERCENT", "Max Bankroll %", "Max % of bankroll per trade"),
        ("MAX_DAILY_LOSS_PERCENT", "Max Daily Loss %", "Kill-switch: pause if daily loss exceeds this"),
        ("MAX_CONSECUTIVE_LOSSES", "Max Consecutive Losses", "Kill-switch: pause after N losses in a row"),
        ("KELLY_FRACTION", "Kelly Fraction", "Fraction of Kelly criterion to use (0.25 = quarter Kelly)"),
    ],
    "Orders": [
        ("MIN_ORDER_SIZE", "Min Order Size ($)", "Minimum order in dollars"),
        ("MAX_ORDER_SIZE", "Max Order Size ($)", "Maximum order in dollars"),
    ],
    "Mode": [
        ("TRADING_MODE", "Trading Mode", "paper_trade or live"),
        ("SYMBOLS", "Symbols", "Comma-separated: BTCUSDT,ETHUSDT,SOLUSDT"),
    ],
}


def settings_page():
    st.title("Settings")

    env = load_env()
    updated = {}

    for group_name, fields in SETTINGS_GROUPS.items():
        st.subheader(group_name)
        cols = st.columns(2)
        for i, (key, label, help_text) in enumerate(fields):
            with cols[i % 2]:
                current = env.get(key, "")
                if key == "TRADING_MODE":
                    new_val = st.selectbox(
                        label, ["paper_trade", "live"],
                        index=0 if current != "live" else 1,
                        help=help_text, key=key,
                    )
                else:
                    new_val = st.text_input(label, value=current, help=help_text, key=key)
                updated[key] = str(new_val)

    st.divider()

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Save Settings", type="primary"):
            save_env(updated)
            st.success("Settings saved to .env")
    with col2:
        if st.button("Save & Restart Bot"):
            save_env(updated)
            msg = restart_bot()
            if "successfully" in msg:
                st.success(msg)
            else:
                st.error(msg)
    with col3:
        if st.button("Restart Bot (no save)"):
            msg = restart_bot()
            if "successfully" in msg:
                st.success(msg)
            else:
                st.error(msg)

    st.divider()
    st.caption("Changes only take effect after restarting the bot.")


# ── Page Router ────────────────────────────────────────────────────
page = st.sidebar.radio("Page", ["Dashboard", "Settings"], index=0)

if page == "Dashboard":
    main()
else:
    settings_page()
