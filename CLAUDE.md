# Polymarket Crypto Micro-Arb Bot

## What This Is

A production trading bot that exploits the latency gap between Binance/Bybit spot prices and Polymarket CLOB prediction markets on 5-minute and 15-minute BTC/ETH/SOL Up/Down micro-buckets.

**DO NOT** place real orders, modify risk parameters, or switch to live mode without explicit user approval.

## Architecture

```
polymarket_micro_arb/
├── data/           # Market discovery (Gamma API) + price feeds (Binance/Bybit/Polymarket WS)
├── strategy/       # Signal generation (momentum_latency + cross_outcome_arb)
├── execution/      # CLOB order placement with EIP-712 signing
├── risk/           # Kelly sizing, drawdown kill-switch, position limits
├── dashboard/      # State writer for Streamlit dashboard
├── utils/          # Logging, Telegram alerts, backtester
├── config.py       # Pydantic settings from .env
├── models.py       # Data models (MarketInfo, Signal, Position, etc.)
├── constants.py    # Enums, timing constants, symbol mappings
└── main.py         # Bot orchestrator (asyncio.TaskGroup, signal handlers)
```

## Key Files

- `main.py` — Entry point. Launches 9 concurrent tasks via TaskGroup.
- `config.py` — All settings from `.env`. Import `settings` singleton.
- `strategy/momentum_latency.py` — Core strategy. Only fires in 15-45s window after bucket open, requires >=0.35% move + volume + multi-venue confirmation.
- `strategy/cross_outcome_arb.py` — Detects YES+NO ask < $0.99 for risk-free arb.
- `risk/risk_engine.py` — Non-negotiable: 4% max/trade, -8% daily kill-switch, 3 consec loss cooldown, 8 max buckets.
- `execution/clob_executor.py` — Limit orders only, 60s TTL auto-cancel.
- `dashboard.py` — Streamlit UI at port 8501. Reads `bot_state.json`.

## Running

```bash
# Paper trade (default)
python -m polymarket_micro_arb.main

# Dashboard (separate terminal)
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0

# Health check
python scripts/health_check.py

# Daily report
python scripts/daily_report.py
```

## Trading Modes

| Mode | .env Value | Description |
|------|-----------|-------------|
| Paper Trade | `paper_trade` | Real data feeds, simulated fills. Default. |
| Live | `live` | Real CLOB orders with real USDC. Requires funded wallet. |
| Backtest | `backtest` | Replays CSV kline data from `data/backtest/`. |

## Risk Rules (never weaken these)

- Max 4% bankroll per trade (Kelly-sized, hard-capped)
- Daily loss -8% → 24h full pause
- 3 consecutive losses → 30min cooldown (auto-resumes)
- Max 8 concurrent bucket positions
- Limit orders only, auto-cancel unfilled after 60s
- Low balance alerts via Telegram

## Environment

- Python 3.11+
- Key deps: `py-clob-client`, `websockets`, `aiohttp`, `pydantic`, `loguru`, `streamlit`
- Config via `.env` (see `.env.example`)
- Secrets: `PRIVATE_KEY` (Polygon EOA) — NEVER log or display this

## Common Tasks

**Check if bot is healthy:** `python scripts/health_check.py`
**View live dashboard:** `http://server-ip:8501`
**Check logs:** `ls logs/` — JSON structured, rotated at 50MB
**Generate daily report:** `python scripts/daily_report.py`
**Force unpause risk engine:** Not available via script — restart the bot

## When Making Changes

- Always test in `paper_trade` mode first
- Never weaken risk parameters without explicit user approval
- The strategy's 15-45s window and 0.35% threshold are tuned for high winrate — don't lower them
- All WebSocket clients auto-reconnect; don't add manual restart logic
- State file `bot_state.json` is the bridge between bot and dashboard — don't change its schema without updating both sides
- Commit messages should describe what changed AND why

## Monitoring Agents

- **Health check** (`scripts/health_check.py`): Run every 5 min via `/loop`. Checks process, state freshness, connections, risk status, log errors. Exit code 0=healthy, 1=warning, 2=critical.
- **Daily report** (`scripts/daily_report.py`): Run daily via `/schedule`. Generates full P&L summary with ASCII equity curve, saves to `reports/`, sends Telegram digest.
