#!/bin/bash
# setup_predictor.sh — One-shot setup and run for the crypto predictor
set -e

echo "=== Step 1: Install dependencies ==="
pip install xgboost scikit-learn pandas-ta pyarrow matplotlib 2>&1 | tail -5

echo ""
echo "=== Step 2: Fetch 90 days of historical data from Binance ==="
python -m predictor fetch --days 90

echo ""
echo "=== Step 3: Train models ==="
python -m predictor train

echo ""
echo "=== Step 4: Backtest ==="
python -m predictor backtest

echo ""
echo "=== Step 5: One-shot prediction ==="
python -m predictor predict --once

echo ""
echo "=== Step 6: Launch dashboard on port 8502 ==="
streamlit run predictor_dashboard.py --server.port 8502 --server.address 0.0.0.0 &
echo "Dashboard running at http://$(hostname -I | awk '{print $1}'):8502"

echo ""
echo "=== DONE ==="
