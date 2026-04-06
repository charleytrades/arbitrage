"""Backtest CLI. Entry point: python -m predictor backtest."""

from __future__ import annotations

import argparse
from dataclasses import asdict

from predictor.backtest.evaluator import run_all_backtests
from predictor.config import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Run prediction backtests")
    parser.add_argument("--symbol", type=str, default="", help="Single symbol (default: all)")
    parser.add_argument("--horizon", type=str, default="", help="Single horizon (default: all)")
    args = parser.parse_args()

    if args.symbol:
        settings.symbols = args.symbol.upper()
    if args.horizon:
        settings.horizons = args.horizon

    print(f"Running backtests for {', '.join(settings.symbol_list)}")
    print(f"Horizons: {', '.join(settings.horizon_list)}")
    print()

    reports = run_all_backtests()

    # Print summary table
    print("\n" + "=" * 80)
    print("BACKTEST RESULTS")
    print("=" * 80)
    print(f"{'Symbol':<10} {'Horizon':>8} {'Accuracy':>10} {'AUC':>8} "
          f"{'Trades':>8} {'WinRate':>10} {'P&L':>8}")
    print("-" * 80)

    for r in reports:
        print(
            f"{r.symbol:<10} {r.horizon:>8} {r.accuracy:>10.4f} {r.auc:>8.4f} "
            f"{r.simulated_trades:>8} {r.simulated_win_rate:>10.4f} {r.simulated_pnl:>8.1f}"
        )

    print("-" * 80)

    # Calibration
    print("\nCALIBRATION (predicted probability bin -> actual up %)")
    for r in reports:
        if r.calibration:
            print(f"\n  {r.symbol} {r.horizon}:")
            for bin_label, val in r.calibration.items():
                print(f"    {bin_label}: {val}")

    # Plots
    print("\nPlots saved:")
    for r in reports:
        for p in r.plots:
            print(f"  {p}")

    print()


if __name__ == "__main__":
    main()
