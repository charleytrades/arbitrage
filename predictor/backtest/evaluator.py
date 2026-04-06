"""Backtester — evaluates predictor accuracy on historical data.

Runs walk-forward evaluation: for each test window, trains a fresh model
on the preceding train window, generates predictions, and compares to
actual outcomes. Produces accuracy metrics, calibration analysis, and
simulated P&L if you had traded the signals.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend for server
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, roc_auc_score

from predictor.config import settings
from predictor.features.pipeline import build_features, get_feature_columns
from predictor.model.target import add_target
from predictor.model.trainer import WalkForwardTrainer
from predictor.utils.logger import logger


@dataclass
class BacktestReport:
    """Full backtest evaluation report."""

    symbol: str
    base_interval: str
    horizon: str
    total_predictions: int = 0
    accuracy: float = 0.0
    auc: float = 0.0
    up_ratio: float = 0.0  # Actual % of "up" outcomes
    predicted_up_ratio: float = 0.0  # % we predicted "up"
    # Calibration: for predictions in each bin, what was the actual up%?
    calibration: dict[str, float] = field(default_factory=dict)
    # Simulated P&L
    simulated_trades: int = 0
    simulated_win_rate: float = 0.0
    simulated_pnl: float = 0.0  # In basis points per trade
    # Paths to saved plots
    plots: list[str] = field(default_factory=list)


def run_backtest(
    symbol: str,
    df_dict: dict[str, pd.DataFrame],
    base_interval: str = "1m",
    horizon: str = "5m",
) -> BacktestReport:
    """Run full walk-forward backtest for one symbol/horizon."""
    report = BacktestReport(
        symbol=symbol,
        base_interval=base_interval,
        horizon=horizon,
    )

    # Build features and target
    df = build_features(df_dict, base_interval=base_interval)
    df = add_target(df, horizon=horizon, base_interval=base_interval)

    target_col = f"target_up_{horizon}"
    feature_cols = get_feature_columns(df)

    if len(feature_cols) == 0 or len(df) < 500:
        logger.warning("Insufficient data for backtest", symbol=symbol, rows=len(df))
        return report

    # Walk-forward splits
    train_td = pd.Timedelta(days=settings.train_days)
    test_td = pd.Timedelta(days=settings.test_days)

    all_predictions: list[dict] = []

    fold_start = df.index.min()
    df_end = df.index.max()

    while fold_start + train_td + test_td <= df_end:
        train_end = fold_start + train_td
        test_end = train_end + test_td

        train_mask = (df.index >= fold_start) & (df.index < train_end)
        test_mask = (df.index >= train_end) & (df.index < test_end)

        X_train = df.loc[train_mask, feature_cols]
        y_train = df.loc[train_mask, target_col]
        X_test = df.loc[test_mask, feature_cols]
        y_test = df.loc[test_mask, target_col]

        if len(X_train) < 100 or len(X_test) < 20:
            fold_start += test_td
            continue

        # Train fresh model on this fold's training data
        import xgboost as xgb

        n_up = y_train.sum()
        n_down = len(y_train) - n_up
        model = xgb.XGBClassifier(
            max_depth=settings.xgb_max_depth,
            n_estimators=settings.xgb_n_estimators,
            learning_rate=settings.xgb_learning_rate,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=n_down / max(n_up, 1),
            eval_metric="logloss",
            early_stopping_rounds=settings.xgb_early_stopping,
            tree_method="hist",
            random_state=42,
            verbosity=0,
        )
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)])

        y_prob = model.predict_proba(X_test)[:, 1]

        for ts, prob, actual in zip(X_test.index, y_prob, y_test):
            all_predictions.append({
                "timestamp": ts,
                "prob_up": float(prob),
                "actual_up": int(actual),
            })

        fold_start += test_td

    if not all_predictions:
        return report

    pred_df = pd.DataFrame(all_predictions)
    pred_df["predicted_up"] = (pred_df["prob_up"] >= 0.5).astype(int)

    # ── Metrics ──────────────────────────────────────────────
    report.total_predictions = len(pred_df)
    report.accuracy = round(accuracy_score(pred_df["actual_up"], pred_df["predicted_up"]), 4)
    try:
        report.auc = round(roc_auc_score(pred_df["actual_up"], pred_df["prob_up"]), 4)
    except ValueError:
        report.auc = 0.5
    report.up_ratio = round(pred_df["actual_up"].mean(), 4)
    report.predicted_up_ratio = round(pred_df["predicted_up"].mean(), 4)

    # ── Calibration ──────────────────────────────────────────
    bins = [0.0, 0.35, 0.45, 0.5, 0.55, 0.65, 1.0]
    bin_labels = ["<35%", "35-45%", "45-50%", "50-55%", "55-65%", ">65%"]
    pred_df["prob_bin"] = pd.cut(pred_df["prob_up"], bins=bins, labels=bin_labels)
    calibration = pred_df.groupby("prob_bin", observed=True)["actual_up"].agg(["mean", "count"])
    report.calibration = {
        str(idx): f"{row['mean']:.3f} (n={int(row['count'])})"
        for idx, row in calibration.iterrows()
    }

    # ── Simulated P&L ────────────────────────────────────────
    # Only trade when confidence > 55%
    confident = pred_df[(pred_df["prob_up"] >= 0.55) | (pred_df["prob_up"] <= 0.45)]
    if len(confident) > 0:
        # Bet direction matches prediction
        confident = confident.copy()
        confident["correct"] = (
            ((confident["prob_up"] >= 0.55) & (confident["actual_up"] == 1))
            | ((confident["prob_up"] <= 0.45) & (confident["actual_up"] == 0))
        )
        report.simulated_trades = len(confident)
        report.simulated_win_rate = round(confident["correct"].mean(), 4)
        # Simple: +1bp per correct, -1bp per incorrect
        report.simulated_pnl = round(
            (confident["correct"].sum() - (~confident["correct"]).sum()) * 1.0, 2
        )

    # ── Plots ────────────────────────────────────────────────
    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    # Accuracy over time
    pred_df = pred_df.set_index("timestamp")
    rolling_acc = pred_df["predicted_up"].eq(pred_df["actual_up"]).rolling(100).mean()

    fig, axes = plt.subplots(3, 1, figsize=(12, 10))

    # Plot 1: Rolling accuracy
    axes[0].plot(rolling_acc.index, rolling_acc.values, linewidth=0.8)
    axes[0].axhline(y=0.5, color="red", linestyle="--", alpha=0.5, label="50% (coin flip)")
    axes[0].set_title(f"{symbol} — {horizon} Prediction Accuracy (100-bar rolling)")
    axes[0].set_ylabel("Accuracy")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # Plot 2: Predicted probabilities distribution
    axes[1].hist(pred_df["prob_up"], bins=50, alpha=0.7, edgecolor="black", linewidth=0.5)
    axes[1].axvline(x=0.5, color="red", linestyle="--", alpha=0.5)
    axes[1].set_title("Predicted Probability Distribution")
    axes[1].set_xlabel("P(up)")
    axes[1].set_ylabel("Count")
    axes[1].grid(True, alpha=0.3)

    # Plot 3: Cumulative P&L (confident trades only)
    if len(confident) > 0:
        cum_pnl = confident["correct"].astype(int).replace(0, -1).cumsum()
        axes[2].plot(cum_pnl.index, cum_pnl.values, linewidth=0.8, color="green")
        axes[2].axhline(y=0, color="red", linestyle="--", alpha=0.5)
        axes[2].set_title(f"Cumulative P&L (confidence > 55%, {report.simulated_trades} trades)")
        axes[2].set_ylabel("Cumulative Units")
        axes[2].grid(True, alpha=0.3)

    plt.tight_layout()
    plot_path = str(report_dir / f"backtest_{symbol}_{horizon}.png")
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    report.plots.append(plot_path)

    logger.info(
        "Backtest complete",
        symbol=symbol,
        horizon=horizon,
        accuracy=report.accuracy,
        auc=report.auc,
        trades=report.simulated_trades,
        win_rate=report.simulated_win_rate,
        plot=plot_path,
    )

    return report


def run_all_backtests() -> list[BacktestReport]:
    """Run backtests for all symbols and horizons."""
    from predictor.data.fetcher import load_klines

    reports: list[BacktestReport] = []

    for symbol in settings.symbol_list:
        df_dict: dict[str, pd.DataFrame] = {}
        for tf in settings.timeframe_list:
            data = load_klines(symbol, tf)
            if data is not None and len(data) > 0:
                df_dict[tf] = data

        if not df_dict:
            continue

        base = settings.timeframe_list[0]
        for tf in settings.timeframe_list:
            if tf in df_dict:
                base = tf
                break

        for horizon in settings.horizon_list:
            report = run_backtest(symbol, df_dict, base_interval=base, horizon=horizon)
            reports.append(report)

    return reports
