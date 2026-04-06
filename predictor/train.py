"""Train all models. Entry point: python -m predictor train."""

from __future__ import annotations

import argparse

from predictor.config import settings
from predictor.model.trainer import train_all
from predictor.utils.logger import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Train prediction models")
    parser.add_argument("--train-days", type=int, default=settings.train_days)
    parser.add_argument("--test-days", type=int, default=settings.test_days)
    args = parser.parse_args()

    settings.train_days = args.train_days
    settings.test_days = args.test_days

    print(f"Training models for {', '.join(settings.symbol_list)}")
    print(f"Horizons: {', '.join(settings.horizon_list)}")
    print(f"Train window: {settings.train_days}d, Test window: {settings.test_days}d")
    print()

    results = train_all()

    # Print summary
    print("\n" + "=" * 70)
    print("TRAINING RESULTS")
    print("=" * 70)

    for r in results:
        status = "OK" if r.mean_accuracy > 0.5 else "WEAK"
        print(
            f"  [{status}] {r.symbol} {r.horizon:>4s}  "
            f"Acc={r.mean_accuracy:.4f}  AUC={r.mean_auc:.4f}  "
            f"LogLoss={r.mean_log_loss:.4f}  "
            f"Folds={len(r.folds)}  Time={r.train_duration_sec}s"
        )
        if r.feature_importance:
            top3 = list(r.feature_importance.items())[:3]
            feat_str = ", ".join(f"{k}={v}" for k, v in top3)
            print(f"         Top features: {feat_str}")

    print()

    # Overall summary
    good = [r for r in results if r.mean_accuracy > 0.52]
    weak = [r for r in results if 0.5 <= r.mean_accuracy <= 0.52]
    bad = [r for r in results if r.mean_accuracy < 0.5]

    print(f"Models above 52% accuracy: {len(good)}/{len(results)}")
    if good:
        best = max(good, key=lambda r: r.mean_accuracy)
        print(f"Best: {best.symbol} {best.horizon} — {best.mean_accuracy:.4f}")
    if bad:
        print(f"Below 50% (underperforming): {len(bad)}")

    print()


if __name__ == "__main__":
    main()
