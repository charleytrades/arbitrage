"""Live scoring loop. Entry point: python -m predictor score."""

from __future__ import annotations

import argparse
import asyncio

from predictor.config import settings
from predictor.scorer import PredictionScorer
from predictor.utils.logger import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Score live predictions against reality")
    parser.add_argument(
        "--interval", type=int, default=300,
        help="Seconds between prediction rounds (default: 300 = 5min)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  LIVE PREDICTION SCORER")
    print("=" * 60)
    print(f"Symbols: {', '.join(settings.symbol_list)}")
    print(f"Horizons: {', '.join(settings.horizon_list)}")
    print(f"Interval: {args.interval}s")
    print()
    print("Every 5 minutes:")
    print("  1. Predict direction for each symbol/horizon")
    print("  2. Wait for the horizon to pass")
    print("  3. Check actual price and score the prediction")
    print("  4. Log results to data/predictor/reports/live_scores.jsonl")
    print()
    print("Press Ctrl+C to stop.")
    print("=" * 60)
    print()

    scorer = PredictionScorer()
    try:
        asyncio.run(scorer.run(interval_sec=args.interval))
    except KeyboardInterrupt:
        print("\nStopped. Results saved to data/predictor/reports/")


if __name__ == "__main__":
    main()
