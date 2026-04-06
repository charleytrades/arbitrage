"""Live prediction loop. Entry point: python -m predictor predict."""

from __future__ import annotations

import argparse
import asyncio
import time

from predictor.config import settings
from predictor.data.fetcher import BinanceKlineFetcher, load_klines
from predictor.model.predictor import LivePredictor
from predictor.utils.logger import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Run live predictions")
    parser.add_argument("--interval", type=int, default=60, help="Refresh interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    predictor = LivePredictor()

    if not predictor.available_models:
        print("No trained models found. Run 'python -m predictor train' first.")
        return

    print(f"Loaded {len(predictor.available_models)} models")
    print(f"Symbols: {', '.join(settings.symbol_list)}")
    print(f"Horizons: {', '.join(settings.horizon_list)}")
    print()

    if args.once:
        asyncio.run(_predict_once(predictor))
    else:
        asyncio.run(_predict_loop(predictor, args.interval))


async def _predict_once(predictor: LivePredictor) -> None:
    """Fetch latest data and generate predictions."""
    fetcher = BinanceKlineFetcher()

    for symbol in settings.symbol_list:
        # Fetch recent candles (only need ~200 for feature warmup)
        df_dict: dict = {}
        for tf in settings.timeframe_list:
            # Try disk first, fetch fresh if stale
            existing = load_klines(symbol, tf)
            if existing is not None and len(existing) > 100:
                df_dict[tf] = existing.tail(500)
            else:
                df = await fetcher.fetch_symbol(symbol, tf, days=3)
                df_dict[tf] = df

        if not df_dict:
            continue

        base = settings.timeframe_list[0]

        # Ensemble prediction
        ensemble = predictor.predict_ensemble(symbol, df_dict, base_interval=base)
        if ensemble is None:
            print(f"{symbol}: No prediction available")
            continue

        # Print ensemble
        arrow = "^" if ensemble.direction == "UP" else "v"
        conf_bar = "#" * int(ensemble.confidence * 20) + "-" * (20 - int(ensemble.confidence * 20))
        print(f"{symbol}  {arrow} {ensemble.direction:>4s}  "
              f"P(up)={ensemble.weighted_prob_up:.1%}  "
              f"Conf=[{conf_bar}] {ensemble.confidence:.1%}")

        # Individual horizons
        for horizon, pred in sorted(ensemble.predictions.items()):
            weight = settings.ensemble_weights.get(horizon, 0)
            print(f"  {horizon:>4s}: P(up)={pred.prob_up:.1%}  "
                  f"Conf={pred.confidence:.1%}  "
                  f"Weight={weight:.0%}  "
                  f"Age={pred.model_age_hours:.0f}h")
        print()


async def _predict_loop(predictor: LivePredictor, interval: int) -> None:
    """Continuous prediction loop."""
    fetcher = BinanceKlineFetcher()

    while True:
        print(f"\n{'=' * 60}")
        print(f"  PREDICTIONS — {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
        print(f"{'=' * 60}\n")

        await _predict_once(predictor)

        print(f"Next update in {interval}s...")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    main()
