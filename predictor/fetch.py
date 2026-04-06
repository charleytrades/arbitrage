"""Fetch historical klines from Binance. Entry point: python -m predictor fetch."""

from __future__ import annotations

import argparse
import asyncio

from predictor.config import settings
from predictor.data.fetcher import BinanceKlineFetcher, list_available
from predictor.utils.logger import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Binance klines")
    parser.add_argument("--days", type=int, default=settings.fetch_days)
    parser.add_argument("--symbol", type=str, default="", help="Single symbol (default: all)")
    args = parser.parse_args()

    asyncio.run(_fetch(args.days, args.symbol))


async def _fetch(days: int, symbol: str) -> None:
    fetcher = BinanceKlineFetcher()

    if symbol:
        # Fetch single symbol
        for tf in settings.timeframe_list:
            from predictor.data.fetcher import save_klines
            df = await fetcher.fetch_symbol(symbol.upper(), tf, days)
            save_klines(df, symbol.upper(), tf)
    else:
        await fetcher.fetch_all(days)

    # Print inventory
    available = list_available()
    print("\n=== Data Inventory ===")
    for sym, intervals in sorted(available.items()):
        print(f"  {sym}: {', '.join(intervals)}")
    print()


if __name__ == "__main__":
    main()
