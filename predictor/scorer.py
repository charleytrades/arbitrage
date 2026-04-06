"""Live prediction scorer — logs predictions and verifies them against reality.

Runs continuously: every N minutes, makes a prediction, then waits for the
horizon to pass, fetches the actual price, and scores the prediction.
Results are appended to a JSONL file for analysis and displayed on the dashboard.

This is the honest out-of-sample test. No backtesting tricks — just
"I predicted UP, did it actually go up?"
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import httpx
import pandas as pd

from predictor.config import settings
from predictor.data.fetcher import BinanceKlineFetcher, load_klines, save_klines
from predictor.model.predictor import LivePredictor
from predictor.utils.logger import logger

SCORES_FILE = Path(settings.report_dir) / "live_scores.jsonl"
SUMMARY_FILE = Path(settings.report_dir) / "live_summary.json"


@dataclass
class ScoredPrediction:
    """A prediction that has been verified against actual outcome."""

    timestamp: float
    symbol: str
    horizon: str
    predicted_direction: str  # "UP" or "DOWN"
    prob_up: float
    confidence: float
    price_at_prediction: float
    price_at_resolution: float = 0.0
    actual_direction: str = ""  # "UP" or "DOWN"
    correct: bool = False
    resolved: bool = False
    resolved_at: float = 0.0


@dataclass
class LiveSummary:
    """Rolling summary of live prediction accuracy."""

    total_predictions: int = 0
    resolved: int = 0
    correct: int = 0
    accuracy: float = 0.0
    accuracy_by_symbol: dict[str, float] = field(default_factory=dict)
    accuracy_by_horizon: dict[str, float] = field(default_factory=dict)
    accuracy_high_confidence: float = 0.0  # Confidence > 20%
    last_updated: str = ""


class PredictionScorer:
    """Logs predictions, waits for outcomes, scores them."""

    def __init__(self) -> None:
        self.predictor = LivePredictor()
        self.fetcher = BinanceKlineFetcher()
        self._pending: list[ScoredPrediction] = []
        self._resolved: list[ScoredPrediction] = []
        self._load_history()

    def _load_history(self) -> None:
        """Load previously scored predictions from disk."""
        if SCORES_FILE.exists():
            with open(SCORES_FILE) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        sp = ScoredPrediction(**data)
                        if sp.resolved:
                            self._resolved.append(sp)
                        else:
                            self._pending.append(sp)
                    except (json.JSONDecodeError, TypeError):
                        continue
        logger.info(
            "Scorer loaded history",
            resolved=len(self._resolved),
            pending=len(self._pending),
        )

    async def run(self, interval_sec: int = 300) -> None:
        """Main loop: predict, wait, score, repeat.

        Default interval: 5 minutes (matches shortest horizon).
        """
        logger.info("Prediction scorer started", interval=f"{interval_sec}s")

        while True:
            try:
                # Step 1: Resolve any pending predictions whose horizon has passed
                await self._resolve_pending()

                # Step 2: Make new predictions
                await self._make_predictions()

                # Step 3: Update summary
                self._write_summary()

                await asyncio.sleep(interval_sec)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Scorer error", error=str(exc))
                await asyncio.sleep(30)

    async def _make_predictions(self) -> None:
        """Generate predictions for all symbols and horizons."""
        for symbol in settings.symbol_list:
            # Fetch fresh candles
            df_dict: dict[str, pd.DataFrame] = {}
            for tf in settings.timeframe_list:
                existing = load_klines(symbol, tf)
                if existing is not None and len(existing) > 100:
                    # Fetch just the latest few candles to update
                    fresh = await self.fetcher.fetch_symbol(symbol, tf, days=1)
                    save_klines(fresh, symbol, tf)
                    df_dict[tf] = fresh.tail(500)
                else:
                    df = await self.fetcher.fetch_symbol(symbol, tf, days=3)
                    save_klines(df, symbol, tf)
                    df_dict[tf] = df

            if not df_dict:
                continue

            base = settings.timeframe_list[0]

            # Get current price
            current_price = 0.0
            if base in df_dict and len(df_dict[base]) > 0:
                current_price = float(df_dict[base].iloc[-1]["close"])

            if current_price <= 0:
                continue

            # Predict each horizon
            for horizon in settings.horizon_list:
                pred = self.predictor.predict(symbol, horizon, df_dict, base_interval=base)
                if pred is None:
                    continue

                sp = ScoredPrediction(
                    timestamp=time.time(),
                    symbol=symbol,
                    horizon=horizon,
                    predicted_direction="UP" if pred.prob_up >= 0.5 else "DOWN",
                    prob_up=pred.prob_up,
                    confidence=pred.confidence,
                    price_at_prediction=current_price,
                )
                self._pending.append(sp)
                self._append_to_file(sp)

                logger.info(
                    "Prediction logged",
                    symbol=symbol,
                    horizon=horizon,
                    direction=sp.predicted_direction,
                    prob_up=f"{pred.prob_up:.1%}",
                    confidence=f"{pred.confidence:.1%}",
                    price=f"{current_price:.2f}",
                )

    async def _resolve_pending(self) -> None:
        """Check pending predictions whose horizon has elapsed."""
        from predictor.constants import INTERVAL_MINUTES

        now = time.time()
        still_pending: list[ScoredPrediction] = []

        for sp in self._pending:
            horizon_sec = INTERVAL_MINUTES.get(sp.horizon, 5) * 60
            resolve_time = sp.timestamp + horizon_sec

            if now < resolve_time:
                still_pending.append(sp)
                continue

            # Horizon has passed — fetch current price to score
            try:
                price_now = await self._get_current_price(sp.symbol)
                if price_now <= 0:
                    still_pending.append(sp)  # Try again later
                    continue

                sp.price_at_resolution = price_now
                sp.actual_direction = "UP" if price_now > sp.price_at_prediction else "DOWN"
                sp.correct = sp.predicted_direction == sp.actual_direction
                sp.resolved = True
                sp.resolved_at = now

                self._resolved.append(sp)
                self._append_to_file(sp)  # Overwrite with resolved version

                result = "CORRECT" if sp.correct else "WRONG"
                logger.info(
                    f"Prediction {result}",
                    symbol=sp.symbol,
                    horizon=sp.horizon,
                    predicted=sp.predicted_direction,
                    actual=sp.actual_direction,
                    price_start=f"{sp.price_at_prediction:.2f}",
                    price_end=f"{sp.price_at_resolution:.2f}",
                    move=f"{(price_now / sp.price_at_prediction - 1) * 100:.3f}%",
                )

            except Exception as exc:
                logger.debug("Price fetch failed for scoring", error=str(exc))
                still_pending.append(sp)

        self._pending = still_pending

    async def _get_current_price(self, symbol: str) -> float:
        """Fetch the latest price for a symbol from Binance."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{settings.binance_rest_base}/api/v3/ticker/price",
                    params={"symbol": symbol},
                )
                resp.raise_for_status()
                return float(resp.json()["price"])
        except Exception:
            return 0.0

    def _append_to_file(self, sp: ScoredPrediction) -> None:
        """Append a scored prediction to the JSONL file."""
        SCORES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SCORES_FILE, "a") as f:
            f.write(json.dumps(asdict(sp)) + "\n")

    def _write_summary(self) -> None:
        """Write rolling accuracy summary to JSON for the dashboard."""
        resolved = self._resolved
        if not resolved:
            return

        summary = LiveSummary(
            total_predictions=len(self._pending) + len(resolved),
            resolved=len(resolved),
            correct=sum(1 for r in resolved if r.correct),
            last_updated=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        )
        summary.accuracy = summary.correct / max(summary.resolved, 1)

        # By symbol
        for symbol in settings.symbol_list:
            sym_resolved = [r for r in resolved if r.symbol == symbol]
            if sym_resolved:
                acc = sum(1 for r in sym_resolved if r.correct) / len(sym_resolved)
                summary.accuracy_by_symbol[symbol] = round(acc, 4)

        # By horizon
        for horizon in settings.horizon_list:
            h_resolved = [r for r in resolved if r.horizon == horizon]
            if h_resolved:
                acc = sum(1 for r in h_resolved if r.correct) / len(h_resolved)
                summary.accuracy_by_horizon[horizon] = round(acc, 4)

        # High confidence only (>20%)
        high_conf = [r for r in resolved if r.confidence > 0.2]
        if high_conf:
            summary.accuracy_high_confidence = round(
                sum(1 for r in high_conf if r.correct) / len(high_conf), 4
            )

        SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SUMMARY_FILE, "w") as f:
            json.dump(asdict(summary), f, indent=2)

    def get_summary(self) -> LiveSummary | None:
        """Read the current summary from disk."""
        if not SUMMARY_FILE.exists():
            return None
        try:
            with open(SUMMARY_FILE) as f:
                data = json.load(f)
            return LiveSummary(**data)
        except Exception:
            return None

    def get_recent_scores(self, limit: int = 50) -> list[dict]:
        """Get recent scored predictions for the dashboard."""
        recent = self._resolved[-limit:]
        return [asdict(r) for r in reversed(recent)]
