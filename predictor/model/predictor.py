"""Live predictor — loads trained models and outputs predictions.

Supports individual model predictions and weighted ensemble across
multiple horizons for a single directional view.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from predictor.config import settings
from predictor.constants import ALL_FEATURES
from predictor.features.pipeline import build_features, get_feature_columns
from predictor.utils.logger import logger


@dataclass
class Prediction:
    """Single model prediction."""

    symbol: str
    horizon: str
    prob_up: float
    prob_down: float
    confidence: float  # 0-1, how far from 50/50
    timestamp: float = field(default_factory=time.time)
    model_age_hours: float = 0.0


@dataclass
class EnsemblePrediction:
    """Weighted ensemble prediction across multiple horizons."""

    symbol: str
    direction: str  # "UP" or "DOWN"
    weighted_prob_up: float
    confidence: float
    predictions: dict[str, Prediction] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class LivePredictor:
    """Loads trained models and generates predictions from recent candle data.

    Prediction latency target: <10ms per symbol.
    """

    def __init__(self, model_dir: str | None = None) -> None:
        self.model_dir = Path(model_dir or settings.model_dir)
        # {(symbol, horizon): (model, feature_list, trained_at)}
        self._models: dict[tuple[str, str], tuple[xgb.XGBClassifier, list[str], str]] = {}
        self._load_all_models()

    def _load_all_models(self) -> None:
        """Load all trained models from disk."""
        if not self.model_dir.exists():
            logger.warning("Model directory not found", path=str(self.model_dir))
            return

        for meta_path in self.model_dir.glob("*_meta.json"):
            try:
                with open(meta_path) as f:
                    meta = json.load(f)

                model_path = meta_path.with_suffix("").with_suffix(".json")
                # meta file is NAME_meta.json, model is NAME.json
                model_name = meta_path.stem.replace("_meta", "")
                model_path = self.model_dir / f"{model_name}.json"

                if not model_path.exists():
                    continue

                model = xgb.XGBClassifier()
                model.load_model(str(model_path))

                key = (meta["symbol"], meta["horizon"])
                self._models[key] = (model, meta["features"], meta.get("trained_at", ""))

                logger.info(
                    "Model loaded",
                    symbol=meta["symbol"],
                    horizon=meta["horizon"],
                    features=len(meta["features"]),
                )
            except Exception as exc:
                logger.error("Failed to load model", path=str(meta_path), error=str(exc))

        logger.info("Models loaded", total=len(self._models))

    def predict(
        self,
        symbol: str,
        horizon: str,
        df_dict: dict[str, pd.DataFrame],
        base_interval: str = "1m",
    ) -> Prediction | None:
        """Generate a prediction for one symbol/horizon.

        Args:
            symbol: e.g., "BTCUSDT"
            horizon: e.g., "5m", "15m", "1h"
            df_dict: {interval: DataFrame} with recent OHLCV data.
            base_interval: Base candle interval of the data.

        Returns:
            Prediction or None if model not available.
        """
        key = (symbol, horizon)
        if key not in self._models:
            return None

        model, feature_list, trained_at = self._models[key]

        # Build features from recent data
        df = build_features(df_dict, base_interval=base_interval)
        if df.empty:
            return None

        # Take the last row (most recent candle)
        available_features = [f for f in feature_list if f in df.columns]
        if len(available_features) < len(feature_list) * 0.8:
            logger.warning(
                "Too many missing features",
                symbol=symbol,
                horizon=horizon,
                available=len(available_features),
                expected=len(feature_list),
            )
            return None

        # Fill missing features with 0
        last_row = df.iloc[[-1]][feature_list].fillna(0)

        prob = model.predict_proba(last_row)[0]
        prob_up = float(prob[1])
        prob_down = float(prob[0])
        confidence = abs(prob_up - 0.5) * 2  # Scale 0-1

        # Model age
        age_hours = 0.0
        if trained_at:
            try:
                trained_ts = pd.Timestamp(trained_at).timestamp()
                age_hours = (time.time() - trained_ts) / 3600
            except Exception:
                pass

        return Prediction(
            symbol=symbol,
            horizon=horizon,
            prob_up=round(prob_up, 4),
            prob_down=round(prob_down, 4),
            confidence=round(confidence, 4),
            model_age_hours=round(age_hours, 1),
        )

    def predict_ensemble(
        self,
        symbol: str,
        df_dict: dict[str, pd.DataFrame],
        base_interval: str = "1m",
    ) -> EnsemblePrediction | None:
        """Weighted ensemble prediction across all horizons for a symbol.

        Combines 5m, 15m, 1h predictions with configurable weights.
        Longer timeframes get more weight by default (they're more predictable).
        """
        predictions: dict[str, Prediction] = {}
        weights = settings.ensemble_weights

        for horizon in settings.horizon_list:
            pred = self.predict(symbol, horizon, df_dict, base_interval)
            if pred is not None:
                predictions[horizon] = pred

        if not predictions:
            return None

        # Weighted average of prob_up
        total_weight = 0.0
        weighted_sum = 0.0
        for horizon, pred in predictions.items():
            w = weights.get(horizon, 1.0 / len(predictions))
            weighted_sum += pred.prob_up * w
            total_weight += w

        if total_weight > 0:
            weighted_prob_up = weighted_sum / total_weight
        else:
            weighted_prob_up = 0.5

        confidence = abs(weighted_prob_up - 0.5) * 2
        direction = "UP" if weighted_prob_up >= 0.5 else "DOWN"

        return EnsemblePrediction(
            symbol=symbol,
            direction=direction,
            weighted_prob_up=round(weighted_prob_up, 4),
            confidence=round(confidence, 4),
            predictions=predictions,
        )

    @property
    def available_models(self) -> list[tuple[str, str]]:
        """List of (symbol, horizon) pairs with loaded models."""
        return list(self._models.keys())
