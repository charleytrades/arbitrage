"""Walk-forward XGBoost trainer.

Trains one model per (symbol, base_interval, horizon) combination.
Walk-forward: train on [t, t+30d], test on [t+30d, t+37d], slide forward.
Final production model trained on the most recent train_days of data.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

from predictor.config import settings
from predictor.features.pipeline import build_features, get_feature_columns
from predictor.model.target import add_target
from predictor.utils.logger import logger


@dataclass
class FoldResult:
    """Metrics from one walk-forward fold."""

    fold: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    train_size: int
    test_size: int
    accuracy: float
    auc: float
    log_loss_val: float
    up_ratio_train: float
    up_ratio_test: float


@dataclass
class TrainResult:
    """Overall training result for one model."""

    symbol: str
    base_interval: str
    horizon: str
    folds: list[FoldResult] = field(default_factory=list)
    mean_accuracy: float = 0.0
    mean_auc: float = 0.0
    mean_log_loss: float = 0.0
    model_path: str = ""
    feature_importance: dict[str, float] = field(default_factory=dict)
    train_duration_sec: float = 0.0


class WalkForwardTrainer:
    """Walk-forward cross-validated XGBoost trainer."""

    def __init__(
        self,
        symbol: str,
        base_interval: str = "5m",
        horizon: str = "5m",
        train_days: int | None = None,
        test_days: int | None = None,
    ) -> None:
        self.symbol = symbol
        self.base_interval = base_interval
        self.horizon = horizon
        self.train_days = train_days or settings.train_days
        self.test_days = test_days or settings.test_days

    def train(self, df_dict: dict[str, pd.DataFrame]) -> TrainResult:
        """Run walk-forward training and save the final model.

        Args:
            df_dict: {interval: DataFrame} with OHLCV data for one symbol.

        Returns:
            TrainResult with fold metrics and model path.
        """
        t0 = time.time()
        result = TrainResult(
            symbol=self.symbol,
            base_interval=self.base_interval,
            horizon=self.horizon,
        )

        # Build features
        df = build_features(df_dict, base_interval=self.base_interval)
        df = add_target(df, horizon=self.horizon, base_interval=self.base_interval)

        target_col = f"target_up_{self.horizon}"
        feature_cols = get_feature_columns(df)

        if len(feature_cols) == 0:
            logger.error("No features available", symbol=self.symbol)
            return result

        logger.info(
            "Training",
            symbol=self.symbol,
            interval=self.base_interval,
            horizon=self.horizon,
            rows=len(df),
            features=len(feature_cols),
        )

        # ── Walk-forward splits ──────────────────────────────
        train_td = pd.Timedelta(days=self.train_days)
        test_td = pd.Timedelta(days=self.test_days)

        df_start = df.index.min()
        df_end = df.index.max()

        fold_start = df_start
        fold_num = 0

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

            fold_result = self._train_fold(
                fold_num, X_train, y_train, X_test, y_test
            )
            result.folds.append(fold_result)
            fold_num += 1

            # Slide forward by test_days
            fold_start += test_td

        if result.folds:
            result.mean_accuracy = np.mean([f.accuracy for f in result.folds])
            result.mean_auc = np.mean([f.auc for f in result.folds])
            result.mean_log_loss = np.mean([f.log_loss_val for f in result.folds])

        # ── Train final production model on most recent data ─
        recent_start = df_end - train_td
        recent_mask = df.index >= recent_start
        X_final = df.loc[recent_mask, feature_cols]
        y_final = df.loc[recent_mask, target_col]

        if len(X_final) > 100:
            model = self._fit_model(X_final, y_final)
            model_path = self._save_model(model, feature_cols)
            result.model_path = model_path

            # Feature importance
            importance = model.get_booster().get_score(importance_type="gain")
            # Map from f0, f1... to actual feature names
            fname_map = {f"f{i}": col for i, col in enumerate(feature_cols)}
            result.feature_importance = {
                fname_map.get(k, k): round(v, 2)
                for k, v in sorted(importance.items(), key=lambda x: -x[1])
            }

        result.train_duration_sec = round(time.time() - t0, 2)

        logger.info(
            "Training complete",
            symbol=self.symbol,
            horizon=self.horizon,
            folds=len(result.folds),
            mean_accuracy=f"{result.mean_accuracy:.4f}",
            mean_auc=f"{result.mean_auc:.4f}",
            duration=f"{result.train_duration_sec}s",
        )

        return result

    def _train_fold(
        self,
        fold: int,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: pd.DataFrame,
        y_test: pd.Series,
    ) -> FoldResult:
        """Train and evaluate one fold."""
        model = self._fit_model(X_train, y_train, X_test, y_test)

        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)

        acc = accuracy_score(y_test, y_pred)
        try:
            auc = roc_auc_score(y_test, y_prob)
        except ValueError:
            auc = 0.5  # Single class in test set
        ll = log_loss(y_test, y_prob, labels=[0, 1])

        return FoldResult(
            fold=fold,
            train_start=str(X_train.index.min()),
            train_end=str(X_train.index.max()),
            test_start=str(X_test.index.min()),
            test_end=str(X_test.index.max()),
            train_size=len(X_train),
            test_size=len(X_test),
            accuracy=round(acc, 4),
            auc=round(auc, 4),
            log_loss_val=round(ll, 4),
            up_ratio_train=round(y_train.mean(), 4),
            up_ratio_test=round(y_test.mean(), 4),
        )

    def _fit_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
    ) -> xgb.XGBClassifier:
        """Fit an XGBoost classifier."""
        # Compute class weight
        n_up = y_train.sum()
        n_down = len(y_train) - n_up
        scale_pos_weight = n_down / max(n_up, 1)

        model = xgb.XGBClassifier(
            max_depth=settings.xgb_max_depth,
            n_estimators=settings.xgb_n_estimators,
            learning_rate=settings.xgb_learning_rate,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            eval_metric="logloss",
            early_stopping_rounds=settings.xgb_early_stopping if X_val is not None else None,
            tree_method="hist",
            random_state=42,
            verbosity=0,
        )

        fit_params: dict = {}
        if X_val is not None and y_val is not None:
            fit_params["eval_set"] = [(X_val, y_val)]

        model.fit(X_train, y_train, **fit_params)
        return model

    def _save_model(self, model: xgb.XGBClassifier, feature_cols: list[str]) -> str:
        """Save model and feature list to disk."""
        model_dir = Path(settings.model_dir)
        model_dir.mkdir(parents=True, exist_ok=True)

        name = f"{self.symbol}_{self.base_interval}_{self.horizon}"
        model_path = str(model_dir / f"{name}.json")
        meta_path = str(model_dir / f"{name}_meta.json")

        model.save_model(model_path)

        # Save metadata (features, training info)
        meta = {
            "symbol": self.symbol,
            "base_interval": self.base_interval,
            "horizon": self.horizon,
            "features": feature_cols,
            "trained_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        logger.info("Model saved", path=model_path)
        return model_path


def train_all() -> list[TrainResult]:
    """Train models for all symbols and horizons. Returns list of results."""
    from predictor.data.fetcher import load_klines

    results: list[TrainResult] = []

    for symbol in settings.symbol_list:
        # Load all timeframe data for this symbol
        df_dict: dict[str, pd.DataFrame] = {}
        for tf in settings.timeframe_list:
            data = load_klines(symbol, tf)
            if data is not None and len(data) > 0:
                df_dict[tf] = data

        if not df_dict:
            logger.warning("No data for symbol", symbol=symbol)
            continue

        # Determine base interval (smallest available)
        base = settings.timeframe_list[0]
        for tf in settings.timeframe_list:
            if tf in df_dict:
                base = tf
                break

        # Train one model per horizon
        for horizon in settings.horizon_list:
            trainer = WalkForwardTrainer(
                symbol=symbol,
                base_interval=base,
                horizon=horizon,
            )
            result = trainer.train(df_dict)
            results.append(result)

    return results
