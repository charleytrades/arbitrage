"""Predictor configuration — standalone from the arb bot."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class PredictorSettings(BaseSettings):
    """All predictor settings with sensible defaults. No .env required."""

    # Symbols
    symbols: str = Field("BTCUSDT,ETHUSDT,SOLUSDT", alias="PREDICTOR_SYMBOLS")

    # Timeframes for feature computation
    timeframes: str = Field("1m,5m,15m", alias="PREDICTOR_TIMEFRAMES")

    # Prediction horizons (what we predict: "price up in next Xm?")
    horizons: str = Field("5m,15m,1h", alias="PREDICTOR_HORIZONS")

    # Data storage
    data_dir: str = Field(
        str(_PROJECT_ROOT / "data" / "predictor" / "klines"),
        alias="PREDICTOR_DATA_DIR",
    )
    model_dir: str = Field(
        str(_PROJECT_ROOT / "data" / "predictor" / "models"),
        alias="PREDICTOR_MODEL_DIR",
    )
    report_dir: str = Field(
        str(_PROJECT_ROOT / "data" / "predictor" / "reports"),
        alias="PREDICTOR_REPORT_DIR",
    )

    # Training
    train_days: int = Field(30, alias="PREDICTOR_TRAIN_DAYS")
    test_days: int = Field(7, alias="PREDICTOR_TEST_DAYS")
    fetch_days: int = Field(90, alias="PREDICTOR_FETCH_DAYS")

    # Binance REST
    binance_rest_base: str = Field(
        "https://api.binance.com", alias="BINANCE_REST_BASE"
    )

    # XGBoost
    xgb_max_depth: int = 4
    xgb_n_estimators: int = 200
    xgb_learning_rate: float = 0.05
    xgb_early_stopping: int = 20

    # Ensemble weights (sum to 1.0)
    # Longer horizons get more weight — they're more predictable
    ensemble_weight_5m: float = Field(0.2, alias="ENSEMBLE_WEIGHT_5M")
    ensemble_weight_15m: float = Field(0.35, alias="ENSEMBLE_WEIGHT_15M")
    ensemble_weight_1h: float = Field(0.45, alias="ENSEMBLE_WEIGHT_1H")

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols.split(",")]

    @property
    def timeframe_list(self) -> list[str]:
        return [t.strip() for t in self.timeframes.split(",")]

    @property
    def horizon_list(self) -> list[str]:
        return [h.strip() for h in self.horizons.split(",")]

    @property
    def ensemble_weights(self) -> dict[str, float]:
        return {
            "5m": self.ensemble_weight_5m,
            "15m": self.ensemble_weight_15m,
            "1h": self.ensemble_weight_1h,
        }


settings = PredictorSettings()  # type: ignore[call-arg]
