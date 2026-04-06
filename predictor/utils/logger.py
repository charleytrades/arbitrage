"""Logger for the predictor module."""

from __future__ import annotations

try:
    from polymarket_micro_arb.utils.logger import logger
except ImportError:
    from loguru import logger

    logger.add(
        "logs/predictor_{time:YYYY-MM-DD}.log",
        rotation="50 MB",
        retention="14 days",
        serialize=True,
        level="INFO",
    )

__all__ = ["logger"]
