"""Structured JSON logging via Loguru.

Usage:
    from polymarket_micro_arb.utils.logger import logger
    logger.info("message", extra_field=42)
"""

from __future__ import annotations

import sys

from loguru import logger as _loguru_logger

# Remove default handler so we control the format
_loguru_logger.remove()

# ── Human-readable handler to stderr (what you see in terminal) ─────
_loguru_logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    backtrace=True,
    diagnose=False,
    enqueue=True,
)

# ── Rotating file handler ──────────────────────────────────────────
_loguru_logger.add(
    "logs/bot_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    serialize=True,
    rotation="50 MB",
    retention="7 days",
    compression="gz",
    enqueue=True,
)

# Re-export as `logger` for convenience
logger = _loguru_logger
