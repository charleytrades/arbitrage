"""Lightweight async Telegram alerting via httpx.

We use httpx directly instead of python-telegram-bot to keep the
dependency footprint minimal and avoid blocking the event loop.
"""

from __future__ import annotations

import httpx

from polymarket_micro_arb.config import settings
from polymarket_micro_arb.utils.logger import logger

_BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"


async def send_alert(message: str, parse_mode: str = "HTML") -> bool:
    """Send a Telegram message. Returns True on success."""
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id

    if not token or not chat_id:
        logger.debug("Telegram not configured – skipping alert")
        return False

    url = _BASE_URL.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            logger.debug("Telegram alert sent", status=resp.status_code)
            return True
    except httpx.HTTPError as exc:
        logger.warning("Telegram alert failed", error=str(exc))
        return False


async def send_trade_alert(
    action: str,
    market_slug: str,
    side: str,
    outcome: str,
    size: float,
    price: float,
    edge: float,
) -> bool:
    """Format and send a trade execution alert."""
    msg = (
        f"<b>{action}</b>\n"
        f"Market: <code>{market_slug}</code>\n"
        f"Side: {side} {outcome}\n"
        f"Size: ${size:.2f} @ {price:.4f}\n"
        f"Edge: {edge:.4f}"
    )
    return await send_alert(msg)


async def send_risk_alert(reason: str, details: str = "") -> bool:
    """Send a risk / kill-switch alert."""
    msg = f"⚠️ <b>RISK ALERT</b>\n{reason}"
    if details:
        msg += f"\n<pre>{details}</pre>"
    return await send_alert(msg)
