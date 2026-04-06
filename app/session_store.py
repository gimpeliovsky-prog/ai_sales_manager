import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as aioredis

from app.config import get_settings

logger = logging.getLogger(__name__)
_redis: aioredis.Redis | None = None
_DIALOG_STALE_AFTER = timedelta(hours=24)
_ORDER_STALE_AFTER = timedelta(hours=12)
_PENDING_CONFIRMATION_TTL = timedelta(minutes=20)


async def init_redis() -> None:
    global _redis
    settings = get_settings()
    _redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    await _redis.ping()
    logger.info("Redis OK")


def _client() -> aioredis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialised")
    return _redis


def _key(channel: str, uid: str) -> str:
    return f"ai_session:{channel}:{uid}"


def _empty_session() -> dict[str, Any]:
    return {
        "messages": [],
        "company_code": None,
        "erp_customer_id": None,
        "buyer_name": None,
        "buyer_identity_id": None,
        "buyer_phone": None,
        "buyer_recognized_via": None,
        "recent_sales_orders": [],
        "recent_sales_invoices": [],
        "returning_customer_announced": False,
        "lang": None,
        "stage": "new",
        "stage_confidence": 0.0,
        "behavior_class": "unclear_request",
        "behavior_confidence": 0.0,
        "last_intent": None,
        "last_intent_confidence": 0.0,
        "failed_clarification_count": 0,
        "handoff_required": False,
        "handoff_reason": None,
        "last_channel": None,
        "last_sales_order_name": None,
        "pending_confirmation_text": None,
        "last_interaction_at": None,
        "last_order_activity_at": None,
        "pending_confirmation_set_at": None,
    }


def new_session(*, company_code: str | None = None) -> dict[str, Any]:
    session = _empty_session()
    session["company_code"] = company_code
    return session


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _cleanup_session(session: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(UTC)
    last_interaction_at = _parse_dt(session.get("last_interaction_at"))
    if last_interaction_at and now - last_interaction_at > _DIALOG_STALE_AFTER:
        session["messages"] = []
        session["last_sales_order_name"] = None
        session["pending_confirmation_text"] = None
        session["last_order_activity_at"] = None
        session["pending_confirmation_set_at"] = None
        session["returning_customer_announced"] = False

    last_order_activity_at = _parse_dt(session.get("last_order_activity_at"))
    if last_order_activity_at and now - last_order_activity_at > _ORDER_STALE_AFTER:
        session["last_sales_order_name"] = None
        session["last_order_activity_at"] = None

    pending_confirmation_set_at = _parse_dt(session.get("pending_confirmation_set_at"))
    if pending_confirmation_set_at and now - pending_confirmation_set_at > _PENDING_CONFIRMATION_TTL:
        session["pending_confirmation_text"] = None
        session["pending_confirmation_set_at"] = None

    return session


async def load_session(channel: str, uid: str) -> dict[str, Any]:
    raw = await _client().get(_key(channel, uid))
    if not raw:
        return new_session()
    try:
        session = json.loads(raw)
        if not isinstance(session, dict):
            return new_session()
        merged = new_session()
        merged.update(session)
        return _cleanup_session(merged)
    except json.JSONDecodeError:
        return new_session()


async def save_session(channel: str, uid: str, session: dict[str, Any]) -> None:
    settings = get_settings()
    now_iso = datetime.now(UTC).isoformat()
    session["messages"] = session.get("messages", [])[-40:]
    session["last_interaction_at"] = now_iso
    if session.get("last_sales_order_name"):
        session["last_order_activity_at"] = session.get("last_order_activity_at") or now_iso
    if session.get("pending_confirmation_text"):
        session["pending_confirmation_set_at"] = session.get("pending_confirmation_set_at") or now_iso
    else:
        session["pending_confirmation_set_at"] = None
    await _client().setex(
        _key(channel, uid),
        settings.session_ttl_seconds,
        json.dumps(session, ensure_ascii=False, default=str),
    )


async def clear_session(channel: str, uid: str) -> None:
    await _client().delete(_key(channel, uid))
