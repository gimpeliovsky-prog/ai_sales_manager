import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as aioredis

from app.config import get_settings
from app.lead_management import normalize_telegram_username

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


def redis_client() -> aioredis.Redis:
    return _client()


def _key(channel: str, uid: str) -> str:
    return f"ai_session:{channel}:{uid}"


def _lead_index_key(lead_id: str) -> str:
    return f"ai_lead_index:{lead_id}"


def _sales_owner_telegram_key(company_code: str, username: str) -> str:
    return f"ai_sales_owner_telegram:{company_code}:{normalize_telegram_username(username)}"


def _sales_owner_route_key(company_code: str, route_key: str) -> str:
    safe_route_key = str(route_key or "default").replace(":", "_")
    return f"ai_sales_owner_route:{company_code}:{safe_route_key}"


def _parse_key(key: str) -> tuple[str, str] | None:
    parts = str(key or "").split(":", 2)
    if len(parts) != 3 or parts[0] != "ai_session":
        return None
    return parts[1], parts[2]


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
        "channel_context": {},
        "lead_timeline": [],
        "sla_breaches": [],
        "conversation_quality_score": None,
        "quality_flags": [],
        "coaching_notes": [],
        "quality_evaluated_at": None,
        "lang": None,
        "stage": "new",
        "stage_confidence": 0.0,
        "behavior_class": "unclear_request",
        "behavior_confidence": 0.0,
        "last_intent": None,
        "last_intent_confidence": 0.0,
        "lead_profile": {
            "status": "none",
            "lead_id": None,
            "created_at": None,
            "qualified_at": None,
            "quote_needed_at": None,
            "order_ready_at": None,
            "order_created_at": None,
            "won_at": None,
            "lost_at": None,
            "stalled_at": None,
            "hot_at": None,
            "handoff_at": None,
            "score": 0,
            "temperature": "cold",
            "next_action": "ask_need",
            "qualification_priority": "product_need",
            "qualification_priority_reason": None,
            "source_channel": None,
            "source_campaign": None,
            "source_utm_source": None,
            "source_utm_medium": None,
            "source_utm_campaign": None,
            "source_utm_term": None,
            "source_utm_content": None,
            "source_referrer": None,
            "source_landing_page": None,
            "source_product_page": None,
            "first_source_channel": None,
            "first_source_context": None,
            "need": None,
            "product_interest": None,
            "quantity": None,
            "uom": None,
            "requested_items": [],
            "requested_item_count": 0,
            "requested_items_have_quantities": False,
            "requested_items_need_uom_confirmation": False,
            "requested_items_assumed_uom": None,
            "requested_items_uom_assumption_status": None,
            "urgency": None,
            "delivery_need": None,
            "price_sensitivity": False,
            "decision_status": "unknown",
            "duplicate_of_lead_id": None,
            "dedupe_reason": None,
            "dedupe_score": None,
            "dedupe_checked_at": None,
            "merged_into_lead_id": None,
            "merged_duplicate_lead_ids": [],
            "merged_at": None,
            "merged_by": None,
            "quote_status": "none",
            "quote_id": None,
            "quote_total": None,
            "quote_currency": None,
            "quote_pdf_url": None,
            "quote_requested_at": None,
            "quote_prepared_at": None,
            "quote_sent_at": None,
            "quote_accepted_at": None,
            "quote_rejected_at": None,
            "quote_last_actor_id": None,
            "quote_last_comment": None,
            "quote_last_updated_at": None,
            "expected_revenue": None,
            "order_total": None,
            "currency": None,
            "won_revenue": None,
            "active_order_state": None,
            "active_order_can_modify": None,
            "active_order_checked_at": None,
            "order_correction_status": "none",
            "target_order_id": None,
            "correction_type": None,
            "correction_requested_at": None,
            "correction_confirmed_at": None,
            "correction_applied_at": None,
            "correction_rejected_at": None,
            "correction_last_actor_id": None,
            "correction_last_comment": None,
            "followup_count": 0,
            "last_followup_attempt_at": None,
            "last_followup_at": None,
            "last_followup_delivery": None,
            "do_not_contact": False,
            "do_not_contact_reason": None,
            "do_not_contact_until": None,
            "lost_reason": None,
            "sales_owner_status": None,
            "sales_owner_action_by": None,
            "sales_owner_action_at": None,
            "sales_owner_notified_at": None,
            "sales_owner_delivery": None,
            "last_sales_event": None,
            "last_updated_at": None,
            "playbook_version": None,
        },
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
        session["lead_profile"] = _empty_session()["lead_profile"]
        session["lead_timeline"] = []
        session["sla_breaches"] = []
        session["conversation_quality_score"] = None
        session["quality_flags"] = []
        session["coaching_notes"] = []
        session["quality_evaluated_at"] = None

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
    await _save_lead_index(channel, uid, session, settings.session_ttl_seconds)
    await _persist_sales_lead(channel, uid, session)


async def _save_lead_index(channel: str, uid: str, session: dict[str, Any], ttl_seconds: int) -> None:
    lead_profile = session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else {}
    lead_id = str(lead_profile.get("lead_id") or "").strip()
    if not lead_id:
        return
    await _client().setex(
        _lead_index_key(lead_id),
        max(60, int(ttl_seconds or 60)),
        json.dumps({"channel": channel, "uid": uid}, ensure_ascii=False),
    )


async def save_session_snapshot(channel: str, uid: str, session: dict[str, Any]) -> None:
    settings = get_settings()
    key = _key(channel, uid)
    ttl = await _client().ttl(key)
    if ttl is None or ttl <= 0:
        ttl = settings.session_ttl_seconds
    await _client().setex(
        key,
        ttl,
        json.dumps(session, ensure_ascii=False, default=str),
    )
    await _save_lead_index(channel, uid, session, ttl)
    await _persist_sales_lead(channel, uid, session)


async def _persist_sales_lead(channel: str, uid: str, session: dict[str, Any]) -> None:
    try:
        from app.sales_lead_repository import get_sales_lead_repository

        await get_sales_lead_repository().upsert_from_session(channel=channel, uid=uid, session=session)
    except Exception as exc:
        logger.warning("Failed to persist sales lead snapshot for %s:%s: %s", channel, uid, exc)


async def resolve_lead_session(lead_id: str) -> tuple[str, str, dict[str, Any]] | None:
    raw_index = await _client().get(_lead_index_key(lead_id))
    if not raw_index:
        return None
    try:
        index = json.loads(raw_index)
    except json.JSONDecodeError:
        return None
    if not isinstance(index, dict):
        return None
    channel = str(index.get("channel") or "").strip()
    uid = str(index.get("uid") or "").strip()
    if not channel or not uid:
        return None
    return channel, uid, await load_session(channel, uid)


async def save_sales_owner_telegram_chat(
    *,
    company_code: str,
    username: str,
    chat_id: str,
    user: dict[str, Any] | None = None,
) -> None:
    normalized_username = normalize_telegram_username(username)
    resolved_company_code = str(company_code or "").strip()
    resolved_chat_id = str(chat_id or "").strip()
    if not resolved_company_code or not normalized_username or not resolved_chat_id:
        return
    payload = {
        "company_code": resolved_company_code,
        "username": normalized_username,
        "chat_id": resolved_chat_id,
        "registered_at": datetime.now(UTC).isoformat(),
    }
    if isinstance(user, dict):
        payload.update(
            {
                "telegram_user_id": str(user.get("id") or ""),
                "first_name": user.get("first_name"),
                "last_name": user.get("last_name"),
                "language_code": user.get("language_code"),
            }
        )
    await _client().set(
        _sales_owner_telegram_key(resolved_company_code, normalized_username),
        json.dumps(payload, ensure_ascii=False, default=str),
    )


async def resolve_sales_owner_telegram_chat(*, company_code: str, username: str) -> dict[str, Any] | None:
    normalized_username = normalize_telegram_username(username)
    resolved_company_code = str(company_code or "").strip()
    if not resolved_company_code or not normalized_username:
        return None
    raw = await _client().get(_sales_owner_telegram_key(resolved_company_code, normalized_username))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


async def next_sales_owner_route_index(*, company_code: str, route_key: str, modulo: int) -> int:
    safe_modulo = max(1, int(modulo or 1))
    counter = await _client().incr(_sales_owner_route_key(str(company_code or "").strip(), route_key))
    return (int(counter) - 1) % safe_modulo


async def iter_session_snapshots(*, batch_size: int = 500):
    async for key in _client().scan_iter(match="ai_session:*", count=batch_size):
        parsed = _parse_key(str(key))
        if not parsed:
            continue
        raw = await _client().get(str(key))
        if not raw:
            continue
        try:
            session = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(session, dict):
            continue
        merged = new_session()
        merged.update(session)
        channel, uid = parsed
        yield channel, uid, _cleanup_session(merged)


async def clear_session(channel: str, uid: str) -> None:
    raw = await _client().get(_key(channel, uid))
    lead_id = None
    if raw:
        try:
            session = json.loads(raw)
            if isinstance(session, dict) and isinstance(session.get("lead_profile"), dict):
                lead_id = session["lead_profile"].get("lead_id")
        except json.JSONDecodeError:
            lead_id = None
    keys = [_key(channel, uid)]
    if lead_id:
        keys.append(_lead_index_key(str(lead_id)))
    await _client().delete(*keys)
