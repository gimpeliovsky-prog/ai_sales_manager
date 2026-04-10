from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from app.conversation_contexts import active_lead_profile
from app.lead_management import normalize_lead_profile
from app.sales_policy import sales_policy

_CONTACT_RE = re.compile(r"\b(?:phone|телефон|номер|contact|email|почт|whatsapp)\b", re.IGNORECASE)
_RISKY_PROMISE_RE = re.compile(
    r"\b(?:discount|скидк|free delivery|доставка сегодня|today delivery|in stock|в наличии|guarantee|гарантир)\b",
    re.IGNORECASE,
)
_DISCOUNT_PROMISE_RE = re.compile(r"\b(?:discount|special price|cheaper|free|СЃРєРёРґРє|РґРµС€РµРІР»Рµ)\b", re.IGNORECASE)
_STOCK_PROMISE_RE = re.compile(r"\b(?:in stock|available|availability|РІ РЅР°Р»РёС‡РёРё|РµСЃС‚СЊ РІ РЅР°Р»РёС‡)\b", re.IGNORECASE)
_DELIVERY_PROMISE_RE = re.compile(r"\b(?:today delivery|deliver today|delivery today|tomorrow delivery|РґРѕСЃС‚Р°РІРєР° СЃРµРіРѕРґРЅСЏ)\b", re.IGNORECASE)
_HUMAN_REQUEST_RE = re.compile(r"\b(?:human|manager|agent|person|человек|менеджер|оператор|представитель)\b", re.IGNORECASE)


def _messages(session: dict[str, Any]) -> list[dict[str, Any]]:
    messages = session.get("messages")
    return [message for message in messages if isinstance(message, dict)] if isinstance(messages, list) else []


def _assistant_messages(session: dict[str, Any]) -> list[str]:
    return [str(message.get("content") or "") for message in _messages(session) if message.get("role") == "assistant"]


def _user_messages(session: dict[str, Any]) -> list[str]:
    return [str(message.get("content") or "") for message in _messages(session) if message.get("role") == "user"]


def _timeline_has(session: dict[str, Any], *event_types: str) -> bool:
    timeline = session.get("lead_timeline")
    if not isinstance(timeline, list):
        return False
    return any(isinstance(entry, dict) and entry.get("event_type") in event_types for entry in timeline)


def evaluate_conversation_quality(session: dict[str, Any], ai_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = normalize_lead_profile(active_lead_profile(session))
    policy = sales_policy(ai_policy)
    flags: list[str] = []
    notes: list[str] = []
    assistant_text = "\n".join(_assistant_messages(session))
    user_text = "\n".join(_user_messages(session))

    if profile.get("status") in {"new_lead", "quote_needed"} and profile.get("next_action") in {None, "", "ask_need"}:
        flags.append("missing_next_step")
        notes.append("Lead has commercial intent but no actionable next step.")

    if not profile.get("product_interest") and not profile.get("need") and _CONTACT_RE.search(assistant_text):
        flags.append("contact_requested_too_early")
        notes.append("Assistant appears to request contact details before capturing a product need.")

    has_tool_result = _timeline_has(session, "tool_call_finished")
    if _RISKY_PROMISE_RE.search(assistant_text) and not has_tool_result:
        flags.append("risky_promise_without_tool")
        notes.append("Assistant may have promised commercial terms, stock or delivery without a tool-backed result.")
    if _DISCOUNT_PROMISE_RE.search(assistant_text) and not policy.get("allow_discount_promises", False):
        flags.append("discount_promise_blocked_by_sales_policy")
        notes.append("Assistant appears to mention a discount while tenant sales policy does not allow discount promises.")
    if _STOCK_PROMISE_RE.search(assistant_text) and not has_tool_result and not policy.get("allow_stock_promises_without_tool", False):
        flags.append("stock_promise_without_tool")
        notes.append("Assistant appears to promise stock or availability without a tool-backed result.")
    if _DELIVERY_PROMISE_RE.search(assistant_text) and not has_tool_result and not policy.get("allow_delivery_promises_without_tool", False):
        flags.append("delivery_promise_without_tool")
        notes.append("Assistant appears to promise delivery timing without a tool-backed result.")

    if profile.get("temperature") in {"warm", "hot"} and profile.get("status") not in {"won", "lost", "order_created"}:
        if not profile.get("last_followup_at") and profile.get("status") == "stalled":
            flags.append("warm_hot_without_followup")
            notes.append("Warm/hot stalled lead has no recorded proactive follow-up.")

    if profile.get("temperature") == "hot" and profile.get("sales_owner_status") in {None, "", "delivery_failed"}:
        flags.append("hot_lead_not_handed_to_owner")
        notes.append("Hot lead is not handed to a sales owner.")

    if _HUMAN_REQUEST_RE.search(user_text) and profile.get("sales_owner_status") in {None, "", "delivery_failed"}:
        flags.append("human_requested_without_owner_handoff")
        notes.append("Customer appears to ask for a human but no owner handoff is recorded.")

    if profile.get("do_not_contact") and profile.get("last_followup_at"):
        flags.append("followup_after_do_not_contact")
        notes.append("Lead has do_not_contact but also has a recorded follow-up.")

    score = max(0, 100 - len(flags) * 15)
    if "risky_promise_without_tool" in flags:
        score = max(0, score - 10)

    return {
        "conversation_quality_score": score,
        "quality_flags": flags,
        "coaching_notes": notes,
        "quality_evaluated_at": datetime.now(UTC).isoformat(),
    }


def update_session_quality(session: dict[str, Any], ai_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    quality = evaluate_conversation_quality(session, ai_policy=ai_policy)
    session.update(quality)
    return quality
