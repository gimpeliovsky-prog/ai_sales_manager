from __future__ import annotations

import json
import re
from typing import Any

from app.conversation_flow import looks_like_small_talk

ALLOWED_INTENTS = {
    "low_signal",
    "small_talk",
    "find_product",
    "browse_catalog",
    "order_detail",
    "confirm_order",
    "add_to_order",
    "service_request",
    "human_handoff",
}

ALLOWED_SIGNAL_TYPES = {
    "deal_progress",
    "small_talk",
    "price_objection",
    "discount_request",
    "analogs_request",
    "comparison_request",
    "delivery_question",
    "availability_question",
    "topic_shift",
    "frustration",
    "confirmation",
    "service_request",
    "stalling",
    "resume_previous_context",
    "low_signal",
    "handoff_request",
}

ALLOWED_SIGNAL_EMOTIONS = {
    "neutral",
    "positive",
    "impatient",
    "skeptical",
}

ALLOWED_BEHAVIOR_CLASSES = {
    "direct_buyer",
    "explorer",
    "unclear_request",
    "price_sensitive",
    "frustrated",
    "service_request",
    "returning_customer",
    "silent_or_low_signal",
}

ALLOWED_DECISION_STATUSES = {
    "evaluating",
    "ready_to_buy",
}

ALLOWED_NEXT_ACTIONS = {
    "handoff_manager",
    "fulfill_service_request",
    "ask_need",
    "show_matching_options",
    "select_specific_item",
    "ask_quantity",
    "ask_unit",
    "ask_delivery_timing",
    "ask_contact",
    "quote_or_clarify_price",
    "confirm_order",
    "recommend_next_step",
}

SAFE_PATCH_FIELDS = {
    "product_interest",
    "quantity",
    "uom",
    "urgency",
    "delivery_need",
    "price_sensitivity",
    "decision_status",
}


def _clean_text(value: Any) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text or None


def parse_llm_signal_classification(text: str) -> dict[str, Any]:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return {"valid": False, "reason": "invalid_json"}
    if not isinstance(payload, dict):
        return {"valid": False, "reason": "invalid_payload"}

    signal_type = str(payload.get("signal_type") or "").strip()
    signal_emotion = str(payload.get("signal_emotion") or "").strip()
    preserves_deal = payload.get("signal_preserves_deal")
    try:
        confidence = float(payload.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0

    if signal_type not in ALLOWED_SIGNAL_TYPES:
        signal_type = ""
    if signal_emotion not in ALLOWED_SIGNAL_EMOTIONS:
        signal_emotion = ""
    preserves_deal = bool(preserves_deal) if isinstance(preserves_deal, bool) else None

    return {
        "valid": bool(signal_type),
        "signal_type": signal_type or None,
        "signal_emotion": signal_emotion or None,
        "signal_preserves_deal": preserves_deal,
        "confidence": max(0.0, min(1.0, confidence)),
        "reason": _clean_text(payload.get("reason")),
        "source": "llm",
    }


def parse_llm_state_update(text: str) -> dict[str, Any]:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return {"valid": False, "reason": "invalid_json"}
    if not isinstance(payload, dict):
        return {"valid": False, "reason": "invalid_payload"}

    intent = str(payload.get("intent") or "").strip()
    behavior_class = str(payload.get("behavior_class") or "").strip()
    try:
        confidence = float(payload.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    next_action = str(payload.get("next_action") or "").strip()
    signal_type = str(payload.get("signal_type") or "").strip()
    signal_emotion = str(payload.get("signal_emotion") or "").strip()
    preserves_deal = payload.get("signal_preserves_deal")
    lead_patch = payload.get("lead_patch") if isinstance(payload.get("lead_patch"), dict) else {}

    sanitized_patch: dict[str, Any] = {}
    for key in SAFE_PATCH_FIELDS:
        if key not in lead_patch:
            continue
        value = lead_patch.get(key)
        if key == "quantity":
            try:
                sanitized_patch[key] = float(value)
            except (TypeError, ValueError):
                continue
        elif key == "price_sensitivity":
            sanitized_patch[key] = bool(value)
        elif key == "decision_status":
            decision_status = str(value or "").strip()
            if decision_status in ALLOWED_DECISION_STATUSES:
                sanitized_patch[key] = decision_status
        else:
            cleaned_value = _clean_text(value)
            if cleaned_value:
                if key == "product_interest" and looks_like_small_talk(cleaned_value):
                    continue
                sanitized_patch[key] = cleaned_value

    if intent not in ALLOWED_INTENTS:
        intent = ""
    if behavior_class not in ALLOWED_BEHAVIOR_CLASSES:
        behavior_class = ""
    if next_action not in ALLOWED_NEXT_ACTIONS:
        next_action = ""
    if signal_type not in ALLOWED_SIGNAL_TYPES:
        signal_type = ""
    if signal_emotion not in ALLOWED_SIGNAL_EMOTIONS:
        signal_emotion = ""
    preserves_deal = bool(preserves_deal) if isinstance(preserves_deal, bool) else None

    return {
        "valid": bool(intent or behavior_class or sanitized_patch or next_action or signal_type),
        "intent": intent or None,
        "behavior_class": behavior_class or None,
        "next_action": next_action or None,
        "signal_type": signal_type or None,
        "signal_emotion": signal_emotion or None,
        "signal_preserves_deal": preserves_deal,
        "confidence": max(0.0, min(1.0, confidence)),
        "lead_patch": sanitized_patch,
        "reason": _clean_text(payload.get("reason")),
    }
