from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from app.lead_management import normalize_lead_profile

DEFAULT_CONTEXT_TYPE = "new_purchase"
OPEN_CONTEXT_STATUSES = {"open", "waiting_customer", "waiting_internal", "ready_to_execute"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _active_context_id(session: dict[str, Any]) -> str | None:
    value = session.get("active_context_id")
    text = str(value or "").strip()
    return text or None


def _context_map(session: dict[str, Any]) -> dict[str, Any]:
    value = session.get("contexts")
    return value if isinstance(value, dict) else {}


def _require_active_context(session: dict[str, Any]) -> dict[str, Any]:
    contexts = _context_map(session)
    active_context_id = _active_context_id(session)
    active_context = contexts.get(active_context_id) if active_context_id else None
    if isinstance(active_context, dict):
        return active_context
    raise KeyError("active context is not initialized")


def _bootstrap_contexts_from_legacy(session: dict[str, Any]) -> None:
    contexts = _context_map(session)
    active_context_id = _active_context_id(session)
    active_context = contexts.get(active_context_id) if active_context_id else None
    if isinstance(active_context, dict):
        session["contexts"] = contexts
        session["active_context_id"] = active_context_id
        return
    active_context = empty_context(
        lead_profile=session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else None,
        stage=str(session.get("stage") or "new").strip() or "new",
        stage_confidence=float(session.get("stage_confidence") or 0.0),
        behavior_class=str(session.get("behavior_class") or "unclear_request").strip() or "unclear_request",
        behavior_confidence=float(session.get("behavior_confidence") or 0.0),
        last_intent=str(session.get("last_intent") or "").strip() or None,
        last_intent_confidence=float(session.get("last_intent_confidence") or 0.0),
    )
    session["contexts"] = {active_context["context_id"]: active_context}
    session["active_context_id"] = active_context["context_id"]


def empty_context(
    *,
    context_type: str = DEFAULT_CONTEXT_TYPE,
    lead_profile: dict[str, Any] | None = None,
    stage: str = "new",
    stage_confidence: float = 0.0,
    behavior_class: str = "unclear_request",
    behavior_confidence: float = 0.0,
    last_intent: str | None = None,
    last_intent_confidence: float = 0.0,
    title: str | None = None,
    related_order_id: str | None = None,
) -> dict[str, Any]:
    created_at = _now_iso()
    return {
        "context_id": f"ctx_{uuid.uuid4().hex[:16]}",
        "context_type": str(context_type or DEFAULT_CONTEXT_TYPE).strip() or DEFAULT_CONTEXT_TYPE,
        "status": "open",
        "title": str(title or "").strip() or None,
        "priority": "normal",
        "created_at": created_at,
        "updated_at": created_at,
        "last_customer_message_at": None,
        "last_agent_message_at": None,
        "is_active": True,
        "supersedes_context_id": None,
        "related_order_id": str(related_order_id or "").strip() or None,
        "stage": str(stage or "new").strip() or "new",
        "stage_confidence": float(stage_confidence or 0.0),
        "behavior_class": str(behavior_class or "unclear_request").strip() or "unclear_request",
        "behavior_confidence": float(behavior_confidence or 0.0),
        "last_intent": str(last_intent or "").strip() or None,
        "last_intent_confidence": float(last_intent_confidence or 0.0),
        "lead_profile": normalize_lead_profile(lead_profile),
    }


def ensure_session_contexts(session: dict[str, Any]) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    sync_legacy_from_active_context(session)
    return session


def active_context(session: dict[str, Any]) -> dict[str, Any]:
    ensure_session_contexts(session)
    return _require_active_context(session)


def sync_legacy_to_active_context(session: dict[str, Any]) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    context = _require_active_context(session)
    context["stage"] = str(session.get("stage") or context.get("stage") or "new").strip() or "new"
    context["stage_confidence"] = float(session.get("stage_confidence") or 0.0)
    context["behavior_class"] = str(session.get("behavior_class") or context.get("behavior_class") or "unclear_request").strip() or "unclear_request"
    context["behavior_confidence"] = float(session.get("behavior_confidence") or 0.0)
    context["last_intent"] = str(session.get("last_intent") or "").strip() or None
    context["last_intent_confidence"] = float(session.get("last_intent_confidence") or 0.0)
    context["lead_profile"] = normalize_lead_profile(session.get("lead_profile"))
    context["updated_at"] = _now_iso()
    open_ids: list[str] = []
    for context_id, candidate in session["contexts"].items():
        if not isinstance(candidate, dict):
            continue
        candidate["is_active"] = context_id == session["active_context_id"]
        if str(candidate.get("status") or "open").strip() in OPEN_CONTEXT_STATUSES:
            open_ids.append(context_id)
    session["open_context_ids"] = open_ids
    return session


def sync_legacy_from_active_context(session: dict[str, Any]) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    context = _require_active_context(session)
    session["stage"] = context.get("stage") or "new"
    session["stage_confidence"] = float(context.get("stage_confidence") or 0.0)
    session["behavior_class"] = context.get("behavior_class") or "unclear_request"
    session["behavior_confidence"] = float(context.get("behavior_confidence") or 0.0)
    session["last_intent"] = context.get("last_intent")
    session["last_intent_confidence"] = float(context.get("last_intent_confidence") or 0.0)
    session["lead_profile"] = normalize_lead_profile(context.get("lead_profile"))
    open_ids: list[str] = []
    for context_id, candidate in session["contexts"].items():
        if not isinstance(candidate, dict):
            continue
        candidate["is_active"] = context_id == session["active_context_id"]
        if str(candidate.get("status") or "open").strip() in OPEN_CONTEXT_STATUSES:
            open_ids.append(context_id)
    session["open_context_ids"] = open_ids
    return session


def create_context(
    session: dict[str, Any],
    *,
    context_type: str,
    title: str | None = None,
    related_order_id: str | None = None,
    lead_profile: dict[str, Any] | None = None,
    activate: bool = True,
) -> dict[str, Any]:
    ensure_session_contexts(session)
    context = empty_context(
        context_type=context_type,
        title=title,
        related_order_id=related_order_id,
        lead_profile=lead_profile,
    )
    session["contexts"][context["context_id"]] = context
    if activate:
        session["active_context_id"] = context["context_id"]
    sync_legacy_from_active_context(session)
    return context


def set_active_context(session: dict[str, Any], context_id: str) -> dict[str, Any]:
    ensure_session_contexts(session)
    if context_id in session["contexts"]:
        session["active_context_id"] = context_id
    return sync_legacy_from_active_context(session)
