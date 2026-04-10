from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from app.lead_management import normalize_lead_profile

DEFAULT_CONTEXT_TYPE = "new_purchase"
OPEN_CONTEXT_STATUSES = {"open", "waiting_customer", "waiting_internal", "ready_to_execute"}
_ORDER_EDIT_FIELDS = {
    "order_correction_status",
    "target_order_id",
    "correction_type",
    "correction_requested_at",
    "correction_confirmed_at",
    "correction_applied_at",
    "correction_rejected_at",
    "correction_last_actor_id",
    "correction_last_comment",
    "active_order_state",
    "active_order_can_modify",
    "active_order_checked_at",
    "order_total",
    "won_at",
    "won_revenue",
}
_QUOTE_FIELDS = {
    "quote_status",
    "quote_id",
    "quote_total",
    "quote_currency",
    "quote_pdf_url",
    "quote_requested_at",
    "quote_prepared_at",
    "quote_sent_at",
    "quote_accepted_at",
    "quote_rejected_at",
    "quote_last_actor_id",
    "quote_last_comment",
    "quote_last_updated_at",
}


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


def _same_text(left: Any, right: Any) -> bool:
    return str(left or "").strip().casefold() == str(right or "").strip().casefold()


def _product_interest(profile: dict[str, Any]) -> str | None:
    normalized = normalize_lead_profile(profile)
    value = str(
        normalized.get("catalog_item_name")
        or normalized.get("catalog_item_code")
        or normalized.get("product_interest")
        or ""
    ).strip()
    return value or None


def _context_title(context_type: str, profile: dict[str, Any], related_order_id: str | None) -> str | None:
    if context_type == "order_edit":
        return f"Edit {related_order_id}" if related_order_id else "Order edit"
    interest = _product_interest(profile)
    if interest:
        return f"Purchase: {interest}"
    return "Purchase"


def _copy_session_state_into_context(
    session: dict[str, Any],
    context: dict[str, Any],
    *,
    lead_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(lead_profile if isinstance(lead_profile, dict) else session.get("lead_profile"))
    context["stage"] = str(session.get("stage") or context.get("stage") or "new").strip() or "new"
    context["stage_confidence"] = float(session.get("stage_confidence") or 0.0)
    context["behavior_class"] = str(session.get("behavior_class") or context.get("behavior_class") or "unclear_request").strip() or "unclear_request"
    context["behavior_confidence"] = float(session.get("behavior_confidence") or 0.0)
    context["last_intent"] = str(session.get("last_intent") or "").strip() or None
    context["last_intent_confidence"] = float(session.get("last_intent_confidence") or 0.0)
    context["lead_profile"] = profile
    context["updated_at"] = _now_iso()
    if context.get("context_type") == "order_edit":
        context["related_order_id"] = str(
            context.get("related_order_id")
            or profile.get("target_order_id")
            or session.get("last_sales_order_name")
            or ""
        ).strip() or None
    context["title"] = _context_title(str(context.get("context_type") or DEFAULT_CONTEXT_TYPE), profile, context.get("related_order_id"))
    return context


def _context_matches(
    candidate: dict[str, Any],
    *,
    context_type: str,
    related_order_id: str | None = None,
    product_interest: str | None = None,
) -> bool:
    if str(candidate.get("context_type") or "").strip() != context_type:
        return False
    if related_order_id is not None:
        return _same_text(candidate.get("related_order_id"), related_order_id)
    if product_interest is not None:
        return _same_text(_product_interest(candidate.get("lead_profile") if isinstance(candidate.get("lead_profile"), dict) else {}), product_interest)
    return True


def _find_context(
    session: dict[str, Any],
    *,
    context_type: str,
    related_order_id: str | None = None,
    product_interest: str | None = None,
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    _bootstrap_contexts_from_legacy(session)
    for context_id, candidate in session["contexts"].items():
        if not isinstance(candidate, dict):
            continue
        if _context_matches(candidate, context_type=context_type, related_order_id=related_order_id, product_interest=product_interest):
            return context_id, candidate
    return None, None


def _new_purchase_profile_from(profile: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_lead_profile(profile)
    for key in _ORDER_EDIT_FIELDS:
        normalized[key] = None
    normalized["order_correction_status"] = "none"
    normalized["target_order_id"] = None
    normalized["correction_type"] = None
    for key in _QUOTE_FIELDS:
        normalized[key] = None
    normalized["quote_status"] = "none"
    if normalized.get("product_interest"):
        normalized["status"] = "new_lead"
    elif normalized.get("status") not in {"lost", "handoff", "service"}:
        normalized["status"] = "none"
    return normalized


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


def reconcile_contexts_after_state_update(
    session: dict[str, Any],
    *,
    previous_lead_profile: dict[str, Any] | None,
    active_order_name: str | None,
) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    current_context = _require_active_context(session)
    current_profile = normalize_lead_profile(session.get("lead_profile"))
    previous_profile = normalize_lead_profile(previous_lead_profile)
    current_intent = str(session.get("last_intent") or "").strip()
    current_context_type = str(current_context.get("context_type") or DEFAULT_CONTEXT_TYPE).strip() or DEFAULT_CONTEXT_TYPE

    if str(current_profile.get("order_correction_status") or "").strip() == "requested" and not bool(current_profile.get("separate_order_requested")):
        target_order_id = str(current_profile.get("target_order_id") or active_order_name or "").strip() or None
        if current_context_type != "order_edit" or not _same_text(current_context.get("related_order_id"), target_order_id):
            context_id, context = _find_context(session, context_type="order_edit", related_order_id=target_order_id)
            if context is None:
                context = empty_context(context_type="order_edit", related_order_id=target_order_id, lead_profile=current_profile)
                session["contexts"][context["context_id"]] = context
                context_id = context["context_id"]
            _copy_session_state_into_context(session, context, lead_profile=current_profile)
            session["active_context_id"] = context_id
            return sync_legacy_from_active_context(session)
        _copy_session_state_into_context(session, current_context, lead_profile=current_profile)
        return sync_legacy_from_active_context(session)

    previous_interest = _product_interest(previous_profile)
    current_interest = _product_interest(current_profile)
    should_open_new_purchase = bool(
        current_context_type == "order_edit"
        and current_interest
        and current_intent in {"find_product", "browse_catalog"}
        and not bool(current_profile.get("order_correction_status") == "requested")
        and not _same_text(previous_interest, current_interest)
    )
    if bool(current_profile.get("separate_order_requested")):
        should_open_new_purchase = True

    if should_open_new_purchase:
        purchase_profile = _new_purchase_profile_from(current_profile)
        context_id, context = _find_context(
            session,
            context_type="new_purchase",
            product_interest=_product_interest(purchase_profile),
        )
        if context is None:
            context = empty_context(context_type="new_purchase", lead_profile=purchase_profile)
            session["contexts"][context["context_id"]] = context
            context_id = context["context_id"]
        _copy_session_state_into_context(session, context, lead_profile=purchase_profile)
        session["active_context_id"] = context_id
        return sync_legacy_from_active_context(session)

    _copy_session_state_into_context(session, current_context, lead_profile=current_profile)
    return sync_legacy_from_active_context(session)
