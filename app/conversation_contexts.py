from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Callable

from app.lead_management import LEAD_DEAL_FIELDS, LEAD_PROGRESS_FIELDS, normalize_lead_profile

DEFAULT_CONTEXT_TYPE = "new_purchase"
CONTEXT_TYPES = {
    "new_purchase",
    "order_edit",
    "quote_negotiation",
    "service_request",
    "identity_resolution",
}
OPEN_CONTEXT_STATUSES = {"open", "waiting_customer", "waiting_internal", "ready_to_execute"}
_MAX_CONTEXT_EVENTS = 200
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


def _context_events(session: dict[str, Any]) -> list[dict[str, Any]]:
    value = session.get("context_events")
    return value if isinstance(value, list) else []


def _require_active_context(session: dict[str, Any]) -> dict[str, Any]:
    contexts = _context_map(session)
    active_context_id = _active_context_id(session)
    active_context = contexts.get(active_context_id) if active_context_id else None
    if isinstance(active_context, dict):
        return active_context
    raise KeyError("active context is not initialized")


def _bootstrap_contexts_from_legacy(session: dict[str, Any]) -> None:
    if not isinstance(session.get("context_events"), list):
        session["context_events"] = []
    contexts = _context_map(session)
    active_context_id = _active_context_id(session)
    active_context = contexts.get(active_context_id) if active_context_id else None
    if isinstance(active_context, dict):
        session["contexts"] = contexts
        session["active_context_id"] = active_context_id
        if not isinstance(active_context.get("deal_state"), dict) or not isinstance(active_context.get("progress_state"), dict) or not isinstance(active_context.get("signal_state"), dict):
            _copy_session_state_into_context(session, active_context)
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
    _copy_session_state_into_context(session, active_context)
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
        "context_type": str(context_type or DEFAULT_CONTEXT_TYPE).strip() if str(context_type or DEFAULT_CONTEXT_TYPE).strip() in CONTEXT_TYPES else DEFAULT_CONTEXT_TYPE,
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
        "deal_state": {},
        "progress_state": {},
        "signal_state": {},
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
    if context_type == "quote_negotiation":
        interest = _product_interest(profile)
        return f"Quote: {interest}" if interest else "Quote negotiation"
    if context_type == "service_request":
        return f"Service for {related_order_id}" if related_order_id else "Service request"
    if context_type == "identity_resolution":
        return "Buyer identification"
    interest = _product_interest(profile)
    if interest:
        return f"Purchase: {interest}"
    return "Purchase"


def _append_context_event(
    session: dict[str, Any],
    *,
    event_type: str,
    context_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    _bootstrap_contexts_from_legacy(session)
    event = {
        "event_id": f"ctxevt_{uuid.uuid4().hex[:12]}",
        "event_type": str(event_type or "").strip() or "context_updated",
        "context_id": str(context_id or "").strip() or None,
        "active_context_id": _active_context_id(session),
        "created_at": _now_iso(),
        "payload": payload if isinstance(payload, dict) else {},
    }
    events = _context_events(session)
    events.append(event)
    session["context_events"] = events[-_MAX_CONTEXT_EVENTS:]


def _dict_subset(profile: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    return {key: profile.get(key) for key in keys if profile.get(key) not in (None, "", [])}


def _deal_state_from_profile(profile: dict[str, Any], *, related_order_id: str | None = None) -> dict[str, Any]:
    deal = _dict_subset(profile, LEAD_DEAL_FIELDS)
    if related_order_id:
        deal["related_order_id"] = related_order_id
    return deal


def _progress_state_from_profile(
    profile: dict[str, Any],
    *,
    stage: str,
    stage_confidence: float,
    behavior_class: str,
    behavior_confidence: float,
) -> dict[str, Any]:
    progress = _dict_subset(profile, LEAD_PROGRESS_FIELDS)
    progress["stage"] = stage
    progress["stage_confidence"] = float(stage_confidence or 0.0)
    progress["behavior_class"] = behavior_class
    progress["behavior_confidence"] = float(behavior_confidence or 0.0)
    return progress


def _signal_state_from_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": str(session.get("signal_type") or "").strip() or "deal_progress",
        "confidence": float(session.get("signal_confidence") or 0.0),
        "preserves_deal": bool(session.get("signal_preserves_deal", True)),
        "emotion": str(session.get("signal_emotion") or "").strip() or "neutral",
        "intent": str(session.get("last_intent") or "").strip() or None,
        "intent_confidence": float(session.get("last_intent_confidence") or 0.0),
    }


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
    context["deal_state"] = _deal_state_from_profile(profile, related_order_id=context.get("related_order_id"))
    context["progress_state"] = _progress_state_from_profile(
        profile,
        stage=context["stage"],
        stage_confidence=context["stage_confidence"],
        behavior_class=context["behavior_class"],
        behavior_confidence=context["behavior_confidence"],
    )
    context["signal_state"] = _signal_state_from_session(session)
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


def active_lead_profile(session: dict[str, Any]) -> dict[str, Any]:
    context = active_context(session)
    return normalize_lead_profile(context.get("lead_profile"))


def active_context_type(session: dict[str, Any]) -> str:
    context = active_context(session)
    value = str(context.get("context_type") or DEFAULT_CONTEXT_TYPE).strip()
    return value if value in CONTEXT_TYPES else DEFAULT_CONTEXT_TYPE


def active_related_order_id(session: dict[str, Any]) -> str | None:
    context = active_context(session)
    value = str(context.get("related_order_id") or "").strip()
    return value or None


def context_events(session: dict[str, Any]) -> list[dict[str, Any]]:
    _bootstrap_contexts_from_legacy(session)
    return list(_context_events(session))


def active_deal_state(session: dict[str, Any]) -> dict[str, Any]:
    context = active_context(session)
    value = context.get("deal_state")
    return value if isinstance(value, dict) else {}


def active_progress_state(session: dict[str, Any]) -> dict[str, Any]:
    context = active_context(session)
    value = context.get("progress_state")
    return value if isinstance(value, dict) else {}


def active_signal_state(session: dict[str, Any]) -> dict[str, Any]:
    context = active_context(session)
    value = context.get("signal_state")
    return value if isinstance(value, dict) else {}


def refresh_active_context_state(
    session: dict[str, Any],
    *,
    event_type: str | None = None,
    event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    context = _require_active_context(session)
    _copy_session_state_into_context(session, context)
    sync_legacy_from_active_context(session)
    if event_type:
        _append_context_event(
            session,
            event_type=event_type,
            context_id=str(context.get("context_id") or "").strip() or None,
            payload=event_payload,
        )
    return session


def set_active_lead_profile(
    session: dict[str, Any],
    lead_profile: dict[str, Any] | None,
    *,
    event_type: str | None = None,
    event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session["lead_profile"] = normalize_lead_profile(lead_profile)
    refresh_active_context_state(
        session,
        event_type=event_type,
        event_payload=event_payload,
    )
    return active_lead_profile(session)


def mutate_active_lead_profile(
    session: dict[str, Any],
    mutator: Callable[[dict[str, Any]], dict[str, Any] | None],
    *,
    event_type: str | None = None,
    event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_profile = active_lead_profile(session)
    updated_profile = mutator(current_profile)
    return set_active_lead_profile(
        session,
        updated_profile if isinstance(updated_profile, dict) else current_profile,
        event_type=event_type,
        event_payload=event_payload,
    )


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
    if context.get("context_type") == "order_edit":
        context["related_order_id"] = str(
            context.get("related_order_id")
            or context["lead_profile"].get("target_order_id")
            or session.get("last_sales_order_name")
            or ""
        ).strip() or None
    context["deal_state"] = _deal_state_from_profile(context["lead_profile"], related_order_id=context.get("related_order_id"))
    context["progress_state"] = _progress_state_from_profile(
        context["lead_profile"],
        stage=context["stage"],
        stage_confidence=context["stage_confidence"],
        behavior_class=context["behavior_class"],
        behavior_confidence=context["behavior_confidence"],
    )
    context["signal_state"] = _signal_state_from_session(session)
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
    signal_state = context.get("signal_state") if isinstance(context.get("signal_state"), dict) else {}
    session["signal_type"] = str(signal_state.get("type") or session.get("signal_type") or "deal_progress").strip() or "deal_progress"
    session["signal_confidence"] = float(signal_state.get("confidence") or session.get("signal_confidence") or 0.0)
    session["signal_preserves_deal"] = bool(signal_state.get("preserves_deal", session.get("signal_preserves_deal", True)))
    session["signal_emotion"] = str(signal_state.get("emotion") or session.get("signal_emotion") or "neutral").strip() or "neutral"
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
    _copy_session_state_into_context(
        session,
        context,
        lead_profile=lead_profile if isinstance(lead_profile, dict) else context.get("lead_profile"),
    )
    session["contexts"][context["context_id"]] = context
    _append_context_event(
        session,
        event_type="context_created",
        context_id=context["context_id"],
        payload={
            "context_type": context.get("context_type"),
            "related_order_id": context.get("related_order_id"),
            "title": context.get("title"),
        },
    )
    if activate:
        session["active_context_id"] = context["context_id"]
        _append_context_event(
            session,
            event_type="context_activated",
            context_id=context["context_id"],
            payload={"reason": "create_context"},
        )
    sync_legacy_from_active_context(session)
    return context


def set_active_context(session: dict[str, Any], context_id: str) -> dict[str, Any]:
    ensure_session_contexts(session)
    if context_id in session["contexts"]:
        previous_context_id = session.get("active_context_id")
        session["active_context_id"] = context_id
        if previous_context_id != context_id:
            _append_context_event(
                session,
                event_type="context_switched",
                context_id=context_id,
                payload={"previous_context_id": previous_context_id},
            )
    return sync_legacy_from_active_context(session)


def _select_context(
    session: dict[str, Any],
    *,
    context_type: str,
    reason: str,
    lead_profile: dict[str, Any],
    related_order_id: str | None = None,
    product_interest: str | None = None,
) -> tuple[str, dict[str, Any]]:
    context_id, context = _find_context(
        session,
        context_type=context_type,
        related_order_id=related_order_id,
        product_interest=product_interest,
    )
    if context is None:
        context = empty_context(
            context_type=context_type,
            related_order_id=related_order_id,
            lead_profile=lead_profile,
        )
        session["contexts"][context["context_id"]] = context
        context_id = context["context_id"]
        _append_context_event(
            session,
            event_type="context_created",
            context_id=context_id,
            payload={
                "context_type": context_type,
                "reason": reason,
                "related_order_id": related_order_id,
                "product_interest": product_interest,
            },
        )
    return context_id, context


def _activate_context_for_reason(
    session: dict[str, Any],
    *,
    context_id: str,
    reason: str,
) -> None:
    previous_context_id = session.get("active_context_id")
    session["active_context_id"] = context_id
    if previous_context_id != context_id:
        _append_context_event(
            session,
            event_type="context_switched",
            context_id=context_id,
            payload={"previous_context_id": previous_context_id, "reason": reason},
        )


def route_active_context(
    session: dict[str, Any],
    *,
    previous_lead_profile: dict[str, Any] | None,
    active_order_name: str | None,
) -> dict[str, Any]:
    _bootstrap_contexts_from_legacy(session)
    current_context = _require_active_context(session)
    current_profile = normalize_lead_profile(session.get("lead_profile"))
    previous_profile = normalize_lead_profile(previous_lead_profile)
    signal_type = str(session.get("signal_type") or "").strip()
    current_intent = str(session.get("last_intent") or "").strip()
    current_context_type = str(current_context.get("context_type") or DEFAULT_CONTEXT_TYPE).strip() or DEFAULT_CONTEXT_TYPE
    target_order_id = str(current_profile.get("target_order_id") or active_order_name or "").strip() or None
    current_interest = _product_interest(current_profile)
    previous_interest = _product_interest(previous_profile)

    desired_context_type = current_context_type
    desired_related_order_id = current_context.get("related_order_id")
    desired_product_interest = current_interest
    reason = "state_refresh"

    if not bool(session.get("erp_customer_id")) and (
        str(session.get("stage") or "").strip() == "identify"
        or bool(session.get("buyer_company_pending"))
        or bool(session.get("buyer_review_required"))
    ):
        desired_context_type = "identity_resolution"
        desired_related_order_id = None
        desired_product_interest = None
        reason = "identity_resolution"
    elif signal_type == "service_request" or str(session.get("stage") or "").strip() == "service":
        desired_context_type = "service_request"
        desired_related_order_id = target_order_id or current_context.get("related_order_id")
        reason = "service_request"
    elif str(current_profile.get("order_correction_status") or "").strip() == "requested" and not bool(current_profile.get("separate_order_requested")):
        desired_context_type = "order_edit"
        desired_related_order_id = target_order_id
        desired_product_interest = current_interest or previous_interest
        reason = "order_correction"
    elif signal_type in {"price_objection", "discount_request", "comparison_request", "analogs_request"} or str(current_profile.get("next_action") or "").strip() == "quote_or_clarify_price":
        desired_context_type = "quote_negotiation"
        desired_related_order_id = target_order_id if current_context_type == "order_edit" else None
        reason = "quote_negotiation"
    elif signal_type in {"delivery_question", "availability_question"} and current_context_type in {"order_edit", "service_request"}:
        desired_context_type = "service_request"
        desired_related_order_id = target_order_id or current_context.get("related_order_id")
        reason = "operational_question"
    elif signal_type == "resume_previous_context":
        open_contexts = [
            (context_id, candidate)
            for context_id, candidate in session["contexts"].items()
            if isinstance(candidate, dict) and context_id != session.get("active_context_id") and str(candidate.get("status") or "open").strip() in OPEN_CONTEXT_STATUSES
        ]
        if open_contexts:
            open_contexts.sort(key=lambda pair: str(pair[1].get("updated_at") or ""), reverse=True)
            desired_context_id, desired_context = open_contexts[0]
            _activate_context_for_reason(session, context_id=desired_context_id, reason="resume_previous_context")
            _copy_session_state_into_context(session, desired_context, lead_profile=current_profile)
            return sync_legacy_from_active_context(session)
    elif signal_type == "topic_shift" or bool(current_profile.get("separate_order_requested")):
        desired_context_type = "new_purchase"
        desired_related_order_id = None
        reason = "topic_shift"
    elif (
        current_context_type == "order_edit"
        and current_interest
        and current_intent in {"find_product", "browse_catalog"}
        and not bool(current_profile.get("order_correction_status") == "requested")
        and not _same_text(previous_interest, current_interest)
    ):
        desired_context_type = "new_purchase"
        desired_related_order_id = None
        reason = "new_purchase_from_order_edit"

    if desired_context_type == "new_purchase":
        current_profile = _new_purchase_profile_from(current_profile)
        session["lead_profile"] = current_profile

    context_id, context = _select_context(
        session,
        context_type=desired_context_type,
        reason=reason,
        lead_profile=current_profile,
        related_order_id=desired_related_order_id,
        product_interest=desired_product_interest if desired_context_type in {"new_purchase", "quote_negotiation"} else None,
    )
    _activate_context_for_reason(session, context_id=context_id, reason=reason)
    _copy_session_state_into_context(session, context, lead_profile=current_profile)
    return sync_legacy_from_active_context(session)


def reconcile_contexts_after_state_update(
    session: dict[str, Any],
    *,
    previous_lead_profile: dict[str, Any] | None,
    active_order_name: str | None,
) -> dict[str, Any]:
    return route_active_context(
        session,
        previous_lead_profile=previous_lead_profile,
        active_order_name=active_order_name,
    )
