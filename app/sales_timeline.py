from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.lead_management import normalize_lead_profile


def _safe_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    allowed_keys = {
        "reason",
        "status",
        "previous_status",
        "temperature",
        "previous_temperature",
        "next_action",
        "followup_strategy",
        "quote_status",
        "lost_reason",
        "followup_delivery",
        "sales_owner_delivery",
        "owner_action",
        "owner_actor_id",
        "tool_name",
        "name",
        "channel",
        "via",
        "sla_rule",
        "sla_minutes",
        "quality_score",
        "quality_flags",
        "source",
        "outcome",
        "comment",
        "actor_id",
        "order_total",
        "won_revenue",
        "currency",
        "quote_id",
        "quote_total",
        "quote_currency",
        "quote_pdf_url",
        "duplicate_of_lead_id",
        "dedupe_reason",
        "dedupe_score",
        "merged_into_lead_id",
        "target_lead_id",
        "duplicate_lead_id",
        "order_correction_status",
        "target_order_id",
        "correction_type",
        "order_state",
        "can_modify",
        "error_code",
    }
    return {key: value for key, value in payload.items() if key in allowed_keys}


def append_lead_timeline_event(
    session: dict[str, Any],
    *,
    event_type: str,
    payload: dict[str, Any] | None = None,
    actor: str | None = None,
    now: datetime | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    profile = normalize_lead_profile(session.get("lead_profile"))
    timeline = session.get("lead_timeline")
    if not isinstance(timeline, list):
        timeline = []
    entry = {
        "at": (now or datetime.now(UTC)).isoformat(),
        "event_type": str(event_type or "event"),
        "lead_id": profile.get("lead_id"),
        "status": profile.get("status"),
        "temperature": profile.get("temperature"),
        "score": profile.get("score"),
        "next_action": profile.get("next_action"),
        "followup_strategy": profile.get("followup_strategy"),
        "sales_owner_status": profile.get("sales_owner_status"),
        "actor": actor,
        "payload": _safe_payload(payload),
    }
    timeline.append(entry)
    session["lead_timeline"] = timeline[-max(1, int(limit or 100)) :]
    return entry


def latest_timeline_event(session: dict[str, Any], event_type: str) -> dict[str, Any] | None:
    timeline = session.get("lead_timeline")
    if not isinstance(timeline, list):
        return None
    for entry in reversed(timeline):
        if isinstance(entry, dict) and entry.get("event_type") == event_type:
            return entry
    return None
