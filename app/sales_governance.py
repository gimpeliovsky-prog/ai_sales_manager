from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.lead_management import normalize_lead_profile


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        resolved = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return resolved if resolved.tzinfo else resolved.replace(tzinfo=UTC)


def _minutes(config: dict[str, Any], key: str, default: int) -> int:
    try:
        return max(1, int(config.get(key, default) or default))
    except (TypeError, ValueError):
        return default


def _breach_key(rule: str, lead_id: str | None) -> str:
    return f"{rule}:{lead_id or 'unknown'}"


def evaluate_sla_breaches(
    *,
    session: dict[str, Any],
    lead_config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    config = lead_config if isinstance(lead_config, dict) else {}
    profile = normalize_lead_profile(session.get("lead_profile"))
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=UTC)
    if profile.get("status") in {"none", "won", "lost", "merged"}:
        return []

    breaches: list[dict[str, Any]] = []
    lead_id = str(profile.get("lead_id") or "")

    hot_at = _parse_dt(profile.get("hot_at"))
    owner_status = str(profile.get("sales_owner_status") or "")
    if profile.get("temperature") == "hot" and hot_at and owner_status not in {"accepted", "closed_not_target"}:
        sla_minutes = _minutes(config, "hot_lead_owner_accept_sla_minutes", 10)
        if resolved_now - hot_at >= timedelta(minutes=sla_minutes):
            breaches.append(
                {
                    "breach_key": _breach_key("hot_lead_owner_accept", lead_id),
                    "rule": "hot_lead_owner_accept",
                    "sla_minutes": sla_minutes,
                    "started_at": hot_at.isoformat(),
                    "breached_at": resolved_now.isoformat(),
                    "lead_id": lead_id,
                }
            )

    quote_requested_at = _parse_dt(profile.get("quote_requested_at") or profile.get("quote_needed_at"))
    if profile.get("quote_status") == "requested" and quote_requested_at and not profile.get("quote_sent_at"):
        sla_minutes = _minutes(config, "quote_prepare_sla_minutes", 30)
        if resolved_now - quote_requested_at >= timedelta(minutes=sla_minutes):
            breaches.append(
                {
                    "breach_key": _breach_key("quote_prepare", lead_id),
                    "rule": "quote_prepare",
                    "sla_minutes": sla_minutes,
                    "started_at": quote_requested_at.isoformat(),
                    "breached_at": resolved_now.isoformat(),
                    "lead_id": lead_id,
                }
            )

    stalled_at = _parse_dt(profile.get("stalled_at"))
    if profile.get("status") == "stalled" and stalled_at and owner_status not in {"accepted", "closed_not_target"}:
        sla_minutes = _minutes(config, "stalled_owner_escalation_minutes", 60)
        if resolved_now - stalled_at >= timedelta(minutes=sla_minutes):
            breaches.append(
                {
                    "breach_key": _breach_key("stalled_owner_escalation", lead_id),
                    "rule": "stalled_owner_escalation",
                    "sla_minutes": sla_minutes,
                    "started_at": stalled_at.isoformat(),
                    "breached_at": resolved_now.isoformat(),
                    "lead_id": lead_id,
                }
            )

    return breaches


def record_new_sla_breaches(session: dict[str, Any], breaches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = session.get("sla_breaches")
    if not isinstance(existing, list):
        existing = []
    seen = {str(item.get("breach_key") or "") for item in existing if isinstance(item, dict)}
    new_breaches: list[dict[str, Any]] = []
    for breach in breaches:
        breach_key = str(breach.get("breach_key") or "")
        if not breach_key or breach_key in seen:
            continue
        existing.append(breach)
        seen.add(breach_key)
        new_breaches.append(breach)
    session["sla_breaches"] = existing[-100:]
    return new_breaches
