from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any

from app.lead_management import normalize_lead_profile


def _text(value: Any) -> str:
    return str(value or "").strip()


def _matches_optional(actual: Any, expected: str | None) -> bool:
    if expected is None or expected == "":
        return True
    return _text(actual).casefold() == _text(expected).casefold()


def _message_preview(session: dict[str, Any], limit: int = 160) -> str | None:
    messages = session.get("messages")
    if not isinstance(messages, list):
        return None
    for message in reversed(messages):
        if not isinstance(message, dict):
            continue
        content = _text(message.get("content"))
        if content:
            return content[:limit]
    return None


def _parse_dt(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _minutes_between(start: Any, end: Any) -> float | None:
    started_at = _parse_dt(start)
    ended_at = _parse_dt(end)
    if not started_at or not ended_at:
        return None
    return round((ended_at - started_at).total_seconds() / 60, 2)


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def lead_snapshot(*, channel: str, uid: str, session: dict[str, Any]) -> dict[str, Any]:
    profile = normalize_lead_profile(session.get("lead_profile"))
    return {
        "session_id": f"{channel}:{uid}",
        "channel": channel,
        "channel_uid": uid,
        "company_code": session.get("company_code"),
        "lead_id": profile.get("lead_id"),
        "status": profile.get("status"),
        "score": profile.get("score"),
        "temperature": profile.get("temperature"),
        "next_action": profile.get("next_action"),
        "playbook_version": profile.get("playbook_version"),
        "quote_status": profile.get("quote_status"),
        "quote_id": profile.get("quote_id"),
        "quote_total": profile.get("quote_total"),
        "quote_currency": profile.get("quote_currency"),
        "quote_pdf_url": profile.get("quote_pdf_url"),
        "quote_prepared_at": profile.get("quote_prepared_at"),
        "quote_sent_at": profile.get("quote_sent_at"),
        "quote_accepted_at": profile.get("quote_accepted_at"),
        "quote_rejected_at": profile.get("quote_rejected_at"),
        "quote_last_actor_id": profile.get("quote_last_actor_id"),
        "quote_last_updated_at": profile.get("quote_last_updated_at"),
        "expected_revenue": profile.get("expected_revenue"),
        "order_total": profile.get("order_total"),
        "currency": profile.get("currency"),
        "won_revenue": profile.get("won_revenue"),
        "active_order_state": profile.get("active_order_state"),
        "active_order_can_modify": profile.get("active_order_can_modify"),
        "active_order_checked_at": profile.get("active_order_checked_at"),
        "lost_reason": profile.get("lost_reason"),
        "manual_close_actor_id": profile.get("manual_close_actor_id"),
        "manual_closed_at": profile.get("manual_closed_at"),
        "followup_count": profile.get("followup_count"),
        "last_followup_at": profile.get("last_followup_at"),
        "sales_owner_status": profile.get("sales_owner_status"),
        "sales_owner_action_by": profile.get("sales_owner_action_by"),
        "sales_owner_action_at": profile.get("sales_owner_action_at"),
        "sales_owner_notified_at": profile.get("sales_owner_notified_at"),
        "sales_owner_accept_minutes": _minutes_between(
            profile.get("sales_owner_notified_at"),
            profile.get("sales_owner_action_at"),
        )
        if profile.get("sales_owner_status") == "accepted"
        else None,
        "created_at": profile.get("created_at"),
        "qualified_at": profile.get("qualified_at"),
        "quote_needed_at": profile.get("quote_needed_at"),
        "order_ready_at": profile.get("order_ready_at"),
        "order_created_at": profile.get("order_created_at"),
        "won_at": profile.get("won_at"),
        "lost_at": profile.get("lost_at"),
        "stalled_at": profile.get("stalled_at"),
        "hot_at": profile.get("hot_at"),
        "handoff_at": profile.get("handoff_at"),
        "source_channel": profile.get("source_channel"),
        "source_campaign": profile.get("source_campaign"),
        "source_utm_source": profile.get("source_utm_source"),
        "source_utm_medium": profile.get("source_utm_medium"),
        "source_utm_campaign": profile.get("source_utm_campaign"),
        "source_referrer": profile.get("source_referrer"),
        "source_landing_page": profile.get("source_landing_page"),
        "source_product_page": profile.get("source_product_page"),
        "product_interest": profile.get("product_interest"),
        "need": profile.get("need"),
        "quantity": profile.get("quantity"),
        "uom": profile.get("uom"),
        "urgency": profile.get("urgency"),
        "delivery_need": profile.get("delivery_need"),
        "price_sensitivity": profile.get("price_sensitivity"),
        "decision_status": profile.get("decision_status"),
        "duplicate_of_lead_id": profile.get("duplicate_of_lead_id"),
        "dedupe_reason": profile.get("dedupe_reason"),
        "dedupe_score": profile.get("dedupe_score"),
        "dedupe_checked_at": profile.get("dedupe_checked_at"),
        "merged_into_lead_id": profile.get("merged_into_lead_id"),
        "merged_duplicate_lead_ids": profile.get("merged_duplicate_lead_ids") if isinstance(profile.get("merged_duplicate_lead_ids"), list) else [],
        "merged_at": profile.get("merged_at"),
        "merged_by": profile.get("merged_by"),
        "order_correction_status": profile.get("order_correction_status"),
        "target_order_id": profile.get("target_order_id"),
        "correction_type": profile.get("correction_type"),
        "correction_requested_at": profile.get("correction_requested_at"),
        "correction_confirmed_at": profile.get("correction_confirmed_at"),
        "correction_applied_at": profile.get("correction_applied_at"),
        "correction_rejected_at": profile.get("correction_rejected_at"),
        "correction_last_actor_id": profile.get("correction_last_actor_id"),
        "buyer_name": session.get("buyer_name"),
        "buyer_phone": session.get("buyer_phone"),
        "buyer_identity_id": session.get("buyer_identity_id"),
        "erp_customer_id": session.get("erp_customer_id"),
        "active_order_name": session.get("last_sales_order_name"),
        "stage": session.get("stage"),
        "behavior_class": session.get("behavior_class"),
        "intent": session.get("last_intent"),
        "last_interaction_at": session.get("last_interaction_at"),
        "last_message_preview": _message_preview(session),
        "conversation_quality_score": session.get("conversation_quality_score"),
        "quality_flags": session.get("quality_flags") if isinstance(session.get("quality_flags"), list) else [],
        "coaching_notes": session.get("coaching_notes") if isinstance(session.get("coaching_notes"), list) else [],
        "quality_evaluated_at": session.get("quality_evaluated_at"),
        "sla_breaches": session.get("sla_breaches") if isinstance(session.get("sla_breaches"), list) else [],
    }


def filter_leads(
    leads: list[dict[str, Any]],
    *,
    company_code: str | None = None,
    status: str | None = None,
    temperature: str | None = None,
    sales_owner_status: str | None = None,
    source_channel: str | None = None,
    include_none: bool = False,
    include_lost: bool = True,
    q: str | None = None,
) -> list[dict[str, Any]]:
    query = _text(q).casefold()
    filtered: list[dict[str, Any]] = []
    for lead in leads:
        if not include_none and lead.get("status") == "none":
            continue
        if not include_lost and lead.get("status") == "lost":
            continue
        if not _matches_optional(lead.get("company_code"), company_code):
            continue
        if not _matches_optional(lead.get("status"), status):
            continue
        if not _matches_optional(lead.get("temperature"), temperature):
            continue
        if not _matches_optional(lead.get("sales_owner_status"), sales_owner_status):
            continue
        if not _matches_optional(lead.get("source_channel"), source_channel):
            continue
        if query:
            searchable = " ".join(
                _text(lead.get(key))
                for key in [
                    "lead_id",
                    "buyer_name",
                    "buyer_phone",
                    "erp_customer_id",
                    "product_interest",
                    "need",
                    "active_order_name",
                    "source_campaign",
                    "source_utm_campaign",
                    "last_message_preview",
                ]
            ).casefold()
            if query not in searchable:
                continue
        filtered.append(lead)
    return filtered


def paginate_leads(leads: list[dict[str, Any]], *, offset: int = 0, limit: int = 100) -> list[dict[str, Any]]:
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, min(500, int(limit or 100)))
    return leads[safe_offset : safe_offset + safe_limit]


def summarize_leads(leads: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(_text(lead.get("status")) or "unknown" for lead in leads)
    by_temperature = Counter(_text(lead.get("temperature")) or "unknown" for lead in leads)
    by_owner_status = Counter(_text(lead.get("sales_owner_status")) or "not_notified" for lead in leads)
    by_source_channel = Counter(_text(lead.get("source_channel")) or _text(lead.get("channel")) or "unknown" for lead in leads)
    by_utm_source = Counter(_text(lead.get("source_utm_source")) or "unknown" for lead in leads)
    by_utm_campaign = Counter(_text(lead.get("source_utm_campaign")) or _text(lead.get("source_campaign")) or "unknown" for lead in leads)
    by_playbook_version = Counter(_text(lead.get("playbook_version")) or "default" for lead in leads)
    by_correction_status = Counter(_text(lead.get("order_correction_status")) or "none" for lead in leads)
    followup_sent = sum(1 for lead in leads if int(lead.get("followup_count") or 0) > 0)
    expected_revenue = sum(_float(lead.get("expected_revenue")) for lead in leads)
    order_revenue = sum(_float(lead.get("order_total")) for lead in leads)
    won_revenue = sum(_float(lead.get("won_revenue")) for lead in leads)
    sla_breached = sum(1 for lead in leads if lead.get("sla_breaches"))
    quality_scores = [
        int(lead.get("conversation_quality_score"))
        for lead in leads
        if isinstance(lead.get("conversation_quality_score"), int)
    ]
    hot_count = by_temperature.get("hot", 0)
    owner_accept_minutes = [
        float(lead.get("sales_owner_accept_minutes"))
        for lead in leads
        if lead.get("sales_owner_accept_minutes") is not None
    ]
    total = len(leads)
    playbook_metrics: dict[str, dict[str, Any]] = {}
    for version, version_total in by_playbook_version.items():
        version_leads = [lead for lead in leads if (_text(lead.get("playbook_version")) or "default") == version]
        version_status = Counter(_text(lead.get("status")) or "unknown" for lead in version_leads)
        version_followup_sent = sum(1 for lead in version_leads if int(lead.get("followup_count") or 0) > 0)
        playbook_metrics[version] = {
            "total": version_total,
            "order_conversion_rate": (
                round((version_status.get("order_created", 0) + version_status.get("won", 0)) / version_total, 4)
                if version_total
                else 0.0
            ),
            "won_rate": round(version_status.get("won", 0) / version_total, 4) if version_total else 0.0,
            "followup_sent_count": version_followup_sent,
        }
    return {
        "total": total,
        "by_status": dict(by_status),
        "by_temperature": dict(by_temperature),
        "by_sales_owner_status": dict(by_owner_status),
        "by_source_channel": dict(by_source_channel),
        "by_utm_source": dict(by_utm_source),
        "by_utm_campaign": dict(by_utm_campaign),
        "by_playbook_version": dict(by_playbook_version),
        "by_order_correction_status": dict(by_correction_status),
        "by_playbook_version_metrics": playbook_metrics,
        "hot_count": hot_count,
        "stalled_count": by_status.get("stalled", 0),
        "quote_needed_count": by_status.get("quote_needed", 0),
        "order_ready_count": by_status.get("order_ready", 0),
        "order_created_count": by_status.get("order_created", 0),
        "won_count": by_status.get("won", 0),
        "lost_count": by_status.get("lost", 0),
        "merged_count": by_status.get("merged", 0),
        "order_correction_requested_count": by_correction_status.get("requested", 0),
        "order_correction_applied_count": by_correction_status.get("applied", 0),
        "followup_sent_count": followup_sent,
        "expected_revenue": round(expected_revenue, 2),
        "order_revenue": round(order_revenue, 2),
        "won_revenue": round(won_revenue, 2),
        "sla_breached_count": sla_breached,
        "accepted_by_owner_count": by_owner_status.get("accepted", 0),
        "average_owner_accept_minutes": (
            round(sum(owner_accept_minutes) / len(owner_accept_minutes), 2) if owner_accept_minutes else None
        ),
        "average_quality_score": round(sum(quality_scores) / len(quality_scores), 2) if quality_scores else None,
        "hot_owner_acceptance_rate": (
            round(by_owner_status.get("accepted", 0) / hot_count, 4) if hot_count else 0.0
        ),
        "order_conversion_rate": (
            round((by_status.get("order_created", 0) + by_status.get("won", 0)) / total, 4)
            if total
            else 0.0
        ),
        "won_rate": round(by_status.get("won", 0) / total, 4) if total else 0.0,
    }


def summarize_quality(leads: list[dict[str, Any]], *, worst_limit: int = 20) -> dict[str, Any]:
    quality_scores = [
        int(lead.get("conversation_quality_score"))
        for lead in leads
        if isinstance(lead.get("conversation_quality_score"), int)
    ]
    flags = Counter(
        str(flag)
        for lead in leads
        for flag in (lead.get("quality_flags") if isinstance(lead.get("quality_flags"), list) else [])
    )
    risky_promises = flags.get("risky_promise_without_tool", 0)
    scored_leads = [
        lead
        for lead in leads
        if isinstance(lead.get("conversation_quality_score"), int)
    ]
    worst = sorted(scored_leads, key=lambda lead: int(lead.get("conversation_quality_score") or 0))[: max(1, worst_limit)]
    return {
        "total": len(leads),
        "scored_count": len(quality_scores),
        "average_quality_score": round(sum(quality_scores) / len(quality_scores), 2) if quality_scores else None,
        "flags_by_type": dict(flags),
        "risky_promises_count": risky_promises,
        "worst_conversations": [
            {
                "lead_id": lead.get("lead_id"),
                "session_id": lead.get("session_id"),
                "company_code": lead.get("company_code"),
                "score": lead.get("conversation_quality_score"),
                "quality_flags": lead.get("quality_flags") if isinstance(lead.get("quality_flags"), list) else [],
                "coaching_notes": lead.get("coaching_notes") if isinstance(lead.get("coaching_notes"), list) else [],
                "last_message_preview": lead.get("last_message_preview"),
            }
            for lead in worst
        ],
    }


def summarize_manager_performance(leads: list[dict[str, Any]]) -> dict[str, Any]:
    managers: dict[str, dict[str, Any]] = {}

    def bucket(actor_id: Any) -> dict[str, Any]:
        key = _text(actor_id) or "unassigned"
        if key not in managers:
            managers[key] = {
                "manager_id": key,
                "accepted_count": 0,
                "won_count": 0,
                "lost_count": 0,
                "sla_breach_count": 0,
                "won_revenue": 0.0,
                "accepted_minutes": [],
            }
        return managers[key]

    for lead in leads:
        owner_actor = _text(lead.get("sales_owner_action_by"))
        close_actor = _text(lead.get("manual_close_actor_id"))
        manager = bucket(owner_actor or close_actor)
        if lead.get("sales_owner_status") == "accepted":
            manager["accepted_count"] += 1
            if lead.get("sales_owner_accept_minutes") is not None:
                manager["accepted_minutes"].append(float(lead.get("sales_owner_accept_minutes")))
        if lead.get("status") == "won":
            manager = bucket(close_actor or owner_actor)
            manager["won_count"] += 1
            manager["won_revenue"] += _float(lead.get("won_revenue") or lead.get("order_total"))
        elif lead.get("status") == "lost":
            manager = bucket(close_actor or owner_actor)
            manager["lost_count"] += 1
        breach_count = len(lead.get("sla_breaches") if isinstance(lead.get("sla_breaches"), list) else [])
        if breach_count:
            manager["sla_breach_count"] += breach_count

    rows: list[dict[str, Any]] = []
    for manager in managers.values():
        minutes = manager.pop("accepted_minutes")
        manager["average_accept_minutes"] = round(sum(minutes) / len(minutes), 2) if minutes else None
        manager["won_revenue"] = round(float(manager["won_revenue"]), 2)
        rows.append(manager)
    rows.sort(key=lambda item: (item["manager_id"] == "unassigned", item["manager_id"]))
    return {
        "total_managers": len(rows),
        "managers": rows,
    }


def summarize_source_funnel(leads: list[dict[str, Any]], *, group_by: str = "source_channel") -> dict[str, Any]:
    allowed_group_keys = {
        "source_channel",
        "source_utm_source",
        "source_utm_campaign",
        "source_campaign",
        "channel",
    }
    group_key = group_by if group_by in allowed_group_keys else "source_channel"
    buckets: dict[str, dict[str, Any]] = {}
    for lead in leads:
        key = _text(lead.get(group_key)) or "unknown"
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total": 0,
                "qualified": 0,
                "hot": 0,
                "quote_needed": 0,
                "order_created": 0,
                "won": 0,
                "lost": 0,
                "merged": 0,
                "won_revenue": 0.0,
                "lost_reasons": Counter(),
            },
        )
        status = _text(lead.get("status"))
        bucket["total"] += 1
        if status in {"qualified", "quote_needed", "order_ready", "order_created", "won"}:
            bucket["qualified"] += 1
        if lead.get("temperature") == "hot":
            bucket["hot"] += 1
        if status == "quote_needed":
            bucket["quote_needed"] += 1
        if status == "order_created":
            bucket["order_created"] += 1
        if status == "won":
            bucket["won"] += 1
            bucket["won_revenue"] += _float(lead.get("won_revenue") or lead.get("order_total"))
        if status == "lost":
            bucket["lost"] += 1
            bucket["lost_reasons"][_text(lead.get("lost_reason")) or "unknown"] += 1
        if status == "merged":
            bucket["merged"] += 1

    rows: list[dict[str, Any]] = []
    for bucket in buckets.values():
        total = int(bucket["total"] or 0)
        bucket["qualified_rate"] = round(bucket["qualified"] / total, 4) if total else 0.0
        bucket["won_rate"] = round(bucket["won"] / total, 4) if total else 0.0
        bucket["lost_rate"] = round(bucket["lost"] / total, 4) if total else 0.0
        bucket["won_revenue"] = round(float(bucket["won_revenue"]), 2)
        bucket["lost_reasons"] = dict(bucket["lost_reasons"])
        rows.append(bucket)
    rows.sort(key=lambda item: (float(item.get("won_revenue") or 0), int(item.get("total") or 0)), reverse=True)
    return {
        "group_by": group_key,
        "total_sources": len(rows),
        "sources": rows,
    }


def _period_key(value: Any, *, granularity: str) -> str | None:
    parsed = _parse_dt(value)
    if not parsed:
        return None
    if granularity == "week":
        iso = parsed.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return parsed.date().isoformat()


def summarize_time_funnel(
    leads: list[dict[str, Any]],
    *,
    granularity: str = "day",
    date_field: str = "created_at",
    periods: int = 30,
) -> dict[str, Any]:
    resolved_granularity = "week" if granularity == "week" else "day"
    resolved_date_field = date_field if date_field in {"created_at", "last_interaction_at", "won_at", "lost_at"} else "created_at"
    safe_periods = max(1, min(370, int(periods or 30)))
    buckets: dict[str, dict[str, Any]] = {}
    for lead in leads:
        key = _period_key(lead.get(resolved_date_field) or lead.get("last_interaction_at"), granularity=resolved_granularity)
        if not key:
            continue
        bucket = buckets.setdefault(
            key,
            {
                "period": key,
                "total": 0,
                "hot": 0,
                "quote_needed": 0,
                "order_created": 0,
                "won": 0,
                "lost": 0,
                "sla_breaches": 0,
                "followup_sent": 0,
                "won_revenue": 0.0,
            },
        )
        status = _text(lead.get("status"))
        bucket["total"] += 1
        if lead.get("temperature") == "hot":
            bucket["hot"] += 1
        if status == "quote_needed":
            bucket["quote_needed"] += 1
        if status == "order_created":
            bucket["order_created"] += 1
        if status == "won":
            bucket["won"] += 1
            bucket["won_revenue"] += _float(lead.get("won_revenue") or lead.get("order_total"))
        if status == "lost":
            bucket["lost"] += 1
        if int(lead.get("followup_count") or 0) > 0:
            bucket["followup_sent"] += 1
        bucket["sla_breaches"] += len(lead.get("sla_breaches") if isinstance(lead.get("sla_breaches"), list) else [])
    rows = sorted(buckets.values(), key=lambda item: item["period"])[-safe_periods:]
    for row in rows:
        total = int(row["total"] or 0)
        row["won_revenue"] = round(float(row["won_revenue"]), 2)
        row["won_rate"] = round(int(row["won"] or 0) / total, 4) if total else 0.0
        row["followup_rate"] = round(int(row["followup_sent"] or 0) / total, 4) if total else 0.0
    return {
        "granularity": resolved_granularity,
        "date_field": resolved_date_field,
        "periods": rows,
    }


def dashboard_contract() -> dict[str, Any]:
    return {
        "endpoints": {
            "list_leads": "GET /sales/leads?company_code={company_code}",
            "summary": "GET /sales/summary?company_code={company_code}",
            "lead_timeline": "GET /sales/leads/{lead_id}/timeline",
            "crm_export": "GET /sales/leads/{lead_id}/crm-export",
            "close_lead": "POST /sales/leads/{lead_id}/close",
            "merge_lead": "POST /sales/leads/{lead_id}/merge",
            "quote_sent": "POST /sales/leads/{lead_id}/quote/sent",
            "quote_accepted": "POST /sales/leads/{lead_id}/quote/accepted",
            "quote_rejected": "POST /sales/leads/{lead_id}/quote/rejected",
            "order_correction": "POST /sales/leads/{lead_id}/order-correction/{status}",
            "manager_performance": "GET /sales/managers/performance?company_code={company_code}",
            "source_funnel": "GET /sales/funnel/source?company_code={company_code}",
            "time_funnel": "GET /sales/funnel/time?company_code={company_code}",
            "quality_summary": "GET /sales/quality/summary?company_code={company_code}",
            "storage_summary": "GET /sales/admin/storage-summary",
            "prune": "POST /sales/admin/prune",
        },
        "auth": {"header": "X-AI-Agent-Token"},
        "roles": {
            "read": ["list_leads", "summary", "quality_summary", "manager_performance", "source_funnel", "time_funnel", "lead_timeline", "crm_export"],
            "manager": ["close_lead", "merge_lead", "quote_sent", "quote_accepted", "quote_rejected", "order_correction"],
            "admin": ["crm_sync_summary", "storage_summary", "prune"],
        },
        "lead_fields": [
            "lead_id",
            "status",
            "temperature",
            "next_action",
            "sales_owner_status",
            "quote_status",
            "order_correction_status",
            "duplicate_of_lead_id",
            "merged_into_lead_id",
            "source_channel",
            "source_utm_source",
            "source_utm_campaign",
            "won_revenue",
            "lost_reason",
        ],
    }


def crm_export_contract(*, channel: str, uid: str, session: dict[str, Any]) -> dict[str, Any]:
    lead = lead_snapshot(channel=channel, uid=uid, session=session)
    timeline = session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else []
    return crm_export_from_lead(lead=lead, timeline=timeline)


def crm_export_from_lead(*, lead: dict[str, Any], timeline: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    safe_timeline = timeline if isinstance(timeline, list) else []
    return {
        "lead_id": lead.get("lead_id"),
        "company_code": lead.get("company_code"),
        "session": {
            "session_id": lead.get("session_id"),
            "channel": lead.get("channel"),
            "channel_uid": lead.get("channel_uid"),
            "last_interaction_at": lead.get("last_interaction_at"),
        },
        "customer": {
            "buyer_name": lead.get("buyer_name"),
            "buyer_phone": lead.get("buyer_phone"),
            "buyer_identity_id": lead.get("buyer_identity_id"),
            "erp_customer_id": lead.get("erp_customer_id"),
        },
        "pipeline": {
            "status": lead.get("status"),
            "score": lead.get("score"),
            "temperature": lead.get("temperature"),
            "next_action": lead.get("next_action"),
            "quote_status": lead.get("quote_status"),
            "quote_id": lead.get("quote_id"),
            "quote_total": lead.get("quote_total"),
            "quote_currency": lead.get("quote_currency"),
            "lost_reason": lead.get("lost_reason"),
            "sales_owner_status": lead.get("sales_owner_status"),
            "sales_owner_action_by": lead.get("sales_owner_action_by"),
            "sales_owner_action_at": lead.get("sales_owner_action_at"),
            "sales_owner_accept_minutes": lead.get("sales_owner_accept_minutes"),
            "playbook_version": lead.get("playbook_version"),
            "duplicate_of_lead_id": lead.get("duplicate_of_lead_id"),
            "dedupe_reason": lead.get("dedupe_reason"),
            "merged_into_lead_id": lead.get("merged_into_lead_id"),
            "merged_duplicate_lead_ids": lead.get("merged_duplicate_lead_ids"),
            "merged_at": lead.get("merged_at"),
            "merged_by": lead.get("merged_by"),
            "order_correction_status": lead.get("order_correction_status"),
            "target_order_id": lead.get("target_order_id"),
            "correction_type": lead.get("correction_type"),
        },
        "source": {
            "channel": lead.get("source_channel"),
            "campaign": lead.get("source_campaign"),
            "utm_source": lead.get("source_utm_source"),
            "utm_medium": lead.get("source_utm_medium"),
            "utm_campaign": lead.get("source_utm_campaign"),
            "referrer": lead.get("source_referrer"),
            "landing_page": lead.get("source_landing_page"),
            "product_page": lead.get("source_product_page"),
        },
        "commercial_context": {
            "product_interest": lead.get("product_interest"),
            "need": lead.get("need"),
            "quantity": lead.get("quantity"),
            "uom": lead.get("uom"),
            "urgency": lead.get("urgency"),
            "delivery_need": lead.get("delivery_need"),
            "price_sensitivity": lead.get("price_sensitivity"),
            "decision_status": lead.get("decision_status"),
            "active_order_name": lead.get("active_order_name"),
            "active_order_state": lead.get("active_order_state"),
            "active_order_can_modify": lead.get("active_order_can_modify"),
            "expected_revenue": lead.get("expected_revenue"),
            "order_total": lead.get("order_total"),
            "currency": lead.get("currency"),
            "won_revenue": lead.get("won_revenue"),
        },
        "quote": {
            "quote_id": lead.get("quote_id"),
            "quote_status": lead.get("quote_status"),
            "quote_total": lead.get("quote_total"),
            "quote_currency": lead.get("quote_currency"),
            "quote_pdf_url": lead.get("quote_pdf_url"),
            "quote_prepared_at": lead.get("quote_prepared_at"),
            "quote_sent_at": lead.get("quote_sent_at"),
            "quote_accepted_at": lead.get("quote_accepted_at"),
            "quote_rejected_at": lead.get("quote_rejected_at"),
        },
        "timestamps": {
            "created_at": lead.get("created_at"),
            "qualified_at": lead.get("qualified_at"),
            "quote_needed_at": lead.get("quote_needed_at"),
            "order_ready_at": lead.get("order_ready_at"),
            "order_created_at": lead.get("order_created_at"),
            "won_at": lead.get("won_at"),
            "lost_at": lead.get("lost_at"),
            "stalled_at": lead.get("stalled_at"),
            "hot_at": lead.get("hot_at"),
            "handoff_at": lead.get("handoff_at"),
        },
        "quality": {
            "score": lead.get("conversation_quality_score"),
            "flags": lead.get("quality_flags"),
            "coaching_notes": lead.get("coaching_notes"),
            "evaluated_at": lead.get("quality_evaluated_at"),
        },
        "governance": {
            "followup_count": lead.get("followup_count"),
            "last_followup_at": lead.get("last_followup_at"),
            "sla_breaches": lead.get("sla_breaches"),
        },
        "conversation_summary": {
            "last_message_preview": lead.get("last_message_preview"),
            "timeline": safe_timeline[-50:],
        },
    }
