from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Query

from app.config import get_settings
from app.lead_management import apply_lead_merge, apply_manual_close, apply_order_correction_update, apply_quote_update, record_merged_duplicate
from app.sales_quality import update_session_quality
from app.sales_lead_repository import get_sales_lead_repository
from app.sales_reporting import (
    crm_export_contract,
    crm_export_from_lead,
    filter_leads,
    lead_snapshot,
    paginate_leads,
    summarize_leads,
    summarize_manager_performance,
    summarize_quality,
    summarize_source_funnel,
    summarize_time_funnel,
    dashboard_contract,
)
from app.sales_timeline import append_lead_timeline_event
from app.session_store import resolve_lead_session, save_session_snapshot

router = APIRouter()


def _authorize(x_ai_agent_token: str | None, *, role: str = "read") -> None:
    settings = get_settings()
    token = str(x_ai_agent_token or "")
    legacy_token = settings.ai_agent_token
    read_tokens = {legacy_token, settings.sales_dashboard_read_token, settings.sales_dashboard_manager_token, settings.sales_dashboard_admin_token}
    manager_tokens = {legacy_token, settings.sales_dashboard_manager_token, settings.sales_dashboard_admin_token}
    admin_tokens = {legacy_token, settings.sales_dashboard_admin_token}
    allowed = {
        "read": read_tokens,
        "manager": manager_tokens,
        "admin": admin_tokens,
    }.get(role, read_tokens)
    if token not in {item for item in allowed if item}:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _load_persisted_leads(company_code: str, limit: int) -> list[dict[str, Any]]:
    records = await get_sales_lead_repository().list_by_company(company_code=company_code, limit=limit)
    return [record["lead"] for record in records if isinstance(record, dict) and isinstance(record.get("lead"), dict)]


@router.get("/leads")
async def list_sales_leads(
    company_code: str = Query(..., min_length=1),
    status: str | None = Query(default=None),
    temperature: str | None = Query(default=None),
    sales_owner_status: str | None = Query(default=None),
    source_channel: str | None = Query(default=None),
    q: str | None = Query(default=None),
    include_none: bool = Query(default=False),
    include_lost: bool = Query(default=True),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(
        leads,
        company_code=company_code,
        status=status,
        temperature=temperature,
        sales_owner_status=sales_owner_status,
        source_channel=source_channel,
        include_none=include_none,
        include_lost=include_lost,
        q=q,
    )
    return {
        "company_code": company_code,
        "total": len(filtered),
        "offset": offset,
        "limit": limit,
        "leads": paginate_leads(filtered, offset=offset, limit=limit),
        "summary": summarize_leads(filtered),
        "filters": {
            "status": status,
            "temperature": temperature,
            "sales_owner_status": sales_owner_status,
            "source_channel": source_channel,
            "q": q,
            "include_none": include_none,
            "include_lost": include_lost,
        },
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/summary")
async def sales_lead_summary(
    company_code: str = Query(..., min_length=1),
    include_none: bool = Query(default=False),
    include_lost: bool = Query(default=True),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(
        leads,
        company_code=company_code,
        include_none=include_none,
        include_lost=include_lost,
    )
    return {
        "company_code": company_code,
        "summary": summarize_leads(filtered),
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/quality/summary")
async def sales_quality_summary(
    company_code: str = Query(..., min_length=1),
    include_none: bool = Query(default=False),
    include_lost: bool = Query(default=True),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    worst_limit: int = Query(default=20, ge=1, le=100),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(
        leads,
        company_code=company_code,
        include_none=include_none,
        include_lost=include_lost,
    )
    return {
        "company_code": company_code,
        "summary": summarize_quality(filtered, worst_limit=worst_limit),
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/crm-sync/summary")
async def sales_crm_sync_summary(
    company_code: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="admin")
    return {
        "company_code": company_code,
        "summary": await get_sales_lead_repository().crm_sync_summary(company_code=company_code, limit=limit),
    }


@router.get("/managers/performance")
async def sales_manager_performance(
    company_code: str = Query(..., min_length=1),
    include_lost: bool = Query(default=True),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(leads, company_code=company_code, include_lost=include_lost)
    return {
        "company_code": company_code,
        "summary": summarize_manager_performance(filtered),
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/funnel/source")
async def sales_source_funnel(
    company_code: str = Query(..., min_length=1),
    group_by: str = Query(default="source_channel"),
    include_lost: bool = Query(default=True),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(leads, company_code=company_code, include_lost=include_lost)
    return {
        "company_code": company_code,
        "summary": summarize_source_funnel(filtered, group_by=group_by),
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/funnel/time")
async def sales_time_funnel(
    company_code: str = Query(..., min_length=1),
    granularity: str = Query(default="day"),
    date_field: str = Query(default="created_at"),
    periods: int = Query(default=30, ge=1, le=370),
    include_lost: bool = Query(default=True),
    repository_limit: int = Query(default=5000, ge=100, le=50000),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    leads = await _load_persisted_leads(company_code=company_code, limit=repository_limit)
    filtered = filter_leads(leads, company_code=company_code, include_lost=include_lost)
    return {
        "company_code": company_code,
        "summary": summarize_time_funnel(filtered, granularity=granularity, date_field=date_field, periods=periods),
        "source": "sales_lead_repository",
        "repository_limit": repository_limit,
    }


@router.get("/dashboard/contract")
async def sales_dashboard_contract(
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    return dashboard_contract()


@router.get("/admin/storage-summary")
async def sales_storage_summary(
    company_code: str | None = Query(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="admin")
    return await get_sales_lead_repository().storage_summary(company_code=company_code)


@router.post("/admin/prune")
async def prune_sales_storage(
    payload: dict[str, Any] | None = Body(default=None),
    company_code: str | None = Query(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="admin")
    data = payload if isinstance(payload, dict) else {}
    resolved_company_code = str(company_code or data.get("company_code") or "").strip() or None
    return await get_sales_lead_repository().prune_storage(
        company_code=resolved_company_code,
        retention_days=_optional_int(data.get("retention_days")),
        max_per_company=_optional_int(data.get("max_per_company")),
    )


def _update_persisted_lead_record(
    *,
    record: dict[str, Any],
    profile: dict[str, Any],
    payload: dict[str, Any],
    actor_id: str | None,
    now_iso: str,
) -> dict[str, Any]:
    session = dict(record.get("session_context") if isinstance(record.get("session_context"), dict) else {})
    session["lead_profile"] = profile
    session["lead_timeline"] = record.get("timeline") if isinstance(record.get("timeline"), list) else []
    append_lead_timeline_event(
        session,
        event_type=f"lead_manually_closed_{profile.get('status')}",
        payload={
            "outcome": profile.get("status"),
            "lost_reason": profile.get("lost_reason"),
            "comment": payload.get("comment"),
            "actor_id": actor_id,
            "order_total": profile.get("order_total"),
            "won_revenue": profile.get("won_revenue"),
            "currency": profile.get("currency"),
            "source": "sales_dashboard",
        },
        actor=actor_id,
    )

    lead = dict(record.get("lead") if isinstance(record.get("lead"), dict) else {})
    for key in [
        "status",
        "lost_reason",
        "next_action",
        "quote_status",
        "quote_accepted_at",
        "quote_rejected_at",
        "order_total",
        "currency",
        "won_revenue",
        "manual_close_actor_id",
        "manual_closed_at",
        "manual_close_comment",
        "do_not_contact",
        "do_not_contact_reason",
        "won_at",
        "lost_at",
    ]:
        lead[key] = profile.get(key)
    lead["last_interaction_at"] = lead.get("last_interaction_at") or session.get("last_interaction_at")
    record["lead"] = lead
    record["lead_profile"] = profile
    record["session_context"] = {key: value for key, value in session.items() if key not in {"lead_profile", "lead_timeline"}}
    record["timeline"] = session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else []
    record["updated_at"] = now_iso
    return record


def _update_persisted_quote_record(
    *,
    record: dict[str, Any],
    profile: dict[str, Any],
    payload: dict[str, Any],
    actor_id: str | None,
    quote_status: str,
    now_iso: str,
) -> dict[str, Any]:
    session = dict(record.get("session_context") if isinstance(record.get("session_context"), dict) else {})
    session["lead_profile"] = profile
    session["lead_timeline"] = record.get("timeline") if isinstance(record.get("timeline"), list) else []
    append_lead_timeline_event(
        session,
        event_type=f"quote_{quote_status}",
        payload={
            "quote_status": profile.get("quote_status"),
            "quote_id": profile.get("quote_id"),
            "quote_total": profile.get("quote_total"),
            "quote_currency": profile.get("quote_currency"),
            "quote_pdf_url": profile.get("quote_pdf_url"),
            "comment": payload.get("comment"),
            "actor_id": actor_id,
            "source": "sales_dashboard",
        },
        actor=actor_id,
    )

    lead = dict(record.get("lead") if isinstance(record.get("lead"), dict) else {})
    for key in [
        "status",
        "next_action",
        "quote_status",
        "quote_id",
        "quote_total",
        "quote_currency",
        "quote_pdf_url",
        "quote_prepared_at",
        "quote_sent_at",
        "quote_accepted_at",
        "quote_rejected_at",
        "quote_last_actor_id",
        "quote_last_updated_at",
        "expected_revenue",
        "lost_reason",
        "do_not_contact",
        "do_not_contact_reason",
        "order_ready_at",
        "lost_at",
    ]:
        lead[key] = profile.get(key)
    record["lead"] = lead
    record["lead_profile"] = profile
    record["session_context"] = {key: value for key, value in session.items() if key not in {"lead_profile", "lead_timeline"}}
    record["timeline"] = session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else []
    record["updated_at"] = now_iso
    return record


async def _update_quote_status(
    *,
    lead_id: str,
    quote_status: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    actor_id = str(data.get("actor_id") or data.get("manager_id") or "").strip() or None
    now_iso = datetime.now(UTC).isoformat()

    resolved = await resolve_lead_session(lead_id)
    if resolved:
        channel, uid, session = resolved
        session["lead_profile"] = apply_quote_update(
            current_profile=session.get("lead_profile"),
            quote_status=quote_status,
            actor_id=actor_id,
            quote_id=data.get("quote_id"),
            quote_total=data.get("quote_total"),
            quote_currency=data.get("quote_currency") or data.get("currency"),
            quote_pdf_url=data.get("quote_pdf_url") or data.get("pdf_url"),
            comment=data.get("comment"),
        )
        append_lead_timeline_event(
            session,
            event_type=f"quote_{quote_status}",
            payload={
                "quote_status": session["lead_profile"].get("quote_status"),
                "quote_id": session["lead_profile"].get("quote_id"),
                "quote_total": session["lead_profile"].get("quote_total"),
                "quote_currency": session["lead_profile"].get("quote_currency"),
                "quote_pdf_url": session["lead_profile"].get("quote_pdf_url"),
                "comment": data.get("comment"),
                "actor_id": actor_id,
                "source": "sales_dashboard",
            },
            actor=actor_id,
        )
        await save_session_snapshot(channel, uid, session)
        return {
            "lead_id": lead_id,
            "session_id": f"{channel}:{uid}",
            "quote_status": session["lead_profile"].get("quote_status"),
            "status": session["lead_profile"].get("status"),
            "source": "active_session",
            "lead": lead_snapshot(channel=channel, uid=uid, session=session),
        }

    repo = get_sales_lead_repository()
    record = await repo.get(lead_id)
    if not record:
        raise HTTPException(status_code=404, detail="Lead not found")
    profile = apply_quote_update(
        current_profile=record.get("lead_profile"),
        quote_status=quote_status,
        actor_id=actor_id,
        quote_id=data.get("quote_id"),
        quote_total=data.get("quote_total"),
        quote_currency=data.get("quote_currency") or data.get("currency"),
        quote_pdf_url=data.get("quote_pdf_url") or data.get("pdf_url"),
        comment=data.get("comment"),
    )
    record = _update_persisted_quote_record(
        record=record,
        profile=profile,
        payload=data,
        actor_id=actor_id,
        quote_status=quote_status,
        now_iso=now_iso,
    )
    await repo.upsert_record(record)
    return {
        "lead_id": lead_id,
        "session_id": record.get("session_id"),
        "quote_status": profile.get("quote_status"),
        "status": profile.get("status"),
        "source": "sales_lead_repository",
        "lead": record.get("lead"),
    }


def _update_persisted_profile_record(
    *,
    record: dict[str, Any],
    profile: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
    actor_id: str | None,
    now_iso: str,
) -> dict[str, Any]:
    session = dict(record.get("session_context") if isinstance(record.get("session_context"), dict) else {})
    session["lead_profile"] = profile
    session["lead_timeline"] = record.get("timeline") if isinstance(record.get("timeline"), list) else []
    append_lead_timeline_event(session, event_type=event_type, payload=payload, actor=actor_id)
    lead = dict(record.get("lead") if isinstance(record.get("lead"), dict) else {})
    lead.update(profile)
    record["lead"] = lead
    record["lead_profile"] = profile
    record["session_context"] = {key: value for key, value in session.items() if key not in {"lead_profile", "lead_timeline"}}
    record["timeline"] = session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else []
    record["updated_at"] = now_iso
    return record


async def _update_order_correction(
    *,
    lead_id: str,
    correction_status: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    actor_id = str(data.get("actor_id") or data.get("manager_id") or "").strip() or None
    now_iso = datetime.now(UTC).isoformat()
    event_payload = {
        "order_correction_status": correction_status,
        "target_order_id": data.get("target_order_id"),
        "correction_type": data.get("correction_type"),
        "comment": data.get("comment"),
        "actor_id": actor_id,
        "source": "sales_dashboard",
    }

    resolved = await resolve_lead_session(lead_id)
    if resolved:
        channel, uid, session = resolved
        session["lead_profile"] = apply_order_correction_update(
            current_profile=session.get("lead_profile"),
            correction_status=correction_status,
            target_order_id=data.get("target_order_id") or session.get("last_sales_order_name"),
            correction_type=data.get("correction_type"),
            actor_id=actor_id,
            comment=data.get("comment"),
        )
        append_lead_timeline_event(session, event_type=f"order_correction_{correction_status}", payload=event_payload, actor=actor_id)
        await save_session_snapshot(channel, uid, session)
        return {
            "lead_id": lead_id,
            "session_id": f"{channel}:{uid}",
            "order_correction_status": session["lead_profile"].get("order_correction_status"),
            "source": "active_session",
            "lead": lead_snapshot(channel=channel, uid=uid, session=session),
        }

    repo = get_sales_lead_repository()
    record = await repo.get(lead_id)
    if not record:
        raise HTTPException(status_code=404, detail="Lead not found")
    profile = apply_order_correction_update(
        current_profile=record.get("lead_profile"),
        correction_status=correction_status,
        target_order_id=data.get("target_order_id"),
        correction_type=data.get("correction_type"),
        actor_id=actor_id,
        comment=data.get("comment"),
    )
    record = _update_persisted_profile_record(
        record=record,
        profile=profile,
        event_type=f"order_correction_{correction_status}",
        payload=event_payload,
        actor_id=actor_id,
        now_iso=now_iso,
    )
    await repo.upsert_record(record)
    return {
        "lead_id": lead_id,
        "session_id": record.get("session_id"),
        "order_correction_status": profile.get("order_correction_status"),
        "source": "sales_lead_repository",
        "lead": record.get("lead"),
    }


@router.post("/leads/{lead_id}/close")
async def manually_close_lead(
    lead_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    data = payload if isinstance(payload, dict) else {}
    outcome = str(data.get("outcome") or data.get("status") or "").strip().casefold()
    if outcome not in {"won", "lost"}:
        raise HTTPException(status_code=400, detail="outcome must be 'won' or 'lost'")
    actor_id = str(data.get("actor_id") or data.get("manager_id") or "").strip() or None
    now_iso = datetime.now(UTC).isoformat()

    resolved = await resolve_lead_session(lead_id)
    if resolved:
        channel, uid, session = resolved
        session["lead_profile"] = apply_manual_close(
            current_profile=session.get("lead_profile"),
            outcome=outcome,
            actor_id=actor_id,
            lost_reason=data.get("lost_reason"),
            comment=data.get("comment"),
            order_total=data.get("order_total"),
            won_revenue=data.get("won_revenue"),
            currency=data.get("currency"),
        )
        append_lead_timeline_event(
            session,
            event_type=f"lead_manually_closed_{outcome}",
            payload={
                "outcome": outcome,
                "lost_reason": session["lead_profile"].get("lost_reason"),
                "comment": data.get("comment"),
                "actor_id": actor_id,
                "order_total": session["lead_profile"].get("order_total"),
                "won_revenue": session["lead_profile"].get("won_revenue"),
                "currency": session["lead_profile"].get("currency"),
                "source": "sales_dashboard",
            },
            actor=actor_id,
        )
        await save_session_snapshot(channel, uid, session)
        return {
            "lead_id": lead_id,
            "session_id": f"{channel}:{uid}",
            "status": session["lead_profile"].get("status"),
            "lost_reason": session["lead_profile"].get("lost_reason"),
            "source": "active_session",
            "lead": lead_snapshot(channel=channel, uid=uid, session=session),
        }

    repo = get_sales_lead_repository()
    record = await repo.get(lead_id)
    if not record:
        raise HTTPException(status_code=404, detail="Lead not found")
    profile = apply_manual_close(
        current_profile=record.get("lead_profile"),
        outcome=outcome,
        actor_id=actor_id,
        lost_reason=data.get("lost_reason"),
        comment=data.get("comment"),
        order_total=data.get("order_total"),
        won_revenue=data.get("won_revenue"),
        currency=data.get("currency"),
    )
    record = _update_persisted_lead_record(
        record=record,
        profile=profile,
        payload=data,
        actor_id=actor_id,
        now_iso=now_iso,
    )
    await repo.upsert_record(record)
    return {
        "lead_id": lead_id,
        "session_id": record.get("session_id"),
        "status": profile.get("status"),
        "lost_reason": profile.get("lost_reason"),
        "source": "sales_lead_repository",
        "lead": record.get("lead"),
    }


@router.post("/leads/{lead_id}/merge")
async def merge_duplicate_lead(
    lead_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    data = payload if isinstance(payload, dict) else {}
    target_lead_id = str(data.get("target_lead_id") or data.get("merge_into_lead_id") or "").strip()
    if not target_lead_id:
        raise HTTPException(status_code=400, detail="target_lead_id is required")
    if target_lead_id == lead_id:
        raise HTTPException(status_code=400, detail="target_lead_id must differ from lead_id")
    actor_id = str(data.get("actor_id") or data.get("manager_id") or "").strip() or None
    now_iso = datetime.now(UTC).isoformat()
    repo = get_sales_lead_repository()

    duplicate_record = await repo.get(lead_id)
    target_record = await repo.get(target_lead_id)
    if not duplicate_record:
        raise HTTPException(status_code=404, detail="Duplicate lead not found")
    if not target_record:
        raise HTTPException(status_code=404, detail="Target lead not found")
    if duplicate_record.get("company_code") != target_record.get("company_code"):
        raise HTTPException(status_code=400, detail="Cannot merge leads from different tenants")

    duplicate_profile = apply_lead_merge(
        current_profile=duplicate_record.get("lead_profile"),
        target_lead_id=target_lead_id,
        actor_id=actor_id,
        comment=data.get("comment"),
    )
    target_profile = record_merged_duplicate(
        current_profile=target_record.get("lead_profile"),
        duplicate_lead_id=lead_id,
        actor_id=actor_id,
    )
    duplicate_record = _update_persisted_profile_record(
        record=duplicate_record,
        profile=duplicate_profile,
        event_type="lead_merged",
        payload={
            "target_lead_id": target_lead_id,
            "merged_into_lead_id": target_lead_id,
            "comment": data.get("comment"),
            "actor_id": actor_id,
            "source": "sales_dashboard",
        },
        actor_id=actor_id,
        now_iso=now_iso,
    )
    target_record = _update_persisted_profile_record(
        record=target_record,
        profile=target_profile,
        event_type="duplicate_lead_merged_into_this_lead",
        payload={
            "duplicate_lead_id": lead_id,
            "comment": data.get("comment"),
            "actor_id": actor_id,
            "source": "sales_dashboard",
        },
        actor_id=actor_id,
        now_iso=now_iso,
    )
    await repo.upsert_record(duplicate_record)
    await repo.upsert_record(target_record)
    return {
        "lead_id": lead_id,
        "target_lead_id": target_lead_id,
        "status": duplicate_profile.get("status"),
        "source": "sales_lead_repository",
        "lead": duplicate_record.get("lead"),
        "target_lead": target_record.get("lead"),
    }


@router.post("/leads/{lead_id}/quote/sent")
async def mark_quote_sent(
    lead_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    return await _update_quote_status(lead_id=lead_id, quote_status="sent", payload=payload)


@router.post("/leads/{lead_id}/quote/accepted")
async def mark_quote_accepted(
    lead_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    return await _update_quote_status(lead_id=lead_id, quote_status="accepted", payload=payload)


@router.post("/leads/{lead_id}/quote/rejected")
async def mark_quote_rejected(
    lead_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    return await _update_quote_status(lead_id=lead_id, quote_status="rejected", payload=payload)


@router.post("/leads/{lead_id}/order-correction/{correction_status}")
async def update_order_correction(
    lead_id: str,
    correction_status: str,
    payload: dict[str, Any] | None = Body(default=None),
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    if correction_status not in {"requested", "confirmed", "applied", "rejected"}:
        raise HTTPException(status_code=400, detail="correction_status must be requested, confirmed, applied, or rejected")
    return await _update_order_correction(lead_id=lead_id, correction_status=correction_status, payload=payload)


@router.get("/leads/{lead_id}/timeline")
async def lead_timeline(
    lead_id: str,
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    record = await get_sales_lead_repository().get(lead_id)
    if record:
        return {
            "lead_id": lead_id,
            "session_id": record.get("session_id"),
            "timeline": record.get("timeline") if isinstance(record.get("timeline"), list) else [],
            "sla_breaches": record.get("lead", {}).get("sla_breaches") if isinstance(record.get("lead"), dict) else [],
            "source": "sales_lead_repository",
        }
    resolved = await resolve_lead_session(lead_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Lead not found")
    channel, uid, session = resolved
    return {
        "lead_id": lead_id,
        "session_id": f"{channel}:{uid}",
        "timeline": session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else [],
        "sla_breaches": session.get("sla_breaches") if isinstance(session.get("sla_breaches"), list) else [],
    }


@router.get("/leads/{lead_id}/crm-export")
async def lead_crm_export(
    lead_id: str,
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token)
    record = await get_sales_lead_repository().get(lead_id)
    if record and isinstance(record.get("lead"), dict):
        return crm_export_from_lead(
            lead=record["lead"],
            timeline=record.get("timeline") if isinstance(record.get("timeline"), list) else [],
        )
    resolved = await resolve_lead_session(lead_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Lead not found")
    channel, uid, session = resolved
    return crm_export_contract(channel=channel, uid=uid, session=session)


@router.post("/leads/{lead_id}/quality/evaluate")
async def evaluate_lead_quality(
    lead_id: str,
    x_ai_agent_token: str | None = Header(default=None, alias="X-AI-Agent-Token"),
) -> dict[str, Any]:
    _authorize(x_ai_agent_token, role="manager")
    resolved = await resolve_lead_session(lead_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Lead not found")
    channel, uid, session = resolved
    quality = update_session_quality(session)
    await save_session_snapshot(channel, uid, session)
    return {
        "lead_id": lead_id,
        "session_id": f"{channel}:{uid}",
        **quality,
    }
