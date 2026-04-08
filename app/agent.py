import json
import logging
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.buyer_resolver import create_buyer_from_intro, resolve_buyer
from app.conversation_flow import advance_stage_after_tool, classify_behavior, classify_intent, derive_conversation_state, get_handoff_message
from app.config import get_settings
from app.i18n import text as i18n_text
from app.interaction_patterns import has_explicit_confirmation
from app.language_policy import resolve_conversation_language
from app.lead_management import (
    apply_llm_lead_patch,
    build_lead_event_payload,
    build_handoff_summary,
    ensure_lead_identity,
    mark_stalled_if_needed,
    normalize_lead_profile,
    sales_alert_event_types,
    sales_event_type,
    update_lead_profile_from_message,
    update_lead_profile_source,
    update_lead_profile_from_tool,
)
from app.license_client import get_license_client
from app.llm_state_updater import parse_llm_state_update
from app.outbound_channels import mark_sales_owner_notification, notify_sales_owner
from app.prompt_registry import build_runtime_system_prompt
from app.runtime_availability_context import build_availability_prefetch_context, selected_item_code, should_prefetch_item_availability
from app.runtime_catalog_context import build_catalog_prefetch_context, catalog_prefetch_search_term, should_prefetch_catalog_options
from app.sales_dedupe import detect_duplicate_lead
from app.sales_lead_repository import get_sales_lead_repository
from app.sales_quality import update_session_quality
from app.sales_reporting import lead_snapshot
from app.sales_timeline import append_lead_timeline_event
from app.session_store import load_session, new_session, save_session, save_session_snapshot
from app.tool_policy import evaluate_tool_call
from app.tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

_PHONE_RE = re.compile(r"(\+?\d[\d\s\-\(\)]{7,}\d)")
_MAX_OPENAI_INPUT_ITEMS = 48
_MAX_OPENAI_INPUT_BYTES = 180_000

def _empty_result() -> dict[str, Any]:
    return {"text": "...", "documents": []}


def _preview_text(text: str | None, limit: int = 160) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _log_event(event: str, **payload: Any) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))


def _tool_result_summary(tool_name: str, result: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "tool_name": tool_name,
        "ok": not bool(result.get("error")),
    }
    if result.get("error"):
        summary["error"] = _preview_text(str(result.get("error")))
    if result.get("error_code"):
        summary["error_code"] = result.get("error_code")
    if result.get("name"):
        summary["name"] = result.get("name")
    if result.get("erp_customer_id"):
        summary["erp_customer_id"] = result.get("erp_customer_id")
    if result.get("order_print_url"):
        summary["has_order_print_url"] = True
    if tool_name == "get_sales_order_status":
        summary["order_state"] = result.get("order_state")
        summary["can_modify"] = result.get("can_modify")
    if tool_name == "get_item_availability":
        summary["item_code"] = result.get("item_code")
        summary["in_stock"] = result.get("in_stock")
        summary["total_available_qty"] = result.get("total_available_qty")
    return summary


def _session_id(channel: str, channel_uid: str) -> str:
    return f"{channel}:{channel_uid}"


def _handoff_target(tenant: dict[str, Any]) -> dict[str, Any]:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    target = ai_policy.get("handoff_target") if isinstance(ai_policy.get("handoff_target"), dict) else {}
    return {
        "handoff_target_type": target.get("target_type") or "none",
        "handoff_target_destination": target.get("destination"),
        "handoff_target_instructions": target.get("instructions"),
    }


def _lead_management_config(tenant: dict[str, Any]) -> dict[str, Any]:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    lead_config = dict(ai_policy.get("lead_management")) if isinstance(ai_policy.get("lead_management"), dict) else {}
    catalog_policy = ai_policy.get("catalog") if isinstance(ai_policy.get("catalog"), dict) else {}
    if isinstance(catalog_policy.get("uom_aliases"), dict) and not isinstance(lead_config.get("uom_aliases"), dict):
        lead_config["uom_aliases"] = catalog_policy.get("uom_aliases")
    if isinstance(catalog_policy.get("uom_labels"), dict) and not isinstance(lead_config.get("uom_labels"), dict):
        lead_config["uom_labels"] = catalog_policy.get("uom_labels")
    return lead_config


def _playbook_version(tenant: dict[str, Any]) -> str | None:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    value = ai_policy.get("playbook_version") or ai_policy.get("sales_playbook_version")
    if value:
        return str(value).strip()
    prompt_overrides = ai_policy.get("prompt_overrides") if isinstance(ai_policy.get("prompt_overrides"), dict) else {}
    value = prompt_overrides.get("playbook_version")
    return str(value).strip() if value else None


def _lead_idle_after(tenant: dict[str, Any]) -> timedelta:
    lead_config = _lead_management_config(tenant)
    try:
        minutes = int(lead_config.get("stalled_after_minutes", 60) or 60)
    except (TypeError, ValueError):
        minutes = 60
    return timedelta(minutes=max(5, minutes))


def _dedupe_config(tenant: dict[str, Any]) -> dict[str, Any]:
    lead_config = _lead_management_config(tenant)
    return lead_config.get("dedupe") if isinstance(lead_config.get("dedupe"), dict) else lead_config


async def _apply_lead_dedupe(
    *,
    company_code: str,
    channel: str,
    channel_uid: str,
    session: dict[str, Any],
    tenant: dict[str, Any],
) -> None:
    config = _dedupe_config(tenant)
    if config.get("dedupe_enabled", True) is False:
        return
    profile = normalize_lead_profile(session.get("lead_profile"))
    if profile.get("duplicate_of_lead_id") or profile.get("merged_into_lead_id"):
        return
    if profile.get("status") in {"none", "won", "lost"}:
        return
    try:
        window_days = int(config.get("dedupe_window_days", 7) or 7)
    except (TypeError, ValueError):
        window_days = 7
    try:
        scan_limit = int(config.get("dedupe_scan_limit", 5000) or 5000)
    except (TypeError, ValueError):
        scan_limit = 5000
    try:
        records = await get_sales_lead_repository().list_by_company(
            company_code=company_code,
            limit=max(100, min(50000, scan_limit)),
        )
    except Exception as exc:
        logger.warning("Lead dedupe lookup failed for %s: %s", company_code, exc)
        return
    candidates = [record.get("lead") for record in records if isinstance(record, dict) and isinstance(record.get("lead"), dict)]
    current = lead_snapshot(channel=channel, uid=channel_uid, session=session)
    match = detect_duplicate_lead(current=current, candidates=candidates, window_days=window_days)
    if not match:
        profile["dedupe_checked_at"] = datetime.now(UTC).isoformat()
        session["lead_profile"] = profile
        return
    profile.update(
        {
            "duplicate_of_lead_id": match.get("duplicate_of_lead_id"),
            "dedupe_reason": match.get("dedupe_reason"),
            "dedupe_score": match.get("dedupe_score"),
            "dedupe_checked_at": match.get("dedupe_checked_at"),
            "merged_into_lead_id": match.get("duplicate_of_lead_id"),
        }
    )
    session["lead_profile"] = profile
    append_lead_timeline_event(
        session,
        event_type="lead_duplicate_detected",
        payload={
            "duplicate_of_lead_id": match.get("duplicate_of_lead_id"),
            "dedupe_reason": match.get("dedupe_reason"),
            "dedupe_score": match.get("dedupe_score"),
            "merged_into_lead_id": match.get("duplicate_of_lead_id"),
        },
    )


def _lead_event_payload(session: dict[str, Any], previous_profile: dict[str, Any] | None = None) -> dict[str, Any]:
    return build_lead_event_payload(session=session, previous_profile=previous_profile)


async def _emit_control_plane_event(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    try:
        await lc.create_conversation_event(
            company_code,
            event_type=event_type,
            session_id=_session_id(channel, channel_uid),
            channel_type=channel,
            channel_user_id=channel_uid,
            payload_json=payload,
        )
    except Exception as exc:
        logger.warning("Failed to send AI event %s to License Server: %s", event_type, exc)


async def _emit_sales_event_if_changed(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    session: dict[str, Any],
    previous_profile: dict[str, Any] | None,
    lead_config: dict[str, Any] | None = None,
) -> None:
    event_type = sales_event_type(previous_profile, session.get("lead_profile"))
    alert_event_types = sales_alert_event_types(previous_profile, session.get("lead_profile"))
    if not event_type and not alert_event_types:
        return
    profile = normalize_lead_profile(session.get("lead_profile"))
    previous = normalize_lead_profile(previous_profile)
    if previous.get("status") == "none" and profile.get("status") != "none" and event_type != "lead_created":
        append_lead_timeline_event(
            session,
            event_type="lead_created",
            payload={"previous_status": previous.get("status"), "status": profile.get("status")},
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="lead_created",
            payload=_lead_event_payload(session, previous_profile),
        )
    profile["last_sales_event"] = event_type or alert_event_types[-1]
    session["lead_profile"] = profile
    if event_type:
        append_lead_timeline_event(
            session,
            event_type=event_type,
            payload={"previous_status": previous.get("status"), "status": profile.get("status")},
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type=event_type,
            payload=_lead_event_payload(session, previous_profile),
        )
    for alert_event_type in alert_event_types:
        append_lead_timeline_event(
            session,
            event_type=alert_event_type,
            payload={
                "previous_temperature": previous.get("temperature"),
                "temperature": profile.get("temperature"),
                "next_action": profile.get("next_action"),
                "quote_status": profile.get("quote_status"),
            },
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type=alert_event_type,
            payload=_lead_event_payload(session, previous_profile),
        )
        if alert_event_type in {"hot_lead_detected", "manager_attention_required"}:
            if normalize_lead_profile(session.get("lead_profile")).get("sales_owner_status") in {"accepted", "closed_not_target"}:
                continue
            delivery = await _notify_sales_owner_if_configured(
                lead_config=lead_config or {},
                session=session,
                reason=alert_event_type,
            )
            mark_sales_owner_notification(session, delivery)
            if delivery.get("sent"):
                append_lead_timeline_event(
                    session,
                    event_type="sales_owner_notified",
                    payload={"reason": alert_event_type, "sales_owner_delivery": delivery},
                )
                await _emit_control_plane_event(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    event_type="sales_owner_notified",
                    payload={
                        **_lead_event_payload(session, previous_profile),
                        "reason": alert_event_type,
                        "sales_owner_delivery": {
                            "sent": True,
                            "status": delivery.get("status"),
                            "channel": delivery.get("channel"),
                        },
                    },
                )


async def _notify_sales_owner_if_configured(
    *,
    lead_config: dict[str, Any],
    session: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    if not lead_config.get("sales_owner_telegram_chat_id") and not lead_config.get("sales_owner_telegram_username"):
        return {"sent": False, "status": "sales_owner_not_configured"}
    try:
        return await notify_sales_owner(session=session, ai_policy={"lead_management": lead_config}, reason=reason)
    except Exception as exc:
        logger.warning("Failed to notify sales owner: %s", exc)
        return {"sent": False, "status": "send_failed", "error": str(exc)}


async def _emit_transcript_message(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    session: dict[str, Any],
    role: str,
    content: str | None = None,
    message_type: str = "chat",
    tool_name: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    try:
        await lc.create_transcript_message(
            company_code,
            _session_id(channel, channel_uid),
            message_id=str(uuid.uuid4()),
            channel_type=channel,
            channel_user_id=channel_uid,
            role=role,
            message_type=message_type,
            content=content,
            stage=session.get("stage"),
            behavior_class=session.get("behavior_class"),
            tool_name=tool_name,
            payload_json=payload or {},
            buyer_identity_id=session.get("buyer_identity_id"),
            erp_customer_id=session.get("erp_customer_id"),
            buyer_name=session.get("buyer_name"),
            buyer_phone=session.get("buyer_phone"),
        )
    except Exception as exc:
        logger.warning("Failed to send transcript message to License Server: %s", exc)


async def _emit_handoff(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    reason: str | None,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        return await lc.create_handoff(
            company_code,
            channel_type=channel,
            channel_user_id=channel_uid,
            session_id=_session_id(channel, channel_uid),
            reason=reason,
            payload_json=payload,
        )
    except Exception as exc:
        logger.warning("Failed to create AI handoff in License Server: %s", exc)
        return None


def _normalize_phone(text: str) -> str | None:
    digits = re.sub(r"[^\d+]", "", text)
    if not digits:
        return None
    if digits.startswith("+"):
        normalized = "+" + re.sub(r"\D", "", digits[1:])
    else:
        normalized = "+" + re.sub(r"\D", "", digits)
    return normalized if len(re.sub(r"\D", "", normalized)) >= 10 else None


def _extract_intro_contact(user_text: str) -> tuple[str | None, str | None]:
    match = _PHONE_RE.search(user_text)
    if not match:
        return None, None
    phone = _normalize_phone(match.group(1))
    if not phone:
        return None, None

    name = (user_text[: match.start()] + " " + user_text[match.end() :]).strip(" ,.;:-")
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        return None, None
    return name, phone


def get_intro_message(lang: str) -> str:
    return i18n_text("intro.sales_contact", lang)


def _apply_buyer_context(session: dict[str, Any], buyer_result: dict[str, Any]) -> None:
    if not isinstance(buyer_result, dict):
        return
    if buyer_result.get("erp_customer_id"):
        session["erp_customer_id"] = buyer_result.get("erp_customer_id")
    if buyer_result.get("erp_customer_name"):
        session["buyer_name"] = buyer_result.get("erp_customer_name")
    if buyer_result.get("buyer_identity_id"):
        session["buyer_identity_id"] = buyer_result.get("buyer_identity_id")
    if buyer_result.get("phone"):
        session["buyer_phone"] = buyer_result.get("phone")
    if buyer_result.get("recognized_via"):
        session["buyer_recognized_via"] = buyer_result.get("recognized_via")
    session["recent_sales_orders"] = buyer_result.get("recent_sales_orders") or []
    session["recent_sales_invoices"] = buyer_result.get("recent_sales_invoices") or []
    session["returning_customer_announced"] = False


def _is_returning_customer(session: dict[str, Any]) -> bool:
    return bool(session.get("recent_sales_orders") or session.get("recent_sales_invoices"))


def _returning_customer_prefix(lang: str, buyer_name: str | None = None) -> str:
    display_name = str(buyer_name or "").strip()
    suffix = f", {display_name}" if display_name else ""
    return i18n_text("returning_customer.prefix", lang, {"customer_suffix": suffix})

def _maybe_prefix_returning_customer(session: dict[str, Any], lang: str, reply: str) -> str:
    if not reply:
        return reply
    if _is_returning_customer(session) and not session.get("returning_customer_announced"):
        prefix = _returning_customer_prefix(lang, session.get("buyer_name"))
        session["returning_customer_announced"] = True
        return f"{prefix} {reply}".strip()
    return reply


def _build_system_prompt(
    tenant: dict,
    lang: str,
    channel: str,
    stage: str | None = None,
    behavior_class: str | None = None,
    buyer_name: str | None = None,
    erp_customer_id: str | None = None,
    last_sales_order_name: str | None = None,
    recent_sales_orders: list[dict[str, Any]] | None = None,
    recent_sales_invoices: list[dict[str, Any]] | None = None,
    lead_profile: dict[str, Any] | None = None,
    handoff_required: bool = False,
    handoff_reason: str | None = None,
) -> str:
    return build_runtime_system_prompt(
        tenant=tenant,
        lang=lang,
        channel=channel,
        stage=stage,
        behavior_class=behavior_class,
        buyer_name=buyer_name,
        erp_customer_id=erp_customer_id,
        last_sales_order_name=last_sales_order_name,
        recent_sales_orders=recent_sales_orders,
        recent_sales_invoices=recent_sales_invoices,
        lead_profile=lead_profile,
        handoff_required=handoff_required,
        handoff_reason=handoff_reason,
    )


def _history_to_openai_input(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for message in messages:
        role = message.get("role")
        content = message.get("content")
        if role not in {"user", "assistant"}:
            continue
        if isinstance(content, str):
            items.append({"role": role, "content": content})
            continue
        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") in {"text", "output_text"}:
                    text = block.get("text")
                    if isinstance(text, str) and text.strip():
                        text_parts.append(text)
            if text_parts:
                items.append({"role": role, "content": "\n".join(text_parts)})
    return items

def _estimate_input_items_size(input_items: list[dict[str, Any]]) -> int:
    return len(json.dumps(input_items, ensure_ascii=False, default=str))


def _trim_input_items(input_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trimmed = list(input_items)
    removed = 0
    while len(trimmed) > _MAX_OPENAI_INPUT_ITEMS or _estimate_input_items_size(trimmed) > _MAX_OPENAI_INPUT_BYTES:
        if len(trimmed) <= 2:
            break
        trimmed.pop(0)
        removed += 1
    if removed:
        logger.warning(
            "Trimmed OpenAI input context",
            extra={
                "removed_items": removed,
                "remaining_items": len(trimmed),
                "estimated_bytes": _estimate_input_items_size(trimmed),
            },
        )
    return trimmed


async def _maybe_prefetch_catalog_context(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    current_lang: str,
    user_text: str,
    tenant: dict[str, Any],
    session: dict[str, Any],
    intent: str | None,
) -> str | None:
    lead_profile = normalize_lead_profile(session.get("lead_profile"))
    if not should_prefetch_catalog_options(lead_profile=lead_profile, intent=intent):
        return None
    search_term = catalog_prefetch_search_term(lead_profile)
    if not search_term:
        return None

    inputs = {"item_name": search_term}
    result_str = await execute_tool(
        name="get_product_catalog",
        inputs=inputs,
        company_code=company_code,
        erp_customer_id=session.get("erp_customer_id"),
        active_sales_order_name=session.get("last_sales_order_name"),
        current_lang=current_lang,
        user_text=user_text,
        channel=channel,
        channel_uid=channel_uid,
        lc=lc,
        ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
        lead_profile=lead_profile,
    )
    try:
        parsed_result = json.loads(result_str)
    except json.JSONDecodeError:
        parsed_result = {"raw_result": result_str, "error": "invalid_prefetch_result"}

    previous_tool_lead_profile = normalize_lead_profile(session.get("lead_profile"))
    session["lead_profile"] = update_lead_profile_from_tool(
        current_profile=previous_tool_lead_profile,
        tool_name="get_product_catalog",
        inputs=inputs,
        tool_result=parsed_result if isinstance(parsed_result, dict) else {},
        stage=session.get("stage"),
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=session.get("last_sales_order_name"),
    )
    await save_session_snapshot(channel, channel_uid, session)
    await _emit_sales_event_if_changed(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        previous_profile=previous_tool_lead_profile,
        lead_config=_lead_management_config(tenant),
    )

    summary = _tool_result_summary("get_product_catalog", parsed_result if isinstance(parsed_result, dict) else {})
    summary["source"] = "runtime_prefetch"
    summary["search_term"] = search_term
    _log_event(
        "catalog_prefetch_finished",
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        stage=session.get("stage"),
        summary=summary,
    )
    await _emit_control_plane_event(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        event_type="catalog_prefetch_finished",
        payload={
            "tool_name": "get_product_catalog",
            "stage": session.get("stage"),
            "lead_profile": session.get("lead_profile"),
            **summary,
        },
    )
    append_lead_timeline_event(
        session,
        event_type="catalog_prefetch_finished",
        payload=summary,
    )
    await _emit_transcript_message(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        role="tool",
        message_type="tool_prefetch",
        tool_name="get_product_catalog",
        content=result_str,
        payload=summary,
    )
    return build_catalog_prefetch_context(parsed_result if isinstance(parsed_result, dict) else {}, search_term=search_term)


async def _maybe_prefetch_availability_context(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    current_lang: str,
    user_text: str,
    tenant: dict[str, Any],
    session: dict[str, Any],
) -> str | None:
    lead_profile = normalize_lead_profile(session.get("lead_profile"))
    if not should_prefetch_item_availability(lead_profile=lead_profile, user_text=user_text):
        return None
    item_code = selected_item_code(lead_profile)
    if not item_code:
        return None

    inputs = {"item_code": item_code}
    result_str = await execute_tool(
        name="get_item_availability",
        inputs=inputs,
        company_code=company_code,
        erp_customer_id=session.get("erp_customer_id"),
        active_sales_order_name=session.get("last_sales_order_name"),
        current_lang=current_lang,
        user_text=user_text,
        channel=channel,
        channel_uid=channel_uid,
        lc=lc,
        ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
        lead_profile=lead_profile,
    )
    try:
        parsed_result = json.loads(result_str)
    except json.JSONDecodeError:
        parsed_result = {"raw_result": result_str, "error": "invalid_availability_prefetch_result"}

    previous_tool_lead_profile = normalize_lead_profile(session.get("lead_profile"))
    session["lead_profile"] = update_lead_profile_from_tool(
        current_profile=previous_tool_lead_profile,
        tool_name="get_item_availability",
        inputs=inputs,
        tool_result=parsed_result if isinstance(parsed_result, dict) else {},
        stage=session.get("stage"),
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=session.get("last_sales_order_name"),
    )
    await save_session_snapshot(channel, channel_uid, session)
    await _emit_sales_event_if_changed(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        previous_profile=previous_tool_lead_profile,
        lead_config=_lead_management_config(tenant),
    )

    summary = _tool_result_summary("get_item_availability", parsed_result if isinstance(parsed_result, dict) else {})
    summary["source"] = "runtime_prefetch"
    _log_event(
        "availability_prefetch_finished",
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        stage=session.get("stage"),
        summary=summary,
    )
    await _emit_control_plane_event(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        event_type="availability_prefetch_finished",
        payload={
            "tool_name": "get_item_availability",
            "stage": session.get("stage"),
            "lead_profile": session.get("lead_profile"),
            **summary,
        },
    )
    append_lead_timeline_event(
        session,
        event_type="availability_prefetch_finished",
        payload=summary,
    )
    await _emit_transcript_message(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        role="tool",
        message_type="tool_prefetch",
        tool_name="get_item_availability",
        content=result_str,
        payload=summary,
    )
    return build_availability_prefetch_context(parsed_result if isinstance(parsed_result, dict) else {})


def _extract_output_text(response: dict[str, Any]) -> str:
    text_parts: list[str] = []
    for item in response.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                text = content.get("text")
                if isinstance(text, str):
                    text_parts.append(text)
    return " ".join(part.strip() for part in text_parts if part.strip()).strip()


def _extract_function_calls(response: dict[str, Any]) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    for item in response.get("output", []):
        if item.get("type") == "function_call":
            calls.append(item)
    return calls


def _format_customer_reply(text: str) -> str:
    if not text:
        return text

    cleaned = text.replace("**", "").replace("__", "")
    result = "\n".join(line.rstrip() for line in cleaned.splitlines()).strip()
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result or "..."


async def _create_openai_response(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    input_items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model,
        "instructions": system_prompt,
        "input": input_items,
        "tools": TOOLS,
        "tool_choice": "auto",
        "parallel_tool_calls": False,
        "max_output_tokens": 2048,
    }
    response = await client.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
    )
    response.raise_for_status()
    return response.json()


def _confirmation_classifier_enabled(tenant: dict[str, Any]) -> bool:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    return bool(sales_policy.get("llm_confirmation_classifier_enabled", True))


def _state_updater_enabled(tenant: dict[str, Any]) -> bool:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    return bool(sales_policy.get("llm_state_updater_enabled", False))


def _state_updater_min_confidence(tenant: dict[str, Any]) -> float:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    try:
        return max(0.5, min(0.99, float(sales_policy.get("llm_state_updater_min_confidence", 0.7) or 0.7)))
    except (TypeError, ValueError):
        return 0.7


def _confirmation_min_confidence(tenant: dict[str, Any]) -> float:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    try:
        return max(0.5, min(0.99, float(sales_policy.get("llm_confirmation_min_confidence", 0.72) or 0.72)))
    except (TypeError, ValueError):
        return 0.72


def _parse_confirmation_classifier_response(text: str) -> dict[str, Any]:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return {"confirmed": False, "confidence": 0.0, "reason": "invalid_json"}
    if not isinstance(payload, dict):
        return {"confirmed": False, "confidence": 0.0, "reason": "invalid_payload"}
    try:
        confidence = float(payload.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    return {
        "confirmed": bool(payload.get("confirmed")),
        "confidence": max(0.0, min(1.0, confidence)),
        "reason": _preview_text(str(payload.get("reason") or "")),
    }


async def _classify_state_with_llm(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    user_text: str,
    session: dict[str, Any],
    tenant: dict[str, Any],
) -> dict[str, Any]:
    lead_profile = normalize_lead_profile(session.get("lead_profile"))
    payload = {
        "model": model,
        "instructions": (
            "Classify the customer's latest message for sales-runtime state update. "
            "Return only compact JSON with keys: intent, behavior_class, confidence, lead_patch, reason. "
            "intent must be one of: low_signal, find_product, browse_catalog, order_detail, confirm_order, add_to_order, service_request, human_handoff. "
            "behavior_class must be one of: direct_buyer, explorer, unclear_request, price_sensitive, frustrated, service_request, returning_customer, silent_or_low_signal. "
            "lead_patch may include only these keys when clearly supported by the message and context: product_interest, quantity, uom, urgency, delivery_need, price_sensitivity, decision_status. "
            "Do not invent values. Use any customer language. If unsure, keep lead_patch empty and lower confidence."
        ),
        "input": [
            {
                "role": "system",
                "content": json.dumps(
                    {
                        "current_stage": session.get("stage"),
                        "current_intent": session.get("last_intent"),
                        "current_behavior_class": session.get("behavior_class"),
                        "customer_identified": bool(session.get("erp_customer_id")),
                        "active_order_name": session.get("last_sales_order_name"),
                        "lead_profile": {
                            "status": lead_profile.get("status"),
                            "product_interest": lead_profile.get("product_interest"),
                            "quantity": lead_profile.get("quantity"),
                            "uom": lead_profile.get("uom"),
                            "product_resolution_status": lead_profile.get("product_resolution_status"),
                            "next_action": lead_profile.get("next_action"),
                            "qualification_priority": lead_profile.get("qualification_priority"),
                            "price_sensitivity": lead_profile.get("price_sensitivity"),
                        },
                    },
                    ensure_ascii=False,
                ),
            },
            {"role": "user", "content": user_text},
        ],
        "max_output_tokens": 300,
    }
    response = await client.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
    )
    response.raise_for_status()
    result = parse_llm_state_update(_extract_output_text(response.json()))
    result["source"] = "llm"
    return result


async def _classify_order_confirmation_with_llm(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    user_text: str,
    session: dict[str, Any],
    tool_name: str,
    inputs: dict[str, Any],
    tenant: dict[str, Any],
) -> dict[str, Any]:
    if not _confirmation_classifier_enabled(tenant):
        return {
            "confirmed": has_explicit_confirmation(user_text),
            "confidence": 1.0 if has_explicit_confirmation(user_text) else 0.0,
            "reason": "regex_fallback_classifier_disabled" if has_explicit_confirmation(user_text) else "classifier_disabled",
            "source": "regex" if has_explicit_confirmation(user_text) else "disabled",
        }
    lead_profile = normalize_lead_profile(session.get("lead_profile"))
    context = {
        "customer_message": user_text,
        "regex_confirmation_hint": has_explicit_confirmation(user_text),
        "conversation_stage": session.get("stage"),
        "last_intent": session.get("last_intent"),
        "tool_name": tool_name,
        "tool_inputs": inputs,
        "active_order_name": session.get("last_sales_order_name"),
        "lead_profile": {
            "status": lead_profile.get("status"),
            "next_action": lead_profile.get("next_action"),
            "qualification_priority": lead_profile.get("qualification_priority"),
            "product_interest": lead_profile.get("product_interest"),
            "quantity": lead_profile.get("quantity"),
            "uom": lead_profile.get("uom"),
            "requested_items": lead_profile.get("requested_items"),
            "requested_items_need_uom_confirmation": lead_profile.get("requested_items_need_uom_confirmation"),
        },
    }
    payload = {
        "model": model,
        "instructions": (
            "Classify whether the customer's latest message explicitly confirms proceeding with the current order or order change. "
            "Handle any language, especially Hebrew. Return only compact JSON with keys: confirmed, confidence, reason. "
            "confirmed must be true only when the message clearly authorizes proceeding with the current order/change in context. "
            "Reluctant but affirmative confirmations can be true. Questions, new details, price requests, corrections, and negations must be false. "
            "If ambiguous, return confirmed=false."
        ),
        "input": [{"role": "user", "content": json.dumps(context, ensure_ascii=False, default=str)}],
        "max_output_tokens": 120,
    }
    response = await client.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
    )
    response.raise_for_status()
    result = _parse_confirmation_classifier_response(_extract_output_text(response.json()))
    result["source"] = "llm"
    return result


async def process_message_result(
    channel: str,
    channel_uid: str,
    user_text: str,
    tenant: dict,
    channel_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    company_code: str = tenant["company_code"]
    lc = get_license_client()
    result = _empty_result()

    if not isinstance(tenant.get("ai_policy"), dict):
        try:
            policy_response = await lc.get_ai_policy(company_code)
            tenant["ai_policy"] = policy_response.get("ai_policy", {})
        except Exception as exc:
            logger.warning("Failed to load AI policy for %s: %s", company_code, exc)

    await _emit_control_plane_event(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        event_type="tenant_resolved",
        payload={
            "company_code": company_code,
            "company_name": tenant.get("company_name"),
        },
    )

    session = await load_session(channel, channel_uid)
    if session.get("company_code") and session["company_code"] != company_code:
        session = new_session(company_code=company_code)
    session["company_code"] = company_code
    if isinstance(channel_context, dict) and channel_context:
        merged_channel_context = dict(session.get("channel_context") or {})
        merged_channel_context.update(channel_context)
        session["channel_context"] = merged_channel_context
    session["lead_profile"] = ensure_lead_identity(
        current_profile=session.get("lead_profile"),
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
    )
    if _playbook_version(tenant):
        session["lead_profile"]["playbook_version"] = _playbook_version(tenant)
    session["lead_profile"] = update_lead_profile_source(
        current_profile=session.get("lead_profile"),
        channel=channel,
        channel_context=session.get("channel_context") if isinstance(session.get("channel_context"), dict) else {},
    )
    default_lang = tenant.get("ai_language", "ru")
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=session.get("lang"),
        user_text=user_text,
        default_lang=default_lang,
    )
    if lang_to_lock:
        session["lang"] = lang_to_lock
    previous_stalled_profile = normalize_lead_profile(session.get("lead_profile"))
    session["lead_profile"] = mark_stalled_if_needed(
        current_profile=previous_stalled_profile,
        last_interaction_at=session.get("last_interaction_at"),
        idle_after=_lead_idle_after(tenant),
    )
    await save_session_snapshot(channel, channel_uid, session)
    await _emit_sales_event_if_changed(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        previous_profile=previous_stalled_profile,
        lead_config=_lead_management_config(tenant),
    )

    buyer_result, needs_intro = await resolve_buyer(
        session=session,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        lc=lc,
    )
    if buyer_result and buyer_result.get("erp_customer_id"):
        _apply_buyer_context(session, buyer_result)
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="buyer_resolved",
            payload={
                "erp_customer_id": session.get("erp_customer_id"),
                "buyer_identity_id": session.get("buyer_identity_id"),
                "recognized_via": session.get("buyer_recognized_via"),
                "history_orders": len(session.get("recent_sales_orders") or []),
                "history_invoices": len(session.get("recent_sales_invoices") or []),
                "source": "central_resolve",
            },
        )

    if needs_intro and not session.get("erp_customer_id"):
        intro_name, intro_phone = _extract_intro_contact(user_text)
        if intro_name and intro_phone:
            buyer_result = await create_buyer_from_intro(
                session=session,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                full_name=intro_name,
                phone=intro_phone,
                lc=lc,
            )
            if buyer_result and buyer_result.get("erp_customer_id"):
                _apply_buyer_context(session, buyer_result)
                session["buyer_name"] = intro_name
                needs_intro = False
                await _emit_control_plane_event(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    event_type="buyer_resolved",
                    payload={
                        "erp_customer_id": session.get("erp_customer_id"),
                        "buyer_identity_id": session.get("buyer_identity_id"),
                        "recognized_via": buyer_result.get("recognized_via"),
                        "source": "intro_registration",
                    },
                )

    active_order_name = session.get("last_sales_order_name")
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None
    previous_lead_profile = normalize_lead_profile(session.get("lead_profile"))
    llm_state_result: dict[str, Any] | None = None
    if _state_updater_enabled(tenant):
        try:
            async with httpx.AsyncClient(timeout=30.0) as state_client:
                llm_state_result = await _classify_state_with_llm(
                    client=state_client,
                    api_key=settings.openai_api_key,
                    model=settings.openai_model,
                    user_text=user_text,
                    session=session,
                    tenant=tenant,
                )
        except Exception as exc:
            logger.warning("LLM state updater failed: %s", exc)
            llm_state_result = {"valid": False, "reason": str(exc), "source": "error"}

    seeded_profile = previous_lead_profile
    use_llm_state = (
        isinstance(llm_state_result, dict)
        and llm_state_result.get("valid")
        and float(llm_state_result.get("confidence") or 0) >= _state_updater_min_confidence(tenant)
    )
    if use_llm_state:
        seeded_profile = apply_llm_lead_patch(
            current_profile=previous_lead_profile,
            patch=llm_state_result.get("lead_patch") if isinstance(llm_state_result.get("lead_patch"), dict) else {},
            lead_config=_lead_management_config(tenant),
        )

    fallback_behavior_class, fallback_behavior_confidence = classify_behavior(user_text, session, ai_policy=ai_policy)
    fallback_intent, fallback_intent_confidence = classify_intent(user_text, ai_policy=ai_policy)
    behavior_class = str(llm_state_result.get("behavior_class") or "") if use_llm_state else ""
    intent = str(llm_state_result.get("intent") or "") if use_llm_state else ""
    llm_confidence = float(llm_state_result.get("confidence") or 0) if use_llm_state else 0.0
    behavior_confidence = llm_confidence if behavior_class else fallback_behavior_confidence
    intent_confidence = llm_confidence if intent else fallback_intent_confidence
    if not behavior_class:
        behavior_class = fallback_behavior_class
        behavior_confidence = fallback_behavior_confidence
    if not intent:
        intent = fallback_intent
        intent_confidence = fallback_intent_confidence
    if behavior_class == "silent_or_low_signal" and intent == "find_product":
        intent = "low_signal"
        intent_confidence = max(intent_confidence, behavior_confidence)
    session["lead_profile"] = update_lead_profile_from_message(
        current_profile=seeded_profile,
        user_text=user_text,
        stage=session.get("stage"),
        behavior_class=behavior_class,
        intent=intent,
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=active_order_name,
        lead_config=_lead_management_config(tenant),
    )
    session.update(
        derive_conversation_state(
            session=session,
            user_text=user_text,
            channel=channel,
            needs_intro=needs_intro,
            customer_identified=bool(session.get("erp_customer_id")),
            active_order_name=active_order_name,
            ai_policy=ai_policy,
            lead_profile=session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else None,
            previous_lead_profile=previous_lead_profile,
            behavior_class=behavior_class,
            behavior_confidence=behavior_confidence,
            intent=intent,
            intent_confidence=intent_confidence,
        )
    )
    await _apply_lead_dedupe(
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        tenant=tenant,
    )
    await save_session_snapshot(channel, channel_uid, session)
    await _emit_sales_event_if_changed(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        previous_profile=previous_lead_profile,
        lead_config=_lead_management_config(tenant),
    )
    _log_event(
        "conversation_state",
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        lang=current_lang,
        stage=session.get("stage"),
        stage_confidence=session.get("stage_confidence"),
        behavior_class=session.get("behavior_class"),
        behavior_confidence=session.get("behavior_confidence"),
        intent=session.get("last_intent"),
        intent_confidence=session.get("last_intent_confidence"),
        handoff_required=session.get("handoff_required"),
        handoff_reason=session.get("handoff_reason"),
        lead_profile=session.get("lead_profile"),
        has_customer=bool(session.get("erp_customer_id")),
        has_active_order=bool(active_order_name),
        user_text_preview=_preview_text(user_text),
    )
    await _emit_control_plane_event(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        event_type="conversation_state_changed",
        payload={
            "lang": current_lang,
            "stage": session.get("stage"),
            "stage_confidence": session.get("stage_confidence"),
            "behavior_class": session.get("behavior_class"),
            "behavior_confidence": session.get("behavior_confidence"),
            "intent": session.get("last_intent"),
            "intent_confidence": session.get("last_intent_confidence"),
            "handoff_required": session.get("handoff_required"),
            "handoff_reason": session.get("handoff_reason"),
            "lead_profile": session.get("lead_profile"),
            "has_customer": bool(session.get("erp_customer_id")),
            "has_active_order": bool(active_order_name),
        },
    )
    await _emit_transcript_message(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        role="user",
        content=user_text,
        message_type="chat",
        payload={"lang": current_lang, "lead_profile": session.get("lead_profile")},
    )

    if normalize_lead_profile(session.get("lead_profile")).get("sales_owner_status") == "accepted":
        reply = get_handoff_message(
            current_lang,
            "sales_owner_accepted",
            ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
        )
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": reply})
        append_lead_timeline_event(
            session,
            event_type="human_takeover_active",
            payload={"reason": "sales_owner_accepted"},
        )
        await save_session(channel, channel_uid, session)
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="human_takeover_active",
            payload={"lead_profile": session.get("lead_profile")},
        )
        await _emit_transcript_message(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            session=session,
            role="assistant",
            content=reply,
            message_type="handoff",
            payload={"reason": "sales_owner_accepted", "lead_profile": session.get("lead_profile")},
        )
        result["handoff_required"] = True
        result["handoff_reason"] = "sales_owner_accepted"
        result["text"] = reply
        return result

    if (
        needs_intro
        and not session.get("erp_customer_id")
        and session.get("stage") == "identify"
        and session.get("last_intent") in {"low_signal", "service_request"}
        and not normalize_lead_profile(session.get("lead_profile")).get("product_interest")
    ):
        if session.get("handoff_required"):
            reply = get_handoff_message(
                current_lang,
                session.get("handoff_reason"),
                ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
            )
            session["messages"].append({"role": "user", "content": user_text})
            session["messages"].append({"role": "assistant", "content": reply})
            await save_session(channel, channel_uid, session)
            _log_event(
                "conversation_outcome",
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                outcome="handoff",
                stage=session.get("stage"),
                handoff_reason=session.get("handoff_reason"),
                reply_preview=_preview_text(reply),
            )
            handoff_response = await _emit_handoff(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                reason=session.get("handoff_reason"),
                payload={
                    "stage": session.get("stage"),
                    "reply_preview": _preview_text(reply),
                    "erp_customer_id": session.get("erp_customer_id"),
                    "buyer_name": session.get("buyer_name"),
                    "active_order_name": session.get("last_sales_order_name"),
                    "handoff_summary": build_handoff_summary(session, reason=session.get("handoff_reason")),
                    "lead_profile": session.get("lead_profile"),
                    **_handoff_target(tenant),
                },
            )
            await _emit_control_plane_event(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                event_type="handoff_triggered",
                payload={
                    "reason": session.get("handoff_reason"),
                    "stage": session.get("stage"),
                    "delivery_status": (
                        handoff_response.get("delivery", {}).get("status")
                        if isinstance(handoff_response, dict)
                        else None
                    ),
                },
            )
            result["handoff_required"] = True
            result["handoff_reason"] = session.get("handoff_reason")
            result["text"] = reply
            await _emit_transcript_message(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                role="assistant",
                content=reply,
                message_type="handoff",
                payload={
                    "reason": session.get("handoff_reason"),
                    "handoff_summary": build_handoff_summary(session, reason=session.get("handoff_reason")),
                },
            )
            return result
        reply = get_intro_message(current_lang)
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": reply})
        await save_session(channel, channel_uid, session)
        _log_event(
            "conversation_outcome",
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            outcome="intro_requested",
            stage=session.get("stage"),
            reply_preview=_preview_text(reply),
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="conversation_outcome",
            payload={
                "outcome": "intro_requested",
                "stage": session.get("stage"),
                "reply_preview": _preview_text(reply),
            },
        )
        result["text"] = reply
        await _emit_transcript_message(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            session=session,
            role="assistant",
            content=reply,
            message_type="intro",
        )
        return result

    if session.get("handoff_required"):
        final_reply = get_handoff_message(
            current_lang,
            session.get("handoff_reason"),
            ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
        )
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": final_reply})
        await save_session(channel, channel_uid, session)
        _log_event(
            "conversation_outcome",
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            outcome="handoff",
            stage=session.get("stage"),
            handoff_reason=session.get("handoff_reason"),
            reply_preview=_preview_text(final_reply),
        )
        handoff_response = await _emit_handoff(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            reason=session.get("handoff_reason"),
            payload={
                "stage": session.get("stage"),
                "reply_preview": _preview_text(final_reply),
                "erp_customer_id": session.get("erp_customer_id"),
                "buyer_name": session.get("buyer_name"),
                "active_order_name": session.get("last_sales_order_name"),
                "handoff_summary": build_handoff_summary(session, reason=session.get("handoff_reason")),
                "lead_profile": session.get("lead_profile"),
                **_handoff_target(tenant),
            },
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="handoff_triggered",
            payload={
                "reason": session.get("handoff_reason"),
                "stage": session.get("stage"),
                "delivery_status": (
                    handoff_response.get("delivery", {}).get("status")
                    if isinstance(handoff_response, dict)
                    else None
                ),
            },
        )
        result["handoff_required"] = True
        result["handoff_reason"] = session.get("handoff_reason")
        result["text"] = final_reply
        await _emit_transcript_message(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            session=session,
            role="assistant",
            content=final_reply,
            message_type="handoff",
            payload={
                "reason": session.get("handoff_reason"),
                "handoff_summary": build_handoff_summary(session, reason=session.get("handoff_reason")),
            },
        )
        return result

    prefetched_catalog_context = await _maybe_prefetch_catalog_context(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        current_lang=current_lang,
        user_text=user_text,
        tenant=tenant,
        session=session,
        intent=session.get("last_intent"),
    )
    prefetched_availability_context = await _maybe_prefetch_availability_context(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        current_lang=current_lang,
        user_text=user_text,
        tenant=tenant,
        session=session,
    )
    history = _history_to_openai_input(session["messages"])
    input_items: list[dict[str, Any]] = _trim_input_items(history + [{"role": "user", "content": user_text}])
    max_iter = 10
    final_reply = "..."

    async with httpx.AsyncClient(timeout=60.0) as client:
        for _ in range(max_iter):
            system_prompt = _build_system_prompt(
                tenant,
                current_lang,
                channel,
                stage=session.get("stage"),
                behavior_class=session.get("behavior_class"),
                buyer_name=session.get("buyer_name"),
                erp_customer_id=session.get("erp_customer_id"),
                last_sales_order_name=session.get("last_sales_order_name"),
                recent_sales_orders=session.get("recent_sales_orders"),
                recent_sales_invoices=session.get("recent_sales_invoices"),
                lead_profile=session.get("lead_profile"),
                handoff_required=bool(session.get("handoff_required")),
                handoff_reason=session.get("handoff_reason"),
            )
            if prefetched_catalog_context:
                system_prompt = system_prompt + "\n\n" + prefetched_catalog_context
            if prefetched_availability_context:
                system_prompt = system_prompt + "\n\n" + prefetched_availability_context
            response = await _create_openai_response(
                client,
                api_key=settings.openai_api_key,
                model=settings.openai_model,
                system_prompt=system_prompt,
                input_items=input_items,
            )

            function_calls = _extract_function_calls(response)
            reply_text = _extract_output_text(response)
            if reply_text:
                final_reply = reply_text

            if not function_calls:
                break

            tool_outputs: list[dict[str, Any]] = []
            for call in function_calls:
                tool_name = str(call.get("name") or "")
                raw_arguments = call.get("arguments") or "{}"
                try:
                    inputs = json.loads(raw_arguments)
                except json.JSONDecodeError:
                    inputs = {}

                _log_event(
                    "tool_call_started",
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    tool_name=tool_name,
                    stage=session.get("stage"),
                    behavior_class=session.get("behavior_class"),
                    inputs=inputs,
                )
                await _emit_control_plane_event(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    event_type="tool_call_started",
                    payload={
                        "tool_name": tool_name,
                        "stage": session.get("stage"),
                        "behavior_class": session.get("behavior_class"),
                        "lead_profile": session.get("lead_profile"),
                        "inputs": inputs,
                    },
                )
                await _emit_transcript_message(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    session=session,
                    role="tool",
                    message_type="tool_call",
                    tool_name=tool_name,
                    content=json.dumps(inputs, ensure_ascii=False, default=str),
                    payload={"inputs": inputs},
                )

                confirmation_result: dict[str, Any] | None = None
                confirmation_override: bool | None = None
                if tool_name in {"create_sales_order", "update_sales_order"}:
                    try:
                        confirmation_result = await _classify_order_confirmation_with_llm(
                            client=client,
                            api_key=settings.openai_api_key,
                            model=settings.openai_model,
                            user_text=user_text,
                            session=session,
                            tool_name=tool_name,
                            inputs=inputs,
                            tenant=tenant,
                        )
                        min_confidence = _confirmation_min_confidence(tenant)
                        if _confirmation_classifier_enabled(tenant):
                            if (
                                confirmation_result.get("confirmed")
                                and float(confirmation_result.get("confidence") or 0) >= min_confidence
                            ):
                                confirmation_override = True
                            else:
                                confirmation_override = False
                        elif (
                            confirmation_result.get("confirmed")
                            and float(confirmation_result.get("confidence") or 0) >= min_confidence
                        ):
                            confirmation_override = True
                        _log_event(
                            "order_confirmation_classified",
                            company_code=company_code,
                            channel=channel,
                            channel_uid=channel_uid,
                            tool_name=tool_name,
                            min_confidence=min_confidence,
                            classification=confirmation_result,
                        )
                    except Exception as exc:
                        confirmation_result = {"confirmed": False, "confidence": 0.0, "reason": str(exc), "source": "error"}
                        logger.warning("Order confirmation classifier failed: %s", exc)

                policy_result = evaluate_tool_call(
                    tool_name=tool_name,
                    inputs=inputs,
                    session=session,
                    tenant=tenant,
                    user_text=user_text,
                    confirmation_override=confirmation_override,
                )
                if policy_result:
                    parsed_result = policy_result
                    model_result_str = json.dumps(policy_result, ensure_ascii=False, default=str)
                else:
                    result_str = await execute_tool(
                        name=tool_name,
                        inputs=inputs,
                        company_code=company_code,
                        erp_customer_id=session.get("erp_customer_id"),
                        active_sales_order_name=session.get("last_sales_order_name"),
                        current_lang=current_lang,
                        user_text=user_text,
                        channel=channel,
                        channel_uid=channel_uid,
                        lc=lc,
                        ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
                        lead_profile=session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else None,
                        confirmation_override=confirmation_override,
                    )
                    try:
                        parsed_result = json.loads(result_str)
                    except json.JSONDecodeError:
                        parsed_result = {"raw_result": result_str}
                    model_result_str = json.dumps(parsed_result, ensure_ascii=False, default=str)

                if tool_name == "register_buyer" and parsed_result.get("erp_customer_id"):
                    _apply_buyer_context(session, parsed_result)
                if tool_name in {"create_sales_order", "update_sales_order", "send_sales_order_pdf"} and parsed_result.get("name"):
                    session["last_sales_order_name"] = parsed_result.get("name")
                    session["last_order_activity_at"] = datetime.now(UTC).isoformat()
                if tool_name in {"create_sales_order", "update_sales_order", "send_sales_order_pdf"} and parsed_result.get("order_print_url"):
                    result.setdefault("documents", []).append(
                        {
                            "type": "sales_order_pdf",
                            "url": parsed_result.get("order_print_url"),
                            "filename": f"{parsed_result.get('name') or 'sales-order'}.pdf",
                        }
                    )
                if tool_name == "create_invoice" and parsed_result.get("invoice_url"):
                    result.setdefault("documents", []).append(
                        {
                            "type": "sales_invoice",
                            "url": parsed_result.get("invoice_url"),
                            "filename": f"{parsed_result.get('name') or 'sales-invoice'}",
                        }
                    )
                advance_stage_after_tool(session, tool_name, parsed_result)
                previous_tool_lead_profile = normalize_lead_profile(session.get("lead_profile"))
                session["lead_profile"] = update_lead_profile_from_tool(
                    current_profile=previous_tool_lead_profile,
                    tool_name=tool_name,
                    inputs=inputs,
                    tool_result=parsed_result if isinstance(parsed_result, dict) else {},
                    stage=session.get("stage"),
                    customer_identified=bool(session.get("erp_customer_id")),
                    active_order_name=session.get("last_sales_order_name"),
                )
                await save_session_snapshot(channel, channel_uid, session)
                await _emit_sales_event_if_changed(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    session=session,
                    previous_profile=previous_tool_lead_profile,
                    lead_config=_lead_management_config(tenant),
                )

                summary = _tool_result_summary(tool_name, parsed_result if isinstance(parsed_result, dict) else {})
                if confirmation_result:
                    summary["confirmation_classification"] = confirmation_result
                _log_event(
                    "tool_call_finished",
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    tool_name=tool_name,
                    stage=session.get("stage"),
                    summary=summary,
                )
                await _emit_control_plane_event(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    event_type="tool_call_finished",
                    payload={
                        "tool_name": tool_name,
                        "stage": session.get("stage"),
                        "lead_profile": session.get("lead_profile"),
                        **summary,
                    },
                )
                append_lead_timeline_event(
                    session,
                    event_type="tool_call_finished",
                    payload={"tool_name": tool_name, **summary},
                )
                if tool_name == "create_sales_order" and parsed_result.get("name"):
                    append_lead_timeline_event(
                        session,
                        event_type="order_created",
                        payload={"name": parsed_result.get("name"), "tool_name": tool_name},
                    )
                    await _emit_control_plane_event(
                        lc=lc,
                        company_code=company_code,
                        channel=channel,
                        channel_uid=channel_uid,
                        event_type="order_created",
                        payload={"name": parsed_result.get("name")},
                    )
                if tool_name == "create_invoice" and parsed_result.get("name"):
                    append_lead_timeline_event(
                        session,
                        event_type="invoice_created",
                        payload={"name": parsed_result.get("name"), "tool_name": tool_name},
                    )
                    await _emit_control_plane_event(
                        lc=lc,
                        company_code=company_code,
                        channel=channel,
                        channel_uid=channel_uid,
                        event_type="invoice_created",
                        payload={"name": parsed_result.get("name")},
                    )
                await _emit_transcript_message(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    session=session,
                    role="tool",
                    message_type="tool_result",
                    tool_name=tool_name,
                    content=model_result_str,
                    payload=summary,
                )

                tool_outputs.append(
                    {
                        "type": "function_call_output",
                        "call_id": call["call_id"],
                        "output": model_result_str,
                    }
                )

            input_items = _trim_input_items(input_items + response.get("output", []) + tool_outputs)
        else:
            final_reply = "Произошла внутренняя ошибка, попробуйте позже."

    session["messages"].append({"role": "user", "content": user_text})
    final_reply = _format_customer_reply(final_reply)
    final_reply = _maybe_prefix_returning_customer(session, current_lang, final_reply)
    session["messages"].append({"role": "assistant", "content": final_reply})
    session["messages"] = session["messages"][-40:]
    previous_quality_flags = set(session.get("quality_flags") or []) if isinstance(session.get("quality_flags"), list) else set()
    quality = update_session_quality(session, ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None)
    if quality.get("quality_flags"):
        append_lead_timeline_event(
            session,
            event_type="conversation_quality_flagged",
            payload={
                "quality_score": quality.get("conversation_quality_score"),
                "quality_flags": quality.get("quality_flags"),
            },
        )
        new_quality_flags = set(quality.get("quality_flags") or []) - previous_quality_flags
        if new_quality_flags & {"risky_promise_without_tool", "discount_promise_blocked_by_sales_policy", "stock_promise_without_tool", "delivery_promise_without_tool"}:
            delivery = await _notify_sales_owner_if_configured(
                session=session,
                lead_config=_lead_management_config(tenant),
                reason="sales_quality_risk",
            )
            mark_sales_owner_notification(session, delivery)
            if delivery.get("sent"):
                append_lead_timeline_event(
                    session,
                    event_type="sales_owner_notified",
                    payload={"reason": "sales_quality_risk", "sales_owner_delivery": delivery},
                )
    await save_session(channel, channel_uid, session)
    _log_event(
        "conversation_outcome",
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        outcome="assistant_reply",
        stage=session.get("stage"),
        behavior_class=session.get("behavior_class"),
        handoff_required=session.get("handoff_required"),
        lead_profile=session.get("lead_profile"),
        active_order_name=session.get("last_sales_order_name"),
        document_count=len(result.get("documents", [])),
        reply_preview=_preview_text(final_reply),
    )
    await _emit_control_plane_event(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        event_type="conversation_outcome",
        payload={
            "outcome": "assistant_reply",
            "stage": session.get("stage"),
            "behavior_class": session.get("behavior_class"),
            "handoff_required": session.get("handoff_required"),
            "lead_profile": session.get("lead_profile"),
            "active_order_name": session.get("last_sales_order_name"),
            "document_count": len(result.get("documents", [])),
        },
    )
    if session.get("stage") == "closed":
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="conversation_closed",
            payload={
                "active_order_name": session.get("last_sales_order_name"),
                "lead_profile": session.get("lead_profile"),
            },
        )
    await _emit_transcript_message(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        role="assistant",
        content=final_reply,
        message_type="closed" if session.get("stage") == "closed" else "chat",
        payload={"documents": result.get("documents", []), "lead_profile": session.get("lead_profile")},
    )
    result["text"] = final_reply
    return result


async def process_message(
    channel: str,
    channel_uid: str,
    user_text: str,
    tenant: dict,
    channel_context: dict[str, Any] | None = None,
) -> str:
    result = await process_message_result(channel, channel_uid, user_text, tenant, channel_context=channel_context)
    return str(result.get("text") or "")
