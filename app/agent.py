import json
import logging
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.buyer_resolver import resolve_buyer, resolve_buyer_from_intro
from app.buyer_intake import (
    buyer_company_ambiguous_message as _buyer_company_ambiguous_message_text,
    buyer_company_lookup_error_message as _buyer_company_lookup_error_message_text,
    buyer_company_request_message as _buyer_company_request_message_text,
    buyer_company_retry_message as _buyer_company_retry_message_text,
    buyer_identity_review_message as _buyer_identity_review_message_text,
    clean_company_candidate as _clean_company_candidate_text,
    get_intro_sales_contact_message as _intro_sales_contact_message_text,
    get_known_buyer_greeting as _known_buyer_greeting_text,
)
from app.conversation_flow import (
    advance_stage_after_tool,
    classify_behavior,
    classify_intent,
    derive_conversation_state,
    get_handoff_message,
)
from app.conversation_boundary import is_short_greeting_message
from app.conversation_contexts import (
    active_lead_profile,
    active_related_order_id,
    active_signal_state,
    mark_active_context_status,
    mutate_active_lead_profile,
    reconcile_contexts_after_state_update,
    set_active_lead_profile,
)
from app.config import get_settings
from app.i18n import text as i18n_text
from app.interaction_patterns import has_explicit_confirmation
from app.inbound_policy import should_block_for_intro_before_assistance, should_request_intro_before_next_step
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
from app.lead_runtime_config import lead_config_from_ai_policy
from app.license_client import get_license_client
from app.llm_state_updater import parse_llm_signal_classification, parse_llm_state_update
from app.outbound_channels import mark_sales_owner_notification, notify_sales_owner
from app.order_confirmation import message_completes_order_details
from app.phone_numbers import normalize_phone as _normalize_phone
from app.prompt_registry import build_runtime_system_prompt
from app.greeting_policy import (
    returning_customer_prefix as _returning_customer_prefix_text,
    select_contact_display_name,
    should_send_known_buyer_greeting,
)
from app.runtime_availability_context import build_availability_prefetch_context, selected_item_code, should_prefetch_item_availability
from app.runtime_catalog_context import build_catalog_prefetch_context, catalog_prefetch_search_term, should_prefetch_catalog_options
from app.sales_dedupe import detect_duplicate_lead
from app.sales_lead_repository import get_sales_lead_repository
from app.sales_quality import update_session_quality
from app.sales_reporting import lead_snapshot
from app.sales_timeline import append_lead_timeline_event
from app.session_store import load_session, new_session, save_session, save_session_snapshot, session_processing_lock
from app.tool_policy import evaluate_tool_call
from app.tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

_PHONE_RE = re.compile(r"(\+?\d[\d\s\-\(\)]{7,}\d)")
_INTRO_NAME_PREFIX_RE = re.compile(
    r"(?is)^\s*(?:"
    r"hello|hi|hey|good\s+(?:day|morning|afternoon|evening)|"
    r"привет|здравствуйте|добрый\s+\w+|"
    r"שלום|shalom|"
    r"مرحبا|أهلا|اهلا|السلام عليكم|"
    r"my\s+name\s+is|name\s+is|i\s+am|i'm|call\s+me|"
    r"меня\s+зовут|это|"
    r"שמי|קוראים\s+לי|אני|"
    r"اسمي|انا|أنا"
    r")[\s,:-]*"
)
_INTRO_NAME_STOP_RE = re.compile(
    r"(?is)\b(?:"
    r"i\s+need|need|want|order|check|looking\s+for|interested\s+in|"
    r"мне\s+нуж|нужен|нужна|хочу|интересует|заказ|провер|"
    r"אני\s+צריך|אני\s+רוצה|צריך|רוצה|הזמנה|"
    r"أريد|احتاج|أحتاج|طلب|please"
    r")\b"
)
_PERSON_NAME_TOKEN_RE = re.compile(r"^[A-Za-zА-Яа-яЁё\u0590-\u05FF\u0600-\u06FF][A-Za-zА-Яа-яЁё\u0590-\u05FF\u0600-\u06FF'._-]*$")
_COMPANY_PREFIX_RE = re.compile(
    r"(?is)^\s*(?:"
    r"i\s+work\s+(?:at|for)|i(?:\s*am|'m)\s+from|from|company|my\s+company\s+is|"
    r"я\s+работаю\s+в|я\s+из|я\s+из\s+компании|компания|из\s+компании|"
    r"אני\s+עובד(?:ת)?\s+ב|אני\s+מ|אני\s+מחברת|מחברת|מהחברה|שם\s+החברה(?:\s+שלי)?|"
    r"أعمل\s+في|أنا\s+من|أنا\s+من\s+شركة|الشركة|شركة"
    r")[\s,:-]*"
)
_GENERIC_COMPANY_VALUES = {
    "company",
    "my company",
    "компания",
    "из компании",
    "חברה",
    "החברה",
    "شركة",
    "الشركة",
}
_KNOWN_BUYER_GREETING = {
    "en": "Hello, {buyer_name}. How can I help?",
    "ru": "Здравствуйте, {buyer_name}. Чем могу помочь?",
    "he": "שלום, {buyer_name}. איך אפשר לעזור?",
    "ar": "مرحبًا، {buyer_name}. كيف أستطيع المساعدة؟",
}
_BUYER_COMPANY_REQUEST = {
    "en": "Thanks, {buyer_name}. I couldn't match your phone to an existing customer yet. Which company do you work for?",
    "ru": "Спасибо, {buyer_name}. Я пока не нашёл клиента по вашему номеру. В какой компании вы работаете?",
    "he": "תודה, {buyer_name}. עדיין לא הצלחתי לזהות לקוח קיים לפי מספר הטלפון שלך. באיזו חברה אתה עובד?",
    "ar": "شكرًا، {buyer_name}. لم أتمكن بعد من לזהות العميل الحالي לפי رقم الهاتف. في أي شركة تعمل؟",
}
_BUYER_IDENTITY_REVIEW = {
    "en": "Thanks. I saved your details and sent them to a manager so they can link your contact to the correct company in ERP before we continue.",
    "ru": "Спасибо. Я сохранил ваши данные и передал их менеджеру, чтобы он связал ваш контакт с нужной компанией в ERP перед продолжением.",
    "he": "תודה. שמרתי את הפרטים שלך והעברתי אותם למנהל כדי שיקשר את איש הקשר שלך לחברה הנכונה ב-ERP לפני שנמשיך.",
    "ar": "شكرًا. حفظت تفاصيلك وأرسلتها إلى المدير ليربط جهة الاتصال الخاصة بك بالشركة الصحيحة في ERP قبل أن نتابع.",
}
_MAX_OPENAI_INPUT_ITEMS = 48
_MAX_OPENAI_INPUT_BYTES = 180_000
_MAX_OPENAI_HISTORY_ITEMS = 10
_MAX_OPENAI_MESSAGE_CHARS = 700
_KNOWN_TOOL_NAMES = {str(tool.get("name") or "").strip() for tool in TOOLS if isinstance(tool, dict)}

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


def _compact_sales_order_items_for_model(items: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    compact_items: list[dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        compact = {
            "name": item.get("name"),
            "item_code": item.get("item_code"),
            "item_name": item.get("item_name"),
            "qty": item.get("qty"),
            "uom": item.get("uom") or item.get("stock_uom"),
        }
        compact = {key: value for key, value in compact.items() if value not in (None, "", [])}
        if compact:
            compact_items.append(compact)
        if len(compact_items) >= limit:
            break
    return compact_items


def _compact_catalog_items_for_model(items: Any, *, limit: int = 5) -> list[dict[str, Any]]:
    compact_items: list[dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        uoms: list[dict[str, Any]] = []
        for uom in item.get("available_uoms") if isinstance(item.get("available_uoms"), list) else []:
            if not isinstance(uom, dict):
                continue
            uom_compact = {
                "uom": uom.get("uom"),
                "display_name": uom.get("display_name"),
                "uom_semantic": uom.get("uom_semantic"),
            }
            uoms.append({key: value for key, value in uom_compact.items() if value not in (None, "", [])})
        compact = {
            "item_code": item.get("item_code"),
            "item_name": item.get("item_name"),
            "display_item_name": item.get("display_item_name"),
            "description": item.get("description"),
            "currency": item.get("currency"),
            "image_url": item.get("image_url"),
            "stock_uom_label": item.get("stock_uom_label"),
            "customer_uom_options": item.get("customer_uom_options"),
            "customer_uom_summary": item.get("customer_uom_summary"),
            "available_uoms": uoms,
        }
        compact = {key: value for key, value in compact.items() if value not in (None, "", [])}
        if compact:
            compact_items.append(compact)
        if len(compact_items) >= limit:
            break
    return compact_items


def _compact_tool_result_for_model(tool_name: str, result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "error": "invalid_tool_result"}
    if result.get("error"):
        compact_error = {
            "error": result.get("error"),
            "error_code": result.get("error_code"),
            "tool_name": tool_name,
        }
        if tool_name == "get_sales_order_status" or result.get("sales_order_name") or result.get("name"):
            compact_error.update(
                {
                    "sales_order_name": result.get("sales_order_name") or result.get("name"),
                    "order_state": result.get("order_state"),
                    "can_modify": result.get("can_modify"),
                }
            )
        return {key: value for key, value in compact_error.items() if value not in (None, "", [])}

    if tool_name == "get_sales_order_status":
        compact = {
            "sales_order_name": result.get("sales_order_name") or result.get("name"),
            "order_state": result.get("order_state"),
            "can_modify": result.get("can_modify"),
            "status": result.get("status"),
            "docstatus": result.get("docstatus"),
            "delivery_status": result.get("delivery_status"),
            "billing_status": result.get("billing_status"),
            "order_total": result.get("order_total") or result.get("grand_total") or result.get("total"),
            "currency": result.get("currency"),
            "items": _compact_sales_order_items_for_model(result.get("items")),
        }
        return {key: value for key, value in compact.items() if value not in (None, "", [])}

    if tool_name == "get_product_catalog":
        compact = {
            "items": _compact_catalog_items_for_model(result.get("items")),
            "resolved_via_item_code": result.get("resolved_via_item_code"),
            "price_display_blocked": result.get("price_display_blocked"),
            "price_display_blocked_reason": result.get("price_display_blocked_reason"),
            "price_anchor": result.get("price_anchor"),
        }
        return {key: value for key, value in compact.items() if value not in (None, "", [])}

    if tool_name == "get_item_availability":
        compact = {
            "item_code": result.get("item_code"),
            "item_name": result.get("item_name"),
            "stock_uom": result.get("stock_uom"),
            "in_stock": result.get("in_stock"),
            "total_available_qty": result.get("total_available_qty"),
            "effective_warehouse": result.get("effective_warehouse") or result.get("warehouse"),
            "default_warehouse": result.get("default_warehouse"),
            "known_warehouses": (result.get("known_warehouses") or [])[:5] if isinstance(result.get("known_warehouses"), list) else None,
            "needs_warehouse_selection": result.get("needs_warehouse_selection"),
        }
        return {key: value for key, value in compact.items() if value not in (None, "", [])}

    return result


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
    return lead_config_from_ai_policy(ai_policy)


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
    profile = normalize_lead_profile(active_lead_profile(session))
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
        set_active_lead_profile(session, profile)
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
    set_active_lead_profile(session, profile)
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
    event_type = sales_event_type(previous_profile, active_lead_profile(session))
    alert_event_types = sales_alert_event_types(previous_profile, active_lead_profile(session))
    if not event_type and not alert_event_types:
        return
    profile = normalize_lead_profile(active_lead_profile(session))
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
    set_active_lead_profile(session, profile)
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
            if normalize_lead_profile(active_lead_profile(session)).get("sales_owner_status") in {"accepted", "closed_not_target"}:
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


def _clean_intro_name_candidate(text: str) -> str | None:
    candidate = re.sub(r"\s+", " ", str(text or "")).strip(" ,.;:-")
    if not candidate:
        return None
    candidate = _INTRO_NAME_PREFIX_RE.sub("", candidate).strip(" ,.;:-")
    stop_match = _INTRO_NAME_STOP_RE.search(candidate)
    if stop_match:
        candidate = candidate[: stop_match.start()].strip(" ,.;:-")
    if not candidate:
        return None
    chunks = [chunk.strip(" ,.;:-") for chunk in re.split(r"[,;|/]+", candidate) if chunk.strip(" ,.;:-")]
    scored_candidates: list[tuple[int, str]] = []
    for chunk in chunks or [candidate]:
        tokens = [token for token in re.split(r"\s+", chunk) if token]
        if not tokens or len(tokens) > 4:
            continue
        if not all(_PERSON_NAME_TOKEN_RE.match(token) for token in tokens):
            continue
        score = len(tokens) * 10 - len(chunk)
        scored_candidates.append((score, " ".join(tokens)))
    if not scored_candidates:
        return None
    scored_candidates.sort(reverse=True)
    return scored_candidates[0][1]


def _extract_intro_contact(user_text: str) -> tuple[str | None, str | None]:
    match = _PHONE_RE.search(user_text)
    if not match:
        return None, None
    phone = _normalize_phone(match.group(1))
    if not phone:
        return None, None

    name = _clean_intro_name_candidate((user_text[: match.start()] + " " + user_text[match.end() :]).strip(" ,.;:-"))
    if not name:
        return None, None
    return name, phone


def _clean_company_candidate(text: str) -> str | None:
    return _clean_company_candidate_text(text)


def _normalize_company_match_text(value: str | None) -> str:
    normalized = re.sub(r"[\W_~]+", " ", str(value or "").casefold(), flags=re.UNICODE)
    return re.sub(r"\s+", " ", normalized).strip()


def _select_company_candidate_query(text: str, candidates: list[dict[str, Any]] | None) -> str | None:
    raw = str(text or "").strip()
    if not raw or not isinstance(candidates, list):
        return None

    if raw.isdigit():
        index = int(raw)
        if 1 <= index <= len(candidates):
            candidate = candidates[index - 1]
            if isinstance(candidate, dict):
                company_number = str(candidate.get("company_number") or "").strip()
                company_name = str(candidate.get("company_name") or "").strip()
                return company_number or company_name or None

    normalized = raw.casefold()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        company_number = str(candidate.get("company_number") or "").strip()
        company_name = str(candidate.get("company_name") or "").strip()
        if company_number and normalized == company_number.casefold():
            return company_number
        if company_name and normalized == company_name.casefold():
            return company_number or company_name

    cleaned_query = _clean_company_candidate(raw) or raw
    normalized_query = _normalize_company_match_text(cleaned_query)
    if not normalized_query:
        return None

    query_tokens = [token for token in normalized_query.split(" ") if token]
    narrowed: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        company_number = str(candidate.get("company_number") or "").strip()
        company_name = str(candidate.get("company_name") or "").strip()
        normalized_name = _normalize_company_match_text(company_name)
        if not normalized_name:
            continue
        if normalized_query in normalized_name:
            narrowed.append(candidate)
            continue
        if query_tokens and all(token in normalized_name for token in query_tokens):
            narrowed.append(candidate)
            continue
        if company_number and normalized_query == company_number:
            narrowed.append(candidate)

    if len(narrowed) == 1:
        candidate = narrowed[0]
        company_number = str(candidate.get("company_number") or "").strip()
        company_name = str(candidate.get("company_name") or "").strip()
        return company_number or company_name or None
    return None


def get_intro_message(lang: str) -> str:
    return _intro_sales_contact_message_text(lang)


def get_known_buyer_greeting(lang: str, buyer_name: str | None = None) -> str:
    return _known_buyer_greeting_text(lang, buyer_name)


def _buyer_company_request_message(lang: str, buyer_name: str | None = None) -> str:
    return _buyer_company_request_message_text(lang, buyer_name)


def _buyer_identity_review_message(lang: str) -> str:
    return _buyer_identity_review_message_text(lang)


def _buyer_company_retry_message(lang: str) -> str:
    return _buyer_company_retry_message_text(lang)


def _buyer_company_ambiguous_message(lang: str, options: list[str]) -> str:
    return _buyer_company_ambiguous_message_text(lang, options)


def _buyer_company_lookup_error_message(lang: str) -> str:
    return _buyer_company_lookup_error_message_text(lang)


def _small_talk_reply(lang: str) -> str:
    return i18n_text("welcome.generic", lang)
    replies = {
        "en": "I'm good, thanks. How can I help?",
        "ru": "Все хорошо, спасибо. Чем могу помочь?",
        "he": "הכול טוב, תודה. איך אפשר לעזור?",
        "ar": "أنا بخير، شكرًا. كيف أستطيع المساعدة؟",
    }
    return replies.get(str(lang or "").strip().lower(), replies["en"])


def _normalize_buyer_language_code(value: str | None) -> str | None:
    raw = str(value or "").strip().lower().replace("_", "-")
    if not raw:
        return None
    base = raw.split("-", 1)[0]
    aliases = {"iw": "he", "heb": "he", "eng": "en", "rus": "ru", "ara": "ar"}
    normalized = aliases.get(base, base)
    if normalized not in {"en", "ru", "he", "ar"}:
        return None
    return normalized


def _clear_pending_buyer_state(session: dict[str, Any]) -> None:
    session["buyer_company_name"] = None
    session["buyer_company_registry_number"] = None
    session["buyer_company_candidates"] = []
    session["buyer_company_pending"] = False
    session["buyer_review_required"] = False
    session["buyer_review_case_id"] = None
    session["buyer_identity_status"] = None


def _set_pending_buyer_contact(session: dict[str, Any], *, full_name: str, phone: str) -> None:
    session["erp_customer_id"] = None
    session["buyer_identity_id"] = None
    session["buyer_name"] = full_name
    session["buyer_phone"] = phone
    session["buyer_company_name"] = None
    session["buyer_company_registry_number"] = None
    session["buyer_company_candidates"] = []
    session["buyer_company_pending"] = True
    session["buyer_review_required"] = False
    session["buyer_review_case_id"] = None
    session["buyer_identity_status"] = "unresolved_contact"
    session["buyer_recognized_via"] = "manual_intro_unmatched"
    session["recent_sales_orders"] = []
    session["recent_sales_invoices"] = []
    session["returning_customer_announced"] = False


def _apply_buyer_context(session: dict[str, Any], buyer_result: dict[str, Any]) -> None:
    if not isinstance(buyer_result, dict):
        return
    _clear_pending_buyer_state(session)
    if buyer_result.get("erp_customer_id"):
        session["erp_customer_id"] = buyer_result.get("erp_customer_id")
    contact_name = select_contact_display_name(buyer_result.get("contact_name"), session.get("buyer_name"))
    if contact_name:
        session["buyer_name"] = contact_name
    if buyer_result.get("erp_customer_name"):
        session["buyer_company_name"] = buyer_result.get("erp_customer_name")
    if buyer_result.get("company_number"):
        session["buyer_company_registry_number"] = buyer_result.get("company_number")
    if buyer_result.get("buyer_identity_id"):
        session["buyer_identity_id"] = buyer_result.get("buyer_identity_id")
    if buyer_result.get("phone"):
        session["buyer_phone"] = buyer_result.get("phone")
    preferred_language = _normalize_buyer_language_code(buyer_result.get("preferred_language"))
    if preferred_language:
        session["buyer_preferred_language"] = preferred_language
        if not str(session.get("lang") or "").strip():
            session["lang"] = preferred_language
    if buyer_result.get("recognized_via"):
        session["buyer_recognized_via"] = buyer_result.get("recognized_via")
    if buyer_result.get("recognition_status"):
        session["buyer_identity_status"] = buyer_result.get("recognition_status")
    session["buyer_review_required"] = bool(buyer_result.get("needs_review"))
    session["recent_sales_orders"] = buyer_result.get("recent_sales_orders") or []
    session["recent_sales_invoices"] = buyer_result.get("recent_sales_invoices") or []
    session["returning_customer_announced"] = False


async def _maybe_update_buyer_preferred_language(
    *,
    lc: Any,
    company_code: str,
    session: dict[str, Any],
    current_lang: str | None,
    lang_to_lock: str | None,
) -> None:
    buyer_identity_id = str(session.get("buyer_identity_id") or "").strip()
    if not buyer_identity_id:
        return
    preferred_language = _normalize_buyer_language_code(lang_to_lock or current_lang)
    if not preferred_language:
        return
    current_preferred = _normalize_buyer_language_code(session.get("buyer_preferred_language"))
    if current_preferred == preferred_language:
        return
    try:
        response = await lc.update_buyer_preferred_language(
            company_code,
            buyer_identity_id,
            preferred_language=preferred_language,
            source="message_language_signal",
        )
    except Exception as exc:
        logger.warning("Failed to persist buyer preferred language for %s: %s", buyer_identity_id, exc)
        return
    session["buyer_preferred_language"] = _normalize_buyer_language_code(
        response.get("preferred_language") if isinstance(response, dict) else preferred_language
    ) or preferred_language


def _is_returning_customer(session: dict[str, Any]) -> bool:
    return bool(session.get("recent_sales_orders") or session.get("recent_sales_invoices"))


def _returning_customer_prefix(lang: str, buyer_name: str | None = None) -> str:
    return _returning_customer_prefix_text(lang)

def _maybe_prefix_returning_customer(session: dict[str, Any], lang: str, reply: str) -> str:
    if not reply:
        return reply
    if _is_returning_customer(session) and not session.get("returning_customer_announced"):
        prefix = _returning_customer_prefix(lang, session.get("buyer_name"))
        session["returning_customer_announced"] = True
        return f"{prefix} {reply}".strip()
    return reply


async def _finalize_intake_reply(
    *,
    lc: Any,
    company_code: str,
    channel: str,
    channel_uid: str,
    session: dict[str, Any],
    user_text: str,
    reply: str,
    result: dict[str, Any],
    message_type: str,
    payload: dict[str, Any] | None = None,
    handoff_reason: str | None = None,
    handoff_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session["messages"].append({"role": "user", "content": user_text})
    session["messages"].append({"role": "assistant", "content": reply})
    session["messages"] = session.get("messages", [])[-40:]
    await save_session(channel, channel_uid, session)
    result["text"] = reply
    if handoff_reason:
        result["handoff_required"] = True
        result["handoff_reason"] = handoff_reason
        _log_event(
            "conversation_outcome",
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            outcome="handoff",
            stage=session.get("stage"),
            handoff_reason=handoff_reason,
            reply_preview=_preview_text(reply),
        )
        handoff_response = await _emit_handoff(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            reason=handoff_reason,
            payload=handoff_payload or {},
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="handoff_triggered",
            payload={
                "reason": handoff_reason,
                "stage": session.get("stage"),
                "delivery_status": (
                    handoff_response.get("delivery", {}).get("status") if isinstance(handoff_response, dict) else None
                ),
            },
        )
    else:
        _log_event(
            "conversation_outcome",
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            outcome=message_type,
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
                "outcome": message_type,
                "stage": session.get("stage"),
                "reply_preview": _preview_text(reply),
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
        payload={"lead_profile": active_lead_profile(session)},
    )
    await _emit_transcript_message(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        session=session,
        role="assistant",
        content=reply,
        message_type=message_type,
        payload=payload or {},
    )
    return result


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
    contexts: dict[str, Any] | None = None,
    active_context_id: str | None = None,
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
        contexts=contexts,
        active_context_id=active_context_id,
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
            compact = _preview_text(content, limit=_MAX_OPENAI_MESSAGE_CHARS)
            if compact:
                items.append({"role": role, "content": compact})
            continue
        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") in {"text", "output_text"}:
                    text = block.get("text")
                    if isinstance(text, str) and text.strip():
                        text_parts.append(_preview_text(text, limit=_MAX_OPENAI_MESSAGE_CHARS))
            if text_parts:
                items.append({"role": role, "content": _preview_text("\n".join(text_parts), limit=_MAX_OPENAI_MESSAGE_CHARS)})
    return items

def _estimate_input_items_size(input_items: list[dict[str, Any]]) -> int:
    return len(json.dumps(input_items, ensure_ascii=False, default=str))


def _group_input_items(input_items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    index = 0
    while index < len(input_items):
        item = input_items[index]
        if not isinstance(item, dict):
            groups.append([item])
            index += 1
            continue
        if item.get("type") == "function_call":
            group = [item]
            call_id = str(item.get("call_id") or "").strip()
            next_index = index + 1
            if next_index < len(input_items):
                next_item = input_items[next_index]
                if (
                    isinstance(next_item, dict)
                    and next_item.get("type") == "function_call_output"
                    and call_id
                    and str(next_item.get("call_id") or "").strip() == call_id
                ):
                    group.append(next_item)
                    next_index += 1
            groups.append(group)
            index = next_index
            continue
        groups.append([item])
        index += 1
    return groups


def _flatten_input_groups(groups: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for group in groups:
        flattened.extend(group)
    return flattened


def _trim_input_items(input_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = _group_input_items(list(input_items))
    removed = 0
    if len(groups) > _MAX_OPENAI_HISTORY_ITEMS:
        removed += len(groups) - _MAX_OPENAI_HISTORY_ITEMS
        groups = groups[-_MAX_OPENAI_HISTORY_ITEMS:]
    trimmed = _flatten_input_groups(groups)
    while len(trimmed) > _MAX_OPENAI_INPUT_ITEMS or _estimate_input_items_size(trimmed) > _MAX_OPENAI_INPUT_BYTES:
        if len(groups) <= 1:
            break
        removed += len(groups[0])
        groups.pop(0)
        trimmed = _flatten_input_groups(groups)
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
    lead_profile = normalize_lead_profile(active_lead_profile(session))
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

    previous_tool_lead_profile = normalize_lead_profile(active_lead_profile(session))
    set_active_lead_profile(session, update_lead_profile_from_tool(
        current_profile=previous_tool_lead_profile,
        tool_name="get_product_catalog",
        inputs=inputs,
        tool_result=parsed_result if isinstance(parsed_result, dict) else {},
        stage=session.get("stage"),
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=session.get("last_sales_order_name"),
    ))
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
            "lead_profile": active_lead_profile(session),
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
    lead_profile = normalize_lead_profile(active_lead_profile(session))
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

    previous_tool_lead_profile = normalize_lead_profile(active_lead_profile(session))
    set_active_lead_profile(session, update_lead_profile_from_tool(
        current_profile=previous_tool_lead_profile,
        tool_name="get_item_availability",
        inputs=inputs,
        tool_result=parsed_result if isinstance(parsed_result, dict) else {},
        stage=session.get("stage"),
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=session.get("last_sales_order_name"),
    ))
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
            "lead_profile": active_lead_profile(session),
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


async def _maybe_prefetch_order_status_context(
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
    active_order_name = str(session.get("last_sales_order_name") or "").strip()
    if not active_order_name:
        return None
    lead_profile = normalize_lead_profile(active_lead_profile(session))
    stage = str(session.get("stage") or "").strip()
    intent = str(session.get("last_intent") or "").strip()
    correction_status = str(lead_profile.get("order_correction_status") or "").strip()
    should_prefetch = bool(
        stage in {"invoice", "service", "closed"}
        and (
            intent in {"service_request", "add_to_order", "order_detail"}
            or correction_status == "requested"
            or lead_profile.get("active_order_can_modify") is None
        )
    )
    if not should_prefetch:
        return None

    inputs = {"sales_order_name": active_order_name}
    result_str = await execute_tool(
        name="get_sales_order_status",
        inputs=inputs,
        company_code=company_code,
        erp_customer_id=session.get("erp_customer_id"),
        active_sales_order_name=active_order_name,
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
        parsed_result = {"raw_result": result_str, "error": "invalid_order_status_prefetch_result"}

    previous_tool_lead_profile = normalize_lead_profile(active_lead_profile(session))
    set_active_lead_profile(session, update_lead_profile_from_tool(
        current_profile=previous_tool_lead_profile,
        tool_name="get_sales_order_status",
        inputs=inputs,
        tool_result=parsed_result if isinstance(parsed_result, dict) else {},
        stage=session.get("stage"),
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=active_order_name,
    ))
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

    summary = _tool_result_summary("get_sales_order_status", parsed_result if isinstance(parsed_result, dict) else {})
    summary["source"] = "runtime_prefetch"
    _log_event(
        "order_status_prefetch_finished",
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
        event_type="order_status_prefetch_finished",
        payload={
            "tool_name": "get_sales_order_status",
            "stage": session.get("stage"),
            "lead_profile": active_lead_profile(session),
            **summary,
        },
    )
    append_lead_timeline_event(
        session,
        event_type="order_status_prefetch_finished",
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
        tool_name="get_sales_order_status",
        content=result_str,
        payload=summary,
    )

    result = parsed_result if isinstance(parsed_result, dict) else {}
    if result.get("error"):
        return (
            f"Runtime order status lookup for {active_order_name} returned an error. "
            "Do not claim the order is editable or locked without a successful status tool result."
        )
    if result.get("can_modify") is True:
        return (
            f"Runtime order status lookup confirms that sales order {active_order_name} is in state "
            f"{result.get('order_state') or result.get('status') or 'unknown'} and can be modified. "
            "When the customer asks to add items to the current order, use update_sales_order instead of saying it is locked."
        )
    if result.get("can_modify") is False:
        return (
            f"Runtime order status lookup confirms that sales order {active_order_name} cannot be modified in its current state. "
            "Do not attempt to update it. Offer a new order or the current order PDF instead."
        )
    return None


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


def _build_confirmation_fallback_call(*, session: dict[str, Any], user_text: str) -> dict[str, Any] | None:
    if not has_explicit_confirmation(user_text):
        return None
    lead_profile = normalize_lead_profile(active_lead_profile(session))
    separate_order_requested = bool(lead_profile.get("separate_order_requested"))
    next_action = str(lead_profile.get("next_action") or "").strip()
    stage = str(session.get("stage") or "").strip()
    if next_action != "confirm_order" and stage not in {"order_build", "confirm"} and not separate_order_requested:
        return None
    if lead_profile.get("requested_items_need_uom_confirmation"):
        return None
    if str(lead_profile.get("product_resolution_status") or "").strip() != "specific":
        return None
    if str(lead_profile.get("catalog_lookup_status") or "").strip() not in {"found", "unknown"}:
        return None

    item_code = str(lead_profile.get("catalog_item_code") or "").strip()
    uom = str(lead_profile.get("uom") or "").strip()
    try:
        qty = float(lead_profile.get("quantity") or 0)
    except (TypeError, ValueError):
        qty = 0.0
    if not item_code or qty <= 0 or not uom:
        return None

    items = [{"item_code": item_code, "qty": qty, "uom": uom}]
    active_order_name = str(session.get("last_sales_order_name") or "").strip()
    correction_requested = str(lead_profile.get("order_correction_status") or "").strip() == "requested"
    last_intent = str(session.get("last_intent") or "").strip()
    if active_order_name and not separate_order_requested and (last_intent == "add_to_order" or correction_requested or stage in {"invoice", "service", "closed"}):
        return None
    inputs = {"items": items}
    tool_name = "create_sales_order"
    return {
        "type": "function_call",
        "name": tool_name,
        "arguments": json.dumps(inputs, ensure_ascii=False, default=str),
        "call_id": f"fallback_{uuid.uuid4().hex}",
    }


def _tool_success_fallback_reply(tool_name: str, tool_result: dict[str, Any], lang: str) -> str | None:
    if not isinstance(tool_result, dict) or tool_result.get("error"):
        return None
    order_name = str(tool_result.get("name") or "").strip()
    if tool_name == "create_sales_order" and order_name:
        templates = {
            "ru": f"Заказ {order_name} создан. При необходимости я могу отправить PDF заказа или помочь со счетом.",
            "he": f"ההזמנה {order_name} נוצרה. אם תרצה, אוכל לשלוח את ה-PDF של ההזמנה או לעזור עם החשבונית.",
            "ar": f"تم إنشاء الطلب {order_name}. إذا أردت، يمكنني إرسال ملف PDF للطلب أو المساعدة في الفاتورة.",
        }
        return templates.get(lang, f"Your order has been created as {order_name}. If you want, I can send the order PDF or create an invoice.")
    if tool_name == "update_sales_order" and order_name:
        templates = {
            "ru": f"Я обновил заказ {order_name}. При необходимости могу отправить обновленный PDF.",
            "he": f"עדכנתי את ההזמנה {order_name}. אם תרצה, אוכל לשלוח את ה-PDF המעודכן.",
            "ar": f"قمت بتحديث أمر البيع {order_name}. إذا أردت، يمكنني إرسال ملف PDF المحدث.",
        }
        return templates.get(lang, f"I updated sales order {order_name}. If you want, I can send the updated order PDF.")
    if tool_name == "create_invoice" and order_name:
        templates = {
            "ru": f"Я создал счет {order_name}.",
            "he": f"יצרתי את החשבונית {order_name}.",
            "ar": f"أنشأت الفاتورة {order_name}.",
        }
        return templates.get(lang, f"I created invoice {order_name}.")
    return None


def _is_terminal_write_tool_success(tool_name: str, tool_result: dict[str, Any] | None) -> bool:
    if tool_name not in {"create_sales_order", "update_sales_order", "create_invoice"}:
        return False
    if not isinstance(tool_result, dict) or tool_result.get("error"):
        return False
    return bool(str(tool_result.get("name") or "").strip())


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
        return max(0.4, min(0.99, float(sales_policy.get("llm_state_updater_min_confidence", 0.55) or 0.55)))
    except (TypeError, ValueError):
        return 0.55


def _signal_classifier_enabled(tenant: dict[str, Any]) -> bool:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    if "llm_signal_classifier_enabled" in sales_policy:
        return bool(sales_policy.get("llm_signal_classifier_enabled"))
    return bool(sales_policy.get("llm_state_updater_enabled", False))


def _signal_classifier_min_confidence(tenant: dict[str, Any]) -> float:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    sales_policy = ai_policy.get("sales_policy") if isinstance(ai_policy.get("sales_policy"), dict) else {}
    try:
        return max(0.4, min(0.99, float(sales_policy.get("llm_signal_classifier_min_confidence", 0.58) or 0.58)))
    except (TypeError, ValueError):
        return 0.58


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


def _intent_from_signal_classifier(*, signal_type: str, current_intent: str) -> str:
    if signal_type == "small_talk":
        return "small_talk"
    if signal_type in {"service_request", "delivery_question", "availability_question"}:
        return "service_request"
    if signal_type == "handoff_request":
        return "human_handoff"
    if signal_type == "confirmation":
        return "confirm_order"
    if signal_type in {"low_signal", "frustration"}:
        return "low_signal"
    if signal_type in {"topic_shift", "deal_progress"}:
        return current_intent
    return current_intent


async def _classify_state_with_llm(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    user_text: str,
    session: dict[str, Any],
    tenant: dict[str, Any],
) -> dict[str, Any]:
    lead_profile = normalize_lead_profile(active_lead_profile(session))
    recent_messages: list[dict[str, Any]] = []
    for row in list(session.get("messages") or [])[-6:]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role") or "").strip()
        content = _preview_text(str(row.get("content") or ""))
        if role and content:
            recent_messages.append({"role": role, "content": content})
    payload = {
        "model": model,
        "instructions": (
            "Classify the customer's latest message for sales-runtime state update. "
            "Return only compact JSON with keys: intent, signal_type, signal_emotion, signal_preserves_deal, behavior_class, confidence, next_action, lead_patch, reason. "
            "intent must be one of: low_signal, small_talk, find_product, browse_catalog, order_detail, confirm_order, add_to_order, service_request, human_handoff. "
            "signal_type must be one of: deal_progress, small_talk, price_objection, discount_request, analogs_request, comparison_request, delivery_question, availability_question, topic_shift, frustration, confirmation, service_request, stalling, resume_previous_context, low_signal, handoff_request. "
            "signal_emotion must be one of: neutral, positive, impatient, skeptical. "
            "signal_preserves_deal must be true when the current commercial thread should stay active after handling the message, and false when the customer is explicitly shifting away from it. "
            "behavior_class must be one of: direct_buyer, explorer, unclear_request, price_sensitive, frustrated, service_request, returning_customer, silent_or_low_signal. "
            "next_action must be one of: handoff_manager, fulfill_service_request, ask_need, show_matching_options, select_specific_item, ask_quantity, ask_unit, ask_delivery_timing, ask_contact, quote_or_clarify_price, confirm_order, recommend_next_step. "
            "lead_patch may include only these keys when clearly supported by the message and context: product_interest, quantity, uom, urgency, delivery_need, price_sensitivity, decision_status. "
            "quantity must be a numeric value or null, never a string with unit words or approximations. "
            "Use next_action to express the single best next sales step after this message. "
            "Use small_talk for greetings, social check-ins, or politeness in any language when there is no product, service, or order request yet. "
            "Use price_objection for messages such as 'expensive', 'too much', or equivalent objections in any language when the deal should continue after the response. "
            "Use discount_request when the customer explicitly asks for a discount or better commercial terms. "
            "Use analogs_request when the customer asks for a cheaper or alternative item while keeping the same need. "
            "Use comparison_request when the customer asks to compare options, models, or alternatives. "
            "Use delivery_question or availability_question when the customer asks operational questions about delivery or stock for the current item or order. "
            "Use topic_shift when the customer is moving from the current deal or order-edit thread to another product or a separate order. "
            "Use resume_previous_context when the customer is clearly returning to an earlier open thread in the same conversation. "
            "Prefer show_matching_options or select_specific_item when the customer named only a broad product and asks what exists. "
            "Do not ask again for quantity or UOM when they are already known in the lead profile unless the customer changes them. "
            "If there is an active order and the customer explicitly asks for a new or separate order, do not treat that as add_to_order. "
            "Do not invent values. Use any customer language. If unsure, keep lead_patch empty, next_action empty, and lower confidence."
        ),
        "input": [
            {
                "role": "system",
                "content": json.dumps(
                    {
                        "current_stage": session.get("stage"),
                        "current_intent": session.get("last_intent"),
                        "current_signal_type": session.get("signal_type"),
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
                            "catalog_lookup_query": lead_profile.get("catalog_lookup_query"),
                            "catalog_lookup_status": lead_profile.get("catalog_lookup_status"),
                            "catalog_lookup_match_count": lead_profile.get("catalog_lookup_match_count"),
                            "catalog_item_code": lead_profile.get("catalog_item_code"),
                            "catalog_item_name": lead_profile.get("catalog_item_name"),
                        },
                        "recent_messages": recent_messages,
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


async def _classify_signal_with_llm(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    user_text: str,
    session: dict[str, Any],
) -> dict[str, Any]:
    lead_profile = normalize_lead_profile(active_lead_profile(session))
    recent_messages: list[dict[str, Any]] = []
    for row in list(session.get("messages") or [])[-6:]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role") or "").strip()
        content = _preview_text(str(row.get("content") or ""))
        if role and content:
            recent_messages.append({"role": role, "content": content})
    payload = {
        "model": model,
        "instructions": (
            "Classify only the customer's latest conversational signal. "
            "Return only compact JSON with keys: signal_type, signal_emotion, signal_preserves_deal, confidence, reason. "
            "signal_type must be one of: deal_progress, small_talk, price_objection, discount_request, analogs_request, comparison_request, delivery_question, availability_question, topic_shift, frustration, confirmation, service_request, stalling, resume_previous_context, low_signal, handoff_request. "
            "signal_emotion must be one of: neutral, positive, impatient, skeptical. "
            "signal_preserves_deal must be true when the current commercial thread should remain active after handling this message, and false only when the customer is explicitly moving away from it. "
            "Use small_talk for greetings, social check-ins, and politeness in any language. "
            "Use price_objection for 'expensive', 'too much', or equivalent multilingual objections when the deal should continue. "
            "Use discount_request only when the customer is explicitly asking for a discount or better price terms. "
            "Use analogs_request or comparison_request when the customer wants alternatives or comparison, not when they are merely greeting. "
            "Use service_request, delivery_question, or availability_question when the customer asks for operational help about the current item or order. "
            "Use topic_shift when the customer is clearly switching to another product, another order, or another commercial thread. "
            "Use resume_previous_context when the customer is clearly returning to a previously open thread. "
            "Do not infer product details, quantity, UOM, or next actions. Focus only on the signal. "
            "Use any customer language. If unsure, prefer low_signal with lower confidence."
        ),
        "input": [
            {
                "role": "system",
                "content": json.dumps(
                    {
                        "current_stage": session.get("stage"),
                        "current_signal_type": session.get("signal_type"),
                        "current_behavior_class": session.get("behavior_class"),
                        "current_context_type": session.get("contexts", {}).get(session.get("active_context_id"), {}).get("context_type") if isinstance(session.get("contexts"), dict) else None,
                        "customer_identified": bool(session.get("erp_customer_id")),
                        "active_order_name": session.get("last_sales_order_name"),
                        "lead_profile": {
                            "status": lead_profile.get("status"),
                            "product_interest": lead_profile.get("product_interest"),
                            "catalog_item_name": lead_profile.get("catalog_item_name"),
                            "quantity": lead_profile.get("quantity"),
                            "uom": lead_profile.get("uom"),
                            "next_action": lead_profile.get("next_action"),
                        },
                        "recent_messages": recent_messages,
                    },
                    ensure_ascii=False,
                ),
            },
            {"role": "user", "content": user_text},
        ],
        "max_output_tokens": 180,
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
    return parse_llm_signal_classification(_extract_output_text(response.json()))


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
    lead_profile = normalize_lead_profile(active_lead_profile(session))
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
            "Reluctant but affirmative confirmations can be true. "
            "For a draft-order correction, a direct imperative that contains the requested change and concrete business details also counts as confirmed, "
            "for example: add 7 shirts, change the quantity to 10, remove backpack from the order. "
            "Questions, price requests, unclear corrections without enough detail, and negations must be false. "
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


async def _process_message_result_locked(
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
    set_active_lead_profile(session, ensure_lead_identity(
        current_profile=active_lead_profile(session),
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
    ))
    if _playbook_version(tenant):
        mutate_active_lead_profile(
            session,
            lambda profile: {**profile, "playbook_version": _playbook_version(tenant)},
        )
    set_active_lead_profile(session, update_lead_profile_source(
        current_profile=active_lead_profile(session),
        channel=channel,
        channel_context=session.get("channel_context") if isinstance(session.get("channel_context"), dict) else {},
    ))
    default_lang = tenant.get("ai_language", "ru")
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=session.get("lang"),
        user_text=user_text,
        default_lang=default_lang,
    )
    if lang_to_lock:
        session["lang"] = lang_to_lock
    previous_stalled_profile = normalize_lead_profile(active_lead_profile(session))
    set_active_lead_profile(session, mark_stalled_if_needed(
        current_profile=previous_stalled_profile,
        last_interaction_at=session.get("last_interaction_at"),
        idle_after=_lead_idle_after(tenant),
    ))
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
        current_lang = _normalize_buyer_language_code(session.get("lang")) or current_lang
        await _maybe_update_buyer_preferred_language(
            lc=lc,
            company_code=company_code,
            session=session,
            current_lang=current_lang,
            lang_to_lock=lang_to_lock,
        )
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
        if should_send_known_buyer_greeting(
            user_text=user_text,
            buyer_identified=bool(session.get("erp_customer_id")),
            stage=session.get("stage"),
            conversation_reopened=bool(session.get("conversation_reopened")),
        ):
            session["stage"] = "new"
            session["stage_confidence"] = 0.98
            session["behavior_class"] = "returning_customer"
            session["behavior_confidence"] = 0.98
            session["last_intent"] = "low_signal"
            session["last_intent_confidence"] = 0.98
            session["handoff_required"] = False
            session["handoff_reason"] = None
            session["returning_customer_announced"] = True
            reply = get_known_buyer_greeting(current_lang, session.get("buyer_name"))
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="returning_customer_greeting",
                payload={
                    "erp_customer_id": session.get("erp_customer_id"),
                    "buyer_identity_id": session.get("buyer_identity_id"),
                    "buyer_name": session.get("buyer_name"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "conversation_reopened": True,
                },
            )
    if not session.get("erp_customer_id") and session.get("buyer_company_pending"):
        selected_company_query = _select_company_candidate_query(user_text, session.get("buyer_company_candidates"))
        company_name = selected_company_query or _clean_company_candidate(user_text)
        if not company_name:
            session["stage"] = "identify"
            session["stage_confidence"] = 0.97
            reply = _buyer_company_request_message(current_lang, session.get("buyer_name"))
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="buyer_company_request",
                payload={"buyer_name": session.get("buyer_name"), "buyer_phone": session.get("buyer_phone")},
            )
        try:
            company_resolution = await lc.identify_buyer_company(
                company_code,
                channel_type=channel,
                channel_user_id=channel_uid,
                full_name=str(session.get("buyer_name") or "").strip(),
                phone=session.get("buyer_phone"),
                company_query=company_name,
            )
        except Exception as exc:
            logger.exception("Buyer company identification failed for %s/%s: %s", channel, channel_uid, exc)
            session["stage"] = "identify"
            session["stage_confidence"] = 0.98
            reply = _buyer_company_lookup_error_message(current_lang)
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="buyer_company_request",
                payload={
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "company_query": company_name,
                    "lookup_error": type(exc).__name__,
                },
            )
        match_status = str(company_resolution.get("match_status") or "none").strip() or "none"
        if match_status == "ambiguous":
            candidates = company_resolution.get("candidates") or []
            option_lines: list[str] = []
            session["buyer_company_candidates"] = candidates if isinstance(candidates, list) else []
            for index, candidate in enumerate(session["buyer_company_candidates"][:5], start=1):
                if not isinstance(candidate, dict):
                    continue
                number = str(candidate.get("company_number") or "").strip()
                name = str(candidate.get("company_name") or "").strip()
                if number and name:
                    option_lines.append(f"{index}. {name} ({number})")
            session["stage"] = "identify"
            session["stage_confidence"] = 0.98
            reply = _buyer_company_ambiguous_message(current_lang, option_lines)
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="buyer_company_request",
                payload={
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "company_query": company_name,
                    "company_candidates": session.get("buyer_company_candidates"),
                },
            )

        if match_status == "none":
            session["stage"] = "identify"
            session["stage_confidence"] = 0.98
            reply = _buyer_company_retry_message(current_lang)
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="buyer_company_request",
                payload={
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "company_query": company_name,
                },
            )

        candidate = company_resolution.get("candidate") if isinstance(company_resolution.get("candidate"), dict) else {}
        official_company_name = str(candidate.get("company_name") or "").strip() or company_name
        company_number = str(candidate.get("company_number") or "").strip() or None
        session["buyer_company_name"] = official_company_name
        session["buyer_company_registry_number"] = company_number
        session["buyer_company_candidates"] = []
        session["buyer_company_pending"] = False
        session["buyer_identity_status"] = company_resolution.get("recognition_status")
        if company_resolution.get("found") and company_resolution.get("erp_customer_id"):
            _apply_buyer_context(
                session,
                {
                    "erp_customer_id": company_resolution.get("erp_customer_id"),
                    "erp_customer_name": company_resolution.get("erp_customer_name") or official_company_name,
                    "buyer_identity_id": company_resolution.get("buyer_identity_id"),
                    "contact_name": session.get("buyer_name"),
                    "phone": session.get("buyer_phone"),
                    "recognized_via": "company_registry",
                    "recognition_status": company_resolution.get("recognition_status"),
                    "match_confidence": company_resolution.get("match_confidence"),
                    "needs_review": company_resolution.get("needs_review"),
                    "company_number": company_number,
                    "recent_sales_orders": company_resolution.get("recent_sales_orders") or [],
                    "recent_sales_invoices": company_resolution.get("recent_sales_invoices") or [],
                },
            )
            current_lang = _normalize_buyer_language_code(session.get("lang")) or current_lang
            await _maybe_update_buyer_preferred_language(
                lc=lc,
                company_code=company_code,
                session=session,
                current_lang=current_lang,
                lang_to_lock=lang_to_lock,
            )
            await _emit_control_plane_event(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                event_type="buyer_resolved",
                payload={
                    "erp_customer_id": session.get("erp_customer_id"),
                    "buyer_identity_id": session.get("buyer_identity_id"),
                    "recognized_via": "company_registry",
                    "recognition_status": session.get("buyer_identity_status"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "buyer_company_registry_number": session.get("buyer_company_registry_number"),
                },
            )
            session["stage"] = "identify"
            session["stage_confidence"] = 0.98
            reply = get_known_buyer_greeting(current_lang, session.get("buyer_name"))
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="buyer_resolved",
                payload={
                    "erp_customer_id": session.get("erp_customer_id"),
                    "buyer_identity_id": session.get("buyer_identity_id"),
                    "buyer_name": session.get("buyer_name"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "buyer_company_registry_number": session.get("buyer_company_registry_number"),
                    "recognition_status": session.get("buyer_identity_status"),
                },
            )
        else:
            session["buyer_review_required"] = True
            session["buyer_review_case_id"] = company_resolution.get("review_case_id")
            session["stage"] = "handoff"
            session["stage_confidence"] = 0.99
            session["handoff_required"] = True
            session["handoff_reason"] = "buyer_identity_review_required"
            await _emit_control_plane_event(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                event_type="buyer_company_captured",
                payload={
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "buyer_company_registry_number": session.get("buyer_company_registry_number"),
                    "buyer_review_case_id": session.get("buyer_review_case_id"),
                },
            )
            reply = _buyer_identity_review_message(current_lang)
            return await _finalize_intake_reply(
                lc=lc,
                company_code=company_code,
                channel=channel,
                channel_uid=channel_uid,
                session=session,
                user_text=user_text,
                reply=reply,
                result=result,
                message_type="handoff",
                payload={
                    "reason": "buyer_identity_review_required",
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "buyer_company_registry_number": session.get("buyer_company_registry_number"),
                    "buyer_review_case_id": session.get("buyer_review_case_id"),
                    "handoff_summary": build_handoff_summary(session, reason="buyer_identity_review_required"),
                },
                handoff_reason="buyer_identity_review_required",
                handoff_payload={
                    "stage": session.get("stage"),
                    "reply_preview": _preview_text(reply),
                    "erp_customer_id": session.get("erp_customer_id"),
                    "buyer_name": session.get("buyer_name"),
                    "buyer_phone": session.get("buyer_phone"),
                    "buyer_company_name": session.get("buyer_company_name"),
                    "buyer_company_registry_number": session.get("buyer_company_registry_number"),
                    "buyer_review_case_id": session.get("buyer_review_case_id"),
                    "handoff_summary": build_handoff_summary(session, reason="buyer_identity_review_required"),
                    "lead_profile": active_lead_profile(session),
                    **_handoff_target(tenant),
                },
            )

    if needs_intro and not session.get("erp_customer_id"):
        intro_name, intro_phone = _extract_intro_contact(user_text)
        if intro_name and intro_phone:
            buyer_result = await resolve_buyer_from_intro(
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
                current_lang = _normalize_buyer_language_code(session.get("lang")) or current_lang
                await _maybe_update_buyer_preferred_language(
                    lc=lc,
                    company_code=company_code,
                    session=session,
                    current_lang=current_lang,
                    lang_to_lock=lang_to_lock,
                )
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
            else:
                _set_pending_buyer_contact(session, full_name=intro_name, phone=intro_phone)
                session["stage"] = "identify"
                session["stage_confidence"] = 0.97
                await _emit_control_plane_event(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    event_type="buyer_contact_captured_unmatched",
                    payload={"buyer_name": intro_name, "buyer_phone": intro_phone},
                )
                reply = _buyer_company_request_message(current_lang, intro_name)
                return await _finalize_intake_reply(
                    lc=lc,
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    session=session,
                    user_text=user_text,
                    reply=reply,
                    result=result,
                    message_type="buyer_company_request",
                    payload={"buyer_name": intro_name, "buyer_phone": intro_phone},
                )
    active_order_name = active_related_order_id(session) or session.get("last_sales_order_name")
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None
    previous_lead_profile = normalize_lead_profile(active_lead_profile(session))
    llm_state_result: dict[str, Any] | None = None
    llm_signal_result: dict[str, Any] | None = None
    if _signal_classifier_enabled(tenant):
        try:
            async with httpx.AsyncClient(timeout=20.0) as signal_client:
                llm_signal_result = await _classify_signal_with_llm(
                    client=signal_client,
                    api_key=settings.openai_api_key,
                    model=settings.openai_model,
                    user_text=user_text,
                    session=session,
                )
        except Exception as exc:
            logger.warning("LLM signal classifier failed: %s", exc)
            llm_signal_result = {"valid": False, "reason": str(exc), "source": "error"}
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
    llm_is_valid = isinstance(llm_state_result, dict) and llm_state_result.get("valid")
    llm_confidence = float(llm_state_result.get("confidence") or 0) if llm_is_valid else 0.0
    llm_intent = str(llm_state_result.get("intent") or "") if llm_is_valid else ""
    llm_signal_type = str(llm_state_result.get("signal_type") or "") if llm_is_valid else ""
    llm_signal_is_valid = isinstance(llm_signal_result, dict) and llm_signal_result.get("valid")
    llm_signal_confidence = float(llm_signal_result.get("confidence") or 0) if llm_signal_is_valid else 0.0
    use_llm_signal = llm_signal_is_valid and llm_signal_confidence >= _signal_classifier_min_confidence(tenant)
    use_llm_state = (
        llm_is_valid
        and (
            llm_confidence >= _state_updater_min_confidence(tenant)
            or (llm_intent == "small_talk" and llm_confidence >= 0.45)
            or (llm_signal_type in {"small_talk", "price_objection", "topic_shift"} and llm_confidence >= 0.45)
        )
    )
    if use_llm_state:
        seeded_profile = apply_llm_lead_patch(
            current_profile=previous_lead_profile,
            patch=llm_state_result.get("lead_patch") if isinstance(llm_state_result.get("lead_patch"), dict) else {},
            lead_config=_lead_management_config(tenant),
            intent=llm_state_result.get("intent"),
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
    resolved_llm_signal_type = str(llm_signal_result.get("signal_type") or "") if use_llm_signal else ""
    if use_llm_signal and resolved_llm_signal_type:
        mapped_intent = _intent_from_signal_classifier(signal_type=resolved_llm_signal_type, current_intent=intent or fallback_intent)
        if mapped_intent and mapped_intent != intent:
            intent = mapped_intent
            intent_confidence = max(intent_confidence, llm_signal_confidence)
    if fallback_intent == "small_talk" and intent != "small_talk":
        intent = "small_talk"
        intent_confidence = fallback_intent_confidence
        if isinstance(llm_state_result, dict):
            llm_state_result["lead_patch"] = {}
            llm_state_result["signal_type"] = "small_talk"
            llm_state_result["signal_emotion"] = "positive"
            llm_state_result["signal_preserves_deal"] = True
        if isinstance(llm_signal_result, dict):
            llm_signal_result["signal_type"] = "small_talk"
            llm_signal_result["signal_emotion"] = "positive"
            llm_signal_result["signal_preserves_deal"] = True
    if not intent:
        intent = fallback_intent
        intent_confidence = fallback_intent_confidence
    elif (
        fallback_intent
        and fallback_intent != intent
        and fallback_intent_confidence >= 0.9
        and intent_confidence < 0.8
        and fallback_intent in {"find_product", "browse_catalog", "confirm_order", "add_to_order", "service_request", "human_handoff"}
    ):
        intent = fallback_intent
        intent_confidence = fallback_intent_confidence
    if behavior_class == "silent_or_low_signal" and intent == "find_product":
        intent = "low_signal"
        intent_confidence = max(intent_confidence, behavior_confidence)
    set_active_lead_profile(session, update_lead_profile_from_message(
        current_profile=seeded_profile,
        user_text=user_text,
        stage=session.get("stage"),
        behavior_class=behavior_class,
        intent=intent,
        customer_identified=bool(session.get("erp_customer_id")),
        active_order_name=active_order_name,
        lead_config=_lead_management_config(tenant),
        llm_state_update=llm_state_result if use_llm_state else None,
    ))
    session.update(
        derive_conversation_state(
            session=session,
            user_text=user_text,
            channel=channel,
            needs_intro=needs_intro,
            customer_identified=bool(session.get("erp_customer_id")),
            active_order_name=active_order_name,
            ai_policy=ai_policy,
            lead_profile=active_lead_profile(session),
            previous_lead_profile=previous_lead_profile,
            behavior_class=behavior_class,
            behavior_confidence=behavior_confidence,
            intent=intent,
            intent_confidence=intent_confidence,
            signal_type=(llm_signal_result.get("signal_type") if use_llm_signal else llm_state_result.get("signal_type") if use_llm_state else None),
            signal_confidence=(llm_signal_confidence if use_llm_signal and llm_signal_result.get("signal_type") else llm_confidence if use_llm_state and llm_state_result.get("signal_type") else None),
            signal_preserves_deal=(llm_signal_result.get("signal_preserves_deal") if use_llm_signal else llm_state_result.get("signal_preserves_deal") if use_llm_state else None),
            signal_emotion=(llm_signal_result.get("signal_emotion") if use_llm_signal else llm_state_result.get("signal_emotion") if use_llm_state else None),
        )
    )
    reconcile_contexts_after_state_update(
        session,
        previous_lead_profile=previous_lead_profile,
        active_order_name=active_order_name,
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
        signal_type=session.get("signal_type"),
        signal_confidence=session.get("signal_confidence"),
        signal_emotion=session.get("signal_emotion"),
        handoff_required=session.get("handoff_required"),
        handoff_reason=session.get("handoff_reason"),
        lead_profile=active_lead_profile(session),
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
            "signal_type": session.get("signal_type"),
            "signal_confidence": session.get("signal_confidence"),
            "signal_emotion": session.get("signal_emotion"),
            "handoff_required": session.get("handoff_required"),
            "handoff_reason": session.get("handoff_reason"),
            "lead_profile": active_lead_profile(session),
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
        payload={"lang": current_lang, "lead_profile": active_lead_profile(session)},
    )

    if normalize_lead_profile(active_lead_profile(session)).get("sales_owner_status") == "accepted":
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
            payload={"lead_profile": active_lead_profile(session)},
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
            payload={"reason": "sales_owner_accepted", "lead_profile": active_lead_profile(session)},
        )
        result["handoff_required"] = True
        result["handoff_reason"] = "sales_owner_accepted"
        result["text"] = reply
        return result

    if str(session.get("signal_type") or "").strip() == "small_talk":
        reply = _small_talk_reply(current_lang)
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": reply})
        await save_session(channel, channel_uid, session)
        await _emit_transcript_message(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            session=session,
            role="assistant",
            content=reply,
            message_type="small_talk",
            payload={
                "lead_profile": active_lead_profile(session),
                "signal_type": session.get("signal_type"),
                "signal_emotion": session.get("signal_emotion"),
            },
        )
        result["text"] = reply
        return result

    normalized_runtime_profile = active_lead_profile(session)
    runtime_signal = str(active_signal_state(session).get("type") or session.get("signal_type") or session.get("last_intent") or "").strip()
    if should_block_for_intro_before_assistance(
        needs_intro=needs_intro,
        customer_identified=bool(session.get("erp_customer_id")),
        intent=runtime_signal,
        lead_profile=normalized_runtime_profile,
    ) or should_request_intro_before_next_step(
        needs_intro=needs_intro,
        customer_identified=bool(session.get("erp_customer_id")),
        lead_profile=normalized_runtime_profile,
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
                    "lead_profile": active_lead_profile(session),
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
                "lead_profile": active_lead_profile(session),
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
    prefetched_order_status_context = await _maybe_prefetch_order_status_context(
        lc=lc,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        current_lang=current_lang,
        user_text=user_text,
        tenant=tenant,
        session=session,
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
    synthesized_confirmation_tool = False
    last_successful_tool_name: str | None = None
    last_successful_tool_result: dict[str, Any] | None = None
    terminal_tool_completed = False

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
                last_sales_order_name=active_order_name,
                recent_sales_orders=session.get("recent_sales_orders"),
                recent_sales_invoices=session.get("recent_sales_invoices"),
                lead_profile=active_lead_profile(session),
                contexts=session.get("contexts") if isinstance(session.get("contexts"), dict) else None,
                active_context_id=session.get("active_context_id"),
                handoff_required=bool(session.get("handoff_required")),
                handoff_reason=session.get("handoff_reason"),
            )
            if prefetched_catalog_context:
                system_prompt = system_prompt + "\n\n" + prefetched_catalog_context
            if prefetched_order_status_context:
                system_prompt = system_prompt + "\n\n" + prefetched_order_status_context
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
                fallback_call = _build_confirmation_fallback_call(session=session, user_text=user_text)
                if not fallback_call:
                    break
                synthesized_confirmation_tool = True
                final_reply = ""
                function_calls = [fallback_call]
                _log_event(
                    "tool_call_synthesized",
                    company_code=company_code,
                    channel=channel,
                    channel_uid=channel_uid,
                    tool_name=fallback_call.get("name"),
                    reason="explicit_confirmation_without_model_tool_call",
                    lead_profile=active_lead_profile(session),
                )

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
                        "lead_profile": active_lead_profile(session),
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
                if tool_name not in _KNOWN_TOOL_NAMES:
                    parsed_result = {"error": f"Unknown tool: {tool_name}", "error_code": "unknown_tool_name"}
                else:
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
                            confirmation_confidence = float(confirmation_result.get("confidence") or 0)
                            confirmation_confirmed = bool(confirmation_result.get("confirmed"))
                            explicit_confirmation = has_explicit_confirmation(user_text)
                            if confirmation_confirmed and confirmation_confidence >= min_confidence:
                                confirmation_override = True
                            elif message_completes_order_details(
                                tool_name=tool_name,
                                session=session,
                                user_text=user_text,
                                tenant=tenant,
                            ):
                                confirmation_override = True
                            elif (
                                _confirmation_classifier_enabled(tenant)
                                and not confirmation_confirmed
                                and confirmation_confidence >= min_confidence
                                and not explicit_confirmation
                            ):
                                confirmation_override = False
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
                    else:
                        result_str = await execute_tool(
                            name=tool_name,
                            inputs=inputs,
                            company_code=company_code,
                            erp_customer_id=session.get("erp_customer_id"),
                            active_sales_order_name=active_order_name,
                            current_lang=current_lang,
                            user_text=user_text,
                            channel=channel,
                            channel_uid=channel_uid,
                            lc=lc,
                            ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
                            lead_profile=active_lead_profile(session),
                            confirmation_override=confirmation_override,
                        )
                        try:
                            parsed_result = json.loads(result_str)
                        except json.JSONDecodeError:
                            parsed_result = {"raw_result": result_str}
                model_result_payload = _compact_tool_result_for_model(tool_name, parsed_result if isinstance(parsed_result, dict) else {})
                model_result_str = json.dumps(model_result_payload, ensure_ascii=False, default=str)

                if tool_name == "register_buyer" and parsed_result.get("erp_customer_id"):
                    _apply_buyer_context(session, parsed_result)
                    await _maybe_update_buyer_preferred_language(
                        lc=lc,
                        company_code=company_code,
                        session=session,
                        current_lang=current_lang,
                        lang_to_lock=lang_to_lock,
                    )
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
                previous_tool_lead_profile = normalize_lead_profile(active_lead_profile(session))
                set_active_lead_profile(session, update_lead_profile_from_tool(
                    current_profile=previous_tool_lead_profile,
                    tool_name=tool_name,
                    inputs=inputs,
                    tool_result=parsed_result if isinstance(parsed_result, dict) else {},
                    stage=session.get("stage"),
                    customer_identified=bool(session.get("erp_customer_id")),
                    active_order_name=session.get("last_sales_order_name"),
                ))
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
                        "lead_profile": active_lead_profile(session),
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
                if tool_name in {"create_sales_order", "update_sales_order", "create_invoice"} and not parsed_result.get("error"):
                    last_successful_tool_name = tool_name
                    last_successful_tool_result = parsed_result if isinstance(parsed_result, dict) else None
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

                if _is_terminal_write_tool_success(tool_name, parsed_result):
                    mark_active_context_status(
                        session,
                        status="completed",
                        event_type="context_completed",
                        event_payload={"tool_name": tool_name, "order_name": parsed_result.get("name")},
                    )
                    fallback_reply = _tool_success_fallback_reply(tool_name, parsed_result, current_lang)
                    if fallback_reply:
                        final_reply = fallback_reply
                    terminal_tool_completed = True
                    break

                tool_outputs.append(
                    {
                        "type": "function_call_output",
                        "call_id": call["call_id"],
                        "output": model_result_str,
                    }
                )

            if terminal_tool_completed:
                break
            input_items = _trim_input_items(input_items + response.get("output", []) + tool_outputs)
        else:
            final_reply = "Произошла внутренняя ошибка, попробуйте позже."

    session["messages"].append({"role": "user", "content": user_text})
    if (not str(final_reply or "").strip() or str(final_reply).strip() == "...") and last_successful_tool_name and last_successful_tool_result:
        fallback_reply = _tool_success_fallback_reply(last_successful_tool_name, last_successful_tool_result, current_lang)
        if fallback_reply:
            final_reply = fallback_reply
    elif synthesized_confirmation_tool and last_successful_tool_name and last_successful_tool_result and not str(final_reply or "").strip():
        fallback_reply = _tool_success_fallback_reply(last_successful_tool_name, last_successful_tool_result, current_lang)
        if fallback_reply:
            final_reply = fallback_reply
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
        lead_profile=active_lead_profile(session),
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
            "lead_profile": active_lead_profile(session),
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
                "lead_profile": active_lead_profile(session),
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
        payload={"documents": result.get("documents", []), "lead_profile": active_lead_profile(session)},
    )
    result["text"] = final_reply
    return result


async def process_message_result(
    channel: str,
    channel_uid: str,
    user_text: str,
    tenant: dict,
    channel_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with session_processing_lock(channel, channel_uid):
        return await _process_message_result_locked(
            channel,
            channel_uid,
            user_text,
            tenant,
            channel_context=channel_context,
        )


async def process_message(
    channel: str,
    channel_uid: str,
    user_text: str,
    tenant: dict,
    channel_context: dict[str, Any] | None = None,
) -> str:
    result = await process_message_result(channel, channel_uid, user_text, tenant, channel_context=channel_context)
    return str(result.get("text") or "")
