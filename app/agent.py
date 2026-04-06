import json
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

from app.buyer_resolver import create_buyer_from_intro, resolve_buyer
from app.conversation_flow import advance_stage_after_tool, derive_conversation_state, get_handoff_message
from app.config import get_settings
from app.interaction_patterns import has_add_to_order_intent, has_explicit_confirmation
from app.language_policy import resolve_conversation_language
from app.license_client import get_license_client
from app.prompt_registry import build_runtime_system_prompt
from app.session_store import load_session, new_session, save_session
from app.tool_policy import evaluate_tool_call
from app.tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

_INTRO_MESSAGES = {
    "ru": "Здравствуйте! Я менеджер по продажам. Прежде чем продолжить, скажите, пожалуйста, ваше имя и номер телефона.",
    "en": "Hello! I'm a sales manager. Before we continue, could you please tell me your name and phone number?",
    "he": "שלום! אני מנהל מכירות. לפני שנמשיך, אפשר בבקשה את השם ומספר הטלפון שלך?",
    "ar": "مرحبًا! أنا مدير مبيعات. قبل أن نتابع، هل يمكنك من فضلك إرسال اسمك ورقم هاتفك؟",
}

_PHONE_RE = re.compile(r"(\+?\d[\d\s\-\(\)]{7,}\d)")

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
    if result.get("name"):
        summary["name"] = result.get("name")
    if result.get("erp_customer_id"):
        summary["erp_customer_id"] = result.get("erp_customer_id")
    if result.get("order_print_url"):
        summary["has_order_print_url"] = True
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
    return _INTRO_MESSAGES.get(lang, _INTRO_MESSAGES["ru"])


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


def _has_explicit_confirmation(user_text: str) -> bool:
    return has_explicit_confirmation(user_text)


def _has_add_to_order_intent(user_text: str) -> bool:
    return has_add_to_order_intent(user_text)


def _existing_order_message(lang: str, order_name: str) -> str:
    if lang == "ru":
        return f"Заказ уже создан. Текущий активный заказ: {order_name}."
    if lang == "he":
        return f"ההזמנה כבר נוצרה. ההזמנה הפעילה כרגע היא {order_name}."
    if lang == "ar":
        return f"تم إنشاء الطلب بالفعل. الطلب النشط الحالي هو {order_name}."
    return f"Your order has already been created. The current active order is {order_name}."

def _is_returning_customer(session: dict[str, Any]) -> bool:
    return bool(session.get("recent_sales_orders") or session.get("recent_sales_invoices"))


def _returning_customer_prefix(lang: str, buyer_name: str | None = None) -> str:
    display_name = str(buyer_name or "").strip()
    if lang == "ru":
        return f"Рад помочь снова, {display_name}." if display_name else "Рад помочь снова."
    if lang == "he":
        return f"שמח לעזור שוב, {display_name}." if display_name else "שמח לעזור שוב."
    if lang == "ar":
        return f"سعيد بمساعدتك مرة أخرى، {display_name}." if display_name else "سعيد بمساعدتك مرة أخرى."
    return f"Glad to help again, {display_name}." if display_name else "Glad to help again."

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


def _sanitize_customer_reply(text: str, *, buyer_identified: bool = False) -> str:
    if not text:
        return text
    return _format_customer_reply(text, buyer_identified=buyer_identified)

    cleaned = text.replace("**", "").replace("__", "")
    lines = [line.rstrip() for line in cleaned.splitlines()]
    filtered_lines: list[str] = []

    for line in lines:
        normalized = line.strip()
        if not normalized:
            if filtered_lines and filtered_lines[-1] != "":
                filtered_lines.append("")
            continue

        lowered = normalized.lower()
        if "base unit" in lowered:
            continue
        if "delivery date" in lowered or "дата доставки" in lowered:
            continue
        if buyer_identified and (
            "full name and phone number" in lowered
            or "name and phone number" in lowered
            or "ваше имя и телефон" in lowered
            or "имя и телефон" in lowered
        ):
            continue

        filtered_lines.append(normalized)

    result = "\n".join(filtered_lines).strip()
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result or "..."


def _format_customer_reply(text: str, *, buyer_identified: bool = False) -> str:
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


async def process_message_result(channel: str, channel_uid: str, user_text: str, tenant: dict) -> dict[str, Any]:
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
    default_lang = tenant.get("ai_language", "ru")
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=session.get("lang"),
        user_text=user_text,
        default_lang=default_lang,
    )
    if lang_to_lock:
        session["lang"] = lang_to_lock

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
    session.update(
        derive_conversation_state(
            session=session,
            user_text=user_text,
            channel=channel,
            needs_intro=needs_intro,
            customer_identified=bool(session.get("erp_customer_id")),
            active_order_name=active_order_name,
            ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None,
        )
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
        payload={"lang": current_lang},
    )

    if needs_intro and not session.get("erp_customer_id"):
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
                payload={"reason": session.get("handoff_reason")},
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

    if active_order_name and _has_explicit_confirmation(user_text) and not _has_add_to_order_intent(user_text):
        order = await lc.get_sales_order(company_code, active_order_name)
        result["text"] = _maybe_prefix_returning_customer(
            session,
            current_lang,
            _existing_order_message(current_lang, active_order_name),
        )
        if order.get("order_print_url"):
            result["documents"].append(
                {
                    "type": "sales_order_pdf",
                    "url": order["order_print_url"],
                    "filename": f"{order.get('name') or active_order_name}.pdf",
                }
            )
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": result["text"]})
        await save_session(channel, channel_uid, session)
        _log_event(
            "conversation_outcome",
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            outcome="existing_order_reused",
            stage=session.get("stage"),
            active_order_name=active_order_name,
            has_document=bool(result.get("documents")),
            reply_preview=_preview_text(result.get("text")),
        )
        await _emit_control_plane_event(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            event_type="conversation_outcome",
            payload={
                "outcome": "existing_order_reused",
                "stage": session.get("stage"),
                "active_order_name": active_order_name,
                "has_document": bool(result.get("documents")),
            },
        )
        await _emit_transcript_message(
            lc=lc,
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            session=session,
            role="assistant",
            content=result["text"],
            message_type="service",
            payload={"documents": result.get("documents", [])},
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
            payload={"reason": session.get("handoff_reason")},
        )
        return result

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
                handoff_required=bool(session.get("handoff_required")),
                handoff_reason=session.get("handoff_reason"),
            )
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

                policy_result = evaluate_tool_call(
                    tool_name=tool_name,
                    inputs=inputs,
                    session=session,
                    tenant=tenant,
                    user_text=user_text,
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

                summary = _tool_result_summary(tool_name, parsed_result if isinstance(parsed_result, dict) else {})
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
                        **summary,
                    },
                )
                if tool_name == "create_sales_order" and parsed_result.get("name"):
                    await _emit_control_plane_event(
                        lc=lc,
                        company_code=company_code,
                        channel=channel,
                        channel_uid=channel_uid,
                        event_type="order_created",
                        payload={"name": parsed_result.get("name")},
                    )
                if tool_name == "create_invoice" and parsed_result.get("name"):
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
    final_reply = _format_customer_reply(final_reply, buyer_identified=bool(session.get("erp_customer_id")))
    final_reply = _maybe_prefix_returning_customer(session, current_lang, final_reply)
    session["messages"].append({"role": "assistant", "content": final_reply})
    session["messages"] = session["messages"][-40:]
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
        payload={"documents": result.get("documents", [])},
    )
    result["text"] = final_reply
    return result


async def process_message(channel: str, channel_uid: str, user_text: str, tenant: dict) -> str:
    result = await process_message_result(channel, channel_uid, user_text, tenant)
    return str(result.get("text") or "")
