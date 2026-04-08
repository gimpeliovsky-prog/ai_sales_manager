import logging
import re
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Header, HTTPException, Path, Request

from app.agent import get_intro_message, process_message_result
from app.config import get_settings
from app.i18n import text as i18n_text
from app.lead_management import apply_sales_owner_action, build_lead_event_payload, normalize_telegram_username
from app.license_client import get_license_client
from app.outbound_channels import lost_reason_buttons
from app.sales_timeline import append_lead_timeline_event
from app.session_store import (
    clear_session,
    load_session,
    resolve_lead_session,
    save_sales_owner_telegram_chat,
    save_session,
    save_session_snapshot,
)

logger = logging.getLogger(__name__)
router = APIRouter()
_DEBUG_CATALOG_COMMAND_RE = re.compile(r"^/debug_catalog(?:\s+\S+)?\s*$", re.IGNORECASE)

_ORDER_PDF_RE = re.compile(
    r"^(?:/order|send me order|send order|send my order|order pdf|order file|пришли заказ|отправь заказ)$",
    re.IGNORECASE,
)
_CONFIRM_PROMPT_RE = re.compile(
    r"(?im)^(?:"
    r"please confirm in one message:?|"
    r"just confirm like this:?|"
    r"to confirm[^:\n]*:?|"
    r"reply:?|"
    r"подтвердите одной фразой:?|"
    r"напишите, пожалуйста:?|"
    r"просто подтвердите так:?|"
    r"для подтверждения[^:\n]*:?|"
    r"שלח(?:י)? לאישור:?|"
    r"للتاكيد[^:\n]*:?"
    r")\s*$"
)
_ORDER_NAME_RE = re.compile(r"\bSAL-ORD-\d{4}-\d+\b", re.IGNORECASE)
_OWNER_CALLBACK_RE = re.compile(r"^lead_owner:(accept|reassign|close_menu):([A-Za-z0-9_:-]+)$")
_OWNER_CLOSE_REASON_RE = re.compile(r"^lead_close_reason:([a-z_]+):([A-Za-z0-9_:-]+)$")


def _language_from_telegram_message(message: dict, tenant: dict, session_lang: str | None = None) -> str:
    if str(session_lang or "").strip():
        return str(session_lang).strip().lower().replace("_", "-").split("-", 1)[0]
    raw_code = str((message.get("from") or {}).get("language_code") or "").strip().lower()
    if raw_code.startswith("ru"):
        return "ru"
    if raw_code.startswith("en"):
        return "en"
    if raw_code.startswith("he") or raw_code.startswith("iw"):
        return "he"
    if raw_code.startswith("ar"):
        return "ar"
    if re.match(r"^[a-z]{2,3}(?:[-_][a-z0-9]+)?$", raw_code):
        return raw_code.replace("_", "-").split("-", 1)[0]
    tenant_lang = tenant.get("ai_language", "ru")
    return str(tenant_lang or "auto").strip().lower().replace("_", "-").split("-", 1)[0] or "auto"


def _lead_management_config(tenant: dict) -> dict:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    return ai_policy.get("lead_management") if isinstance(ai_policy.get("lead_management"), dict) else {}


async def _remember_sales_owner_chat_if_configured(tenant: dict, message: dict, chat_id: str) -> bool:
    lead_config = _lead_management_config(tenant)
    owner_username = normalize_telegram_username(lead_config.get("sales_owner_telegram_username"))
    if not owner_username:
        return False
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    if str(chat.get("type") or "").lower() != "private":
        return False
    user = message.get("from") if isinstance(message.get("from"), dict) else {}
    sender_username = normalize_telegram_username(user.get("username"))
    if sender_username != owner_username:
        return False
    await save_sales_owner_telegram_chat(
        company_code=str(tenant.get("company_code") or ""),
        username=owner_username,
        chat_id=chat_id,
        user=user,
    )
    return True

async def _send_telegram_pdf(
    client: httpx.AsyncClient,
    *,
    bot_token: str,
    chat_id: str,
    tenant: dict,
    url: str,
    filename: str,
) -> None:
    auth = (tenant.get("api_key"), tenant.get("api_secret"))
    headers = {"Authorization": f"token {auth[0]}:{auth[1]}"} if auth[0] and auth[1] else {}
    pdf_response = await client.get(url, headers=headers, follow_redirects=True, timeout=60.0)
    pdf_response.raise_for_status()
    content_type = pdf_response.headers.get("content-type", "")
    if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
        logger.warning("Unexpected content-type for Telegram PDF send: %s", content_type)
    await client.post(
        f"https://api.telegram.org/bot{bot_token}/sendDocument",
        data={"chat_id": chat_id},
        files={"document": (filename, pdf_response.content, "application/pdf")},
    )


def _extract_confirmation_phrase(text: str) -> str | None:
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines()]
    for index, line in enumerate(lines):
        if not line:
            continue
        if _CONFIRM_PROMPT_RE.match(line):
            for next_line in lines[index + 1 :]:
                candidate = next_line.strip()
                if candidate:
                    return candidate
    for line in lines:
        candidate = line.strip()
        if re.match(r"(?i)^(?:i confirm|confirmo|подтверждаю|מאשר|أؤكد)\b", candidate):
            return candidate
    return None


def _normalize_confirmation_phrase(phrase: str | None, active_order_name: str | None) -> str | None:
    if not phrase:
        return None
    normalized = phrase.strip()
    if active_order_name and _ORDER_NAME_RE.search(normalized):
        normalized = _ORDER_NAME_RE.sub(active_order_name, normalized)
    return normalized or None


def _confirm_button_label(lang: str) -> str:
    labels = {
        "ru": "Подтвердить заказ",
        "en": "Confirm order",
        "he": "אשר הזמנה",
        "ar": "تأكيد الطلب",
    }
    return labels.get(lang, labels["en"])


def _cancel_button_label(lang: str) -> str:
    labels = {
        "ru": "Отмена",
        "en": "Cancel",
        "he": "ביטול",
        "ar": "إلغاء",
    }
    return labels.get(lang, labels["en"])


def _expired_confirmation_text(lang: str) -> str:
    texts = {
        "ru": "Подтверждение устарело.",
        "en": "Confirmation expired.",
        "he": "תוקף האישור פג.",
        "ar": "انتهت صلاحية التأكيد.",
    }
    return texts.get(lang, texts["en"])


def _cancel_confirmation_text(lang: str) -> str:
    texts = {
        "ru": "Подтверждение отменено.",
        "en": "Confirmation canceled.",
        "he": "האישור בוטל.",
        "ar": "تم إلغاء التأكيد.",
    }
    return texts.get(lang, texts["en"])


def _temporary_error_text(lang: str) -> str:
    texts = {
        "ru": "Сейчас не получилось обработать подтверждение из-за временной ошибки. Попробуйте ещё раз.",
        "en": "I couldn't process the confirmation right now because of a temporary error. Please try again.",
        "he": "לא הצלחתי לעבד את האישור כרגע בגלל שגיאה זמנית. נסה שוב.",
        "ar": "تعذر عليّ معالجة التأكيد الآن بسبب خطأ مؤقت. حاول مرة أخرى.",
    }
    return texts.get(lang, texts["en"])


def _matches_debug_catalog_command(text: str) -> bool:
    return bool(_DEBUG_CATALOG_COMMAND_RE.match(str(text or "").strip()))


async def _debug_catalog_preview_text(*, lc, tenant: dict, lang: str, limit: int) -> str:
    result = await lc.get_items(
        str(tenant.get("company_code") or ""),
        None,
        None,
        None if lang == "auto" else lang,
    )
    items = result.get("items") if isinstance(result, dict) else None
    if not isinstance(items, list) or not items:
        return "Catalog returned no items for this tenant."
    lines = [f"Catalog preview, first {min(limit, len(items))} items:"]
    for index, item in enumerate(items[: max(1, limit)], start=1):
        if not isinstance(item, dict):
            continue
        name = str(item.get("display_item_name") or item.get("item_name") or item.get("item_code") or "").strip()
        item_code = str(item.get("item_code") or "").strip()
        item_group = str(item.get("item_group") or "").strip()
        line = f"{index}. {name or 'Unnamed item'}"
        if item_code:
            line += f" [{item_code}]"
        if item_group:
            line += f" ({item_group})"
        lines.append(line)
    return "\n".join(lines)


def _owner_action_text(action: str) -> str:
    if action == "accept":
        return "Лид принят в работу."
    if action == "reassign":
        return "Запрошена передача лида другому ответственному."
    if action == "close":
        return "Лид закрыт как нецелевой."
    return "Действие обработано."


def _owner_event_type(action: str) -> str:
    return {
        "accept": "lead_owner_accepted",
        "reassign": "lead_owner_reassign_requested",
        "close": "lead_closed_lost_reason_selected",
    }.get(action, "lead_owner_action")


async def _handle_owner_callback(
    *,
    bot_token: str,
    callback_query: dict,
    action: str,
    lead_id: str,
    lost_reason: str | None = None,
) -> bool:
    callback_id = str(callback_query.get("id") or "")
    actor_id = str((callback_query.get("from") or {}).get("id") or "")
    resolved = await resolve_lead_session(lead_id)
    if not resolved:
        async with httpx.AsyncClient() as client:
            if callback_id:
                await client.post(
                    f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                    json={"callback_query_id": callback_id, "text": "Лид не найден или устарел."},
                )
        return True

    if action == "close_menu":
        message = callback_query.get("message") or {}
        async with httpx.AsyncClient() as client:
            if callback_id:
                await client.post(
                    f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                    json={"callback_query_id": callback_id, "text": "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u043f\u0440\u0438\u0447\u0438\u043d\u0443"},
                )
            chat_id = str((message.get("chat") or {}).get("id") or "")
            message_id = message.get("message_id")
            if chat_id and message_id:
                await client.post(
                    f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup",
                    json={
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "reply_markup": {"inline_keyboard": lost_reason_buttons(lead_id)},
                    },
                )
        return True

    channel, uid, session = resolved
    previous_profile = session.get("lead_profile")
    session["lead_profile"] = apply_sales_owner_action(
        current_profile=session.get("lead_profile"),
        action="close" if lost_reason else action,
        actor_id=actor_id,
        lost_reason=lost_reason,
    )
    append_lead_timeline_event(
        session,
        event_type=_owner_event_type(action),
        payload={"owner_action": action, "owner_actor_id": actor_id, "lost_reason": lost_reason},
        actor=actor_id,
    )
    await save_session_snapshot(channel, uid, session)
    company_code = str(session.get("company_code") or "").strip()
    if company_code:
        payload = build_lead_event_payload(session=session, previous_profile=previous_profile)
        payload["owner_action"] = action
        payload["owner_actor_id"] = actor_id
        payload["lost_reason"] = lost_reason
        await get_license_client().create_conversation_event(
            company_code,
            event_type=_owner_event_type(action),
            session_id=f"{channel}:{uid}",
            channel_type=channel,
            channel_user_id=uid,
            payload_json=payload,
            buyer_identity_id=session.get("buyer_identity_id"),
        )

    message = callback_query.get("message") or {}
    text = _owner_action_text(action)
    if lost_reason:
        text = f"\u041b\u0438\u0434 \u0437\u0430\u043a\u0440\u044b\u0442. \u041f\u0440\u0438\u0447\u0438\u043d\u0430: {lost_reason}"
    async with httpx.AsyncClient() as client:
        if callback_id:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": text},
            )
        chat_id = str((message.get("chat") or {}).get("id") or "")
        message_id = message.get("message_id")
        if chat_id and message_id:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup",
                json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}},
            )
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
            )
    return True


@router.post("/{bot_token}")
async def telegram_webhook(
    request: Request,
    bot_token: str = Path(...),
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    _ = x_telegram_bot_api_secret_token
    lc = get_license_client()
    settings = get_settings()
    data = await request.json()
    callback_query = data.get("callback_query")
    if callback_query:
        callback_data = str(callback_query.get("data") or "")
        owner_match = _OWNER_CALLBACK_RE.match(callback_data)
        if owner_match:
            return {
                "ok": await _handle_owner_callback(
                    bot_token=bot_token,
                    callback_query=callback_query,
                    action=owner_match.group(1),
                    lead_id=owner_match.group(2),
                )
            }
        close_reason_match = _OWNER_CLOSE_REASON_RE.match(callback_data)
        if close_reason_match:
            return {
                "ok": await _handle_owner_callback(
                    bot_token=bot_token,
                    callback_query=callback_query,
                    action="close",
                    lost_reason=close_reason_match.group(1),
                    lead_id=close_reason_match.group(2),
                )
            }

    resolved = await lc.resolve_telegram(bot_token)
    if not resolved.get("found"):
        logger.warning("Unknown bot_token prefix: %s...", bot_token[:8])
        raise HTTPException(status_code=404, detail="Bot not configured")

    tenant = resolved["tenant"]
    callback_query = data.get("callback_query")
    if callback_query:
        chat_id = str(((callback_query.get("message") or {}).get("chat") or {}).get("id") or callback_query.get("from", {}).get("id") or "")
        if not chat_id:
            return {"ok": True}
        session = await load_session("telegram", chat_id)
        greeting_lang = _language_from_telegram_message(callback_query.get("message") or {}, tenant, session.get("lang"))
        callback_data = str(callback_query.get("data") or "")
        callback_id = str(callback_query.get("id") or "")
        if callback_data == "confirm_order":
            confirm_text = session.get("pending_confirmation_text")
            previous_order_name = session.get("last_sales_order_name")
            async with httpx.AsyncClient() as client:
                if callback_id:
                    if not confirm_text:
                        await client.post(
                            f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                            json={"callback_query_id": callback_id, "text": _expired_confirmation_text(greeting_lang)},
                        )
                    else:
                        await client.post(
                            f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                            json={"callback_query_id": callback_id},
                        )
            if not confirm_text:
                return {"ok": True}
            try:
                result = await process_message_result(
                    channel="telegram",
                    channel_uid=chat_id,
                    user_text=str(confirm_text),
                    tenant=tenant,
                    channel_context={"telegram_bot_token": bot_token},
                )
                session = await load_session("telegram", chat_id)
                session["pending_confirmation_text"] = None
                await save_session("telegram", chat_id, session)
                async with httpx.AsyncClient() as client:
                    has_sales_order_pdf = False
                    sent_pdf_keys: set[tuple[str, str]] = set()
                    for document in result.get("documents", []):
                        if document.get("type") == "sales_order_pdf" and document.get("url"):
                            doc_key = (
                                str(document.get("url") or ""),
                                str(document.get("filename") or "sales-order.pdf"),
                            )
                            if doc_key in sent_pdf_keys:
                                continue
                            sent_pdf_keys.add(doc_key)
                            has_sales_order_pdf = True
                            await _send_telegram_pdf(
                                client,
                                bot_token=bot_token,
                                chat_id=chat_id,
                                tenant=tenant,
                                url=document["url"],
                                filename=document.get("filename") or "sales-order.pdf",
                            )
                    current_order_name = session.get("last_sales_order_name")
                    if not has_sales_order_pdf and current_order_name and (
                        current_order_name != previous_order_name or "order" in str(confirm_text).lower() or "заказ" in str(confirm_text).lower()
                    ):
                        order = await lc.get_sales_order(tenant["company_code"], current_order_name)
                        order_pdf_url = order.get("order_print_url")
                        if order_pdf_url:
                            has_sales_order_pdf = True
                            await _send_telegram_pdf(
                                client,
                                bot_token=bot_token,
                                chat_id=chat_id,
                                tenant=tenant,
                                url=order_pdf_url,
                                filename=f"{order.get('name') or current_order_name}.pdf",
                            )
                    if not has_sales_order_pdf and result.get("text"):
                        await client.post(
                            f"https://api.telegram.org/bot{bot_token}/sendMessage",
                            json={"chat_id": chat_id, "text": result.get("text")},
                        )
            except Exception:
                logger.exception("Telegram confirmation callback failed")
                async with httpx.AsyncClient() as client:
                    await client.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={"chat_id": chat_id, "text": _temporary_error_text(greeting_lang)},
                    )
            return {"ok": True}
        if callback_data == "cancel_order_confirmation":
            session["pending_confirmation_text"] = None
            await save_session("telegram", chat_id, session)
            async with httpx.AsyncClient() as client:
                if callback_id:
                    await client.post(
                        f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery",
                        json={"callback_query_id": callback_id, "text": _cancel_confirmation_text(greeting_lang)},
                    )
                await client.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": _cancel_confirmation_text(greeting_lang)},
                )
            return {"ok": True}
        return {"ok": True}
    message = data.get("message") or data.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id = str(message["chat"]["id"])
    text = (message.get("text") or "").strip()
    if not text:
        return {"ok": True}
    session = await load_session("telegram", chat_id)
    greeting_lang = _language_from_telegram_message(message, tenant, session.get("lang"))
    is_sales_owner_chat = await _remember_sales_owner_chat_if_configured(tenant, message, chat_id)

    if text in ("/reset", "/новый"):
        await clear_session("telegram", chat_id)
        result = {"text": get_intro_message(greeting_lang), "documents": []}
    elif _matches_debug_catalog_command(text):
        result = {
            "text": await _debug_catalog_preview_text(
                lc=lc,
                tenant=tenant,
                lang=greeting_lang,
                limit=max(1, int(settings.telegram_debug_catalog_limit or 5)),
            ),
            "documents": [],
        }
    elif is_sales_owner_chat and text in {"/start", "/owner", "/sales_owner"}:
        result = {
            "text": "Sales owner Telegram chat registered for AI lead handoff.",
            "documents": [],
        }
    elif text == "/start":
        known_buyer = await lc.find_buyer_by_telegram(tenant["company_code"], chat_id)
        if known_buyer.get("found"):
            result = {"text": i18n_text("welcome.generic", greeting_lang), "documents": []}
        else:
            await clear_session("telegram", chat_id)
            result = {"text": get_intro_message(greeting_lang), "documents": []}
    elif _ORDER_PDF_RE.match(text):
        order_name = session.get("last_sales_order_name")
        if order_name:
            order = await lc.get_sales_order(tenant["company_code"], order_name)
            result = {
                "text": "",
                "documents": [
                    {
                        "type": "sales_order_pdf",
                        "url": order.get("order_print_url"),
                        "filename": f"{order.get('name') or 'sales-order'}.pdf",
                    }
                ],
            }
        else:
            result = {
                "text": "В этой переписке пока нет активного заказа." if greeting_lang == "ru" else "There is no active order in this chat yet.",
                "documents": [],
            }
    else:
        result = await process_message_result(
            channel="telegram",
            channel_uid=chat_id,
            user_text=text,
            tenant=tenant,
            channel_context={"telegram_bot_token": bot_token},
        )

    async with httpx.AsyncClient() as client:
        has_sales_order_pdf = False
        sent_pdf_keys: set[tuple[str, str]] = set()
        for document in result.get("documents", []):
            if document.get("type") == "sales_order_pdf" and document.get("url"):
                doc_key = (
                    str(document.get("url") or ""),
                    str(document.get("filename") or "sales-order.pdf"),
                )
                if doc_key in sent_pdf_keys:
                    continue
                sent_pdf_keys.add(doc_key)
                has_sales_order_pdf = True
                await _send_telegram_pdf(
                    client,
                    bot_token=bot_token,
                    chat_id=chat_id,
                    tenant=tenant,
                    url=document["url"],
                    filename=document.get("filename") or "sales-order.pdf",
                )
        if has_sales_order_pdf:
            return {"ok": True}
        text_to_send = result.get("text")
        if text_to_send:
            confirmation_phrase = _extract_confirmation_phrase(text_to_send)
            confirmation_phrase = _normalize_confirmation_phrase(confirmation_phrase, session.get("last_sales_order_name"))
            if confirmation_phrase:
                session["pending_confirmation_text"] = confirmation_phrase
                session["pending_confirmation_set_at"] = datetime.now(UTC).isoformat()
            else:
                session["pending_confirmation_text"] = None
                session["pending_confirmation_set_at"] = None
            await save_session("telegram", chat_id, session)
            payload = {"chat_id": chat_id, "text": text_to_send}
            if confirmation_phrase:
                payload["reply_markup"] = {
                    "inline_keyboard": [
                        [
                            {"text": _confirm_button_label(greeting_lang), "callback_data": "confirm_order"},
                            {"text": _cancel_button_label(greeting_lang), "callback_data": "cancel_order_confirmation"},
                        ]
                    ]
                }
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json=payload,
            )

    return {"ok": True}
