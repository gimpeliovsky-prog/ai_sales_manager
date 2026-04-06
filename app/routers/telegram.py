import logging
import re
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Header, HTTPException, Path, Request

from app.agent import get_intro_message, process_message_result
from app.license_client import get_license_client
from app.session_store import clear_session, load_session, save_session

logger = logging.getLogger(__name__)
router = APIRouter()

_WELCOME_MESSAGES = {
    "ru": "Здравствуйте! Чем могу помочь?",
    "en": "Hello! How can I help you today?",
    "he": "שלום! איך אפשר לעזור לך היום?",
    "ar": "مرحبا! كيف يمكنني مساعدتك اليوم؟",
}
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


def _language_from_telegram_message(message: dict, tenant: dict, session_lang: str | None = None) -> str:
    if session_lang in _WELCOME_MESSAGES:
        return session_lang
    raw_code = str((message.get("from") or {}).get("language_code") or "").strip().lower()
    if raw_code.startswith("ru"):
        return "ru"
    if raw_code.startswith("en"):
        return "en"
    if raw_code.startswith("he") or raw_code.startswith("iw"):
        return "he"
    if raw_code.startswith("ar"):
        return "ar"
    tenant_lang = tenant.get("ai_language", "ru")
    return tenant_lang if tenant_lang in _WELCOME_MESSAGES else "ru"

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


@router.post("/{bot_token}")
async def telegram_webhook(
    request: Request,
    bot_token: str = Path(...),
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    _ = x_telegram_bot_api_secret_token
    lc = get_license_client()
    resolved = await lc.resolve_telegram(bot_token)
    if not resolved.get("found"):
        logger.warning("Unknown bot_token prefix: %s...", bot_token[:8])
        raise HTTPException(status_code=404, detail="Bot not configured")

    tenant = resolved["tenant"]
    data = await request.json()
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
                result = await process_message_result(channel="telegram", channel_uid=chat_id, user_text=str(confirm_text), tenant=tenant)
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

    if text in ("/reset", "/новый"):
        await clear_session("telegram", chat_id)
        result = {"text": get_intro_message(greeting_lang), "documents": []}
    elif text == "/start":
        known_buyer = await lc.find_buyer_by_telegram(tenant["company_code"], chat_id)
        if known_buyer.get("found"):
            result = {"text": _WELCOME_MESSAGES.get(greeting_lang, _WELCOME_MESSAGES["ru"]), "documents": []}
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
        result = await process_message_result(channel="telegram", channel_uid=chat_id, user_text=text, tenant=tenant)

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
