import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.agent import process_message
from app.i18n import text as i18n_text
from app.license_client import get_license_client
from app.session_store import clear_session

logger = logging.getLogger(__name__)
router = APIRouter()


def _origin_allowed(tenant: dict, origin: str | None) -> bool:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    channel_policy = ai_policy.get("channel") if isinstance(ai_policy.get("channel"), dict) else {}
    allowed_origins = channel_policy.get("webchat_allowed_origins")
    if not isinstance(allowed_origins, list) or not allowed_origins:
        return True
    normalized_origin = str(origin or "").strip().rstrip("/")
    if not normalized_origin:
        return False
    normalized_allowed = {str(item).strip().rstrip("/") for item in allowed_origins if str(item).strip()}
    return normalized_origin in normalized_allowed


def _webchat_source_context(websocket: WebSocket, company_code: str) -> dict[str, Any]:
    query = websocket.query_params
    source_context: dict[str, Any] = {
        "webchat_company_code": company_code,
        "origin": websocket.headers.get("origin"),
        "referrer": websocket.headers.get("referer"),
    }
    for key in [
        "campaign",
        "source_campaign",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "landing_page",
        "page_url",
        "url",
        "product_page",
        "product_url",
    ]:
        value = query.get(key)
        if value:
            source_context[key] = value
    return source_context


@router.websocket("/{company_code}/{session_id}")
async def webchat_ws(websocket: WebSocket, company_code: str, session_id: str):
    lc = get_license_client()
    resolved = await lc.resolve_webchat(company_code)
    if not resolved.get("found"):
        await websocket.close(code=4004, reason="Webchat not configured for this tenant")
        return

    tenant = resolved["tenant"]
    origin = websocket.headers.get("origin")
    if not _origin_allowed(tenant, origin):
        await websocket.close(code=4003, reason="Origin not allowed for this widget")
        return
    await websocket.accept()
    logger.info("WebChat [%s] session %s connected", company_code, session_id)

    try:
        while True:
            text = (await websocket.receive_text()).strip()
            if not text:
                continue
            if text in ("/reset", "/start"):
                await clear_session("webchat", session_id)
                await websocket.send_text("Диалог сброшен.")
                continue
            try:
                reply = await process_message(
                    channel="webchat",
                    channel_uid=session_id,
                    user_text=text,
                    tenant=tenant,
                    channel_context=_webchat_source_context(websocket, company_code),
                )
            except Exception:
                logger.exception("Webchat message processing failed")
                reply = i18n_text("runtime.temporary_error", tenant.get("ai_language", "auto"))
            await websocket.send_text(reply)
    except WebSocketDisconnect:
        logger.info("WebChat [%s] session %s disconnected", company_code, session_id)
