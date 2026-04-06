import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.agent import process_message
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
            reply = await process_message(channel="webchat", channel_uid=session_id, user_text=text, tenant=tenant)
            await websocket.send_text(reply)
    except WebSocketDisconnect:
        logger.info("WebChat [%s] session %s disconnected", company_code, session_id)
