import logging
from urllib.parse import parse_qs

from fastapi import APIRouter, Request, Response
from twilio.twiml.messaging_response import MessagingResponse

from app.agent import process_message
from app.i18n import text as i18n_text
from app.license_client import get_license_client
from app.session_store import clear_session

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("")
async def whatsapp_webhook(request: Request):
    lc = get_license_client()
    body = await request.body()
    params = parse_qs(body.decode())

    to_number = params.get("To", [""])[0].replace("whatsapp:", "")
    from_number = params.get("From", [""])[0].replace("whatsapp:", "")
    text = params.get("Body", [""])[0].strip()

    if not to_number or not from_number or not text:
        return Response(content="", media_type="text/xml")

    resolved = await lc.resolve_whatsapp(to_number)
    if not resolved.get("found"):
        logger.warning("Unknown WhatsApp number: %s", to_number)
        return Response(content="", media_type="text/xml")

    tenant = resolved["tenant"]
    if text.lower() in ("/start", "/reset"):
        await clear_session("whatsapp", from_number)
        reply = i18n_text("welcome.generic", tenant.get("ai_language", "auto"))
    else:
        reply = await process_message(
            channel="whatsapp",
            channel_uid=from_number,
            user_text=text,
            tenant=tenant,
            channel_context={"whatsapp_to_number": to_number, "whatsapp_from_number": from_number},
        )

    twiml = MessagingResponse()
    twiml.message(reply)
    return Response(content=str(twiml), media_type="text/xml")
