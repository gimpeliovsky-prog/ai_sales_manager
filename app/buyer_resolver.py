from __future__ import annotations

from app.license_client import LicenseClient


async def resolve_buyer(
    session: dict,
    company_code: str,
    channel: str,
    channel_uid: str,
    lc: LicenseClient,
) -> tuple[dict | None, bool]:
    if session.get("erp_customer_id"):
        return {
            "found": True,
            "erp_customer_id": session.get("erp_customer_id"),
            "erp_customer_name": session.get("buyer_name"),
            "buyer_identity_id": session.get("buyer_identity_id"),
            "phone": session.get("buyer_phone"),
            "recognized_via": "session",
            "recent_sales_orders": session.get("recent_sales_orders") or [],
            "recent_sales_invoices": session.get("recent_sales_invoices") or [],
        }, False

    phone = None
    if channel == "whatsapp":
        phone = "+" + channel_uid.lstrip("+")

    result = await lc.resolve_buyer(
        company_code,
        channel_type=channel,
        channel_user_id=channel_uid,
        phone=phone,
    )
    if result.get("found"):
        return result, False

    return None, True


async def create_buyer_from_intro(
    session: dict,
    company_code: str,
    channel: str,
    channel_uid: str,
    full_name: str,
    phone: str | None,
    lc: LicenseClient,
) -> dict | None:
    tg_chat_id = channel_uid if channel == "telegram" else None
    result = await lc.create_buyer(
        company_code,
        full_name,
        phone,
        tg_chat_id,
        channel_type=channel,
        channel_user_id=channel_uid,
    )
    if result.get("found"):
        return result
    return None
