from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
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


async def resolve_buyer_from_intro(
    session: dict,
    company_code: str,
    channel: str,
    channel_uid: str,
    full_name: str,
    phone: str | None,
    lc: LicenseClient,
) -> dict | None:
    result = await lc.resolve_buyer(
        company_code,
        channel_type=channel,
        channel_user_id=channel_uid,
        phone=phone,
        full_name=full_name,
    )
    if isinstance(result, dict) and result.get("found"):
        return result
    return None


async def create_buyer_from_intro(
    session: dict,
    company_code: str,
    channel: str,
    channel_uid: str,
    full_name: str,
    phone: str | None,
    lc: LicenseClient,
) -> dict | None:
    return await resolve_buyer_from_intro(
        session=session,
        company_code=company_code,
        channel=channel,
        channel_uid=channel_uid,
        full_name=full_name,
        phone=phone,
        lc=lc,
    )
