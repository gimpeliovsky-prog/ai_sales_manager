from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


def _setting(name: str, default: Any) -> Any:
    try:
        from app.config import get_settings

        return getattr(get_settings(), name, default)
    except Exception:
        return default


async def sync_sales_lead_record(record: dict[str, Any]) -> dict[str, Any]:
    if not bool(_setting("sales_crm_sync_enabled", False)):
        return {"synced": False, "status": "disabled"}
    webhook_url = str(_setting("sales_crm_sync_webhook_url", "") or "").strip()
    if not webhook_url:
        return {"synced": False, "status": "missing_webhook_url"}
    import httpx

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(webhook_url, json={"sales_lead": record})
            response.raise_for_status()
        return {"synced": True, "status": "sent", "via": "webhook"}
    except Exception as exc:
        logger.warning("Failed to sync sales lead %s to CRM: %s", record.get("lead_id"), exc)
        return {"synced": False, "status": "send_failed", "error": str(exc)}


def build_sales_crm_outbox_event(record: dict[str, Any], event_type: str = "lead_upserted") -> dict[str, Any]:
    return {
        "event_id": f"crm_sync_{uuid4().hex}",
        "event_type": event_type,
        "lead_id": record.get("lead_id"),
        "company_code": record.get("company_code"),
        "status": "pending",
        "attempts": 0,
        "payload": {"sales_lead": record},
    }


async def deliver_sales_crm_outbox_event(event: dict[str, Any]) -> dict[str, Any]:
    if not bool(_setting("sales_crm_sync_enabled", False)):
        return {"synced": False, "status": "disabled"}
    webhook_url = str(_setting("sales_crm_sync_webhook_url", "") or "").strip()
    if not webhook_url:
        return {"synced": False, "status": "missing_webhook_url"}
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    if not payload:
        return {"synced": False, "status": "empty_payload"}
    import httpx

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(webhook_url, json=payload)
        response.raise_for_status()
    return {"synced": True, "status": "sent", "via": "crm_outbox_webhook"}
