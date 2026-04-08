import asyncio
from typing import Any

import httpx

from app.config import get_settings


class LicenseClient:
    def __init__(self):
        settings = get_settings()
        self._base = settings.license_server_url.rstrip("/") + "/api/v1/ai-agent"
        self._headers = {"X-AI-Agent-Token": settings.ai_agent_token, "Content-Type": "application/json"}
        self._http = httpx.AsyncClient(timeout=30.0)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        body: dict | None = None,
    ) -> Any:
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                response = await self._http.request(
                    method,
                    f"{self._base}{path}",
                    headers=self._headers,
                    params=params,
                    json=body,
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500 or attempt == 2:
                    raise
                last_exc = exc
            except httpx.HTTPError as exc:
                if attempt == 2:
                    raise
                last_exc = exc
            await asyncio.sleep(0.5 * (2**attempt))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("License Server request failed without an exception")

    async def _get(self, path: str, params: dict | None = None) -> Any:
        return await self._request("GET", path, params=params)

    async def _post(self, path: str, body: dict | None = None) -> Any:
        return await self._request("POST", path, body=body or {})

    async def _patch(self, path: str, body: dict) -> Any:
        return await self._request("PATCH", path, body=body)

    async def resolve_telegram(self, bot_token: str) -> dict:
        return await self._get(f"/resolve/telegram/{bot_token}")

    async def resolve_whatsapp(self, wa_number: str) -> dict:
        return await self._get(f"/resolve/whatsapp/{wa_number}")

    async def resolve_webchat(self, company_code: str) -> dict:
        return await self._get(f"/resolve/webchat/{company_code}")

    async def find_buyer_by_phone(self, company_code: str, phone: str) -> dict:
        return await self._get(f"/tenants/{company_code}/buyers/by-phone/{phone}")

    async def find_buyer_by_telegram(self, company_code: str, tg_chat_id: str) -> dict:
        return await self._get(f"/tenants/{company_code}/buyers/by-telegram/{tg_chat_id}")

    async def resolve_buyer(
        self,
        company_code: str,
        *,
        channel_type: str,
        channel_user_id: str,
        phone: str | None = None,
        full_name: str | None = None,
    ) -> dict:
        return await self._post(
            f"/tenants/{company_code}/buyers/resolve",
            {
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
                "phone": phone,
                "full_name": full_name,
            },
        )

    async def create_buyer(
        self,
        company_code: str,
        full_name: str,
        phone: str | None,
        tg_chat_id: str | None,
        *,
        channel_type: str | None = None,
        channel_user_id: str | None = None,
    ) -> dict:
        return await self._post(
            f"/tenants/{company_code}/buyers",
            {
                "company_code": company_code,
                "full_name": full_name,
                "phone": phone,
                "tg_chat_id": tg_chat_id,
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
            },
        )

    async def get_buyer_sales_history(self, company_code: str, erp_customer_id: str) -> dict:
        return await self._get(f"/tenants/{company_code}/buyers/{erp_customer_id}/sales-history")

    async def get_ai_policy(self, company_code: str) -> dict:
        return await self._get(f"/tenants/{company_code}/ai-policy")

    async def get_items(
        self,
        company_code: str,
        item_group: str | None = None,
        item_name: str | None = None,
        lang: str | None = None,
        limit: int | None = None,
        enrich: bool | None = None,
    ) -> dict:
        params: dict[str, str] = {}
        if item_group:
            params["item_group"] = item_group
        if item_name:
            params["item_name"] = item_name
        if lang:
            params["lang"] = lang
        if limit is not None:
            params["limit"] = str(limit)
        if enrich is not None:
            params["enrich"] = "true" if enrich else "false"
        return await self._get(f"/tenants/{company_code}/items", params or None)

    async def get_item(self, company_code: str, item_code: str, lang: str | None = None) -> dict:
        params = {"lang": lang} if lang else None
        return await self._get(f"/tenants/{company_code}/items/{item_code}", params)

    async def create_sales_order(self, company_code: str, customer: str, delivery_date: str, items: list) -> dict:
        return await self._post(
            f"/tenants/{company_code}/sales-orders",
            {"company_code": company_code, "customer": customer, "delivery_date": delivery_date, "items": items},
        )

    async def get_sales_order(self, company_code: str, sales_order_name: str) -> dict:
        return await self._get(f"/tenants/{company_code}/sales-orders/{sales_order_name}")

    async def get_sales_order_status(self, company_code: str, sales_order_name: str) -> dict:
        return await self._get(f"/tenants/{company_code}/sales-orders/{sales_order_name}/status")

    async def update_sales_order_items(self, company_code: str, sales_order_name: str, items: list) -> dict:
        return await self._post(
            f"/tenants/{company_code}/sales-orders/{sales_order_name}/items",
            {"company_code": company_code, "sales_order_name": sales_order_name, "items": items},
        )

    async def create_invoice(self, company_code: str, sales_order_name: str) -> dict:
        return await self._post(
            f"/tenants/{company_code}/invoices",
            {"company_code": company_code, "sales_order_name": sales_order_name},
        )

    async def create_license(self, company_code: str, description: str | None = None) -> dict:
        return await self._post(f"/tenants/{company_code}/licenses", {"company_code": company_code, "description": description})

    async def extend_subscription(self, company_code: str, add_days: int) -> dict:
        return await self._patch(f"/tenants/{company_code}/subscription", {"add_days": add_days})

    async def create_conversation_event(
        self,
        company_code: str,
        *,
        event_type: str,
        session_id: str | None,
        channel_type: str,
        channel_user_id: str,
        payload_json: dict[str, Any] | None = None,
        buyer_identity_id: str | None = None,
        ) -> dict:
        return await self._post(
            f"/tenants/{company_code}/conversation-events",
            {
                "event_type": event_type,
                "session_id": session_id,
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
                "buyer_identity_id": buyer_identity_id,
                "payload_json": payload_json or {},
            },
        )

    async def create_transcript_message(
        self,
        company_code: str,
        session_id: str,
        *,
        message_id: str,
        channel_type: str,
        channel_user_id: str,
        role: str,
        message_type: str = "chat",
        content: str | None = None,
        stage: str | None = None,
        behavior_class: str | None = None,
        tool_name: str | None = None,
        payload_json: dict[str, Any] | None = None,
        buyer_identity_id: str | None = None,
        erp_customer_id: str | None = None,
        buyer_name: str | None = None,
        buyer_phone: str | None = None,
    ) -> dict:
        return await self._post(
            f"/tenants/{company_code}/transcript/{session_id}/messages",
            {
                "message_id": message_id,
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
                "role": role,
                "message_type": message_type,
                "content": content,
                "stage": stage,
                "behavior_class": behavior_class,
                "tool_name": tool_name,
                "payload_json": payload_json or {},
                "buyer_identity_id": buyer_identity_id,
                "erp_customer_id": erp_customer_id,
                "buyer_name": buyer_name,
                "buyer_phone": buyer_phone,
            },
        )

    async def create_handoff(
        self,
        company_code: str,
        *,
        channel_type: str,
        channel_user_id: str,
        session_id: str | None,
        reason: str | None,
        payload_json: dict[str, Any] | None = None,
        buyer_identity_id: str | None = None,
    ) -> dict:
        return await self._post(
            f"/tenants/{company_code}/handoffs",
            {
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
                "session_id": session_id,
                "buyer_identity_id": buyer_identity_id,
                "reason": reason,
                "payload_json": payload_json or {},
            },
        )


_instance: LicenseClient | None = None


def get_license_client() -> LicenseClient:
    global _instance
    if _instance is None:
        _instance = LicenseClient()
    return _instance
