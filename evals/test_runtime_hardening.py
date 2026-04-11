from __future__ import annotations

import unittest
from unittest.mock import patch

from app.buyer_intake import truncate_inbound_text
from app.i18n import text as i18n_text

try:
    import app.agent as agent_module
except ModuleNotFoundError:
    agent_module = None


class RuntimeHardeningTests(unittest.TestCase):
    def test_truncate_inbound_text_limits_payload_size(self) -> None:
        raw = "x" * 5000
        normalized = truncate_inbound_text(raw, max_chars=4000)
        self.assertEqual(len(normalized), 4000)

    def test_truncate_inbound_text_strips_null_bytes_and_whitespace(self) -> None:
        normalized = truncate_inbound_text(" \x00 hello \x00 ", max_chars=100)
        self.assertEqual(normalized, "hello")

    def test_runtime_temporary_error_translation_exists(self) -> None:
        message = i18n_text("runtime.temporary_error", "en")
        self.assertIn("temporary error", message.lower())


@unittest.skipIf(agent_module is None, "app.agent dependencies are unavailable in this interpreter")
class RuntimePrefetchHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_catalog_prefetch_tool_failure_does_not_crash_turn(self) -> None:
        session = {
            "stage": "discover",
            "last_intent": "find_product",
            "lead_profile": {
                "product_interest": "laptop",
                "catalog_item_code": None,
                "catalog_lookup_status": "unknown",
            },
        }

        async def _raise_execute_tool(**_: object) -> str:
            raise RuntimeError("prefetch boom")

        with patch.object(agent_module, "execute_tool", _raise_execute_tool):
            context = await agent_module._maybe_prefetch_catalog_context(
                lc=None,
                company_code="dev",
                channel="telegram",
                channel_uid="123",
                current_lang="en",
                user_text="i need a laptop",
                tenant={},
                session=session,
                intent="find_product",
            )
        self.assertIsNone(context)

    async def test_availability_prefetch_tool_failure_does_not_crash_turn(self) -> None:
        session = {
            "stage": "discover",
            "last_sales_order_name": None,
            "lead_profile": {
                "catalog_item_code": "SKU002",
                "catalog_item_name": "Laptop",
                "availability_checked_at": None,
            },
        }

        async def _raise_execute_tool(**_: object) -> str:
            raise RuntimeError("availability boom")

        with patch.object(agent_module, "execute_tool", _raise_execute_tool):
            context = await agent_module._maybe_prefetch_availability_context(
                lc=None,
                company_code="dev",
                channel="telegram",
                channel_uid="123",
                current_lang="en",
                user_text="do you have it in stock?",
                tenant={},
                session=session,
            )
        self.assertIsNone(context)

    async def test_order_status_prefetch_tool_failure_does_not_crash_turn(self) -> None:
        session = {
            "stage": "invoice",
            "last_intent": "service_request",
            "last_sales_order_name": "SO-1",
            "lead_profile": {
                "order_correction_status": "none",
                "active_order_can_modify": None,
            },
        }

        async def _raise_execute_tool(**_: object) -> str:
            raise RuntimeError("order status boom")

        with patch.object(agent_module, "execute_tool", _raise_execute_tool):
            context = await agent_module._maybe_prefetch_order_status_context(
                lc=None,
                company_code="dev",
                channel="telegram",
                channel_uid="123",
                current_lang="en",
                user_text="send pdf",
                tenant={},
                session=session,
            )
        self.assertIsNone(context)


if __name__ == "__main__":
    unittest.main()
