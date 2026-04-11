from __future__ import annotations

import unittest
from unittest.mock import patch

from app.buyer_intake import truncate_inbound_text
from app.conversation_contexts import ensure_session_contexts, reconcile_contexts_after_state_update
from app.i18n import text as i18n_text
from app.tool_policy import evaluate_tool_call

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

    def test_blocked_create_order_returns_readiness_customer_reply(self) -> None:
        session = {
            "stage": "order_build",
            "erp_customer_id": "CUST-1",
            "lead_profile": {
                "catalog_item_code": "SKU002",
                "catalog_item_name": "Laptop",
                "product_interest": "laptop",
                "quantity": 10,
                "uom": "unit",
                "next_action": "ask_delivery_timing",
                "missing_slots": ["delivery_need", "confirmation"],
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(session, previous_lead_profile={}, active_order_name=None)
        reply = evaluate_tool_call(
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU002", "qty": 10, "uom": "unit"}]},
            session=session,
            tenant={},
            user_text="ok",
        )
        self.assertIsNotNone(reply)
        self.assertTrue(reply["blocked_by_policy"])
        self.assertIn("delivery date", str(reply.get("customer_reply") or "").lower())

    def test_blocked_send_pdf_uses_customer_reply_from_order_readiness(self) -> None:
        session = {
            "stage": "confirm",
            "erp_customer_id": "CUST-1",
            "lead_profile": {
                "catalog_item_code": "SKU002",
                "catalog_item_name": "Laptop",
                "product_interest": "laptop",
                "quantity": 10,
                "uom": "unit",
                "next_action": "ask_delivery_timing",
                "missing_slots": ["delivery_need", "confirmation"],
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(session, previous_lead_profile={}, active_order_name=None)
        reply = evaluate_tool_call(
            tool_name="send_sales_order_pdf",
            inputs={"sales_order_name": ""},
            session=session,
            tenant={},
            user_text="send me an order",
        )
        self.assertIsNotNone(reply)
        self.assertTrue(reply["blocked_by_policy"])
        customer_reply = str(reply.get("customer_reply") or "").lower()
        self.assertIn("pdf", customer_reply)
        self.assertIn("delivery date", customer_reply)

    @unittest.skipIf(agent_module is None, "app.agent dependencies are unavailable in this interpreter")
    def test_confirmation_fallback_does_not_synthesize_create_when_delivery_missing(self) -> None:
        session = {
            "stage": "order_build",
            "erp_customer_id": "CUST-1",
            "lead_profile": {
                "catalog_item_code": "SKU002",
                "catalog_item_name": "Laptop",
                "product_interest": "laptop",
                "quantity": 10,
                "uom": "unit",
                "next_action": "ask_delivery_timing",
                "missing_slots": ["delivery_need", "confirmation"],
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(session, previous_lead_profile={}, active_order_name=None)
        reply = agent_module._build_confirmation_fallback_call(
            session=session,
            tenant={},
            user_text="ok",
        )
        self.assertIsNone(reply)


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
