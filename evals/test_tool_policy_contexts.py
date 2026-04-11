import unittest

from app.conversation_contexts import ensure_session_contexts, reconcile_contexts_after_state_update
from app.tool_policy import evaluate_tool_call


class ToolPolicyContextTests(unittest.TestCase):
    def test_create_invoice_allowed_from_service_context(self) -> None:
        session = {
            "erp_customer_id": "CUST-1",
            "stage": "service",
            "behavior_class": "service_request",
            "last_intent": "service_request",
            "signal_type": "service_request",
            "signal_confidence": 0.95,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {
                "status": "service",
                "target_order_id": "SO-100",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "order_created", "target_order_id": "SO-100"},
            active_order_name="SO-100",
        )
        result = evaluate_tool_call(
            tool_name="create_invoice",
            inputs={"sales_order_name": "SO-100"},
            session=session,
            tenant={},
            user_text="send invoice",
        )
        self.assertIsNone(result)

    def test_update_sales_order_blocked_in_identity_resolution_context(self) -> None:
        session = {
            "erp_customer_id": None,
            "stage": "identify",
            "behavior_class": "silent_or_low_signal",
            "last_intent": "low_signal",
            "signal_type": "low_signal",
            "signal_confidence": 0.9,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "buyer_company_pending": True,
            "lead_profile": {
                "status": "none",
                "requested_items_need_uom_confirmation": False,
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "none"},
            active_order_name="SO-101",
        )
        result = evaluate_tool_call(
            tool_name="update_sales_order",
            inputs={"sales_order_name": "SO-101", "items": [{"item_code": "SKU1", "qty": 1}]},
            session=session,
            tenant={},
            user_text="change order",
        )
        self.assertIsNotNone(result)
        self.assertTrue(result["blocked_by_policy"])

    def test_create_sales_order_blocked_after_order_already_created_in_same_context(self) -> None:
        session = {
            "erp_customer_id": "CUST-1",
            "stage": "invoice",
            "behavior_class": "direct_buyer",
            "last_intent": "confirm_order",
            "signal_type": "confirmation",
            "signal_confidence": 0.99,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {
                "status": "order_created",
                "target_order_id": "SO-200",
                "next_action": "send_order_or_offer_invoice",
                "missing_slots": [],
                "catalog_item_code": "SKU002",
                "quantity": 10,
                "uom": "piece",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "order_created", "target_order_id": "SO-200"},
            active_order_name="SO-200",
        )
        result = evaluate_tool_call(
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU002", "qty": 10, "uom": "piece"}]},
            session=session,
            tenant={},
            user_text="I confirm",
        )
        self.assertIsNotNone(result)
        self.assertTrue(result["blocked_by_policy"])
        self.assertIn("already been created", result["error"])

    def test_create_sales_order_blocked_missing_delivery_uses_readiness_reply(self) -> None:
        session = {
            "erp_customer_id": "CUST-1",
            "stage": "order_build",
            "behavior_class": "direct_buyer",
            "last_intent": "confirm_order",
            "signal_type": "confirmation",
            "signal_confidence": 0.9,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {
                "status": "order_ready",
                "next_action": "ask_delivery_timing",
                "missing_slots": ["delivery_need", "confirmation"],
                "catalog_item_code": "SKU002",
                "catalog_item_name": "Laptop",
                "quantity": 10,
                "uom": "piece",
                "product_interest": "laptop",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "qualified"},
            active_order_name=None,
        )
        result = evaluate_tool_call(
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU002", "qty": 10, "uom": "piece"}]},
            session=session,
            tenant={},
            user_text="ok",
        )
        self.assertIsNotNone(result)
        self.assertTrue(result["blocked_by_policy"])
        self.assertEqual(result.get("reason_code"), "missing_details")
        self.assertIn("delivery date", str(result.get("customer_reply") or "").lower())


if __name__ == "__main__":
    unittest.main()
