from __future__ import annotations

import unittest

from app.conversation_flow import derive_conversation_state
from app.lead_management import update_lead_profile_from_message
from app.tool_policy import evaluate_tool_call


class SeparateOrderFlowTests(unittest.TestCase):
    def test_active_order_can_start_separate_new_order(self) -> None:
        lead_profile = update_lead_profile_from_message(
            current_profile={"status": "order_created", "quantity": 3, "uom": "Nos"},
            user_text="новый заказ 25 t-shirt",
            stage="invoice",
            behavior_class="direct_buyer",
            intent="find_product",
            customer_identified=True,
            active_order_name="SO-1",
        )
        self.assertTrue(lead_profile.get("separate_order_requested"))
        self.assertEqual(lead_profile.get("order_correction_status"), "none")
        self.assertEqual(lead_profile.get("quantity"), 25.0)

        state = derive_conversation_state(
            session={"stage": "invoice", "erp_customer_id": "CUST-1"},
            user_text="новый заказ 25 t-shirt",
            channel="telegram",
            needs_intro=False,
            customer_identified=True,
            active_order_name="SO-1",
            lead_profile=lead_profile,
            previous_lead_profile={"status": "order_created"},
            behavior_class="direct_buyer",
            behavior_confidence=0.9,
            intent="find_product",
            intent_confidence=0.8,
        )
        self.assertEqual(state.get("stage"), "order_build")

    def test_active_order_new_product_request_defaults_to_separate_order(self) -> None:
        lead_profile = update_lead_profile_from_message(
            current_profile={
                "status": "order_created",
                "product_interest": "\u05de\u05d4 \u05e9\u05dc\u05d5\u05de\u05da",
                "need": "\u05de\u05d4 \u05e9\u05dc\u05d5\u05de\u05da",
                "order_correction_status": "none",
            },
            user_text="\u05d0\u05e0\u05d9 \u05e8\u05d5\u05e6\u05d4 5 laptop",
            stage="invoice",
            behavior_class="returning_customer",
            intent="order_detail",
            customer_identified=True,
            active_order_name="SO-1",
        )
        self.assertTrue(lead_profile.get("separate_order_requested"))
        self.assertEqual(lead_profile.get("order_correction_status"), "none")
        self.assertEqual(lead_profile.get("product_interest"), "laptop")
        self.assertEqual(lead_profile.get("quantity"), 5.0)

    def test_tool_policy_allows_create_sales_order_from_invoice_when_separate_order_requested(self) -> None:
        session = {
            "stage": "invoice",
            "erp_customer_id": "CUST-1",
            "last_sales_order_name": "SO-1",
            "lead_profile": {
                "separate_order_requested": True,
                "requested_items_need_uom_confirmation": False,
            },
        }
        result = evaluate_tool_call(
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU001", "qty": 25, "uom": "Nos"}]},
            session=session,
            tenant={"ai_policy": {"allowed_tools": ["create_sales_order"]}},
            user_text="да оформляй",
            confirmation_override=True,
        )
        self.assertIsNone(result)

    def test_tool_policy_blocks_update_when_customer_requested_separate_order(self) -> None:
        session = {
            "stage": "invoice",
            "erp_customer_id": "CUST-1",
            "last_sales_order_name": "SO-1",
            "lead_profile": {
                "separate_order_requested": True,
                "requested_items_need_uom_confirmation": False,
            },
        }
        result = evaluate_tool_call(
            tool_name="update_sales_order",
            inputs={"sales_order_name": "SO-1", "items": [{"item_code": "SKU001", "qty": 25, "uom": "Nos", "action": "add"}]},
            session=session,
            tenant={"ai_policy": {"allowed_tools": ["update_sales_order"]}},
            user_text="да оформляй",
            confirmation_override=True,
        )
        self.assertIsNotNone(result)
        self.assertTrue(result.get("blocked_by_policy"))


if __name__ == "__main__":
    unittest.main()
