from __future__ import annotations

import unittest

from app.conversation_boundary import is_short_greeting_message, reset_session_for_new_dialogue


class ConversationBoundaryTests(unittest.TestCase):
    def test_short_greeting_detection(self) -> None:
        self.assertTrue(is_short_greeting_message("היי"))
        self.assertTrue(is_short_greeting_message("שלום!"))
        self.assertTrue(is_short_greeting_message("Hello"))
        self.assertFalse(is_short_greeting_message("היי אני צריך לפטופ"))

    def test_reset_session_for_new_dialogue_preserves_buyer_identity_only(self) -> None:
        session = {
            "company_code": "dev",
            "erp_customer_id": "CUST-001",
            "buyer_name": "Peter",
            "buyer_identity_id": "buyer-1",
            "buyer_phone": "+972557704571",
            "buyer_company_name": "Kad Ltd",
            "buyer_company_registry_number": "513690867",
            "buyer_company_candidates": [],
            "buyer_company_pending": False,
            "buyer_review_required": False,
            "buyer_review_case_id": None,
            "buyer_identity_status": "recognized",
            "buyer_recognized_via": "channel_identity",
            "recent_sales_orders": [{"name": "SAL-ORD-1"}],
            "recent_sales_invoices": [{"name": "ACC-SINV-1"}],
            "channel_context": {"foo": "bar"},
            "lang": "he",
            "last_channel": "telegram",
            "messages": [{"role": "assistant", "content": "old"}],
            "stage": "invoice",
            "last_sales_order_name": "SAL-ORD-1",
            "pending_confirmation_text": "confirm",
            "lead_profile": {"status": "order_created", "product_interest": "Laptop"},
            "returning_customer_announced": True,
        }
        fresh_session = {
            "messages": [],
            "company_code": None,
            "erp_customer_id": None,
            "buyer_name": None,
            "buyer_identity_id": None,
            "buyer_phone": None,
            "buyer_company_name": None,
            "buyer_company_registry_number": None,
            "buyer_company_candidates": [],
            "buyer_company_pending": False,
            "buyer_review_required": False,
            "buyer_review_case_id": None,
            "buyer_identity_status": None,
            "buyer_recognized_via": None,
            "recent_sales_orders": [],
            "recent_sales_invoices": [],
            "returning_customer_announced": False,
            "conversation_reopened": False,
            "conversation_closed_at": None,
            "channel_context": {},
            "lang": None,
            "stage": "new",
            "lead_profile": {"status": "none", "product_interest": None},
            "last_channel": None,
            "last_sales_order_name": None,
            "pending_confirmation_text": None,
        }

        updated = reset_session_for_new_dialogue(session, fresh_session=fresh_session)

        self.assertEqual(updated["erp_customer_id"], "CUST-001")
        self.assertEqual(updated["buyer_name"], "Peter")
        self.assertEqual(updated["buyer_company_name"], "Kad Ltd")
        self.assertEqual(updated["recent_sales_orders"], [{"name": "SAL-ORD-1"}])
        self.assertEqual(updated["stage"], "new")
        self.assertEqual(updated["lead_profile"]["status"], "none")
        self.assertIsNone(updated["last_sales_order_name"])
        self.assertIsNone(updated["pending_confirmation_text"])
        self.assertTrue(updated["conversation_reopened"])
        self.assertFalse(updated["returning_customer_announced"])


if __name__ == "__main__":
    unittest.main()
