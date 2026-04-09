from __future__ import annotations

import unittest

from app.order_confirmation import message_completes_order_details


class AgentOrderCompletionTests(unittest.TestCase):
    def test_separate_order_uom_reply_counts_as_completion(self) -> None:
        session = {
            "lead_profile": {
                "separate_order_requested": True,
                "catalog_item_code": "SKU002",
                "product_interest": "Laptop",
                "quantity": 5,
                "uom": "piece",
            }
        }
        self.assertTrue(
            message_completes_order_details(
                tool_name="create_sales_order",
                session=session,
                user_text="5 Pieces",
                tenant={},
            )
        )

    def test_order_correction_qty_reply_counts_as_completion(self) -> None:
        session = {
            "lead_profile": {
                "order_correction_status": "requested",
                "catalog_item_code": "SKU002",
                "product_interest": "Laptop",
                "quantity": 5,
                "uom": "piece",
            }
        }
        self.assertTrue(
            message_completes_order_details(
                tool_name="update_sales_order",
                session=session,
                user_text="7 pieces",
                tenant={},
            )
        )


if __name__ == "__main__":
    unittest.main()
