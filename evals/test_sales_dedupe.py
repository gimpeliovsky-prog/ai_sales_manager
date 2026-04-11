from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from app.sales_dedupe import detect_duplicate_lead


class SalesDedupeTests(unittest.TestCase):
    def test_same_active_order_is_still_duplicate(self) -> None:
        now = datetime.now(UTC)
        match = detect_duplicate_lead(
            current={
                "lead_id": "lead_current",
                "company_code": "acme",
                "erp_customer_id": "CUST-1",
                "product_interest": "laptop",
                "active_order_name": "SO-1",
                "last_interaction_at": now.isoformat(),
            },
            candidates=[
                {
                    "lead_id": "lead_old",
                    "company_code": "acme",
                    "erp_customer_id": "CUST-1",
                    "product_interest": "other product",
                    "active_order_name": "SO-1",
                    "last_interaction_at": (now - timedelta(days=30)).isoformat(),
                }
            ],
            now=now,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match["dedupe_reason"], "same_active_order")

    def test_same_channel_uid_alone_no_longer_dedupes(self) -> None:
        now = datetime.now(UTC)
        match = detect_duplicate_lead(
            current={
                "lead_id": "lead_current",
                "company_code": "acme",
                "channel": "telegram",
                "channel_uid": "u1",
                "product_interest": "laptop",
                "last_interaction_at": now.isoformat(),
            },
            candidates=[
                {
                    "lead_id": "lead_old",
                    "company_code": "acme",
                    "channel": "telegram",
                    "channel_uid": "u1",
                    "product_interest": "laptops",
                    "last_interaction_at": (now - timedelta(hours=2)).isoformat(),
                }
            ],
            now=now,
        )
        self.assertIsNone(match)

    def test_same_customer_uses_canonical_product_anchor(self) -> None:
        now = datetime.now(UTC)
        match = detect_duplicate_lead(
            current={
                "lead_id": "lead_current",
                "company_code": "acme",
                "erp_customer_id": "CUST-1",
                "product_interest": "maybe laptop",
                "last_interaction_at": now.isoformat(),
            },
            candidates=[
                {
                    "lead_id": "lead_old",
                    "company_code": "acme",
                    "erp_customer_id": "CUST-1",
                    "product_interest": "laptop",
                    "last_interaction_at": (now - timedelta(hours=1)).isoformat(),
                }
            ],
            now=now,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match["dedupe_reason"], "same_customer_similar_product")


if __name__ == "__main__":
    unittest.main()
