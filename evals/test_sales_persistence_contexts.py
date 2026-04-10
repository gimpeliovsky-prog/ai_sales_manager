import unittest

from app.conversation_contexts import (
    context_events,
    context_summaries,
    create_context,
    ensure_session_contexts,
    set_active_context,
)
from app.sales_lead_repository import compact_lead_record, restore_session_from_record
from app.sales_reporting import lead_snapshot


class SalesPersistenceContextTests(unittest.TestCase):
    def test_compact_lead_record_preserves_context_model(self) -> None:
        session = {
            "company_code": "dev",
            "buyer_name": "Peter",
            "erp_customer_id": "CUST-1",
            "stage": "discover",
            "behavior_class": "returning_customer",
            "last_intent": "find_product",
            "signal_type": "deal_progress",
            "signal_emotion": "neutral",
            "lead_profile": {
                "lead_id": "lead_1",
                "status": "new_lead",
                "product_interest": "Laptop",
                "quantity": 2,
                "uom": "piece",
                "next_action": "confirm_order",
            },
            "lead_timeline": [{"event_type": "lead_created"}],
        }
        ensure_session_contexts(session)
        create_context(
            session,
            context_type="service_request",
            related_order_id="SO-1",
            lead_profile={"lead_id": "lead_1", "status": "service", "target_order_id": "SO-1"},
            activate=False,
        )
        record = compact_lead_record(channel="telegram", uid="123", session=session)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record["schema_version"], 2)
        self.assertIsInstance(record.get("contexts"), dict)
        self.assertGreaterEqual(len(record.get("contexts", {})), 2)
        self.assertIsInstance(record.get("context_events"), list)
        self.assertEqual(record.get("active_context", {}).get("context_type"), "new_purchase")
        self.assertEqual(record.get("lead", {}).get("active_context_type"), "new_purchase")
        self.assertTrue(record.get("session_context", {}).get("contexts"))

    def test_restore_session_from_record_roundtrips_context_state(self) -> None:
        session = {
            "company_code": "dev",
            "buyer_name": "Peter",
            "erp_customer_id": "CUST-1",
            "stage": "invoice",
            "behavior_class": "direct_buyer",
            "last_intent": "service_request",
            "signal_type": "service_request",
            "signal_emotion": "neutral",
            "lead_profile": {
                "lead_id": "lead_2",
                "status": "order_created",
                "target_order_id": "SO-9",
                "product_interest": "Laptop",
                "next_action": "send_order_or_offer_invoice",
            },
        }
        ensure_session_contexts(session)
        service_context = create_context(
            session,
            context_type="service_request",
            related_order_id="SO-9",
            lead_profile={"lead_id": "lead_2", "status": "service", "target_order_id": "SO-9"},
            activate=True,
        )
        record = compact_lead_record(channel="telegram", uid="456", session=session)
        assert record is not None
        restored = restore_session_from_record(record)
        ensure_session_contexts(restored)
        self.assertEqual(restored.get("active_context_id"), service_context.get("context_id"))
        self.assertGreaterEqual(len(context_summaries(restored)), 2)
        self.assertGreaterEqual(len(context_events(restored)), 2)

    def test_lead_snapshot_exports_context_model(self) -> None:
        session = {
            "company_code": "dev",
            "buyer_name": "Peter",
            "erp_customer_id": "CUST-1",
            "stage": "discover",
            "behavior_class": "returning_customer",
            "last_intent": "find_product",
            "signal_type": "price_objection",
            "signal_emotion": "skeptical",
            "lead_profile": {
                "lead_id": "lead_3",
                "status": "quote_needed",
                "product_interest": "Laptop",
                "quantity": 5,
                "uom": "piece",
                "quote_status": "requested",
                "next_action": "quote_or_clarify_price",
            },
            "lead_timeline": [{"event_type": "quote_requested"}],
        }
        ensure_session_contexts(session)
        create_context(
            session,
            context_type="quote_negotiation",
            lead_profile={
                "lead_id": "lead_3",
                "status": "quote_needed",
                "product_interest": "Laptop",
                "quantity": 5,
                "uom": "piece",
                "quote_status": "requested",
                "next_action": "quote_or_clarify_price",
            },
            activate=True,
        )
        snapshot = lead_snapshot(channel="telegram", uid="789", session=session)
        self.assertEqual(snapshot.get("signal_state", {}).get("type"), "price_objection")
        self.assertEqual(snapshot.get("deal_state", {}).get("product_interest"), "Laptop")
        self.assertEqual(snapshot.get("progress_state", {}).get("quote_status"), "requested")
        self.assertGreaterEqual(len(snapshot.get("contexts", [])), 2)
        self.assertGreaterEqual(snapshot.get("context_event_count", 0), 2)
        self.assertEqual(snapshot.get("domain_event_count"), 1)


if __name__ == "__main__":
    unittest.main()
