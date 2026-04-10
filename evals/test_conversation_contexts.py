import unittest

from app.conversation_contexts import (
    create_context,
    ensure_session_contexts,
    reconcile_contexts_after_state_update,
    sync_legacy_to_active_context,
)


class ConversationContextsTests(unittest.TestCase):
    def test_legacy_session_is_wrapped_into_default_active_context(self) -> None:
        session = {
            "stage": "discover",
            "stage_confidence": 0.82,
            "behavior_class": "explorer",
            "behavior_confidence": 0.77,
            "last_intent": "browse_catalog",
            "last_intent_confidence": 0.81,
            "lead_profile": {"product_interest": "laptop", "status": "new_lead"},
        }
        ensure_session_contexts(session)
        self.assertIn("active_context_id", session)
        self.assertIn("contexts", session)
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["stage"], "discover")
        self.assertEqual(active["behavior_class"], "explorer")
        self.assertEqual(active["last_intent"], "browse_catalog")
        self.assertEqual(active["lead_profile"]["product_interest"], "laptop")

    def test_active_context_receives_legacy_updates_before_save(self) -> None:
        session = {
            "stage": "new",
            "stage_confidence": 0.0,
            "behavior_class": "unclear_request",
            "behavior_confidence": 0.0,
            "last_intent": None,
            "last_intent_confidence": 0.0,
            "lead_profile": {"status": "none"},
        }
        ensure_session_contexts(session)
        session["stage"] = "clarify"
        session["last_intent"] = "order_detail"
        session["lead_profile"] = {"product_interest": "monitor", "status": "qualified"}
        sync_legacy_to_active_context(session)
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["stage"], "clarify")
        self.assertEqual(active["last_intent"], "order_detail")
        self.assertEqual(active["lead_profile"]["product_interest"], "monitor")

    def test_create_context_can_switch_active_branch(self) -> None:
        session = {
            "stage": "discover",
            "lead_profile": {"product_interest": "laptop", "status": "new_lead"},
        }
        ensure_session_contexts(session)
        original_context_id = session["active_context_id"]
        new_context = create_context(
            session,
            context_type="order_edit",
            related_order_id="SO-001",
            lead_profile={"target_order_id": "SO-001", "order_correction_status": "requested"},
        )
        self.assertNotEqual(original_context_id, new_context["context_id"])
        self.assertEqual(session["active_context_id"], new_context["context_id"])
        self.assertEqual(session["lead_profile"]["target_order_id"], "SO-001")

    def test_order_correction_creates_order_edit_context(self) -> None:
        session = {
            "stage": "order_build",
            "stage_confidence": 0.9,
            "behavior_class": "direct_buyer",
            "behavior_confidence": 0.8,
            "last_intent": "add_to_order",
            "last_intent_confidence": 0.9,
            "lead_profile": {
                "status": "qualified",
                "product_interest": "laptop",
                "order_correction_status": "requested",
                "target_order_id": "SO-777",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "qualified", "product_interest": "laptop"},
            active_order_name="SO-777",
        )
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["context_type"], "order_edit")
        self.assertEqual(active["related_order_id"], "SO-777")
        self.assertEqual(active["lead_profile"]["order_correction_status"], "requested")

    def test_new_product_from_order_edit_opens_purchase_context(self) -> None:
        session = {
            "stage": "discover",
            "stage_confidence": 0.88,
            "behavior_class": "explorer",
            "behavior_confidence": 0.76,
            "last_intent": "browse_catalog",
            "last_intent_confidence": 0.82,
            "lead_profile": {
                "status": "new_lead",
                "product_interest": "monitor",
                "order_correction_status": "requested",
                "target_order_id": "SO-888",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "qualified", "product_interest": "laptop"},
            active_order_name="SO-888",
        )
        order_edit_context_id = session["active_context_id"]
        self.assertEqual(session["contexts"][order_edit_context_id]["context_type"], "order_edit")

        session["last_intent"] = "find_product"
        session["lead_profile"] = {
            "status": "new_lead",
            "product_interest": "monitor",
            "order_correction_status": "none",
            "target_order_id": "SO-888",
        }
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "qualified", "product_interest": "laptop", "order_correction_status": "requested"},
            active_order_name="SO-888",
        )
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["context_type"], "new_purchase")
        self.assertEqual(active["lead_profile"]["product_interest"], "monitor")
        self.assertEqual(active["lead_profile"]["order_correction_status"], "none")


if __name__ == "__main__":
    unittest.main()
