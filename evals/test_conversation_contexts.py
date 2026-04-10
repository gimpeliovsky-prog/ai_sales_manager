import unittest

from app.conversation_contexts import create_context, ensure_session_contexts, sync_legacy_to_active_context


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


if __name__ == "__main__":
    unittest.main()
