import unittest

from app.conversation_contexts import (
    active_deal_state,
    active_progress_state,
    active_signal_state,
    context_events,
    create_context,
    ensure_session_contexts,
    mutate_active_lead_profile,
    reconcile_contexts_after_state_update,
    refresh_active_context_state,
    set_active_lead_profile,
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
            "signal_type": "low_signal",
            "signal_confidence": 0.0,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {"status": "none"},
        }
        ensure_session_contexts(session)
        session["stage"] = "clarify"
        session["last_intent"] = "order_detail"
        session["signal_type"] = "deal_progress"
        session["signal_confidence"] = 0.83
        session["signal_preserves_deal"] = True
        session["signal_emotion"] = "neutral"
        session["lead_profile"] = {"product_interest": "monitor", "status": "qualified"}
        sync_legacy_to_active_context(session)
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["stage"], "clarify")
        self.assertEqual(active["last_intent"], "order_detail")
        self.assertEqual(active["lead_profile"]["product_interest"], "monitor")
        self.assertEqual(active_signal_state(session)["type"], "deal_progress")
        self.assertEqual(active_deal_state(session)["product_interest"], "monitor")
        self.assertEqual(active_progress_state(session)["status"], "qualified")

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
        self.assertTrue(any(event.get("event_type") == "context_created" for event in context_events(session)))

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
        session["signal_type"] = "topic_shift"
        session["signal_confidence"] = 0.86
        session["signal_preserves_deal"] = False
        session["signal_emotion"] = "neutral"
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
        self.assertEqual(active_signal_state(session)["type"], "topic_shift")

    def test_price_objection_routes_to_quote_negotiation_context(self) -> None:
        session = {
            "stage": "discover",
            "stage_confidence": 0.81,
            "behavior_class": "price_sensitive",
            "behavior_confidence": 0.84,
            "last_intent": "find_product",
            "last_intent_confidence": 0.72,
            "signal_type": "price_objection",
            "signal_confidence": 0.88,
            "signal_preserves_deal": True,
            "signal_emotion": "skeptical",
            "lead_profile": {
                "status": "qualified",
                "product_interest": "laptop",
                "next_action": "quote_or_clarify_price",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "qualified", "product_interest": "laptop"},
            active_order_name=None,
        )
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["context_type"], "quote_negotiation")
        self.assertEqual(active_deal_state(session)["product_interest"], "laptop")

    def test_service_request_routes_to_service_context(self) -> None:
        session = {
            "stage": "service",
            "stage_confidence": 0.95,
            "behavior_class": "service_request",
            "behavior_confidence": 0.93,
            "last_intent": "service_request",
            "last_intent_confidence": 0.94,
            "signal_type": "service_request",
            "signal_confidence": 0.95,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {
                "status": "service",
                "target_order_id": "SO-999",
            },
        }
        ensure_session_contexts(session)
        reconcile_contexts_after_state_update(
            session,
            previous_lead_profile={"status": "order_created", "target_order_id": "SO-999"},
            active_order_name="SO-999",
        )
        active = session["contexts"][session["active_context_id"]]
        self.assertEqual(active["context_type"], "service_request")
        self.assertEqual(active["related_order_id"], "SO-999")

    def test_set_active_lead_profile_updates_context_layers_immediately(self) -> None:
        session = {
            "stage": "discover",
            "stage_confidence": 0.84,
            "behavior_class": "explorer",
            "behavior_confidence": 0.73,
            "last_intent": "browse_catalog",
            "last_intent_confidence": 0.79,
            "signal_type": "deal_progress",
            "signal_confidence": 0.8,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {"status": "new_lead", "product_interest": "laptop"},
        }
        ensure_session_contexts(session)
        set_active_lead_profile(
            session,
            {"status": "qualified", "product_interest": "monitor", "quantity": 3},
            event_type="lead_profile_mutated",
            event_payload={"source": "test"},
        )
        self.assertEqual(session["lead_profile"]["product_interest"], "monitor")
        self.assertEqual(active_deal_state(session)["product_interest"], "monitor")
        self.assertEqual(active_deal_state(session)["quantity"], 3)
        self.assertEqual(active_progress_state(session)["status"], "qualified")
        self.assertTrue(any(event.get("event_type") == "lead_profile_mutated" for event in context_events(session)))

    def test_refresh_active_context_state_captures_stage_and_signal_changes(self) -> None:
        session = {
            "stage": "discover",
            "stage_confidence": 0.7,
            "behavior_class": "explorer",
            "behavior_confidence": 0.65,
            "last_intent": "browse_catalog",
            "last_intent_confidence": 0.75,
            "signal_type": "deal_progress",
            "signal_confidence": 0.7,
            "signal_preserves_deal": True,
            "signal_emotion": "neutral",
            "lead_profile": {"status": "new_lead", "product_interest": "laptop"},
        }
        ensure_session_contexts(session)
        session["stage"] = "service"
        session["signal_type"] = "service_request"
        session["signal_emotion"] = "impatient"
        refresh_active_context_state(
            session,
            event_type="context_state_refreshed",
            event_payload={"source": "test"},
        )
        self.assertEqual(active_progress_state(session)["stage"], "service")
        self.assertEqual(active_signal_state(session)["type"], "service_request")
        self.assertEqual(active_signal_state(session)["emotion"], "impatient")
        self.assertTrue(any(event.get("event_type") == "context_state_refreshed" for event in context_events(session)))

    def test_mutate_active_lead_profile_keeps_context_in_sync(self) -> None:
        session = {
            "stage": "discover",
            "behavior_class": "explorer",
            "last_intent": "browse_catalog",
            "lead_profile": {"status": "new_lead", "product_interest": "laptop", "quote_status": "none"},
        }
        ensure_session_contexts(session)
        mutate_active_lead_profile(
            session,
            lambda profile: {**profile, "quote_status": "requested", "next_action": "quote_or_clarify_price"},
        )
        self.assertEqual(session["lead_profile"]["quote_status"], "requested")
        self.assertEqual(active_progress_state(session)["quote_status"], "requested")
        self.assertEqual(active_progress_state(session)["next_action"], "quote_or_clarify_price")


if __name__ == "__main__":
    unittest.main()
