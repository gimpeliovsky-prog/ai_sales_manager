from __future__ import annotations

import unittest

from app.conversation_flow import (
    behavior_from_signal_classifier,
    fallback_intent_can_override_llm,
    intent_from_signal_classifier,
    llm_signal_soft_override_types,
)


class AgentSignalMappingTests(unittest.TestCase):
    def test_small_talk_maps_to_returning_customer_for_known_buyer(self) -> None:
        self.assertEqual(
            behavior_from_signal_classifier(
                signal_type="small_talk",
                current_behavior_class="unclear_request",
                customer_identified=True,
            ),
            "returning_customer",
        )

    def test_small_talk_maps_to_low_signal_for_unknown_buyer(self) -> None:
        self.assertEqual(
            behavior_from_signal_classifier(
                signal_type="small_talk",
                current_behavior_class="unclear_request",
                customer_identified=False,
            ),
            "silent_or_low_signal",
        )

    def test_price_objection_maps_to_price_sensitive_behavior(self) -> None:
        self.assertEqual(
            behavior_from_signal_classifier(
                signal_type="price_objection",
                current_behavior_class="unclear_request",
                customer_identified=True,
            ),
            "price_sensitive",
        )

    def test_service_signal_maps_to_service_request_intent(self) -> None:
        self.assertEqual(
            intent_from_signal_classifier(
                signal_type="delivery_question",
                current_intent="find_product",
            ),
            "service_request",
        )

    def test_confirmation_signal_maps_to_confirm_order_intent(self) -> None:
        self.assertEqual(
            intent_from_signal_classifier(
                signal_type="confirmation",
                current_intent="low_signal",
            ),
            "confirm_order",
        )

    def test_service_and_handoff_signals_are_soft_override_types(self) -> None:
        soft_override_types = llm_signal_soft_override_types()
        self.assertIn("service_request", soft_override_types)
        self.assertIn("delivery_question", soft_override_types)
        self.assertIn("handoff_request", soft_override_types)

    def test_only_hard_commercial_fallback_intents_can_override_llm(self) -> None:
        self.assertTrue(fallback_intent_can_override_llm("find_product"))
        self.assertTrue(fallback_intent_can_override_llm("confirm_order"))
        self.assertFalse(fallback_intent_can_override_llm("service_request"))
        self.assertFalse(fallback_intent_can_override_llm("human_handoff"))


if __name__ == "__main__":
    unittest.main()
