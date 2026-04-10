import unittest

from app.conversation_flow import classify_intent, classify_signal, classify_stage, derive_conversation_state
from app.inbound_policy import should_block_for_intro_before_assistance, should_request_intro_before_next_step


class InboundPolicyTests(unittest.TestCase):
    def test_unknown_buyer_with_product_request_is_not_blocked_before_assistance(self) -> None:
        self.assertFalse(
            should_block_for_intro_before_assistance(
                needs_intro=True,
                customer_identified=False,
                intent="find_product",
                lead_profile={"product_interest": "laptop"},
            )
        )

    def test_unknown_buyer_with_low_signal_and_no_product_is_blocked_for_intro(self) -> None:
        self.assertTrue(
            should_block_for_intro_before_assistance(
                needs_intro=True,
                customer_identified=False,
                intent="low_signal",
                lead_profile={},
            )
        )

    def test_unknown_buyer_small_talk_is_not_forced_into_intro(self) -> None:
        self.assertFalse(
            should_block_for_intro_before_assistance(
                needs_intro=True,
                customer_identified=False,
                intent="small_talk",
                lead_profile={},
            )
        )

    def test_unknown_buyer_requests_intro_only_when_next_action_is_ask_contact(self) -> None:
        self.assertTrue(
            should_request_intro_before_next_step(
                needs_intro=True,
                customer_identified=False,
                lead_profile={"next_action": "ask_contact"},
            )
        )
        self.assertFalse(
            should_request_intro_before_next_step(
                needs_intro=True,
                customer_identified=False,
                lead_profile={"next_action": "ask_quantity"},
            )
        )

    def test_stage_for_unknown_buyer_with_product_context_is_discover_not_lead_capture(self) -> None:
        stage, _ = classify_stage(
            session={},
            intent="find_product",
            customer_identified=False,
            needs_intro=True,
            active_order_name=None,
            lead_profile={
                "status": "new_lead",
                "next_action": "show_matching_options",
                "product_interest": "laptop",
                "product_resolution_status": "broad",
            },
        )
        self.assertEqual(stage, "discover")

    def test_stage_for_small_talk_is_new(self) -> None:
        stage, _ = classify_stage(
            session={},
            intent="small_talk",
            customer_identified=False,
            needs_intro=True,
            active_order_name=None,
            lead_profile={"status": "none", "next_action": "ask_need"},
        )
        self.assertEqual(stage, "new")

    def test_small_talk_intent_is_detected_for_social_check_in(self) -> None:
        intent, confidence = classify_intent("hello how are you")
        self.assertEqual(intent, "small_talk")
        self.assertGreaterEqual(confidence, 0.9)

    def test_unknown_social_text_never_becomes_product_intent(self) -> None:
        intent, _ = classify_intent("how are you going today")
        self.assertIn(intent, {"small_talk", "low_signal"})

    def test_bare_product_phrase_still_classifies_as_find_product(self) -> None:
        intent, confidence = classify_intent("laptop")
        self.assertEqual(intent, "find_product")
        self.assertGreaterEqual(confidence, 0.6)

    def test_small_talk_does_not_reset_existing_order_stage(self) -> None:
        stage, _ = classify_stage(
            session={"stage": "invoice"},
            intent="small_talk",
            signal_type="small_talk",
            customer_identified=True,
            needs_intro=False,
            active_order_name="SO-1",
            lead_profile={"status": "order_created", "next_action": "send_order_or_offer_invoice"},
        )
        self.assertEqual(stage, "invoice")

    def test_price_objection_signal_preserves_deal(self) -> None:
        signal_type, confidence, preserves_deal, emotion = classify_signal(
            session={"stage": "discover"},
            user_text="too expensive",
            intent="find_product",
            behavior_class="price_sensitive",
            active_order_name=None,
            lead_profile={"product_interest": "laptop", "next_action": "quote_or_clarify_price"},
            previous_lead_profile={"product_interest": "laptop"},
        )
        self.assertEqual(signal_type, "price_objection")
        self.assertTrue(preserves_deal)
        self.assertEqual(emotion, "skeptical")
        self.assertGreaterEqual(confidence, 0.8)

    def test_explicit_signal_classifier_output_overrides_fallback_signal_logic(self) -> None:
        state = derive_conversation_state(
            session={"stage": "discover", "last_intent": "find_product"},
            user_text="מה נשמע",
            channel="telegram",
            needs_intro=False,
            customer_identified=True,
            active_order_name=None,
            lead_profile={"status": "new_lead", "next_action": "ask_need"},
            previous_lead_profile={"status": "new_lead"},
            behavior_class="returning_customer",
            behavior_confidence=0.7,
            intent="find_product",
            intent_confidence=0.6,
            signal_type="small_talk",
            signal_confidence=0.83,
            signal_preserves_deal=True,
            signal_emotion="positive",
        )
        self.assertEqual(state["signal_type"], "small_talk")
        self.assertEqual(state["signal_emotion"], "positive")
        self.assertTrue(state["signal_preserves_deal"])

    def test_price_sensitive_behavior_can_still_fallback_to_price_objection_without_phrase_list(self) -> None:
        signal_type, confidence, preserves_deal, emotion = classify_signal(
            session={"stage": "discover"},
            user_text="irrelevant wording",
            intent="find_product",
            behavior_class="price_sensitive",
            active_order_name=None,
            lead_profile={"product_interest": "laptop", "catalog_item_name": "Laptop"},
            previous_lead_profile={"product_interest": "laptop"},
        )
        self.assertEqual(signal_type, "price_objection")
        self.assertGreaterEqual(confidence, 0.8)
        self.assertTrue(preserves_deal)
        self.assertEqual(emotion, "skeptical")


if __name__ == "__main__":
    unittest.main()
