import unittest

from app.conversation_flow import classify_stage
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


if __name__ == "__main__":
    unittest.main()
