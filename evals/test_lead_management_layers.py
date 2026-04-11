import unittest

from app.lead_management import (
    apply_llm_lead_patch,
    apply_lead_state_layers,
    build_handoff_summary,
    build_lead_event_payload,
    update_lead_profile_from_message,
    update_lead_profile_from_tool,
)


class LeadManagementLayersTests(unittest.TestCase):
    def test_apply_lead_state_layers_merges_deal_and_progress(self) -> None:
        profile = apply_lead_state_layers(
            current_profile={"status": "none"},
            deal_state={"product_interest": "laptop", "quantity": 2, "uom": "Nos"},
            progress_state={"status": "qualified", "next_action": "quote_or_clarify_price"},
        )
        self.assertEqual(profile["product_interest"], "laptop")
        self.assertEqual(profile["quantity"], 2)
        self.assertEqual(profile["status"], "qualified")
        self.assertEqual(profile["next_action"], "quote_or_clarify_price")

    def test_build_lead_event_payload_includes_layer_exports(self) -> None:
        session = {
            "stage": "discover",
            "behavior_class": "explorer",
            "last_intent": "browse_catalog",
            "lead_profile": {
                "status": "qualified",
                "product_interest": "monitor",
                "quantity": 5,
                "uom": "Nos",
                "quote_status": "requested",
                "next_action": "quote_or_clarify_price",
            },
        }
        payload = build_lead_event_payload(session=session)
        self.assertEqual(payload["deal_state"]["product_interest"], "monitor")
        self.assertEqual(payload["deal_state"]["quantity"], 5)
        self.assertEqual(payload["progress_state"]["status"], "qualified")
        self.assertEqual(payload["progress_state"]["quote_status"], "requested")

    def test_build_handoff_summary_includes_layer_exports(self) -> None:
        session = {
            "buyer_name": "Peter",
            "lead_profile": {
                "status": "quote_needed",
                "product_interest": "Laptop",
                "quantity": 3,
                "uom": "Nos",
                "quote_status": "requested",
            },
        }
        summary = build_handoff_summary(session, reason="manager_attention_required")
        self.assertEqual(summary["deal_state"]["product_interest"], "Laptop")
        self.assertEqual(summary["progress_state"]["quote_status"], "requested")

    def test_message_update_still_recomputes_progress(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={"status": "none"},
            user_text="need 5 laptops",
            stage="discover",
            behavior_class="direct_buyer",
            intent="find_product",
            customer_identified=False,
            active_order_name=None,
        )
        self.assertEqual(profile["product_interest"], "laptops")
        self.assertEqual(profile["quantity"], 5)
        self.assertEqual(profile["next_action"], "select_specific_item")

    def test_tool_update_still_recomputes_progress(self) -> None:
        profile = update_lead_profile_from_tool(
            current_profile={
                "status": "qualified",
                "product_interest": "laptop",
                "quantity": 5,
                "uom": "Nos",
                "quote_status": "requested",
            },
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU002", "qty": 5, "uom": "Nos"}]},
            tool_result={"name": "SAL-ORD-2026-00025", "grand_total": 21500, "currency": "ILS"},
            stage="confirm",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertEqual(profile["status"], "order_created")
        self.assertEqual(profile["target_order_id"], "SAL-ORD-2026-00025")
        self.assertEqual(profile["quote_status"], "accepted")

    def test_tool_update_authoritatively_reconciles_order_created_state(self) -> None:
        profile = update_lead_profile_from_tool(
            current_profile={
                "status": "new_lead",
                "product_interest": "It seems I have already have open ride check it",
                "catalog_item_name": "Laptop",
                "need": "Laptop",
                "quantity": 10,
                "uom": "piece",
                "missing_slots": ["delivery_need", "confirmation"],
                "next_action": "ask_delivery_timing",
                "qualification_priority": "timing_or_delivery",
                "separate_order_requested": True,
                "order_correction_status": "requested",
            },
            tool_name="create_sales_order",
            inputs={"items": [{"item_code": "SKU002", "qty": 10, "uom": "pcs"}]},
            tool_result={"name": "SAL-ORD-2026-00026", "grand_total": 8000, "currency": "ILS"},
            stage="confirm",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertEqual(profile["status"], "order_created")
        self.assertEqual(profile["target_order_id"], "SAL-ORD-2026-00026")
        self.assertEqual(profile["product_interest"], "Laptop")
        self.assertEqual(profile["need"], "Laptop")
        self.assertEqual(profile["missing_slots"], [])
        self.assertEqual(profile["next_action"], "send_order_or_offer_invoice")
        self.assertEqual(profile["qualification_priority"], "next_best_action")
        self.assertFalse(profile["separate_order_requested"])
        self.assertEqual(profile["order_correction_status"], "none")
        self.assertEqual(profile["uom"], "piece")

    def test_small_talk_message_does_not_seed_product_interest(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={"status": "none"},
            user_text="hello how are you",
            stage="discover",
            behavior_class="returning_customer",
            intent="find_product",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertIsNone(profile.get("product_interest"))
        self.assertIsNone(profile.get("need"))

    def test_llm_patch_rejects_small_talk_as_product_interest(self) -> None:
        profile = apply_llm_lead_patch(
            current_profile={"status": "none"},
            patch={"product_interest": "how are you"},
            intent="find_product",
        )
        self.assertIsNone(profile.get("product_interest"))
        self.assertIsNone(profile.get("need"))

    def test_low_signal_social_phrase_does_not_update_product_slots(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={"status": "none"},
            user_text="how are you going today",
            stage="discover",
            behavior_class="returning_customer",
            intent="find_product",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertIsNone(profile.get("product_interest"))
        self.assertIsNone(profile.get("need"))
        self.assertEqual(profile.get("next_action"), "ask_need")

    def test_missing_slots_are_soft_priority_not_hard_contract(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={"status": "new_lead", "product_interest": "laptop"},
            user_text="thanks",
            stage="discover",
            behavior_class="returning_customer",
            intent="low_signal",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertEqual(profile.get("missing_slots"), ["specific_item", "quantity", "uom"])
        self.assertEqual(profile.get("next_action"), "show_matching_options")

    def test_random_number_without_product_evidence_does_not_become_quantity(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={"status": "none"},
            user_text="513320556",
            stage="discover",
            behavior_class="silent_or_low_signal",
            intent="order_detail",
            customer_identified=False,
            active_order_name=None,
        )
        self.assertIsNone(profile.get("quantity"))
        self.assertEqual(profile.get("next_action"), "ask_need")

    def test_clean_product_anchor_replaces_dirty_hedged_anchor(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={
                "status": "new_lead",
                "product_interest": "maybe laptop",
                "need": "maybe laptop",
                "product_resolution_status": "broad",
            },
            user_text="laptop",
            stage="discover",
            behavior_class="explorer",
            intent="find_product",
            customer_identified=True,
            active_order_name=None,
        )
        self.assertEqual(profile.get("product_interest"), "laptop")
        self.assertEqual(profile.get("need"), "laptop")

    def test_llm_service_target_resolves_current_order_without_phrase_list(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={
                "status": "order_created",
                "target_order_id": "SO-1",
            },
            user_text="please send it",
            stage="service",
            behavior_class="service_request",
            intent="service_request",
            customer_identified=True,
            active_order_name="SO-1",
            llm_state_update={
                "service_request_target": "sales_order_pdf",
                "order_target_reference": "current_order",
            },
        )
        self.assertEqual(profile.get("service_request_target"), "sales_order_pdf")
        self.assertEqual(profile.get("target_order_id"), "SO-1")

    def test_llm_order_correction_target_resolves_current_order_without_regex_phrase(self) -> None:
        profile = update_lead_profile_from_message(
            current_profile={
                "status": "order_created",
                "target_order_id": "SO-1",
            },
            user_text="please fix it there",
            stage="service",
            behavior_class="service_request",
            intent="service_request",
            customer_identified=True,
            active_order_name="SO-1",
            llm_state_update={
                "service_request_target": "order_correction",
                "order_target_reference": "current_order",
                "order_correction_type": "quantity",
                "correction_target_text": "Laptop qty 10",
            },
        )
        self.assertEqual(profile.get("target_order_id"), "SO-1")
        self.assertEqual(profile.get("order_correction_status"), "requested")
        self.assertEqual(profile.get("correction_type"), "quantity")
        self.assertEqual(profile.get("correction_target_text"), "Laptop qty 10")


if __name__ == "__main__":
    unittest.main()
