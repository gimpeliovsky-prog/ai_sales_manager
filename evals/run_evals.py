from __future__ import annotations

import ast
import json
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.conversation_flow import derive_conversation_state, get_handoff_message  # noqa: E402
from app.catalog_localization import catalog_lang, localize_catalog_result  # noqa: E402
from app.i18n import text as i18n_text  # noqa: E402
from app.interaction_patterns import has_explicit_confirmation  # noqa: E402
from app.language_policy import resolve_conversation_language  # noqa: E402
from app.lead_management import (  # noqa: E402
    apply_llm_lead_patch,
    build_handoff_summary,
    can_send_followup,
    apply_sales_owner_action,
    apply_manual_close,
    apply_quote_update,
    apply_lead_merge,
    apply_order_correction_update,
    record_merged_duplicate,
    ensure_lead_identity,
    mark_stalled_if_needed,
    mark_lost_if_followup_exhausted,
    normalize_telegram_username,
    sales_alert_event_types,
    sales_event_type,
    update_lead_profile_from_message,
    update_lead_profile_from_tool,
    update_lead_profile_source,
)
from app.llm_state_updater import parse_llm_state_update  # noqa: E402
from app.outbound_channels import build_followup_message, build_sales_owner_message, mark_followup_attempt  # noqa: E402
from app.prompt_registry import build_runtime_system_prompt  # noqa: E402
from app.runtime_catalog_context import build_catalog_prefetch_context, should_prefetch_catalog_options  # noqa: E402
from app.sales_governance import evaluate_sla_breaches, record_new_sla_breaches  # noqa: E402
from app.sales_crm_sync import build_sales_crm_outbox_event  # noqa: E402
from app.sales_dedupe import detect_duplicate_lead  # noqa: E402
from app.sales_lead_repository import compact_lead_record  # noqa: E402
from app.sales_policy import earliest_delivery_date, minimum_order_violation, normalize_order_state, price_anchor_status, remove_price_fields, sales_policy, should_hide_catalog_prices  # noqa: E402
from app.sales_quality import evaluate_conversation_quality  # noqa: E402
from app.sales_reporting import crm_export_contract, dashboard_contract, filter_leads, lead_snapshot, paginate_leads, summarize_leads, summarize_manager_performance, summarize_source_funnel, summarize_time_funnel  # noqa: E402
from app.sales_timeline import append_lead_timeline_event  # noqa: E402
from app.tool_policy import evaluate_tool_call  # noqa: E402
from app.uom_semantics import localize_uom_label, resolve_catalog_uom  # noqa: E402


def _load_cases(filename: str) -> list[dict[str, Any]]:
    return json.loads((Path(__file__).resolve().parent / filename).read_text(encoding="utf-8"))


def _assert_subset(actual: dict[str, Any], expected: dict[str, Any], case_name: str) -> list[str]:
    failures: list[str] = []
    for key, expected_value in expected.items():
        actual_value = actual.get(key)
        if actual_value != expected_value:
            failures.append(
                f"{case_name}: expected {key}={expected_value!r}, got {actual_value!r}"
            )
    return failures


def run_conversation_flow_evals() -> list[str]:
    failures: list[str] = []
    for case in _load_cases("conversation_flow_cases.json"):
        actual = derive_conversation_state(
            session=case["session"],
            user_text=case["user_text"],
            channel=case["channel"],
            needs_intro=case["needs_intro"],
            customer_identified=case["customer_identified"],
            active_order_name=case["active_order_name"],
            ai_policy=case.get("ai_policy"),
            lead_profile=case.get("lead_profile"),
            previous_lead_profile=case.get("previous_lead_profile"),
            behavior_class=case.get("behavior_class"),
            behavior_confidence=case.get("behavior_confidence"),
            intent=case.get("intent"),
            intent_confidence=case.get("intent_confidence"),
        )
        failures.extend(_assert_subset(actual, case["expected"], case["name"]))
    return failures


def run_tool_policy_evals() -> list[str]:
    failures: list[str] = []
    for case in _load_cases("tool_policy_cases.json"):
        actual = evaluate_tool_call(
            tool_name=case["tool_name"],
            inputs=case["inputs"],
            session=case["session"],
            tenant=case["tenant"],
            user_text=case["user_text"],
        )
        blocked = actual is not None
        expected = case["expected"]
        if blocked != expected["blocked"]:
            failures.append(f"{case['name']}: expected blocked={expected['blocked']!r}, got {blocked!r}")
            continue
        if expected["blocked"]:
            error_text = str((actual or {}).get("error") or "")
            expected_fragment = str(expected.get("error_contains") or "")
            if expected_fragment and expected_fragment not in error_text:
                failures.append(
                    f"{case['name']}: expected error containing {expected_fragment!r}, got {error_text!r}"
                )
    llm_confirmed = evaluate_tool_call(
        tool_name="create_sales_order",
        inputs={"items": [{"item_code": "ITEM-001", "qty": 2}]},
        session={"stage": "confirm", "erp_customer_id": "CUST-0001", "last_intent": "confirm_order"},
        tenant={"ai_policy": {"allowed_tools": ["create_sales_order"]}},
        user_text="יאללה",
        confirmation_override=True,
    )
    if llm_confirmed is not None:
        failures.append(f"tool_policy_confirmation_override_allows_order: got {llm_confirmed!r}")
    llm_denied = evaluate_tool_call(
        tool_name="create_sales_order",
        inputs={"items": [{"item_code": "ITEM-001", "qty": 2}]},
        session={"stage": "confirm", "erp_customer_id": "CUST-0001", "last_intent": "confirm_order"},
        tenant={"ai_policy": {"allowed_tools": ["create_sales_order"]}},
        user_text="ok",
        confirmation_override=False,
    )
    if llm_denied is None or not llm_denied.get("blocked_by_policy"):
        failures.append(f"tool_policy_confirmation_override_denies_order: got {llm_denied!r}")
    llm_not_confirmed = evaluate_tool_call(
        tool_name="create_sales_order",
        inputs={"items": [{"item_code": "ITEM-001", "qty": 2}]},
        session={"stage": "confirm", "erp_customer_id": "CUST-0001", "last_intent": "confirm_order"},
        tenant={"ai_policy": {"allowed_tools": ["create_sales_order"]}},
        user_text="כמה זה עולה?",
        confirmation_override=False,
    )
    if llm_not_confirmed is None:
        failures.append("tool_policy_confirmation_override_false_still_blocks_order: expected block")
    for phrase in ["סבבה", "בסדר", "אוקי", "אוקיי", "טוב", "מעולה", "סגור", "סגרנו", "יאללה"]:
        if not has_explicit_confirmation(phrase):
            failures.append(f"hebrew_confirmation_phrase_detected:{phrase}: expected True")
    return failures


def run_tool_schema_evals() -> list[str]:
    failures: list[str] = []
    cyrillic_re = re.compile(r"[А-Яа-яЁё]")
    tools_source = (ROOT / "app" / "tools.py").read_text(encoding="utf-8")
    parsed = ast.parse(tools_source)
    tools: list[dict[str, Any]] = []
    for node in parsed.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "TOOLS":
            tools = ast.literal_eval(node.value)
            break
    if not tools:
        failures.append("tool_schema_tools_constant: expected TOOLS constant to be parseable")
        return failures
    for tool in tools:
        description = str(tool.get("description") or "")
        if cyrillic_re.search(description):
            failures.append(f"tool_schema_description_english:{tool.get('name')}: got Cyrillic in {description!r}")
    return failures


def run_prompt_override_evals() -> list[str]:
    failures: list[str] = []
    tenant = {
        "company_name": "ACME",
        "company_code": "acme",
        "ai_policy": {
            "prompt_overrides": {
                "stage_prompts": {
                    "discover": ["Prefer premium positioning for this tenant."]
                },
                "sales_playbook": [
                    "Use the tenant-specific discovery question before recommending an item."
                ],
                "handoff_messages": {
                    "ru": "Передаю живому менеджеру."
                },
            }
        },
    }
    prompt = build_runtime_system_prompt(
        tenant=tenant,
        lang="ru",
        channel="telegram",
        stage="discover",
        behavior_class="explorer",
    )
    if "Prefer premium positioning for this tenant." not in prompt:
        failures.append("prompt_override_stage_append: expected custom stage prompt to appear in system prompt")
    if "Use the tenant-specific discovery question before recommending an item." not in prompt:
        failures.append("prompt_override_sales_playbook_append: expected custom sales playbook prompt to appear in system prompt")
    guarded_prompt = build_runtime_system_prompt(
        tenant={"company_name": "ACME", "company_code": "acme"},
        lang="en",
        channel="telegram",
        stage="discover",
        behavior_class="explorer",
        lead_profile={
            "product_interest": "backpack",
            "product_resolution_status": "broad",
            "quantity": 10,
            "uom": "piece",
            "next_action": "show_matching_options",
            "requested_items_need_uom_confirmation": False,
        },
    )
    if "Do not ask for unit or package again" not in guarded_prompt:
        failures.append("prompt_state_guard_known_uom: expected known-uom guard in prompt")
    if "The next step is to show matching catalog options" not in guarded_prompt:
        failures.append("prompt_state_guard_show_matching_options: expected option-selection guard in prompt")

    handoff_message = get_handoff_message("ru", ai_policy=tenant["ai_policy"])
    if handoff_message != "Передаю живому менеджеру.":
        failures.append(
            f"handoff_message_override: expected custom handoff message, got {handoff_message!r}"
        )
    return failures


def run_language_lock_evals() -> list[str]:
    failures: list[str] = []
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=None,
        user_text="שלום, צריך הצעת מחיר",
        default_lang="ru",
    )
    if current_lang != "he" or lang_to_lock != "he":
        failures.append(
            f"language_lock_first_message: expected ('he', 'he'), got ({current_lang!r}, {lang_to_lock!r})"
        )

    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang="he",
        user_text="price please",
        default_lang="ru",
    )
    if current_lang != "he" or lang_to_lock is not None:
        failures.append(
            f"language_lock_persists: expected ('he', None), got ({current_lang!r}, {lang_to_lock!r})"
        )

    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=None,
        user_text="+972541234567",
        default_lang="ru",
    )
    if current_lang != "ru" or lang_to_lock is not None:
        failures.append(
            f"language_lock_waits_for_signal: expected ('ru', None), got ({current_lang!r}, {lang_to_lock!r})"
        )
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=None,
        user_text="precio por favor",
        default_lang="ru",
    )
    if current_lang != "auto" or lang_to_lock != "auto":
        failures.append(
            f"language_lock_latin_unknown_uses_auto: expected ('auto', 'auto'), got ({current_lang!r}, {lang_to_lock!r})"
        )
    return failures


def run_i18n_evals() -> list[str]:
    failures: list[str] = []
    tenant_policy = {
        "i18n": {
            "translations": {
                "pt": {
                    "catalog.sold_in": "Este produto é vendido em: {options}."
                }
            }
        }
    }
    rendered = i18n_text("catalog.sold_in", "pt-BR", {"options": "caixas"}, ai_policy=tenant_policy)
    if rendered != "Este produto é vendido em: caixas.":
        failures.append(f"i18n_tenant_override_any_language: got {rendered!r}")
    fallback = i18n_text("catalog.sold_in", "es", {"options": "cajas"})
    if fallback != "This product is sold in: cajas.":
        failures.append(f"i18n_unknown_language_fallback: got {fallback!r}")
    followup_vars = {"product_interest": "coffee", "next_step": "qty"}
    ru_action_fallback = i18n_text("followup.ask_quantity", "ru", followup_vars)
    ru_generic_followup = i18n_text("followup.default", "ru", followup_vars)
    if ru_action_fallback != ru_generic_followup:
        failures.append(
            f"i18n_action_fallback_prefers_localized_generic: got {ru_action_fallback!r}, expected {ru_generic_followup!r}"
        )
    en_action_followup = i18n_text("followup.ask_quantity", "en", followup_vars)
    en_generic_followup = i18n_text("followup.default", "en", followup_vars)
    if en_action_followup == en_generic_followup:
        failures.append("i18n_action_fallback_keeps_english_specialized: expected specialized English follow-up")
    unknown_next_step = i18n_text("next_step.ask_budget", "ru")
    default_next_step = i18n_text("next_step.default", "ru")
    if unknown_next_step != default_next_step:
        failures.append(
            f"i18n_next_step_unknown_action_uses_localized_default: got {unknown_next_step!r}, expected {default_next_step!r}"
        )
    return failures


def run_catalog_localization_evals() -> list[str]:
    failures: list[str] = []
    localized = localize_catalog_result(
        {
            "items": [
                {
                    "item_code": "ITEM-1",
                    "item_name": "Coffee Machine",
                    "item_name_translations": {
                        "ru": "Кофемашина",
                        "he": {"item_name": "מכונת קפה"},
                    },
                    "stock_uom_label": "pcs",
                    "available_uoms": [{"uom": "Box", "display_name": "Box", "is_stock_uom": False}],
                }
            ]
        },
        "ru",
    )
    item = localized["items"][0]
    expected = {
        "item_name": "Кофемашина",
        "display_item_name": "Кофемашина",
        "display_item_name_lang": "ru",
        "display_item_name_source": "requested_translation",
        "missing_requested_item_name_translation": False,
        "canonical_item_name": "Coffee Machine",
    }
    failures.extend(_assert_subset(item, expected, "catalog_localization_requested_translation"))
    if item.get("customer_uom_options") != ["шт.", "коробки"]:
        failures.append(f"catalog_localization_preserves_uom_labels: got {item!r}")

    fallback = localize_catalog_result(
        {
            "items": [
                {
                    "item_code": "ITEM-2",
                    "item_name": "Coffee Grinder",
                    "translations": {
                        "en": {"display_item_name": "Coffee Grinder"},
                        "he": {"display_item_name": "מטחנת קפה"},
                    },
                }
            ]
        },
        "fr",
        {"catalog": {"item_name_fallback_languages": ["he"]}},
    )["items"][0]
    failures.extend(
        _assert_subset(
            fallback,
            {
                "item_name": "מטחנת קפה",
                "display_item_name_lang": "he",
                "display_item_name_source": "fallback_translation",
                "missing_requested_item_name_translation": True,
                "canonical_item_name": "Coffee Grinder",
            },
            "catalog_localization_configured_fallback_translation",
        )
    )

    canonical = localize_catalog_result(
        {"items": [{"item_code": "ITEM-3", "item_name": "Service Plan"}]},
        "es",
    )["items"][0]
    failures.extend(
        _assert_subset(
            canonical,
            {
                "item_name": "Service Plan",
                "display_item_name_source": "canonical_item_name",
                "missing_requested_item_name_translation": True,
            },
            "catalog_localization_canonical_fallback",
        )
    )
    if catalog_lang("auto") is not None or catalog_lang("pt-BR") != "pt":
        failures.append("catalog_lang_normalization: expected auto -> None and pt-BR -> pt")
    return failures


def run_uom_semantics_evals() -> list[str]:
    failures: list[str] = []
    resolved_piece = resolve_catalog_uom(
        "шт",
        [
            {"uom": "pcs", "conversion_factor": 1.0, "is_stock_uom": True},
            {"uom": "Box", "conversion_factor": 12.0, "is_stock_uom": False},
        ],
    )
    failures.extend(
        _assert_subset(
            resolved_piece,
            {"resolved": True, "uom": "pcs", "match_type": "semantic", "canonical_uom": "piece"},
            "uom_semantics_resolves_ru_piece_to_catalog_pcs",
        )
    )
    resolved_box = resolve_catalog_uom(
        "קופסה",
        [
            {"uom": "pcs", "conversion_factor": 1.0, "is_stock_uom": True},
            {"uom": "Box", "conversion_factor": 12.0, "is_stock_uom": False},
        ],
    )
    failures.extend(
        _assert_subset(
            resolved_box,
            {"resolved": True, "uom": "Box", "match_type": "semantic", "canonical_uom": "box"},
            "uom_semantics_resolves_hebrew_box_alias",
        )
    )
    if localize_uom_label("pcs", "ru") != "шт.":
        failures.append(f"uom_semantics_localizes_piece_label_ru: got {localize_uom_label('pcs', 'ru')!r}")
    if localize_uom_label("Box", "he") != "קרטונים":
        failures.append(f"uom_semantics_localizes_box_label_he: got {localize_uom_label('Box', 'he')!r}")
    return failures


def run_lead_management_evals() -> list[str]:
    failures: list[str] = []
    previous_profile = {"status": "none", "score": 0}
    profile = update_lead_profile_from_message(
        current_profile=previous_profile,
        user_text="Need 5 coffee machines asap, price please",
        stage="lead_capture",
        behavior_class="price_sensitive",
        intent="browse_catalog",
        customer_identified=False,
        active_order_name=None,
    )
    expected = {
        "status": "quote_needed",
        "temperature": "warm",
        "next_action": "show_matching_options",
        "followup_strategy": "product_selection_missing",
        "quantity": 5.0,
        "price_sensitivity": True,
        "urgency": "soon",
    }
    failures.extend(_assert_subset(profile, expected, "lead_profile_hot_inbound"))
    if profile.get("product_interest") != "coffee machines":
        failures.append(f"lead_profile_hot_inbound_product_interest_cleaned: got {profile.get('product_interest')!r}")
    if not profile.get("created_at") or not profile.get("quote_needed_at"):
        failures.append(f"lead_lifecycle_timestamps_set: got {profile!r}")
    event_type = sales_event_type(previous_profile, profile)
    if event_type != "quote_requested":
        failures.append(f"lead_event_quote_requested: expected quote_requested, got {event_type!r}")

    missing_product_profile = update_lead_profile_from_message(
        current_profile={"status": "none", "score": 0},
        user_text="need help",
        stage="clarify",
        behavior_class="unclear_request",
        intent="low_signal",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            missing_product_profile,
            {"qualification_priority": "product_need", "next_action": "ask_need"},
            "qualification_priority_product_first",
        )
    )
    missing_quantity_profile = update_lead_profile_from_message(
        current_profile={"status": "new_lead", "product_interest": "coffee machine"},
        user_text="coffee machine",
        stage="clarify",
        behavior_class="explorer",
        intent="find_product",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            missing_quantity_profile,
            {"qualification_priority": "specific_item_selection", "next_action": "select_specific_item"},
            "qualification_priority_specific_item_before_quantity_for_broad_product",
        )
    )
    missing_unit_profile = update_lead_profile_from_message(
        current_profile={"status": "qualified", "product_interest": "coffee machine", "quantity": 2},
        user_text="2 coffee machines",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            missing_unit_profile,
            {"qualification_priority": "specific_item_selection", "next_action": "select_specific_item"},
            "qualification_priority_specific_item_before_unit_for_broad_product",
        )
    )
    multi_item_profile = update_lead_profile_from_message(
        current_profile={"status": "none", "score": 0},
        user_text="колбаса петровская 4, салями бобруйская 7, докторская 2",
        stage="lead_capture",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            multi_item_profile,
            {
                "requested_item_count": 3,
                "requested_items_have_quantities": True,
                "requested_items_need_uom_confirmation": True,
                "requested_items_assumed_uom": "box",
                "requested_items_uom_assumption_status": "likely",
                "qualification_priority": "unit_or_variant",
                "next_action": "ask_unit",
                "followup_strategy": "uom_confirmation",
            },
            "multi_item_order_list_treats_box_as_likely_but_unconfirmed",
        )
    )
    requested_items = multi_item_profile.get("requested_items")
    if not isinstance(requested_items, list) or len(requested_items) != 3 or requested_items[0].get("qty") != 4.0:
        failures.append(f"multi_item_order_list_extracts_items: got {multi_item_profile!r}")
    premature_order_policy = evaluate_tool_call(
        tool_name="create_sales_order",
        inputs={"items": [{"item_code": "ITEM-1", "qty": 4, "uom": "box"}]},
        session={"stage": "confirm", "erp_customer_id": "CUST-1", "lead_profile": multi_item_profile},
        tenant={"ai_policy": {}},
        user_text="confirm",
    )
    if not premature_order_policy or not premature_order_policy.get("blocked_by_policy"):
        failures.append(f"multi_item_order_blocks_unconfirmed_uom: got {premature_order_policy!r}")
    confirmed_multi_item_profile = update_lead_profile_from_message(
        current_profile=multi_item_profile,
        user_text="yes boxes",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            confirmed_multi_item_profile,
            {
                "uom": "box",
                "requested_items_need_uom_confirmation": False,
                "requested_items_uom_assumption_status": "confirmed",
                "next_action": "ask_contact",
                "qualification_priority": "contact",
                "followup_strategy": "contact_missing",
            },
            "multi_item_order_confirms_likely_box_uom",
        )
    )
    hebrew_box_profile = update_lead_profile_from_message(
        current_profile=multi_item_profile,
        user_text="קרטון",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            hebrew_box_profile,
            {"uom": "box", "requested_items_need_uom_confirmation": False},
            "multi_item_order_confirms_hebrew_karton_as_box",
        )
    )
    hebrew_box_alt_profile = update_lead_profile_from_message(
        current_profile=multi_item_profile,
        user_text="קופסה",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            hebrew_box_alt_profile,
            {"uom": "box", "requested_items_need_uom_confirmation": False},
            "multi_item_order_confirms_hebrew_kufsa_as_box",
        )
    )
    missing_timing_profile = update_lead_profile_from_message(
        current_profile={"status": "qualified", "product_interest": "coffee machine", "quantity": 2, "uom": "box"},
        user_text="2 boxes coffee machines",
        stage="order_build",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            missing_timing_profile,
            {"qualification_priority": "specific_item_selection", "next_action": "select_specific_item"},
            "qualification_priority_specific_item_before_delivery_for_broad_product",
        )
    )
    generic_product_selection_profile = update_lead_profile_from_message(
        current_profile={"status": "qualified", "product_interest": "coffee machine", "quantity": 2, "uom": "box"},
        user_text="2 boxes coffee machines",
        stage="discover",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            generic_product_selection_profile,
            {
                "product_resolution_status": "broad",
                "qualification_priority": "specific_item_selection",
                "next_action": "select_specific_item",
            },
            "qualification_priority_specific_item_before_contact_or_confirmation",
        )
    )
    generic_product_browse_profile = update_lead_profile_from_message(
        current_profile=generic_product_selection_profile,
        user_text="which options do you have?",
        stage="discover",
        behavior_class="explorer",
        intent="browse_catalog",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            generic_product_browse_profile,
            {
                "product_resolution_status": "broad",
                "next_action": "show_matching_options",
                "followup_strategy": "product_selection_missing",
            },
            "generic_product_browse_moves_to_show_matching_options",
        )
    )
    preserve_interest_on_browse_request = update_lead_profile_from_message(
        current_profile={"status": "qualified", "product_interest": "backpack", "quantity": 10, "uom": "piece"},
        user_text="which do you have?",
        stage="discover",
        behavior_class="explorer",
        intent="browse_catalog",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            preserve_interest_on_browse_request,
            {
                "product_interest": "backpack",
                "quantity": 10.0,
                "uom": "piece",
                "next_action": "show_matching_options",
            },
            "browse_request_does_not_replace_existing_product_interest",
        )
    )
    refine_interest_on_browse_request = update_lead_profile_from_message(
        current_profile={"status": "qualified", "product_interest": "backpack", "quantity": 10, "uom": "piece"},
        user_text="show me travel backpacks",
        stage="discover",
        behavior_class="explorer",
        intent="browse_catalog",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            refine_interest_on_browse_request,
            {
                "product_interest": "travel backpacks",
                "quantity": 10.0,
                "uom": "piece",
                "next_action": "show_matching_options",
            },
            "browse_request_can_refine_existing_product_interest",
        )
    )
    preserve_product_on_uom_reply = update_lead_profile_from_message(
        current_profile={"status": "new_lead", "product_interest": "backpack"},
        user_text="pieces",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="find_product",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            preserve_product_on_uom_reply,
            {"product_interest": "backpack", "uom": "piece", "next_action": "select_specific_item"},
            "single_item_uom_reply_preserves_product_interest_and_keeps_item_selection_first",
        )
    )
    preserve_product_on_qty_uom_reply = update_lead_profile_from_message(
        current_profile={"status": "new_lead", "product_interest": "backpack", "uom": "piece"},
        user_text="10 pcs",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            preserve_product_on_qty_uom_reply,
            {"product_interest": "backpack", "uom": "piece", "quantity": 10.0},
            "single_item_qty_uom_reply_preserves_product_interest",
        )
    )
    preserve_product_on_ru_piece_reply = update_lead_profile_from_message(
        current_profile={"status": "new_lead", "product_interest": "рюкзак"},
        user_text="10 шт",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="order_detail",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            preserve_product_on_ru_piece_reply,
            {"product_interest": "рюкзак", "uom": "piece", "quantity": 10.0},
            "single_item_ru_piece_reply_preserves_product_interest",
        )
    )
    normalized_single_item_profile = update_lead_profile_from_message(
        current_profile={"status": "none", "score": 0},
        user_text="10pcs backpacks",
        stage="clarify",
        behavior_class="direct_buyer",
        intent="find_product",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            normalized_single_item_profile,
            {
                "product_interest": "backpacks",
                "uom": "piece",
                "quantity": 10.0,
                "product_resolution_status": "broad",
                "next_action": "select_specific_item",
            },
            "single_item_compact_qty_uom_phrase_is_normalized",
        )
    )
    catalog_resolution_profile = update_lead_profile_from_tool(
        current_profile=normalized_single_item_profile,
        tool_name="get_product_catalog",
        inputs={"item_name": "backpacks"},
        tool_result={
            "items": [
                {"item_code": "BP-1", "display_item_name": "Travel Backpack"},
                {"item_code": "BP-2", "display_item_name": "City Backpack"},
            ]
        },
        stage="discover",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            catalog_resolution_profile,
            {
                "catalog_candidate_count": 2,
                "product_resolution_status": "broad",
                "next_action": "select_specific_item",
            },
            "catalog_multiple_matches_keep_specific_item_selection_open",
        )
    )
    catalog_single_match_profile = update_lead_profile_from_tool(
        current_profile=normalized_single_item_profile,
        tool_name="get_product_catalog",
        inputs={"item_name": "backpacks"},
        tool_result={"items": [{"item_code": "BP-1", "display_item_name": "Travel Backpack"}]},
        stage="discover",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            catalog_single_match_profile,
            {
                "catalog_item_code": "BP-1",
                "catalog_item_name": "Travel Backpack",
                "product_resolution_status": "specific",
            },
            "catalog_single_match_resolves_specific_item",
        )
    )

    sourced_profile = update_lead_profile_source(
        current_profile=profile,
        channel="webchat",
        channel_context={
            "utm_source": "google",
            "utm_campaign": "spring",
            "referrer": "https://example.test/ad",
            "product_page": "https://example.test/products/coffee",
        },
    )
    failures.extend(
        _assert_subset(
            sourced_profile,
            {
                "source_channel": "webchat",
                "source_utm_source": "google",
                "source_utm_campaign": "spring",
                "source_product_page": "https://example.test/products/coffee",
            },
            "lead_source_attribution",
        )
    )

    summary = build_handoff_summary(
        {
            "lead_profile": profile,
            "buyer_name": "Ada",
            "buyer_phone": "+12345678901",
            "erp_customer_id": "CUST-1",
            "last_sales_order_name": "SO-1",
        },
        reason="frustrated_customer",
    )
    if summary.get("lead_status") != "quote_needed" or summary.get("product_interest") is None:
        failures.append(f"handoff_summary_contains_lead_context: got {summary!r}")

    ru_profile = update_lead_profile_from_message(
        current_profile={"status": "none", "score": 0},
        user_text="Нужно 10 коробок завтра, доставка, какая цена?",
        stage="lead_capture",
        behavior_class="unclear_request",
        intent="find_product",
        customer_identified=False,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            ru_profile,
            {"quantity": 10.0, "urgency": "soon", "delivery_need": "mentioned", "price_sensitivity": True},
            "lead_profile_default_multilingual_signals",
        )
    )

    custom_profile = update_lead_profile_from_message(
        current_profile={"status": "none", "score": 0},
        user_text="preciso proposta hoje",
        stage="lead_capture",
        behavior_class="unclear_request",
        intent="browse_catalog",
        customer_identified=False,
        active_order_name=None,
        lead_config={
            "signal_terms": {
                "quote": ["proposta"],
                "urgency": ["hoje"],
            }
        },
    )
    failures.extend(
        _assert_subset(
            custom_profile,
            {"status": "quote_needed", "urgency": "soon", "price_sensitivity": True},
            "lead_profile_configurable_any_language_signals",
        )
    )

    hot_profile = update_lead_profile_from_message(
        current_profile={"status": "qualified", "score": 65, "temperature": "warm", "product_interest": "coffee", "quantity": 2, "uom": "box"},
        user_text="confirm order today",
        stage="confirm",
        behavior_class="direct_buyer",
        intent="confirm_order",
        customer_identified=True,
        active_order_name=None,
    )
    alert_events = sales_alert_event_types({"status": "qualified", "score": 65, "temperature": "warm"}, hot_profile)
    if "hot_lead_detected" not in alert_events:
        failures.append(f"hot_lead_alert: expected hot_lead_detected, got {alert_events!r}")

    revenue_profile = update_lead_profile_from_tool(
        current_profile={"status": "order_ready", "score": 80, "temperature": "hot", "quote_status": "requested"},
        tool_name="create_sales_order",
        inputs={"items": [{"item_code": "ITEM-1", "qty": 2}]},
        tool_result={"name": "SO-1", "grand_total": 250.5, "currency": "USD"},
        stage="closed",
        customer_identified=True,
        active_order_name="SO-1",
    )
    failures.extend(
        _assert_subset(
            revenue_profile,
            {"order_total": 250.5, "won_revenue": 250.5, "currency": "USD", "quote_status": "accepted"},
            "lead_revenue_attribution_from_sales_order",
        )
    )

    stalled_profile = mark_stalled_if_needed(
        current_profile={"status": "qualified", "score": 55, "temperature": "warm"},
        last_interaction_at=(datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
        idle_after=timedelta(minutes=60),
    )
    failures.extend(
        _assert_subset(
            stalled_profile,
            {"status": "stalled", "next_action": "follow_up_or_handoff"},
            "lead_profile_stalled_after_idle",
        )
    )
    followup_message = build_followup_message(
        {
            "lang": "en",
            "lead_profile": {
                "status": "stalled",
                "product_interest": "coffee machines",
                "next_action": "ask_quantity",
            },
        }
    )
    if "coffee machines" not in followup_message or "quantity" not in followup_message:
        failures.append(f"followup_message_contains_context: got {followup_message!r}")
    action_followup = build_followup_message(
        {
            "lang": "en",
            "lead_profile": {
                "status": "quote_needed",
                "product_interest": "coffee machines",
                "next_action": "quote_or_clarify_price",
                "followup_strategy": "price_objection",
            },
        }
    )
    if "adjust" not in action_followup or "alternative" not in action_followup:
        failures.append(f"followup_message_uses_strategy_template: got {action_followup!r}")
    strategy_override_followup = build_followup_message(
        {
            "lang": "en",
            "lead_profile": {
                "status": "quote_needed",
                "product_interest": "coffee machines",
                "next_action": "quote_or_clarify_price",
                "followup_strategy": "price_objection",
            },
        },
        {
            "lead_management": {
                "followup_templates_by_strategy": {
                    "price_objection": {"en": "Custom objection follow-up for {product_interest}."}
                }
            }
        },
    )
    if strategy_override_followup != "Custom objection follow-up for coffee machines.":
        failures.append(f"followup_message_uses_strategy_override: got {strategy_override_followup!r}")
    custom_stage_followup = build_followup_message(
        {
            "lang": "en",
            "lead_profile": {
                "status": "quote_needed",
                "product_interest": "coffee machines",
                "next_action": "quote_or_clarify_price",
            },
        },
        {
            "lead_management": {
                "followup_templates_by_status": {
                    "quote_needed": {"en": "Custom quote follow-up for {product_interest}."}
                }
            }
        },
    )
    if custom_stage_followup != "Custom quote follow-up for coffee machines.":
        failures.append(f"followup_message_uses_status_override: got {custom_stage_followup!r}")

    followup_session = {
        "lead_profile": {"status": "stalled", "temperature": "warm", "followup_count": 1},
    }
    allowed, blocked_reason = can_send_followup(
        current_profile=followup_session["lead_profile"],
        lead_config={"max_followups_per_lead": 1},
    )
    if allowed or blocked_reason != "max_followups_reached":
        failures.append(f"followup_governance_max_count: expected max_followups_reached, got {(allowed, blocked_reason)!r}")
    lost_profile = mark_lost_if_followup_exhausted(
        current_profile=followup_session["lead_profile"],
        reason=blocked_reason,
    )
    failures.extend(
        _assert_subset(
            lost_profile,
            {"status": "lost", "lost_reason": "no_response", "next_action": "stop_followup"},
            "lead_lost_after_followup_exhausted",
        )
    )

    opt_out_profile = update_lead_profile_from_message(
        current_profile={"status": "qualified", "score": 55, "temperature": "warm"},
        user_text="stop, do not contact me",
        stage="discover",
        behavior_class="unclear_request",
        intent="find_product",
        customer_identified=True,
        active_order_name=None,
    )
    failures.extend(
        _assert_subset(
            opt_out_profile,
            {"status": "lost", "lost_reason": "opt_out", "do_not_contact": True},
            "lead_opt_out_stops_contact",
        )
    )

    sent_session = {"lead_profile": {"followup_count": 0}}
    mark_followup_attempt(sent_session, {"sent": True, "status": "sent", "channel": "telegram"})
    if sent_session["lead_profile"].get("followup_count") != 1 or not sent_session["lead_profile"].get("last_followup_at"):
        failures.append(f"followup_attempt_tracks_sent: got {sent_session!r}")

    owner_message = build_sales_owner_message(
        {
            "last_channel": "telegram",
            "buyer_name": "Ada",
            "lead_profile": {
                "lead_id": "lead_test123",
                "status": "quote_needed",
                "temperature": "hot",
                "score": 80,
                "next_action": "handoff_manager",
                "product_interest": "coffee machines",
            },
        },
        reason="hot_lead_detected",
    )
    if "coffee machines" not in owner_message or "hot_lead_detected" not in owner_message or "lead_test123" not in owner_message:
        failures.append(f"sales_owner_message_contains_context: got {owner_message!r}")
    if normalize_telegram_username("@SalesOwner") != "salesowner":
        failures.append("sales_owner_username_normalization: expected @SalesOwner -> salesowner")

    identified_profile = ensure_lead_identity(
        current_profile={},
        company_code="acme",
        channel="telegram",
        channel_uid="42",
    )
    lead_id = str(identified_profile.get("lead_id") or "")
    if not lead_id.startswith("lead_") or identified_profile.get("lead_id") != ensure_lead_identity(
        current_profile=identified_profile,
        company_code="acme",
        channel="telegram",
        channel_uid="42",
    ).get("lead_id"):
        failures.append(f"lead_identity_stable: got {identified_profile!r}")

    accepted_profile = apply_sales_owner_action(
        current_profile={"sales_owner_status": "delivered"},
        action="accept",
        actor_id="manager-1",
    )
    failures.extend(
        _assert_subset(
            accepted_profile,
            {"sales_owner_status": "accepted", "next_action": "human_owner_followup", "sales_owner_action_by": "manager-1"},
            "sales_owner_accept_updates_profile",
        )
    )
    closed_profile = apply_sales_owner_action(
        current_profile={"status": "quote_needed"},
        action="close",
        actor_id="manager-1",
        lost_reason="price_too_high",
    )
    failures.extend(
        _assert_subset(
            closed_profile,
            {"sales_owner_status": "closed_not_target", "status": "lost", "lost_reason": "price_too_high", "do_not_contact": True},
            "sales_owner_close_updates_profile",
        )
    )
    manual_won_profile = apply_manual_close(
        current_profile={"status": "qualified", "quote_status": "sent"},
        outcome="won",
        actor_id="manager-1",
        order_total=300,
        won_revenue=300,
        currency="USD",
    )
    failures.extend(
        _assert_subset(
            manual_won_profile,
            {"status": "won", "quote_status": "accepted", "order_total": 300.0, "won_revenue": 300.0, "currency": "USD"},
            "manual_close_won_updates_revenue",
        )
    )
    quote_sent_profile = apply_quote_update(
        current_profile={"status": "quote_needed", "quote_status": "requested"},
        quote_status="sent",
        quote_id="Q-1",
        quote_total="125.50",
        quote_currency="USD",
        actor_id="manager-1",
    )
    failures.extend(
        _assert_subset(
            quote_sent_profile,
            {"quote_status": "sent", "quote_id": "Q-1", "quote_total": 125.5, "quote_currency": "USD"},
            "quote_sent_updates_profile",
        )
    )
    quote_accepted_profile = apply_quote_update(
        current_profile=quote_sent_profile,
        quote_status="accepted",
    )
    failures.extend(
        _assert_subset(
            quote_accepted_profile,
            {"quote_status": "accepted", "status": "order_ready", "next_action": "confirm_order"},
            "quote_accepted_updates_profile",
        )
    )
    merged_profile = apply_lead_merge(
        current_profile={"status": "stalled", "lead_id": "lead_dup"},
        target_lead_id="lead_main",
        actor_id="manager-1",
    )
    failures.extend(
        _assert_subset(
            merged_profile,
            {"status": "merged", "merged_into_lead_id": "lead_main", "do_not_contact": True},
            "lead_merge_updates_duplicate_profile",
        )
    )
    target_profile = record_merged_duplicate(
        current_profile={"status": "quote_needed", "merged_duplicate_lead_ids": []},
        duplicate_lead_id="lead_dup",
        actor_id="manager-1",
    )
    if "lead_dup" not in target_profile.get("merged_duplicate_lead_ids", []):
        failures.append(f"lead_merge_records_target_duplicate: got {target_profile!r}")
    correction_profile = apply_order_correction_update(
        current_profile={"status": "order_created"},
        correction_status="requested",
        target_order_id="SO-1",
        correction_type="quantity",
        actor_id="manager-1",
    )
    failures.extend(
        _assert_subset(
            correction_profile,
            {"order_correction_status": "requested", "target_order_id": "SO-1", "correction_type": "quantity", "next_action": "clarify_order_correction"},
            "order_correction_requested_updates_profile",
        )
    )
    correction_message_profile = update_lead_profile_from_message(
        current_profile={"status": "order_created"},
        user_text="изменить количество в заказе",
        stage="service",
        behavior_class="service_request",
        intent="service_request",
        customer_identified=True,
        active_order_name="SO-1",
    )
    failures.extend(
        _assert_subset(
            correction_message_profile,
            {"order_correction_status": "requested", "correction_type": "quantity", "target_order_id": "SO-1"},
            "order_correction_signal_is_multilingual_and_state_based",
        )
    )
    order_status_profile = update_lead_profile_from_tool(
        current_profile={"status": "order_created", "order_correction_status": "requested"},
        tool_name="get_sales_order_status",
        inputs={"sales_order_name": "SO-1"},
        tool_result={"sales_order_name": "SO-1", "order_state": "delivered", "can_modify": False},
        stage="service",
        customer_identified=True,
        active_order_name="SO-1",
    )
    failures.extend(
        _assert_subset(
            order_status_profile,
            {"active_order_state": "delivered", "active_order_can_modify": False, "next_action": "handoff_manager"},
            "sales_order_status_updates_profile",
        )
    )
    return failures


def run_agent_runtime_evals() -> list[str]:
    failures: list[str] = []
    broad_profile = {
        "product_interest": "backpacks",
        "product_resolution_status": "broad",
        "next_action": "show_matching_options",
    }
    if not should_prefetch_catalog_options(lead_profile=broad_profile, intent="browse_catalog"):
        failures.append("agent_prefetch_enabled_for_broad_product_browse: expected True")
    if should_prefetch_catalog_options(
        lead_profile={"product_interest": "backpacks", "product_resolution_status": "specific", "next_action": "show_matching_options"},
        intent="browse_catalog",
    ):
        failures.append("agent_prefetch_disabled_for_specific_item: expected False")
    if should_prefetch_catalog_options(
        lead_profile={
            "product_interest": "backpacks",
            "product_resolution_status": "broad",
            "next_action": "ask_unit",
            "catalog_lookup_query": None,
            "catalog_lookup_status": "unknown",
        },
        intent="browse_catalog",
    ) is not True:
        failures.append("agent_prefetch_enabled_for_unresolved_broad_product: expected True")

    context = build_catalog_prefetch_context(
        {
            "items": [
                {"item_code": "BP-1", "display_item_name": "Travel Backpack"},
                {"item_code": "BP-2", "display_item_name": "City Backpack"},
            ]
        },
        search_term="backpacks",
    )
    expected_fragments = [
        'Runtime catalog lookup already ran for broad product "backpacks".',
        "Travel Backpack (BP-1)",
        "City Backpack (BP-2)",
        "Use these matching options directly in your reply",
    ]
    for fragment in expected_fragments:
        if fragment not in context:
            failures.append(f"agent_prefetch_context_missing_fragment: expected {fragment!r} in {context!r}")

    no_match_context = build_catalog_prefetch_context({"items": []}, search_term="backpacks")
    if "found no exact matches" not in no_match_context:
        failures.append(f"agent_prefetch_context_no_match_guidance: got {no_match_context!r}")
    return failures


def run_llm_state_updater_evals() -> list[str]:
    failures: list[str] = []
    parsed = parse_llm_state_update(
        json.dumps(
            {
                "intent": "browse_catalog",
                "behavior_class": "explorer",
                "confidence": 0.86,
                "lead_patch": {
                    "product_interest": "travel backpacks",
                    "quantity": 5,
                    "uom": "pcs",
                    "price_sensitivity": False,
                },
                "reason": "Customer asks to see backpack options and gives quantity/unit.",
            },
            ensure_ascii=False,
        )
    )
    failures.extend(
        _assert_subset(
            parsed,
            {
                "valid": True,
                "intent": "browse_catalog",
                "behavior_class": "explorer",
            },
            "llm_state_updater_parses_valid_response",
        )
    )
    patched_profile = apply_llm_lead_patch(
        current_profile={"product_interest": "backpack", "catalog_item_code": "BP-1", "catalog_item_name": "Old Backpack"},
        patch=parsed.get("lead_patch"),
    )
    failures.extend(
        _assert_subset(
            patched_profile,
            {
                "product_interest": "travel backpacks",
                "quantity": 5.0,
                "uom": "piece",
                "catalog_item_code": None,
                "catalog_item_name": None,
            },
            "llm_state_updater_patch_resets_catalog_selection_when_interest_changes",
        )
    )
    invalid = parse_llm_state_update('{"intent":"made_up","behavior_class":"unknown","lead_patch":{"quantity":"x"}}')
    if invalid.get("valid"):
        failures.append(f"llm_state_updater_rejects_invalid_values: got {invalid!r}")
    return failures


def run_sales_dedupe_evals() -> list[str]:
    failures: list[str] = []
    now = datetime.now(UTC)
    current = {
        "lead_id": "lead_current",
        "company_code": "acme",
        "channel": "telegram",
        "channel_uid": "u1",
        "buyer_phone": "+1 555 123 4567",
        "erp_customer_id": "CUST-1",
        "product_interest": "coffee machine",
        "active_order_name": "SO-1",
        "last_interaction_at": now.isoformat(),
    }
    same_order = detect_duplicate_lead(
        current=current,
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
    if not same_order or same_order.get("dedupe_reason") != "same_active_order":
        failures.append(f"dedupe_same_active_order: got {same_order!r}")
    different_product = detect_duplicate_lead(
        current={**current, "active_order_name": None, "product_interest": "coffee machine"},
        candidates=[
            {
                "lead_id": "lead_other_order",
                "company_code": "acme",
                "channel": "telegram",
                "channel_uid": "u1",
                "product_interest": "printer paper",
                "last_interaction_at": (now - timedelta(hours=2)).isoformat(),
            }
        ],
        now=now,
    )
    if different_product is not None:
        failures.append(f"dedupe_same_channel_different_product_not_duplicate: got {different_product!r}")
    same_phone_product = detect_duplicate_lead(
        current={**current, "active_order_name": None, "product_interest": "coffee machine"},
        candidates=[
            {
                "lead_id": "lead_similar",
                "company_code": "acme",
                "buyer_phone": "15551234567",
                "product_interest": "coffee machines",
                "last_interaction_at": (now - timedelta(days=2)).isoformat(),
            }
        ],
        now=now,
    )
    if not same_phone_product or same_phone_product.get("dedupe_reason") != "same_phone_similar_product":
        failures.append(f"dedupe_same_phone_similar_product: got {same_phone_product!r}")
    return failures


def run_sales_reporting_evals() -> list[str]:
    failures: list[str] = []
    notified_at = (datetime.now(UTC) - timedelta(minutes=12)).isoformat()
    accepted_at = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    leads = [
        lead_snapshot(
            channel="webchat",
            uid="s1",
            session={
                "company_code": "acme",
                "buyer_name": "Ada",
                "lead_profile": {
                    "lead_id": "lead_1",
                    "status": "quote_needed",
                    "temperature": "hot",
                    "score": 80,
                    "source_channel": "webchat",
                    "source_utm_source": "google",
                    "source_utm_campaign": "spring",
                    "sales_owner_status": "accepted",
                    "sales_owner_action_by": "manager-1",
                    "sales_owner_notified_at": notified_at,
                    "sales_owner_action_at": accepted_at,
                    "followup_count": 1,
                    "product_interest": "coffee machines",
                    "playbook_version": "v1",
                    "expected_revenue": 100.0,
                    "order_total": 120.0,
                    "won_revenue": 120.0,
                    "manual_close_actor_id": "manager-1",
                },
            },
        ),
        lead_snapshot(
            channel="telegram",
            uid="s2",
            session={
                "company_code": "acme",
                "lead_profile": {
                    "lead_id": "lead_2",
                    "status": "stalled",
                    "temperature": "warm",
                    "score": 50,
                    "source_channel": "telegram",
                    "sales_owner_status": "delivered",
                },
            },
        ),
        lead_snapshot(
            channel="webchat",
            uid="s3",
            session={
                "company_code": "other",
                "lead_profile": {
                    "lead_id": "lead_3",
                    "status": "won",
                    "temperature": "hot",
                    "score": 90,
                },
            },
        ),
        lead_snapshot(
            channel="webchat",
            uid="s4",
            session={
                "company_code": "acme",
                "lead_profile": {
                    "lead_id": "lead_4",
                    "status": "won",
                    "temperature": "hot",
                    "score": 90,
                    "source_channel": "webchat",
                    "source_utm_source": "google",
                    "source_utm_campaign": "spring",
                    "won_revenue": 400.0,
                    "order_correction_status": "applied",
                },
            },
        ),
    ]
    acme_leads = filter_leads(leads, company_code="acme")
    if len(acme_leads) != 3:
        failures.append(f"sales_reporting_filters_company: expected 2 leads, got {len(acme_leads)}")
    hot_webchat = filter_leads(leads, company_code="acme", temperature="hot", source_channel="webchat", q="coffee")
    if len(hot_webchat) != 1 or hot_webchat[0].get("lead_id") != "lead_1":
        failures.append(f"sales_reporting_filters_search: got {hot_webchat!r}")
    summary = summarize_leads(acme_leads)
    expected_summary = {
        "total": 2,
        "won_count": 1,
        "hot_count": 1,
        "stalled_count": 1,
        "quote_needed_count": 1,
        "followup_sent_count": 1,
        "accepted_by_owner_count": 1,
        "expected_revenue": 100.0,
        "order_revenue": 120.0,
        "won_revenue": 520.0,
        "order_correction_applied_count": 1,
    }
    expected_summary["total"] = 3
    expected_summary["hot_count"] = 2
    failures.extend(_assert_subset(summary, expected_summary, "sales_reporting_summary"))
    source_funnel = summarize_source_funnel(acme_leads, group_by="source_utm_source")
    google = next((item for item in source_funnel.get("sources", []) if item.get("key") == "google"), None)
    if not google or google.get("won") != 1 or google.get("won_revenue") != 400.0:
        failures.append(f"sales_reporting_source_funnel: got {source_funnel!r}")
    time_funnel = summarize_time_funnel(
        [
            {**acme_leads[0], "created_at": "2026-04-07T10:00:00+00:00"},
            {**acme_leads[-1], "created_at": "2026-04-07T12:00:00+00:00"},
        ],
        granularity="day",
    )
    day = next((item for item in time_funnel.get("periods", []) if item.get("period") == "2026-04-07"), None)
    if not day or day.get("total") != 2:
        failures.append(f"sales_reporting_time_funnel: got {time_funnel!r}")
    contract = dashboard_contract()
    if "merge_lead" not in contract.get("endpoints", {}) or "order_correction" not in contract.get("endpoints", {}) or "time_funnel" not in contract.get("endpoints", {}):
        failures.append(f"sales_dashboard_contract: got {contract!r}")
    if summary.get("by_playbook_version_metrics", {}).get("v1", {}).get("total") != 1:
        failures.append(f"sales_reporting_playbook_metrics: got {summary!r}")
    if summary.get("average_owner_accept_minutes") != 10.0:
        failures.append(f"sales_reporting_owner_accept_latency: got {summary!r}")
    manager_summary = summarize_manager_performance(acme_leads)
    manager_1 = next((item for item in manager_summary.get("managers", []) if item.get("manager_id") == "manager-1"), None)
    if not manager_1 or manager_1.get("accepted_count") != 1 or manager_1.get("average_accept_minutes") != 10.0:
        failures.append(f"sales_reporting_manager_performance: got {manager_summary!r}")
    won_manager_summary = summarize_manager_performance(
        [
            {
                "lead_id": "lead_won",
                "status": "won",
                "manual_close_actor_id": "manager-1",
                "won_revenue": 120.0,
            }
        ]
    )
    won_manager = next((item for item in won_manager_summary.get("managers", []) if item.get("manager_id") == "manager-1"), None)
    if not won_manager or won_manager.get("won_count") != 1 or won_manager.get("won_revenue") != 120.0:
        failures.append(f"sales_reporting_manager_revenue: got {won_manager_summary!r}")
    page = paginate_leads(acme_leads, offset=1, limit=1)
    if len(page) != 1 or page[0].get("lead_id") != "lead_2":
        failures.append(f"sales_reporting_pagination: got {page!r}")
    crm_export = crm_export_contract(
        channel="webchat",
        uid="s1",
        session={
            "company_code": "acme",
            "buyer_name": "Ada",
            "lead_timeline": [{"event_type": "lead_created"}],
            "lead_profile": {
                "lead_id": "lead_1",
                "status": "quote_needed",
                "temperature": "hot",
                "score": 80,
                "product_interest": "coffee machines",
            },
        },
    )
    if crm_export.get("lead_id") != "lead_1" or crm_export.get("pipeline", {}).get("status") != "quote_needed":
        failures.append(f"crm_export_contract_shape: got {crm_export!r}")
    compact_record = compact_lead_record(
        channel="webchat",
        uid="s1",
        session={
            "company_code": "acme",
            "messages": [{"role": "user", "content": "full transcript should not persist"}],
            "lead_timeline": [{"event_type": f"event_{index}"} for index in range(150)],
            "lead_profile": {
                "lead_id": "lead_1",
                "status": "quote_needed",
                "temperature": "hot",
                "score": 80,
            },
        },
    )
    if not compact_record or "messages" in compact_record or len(compact_record.get("timeline") or []) > 100:
        failures.append(f"sales_lead_repository_compact_record: got {compact_record!r}")
    crm_outbox_event = build_sales_crm_outbox_event(compact_record or {}, event_type="lead_closed")
    if (
        not str(crm_outbox_event.get("event_id") or "").startswith("crm_sync_")
        or crm_outbox_event.get("event_type") != "lead_closed"
        or crm_outbox_event.get("payload", {}).get("sales_lead", {}).get("lead_id") != "lead_1"
    ):
        failures.append(f"sales_crm_outbox_event_shape: got {crm_outbox_event!r}")
    return failures


def run_sales_governance_evals() -> list[str]:
    failures: list[str] = []
    now = datetime.now(UTC)
    session = {
        "lead_profile": {
            "lead_id": "lead_sla",
            "status": "quote_needed",
            "temperature": "hot",
            "hot_at": (now - timedelta(minutes=20)).isoformat(),
            "quote_status": "requested",
            "quote_requested_at": (now - timedelta(minutes=45)).isoformat(),
            "sales_owner_status": "delivered",
        }
    }
    breaches = evaluate_sla_breaches(
        session=session,
        lead_config={"hot_lead_owner_accept_sla_minutes": 10, "quote_prepare_sla_minutes": 30},
        now=now,
    )
    rules = {breach.get("rule") for breach in breaches}
    if rules != {"hot_lead_owner_accept", "quote_prepare"}:
        failures.append(f"sales_governance_sla_rules: got {breaches!r}")
    new_breaches = record_new_sla_breaches(session, breaches)
    duplicate_breaches = record_new_sla_breaches(session, breaches)
    if len(new_breaches) != 2 or duplicate_breaches:
        failures.append(f"sales_governance_sla_dedup: got {(new_breaches, duplicate_breaches)!r}")
    timeline_entry = append_lead_timeline_event(
        session,
        event_type="sales_sla_breached",
        payload={"sla_rule": "hot_lead_owner_accept", "sla_minutes": 10, "ignored": "x"},
    )
    if timeline_entry.get("payload", {}).get("sla_rule") != "hot_lead_owner_accept" or "ignored" in timeline_entry.get("payload", {}):
        failures.append(f"sales_timeline_safe_payload: got {timeline_entry!r}")
    quality = evaluate_conversation_quality(
        {
            "messages": [
                {"role": "user", "content": "I need a human manager"},
                {"role": "assistant", "content": "We guarantee discount and today delivery."},
            ],
            "lead_profile": {
                "status": "quote_needed",
                "temperature": "hot",
                "next_action": "ask_quantity",
            },
        }
    )
    expected_flags = {"risky_promise_without_tool", "hot_lead_not_handed_to_owner", "human_requested_without_owner_handoff"}
    if not expected_flags.issubset(set(quality.get("quality_flags", []))):
        failures.append(f"sales_quality_flags: got {quality!r}")
    quality_with_policy = evaluate_conversation_quality(
        {"messages": [{"role": "assistant", "content": "I can give a discount and today delivery."}]},
        ai_policy={"sales_policy": {"allow_discount_promises": False}},
    )
    if "discount_promise_blocked_by_sales_policy" not in quality_with_policy.get("quality_flags", []):
        failures.append(f"sales_quality_sales_policy_flags: got {quality_with_policy!r}")
    if not minimum_order_violation([{"qty": 2, "rate": 10}], {"sales_policy": {"minimum_order_total": 50}}):
        failures.append("sales_policy_minimum_order_violation: expected violation")
    incomplete_anchor = price_anchor_status({"product_interest": "coffee", "quantity": 2})
    if incomplete_anchor.get("complete") or incomplete_anchor.get("missing") != ["uom"]:
        failures.append(f"sales_policy_price_anchor_missing_uom: got {incomplete_anchor!r}")
    if not should_hide_catalog_prices({"product_interest": "coffee", "quantity": 2}, {"sales_policy": {}}):
        failures.append("sales_policy_hides_catalog_prices_without_uom: expected hidden prices")
    if should_hide_catalog_prices({"product_interest": "coffee", "quantity": 2, "uom": "box"}, {"sales_policy": {}}):
        failures.append("sales_policy_allows_catalog_prices_with_full_anchor: expected visible prices")
    scrubbed = remove_price_fields({"items": [{"item_name": "Coffee", "rate": 12, "price_list_rate": 15, "nested": {"price": 9}}]})
    if "rate" in scrubbed["items"][0] or "price_list_rate" in scrubbed["items"][0] or "price" in scrubbed["items"][0]["nested"]:
        failures.append(f"sales_policy_remove_price_fields: got {scrubbed!r}")
    if normalize_order_state({"name": "SO-1", "status": "Delivered", "per_delivered": 100}).get("can_modify") is not False:
        failures.append("sales_policy_delivered_order_not_modifiable: expected can_modify False")
    if sales_policy({"sales_policy": {"default_delivery_days": 2}}).get("default_delivery_days") != 2:
        failures.append("sales_policy_override: expected default_delivery_days=2")
    if not re.match(r"\d{4}-\d{2}-\d{2}", earliest_delivery_date({"sales_policy": {"default_delivery_days": 0}})):
        failures.append("sales_policy_earliest_delivery_date: invalid date")
    return failures


def main() -> int:
    failures = run_conversation_flow_evals()
    failures.extend(run_tool_policy_evals())
    failures.extend(run_tool_schema_evals())
    failures.extend(run_prompt_override_evals())
    failures.extend(run_language_lock_evals())
    failures.extend(run_i18n_evals())
    failures.extend(run_catalog_localization_evals())
    failures.extend(run_uom_semantics_evals())
    failures.extend(run_lead_management_evals())
    failures.extend(run_agent_runtime_evals())
    failures.extend(run_llm_state_updater_evals())
    failures.extend(run_sales_dedupe_evals())
    failures.extend(run_sales_reporting_evals())
    failures.extend(run_sales_governance_evals())
    if failures:
        print("AI sales evals failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("AI sales evals passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
