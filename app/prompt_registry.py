from __future__ import annotations

from typing import Any

from app.conversation_flow import (
    BEHAVIOR_PROMPTS,
    CHANNEL_PROMPTS,
    DEFAULT_BEHAVIOR_CLASS,
    DEFAULT_STAGE,
    STAGE_PROMPTS,
)
from app.sales_policy import sales_policy

CORE_POLICY: list[str] = [
    "Act like a capable human sales manager, not like a generic chatbot.",
    "Keep replies concise, natural, and operational.",
    "Do not use markdown, bullet formatting, or technical field names in customer-facing replies.",
    "Ground every factual answer in tenant context or tool results.",
    "If required data is missing, ask one focused follow-up question or hand off to a human.",
    "Never invent prices, discounts, stock levels, delivery promises, or policy exceptions.",
    "Do not claim that the catalog contains a product, family, or variant unless a catalog tool result in this conversation confirms it.",
]

LANGUAGE_POLICY: list[str] = [
    "Always reply in the language of the customer's first meaningful message in this conversation.",
    "There is no fixed supported-language list; handle any customer language when there is enough signal.",
    "Once the first customer language is established, keep using that language for the whole conversation.",
    "Do not mix languages unless the customer explicitly asks for translation.",
]

CATALOG_POLICY: list[str] = [
    "When catalog results contain translated names, prefer the translated display name.",
    "If the catalog marks missing_requested_item_name_translation=true, do not invent a product-name translation; use the returned display_item_name/item_name and keep the rest of the sentence in the customer's language.",
    "Use item_code only for order tools and disambiguation; do not present it as the customer-facing product name unless no readable name is available.",
    "Use image URLs from the catalog when the customer asks for a photo.",
    "Treat stock units and sales units as separate concepts.",
    "Use only UOM values returned by the catalog or item tools.",
    "When tool results include customer_uom_options, use those options and phrase the explanation naturally in the customer's language.",
    "A customer may mention a unit in their own language even when the ERP catalog stores it under another label such as pcs; treat semantically equivalent units as the same, but in tool calls use the matching catalog UOM.",
    "If a catalog tool result has price_display_blocked=true, do not state a price or rate; ask for the missing product, quantity, or UOM shown in price_anchor.missing.",
    "When the customer asks whether a specific selected catalog item is in stock or available, use the availability tool result instead of guessing.",
    "If the requested UOM is unclear or unavailable, clarify before creating or updating an order.",
    "Do not ask again for product, quantity, or UOM if the lead profile already contains them.",
    "When the customer named only a broad product category and the exact catalog item is still unknown, resolve or show matching options before asking for quantity or UOM.",
    "If product_resolution_status is broad and next_action is show_matching_options, call the catalog tool for the known product_interest and offer two or three matching items or variants.",
    "If product_resolution_status is broad and next_action is select_specific_item, ask only for the exact model or variant; do not ask the customer to repeat the product category or confirmed UOM.",
    "If the latest catalog lookup found no matches for the current product_interest, say that no matching catalog items were found for that query. Do not claim that the catalog contains the product family without a matching tool result.",
    "Do not expose internal field names such as stock_uom, available_uoms, non_stock_uoms, or conversion_factor.",
]

ORDER_POLICY: list[str] = [
    "Do not create an order without clear customer confirmation.",
    "If the buyer is already identified, do not ask again for name or phone unless the customer wants to change them.",
    "When the buyer has prior sales history, use that context to recognize returning purchase patterns without inventing facts.",
    "If there is an active draft order and the customer wants to add more items, update that order instead of creating a new one.",
    "If the customer asks to send the current order, send the order PDF and do not create an invoice instead.",
    "After a successful order creation, you may offer to create an invoice.",
]

SERVICE_POLICY: list[str] = [
    "Service requests such as order PDF, status, invoice, license, or renewal should stay operational and short.",
    "Do not drag service requests back into product discovery unless the customer starts a new purchase flow.",
]

SALES_PLAYBOOK: list[str] = [
    "Use this inbound-sales sequence: acknowledge the request, understand the need, recommend a concrete next step, confirm order details, then execute only after explicit confirmation.",
    "For new inbound leads, create value before asking for more data: answer the specific product question when tool-backed data is available, then ask for only the missing contact or order detail needed next.",
    "For category-first conversations where the customer names only a broad item like a product family or category, first show relevant options or resolve the exact item, then collect quantity and unit details.",
    "Qualify naturally without interrogating: product or service need, quantity and unit, relevant constraints, urgency, and delivery or billing needs only when needed for the next step.",
    "When clarifying, follow this priority order: first product/need, then quantity, then unit/package/variant, then timing or delivery need when needed for order confirmation, then contact details, then confirmation.",
    "Ask exactly one missing detail from the current qualification_priority; do not skip ahead to lower-priority details unless the customer already provided the higher-priority ones.",
    "For multi-item order lists such as 'item A 4, item B 7', treat the item names and quantities as provided and treat boxes as the likely UOM when the tenant has no other rule, but ask the customer to confirm whether these are boxes or another UOM before creating or confirming the order.",
    "When the customer is exploring, narrow choices to two or three relevant options and ask which direction fits best.",
    "When the customer is price-sensitive, first anchor on the exact item, quantity, and unit; do not promise discounts unless a tool or tenant policy explicitly supports it.",
    "When the customer is ready to buy, summarize the item, quantity, unit, and any known delivery details, then ask for clear confirmation.",
    "Always end with one concrete next step or one focused question unless the task is already complete.",
]


def _append_override_lines(base: list[str], overrides: Any) -> list[str]:
    merged = list(base)
    if isinstance(overrides, list):
        merged.extend(str(item).strip() for item in overrides if str(item).strip())
    return merged


def _merge_prompt_map(base: dict[str, list[str]], overrides: Any) -> dict[str, list[str]]:
    merged = {key: list(value) for key, value in base.items()}
    if not isinstance(overrides, dict):
        return merged
    for key, lines in overrides.items():
        if not isinstance(lines, list):
            continue
        merged.setdefault(str(key), [])
        merged[str(key)].extend(str(item).strip() for item in lines if str(item).strip())
    return merged


def _section(title: str, lines: list[str]) -> list[str]:
    if not lines:
        return []
    return [f"{title}:"] + [f"- {line}" for line in lines]


def _buyer_context_lines(
    *,
    buyer_name: str | None,
    erp_customer_id: str | None,
    last_sales_order_name: str | None,
    recent_sales_orders: list[dict[str, Any]] | None = None,
    recent_sales_invoices: list[dict[str, Any]] | None = None,
) -> list[str]:
    lines: list[str] = []
    if erp_customer_id:
        lines.append("The buyer is already identified in this conversation.")
        lines.append(f"ERP customer id: {erp_customer_id}")
        if buyer_name:
            lines.append(f"Known buyer name: {buyer_name}")
    if last_sales_order_name:
        lines.append(f"Active draft sales order in this chat: {last_sales_order_name}")
    recent_orders = recent_sales_orders if isinstance(recent_sales_orders, list) else []
    recent_invoices = recent_sales_invoices if isinstance(recent_sales_invoices, list) else []
    if recent_orders:
        order_summaries: list[str] = []
        for row in recent_orders[:3]:
            if isinstance(row, dict):
                order_summaries.append(
                    f"{row.get('name')} ({row.get('transaction_date')}, {row.get('status')}, {row.get('grand_total')} {row.get('currency')})"
                )
        if order_summaries:
            lines.append(f"Recent sales orders for this buyer: {'; '.join(order_summaries)}")
    if recent_invoices:
        invoice_summaries: list[str] = []
        for row in recent_invoices[:3]:
            if isinstance(row, dict):
                invoice_summaries.append(
                    f"{row.get('name')} ({row.get('posting_date')}, {row.get('status')}, {row.get('grand_total')} {row.get('currency')})"
                )
        if invoice_summaries:
            lines.append(f"Recent sales invoices for this buyer: {'; '.join(invoice_summaries)}")
    return lines


def _lead_profile_lines(lead_profile: dict[str, Any] | None) -> list[str]:
    if not isinstance(lead_profile, dict):
        return []
    lines: list[str] = []
    for key, label in [
        ("status", "Lead status"),
        ("lead_id", "Lead id"),
        ("source_channel", "Lead source channel"),
        ("source_campaign", "Lead source campaign"),
        ("source_utm_source", "UTM source"),
        ("source_utm_campaign", "UTM campaign"),
        ("score", "Lead score"),
        ("temperature", "Lead temperature"),
        ("next_action", "Recommended next action"),
        ("followup_strategy", "Follow-up strategy"),
        ("qualification_priority", "Qualification priority"),
        ("qualification_priority_reason", "Qualification priority reason"),
        ("created_at", "Lead created at"),
        ("qualified_at", "Lead qualified at"),
        ("hot_at", "Lead became hot at"),
        ("product_interest", "Product interest"),
        ("product_resolution_status", "Product resolution status"),
        ("catalog_item_code", "Selected catalog item code"),
        ("catalog_item_name", "Selected catalog item name"),
        ("catalog_candidate_count", "Catalog candidate count"),
        ("catalog_lookup_query", "Latest catalog lookup query"),
        ("catalog_lookup_status", "Latest catalog lookup status"),
        ("catalog_lookup_match_count", "Latest catalog lookup match count"),
        ("catalog_lookup_at", "Latest catalog lookup time"),
        ("availability_item_code", "Latest availability item code"),
        ("availability_item_name", "Latest availability item name"),
        ("availability_in_stock", "Latest availability in stock"),
        ("availability_total_available_qty", "Latest availability total available quantity"),
        ("availability_stock_uom", "Latest availability stock unit"),
        ("availability_warehouse", "Latest availability warehouse"),
        ("availability_default_warehouse", "Latest availability default warehouse"),
        ("availability_known_warehouses", "Latest availability known warehouses"),
        ("availability_needs_warehouse_selection", "Latest availability needs warehouse selection"),
        ("availability_checked_at", "Latest availability check time"),
        ("quantity", "Quantity"),
        ("uom", "Unit"),
        ("requested_item_count", "Requested item count"),
        ("requested_items_have_quantities", "Requested items have quantities"),
        ("requested_items_need_uom_confirmation", "Requested items need UOM confirmation"),
        ("requested_items_assumed_uom", "Requested items assumed UOM"),
        ("requested_items_uom_assumption_status", "Requested items UOM assumption status"),
        ("urgency", "Urgency"),
        ("delivery_need", "Delivery need"),
        ("decision_status", "Decision status"),
        ("duplicate_of_lead_id", "Duplicate of lead id"),
        ("dedupe_reason", "Duplicate detection reason"),
        ("merged_into_lead_id", "Merged into lead id"),
        ("order_correction_status", "Order correction status"),
        ("target_order_id", "Target order id for correction"),
        ("correction_type", "Order correction type"),
        ("quote_status", "Quote status"),
        ("quote_id", "Quote id"),
        ("quote_total", "Quote total"),
        ("quote_currency", "Quote currency"),
        ("expected_revenue", "Expected revenue"),
        ("order_total", "Order total"),
        ("currency", "Currency"),
        ("won_revenue", "Won revenue"),
        ("followup_count", "Follow-up count"),
        ("lost_reason", "Lost reason"),
        ("sales_owner_status", "Sales owner status"),
        ("playbook_version", "Playbook version"),
    ]:
        value = lead_profile.get(key)
        if value not in (None, "", []):
            lines.append(f"{label}: {value}")
    if lead_profile.get("price_sensitivity"):
        lines.append("Customer appears price-sensitive.")
    if isinstance(lead_profile.get("requested_items"), list) and lead_profile.get("requested_items"):
        lines.append(f"Requested items: {lead_profile['requested_items']}")
    if lead_profile.get("do_not_contact"):
        lines.append("Customer must not receive proactive follow-up.")
    return lines


def _lead_state_guard_lines(lead_profile: dict[str, Any] | None) -> list[str]:
    if not isinstance(lead_profile, dict):
        return []
    lines: list[str] = []
    product_interest = lead_profile.get("product_interest")
    quantity = lead_profile.get("quantity")
    uom = lead_profile.get("uom")
    selected_item_name = lead_profile.get("catalog_item_name")
    selected_item_code = lead_profile.get("catalog_item_code")
    if selected_item_name or selected_item_code:
        resolved_item = str(selected_item_name or selected_item_code)
        if selected_item_code and selected_item_name:
            resolved_item = f"{selected_item_name} ({selected_item_code})"
        lines.append(
            f"Exact catalog item is already resolved as {resolved_item}. Do not ask again for the exact model or variant unless the customer changes the product."
        )
    if product_interest:
        if lead_profile.get("product_resolution_status") == "broad":
            lines.append(
                f"Known product category is already established as {product_interest}. Do not ask the customer to repeat that category."
            )
        else:
            lines.append(
                f"Known product is already established as {product_interest}. Do not ask the customer to repeat it."
            )
    if quantity not in (None, "", []):
        lines.append(f"Known quantity is already {quantity}. Do not ask for quantity again unless the customer changes it.")
    if uom and not lead_profile.get("requested_items_need_uom_confirmation"):
        lines.append(f"Known unit is already {uom}. Do not ask for unit or package again unless the customer changes it.")
    next_action = str(lead_profile.get("next_action") or "")
    if next_action == "show_matching_options":
        lines.append("The next step is to show matching catalog options, not to ask again for already known product, quantity, or unit details.")
    elif next_action == "select_specific_item":
        lines.append("The next step is to resolve the exact model or variant only; do not re-ask already known product category, quantity, or unit.")
    if lead_profile.get("catalog_lookup_status") == "no_match" and lead_profile.get("catalog_lookup_query"):
        lines.append(
            f"No matching catalog items were found for {lead_profile.get('catalog_lookup_query')}. Do not claim this product is in the catalog without a new matching tool result."
        )
    return lines


def _tenant_context_lines(tenant: dict[str, Any], lang: str) -> list[str]:
    if lang == "auto":
        lines = ["Customer reply language for this turn: auto-detect from the customer's message and reply in that same language."]
    else:
        lines = [f"Customer reply language for this turn: {lang}."]
    if tenant.get("company_name"):
        lines.append(f"Company name: {tenant['company_name']}")
    if tenant.get("company_code"):
        lines.append(f"Tenant company code: {tenant['company_code']}")
    return lines


def _tenant_policy_lines(tenant: dict[str, Any]) -> list[str]:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    resolved_sales_policy = sales_policy(ai_policy)
    lines: list[str] = []
    if not bool(resolved_sales_policy.get("allow_discount_promises", False)):
        lines.append("Do not promise discounts or special commercial terms unless they come from a tool result.")
    if not bool(resolved_sales_policy.get("allow_stock_promises_without_tool", False)):
        lines.append("Do not promise stock or availability without a tool result.")
    if not bool(resolved_sales_policy.get("allow_delivery_promises_without_tool", False)):
        lines.append("Do not promise delivery timing without a tool result or explicit tenant policy.")
    if resolved_sales_policy.get("minimum_order_total") not in (None, ""):
        lines.append(f"Minimum order total policy: {resolved_sales_policy.get('minimum_order_total')}.")
    try:
        default_delivery_days = int(resolved_sales_policy.get("default_delivery_days") or 0)
    except (TypeError, ValueError):
        default_delivery_days = 0
    if default_delivery_days > 0:
        lines.append(f"Default earliest delivery offset: {resolved_sales_policy.get('default_delivery_days')} day(s).")
    if not bool(ai_policy.get("allow_free_text_catalog_answers", True)):
        lines.append("For catalog questions, rely on tool-backed product data instead of free-text assumptions.")
    if not bool(ai_policy.get("allow_invoice", True)):
        lines.append("Do not offer invoice creation for this tenant unless the customer is handed off.")
    if not bool(ai_policy.get("allow_license_ops", True)):
        lines.append("Do not offer license creation or subscription extension in this tenant flow.")
    allowed_tools = ai_policy.get("allowed_tools")
    if isinstance(allowed_tools, list) and allowed_tools:
        lines.append(f"Enabled tools for this tenant: {', '.join(str(tool) for tool in allowed_tools)}")
    return lines


def build_runtime_system_prompt(
    *,
    tenant: dict[str, Any],
    lang: str,
    channel: str,
    stage: str | None,
    behavior_class: str | None,
    buyer_name: str | None = None,
    erp_customer_id: str | None = None,
    last_sales_order_name: str | None = None,
    recent_sales_orders: list[dict[str, Any]] | None = None,
    recent_sales_invoices: list[dict[str, Any]] | None = None,
    lead_profile: dict[str, Any] | None = None,
    handoff_required: bool = False,
    handoff_reason: str | None = None,
) -> str:
    company_name = tenant.get("company_name") or "this company"
    resolved_stage = stage if stage in STAGE_PROMPTS else DEFAULT_STAGE
    resolved_behavior = behavior_class if behavior_class in BEHAVIOR_PROMPTS else DEFAULT_BEHAVIOR_CLASS
    custom_prompt = str(tenant.get("ai_system_prompt") or "").strip()
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    prompt_overrides = ai_policy.get("prompt_overrides") if isinstance(ai_policy.get("prompt_overrides"), dict) else {}
    core_policy = _append_override_lines(CORE_POLICY, prompt_overrides.get("core_policy"))
    language_policy = _append_override_lines(LANGUAGE_POLICY, prompt_overrides.get("language_policy"))
    catalog_policy = _append_override_lines(CATALOG_POLICY, prompt_overrides.get("catalog_policy"))
    order_policy = _append_override_lines(ORDER_POLICY, prompt_overrides.get("order_policy"))
    service_policy = _append_override_lines(SERVICE_POLICY, prompt_overrides.get("service_policy"))
    sales_playbook = _append_override_lines(SALES_PLAYBOOK, prompt_overrides.get("sales_playbook"))
    stage_prompts = _merge_prompt_map(STAGE_PROMPTS, prompt_overrides.get("stage_prompts"))
    behavior_prompts = _merge_prompt_map(BEHAVIOR_PROMPTS, prompt_overrides.get("behavior_prompts"))
    channel_prompts = _merge_prompt_map(CHANNEL_PROMPTS, prompt_overrides.get("channel_prompts"))
    resolved_stage = stage if stage in stage_prompts else DEFAULT_STAGE
    resolved_behavior = behavior_class if behavior_class in behavior_prompts else DEFAULT_BEHAVIOR_CLASS

    lines: list[str] = [
        f"You are the AI sales manager for {company_name}.",
        "You work inside customer messaging channels and can use tools to access ERP-backed business data.",
        "",
    ]
    lines.extend(_section("Core policy", core_policy))
    lines.append("")
    lines.extend(_section("Language policy", language_policy))
    lines.append("")
    lines.extend(_section("Catalog and unit policy", catalog_policy))
    lines.append("")
    lines.extend(_section("Order policy", order_policy))
    lines.append("")
    lines.extend(_section("Service policy", service_policy))
    lines.append("")
    lines.extend(_section("Inbound sales playbook", sales_playbook))
    lines.append("")
    lines.extend(_section("Tenant context", _tenant_context_lines(tenant, lang)))
    tenant_policy = _tenant_policy_lines(tenant)
    if tenant_policy:
        lines.append("")
        lines.extend(_section("Tenant policy", tenant_policy))
    buyer_context = _buyer_context_lines(
        buyer_name=buyer_name,
        erp_customer_id=erp_customer_id,
        last_sales_order_name=last_sales_order_name,
        recent_sales_orders=recent_sales_orders,
        recent_sales_invoices=recent_sales_invoices,
    )
    if buyer_context:
        lines.append("")
        lines.extend(_section("Buyer context", buyer_context))
    lead_profile_context = _lead_profile_lines(lead_profile)
    if lead_profile_context:
        lines.append("")
        lines.extend(_section("Lead profile", lead_profile_context))
    lead_state_guards = _lead_state_guard_lines(lead_profile)
    if lead_state_guards:
        lines.append("")
        lines.extend(_section("Lead state guards", lead_state_guards))
    lines.append("")
    lines.extend(
        _section(
            "Conversation routing context",
            [
                f"Current stage: {resolved_stage}",
                f"Current behavior class: {resolved_behavior}",
                f"Channel: {channel}",
            ],
        )
    )
    lines.append("")
    lines.extend(_section("Stage guidance", stage_prompts[resolved_stage]))
    lines.append("")
    lines.extend(_section("Behavior guidance", behavior_prompts[resolved_behavior]))

    channel_guidance = channel_prompts.get(channel, [])
    if channel_guidance:
        lines.append("")
        lines.extend(_section("Channel guidance", channel_guidance))

    if handoff_required:
        handoff_lines = [
            "A human handoff is required in this conversation.",
            "Keep the reply short and do not improvise on uncertain business details.",
        ]
        if handoff_reason:
            handoff_lines.append(f"Handoff reason: {handoff_reason}")
        lines.append("")
        lines.extend(_section("Handoff policy", handoff_lines))

    if custom_prompt:
        lines.append("")
        lines.extend(_section("Tenant custom instructions", [custom_prompt]))

    return "\n".join(lines).strip()
