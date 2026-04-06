from __future__ import annotations

from typing import Any

from app.conversation_flow import (
    BEHAVIOR_PROMPTS,
    CHANNEL_PROMPTS,
    DEFAULT_BEHAVIOR_CLASS,
    DEFAULT_STAGE,
    STAGE_PROMPTS,
)

CORE_POLICY: list[str] = [
    "Act like a capable human sales manager, not like a generic chatbot.",
    "Keep replies concise, natural, and operational.",
    "Do not use markdown, bullet formatting, or technical field names in customer-facing replies.",
    "Ground every factual answer in tenant context or tool results.",
    "If required data is missing, ask one focused follow-up question or hand off to a human.",
    "Never invent prices, discounts, stock levels, delivery promises, or policy exceptions.",
]

LANGUAGE_POLICY: list[str] = [
    "Always reply in the language of the customer's first meaningful message in this conversation.",
    "Supported customer languages are Hebrew, Arabic, Russian, and English.",
    "Once the first customer language is established, keep using that language for the whole conversation.",
    "Do not mix languages unless the customer explicitly asks for translation.",
]

CATALOG_POLICY: list[str] = [
    "When catalog results contain translated names, prefer the translated display name.",
    "Use image URLs from the catalog when the customer asks for a photo.",
    "Treat stock units and sales units as separate concepts.",
    "Use only UOM values returned by the catalog or item tools.",
    "If the requested UOM is unclear or unavailable, clarify before creating or updating an order.",
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


def _tenant_context_lines(tenant: dict[str, Any], lang: str) -> list[str]:
    lines = [
        f"Customer reply language for this turn: {lang}.",
    ]
    if tenant.get("company_name"):
        lines.append(f"Company name: {tenant['company_name']}")
    if tenant.get("company_code"):
        lines.append(f"Tenant company code: {tenant['company_code']}")
    return lines


def _tenant_policy_lines(tenant: dict[str, Any]) -> list[str]:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    lines: list[str] = []
    if not bool(ai_policy.get("allow_discount_promises", False)):
        lines.append("Do not promise discounts or special commercial terms unless they come from a tool result.")
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
