from __future__ import annotations

from typing import Any

from app.conversation_contexts import (
    active_context_type,
    active_deal_state,
    active_lead_profile,
    active_progress_state,
    active_related_order_id,
    active_signal_state,
)
from app.interaction_patterns import has_explicit_confirmation
from app.sales_policy import minimum_order_violation, sales_policy


def _deny(tool_name: str, reason: str, next_action: str) -> dict[str, Any]:
    return {
        "error": reason,
        "blocked_by_policy": True,
        "tool_name": tool_name,
        "next_action": next_action,
    }


def _tool_enabled_for_tenant(tool_name: str, allowed_tools: Any) -> bool:
    if not isinstance(allowed_tools, list) or not allowed_tools:
        return True
    normalized = {str(item).strip() for item in allowed_tools if str(item).strip()}
    if tool_name in normalized:
        return True
    supporting_order_tools = {"update_sales_order", "send_sales_order_pdf", "create_invoice"}
    if tool_name == "get_sales_order_status" and normalized.intersection(supporting_order_tools):
        return True
    return False


def evaluate_tool_call(
    *,
    tool_name: str,
    inputs: dict[str, Any],
    session: dict[str, Any],
    tenant: dict[str, Any],
    user_text: str,
    confirmation_override: bool | None = None,
) -> dict[str, Any] | None:
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    resolved_sales_policy = sales_policy(ai_policy)
    if session.get("handoff_required"):
        return _deny(
            tool_name,
            "Tool call blocked because this conversation already requires human handoff.",
            "Tell the customer that a manager will continue the conversation.",
        )

    allowed_tools = ai_policy.get("allowed_tools")
    if not _tool_enabled_for_tenant(tool_name, allowed_tools):
        return _deny(
            tool_name,
            f"Tool '{tool_name}' is not enabled for this tenant.",
            "Continue with allowed tools or hand off the conversation.",
        )

    stage = str(session.get("stage") or "")
    last_intent = str(session.get("last_intent") or "")
    has_customer = bool(session.get("erp_customer_id"))
    context_type = active_context_type(session)
    deal_state = active_deal_state(session)
    progress_state = active_progress_state(session)
    signal_state = active_signal_state(session)
    lead_profile = active_lead_profile(session)
    active_order_name = str(active_related_order_id(session) or session.get("last_sales_order_name") or "").strip()
    requested_order_name = str(inputs.get("sales_order_name") or "").strip()
    has_items = isinstance(inputs.get("items"), list) and bool(inputs.get("items"))
    needs_multi_item_uom_confirmation = bool(lead_profile.get("requested_items_need_uom_confirmation"))
    separate_order_requested = bool(lead_profile.get("separate_order_requested"))
    quote_status = str(progress_state.get("quote_status") or lead_profile.get("quote_status") or "").strip()
    signal_type = str(signal_state.get("type") or session.get("signal_type") or "").strip()
    missing_slots = [
        str(item).strip()
        for item in (progress_state.get("missing_slots") if isinstance(progress_state.get("missing_slots"), list) else lead_profile.get("missing_slots") if isinstance(lead_profile.get("missing_slots"), list) else [])
        if str(item).strip()
    ]
    if has_items:
        supplied_item_codes = [
            str(item.get("item_code") or "").strip()
            for item in inputs.get("items")
            if isinstance(item, dict)
        ]
        supplied_qty = any(
            isinstance(item, dict) and item.get("qty") not in (None, "", 0, 0.0)
            for item in inputs.get("items")
        )
        supplied_uom = any(
            isinstance(item, dict) and str(item.get("uom") or "").strip()
            for item in inputs.get("items")
        )
        if any(supplied_item_codes):
            missing_slots = [slot for slot in missing_slots if slot not in {"product_interest", "specific_item"}]
        if supplied_qty:
            missing_slots = [slot for slot in missing_slots if slot != "quantity"]
        if supplied_uom:
            missing_slots = [slot for slot in missing_slots if slot != "uom"]
    blocking_missing_slots = [slot for slot in missing_slots if slot != "confirmation"]
    if progress_state.get("stage"):
        stage = str(progress_state.get("stage") or "").strip()
    if signal_state.get("intent"):
        last_intent = str(signal_state.get("intent") or "").strip()

    if tool_name == "get_product_catalog":
        return None

    if tool_name == "get_item_availability":
        if not str(inputs.get("item_code") or "").strip():
            return _deny(
                tool_name,
                "Stock availability requires a specific catalog item code.",
                "Look up the product in the catalog first and then check availability for the exact item.",
            )
        return None

    if tool_name == "get_sales_order_status":
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "There is no active sales order to check.",
                "Ask which order should be checked.",
            )
        return None

    if tool_name == "get_buyer_sales_history":
        if not has_customer:
            return _deny(
                tool_name,
                "Buyer sales history is unavailable before the buyer is identified.",
                "Identify or register the buyer first.",
            )
        return None

    if tool_name == "register_buyer":
        if not str(inputs.get("full_name") or "").strip():
            return _deny(
                tool_name,
                "Buyer registration requires a customer name.",
                "Ask the customer for their full name first.",
            )
        return None

    if tool_name == "create_sales_order":
        if not has_customer and not resolved_sales_policy.get("allow_order_without_registered_customer"):
            return _deny(
                tool_name,
                "Cannot create a sales order before the buyer is identified.",
                "Identify or register the buyer first.",
            )
        if (
            not separate_order_requested
            and bool(lead_profile.get("target_order_id") or active_order_name)
            and stage in {"invoice", "service", "closed"}
            and str(progress_state.get("next_action") or lead_profile.get("next_action") or "").strip() == "send_order_or_offer_invoice"
        ):
            return _deny(
                tool_name,
                "A sales order has already been created in this conversation context.",
                "Offer the current order PDF, invoice, or ask whether the customer wants to start a separate new order.",
            )
        if stage not in {"order_build", "confirm"} and context_type not in {"new_purchase", "quote_negotiation"} and not (separate_order_requested and stage in {"invoice", "service", "closed"}):
            return _deny(
                tool_name,
                f"Sales order creation is not allowed from stage '{stage or 'unknown'}'.",
                "Clarify the order details and get to order confirmation first.",
            )
        if not has_items:
            return _deny(
                tool_name,
                "Sales order creation requires at least one item.",
                "Ask which item and quantity the customer wants.",
            )
        if blocking_missing_slots:
            return _deny(
                tool_name,
                "Sales order creation is blocked because required order details are still missing in the active context.",
                f"Resolve the missing details first: {', '.join(blocking_missing_slots)}.",
            )
        if needs_multi_item_uom_confirmation:
            assumed_uom = str(lead_profile.get("requested_items_assumed_uom") or "box")
            return _deny(
                tool_name,
                f"Multi-item order UOM is still only a likely assumption ({assumed_uom}).",
                "Ask the customer to confirm whether the listed quantities are boxes or another unit before creating the order.",
            )
        minimum_violation = minimum_order_violation(inputs.get("items"), ai_policy)
        if minimum_violation:
            return _deny(
                tool_name,
                "Sales order is below this tenant's minimum order total.",
                "Explain the minimum order total and ask whether to adjust the order.",
            )
        if confirmation_override is False:
            return _deny(
                tool_name,
                "Sales order creation requires explicit customer confirmation in the current conversation context.",
                "Ask the customer to confirm the current order contents.",
            )
        if not (has_explicit_confirmation(user_text) or confirmation_override is True):
            return _deny(
                tool_name,
                "Sales order creation requires clear customer confirmation.",
                "Ask the customer to confirm the current order contents.",
            )
        return None

    if tool_name == "update_sales_order":
        if not has_customer:
            return _deny(
                tool_name,
                "Cannot update a sales order before the buyer is identified.",
                "Identify the buyer first.",
            )
        if separate_order_requested:
            return _deny(
                tool_name,
                "The customer asked for a separate new order, not a change to the current order.",
                "Create a new sales order instead of updating the current one.",
            )
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "There is no active sales order to update.",
                "Create an order first or ask which order should be updated.",
            )
        if stage in {"identify", "handoff"} or context_type == "identity_resolution":
            return _deny(
                tool_name,
                f"Sales order update is blocked during stage '{stage}'.",
                "Resolve identification or handoff before changing the order.",
            )
        if not has_items:
            return _deny(
                tool_name,
                "Sales order update requires at least one correction operation.",
                "Ask which order line should be added, updated, or removed.",
            )
        if blocking_missing_slots:
            return _deny(
                tool_name,
                "Sales order update is blocked because required order details are still missing in the active context.",
                f"Resolve the missing details first: {', '.join(blocking_missing_slots)}.",
            )
        if needs_multi_item_uom_confirmation:
            assumed_uom = str(lead_profile.get("requested_items_assumed_uom") or "box")
            return _deny(
                tool_name,
                f"Multi-item order UOM is still only a likely assumption ({assumed_uom}).",
                "Ask the customer to confirm whether the listed quantities are boxes or another unit before updating the order.",
            )
        if confirmation_override is False:
            return _deny(
                tool_name,
                "Sales order update requires explicit customer confirmation in the current conversation context.",
                "Ask the customer to confirm the requested order change.",
            )
        return None

    if tool_name == "create_invoice":
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "Cannot create an invoice without a sales order.",
                "Create or locate the sales order first.",
            )
        if stage not in {"invoice", "service", "closed"} and context_type not in {"service_request", "quote_negotiation"}:
            return _deny(
                tool_name,
                f"Invoice creation is not allowed from stage '{stage or 'unknown'}'.",
                "Finish the order flow first, then offer the invoice.",
            )
        return None

    if tool_name == "send_sales_order_pdf":
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "There is no sales order PDF to send yet.",
                "Create or locate the order before sending its PDF.",
            )
        return None

    if tool_name in {"create_license", "extend_subscription"}:
        if stage not in {"service", "closed"} and context_type != "service_request" and signal_type != "service_request" and last_intent != "service_request":
            return _deny(
                tool_name,
                f"Tool '{tool_name}' is reserved for service flows.",
                "Handle the customer's service request explicitly before using this tool.",
            )
        return None

    return None
