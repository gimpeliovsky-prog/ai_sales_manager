from __future__ import annotations

from typing import Any

from app.interaction_patterns import has_explicit_confirmation
from app.sales_policy import minimum_order_violation, sales_policy


def _deny(tool_name: str, reason: str, next_action: str) -> dict[str, Any]:
    return {
        "error": reason,
        "blocked_by_policy": True,
        "tool_name": tool_name,
        "next_action": next_action,
    }


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
    if isinstance(allowed_tools, list) and allowed_tools and tool_name not in allowed_tools:
        return _deny(
            tool_name,
            f"Tool '{tool_name}' is not enabled for this tenant.",
            "Continue with allowed tools or hand off the conversation.",
        )

    stage = str(session.get("stage") or "")
    last_intent = str(session.get("last_intent") or "")
    has_customer = bool(session.get("erp_customer_id"))
    active_order_name = str(session.get("last_sales_order_name") or "").strip()
    requested_order_name = str(inputs.get("sales_order_name") or "").strip()
    has_items = isinstance(inputs.get("items"), list) and bool(inputs.get("items"))
    lead_profile = session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else {}
    needs_multi_item_uom_confirmation = bool(lead_profile.get("requested_items_need_uom_confirmation"))

    if tool_name == "get_product_catalog":
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
        if stage not in {"order_build", "confirm"}:
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
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "There is no active sales order to update.",
                "Create an order first or ask which order should be updated.",
            )
        if stage in {"identify", "handoff"}:
            return _deny(
                tool_name,
                f"Sales order update is blocked during stage '{stage}'.",
                "Resolve identification or handoff before changing the order.",
            )
        if not has_items:
            return _deny(
                tool_name,
                "Sales order update requires items to add.",
                "Ask what item and quantity should be added.",
            )
        if needs_multi_item_uom_confirmation:
            assumed_uom = str(lead_profile.get("requested_items_assumed_uom") or "box")
            return _deny(
                tool_name,
                f"Multi-item order UOM is still only a likely assumption ({assumed_uom}).",
                "Ask the customer to confirm whether the listed quantities are boxes or another unit before updating the order.",
            )
        return None

    if tool_name == "create_invoice":
        if not (requested_order_name or active_order_name):
            return _deny(
                tool_name,
                "Cannot create an invoice without a sales order.",
                "Create or locate the sales order first.",
            )
        if stage not in {"invoice", "service", "closed"}:
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
        if stage not in {"service", "closed"} and last_intent != "service_request":
            return _deny(
                tool_name,
                f"Tool '{tool_name}' is reserved for service flows.",
                "Handle the customer's service request explicitly before using this tool.",
            )
        return None

    return None
