from __future__ import annotations

from typing import Any

from app.conversation_contexts import (
    active_context_type,
    active_lead_profile,
    active_progress_state,
    active_related_order_id,
    active_signal_state,
)
from app.interaction_patterns import has_explicit_confirmation
from app.lead_management import normalize_lead_profile
from app.sales_policy import minimum_order_violation, sales_policy


def _deny(tool_name: str, reason: str, next_action: str) -> dict[str, Any]:
    return {
        "error": reason,
        "blocked_by_policy": True,
        "tool_name": tool_name,
        "next_action": next_action,
    }


def _format_quantity(value: Any) -> str:
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    return str(value)


def _summarize_order_details(lead_profile: dict[str, Any]) -> str:
    quantity = lead_profile.get("quantity")
    product_name = str(
        lead_profile.get("catalog_item_name")
        or lead_profile.get("product_interest")
        or "the item"
    ).strip()
    uom = str(lead_profile.get("uom") or "").strip()
    parts: list[str] = []
    if quantity not in (None, "", []):
        parts.append(_format_quantity(quantity))
    if product_name:
        parts.append(product_name)
    summary = " ".join(parts).strip() or "the order details"
    if uom:
        summary += f" as {uom}"
    delivery_need = str(lead_profile.get("delivery_need") or lead_profile.get("urgency") or "").strip()
    if delivery_need:
        summary += f", delivery {delivery_need}"
    return summary


def _next_action_customer_reply(*, next_action: str, lead_profile: dict[str, Any], tool_name: str) -> str | None:
    product_name = str(
        lead_profile.get("catalog_item_name")
        or lead_profile.get("product_interest")
        or "this item"
    ).strip()
    quantity = lead_profile.get("quantity")
    if next_action == "ask_delivery_timing":
        if tool_name == "update_sales_order":
            return "I still need one detail before I can update the order: what delivery date or timing should I use?"
        return "I still need one detail before I can create the order: what delivery date or timing should I use?"
    if next_action == "ask_unit":
        if quantity not in (None, "", []):
            return f"I still need one detail before I can continue: what unit or package should I use for {_format_quantity(quantity)} {product_name}?"
        return "I still need one detail before I can continue: what unit or package should I use?"
    if next_action == "ask_quantity":
        if product_name and product_name != "this item":
            return f"I still need one detail before I can continue: how many {product_name} do you need?"
        return "I still need one detail before I can continue: what quantity do you need?"
    if next_action == "ask_contact":
        return "Before I can continue with the order, I need the buyer details."
    if next_action == "quote_or_clarify_price":
        return "I still need to finish the price or quote details before I can continue with the order."
    if next_action == "confirm_order":
        return f"Please confirm the order contents one last time: {_summarize_order_details(lead_profile)}."
    return None


def _build_order_policy_state(
    *,
    tool_name: str,
    lead_profile: dict[str, Any],
    progress_state: dict[str, Any],
    signal_state: dict[str, Any],
    stage: str,
    context_type: str,
    has_customer: bool,
    active_order_name: str,
    requested_order_name: str,
    separate_order_requested: bool,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    has_items = isinstance(inputs.get("items"), list) and bool(inputs.get("items"))
    item_entries = inputs.get("items") if isinstance(inputs.get("items"), list) else []
    supplied_item_codes = [
        str(item.get("item_code") or "").strip()
        for item in item_entries
        if isinstance(item, dict)
    ]
    supplied_qty = any(
        isinstance(item, dict) and item.get("qty") not in (None, "", 0, 0.0)
        for item in item_entries
    )
    supplied_uom = any(
        isinstance(item, dict) and str(item.get("uom") or "").strip()
        for item in item_entries
    )
    profile_item_code = str(lead_profile.get("catalog_item_code") or "").strip()
    profile_qty = lead_profile.get("quantity")
    profile_uom = str(lead_profile.get("uom") or "").strip()
    line_ready_from_profile = bool(profile_item_code and profile_qty not in (None, "", 0, 0.0) and profile_uom)
    has_orderable_line = has_items or line_ready_from_profile
    next_action = str(progress_state.get("next_action") or lead_profile.get("next_action") or "").strip()
    missing_slots = [
        str(item).strip()
        for item in (
            progress_state.get("missing_slots")
            if isinstance(progress_state.get("missing_slots"), list)
            else lead_profile.get("missing_slots")
            if isinstance(lead_profile.get("missing_slots"), list)
            else []
        )
        if str(item).strip()
    ]
    if has_orderable_line:
        if any(supplied_item_codes) or profile_item_code:
            missing_slots = [slot for slot in missing_slots if slot not in {"product_interest", "specific_item"}]
        if supplied_qty or profile_qty not in (None, "", 0, 0.0):
            missing_slots = [slot for slot in missing_slots if slot != "quantity"]
        if supplied_uom or profile_uom:
            missing_slots = [slot for slot in missing_slots if slot != "uom"]
    blocking_missing_slots = [slot for slot in missing_slots if slot != "confirmation"]
    return {
        "tool_name": tool_name,
        "lead_profile": lead_profile,
        "progress_state": progress_state,
        "signal_state": signal_state,
        "stage": stage,
        "context_type": context_type,
        "has_customer": has_customer,
        "active_order_name": active_order_name,
        "requested_order_name": requested_order_name,
        "separate_order_requested": separate_order_requested,
        "next_action": next_action,
        "has_items": has_items,
        "has_orderable_line": has_orderable_line,
        "missing_slots": missing_slots,
        "blocking_missing_slots": blocking_missing_slots,
        "needs_multi_item_uom_confirmation": bool(lead_profile.get("requested_items_need_uom_confirmation")),
    }


def _evaluate_order_execution_readiness(
    *,
    state: dict[str, Any],
    tool_name: str,
    ai_policy: dict[str, Any],
    user_text: str,
    confirmation_override: bool | None,
) -> dict[str, Any]:
    lead_profile = state["lead_profile"]
    next_action = str(state.get("next_action") or "").strip()
    active_order_name = str(state.get("active_order_name") or "").strip()
    requested_order_name = str(state.get("requested_order_name") or "").strip()
    blocking_missing_slots = list(state.get("blocking_missing_slots") or [])
    base: dict[str, Any] = {
        "tool_name": tool_name,
        "order_step": "ready_to_execute",
        "ready_to_execute": True,
        "next_action": next_action,
        "missing_slots": list(state.get("missing_slots") or []),
        "blocking_missing_slots": blocking_missing_slots,
        "customer_reply": None,
        "reason_code": "ready_to_execute",
    }
    if tool_name == "create_sales_order":
        if not state["has_customer"] and not sales_policy(ai_policy).get("allow_order_without_registered_customer"):
            base.update(
                {
                    "order_step": "buyer_identification_required",
                    "ready_to_execute": False,
                    "reason_code": "buyer_identification_required",
                    "customer_reply": "Before I can create the order, I need to identify or register the buyer.",
                }
            )
            return base
        if (
            not state["separate_order_requested"]
            and bool(lead_profile.get("target_order_id") or active_order_name)
            and state["stage"] in {"invoice", "service", "closed"}
            and next_action == "send_order_or_offer_invoice"
        ):
            order_name = active_order_name or str(lead_profile.get("target_order_id") or "").strip()
            reply = "A sales order has already been created in this conversation."
            if order_name:
                reply = f"The current order {order_name} is already created. If you want, I can send the order PDF or create an invoice."
            base.update(
                {
                    "order_step": "already_created",
                    "ready_to_execute": False,
                    "reason_code": "already_created",
                    "customer_reply": reply,
                }
            )
            return base
        if state["stage"] not in {"order_build", "confirm"} and state["context_type"] not in {"new_purchase", "quote_negotiation"} and not (state["separate_order_requested"] and state["stage"] in {"invoice", "service", "closed"}):
            base.update(
                {
                    "order_step": "wrong_context",
                    "ready_to_execute": False,
                    "reason_code": "wrong_context",
                    "customer_reply": "I still need to finish the order details before I can create the order.",
                }
            )
            return base
    else:
        if not state["has_customer"]:
            base.update(
                {
                    "order_step": "buyer_identification_required",
                    "ready_to_execute": False,
                    "reason_code": "buyer_identification_required",
                    "customer_reply": "Before I can update the order, I need to identify the buyer.",
                }
            )
            return base
        if state["separate_order_requested"]:
            base.update(
                {
                    "order_step": "separate_order_requested",
                    "ready_to_execute": False,
                    "reason_code": "separate_order_requested",
                    "customer_reply": "You asked for a separate new order, so I should create a new order instead of changing the current one.",
                }
            )
            return base
        if not (requested_order_name or active_order_name):
            base.update(
                {
                    "order_step": "missing_order_target",
                    "ready_to_execute": False,
                    "reason_code": "missing_order_target",
                    "customer_reply": "There is no active sales order to update yet.",
                }
            )
            return base
    if not state["has_orderable_line"]:
        base.update(
            {
                "order_step": "missing_items",
                "ready_to_execute": False,
                "reason_code": "missing_items",
                "customer_reply": "I still need the exact item, quantity, and unit before I can continue with the order.",
            }
        )
        return base
    if blocking_missing_slots:
        base.update(
            {
                "order_step": "missing_details",
                "ready_to_execute": False,
                "reason_code": "missing_details",
                "customer_reply": _next_action_customer_reply(next_action=next_action, lead_profile=lead_profile, tool_name=tool_name)
                or "I still need one more detail before I can continue with the order.",
            }
        )
        return base
    if state["needs_multi_item_uom_confirmation"]:
        assumed_uom = str(lead_profile.get("requested_items_assumed_uom") or "box")
        base.update(
            {
                "order_step": "uom_confirmation_required",
                "ready_to_execute": False,
                "reason_code": "uom_confirmation_required",
                "customer_reply": f"I still need to confirm the unit for the listed items. Right now it only looks like {assumed_uom}.",
            }
        )
        return base
    minimum_violation = minimum_order_violation((state.get("inputs") or {}).get("items"), ai_policy)
    if minimum_violation:
        base.update(
            {
                "order_step": "minimum_order_total",
                "ready_to_execute": False,
                "reason_code": "minimum_order_total",
                "customer_reply": "This order is still below the tenant's minimum order total.",
            }
        )
        return base
    if confirmation_override is False:
        base.update(
            {
                "order_step": "awaiting_confirmation",
                "ready_to_execute": False,
                "reason_code": "awaiting_confirmation",
                "customer_reply": _next_action_customer_reply(next_action="confirm_order", lead_profile=lead_profile, tool_name=tool_name),
            }
        )
        return base
    if not (has_explicit_confirmation(user_text) or confirmation_override is True):
        base.update(
            {
                "order_step": "awaiting_confirmation",
                "ready_to_execute": False,
                "reason_code": "awaiting_confirmation",
                "customer_reply": _next_action_customer_reply(next_action="confirm_order", lead_profile=lead_profile, tool_name=tool_name),
            }
        )
        return base
    return base


def evaluate_order_execution_readiness(
    *,
    tool_name: str,
    session: dict[str, Any],
    tenant: dict[str, Any],
    inputs: dict[str, Any],
    user_text: str,
    confirmation_override: bool | None = None,
) -> dict[str, Any]:
    progress_state = active_progress_state(session)
    lead_profile = active_lead_profile(session)
    state = _build_order_policy_state(
        tool_name=tool_name,
        lead_profile=lead_profile,
        progress_state=progress_state,
        signal_state=active_signal_state(session),
        stage=str(progress_state.get("stage") or session.get("stage") or "").strip(),
        context_type=active_context_type(session),
        has_customer=bool(session.get("erp_customer_id")),
        active_order_name=str(active_related_order_id(session) or session.get("last_sales_order_name") or "").strip(),
        requested_order_name=str(inputs.get("sales_order_name") or "").strip(),
        separate_order_requested=bool(lead_profile.get("separate_order_requested")),
        inputs=inputs,
    )
    state["inputs"] = inputs
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {}
    return _evaluate_order_execution_readiness(
        state=state,
        tool_name=tool_name,
        ai_policy=ai_policy,
        user_text=user_text,
        confirmation_override=confirmation_override,
    )


def prompt_order_execution_readiness(
    *,
    tool_name: str,
    tenant: dict[str, Any],
    stage: str | None,
    lead_profile: dict[str, Any] | None,
    has_customer: bool,
    context_type: str | None = None,
    active_order_name: str | None = None,
) -> dict[str, Any]:
    state = _build_order_policy_state(
        tool_name=tool_name,
        lead_profile=normalize_lead_profile(lead_profile),
        progress_state={},
        signal_state={},
        stage=str(stage or "").strip(),
        context_type=str(context_type or "new_purchase").strip() or "new_purchase",
        has_customer=has_customer,
        active_order_name=str(active_order_name or "").strip(),
        requested_order_name="",
        separate_order_requested=bool((lead_profile or {}).get("separate_order_requested")),
        inputs={},
    )
    return _evaluate_order_execution_readiness(
        state=state,
        tool_name=tool_name,
        ai_policy=tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else {},
        user_text="",
        confirmation_override=None,
    )


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
    progress_state = active_progress_state(session)
    signal_state = active_signal_state(session)
    lead_profile = active_lead_profile(session)
    active_order_name = str(active_related_order_id(session) or session.get("last_sales_order_name") or "").strip()
    requested_order_name = str(inputs.get("sales_order_name") or "").strip()
    signal_type = str(signal_state.get("type") or session.get("signal_type") or "").strip()
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
        readiness = evaluate_order_execution_readiness(
            tool_name=tool_name,
            session=session,
            tenant=tenant,
            inputs=inputs,
            user_text=user_text,
            confirmation_override=confirmation_override,
        )
        if not readiness.get("ready_to_execute"):
            return {
                **_deny(
                    tool_name,
                    {
                        "buyer_identification_required": "Cannot create a sales order before the buyer is identified.",
                        "already_created": "A sales order has already been created in this conversation context.",
                        "wrong_context": f"Sales order creation is not allowed from stage '{stage or 'unknown'}'.",
                        "missing_items": "Sales order creation requires at least one item.",
                        "missing_details": "Sales order creation is blocked because required order details are still missing in the active context.",
                        "uom_confirmation_required": f"Multi-item order UOM is still only a likely assumption ({str(lead_profile.get('requested_items_assumed_uom') or 'box')}).",
                        "minimum_order_total": "Sales order is below this tenant's minimum order total.",
                        "awaiting_confirmation": "Sales order creation requires explicit customer confirmation in the current conversation context.",
                    }.get(str(readiness.get("reason_code") or ""), "Sales order creation is blocked by order readiness."),
                    str(readiness.get("next_action") or "Clarify the order details and get to order confirmation first."),
                ),
                "customer_reply": readiness.get("customer_reply"),
                "reason_code": readiness.get("reason_code"),
                "blocking_missing_slots": readiness.get("blocking_missing_slots"),
            }
        return None

    if tool_name == "update_sales_order":
        if stage in {"identify", "handoff"} or context_type == "identity_resolution":
            return _deny(
                tool_name,
                f"Sales order update is blocked during stage '{stage}'.",
                "Resolve identification or handoff before changing the order.",
            )
        readiness = evaluate_order_execution_readiness(
            tool_name=tool_name,
            session=session,
            tenant=tenant,
            inputs=inputs,
            user_text=user_text,
            confirmation_override=confirmation_override,
        )
        if not readiness.get("ready_to_execute"):
            return {
                **_deny(
                    tool_name,
                    {
                        "buyer_identification_required": "Cannot update a sales order before the buyer is identified.",
                        "separate_order_requested": "The customer asked for a separate new order, not a change to the current order.",
                        "missing_order_target": "There is no active sales order to update.",
                        "missing_items": "Sales order update requires at least one correction operation.",
                        "missing_details": "Sales order update is blocked because required order details are still missing in the active context.",
                        "uom_confirmation_required": f"Multi-item order UOM is still only a likely assumption ({str(lead_profile.get('requested_items_assumed_uom') or 'box')}).",
                        "awaiting_confirmation": "Sales order update requires explicit customer confirmation in the current conversation context.",
                    }.get(str(readiness.get("reason_code") or ""), "Sales order update is blocked by order readiness."),
                    str(readiness.get("next_action") or "Ask which order line should be added, updated, or removed."),
                ),
                "customer_reply": readiness.get("customer_reply"),
                "reason_code": readiness.get("reason_code"),
                "blocking_missing_slots": readiness.get("blocking_missing_slots"),
            }
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
            create_readiness = evaluate_order_execution_readiness(
                tool_name="create_sales_order",
                session=session,
                tenant=tenant,
                inputs={},
                user_text="",
                confirmation_override=None,
            )
            customer_reply = "There isn't a saved sales order PDF yet."
            if create_readiness.get("customer_reply"):
                customer_reply += f"\n\n{create_readiness['customer_reply']}"
            return {
                **_deny(
                    tool_name,
                    "There is no sales order PDF to send yet.",
                    "Create or locate the order before sending its PDF.",
                ),
                "customer_reply": customer_reply,
                "reason_code": "missing_order_pdf",
            }
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
