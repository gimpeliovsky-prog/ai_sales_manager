from __future__ import annotations

from datetime import date, timedelta
from typing import Any


DEFAULT_SALES_POLICY: dict[str, Any] = {
    "allow_discount_promises": False,
    "discount_requires_owner": True,
    "allow_stock_promises_without_tool": False,
    "allow_delivery_promises_without_tool": False,
    "allow_catalog_price_before_full_anchor": False,
    "llm_state_updater_enabled": True,
    "llm_state_updater_min_confidence": 0.55,
    "llm_confirmation_classifier_enabled": True,
    "llm_confirmation_min_confidence": 0.72,
    "allow_order_without_registered_customer": False,
    "default_delivery_days": 0,
    "minimum_order_total": None,
    "proactive_followup_channels": ["telegram"],
}

PRICE_FIELD_NAMES = {
    "rate",
    "price",
    "item_price",
    "standard_rate",
    "base_rate",
    "price_list_rate",
    "base_price_list_rate",
    "discounted_rate",
    "last_purchase_rate",
    "valuation_rate",
}


def sales_policy(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    policy = dict(DEFAULT_SALES_POLICY)
    if isinstance(ai_policy, dict) and isinstance(ai_policy.get("sales_policy"), dict):
        policy.update(ai_policy["sales_policy"])
    return policy


def earliest_delivery_date(ai_policy: dict[str, Any] | None) -> str:
    policy = sales_policy(ai_policy)
    try:
        days = max(0, int(policy.get("default_delivery_days") or 0))
    except (TypeError, ValueError):
        days = 0
    return (date.today() + timedelta(days=days)).isoformat()


def price_anchor_status(lead_profile: dict[str, Any] | None) -> dict[str, Any]:
    profile = lead_profile if isinstance(lead_profile, dict) else {}
    has_product = bool(profile.get("product_interest") or profile.get("requested_item_count"))
    has_quantity = bool(profile.get("quantity")) or bool(profile.get("requested_items_have_quantities") and profile.get("requested_item_count"))
    has_uom = bool(profile.get("uom")) or bool(profile.get("requested_item_count") and not profile.get("requested_items_need_uom_confirmation"))
    missing = []
    if not has_product:
        missing.append("product")
    if not has_quantity:
        missing.append("quantity")
    if not has_uom:
        missing.append("uom")
    return {
        "complete": not missing,
        "missing": missing,
        "has_product": has_product,
        "has_quantity": has_quantity,
        "has_uom": has_uom,
        "uom_assumption_status": profile.get("requested_items_uom_assumption_status"),
        "assumed_uom": profile.get("requested_items_assumed_uom"),
    }


def should_hide_catalog_prices(lead_profile: dict[str, Any] | None, ai_policy: dict[str, Any] | None) -> bool:
    policy = sales_policy(ai_policy)
    if bool(policy.get("allow_catalog_price_before_full_anchor")):
        return False
    return not bool(price_anchor_status(lead_profile).get("complete"))


def remove_price_fields(value: Any) -> Any:
    if isinstance(value, list):
        return [remove_price_fields(item) for item in value]
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item_value in value.items():
            if str(key).casefold() in PRICE_FIELD_NAMES:
                continue
            cleaned[key] = remove_price_fields(item_value)
        return cleaned
    return value


def order_total(items: Any) -> float | None:
    if not isinstance(items, list):
        return None
    total = 0.0
    seen_rate = False
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            qty = float(item.get("qty") or 0)
            rate = float(item.get("rate") or 0)
        except (TypeError, ValueError):
            continue
        if rate:
            seen_rate = True
        total += qty * rate
    return round(total, 2) if seen_rate else None


def minimum_order_violation(items: Any, ai_policy: dict[str, Any] | None) -> dict[str, Any] | None:
    policy = sales_policy(ai_policy)
    minimum = policy.get("minimum_order_total")
    if minimum in (None, ""):
        return None
    try:
        minimum_value = float(minimum)
    except (TypeError, ValueError):
        return None
    total = order_total(items)
    if total is None or total >= minimum_value:
        return None
    return {
        "minimum_order_total": minimum_value,
        "current_order_total": total,
    }


def normalize_order_state(order: dict[str, Any] | None) -> dict[str, Any]:
    data = order if isinstance(order, dict) else {}
    status_parts = " ".join(
        str(data.get(key) or "")
        for key in [
            "status",
            "docstatus",
            "delivery_status",
            "billing_status",
            "per_delivered",
            "per_billed",
        ]
    ).casefold()
    delivered = "delivered" in status_parts or str(data.get("per_delivered") or "") in {"100", "100.0"}
    invoiced = "invoiced" in status_parts or "completed" in status_parts or str(data.get("per_billed") or "") in {"100", "100.0"}
    cancelled = "cancel" in status_parts or str(data.get("docstatus") or "") == "2"
    submitted = str(data.get("docstatus") or "") == "1" or "submitted" in status_parts
    can_modify = bool(data.get("can_modify")) if "can_modify" in data else not (delivered or invoiced or cancelled)
    if str(data.get("docstatus") or "") == "0" or "draft" in status_parts:
        state = "draft"
    elif cancelled:
        state = "cancelled"
    elif delivered:
        state = "delivered"
    elif invoiced:
        state = "invoiced"
    elif submitted:
        state = "submitted"
    else:
        state = str(data.get("status") or "unknown").casefold() or "unknown"
    return {
        "sales_order_name": data.get("name") or data.get("sales_order_name"),
        "order_state": state,
        "can_modify": can_modify,
        "status": data.get("status"),
        "docstatus": data.get("docstatus"),
        "delivery_status": data.get("delivery_status"),
        "billing_status": data.get("billing_status"),
        "per_delivered": data.get("per_delivered"),
        "per_billed": data.get("per_billed"),
        "items": data.get("items") if isinstance(data.get("items"), list) else [],
        "order_total": data.get("grand_total") or data.get("rounded_total") or data.get("total") or data.get("net_total"),
        "currency": data.get("currency") or data.get("company_currency"),
    }
