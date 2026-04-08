import json
import re
from typing import Any

from app.catalog_localization import catalog_lang as _catalog_lang, localize_catalog_result as _localize_catalog_result
from app.i18n import text as i18n_text
from app.interaction_patterns import has_add_to_order_intent, has_explicit_confirmation
from app.lead_management import normalize_catalog_lookup_query
from app.license_client import LicenseClient
from app.sales_policy import (
    earliest_delivery_date,
    minimum_order_violation,
    normalize_order_state,
    price_anchor_status,
    remove_price_fields,
    should_hide_catalog_prices,
)
from app.uom_semantics import localize_available_uom_options, resolve_catalog_uom

TOOLS: list[dict] = [
    {
        "type": "function",
        "name": "get_product_catalog",
        "description": "Search the product and service catalog by item group or item name. Use this to get item codes, display names, stock UOM, available sales UOM options, conversion factors, images, and product metadata before recommending or ordering an item.",
        "parameters": {
            "type": "object",
            "properties": {
                "item_group": {"type": "string", "description": "Optional product group/category to search."},
                "item_name": {"type": "string", "description": "Optional item name or search text from the customer request."},
            },
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_item_availability",
        "description": "Check tool-backed stock availability for a specific catalog item code, optionally in a specific warehouse. Use this before promising stock or availability to the customer.",
        "parameters": {
            "type": "object",
            "properties": {
                "item_code": {"type": "string", "description": "ERP item code from the catalog result."},
                "warehouse": {"type": "string", "description": "Optional warehouse name to narrow availability."},
            },
            "required": ["item_code"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_sales_order",
        "description": "Create a sales order only after the customer clearly confirms the order contents. If the customer asks for boxes, packs, or another non-stock UOM, pass the requested UOM and the matching conversion_factor from the catalog result. If delivery_date is missing, use the earliest reasonable date.",
        "parameters": {
            "type": "object",
            "properties": {
                "delivery_date": {"type": "string", "description": "Optional delivery date in YYYY-MM-DD format."},
                "items": {
                    "type": "array",
                    "description": "Order lines confirmed by the customer.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string", "description": "ERP item code from the catalog."},
                            "qty": {"type": "number", "description": "Customer-confirmed quantity."},
                            "rate": {"type": "number", "description": "Optional item rate only when tool-backed or explicitly provided."},
                            "uom": {"type": "string", "description": "Requested UOM from the catalog result."},
                            "conversion_factor": {"type": "number", "description": "Conversion factor for non-stock UOM from the catalog result."},
                        },
                        "required": ["item_code", "qty"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_invoice",
        "description": "Create an invoice for an existing sales order when invoice creation is allowed and the customer asks for it.",
        "parameters": {
            "type": "object",
            "properties": {"sales_order_name": {"type": "string", "description": "Existing sales order name."}},
            "required": ["sales_order_name"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "update_sales_order",
        "description": "Add or update items in an existing draft sales order. If sales_order_name is missing, use the active order from the current conversation. Do not use this unless the customer explicitly asks to add/update the current order or clearly confirms the change.",
        "parameters": {
            "type": "object",
            "properties": {
                "sales_order_name": {"type": "string", "description": "Optional existing sales order name. Defaults to the active order in the conversation."},
                "items": {
                    "type": "array",
                    "description": "Items to add or update in the order.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string", "description": "ERP item code from the catalog."},
                            "qty": {"type": "number", "description": "Customer-confirmed quantity to add/update."},
                            "rate": {"type": "number", "description": "Optional item rate only when tool-backed or explicitly provided."},
                            "uom": {"type": "string", "description": "Requested UOM from the catalog result."},
                            "conversion_factor": {"type": "number", "description": "Conversion factor for non-stock UOM from the catalog result."},
                        },
                        "required": ["item_code", "qty"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_sales_order_status",
        "description": "Get the current status of an existing sales order before promising or applying an order change. Use this when the customer asks to change, cancel, add to, remove from, invoice, or check an existing order.",
        "parameters": {
            "type": "object",
            "properties": {
                "sales_order_name": {"type": "string", "description": "Optional sales order name. Defaults to the active order in the conversation."},
            },
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "send_sales_order_pdf",
        "description": "Send or re-send the PDF for the current sales order when the customer asks for the order PDF, order file, current order, or sales order document. Do not use this to create an invoice.",
        "parameters": {
            "type": "object",
            "properties": {
                "sales_order_name": {"type": "string", "description": "Optional sales order name. Defaults to the active order in the conversation."},
            },
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "register_buyer",
        "description": "Register or resolve a new buyer after receiving at least the buyer's full name. Include phone when the customer provided it.",
        "parameters": {
            "type": "object",
            "properties": {
                "full_name": {"type": "string", "description": "Buyer full name."},
                "phone": {"type": "string", "description": "Optional buyer phone number."},
            },
            "required": ["full_name"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_buyer_sales_history",
        "description": "Get recent sales orders and invoices for an identified buyer. Use this to support returning-customer flows and repeat purchase context without inventing history.",
        "parameters": {
            "type": "object",
            "properties": {"erp_customer_id": {"type": "string", "description": "Optional ERP customer id. Defaults to the identified customer in the conversation."}},
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_license",
        "description": "Create a license key when this tenant and conversation flow allow license operations.",
        "parameters": {
            "type": "object",
            "properties": {"description": {"type": "string", "description": "Optional license description."}},
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "extend_subscription",
        "description": "Extend a subscription by a specified number of days when this tenant and conversation flow allow subscription operations.",
        "parameters": {
            "type": "object",
            "properties": {"add_days": {"type": "integer", "description": "Number of days to add to the subscription."}},
            "required": ["add_days"],
            "additionalProperties": False,
        },
    },
]

def _has_explicit_confirmation(user_text: str) -> bool:
    return has_explicit_confirmation(user_text)


def _has_add_to_order_intent(user_text: str) -> bool:
    return has_add_to_order_intent(user_text)


def _items_have_qty(items: list[dict[str, Any]] | None) -> bool:
    if not isinstance(items, list) or not items:
        return False
    for item in items:
        qty = item.get("qty") if isinstance(item, dict) else None
        if isinstance(qty, (int, float)) and qty > 0:
            return True
    return False


def _normalize_match_text(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9?-??-???\u0590-\u05FF\u0600-\u06FF]+", " ", text or "").strip().lower()


_QUERY_STOPWORDS = {
    "a", "an", "the", "i", "im", "i'm", "me", "my", "you", "your", "we", "our",
    "want", "need", "looking", "look", "for", "show", "have", "has", "do", "does",
    "what", "which", "who", "where", "when", "how", "please", "can", "could", "would",
    "product", "products", "item", "items", "model", "models", "variant", "variants",
    "option", "options", "type", "types", "name", "exact", "another", "until", "know",
    "ok", "okay", "hi", "hello", "hey", "thanks", "thank", "buy", "order",
}


def _query_tokens(text: str | None) -> list[str]:
    return [
        token
        for token in _normalize_match_text(text or "").split()
        if len(token) >= 3 and token not in _QUERY_STOPWORDS
    ]


def _build_search_candidates(*texts: str | None) -> list[str]:
    candidates: list[str] = []
    for raw_text in texts:
        normalized = _normalize_match_text(raw_text or "")
        if not normalized:
            continue
        if normalized not in candidates:
            candidates.append(normalized)
        for token in _query_tokens(normalized):
            if token not in candidates:
                candidates.append(token)
    return candidates[:6]


def _catalog_item_search_text(item: dict[str, Any]) -> str:
    return _normalize_match_text(
        " ".join(
            str(item.get(key) or "")
            for key in ("display_item_name", "item_name", "item_group", "item_code", "description")
        )
    )


def _token_matches(left: str, right: str) -> bool:
    if left == right:
        return True
    if len(left) >= 4 and len(right) >= 4 and (left.startswith(right) or right.startswith(left)):
        return True
    return len(left) >= 5 and len(right) >= 5 and (left in right or right in left)


def _catalog_item_matches_query(query: str | None, item: dict[str, Any]) -> bool:
    normalized_query = _normalize_match_text(query or "")
    if not normalized_query:
        return True
    item_text = _catalog_item_search_text(item)
    if not item_text:
        return False
    if normalized_query in item_text:
        return True
    query_tokens = _query_tokens(normalized_query)
    item_tokens = [token for token in item_text.split() if len(token) >= 3]
    if not query_tokens or not item_tokens:
        return False
    matched = sum(1 for token in query_tokens if any(_token_matches(token, item_token) for item_token in item_tokens))
    if len(query_tokens) == 1:
        return matched >= 1
    required = min(len(query_tokens), 2)
    return matched >= required


def _filter_catalog_matches(result: dict[str, Any], query: str | None) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"items": []}
    items = result.get("items")
    if not isinstance(items, list):
        return result
    filtered_items = [item for item in items if isinstance(item, dict) and _catalog_item_matches_query(query, item)]
    updated = dict(result)
    updated["items"] = filtered_items
    if filtered_items and len(filtered_items) != len(items):
        updated["match_filter_applied"] = True
    return updated


async def _load_catalog_item(lc: LicenseClient, company_code: str, item_code: str, current_lang: str) -> dict[str, Any]:
    requested_lang = _catalog_lang(current_lang)
    for candidate_lang in (requested_lang, None):
        try:
            result = await lc.get_item(company_code, item_code, candidate_lang)
        except Exception:
            continue
        if isinstance(result, dict) and result:
            return result
    return {}


async def _normalize_order_items_uoms(
    *,
    items: list[dict[str, Any]],
    lc: LicenseClient,
    company_code: str,
    current_lang: str,
    ai_policy: dict[str, Any] | None,
    lead_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_items: list[dict[str, Any]] = []
    item_cache: dict[str, dict[str, Any]] = {}
    fallback_uom = str((lead_profile or {}).get("uom") or "").strip() or None

    for item in items:
        if not isinstance(item, dict):
            normalized_items.append(item)
            continue
        updated = dict(item)
        item_code = str(updated.get("item_code") or "").strip()
        requested_uom = str(updated.get("uom") or fallback_uom or "").strip()
        if not item_code or not requested_uom:
            normalized_items.append(updated)
            continue
        if item_code not in item_cache:
            item_cache[item_code] = await _load_catalog_item(lc, company_code, item_code, current_lang)
        catalog_item = item_cache.get(item_code) or {}
        available_uoms = catalog_item.get("available_uoms")
        resolution = resolve_catalog_uom(requested_uom, available_uoms, config=ai_policy)
        if not resolution.get("resolved"):
            return {
                "error": "Requested unit does not match the catalog UOM options for this item.",
                "error_code": "uom_not_available",
                "item_code": item_code,
                "requested_uom": requested_uom,
                "uom_resolution_reason": resolution.get("reason"),
                "available_uoms": localize_available_uom_options(
                    catalog_item.get("stock_uom_label") or catalog_item.get("stock_uom"),
                    available_uoms,
                    lang=current_lang,
                    config=ai_policy,
                ),
            }
        updated["uom"] = resolution.get("uom")
        if resolution.get("conversion_factor") not in (None, "", 0):
            updated["conversion_factor"] = resolution.get("conversion_factor")
        normalized_items.append(updated)
    return {"items": normalized_items}


async def execute_tool(
    name: str,
    inputs: dict[str, Any],
    company_code: str,
    erp_customer_id: str | None,
    active_sales_order_name: str | None,
    current_lang: str,
    user_text: str,
    channel: str,
    channel_uid: str,
    lc: LicenseClient,
    ai_policy: dict[str, Any] | None = None,
    lead_profile: dict[str, Any] | None = None,
    confirmation_override: bool | None = None,
) -> str:
    try:
        result = await _dispatch(
            name,
            inputs,
            company_code,
            erp_customer_id,
            active_sales_order_name,
            current_lang,
            user_text,
            channel,
            channel_uid,
            lc,
            ai_policy,
            lead_profile,
            confirmation_override,
        )
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


async def _dispatch(name, inp, company_code, erp_customer_id, active_sales_order_name, current_lang, user_text, channel, channel_uid, lc, ai_policy=None, lead_profile=None, confirmation_override=None):
    from app.buyer_resolver import create_buyer_from_intro

    if name == "get_product_catalog":
        lead_config = ai_policy.get("lead_management") if isinstance(ai_policy, dict) and isinstance(ai_policy.get("lead_management"), dict) else ai_policy
        raw_item_group = inp.get("item_group")
        raw_item_name = inp.get("item_name")
        item_group = normalize_catalog_lookup_query(raw_item_group, lead_config) or raw_item_group
        item_name = normalize_catalog_lookup_query(raw_item_name, lead_config) or raw_item_name
        original_query = item_name or item_group or raw_item_name or raw_item_group
        catalog_lang = _catalog_lang(current_lang)
        try:
            result = await lc.get_items(company_code, item_group, item_name, catalog_lang)
        except Exception:
            result = {"items": []}
        if not result.get("items"):
            for candidate in _build_search_candidates(item_name, item_group):
                try:
                    result = await lc.get_items(company_code, None, candidate, catalog_lang)
                except Exception:
                    result = {"items": []}
                if result.get("items"):
                    break
        if not result.get("items") and catalog_lang:
            for candidate in [item_name, item_group, *_build_search_candidates(item_name, item_group)]:
                if not candidate:
                    continue
                try:
                    result = await lc.get_items(company_code, None, candidate, None)
                except Exception:
                    result = {"items": []}
                if result.get("items"):
                    break
        localized = _localize_catalog_result(_filter_catalog_matches(result, original_query), current_lang, ai_policy)
        if should_hide_catalog_prices(lead_profile, ai_policy):
            cleaned = remove_price_fields(localized)
            if isinstance(cleaned, dict):
                cleaned["price_display_blocked"] = True
                cleaned["price_display_blocked_reason"] = "price_requires_product_quantity_and_uom"
                cleaned["price_anchor"] = price_anchor_status(lead_profile)
            return cleaned
        return localized
    if name == "get_item_availability":
        item_code = str(inp.get("item_code") or "").strip()
        if not item_code:
            return {"error": "Item code is required.", "error_code": "item_code_required"}
        return await lc.get_item_availability(company_code, item_code, inp.get("warehouse"))
    if name == "create_sales_order":
        if not erp_customer_id:
            return {"error": i18n_text("tool_error.buyer_not_identified", current_lang, ai_policy=ai_policy), "error_code": "buyer_not_identified"}
        if not _items_have_qty(inp.get("items")):
            return {"error": i18n_text("tool_error.order_qty_required", current_lang, ai_policy=ai_policy), "error_code": "order_qty_required"}
        normalized_items_result = await _normalize_order_items_uoms(
            items=inp.get("items") or [],
            lc=lc,
            company_code=company_code,
            current_lang=current_lang,
            ai_policy=ai_policy,
            lead_profile=lead_profile,
        )
        if normalized_items_result.get("error"):
            return normalized_items_result
        minimum_violation = minimum_order_violation(inp.get("items"), ai_policy)
        if minimum_violation:
            return {"error": "Order total is below the tenant minimum order total.", "error_code": "minimum_order_total_not_met", **minimum_violation}
        if confirmation_override is False:
            return {"error": i18n_text("tool_error.order_confirmation_required", current_lang, ai_policy=ai_policy), "error_code": "order_confirmation_required"}
        if not (_has_explicit_confirmation(user_text) or confirmation_override is True):
            return {"error": i18n_text("tool_error.order_confirmation_required", current_lang, ai_policy=ai_policy), "error_code": "order_confirmation_required"}
        delivery_date = inp.get("delivery_date") or earliest_delivery_date(ai_policy)
        return await lc.create_sales_order(company_code, erp_customer_id, delivery_date, normalized_items_result["items"])
    if name == "create_invoice":
        return await lc.create_invoice(company_code, inp["sales_order_name"])
    if name == "update_sales_order":
        sales_order_name = inp.get("sales_order_name") or active_sales_order_name
        if not sales_order_name:
            return {"error": i18n_text("tool_error.no_active_order", current_lang, ai_policy=ai_policy), "error_code": "no_active_order"}
        if not _items_have_qty(inp.get("items")):
            return {"error": i18n_text("tool_error.add_to_order_qty_required", current_lang, ai_policy=ai_policy), "error_code": "add_to_order_qty_required"}
        if not _has_add_to_order_intent(user_text) and not (_has_explicit_confirmation(user_text) or confirmation_override is True):
            return {"error": i18n_text("tool_error.add_to_order_confirmation_required", current_lang, ai_policy=ai_policy), "error_code": "add_to_order_confirmation_required"}
        normalized_items_result = await _normalize_order_items_uoms(
            items=inp.get("items") or [],
            lc=lc,
            company_code=company_code,
            current_lang=current_lang,
            ai_policy=ai_policy,
            lead_profile=lead_profile,
        )
        if normalized_items_result.get("error"):
            return normalized_items_result
        order = await lc.get_sales_order_status(company_code, sales_order_name)
        state = normalize_order_state(order if isinstance(order, dict) else {})
        if not state.get("can_modify"):
            return {"error": "Sales order cannot be modified in its current state.", "error_code": "sales_order_not_modifiable", **state}
        return await lc.update_sales_order_items(company_code, sales_order_name, normalized_items_result["items"])
    if name == "get_sales_order_status":
        sales_order_name = inp.get("sales_order_name") or active_sales_order_name
        if not sales_order_name:
            return {"error": i18n_text("tool_error.no_active_order", current_lang, ai_policy=ai_policy), "error_code": "no_active_order"}
        order = await lc.get_sales_order_status(company_code, sales_order_name)
        return normalize_order_state(order if isinstance(order, dict) else {})
    if name == "send_sales_order_pdf":
        sales_order_name = inp.get("sales_order_name") or active_sales_order_name
        if not sales_order_name:
            return {"error": i18n_text("tool_error.no_active_order", current_lang, ai_policy=ai_policy), "error_code": "no_active_order"}
        return await lc.get_sales_order(company_code, sales_order_name)
    if name == "register_buyer":
        buyer_result = await create_buyer_from_intro(
            session={},
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            full_name=inp["full_name"],
            phone=inp.get("phone"),
            lc=lc,
        )
        customer_id = buyer_result.get("erp_customer_id") if isinstance(buyer_result, dict) else None
        return {
            "erp_customer_id": customer_id,
            "registered": customer_id is not None,
            "buyer_identity_id": buyer_result.get("buyer_identity_id") if isinstance(buyer_result, dict) else None,
            "recent_sales_orders": buyer_result.get("recent_sales_orders") if isinstance(buyer_result, dict) else [],
        }
    if name == "get_buyer_sales_history":
        target_customer_id = erp_customer_id or inp.get("erp_customer_id")
        if not target_customer_id:
            return {"error": "Buyer is not identified yet."}
        return await lc.get_buyer_sales_history(company_code, target_customer_id)
    if name == "create_license":
        return await lc.create_license(company_code, inp.get("description"))
    if name == "extend_subscription":
        return await lc.extend_subscription(company_code, inp["add_days"])
    return {"error": f"Unknown tool: {name}"}
