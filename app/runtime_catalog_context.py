from __future__ import annotations

import re
from typing import Any

from app.lead_management import normalize_catalog_lookup_query, normalize_lead_profile

_CATALOG_PREFETCH_OPTION_LIMIT = 3
_CATALOG_PREVIEW_LIMIT = 5
_CATALOG_BACKOFF_LIMIT = 3


def catalog_prefetch_search_term(lead_profile: dict[str, Any] | None) -> str | None:
    profile = normalize_lead_profile(lead_profile)
    prioritized_candidates = [
        profile.get("catalog_item_name"),
        normalize_catalog_lookup_query(profile.get("product_interest")),
        normalize_catalog_lookup_query(profile.get("need")),
    ]
    for candidate in prioritized_candidates:
        text = re.sub(r"\s+", " ", str(candidate or "")).strip()
        if text:
            return text[:160]
    return None


def catalog_lookup_backoff_terms(search_term: str | None, *, limit: int = _CATALOG_BACKOFF_LIMIT) -> list[str]:
    normalized = re.sub(r"\s+", " ", str(search_term or "")).strip()
    if not normalized:
        return []
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        candidate = re.sub(r"\s+", " ", str(value or "")).strip()[:160]
        if len(candidate) < 3:
            return
        key = candidate.casefold()
        if key in seen:
            return
        seen.add(key)
        candidates.append(candidate)

    _add(normalized)
    tokens = normalized.split()
    if len(tokens) >= 2:
        for index in range(1, len(tokens)):
            _add(" ".join(tokens[index:]))
            if len(candidates) >= max(1, int(limit or _CATALOG_BACKOFF_LIMIT)):
                break
    return candidates[: max(1, int(limit or _CATALOG_BACKOFF_LIMIT))]


def should_prefetch_catalog_options(*, lead_profile: dict[str, Any] | None, intent: str | None) -> bool:
    profile = normalize_lead_profile(lead_profile)
    if profile.get("product_resolution_status") != "broad":
        return False
    if profile.get("catalog_item_code"):
        return False
    search_term = catalog_prefetch_search_term(profile)
    if not search_term:
        return False
    normalized_search_term = re.sub(r"\s+", " ", str(search_term).strip()).casefold()
    normalized_last_query = re.sub(r"\s+", " ", str(profile.get("catalog_lookup_query") or "").strip()).casefold()
    last_status = str(profile.get("catalog_lookup_status") or "unknown")
    next_action = str(profile.get("next_action") or "")
    correction_requested = str(profile.get("order_correction_status") or "") == "requested"
    if str(intent or "") in {"find_product", "browse_catalog", "order_detail", "add_to_order"} and normalized_last_query != normalized_search_term:
        return True
    if str(intent or "") in {"find_product", "browse_catalog", "order_detail", "add_to_order"} and last_status in {"unknown", "error"}:
        return True
    if correction_requested and next_action in {"clarify_order_correction", "apply_order_correction"}:
        return normalized_last_query != normalized_search_term or last_status in {"unknown", "error", "no_match"}
    if next_action == "show_matching_options":
        return True
    return next_action == "select_specific_item" and str(intent or "") in {"browse_catalog", "order_detail", "add_to_order"}


def should_prefetch_catalog_preview(*, lead_profile: dict[str, Any] | None, intent: str | None) -> bool:
    profile = normalize_lead_profile(lead_profile)
    if str(intent or "") != "browse_catalog":
        return False
    if profile.get("catalog_item_code"):
        return False
    next_action = str(profile.get("next_action") or "")
    last_status = str(profile.get("catalog_lookup_status") or "unknown")
    if catalog_prefetch_search_term(profile):
        return bool(
            profile.get("product_resolution_status") == "broad"
            and last_status == "no_match"
            and next_action in {"show_matching_options", "select_specific_item"}
        )
    next_action = str(profile.get("next_action") or "")
    return next_action in {"show_matching_options", "ask_need", "select_specific_item"}


def build_catalog_prefetch_context(tool_result: dict[str, Any], *, search_term: str) -> str:
    result = tool_result if isinstance(tool_result, dict) else {}
    if result.get("error"):
        return (
            f'Runtime catalog lookup already ran for "{search_term}" but returned an error. '
            "Do not ask again for the product category if it is already known. "
            "Do not claim that this product exists in the catalog. "
            "Ask only for a narrower model, variant, or item type."
        )
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return (
            f'Runtime catalog lookup already ran for "{search_term}" and found no exact matches. '
            "Do not claim that this product family exists in the catalog or is available. "
            "Ask for a narrower model, variant, or item type. "
            "Do not suggest example models, variants, or subtypes unless they came from a catalog tool result in this conversation. "
            "Do not re-ask for quantity or UOM when they are already known."
        )

    option_lines: list[str] = []
    for item in items[:_CATALOG_PREFETCH_OPTION_LIMIT]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("display_item_name") or item.get("item_name") or item.get("item_code") or "").strip()
        item_code = str(item.get("item_code") or "").strip()
        if not name:
            continue
        if item_code and item_code != name:
            option_lines.append(f"- {name} ({item_code})")
        else:
            option_lines.append(f"- {name}")
    if not option_lines:
        option_lines.append("- Matching catalog items are available")

    guidance = [
        f'Runtime catalog lookup already ran for broad product "{search_term}".',
        f"There are {len(items)} matching catalog items.",
        "Use these matching options directly in your reply instead of asking again which product the customer wants:",
        *option_lines,
    ]
    if len(items) == 1:
        guidance.append(
            "Treat the specific catalog item as resolved and continue with the next missing business detail instead of re-asking the product."
        )
    else:
        guidance.append(
            "Ask the customer to choose one of these options or specify the exact model or variant."
        )
    if result.get("price_display_blocked"):
        guidance.append("Do not mention price until product, quantity, and UOM are fully anchored.")
    return "\n".join(guidance)


def build_catalog_preview_context(tool_result: dict[str, Any], *, limit: int = _CATALOG_PREVIEW_LIMIT) -> str:
    result = tool_result if isinstance(tool_result, dict) else {}
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return (
            "Runtime catalog preview returned no items. "
            "Do not claim that the catalog is empty unless a fresh tool result says so. "
            "Ask the customer which product family or item type they want."
        )

    option_lines: list[str] = []
    for item in items[: max(1, int(limit or _CATALOG_PREVIEW_LIMIT))]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("display_item_name") or item.get("item_name") or item.get("item_code") or "").strip()
        item_code = str(item.get("item_code") or "").strip()
        if not name:
            continue
        option_lines.append(f"- {name} ({item_code})" if item_code and item_code != name else f"- {name}")
    if not option_lines:
        option_lines.append("- Catalog items are available")

    return "\n".join(
        [
            "Runtime catalog preview already ran for a broad browse request with no specific product anchor.",
            "Use these real catalog examples directly in your reply instead of saying no match:",
            *option_lines,
            "Present them as examples of what is available and ask which item type the customer wants.",
        ]
    )
