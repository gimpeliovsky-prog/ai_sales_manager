from __future__ import annotations

import re
from typing import Any

from app.lead_management import normalize_lead_profile

_CATALOG_PREFETCH_OPTION_LIMIT = 3


def catalog_prefetch_search_term(lead_profile: dict[str, Any] | None) -> str | None:
    profile = normalize_lead_profile(lead_profile)
    for candidate in (
        profile.get("catalog_item_name"),
        profile.get("product_interest"),
        profile.get("need"),
    ):
        text = re.sub(r"\s+", " ", str(candidate or "")).strip()
        if text:
            return text[:160]
    return None


def should_prefetch_catalog_options(*, lead_profile: dict[str, Any] | None, intent: str | None) -> bool:
    profile = normalize_lead_profile(lead_profile)
    if profile.get("product_resolution_status") != "broad":
        return False
    if profile.get("catalog_item_code"):
        return False
    if not catalog_prefetch_search_term(profile):
        return False
    next_action = str(profile.get("next_action") or "")
    if next_action == "show_matching_options":
        return True
    return next_action == "select_specific_item" and str(intent or "") == "browse_catalog"


def build_catalog_prefetch_context(tool_result: dict[str, Any], *, search_term: str) -> str:
    result = tool_result if isinstance(tool_result, dict) else {}
    if result.get("error"):
        return (
            f'Runtime catalog lookup already ran for "{search_term}" but returned an error. '
            "Do not ask again for the product category if it is already known. "
            "Ask only for a narrower model, variant, or item type."
        )
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return (
            f'Runtime catalog lookup already ran for "{search_term}" and found no exact matches. '
            "Ask for a narrower model, variant, or item type. "
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
