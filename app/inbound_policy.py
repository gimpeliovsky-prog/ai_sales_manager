from __future__ import annotations

from typing import Any


def _profile_dict(profile: dict[str, Any] | None) -> dict[str, Any]:
    return profile if isinstance(profile, dict) else {}


def has_product_context(profile: dict[str, Any] | None) -> bool:
    current = _profile_dict(profile)
    return bool(current.get("product_interest") or current.get("catalog_item_code") or current.get("catalog_item_name"))


def should_block_for_intro_before_assistance(
    *,
    needs_intro: bool,
    customer_identified: bool,
    intent: str | None,
    lead_profile: dict[str, Any] | None,
) -> bool:
    if customer_identified or not needs_intro:
        return False
    if has_product_context(lead_profile):
        return False
    normalized_intent = str(intent or "").strip()
    return normalized_intent in {"low_signal", "service_request"}


def should_request_intro_before_next_step(
    *,
    needs_intro: bool,
    customer_identified: bool,
    lead_profile: dict[str, Any] | None,
) -> bool:
    if customer_identified or not needs_intro:
        return False
    current = _profile_dict(lead_profile)
    return str(current.get("next_action") or "").strip() == "ask_contact"
