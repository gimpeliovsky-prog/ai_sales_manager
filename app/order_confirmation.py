from __future__ import annotations

import re
from typing import Any

from app.interaction_patterns import has_explicit_confirmation
from app.lead_management import normalize_lead_profile
from app.uom_semantics import canonical_uom


def message_completes_order_details(
    *,
    tool_name: str,
    session: dict[str, Any],
    user_text: str,
    tenant: dict[str, Any],
) -> bool:
    if tool_name not in {"create_sales_order", "update_sales_order"}:
        return False
    lead_profile = normalize_lead_profile(session.get("lead_profile"))
    if tool_name == "create_sales_order" and not lead_profile.get("separate_order_requested"):
        return False
    if tool_name == "update_sales_order" and str(lead_profile.get("order_correction_status") or "").strip() != "requested":
        return False
    if not (lead_profile.get("catalog_item_code") or lead_profile.get("product_interest")):
        return False
    if not lead_profile.get("quantity") or not lead_profile.get("uom"):
        return False
    if has_explicit_confirmation(user_text):
        return True
    if re.search(r"\d+(?:[.,]\d+)?", user_text or ""):
        return True
    ai_policy = tenant.get("ai_policy") if isinstance(tenant.get("ai_policy"), dict) else None
    return canonical_uom(user_text, ai_policy) is not None
