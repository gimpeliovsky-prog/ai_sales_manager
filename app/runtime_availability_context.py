from __future__ import annotations

import re
from typing import Any

from app.lead_management import normalize_lead_profile

_STOCK_CHECK_RE = re.compile(
    r"(?:\bin stock\b|\bavailable\b|\bavailability\b|\bhave it\b|\bdo you have\b|"
    r"\bв наличии\b|\bесть\b|\bдоступно\b|"
    r"\bבמלאי\b|\bיש\b)",
    re.IGNORECASE,
)


def selected_item_code(lead_profile: dict[str, Any] | None) -> str | None:
    profile = normalize_lead_profile(lead_profile)
    item_code = str(profile.get("catalog_item_code") or "").strip()
    return item_code or None


def should_prefetch_item_availability(*, lead_profile: dict[str, Any] | None, user_text: str | None) -> bool:
    profile = normalize_lead_profile(lead_profile)
    item_code = selected_item_code(profile)
    if not item_code:
        return False
    text = str(user_text or "").strip()
    if not text or not _STOCK_CHECK_RE.search(text):
        return False
    last_item_code = str(profile.get("availability_item_code") or "").strip()
    if last_item_code != item_code:
        return True
    return not str(profile.get("availability_checked_at") or "").strip()


def build_availability_prefetch_context(tool_result: dict[str, Any]) -> str:
    result = tool_result if isinstance(tool_result, dict) else {}
    item_name = str(result.get("item_name") or result.get("item_code") or "this item").strip()
    if result.get("error"):
        return (
            f"Runtime availability lookup for {item_name} failed. "
            "Do not promise stock or availability. "
            "Explain that stock could not be confirmed right now."
        )
    in_stock = bool(result.get("in_stock"))
    total_available = result.get("total_available_qty")
    stock_uom = str(result.get("stock_uom") or "").strip()
    warehouse_count = result.get("warehouse_count")
    status_line = "available in stock" if in_stock else "not currently available in stock"
    qty_line = ""
    if total_available not in (None, ""):
        qty_line = f" Confirmed available quantity: {total_available}"
        if stock_uom:
            qty_line += f" {stock_uom}"
        qty_line += "."
    warehouse_line = ""
    if warehouse_count not in (None, ""):
        warehouse_line = f" Warehouse records checked: {warehouse_count}."
    return (
        f"Runtime availability lookup already ran for {item_name}. "
        f"This item is {status_line}.{qty_line}{warehouse_line} "
        "Use this tool result directly in your reply and do not ask the customer to repeat the product."
    )
