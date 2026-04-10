from __future__ import annotations

import re
from typing import Any

_GREETING_ONLY_RE = re.compile(
    r"(?is)^\s*(?:"
    r"hello|hi|hey|good\s+(?:day|morning|afternoon|evening)|"
    r"привет|здравствуйте|добрый\s+\w+|"
    r"היי|הי|שלום|shalom|"
    r"مرحبا|اهلا|أهلا|السلام\s+عليكم"
    r")[\s!,.?]*$"
)

_PRESERVED_SESSION_KEYS = {
    "company_code",
    "erp_customer_id",
    "buyer_name",
    "buyer_identity_id",
    "buyer_phone",
    "buyer_preferred_language",
    "buyer_company_name",
    "buyer_company_registry_number",
    "buyer_company_candidates",
    "buyer_company_pending",
    "buyer_review_required",
    "buyer_review_case_id",
    "buyer_identity_status",
    "buyer_recognized_via",
    "recent_sales_orders",
    "recent_sales_invoices",
    "channel_context",
    "lang",
    "last_channel",
}


def is_short_greeting_message(user_text: str | None) -> bool:
    return bool(_GREETING_ONLY_RE.fullmatch(str(user_text or "").strip()))


def reset_session_for_new_dialogue(
    session: dict[str, Any],
    *,
    fresh_session: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(fresh_session)
    for key in _PRESERVED_SESSION_KEYS:
        if key in session:
            updated[key] = session.get(key)
    updated["returning_customer_announced"] = False
    updated["conversation_reopened"] = True
    updated["conversation_closed_at"] = None
    return updated
