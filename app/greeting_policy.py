from __future__ import annotations

from app.i18n import text as i18n_text


def select_contact_display_name(contact_name: str | None, current_name: str | None = None) -> str | None:
    display_name = str(contact_name or "").strip()
    if display_name:
        return display_name
    current_display_name = str(current_name or "").strip()
    return current_display_name or None


def returning_customer_prefix(lang: str) -> str:
    return i18n_text("returning_customer.prefix", lang, {"customer_suffix": ""})


def should_send_known_buyer_greeting(
    *,
    user_text: str | None,
    buyer_identified: bool,
    stage: str | None,
    conversation_reopened: bool,
) -> bool:
    from app.conversation_boundary import is_short_greeting_message

    if not buyer_identified or not is_short_greeting_message(user_text):
        return False
    normalized_stage = str(stage or "").strip().lower()
    return conversation_reopened or normalized_stage in {"", "new", "identify"}
