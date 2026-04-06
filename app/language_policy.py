from __future__ import annotations

import re

_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LATIN_RE = re.compile(r"[A-Za-z]")
_HEBREW_RE = re.compile(r"[\u0590-\u05FF]")
_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")


def has_language_signal(user_text: str) -> bool:
    return bool(
        _HEBREW_RE.search(user_text)
        or _ARABIC_RE.search(user_text)
        or _CYRILLIC_RE.search(user_text)
        or _LATIN_RE.search(user_text)
    )


def detect_language(user_text: str, default_lang: str | None = "ru") -> str | None:
    if _HEBREW_RE.search(user_text):
        return "he"
    if _ARABIC_RE.search(user_text):
        return "ar"
    if _CYRILLIC_RE.search(user_text):
        return "ru"
    if _LATIN_RE.search(user_text):
        return "en"
    return default_lang


def resolve_conversation_language(
    *,
    locked_lang: str | None,
    user_text: str,
    default_lang: str,
) -> tuple[str, str | None]:
    normalized_locked_lang = str(locked_lang or "").strip() or None
    detected_lang = detect_language(user_text, None)
    current_lang = normalized_locked_lang or detected_lang or default_lang
    if normalized_locked_lang:
        return current_lang, None
    if detected_lang and has_language_signal(user_text):
        return current_lang, detected_lang
    return current_lang, None
