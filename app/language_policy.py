from __future__ import annotations

import re

_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LATIN_RE = re.compile(r"[A-Za-z]")
_EN_HINT_RE = re.compile(
    r"\b(?:hello|hi|hey|please|price|cost|quote|order|delivery|ship|need|want|buy|thanks|thank you)\b",
    re.IGNORECASE,
)
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
        return "en" if _EN_HINT_RE.search(user_text) else "auto"
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
        if detected_lang and detected_lang != normalized_locked_lang and has_language_signal(user_text):
            has_non_latin_signal = bool(
                _HEBREW_RE.search(user_text)
                or _ARABIC_RE.search(user_text)
                or _CYRILLIC_RE.search(user_text)
            )
            has_only_english_signal = bool(_LATIN_RE.search(user_text)) and not has_non_latin_signal
            if has_non_latin_signal or (detected_lang == "en" and has_only_english_signal and _EN_HINT_RE.search(user_text)):
                return detected_lang, detected_lang
        return current_lang, None
    if detected_lang and has_language_signal(user_text):
        return current_lang, detected_lang
    return current_lang, None
