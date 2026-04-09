from __future__ import annotations

import re

_NON_DIGIT_PLUS_RE = re.compile(r"[^\d+]+")
_NON_DIGIT_RE = re.compile(r"\D+")


def normalize_phone(value: str | None, *, default_country: str = "IL") -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    compact = _NON_DIGIT_PLUS_RE.sub("", text)
    if not compact:
        return None

    if compact.startswith("00"):
        compact = "+" + compact[2:]

    if compact.startswith("+"):
        digits = _NON_DIGIT_RE.sub("", compact[1:])
        return f"+{digits}" if len(digits) >= 8 else None

    digits = _NON_DIGIT_RE.sub("", compact)
    if len(digits) < 8:
        return None

    if default_country.upper() == "IL":
        if digits.startswith("972"):
            return f"+{digits}"
        if digits.startswith("0") and 9 <= len(digits) <= 10:
            return f"+972{digits[1:]}"

    return f"+{digits}"
