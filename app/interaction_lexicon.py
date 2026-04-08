from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

_LEXICON_DIR = Path(__file__).with_name("lexicons") / "interaction_patterns"
_TERM_SECTIONS = {
    "confirm_terms",
    "negative_confirm_terms",
    "add_to_order_terms",
    "order_change_terms",
}
_REGEX_SECTIONS = {
    "confirm_regexes",
    "negative_confirm_regexes",
    "conversational_confirm_regexes",
    "add_to_order_regexes",
    "order_change_regexes",
}
_EMPTY_REGEX = re.compile(r"(?!x)x")


def _normalize_terms(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        term = re.sub(r"\s+", " ", str(value or "")).strip()
        if not term:
            continue
        key = term.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(term)
    return normalized


def _append_unique(target: list[str], values: Any) -> None:
    seen = {item.casefold() for item in target}
    for term in _normalize_terms(values):
        key = term.casefold()
        if key in seen:
            continue
        seen.add(key)
        target.append(term)


@lru_cache(maxsize=1)
def load_interaction_lexicon() -> dict[str, Any]:
    merged: dict[str, Any] = {section: [] for section in _TERM_SECTIONS | _REGEX_SECTIONS}
    if not _LEXICON_DIR.exists():
        return merged
    for path in sorted(_LEXICON_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        for section in _TERM_SECTIONS | _REGEX_SECTIONS:
            _append_unique(merged[section], payload.get(section))
    return merged


def lexicon_terms(section: str) -> list[str]:
    values = load_interaction_lexicon().get(section)
    return list(values) if isinstance(values, list) else []


def _phrase_pattern(term: str) -> str:
    escaped = re.escape(str(term or "").strip())
    escaped = escaped.replace(r"\ ", r"\s+")
    return rf"(?<!\w){escaped}(?!\w)"


def _compile_phrase_regex(terms: tuple[str, ...]) -> re.Pattern[str]:
    if not terms:
        return _EMPTY_REGEX
    patterns = [_phrase_pattern(term) for term in terms if str(term or "").strip()]
    if not patterns:
        return _EMPTY_REGEX
    return re.compile("|".join(patterns), re.IGNORECASE)


def combined_regex(*, term_section: str | None = None, regex_section: str | None = None) -> re.Pattern[str]:
    patterns: list[str] = []
    if term_section:
        compiled_terms = _compile_phrase_regex(tuple(lexicon_terms(term_section)))
        if compiled_terms.pattern != _EMPTY_REGEX.pattern:
            patterns.append(f"(?:{compiled_terms.pattern})")
    if regex_section:
        regex_values = [str(item).strip() for item in lexicon_terms(regex_section) if str(item).strip()]
        patterns.extend(f"(?:{pattern})" for pattern in regex_values)
    if not patterns:
        return _EMPTY_REGEX
    return re.compile("|".join(patterns), re.IGNORECASE | re.DOTALL)


@lru_cache(maxsize=1)
def confirm_regex() -> re.Pattern[str]:
    return combined_regex(term_section="confirm_terms", regex_section="confirm_regexes")


@lru_cache(maxsize=1)
def negative_confirm_regex() -> re.Pattern[str]:
    return combined_regex(term_section="negative_confirm_terms", regex_section="negative_confirm_regexes")


@lru_cache(maxsize=1)
def conversational_confirm_regex() -> re.Pattern[str]:
    return combined_regex(regex_section="conversational_confirm_regexes")


@lru_cache(maxsize=1)
def add_to_order_regex() -> re.Pattern[str]:
    return combined_regex(term_section="add_to_order_terms", regex_section="add_to_order_regexes")


@lru_cache(maxsize=1)
def order_change_regex() -> re.Pattern[str]:
    return combined_regex(term_section="order_change_terms", regex_section="order_change_regexes")
