from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

_LEXICON_DIR = Path(__file__).with_name("lexicons") / "conversation_flow"
_TERM_SECTIONS = {
    "service_terms",
    "price_terms",
    "direct_buy_terms",
    "explore_terms",
    "frustrated_terms",
    "order_terms",
    "add_to_order_terms",
    "human_terms",
}
_REGEX_SECTIONS = {
    "service_regexes",
    "price_regexes",
    "direct_buy_regexes",
    "explore_regexes",
    "frustrated_regexes",
    "order_regexes",
    "add_to_order_regexes",
    "human_regexes",
}
_PATTERN_SECTIONS = {
    "contact_details_patterns",
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
def load_conversation_lexicon() -> dict[str, Any]:
    merged: dict[str, Any] = {section: [] for section in _TERM_SECTIONS | _REGEX_SECTIONS | _PATTERN_SECTIONS}
    if not _LEXICON_DIR.exists():
        return merged
    for path in sorted(_LEXICON_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        for section in _TERM_SECTIONS | _REGEX_SECTIONS | _PATTERN_SECTIONS:
            _append_unique(merged[section], payload.get(section))
    return merged


def lexicon_terms(section: str) -> list[str]:
    values = load_conversation_lexicon().get(section)
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


def term_regex(section: str) -> re.Pattern[str]:
    return _compile_phrase_regex(tuple(lexicon_terms(section)))


def pattern_regex(section: str) -> re.Pattern[str]:
    patterns = [str(item).strip() for item in lexicon_terms(section) if str(item).strip()]
    if not patterns:
        return _EMPTY_REGEX
    return re.compile("|".join(f"(?:{pattern})" for pattern in patterns), re.IGNORECASE | re.DOTALL)


def combined_regex(*, term_section: str, regex_section: str) -> re.Pattern[str]:
    patterns: list[str] = []
    terms = tuple(lexicon_terms(term_section))
    if terms:
        compiled_terms = _compile_phrase_regex(terms)
        if compiled_terms.pattern != _EMPTY_REGEX.pattern:
            patterns.append(f"(?:{compiled_terms.pattern})")
    regex_values = [str(item).strip() for item in lexicon_terms(regex_section) if str(item).strip()]
    if regex_values:
        patterns.extend(f"(?:{pattern})" for pattern in regex_values)
    if not patterns:
        return _EMPTY_REGEX
    return re.compile("|".join(patterns), re.IGNORECASE | re.DOTALL)


@lru_cache(maxsize=1)
def service_regex() -> re.Pattern[str]:
    return combined_regex(term_section="service_terms", regex_section="service_regexes")


@lru_cache(maxsize=1)
def price_regex() -> re.Pattern[str]:
    return combined_regex(term_section="price_terms", regex_section="price_regexes")


@lru_cache(maxsize=1)
def direct_buy_regex() -> re.Pattern[str]:
    return combined_regex(term_section="direct_buy_terms", regex_section="direct_buy_regexes")


@lru_cache(maxsize=1)
def explore_regex() -> re.Pattern[str]:
    return combined_regex(term_section="explore_terms", regex_section="explore_regexes")


@lru_cache(maxsize=1)
def frustrated_regex() -> re.Pattern[str]:
    return combined_regex(term_section="frustrated_terms", regex_section="frustrated_regexes")


@lru_cache(maxsize=1)
def order_regex() -> re.Pattern[str]:
    return combined_regex(term_section="order_terms", regex_section="order_regexes")


@lru_cache(maxsize=1)
def add_to_order_regex() -> re.Pattern[str]:
    return combined_regex(term_section="add_to_order_terms", regex_section="add_to_order_regexes")


@lru_cache(maxsize=1)
def human_regex() -> re.Pattern[str]:
    return combined_regex(term_section="human_terms", regex_section="human_regexes")


@lru_cache(maxsize=1)
def contact_details_regex() -> re.Pattern[str]:
    return pattern_regex("contact_details_patterns")
