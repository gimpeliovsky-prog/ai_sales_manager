from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

_LEXICON_DIR = Path(__file__).with_name("lexicons") / "lead_management"
_LIST_SECTIONS = {
    "browse_scaffolding_terms",
    "yes_terms",
    "contact_intro_terms",
    "commercial_cue_terms",
    "single_item_cleanup_terms",
    "generic_product_tokens",
    "product_interest_noise_terms",
    "product_interest_filler_terms",
}
_DICT_LIST_SECTIONS = {
    "signal_terms",
    "signal_regexes",
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
def load_lead_lexicon() -> dict[str, Any]:
    merged: dict[str, Any] = {section: [] for section in _LIST_SECTIONS}
    for section in _DICT_LIST_SECTIONS:
        merged[section] = {}
    merged["defaults"] = {}
    if not _LEXICON_DIR.exists():
        return merged

    for path in sorted(_LEXICON_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        for section in _LIST_SECTIONS:
            _append_unique(merged[section], payload.get(section))
        for section in _DICT_LIST_SECTIONS:
            section_payload = payload.get(section)
            if isinstance(section_payload, dict):
                for key, values in section_payload.items():
                    bucket = merged[section].setdefault(str(key), [])
                    _append_unique(bucket, values)
        defaults = payload.get("defaults")
        if isinstance(defaults, dict):
            for key, value in defaults.items():
                merged["defaults"][str(key)] = value
    return merged


def lexicon_terms(section: str) -> list[str]:
    values = load_lead_lexicon().get(section)
    return list(values) if isinstance(values, list) else []


def signal_terms(signal: str) -> list[str]:
    signals = load_lead_lexicon().get("signal_terms")
    if not isinstance(signals, dict):
        return []
    values = signals.get(signal)
    return list(values) if isinstance(values, list) else []


def signal_regexes(signal: str) -> list[str]:
    signals = load_lead_lexicon().get("signal_regexes")
    if not isinstance(signals, dict):
        return []
    values = signals.get(signal)
    return list(values) if isinstance(values, list) else []


def lexicon_default(key: str, default: Any = None) -> Any:
    defaults = load_lead_lexicon().get("defaults")
    if not isinstance(defaults, dict):
        return default
    return defaults.get(key, default)


def generic_product_tokens() -> set[str]:
    return {term.casefold() for term in lexicon_terms("generic_product_tokens")}


def product_interest_noise_terms() -> list[str]:
    return lexicon_terms("product_interest_noise_terms")


def product_interest_filler_terms() -> list[str]:
    return lexicon_terms("product_interest_filler_terms")


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


def _regex_for(section: str) -> re.Pattern[str]:
    return _compile_phrase_regex(tuple(lexicon_terms(section)))


@lru_cache(maxsize=1)
def browse_scaffolding_regex() -> re.Pattern[str]:
    return _regex_for("browse_scaffolding_terms")


@lru_cache(maxsize=1)
def yes_regex() -> re.Pattern[str]:
    return _regex_for("yes_terms")


@lru_cache(maxsize=1)
def contact_intro_regex() -> re.Pattern[str]:
    return _regex_for("contact_intro_terms")


@lru_cache(maxsize=1)
def commercial_cue_regex() -> re.Pattern[str]:
    return _regex_for("commercial_cue_terms")
