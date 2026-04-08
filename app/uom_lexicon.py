from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.i18n import normalize_lang

_LEXICON_DIR = Path(__file__).with_name("lexicons") / "uom"


def _normalize_aliases(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        term = str(value or "").strip()
        if not term:
            continue
        key = term.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(term)
    return normalized


@lru_cache(maxsize=1)
def load_uom_lexicon() -> dict[str, Any]:
    merged: dict[str, Any] = {
        "aliases": {},
        "labels": {},
    }
    if not _LEXICON_DIR.exists():
        return merged

    for path in sorted(_LEXICON_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue

        aliases = payload.get("aliases")
        if isinstance(aliases, dict):
            for canonical, values in aliases.items():
                canonical_key = str(canonical or "").strip()
                if not canonical_key:
                    continue
                bucket = merged["aliases"].setdefault(canonical_key, [])
                seen = {item.casefold() for item in bucket}
                for term in _normalize_aliases(values):
                    key = term.casefold()
                    if key in seen:
                        continue
                    seen.add(key)
                    bucket.append(term)

        labels = payload.get("labels")
        if isinstance(labels, dict):
            for canonical, localized_values in labels.items():
                canonical_key = str(canonical or "").strip()
                if not canonical_key or not isinstance(localized_values, dict):
                    continue
                bucket = merged["labels"].setdefault(canonical_key, {})
                for lang, label in localized_values.items():
                    normalized_lang = normalize_lang(lang)
                    clean_label = str(label or "").strip()
                    if normalized_lang and clean_label:
                        bucket[normalized_lang] = clean_label

    return merged


def uom_alias_entries() -> dict[str, list[str]]:
    aliases = load_uom_lexicon().get("aliases")
    if not isinstance(aliases, dict):
        return {}
    return {str(k): list(v) for k, v in aliases.items() if isinstance(v, list)}


def uom_label_entries() -> dict[str, dict[str, str]]:
    labels = load_uom_lexicon().get("labels")
    if not isinstance(labels, dict):
        return {}
    normalized: dict[str, dict[str, str]] = {}
    for canonical, values in labels.items():
        if isinstance(values, dict):
            normalized[str(canonical)] = {str(lang): str(label) for lang, label in values.items()}
    return normalized
