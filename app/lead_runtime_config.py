from __future__ import annotations

from typing import Any

from app.lead_lexicon import lexicon_default, lexicon_terms, signal_regexes, signal_terms
from app.uom_semantics import canonical_uom, uom_aliases


def lead_config(config: dict[str, Any] | None) -> dict[str, Any]:
    return config if isinstance(config, dict) else {}


def lead_config_from_ai_policy(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    policy = ai_policy if isinstance(ai_policy, dict) else {}
    config = dict(policy.get("lead_management")) if isinstance(policy.get("lead_management"), dict) else {}
    catalog_policy = policy.get("catalog") if isinstance(policy.get("catalog"), dict) else {}
    if isinstance(catalog_policy.get("uom_aliases"), dict) and not isinstance(config.get("uom_aliases"), dict):
        config["uom_aliases"] = catalog_policy.get("uom_aliases")
    if isinstance(catalog_policy.get("uom_labels"), dict) and not isinstance(config.get("uom_labels"), dict):
        config["uom_labels"] = catalog_policy.get("uom_labels")
    return config


def merged_uom_config(config: dict[str, Any] | None, legacy_terms_key: str | None = None) -> dict[str, Any]:
    current = lead_config(config)
    merged: dict[str, Any] = {}

    configured_aliases = current.get("uom_aliases")
    if isinstance(configured_aliases, dict):
        merged["uom_aliases"] = {key: list(values) for key, values in configured_aliases.items() if isinstance(values, list)}

    if legacy_terms_key:
        legacy_terms = current.get(legacy_terms_key)
        if isinstance(legacy_terms, dict):
            merged.setdefault("uom_aliases", {})
            for canonical, values in legacy_terms.items():
                if not isinstance(values, list):
                    continue
                merged["uom_aliases"].setdefault(str(canonical), [])
                merged["uom_aliases"][str(canonical)].extend(values)

    configured_labels = current.get("uom_labels")
    if isinstance(configured_labels, dict):
        merged["uom_labels"] = configured_labels
    return merged


def multi_item_default_uom(config: dict[str, Any] | None) -> str:
    raw_value = str(lead_config(config).get("multi_item_default_uom") or lexicon_default("multi_item_default_uom", "box")).strip() or "box"
    return canonical_uom(raw_value, merged_uom_config(config, "multi_item_uom_terms")) or raw_value


def multi_item_uom_terms(config: dict[str, Any] | None) -> dict[str, list[str]]:
    return uom_aliases(merged_uom_config(config, "multi_item_uom_terms"))


def single_item_uom_terms(config: dict[str, Any] | None) -> dict[str, list[str]]:
    return uom_aliases(merged_uom_config(config, "single_item_uom_terms"))


def single_item_cleanup_terms(config: dict[str, Any] | None) -> list[str]:
    terms = list(lexicon_terms("single_item_cleanup_terms"))
    configured_terms = lead_config(config).get("single_item_cleanup_terms")
    if isinstance(configured_terms, list):
        terms.extend(str(term).strip() for term in configured_terms if str(term).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        key = str(term or "").strip().casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(str(term).strip())
    return deduped


def configured_signal_terms(config: dict[str, Any] | None, signal: str) -> list[str]:
    terms = list(signal_terms(signal))
    configured_terms = lead_config(config).get("signal_terms")
    if isinstance(configured_terms, dict):
        extra_terms = configured_terms.get(signal)
        if isinstance(extra_terms, list):
            terms.extend(str(term).strip() for term in extra_terms if str(term).strip())
    return terms


def configured_signal_regexes(config: dict[str, Any] | None, signal: str) -> list[str]:
    regexes = list(signal_regexes(signal))
    configured_regexes = lead_config(config).get("signal_regexes")
    if isinstance(configured_regexes, dict):
        extra_regexes = configured_regexes.get(signal)
        if isinstance(extra_regexes, list):
            regexes.extend(str(pattern).strip() for pattern in extra_regexes if str(pattern).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for pattern in regexes:
        key = str(pattern or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped
