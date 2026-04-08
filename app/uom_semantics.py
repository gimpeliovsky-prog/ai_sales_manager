from __future__ import annotations

import re
import unicodedata
from typing import Any

from app.i18n import normalize_lang

DEFAULT_UOM_ALIASES: dict[str, list[str]] = {
    "piece": [
        "piece",
        "pieces",
        "pc",
        "pcs",
        "unit",
        "units",
        "each",
        "nos",
        "шт",
        "штука",
        "штук",
        "штуки",
        "ед",
        "единица",
        "единицы",
        "יחידה",
        "יחידות",
        "قطعة",
        "قطع",
    ],
    "box": [
        "box",
        "boxes",
        "case",
        "cases",
        "carton",
        "cartons",
        "коробка",
        "коробки",
        "коробок",
        "коробах",
        "ящик",
        "ящики",
        "קרטון",
        "קרטונים",
        "קופסה",
        "קופסא",
        "קופסאות",
        "קופסות",
        "كرتون",
        "كرتونة",
        "كراتين",
        "علبة",
        "علب",
    ],
    "pack": [
        "pack",
        "packs",
        "package",
        "packages",
        "pkg",
        "packet",
        "packets",
        "упаковка",
        "упаковки",
        "пачка",
        "пачки",
        "חבילה",
        "חבילות",
        "אריזה",
        "אריזות",
        "عبوة",
        "عبوات",
        "حزمة",
        "حزم",
    ],
    "kg": [
        "kg",
        "kgs",
        "kilogram",
        "kilograms",
        "кг",
        "килограмм",
        "килограммы",
        'ק"ג',
        "קילו",
        "كيلو",
        "كيلوغرام",
    ],
    "g": [
        "g",
        "gr",
        "gram",
        "grams",
        "г",
        "гр",
        "грамм",
        "граммы",
        "גרם",
        "גרמים",
        "غرام",
        "غرامات",
    ],
    "l": [
        "l",
        "lt",
        "ltr",
        "liter",
        "liters",
        "litre",
        "litres",
        "л",
        "литр",
        "литры",
        "ליטר",
        "ליטרים",
        "لتر",
        "لترات",
    ],
    "m": [
        "m",
        "meter",
        "meters",
        "metre",
        "metres",
        "м",
        "метр",
        "метры",
        "מטר",
        "מטרים",
        "متر",
        "أمتار",
    ],
}

DEFAULT_UOM_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "piece": "pieces",
        "box": "boxes",
        "pack": "packs",
        "kg": "kg",
        "g": "g",
        "l": "liters",
        "m": "meters",
    },
    "ru": {
        "piece": "шт.",
        "box": "коробки",
        "pack": "упаковки",
        "kg": "кг",
        "g": "г",
        "l": "литры",
        "m": "метры",
    },
    "he": {
        "piece": "יחידות",
        "box": "קרטונים",
        "pack": "חבילות",
        "kg": 'ק"ג',
        "g": "גרם",
        "l": "ליטרים",
        "m": "מטרים",
    },
    "ar": {
        "piece": "قطع",
        "box": "كراتين",
        "pack": "عبوات",
        "kg": "كجم",
        "g": "غرام",
        "l": "لترات",
        "m": "أمتار",
    },
}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_uom_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", _clean_text(value))
    text = text.casefold()
    text = re.sub(r"[\s/_-]+", " ", text)
    text = text.strip(" \t\r\n.,:;()[]{}")
    return text


def _merge_alias_bucket(target: dict[str, list[str]], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for canonical, values in source.items():
        clean_canonical = normalize_uom_text(canonical)
        if not clean_canonical:
            continue
        bucket = target.setdefault(clean_canonical, [])
        if clean_canonical not in bucket:
            bucket.append(clean_canonical)
        if not isinstance(values, list):
            continue
        for value in values:
            clean_value = normalize_uom_text(value)
            if clean_value and clean_value not in bucket:
                bucket.append(clean_value)


def _merge_label_bucket(target: dict[str, dict[str, str]], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for lang, values in source.items():
        normalized_lang = normalize_lang(lang)
        if not normalized_lang or not isinstance(values, dict):
            continue
        bucket = target.setdefault(normalized_lang, {})
        for canonical, label in values.items():
            clean_canonical = normalize_uom_text(canonical)
            clean_label = _clean_text(label)
            if clean_canonical and clean_label:
                bucket[clean_canonical] = clean_label


def _configured_alias_maps(config: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(config, dict):
        return []
    configured: list[dict[str, Any]] = []
    for bucket in (
        config.get("uom_aliases"),
        config.get("catalog", {}).get("uom_aliases") if isinstance(config.get("catalog"), dict) else None,
        config.get("lead_management", {}).get("uom_aliases") if isinstance(config.get("lead_management"), dict) else None,
    ):
        if isinstance(bucket, dict):
            configured.append(bucket)
    return configured


def _configured_label_maps(config: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(config, dict):
        return []
    configured: list[dict[str, Any]] = []
    for bucket in (
        config.get("uom_labels"),
        config.get("catalog", {}).get("uom_labels") if isinstance(config.get("catalog"), dict) else None,
        config.get("lead_management", {}).get("uom_labels") if isinstance(config.get("lead_management"), dict) else None,
    ):
        if isinstance(bucket, dict):
            configured.append(bucket)
    return configured


def uom_aliases(config: dict[str, Any] | None = None) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    _merge_alias_bucket(aliases, DEFAULT_UOM_ALIASES)
    for configured in _configured_alias_maps(config):
        _merge_alias_bucket(aliases, configured)
    return aliases


def canonical_uom(value: Any, config: dict[str, Any] | None = None) -> str | None:
    normalized = normalize_uom_text(value)
    if not normalized:
        return None
    for canonical, values in uom_aliases(config).items():
        if normalized == canonical or normalized in values:
            return canonical
    return None


def localize_uom_label(value: Any, lang: str | None, config: dict[str, Any] | None = None) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    canonical = canonical_uom(text, config)
    if not canonical:
        return text
    labels = {language: dict(values) for language, values in DEFAULT_UOM_LABELS.items()}
    for configured in _configured_label_maps(config):
        _merge_label_bucket(labels, configured)
    normalized_lang = normalize_lang(lang)
    for candidate_lang in (normalized_lang, "default", "en"):
        bucket = labels.get(candidate_lang)
        if isinstance(bucket, dict):
            localized = _clean_text(bucket.get(canonical))
            if localized:
                return localized
    return text


def localize_available_uom_options(
    stock_uom: Any,
    available_uoms: Any,
    *,
    lang: str | None,
    config: dict[str, Any] | None = None,
) -> list[str]:
    labels: list[str] = []
    stock_label = localize_uom_label(stock_uom, lang, config)
    if stock_label and stock_label not in labels:
        labels.append(stock_label)
    if isinstance(available_uoms, list):
        for option in available_uoms:
            if isinstance(option, dict):
                raw = option.get("display_name") or option.get("uom")
            else:
                raw = option
            label = localize_uom_label(raw, lang, config)
            if label and label not in labels:
                labels.append(label)
    return labels


def resolve_catalog_uom(
    requested_uom: Any,
    available_uoms: Any,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_text = _clean_text(requested_uom)
    normalized_requested = normalize_uom_text(requested_text)
    if not requested_text:
        return {"resolved": False, "reason": "missing_requested_uom"}
    if not isinstance(available_uoms, list) or not available_uoms:
        return {"resolved": False, "reason": "missing_catalog_uoms"}

    exact_matches: list[dict[str, Any]] = []
    semantic_matches: list[dict[str, Any]] = []
    requested_semantic = canonical_uom(requested_text, config)

    for option in available_uoms:
        if isinstance(option, dict):
            raw_uom = _clean_text(option.get("uom"))
            raw_display_name = _clean_text(option.get("display_name"))
            conversion_factor = option.get("conversion_factor")
        else:
            raw_uom = _clean_text(option)
            raw_display_name = None
            conversion_factor = None
        if not raw_uom:
            continue

        candidate = {
            "uom": raw_uom,
            "display_name": raw_display_name or raw_uom,
            "conversion_factor": conversion_factor,
            "canonical_uom": canonical_uom(raw_uom, config) or canonical_uom(raw_display_name, config),
        }

        if normalized_requested in {
            normalize_uom_text(raw_uom),
            normalize_uom_text(raw_display_name),
        }:
            exact_matches.append(candidate)
            continue

        if requested_semantic and candidate.get("canonical_uom") == requested_semantic:
            semantic_matches.append(candidate)

    if len(exact_matches) == 1:
        return {"resolved": True, "match_type": "exact", **exact_matches[0]}
    if len(exact_matches) > 1:
        return {
            "resolved": False,
            "reason": "ambiguous_exact_match",
            "canonical_uom": requested_semantic,
            "matches": exact_matches,
        }
    if len(semantic_matches) == 1:
        return {"resolved": True, "match_type": "semantic", **semantic_matches[0]}
    if len(semantic_matches) > 1:
        return {
            "resolved": False,
            "reason": "ambiguous_semantic_match",
            "canonical_uom": requested_semantic,
            "matches": semantic_matches,
        }
    return {
        "resolved": False,
        "reason": "no_matching_uom",
        "canonical_uom": requested_semantic,
    }
