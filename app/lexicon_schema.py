from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_LEXICON_ROOT = Path(__file__).with_name("lexicons")
_LEAD_LIST_SECTIONS = {
    "browse_scaffolding_terms",
    "yes_terms",
    "contact_intro_terms",
    "commercial_cue_terms",
    "single_item_cleanup_terms",
    "generic_product_tokens",
    "product_interest_noise_terms",
    "product_interest_filler_terms",
}
_LEAD_DICT_LIST_SECTIONS = {
    "signal_terms",
    "signal_regexes",
}
_LEAD_ALLOWED_KEYS = _LEAD_LIST_SECTIONS | _LEAD_DICT_LIST_SECTIONS | {"defaults"}
_UOM_ALLOWED_KEYS = {"aliases", "labels"}
_CONVERSATION_TERM_SECTIONS = {
    "service_terms",
    "price_terms",
    "direct_buy_terms",
    "explore_terms",
    "frustrated_terms",
    "order_terms",
    "add_to_order_terms",
    "human_terms",
}
_CONVERSATION_REGEX_SECTIONS = {
    "service_regexes",
    "price_regexes",
    "direct_buy_regexes",
    "explore_regexes",
    "frustrated_regexes",
    "order_regexes",
    "add_to_order_regexes",
    "human_regexes",
}
_CONVERSATION_PATTERN_SECTIONS = {"contact_details_patterns"}
_CONVERSATION_ALLOWED_KEYS = _CONVERSATION_TERM_SECTIONS | _CONVERSATION_REGEX_SECTIONS | _CONVERSATION_PATTERN_SECTIONS
_LANG_FILENAME_RE = re.compile(r"^[a-z]{2,3}(?:-[a-z0-9]+)?$", re.IGNORECASE)


def _validate_string_list(value: Any, *, path_label: str) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, list):
        return [f"{path_label}: expected list[str]"]
    seen: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, str):
            errors.append(f"{path_label}[{index}]: expected string")
            continue
        term = item.strip()
        if not term:
            errors.append(f"{path_label}[{index}]: empty string")
            continue
        key = term.casefold()
        if key in seen:
            errors.append(f"{path_label}[{index}]: duplicate term {term!r}")
            continue
        seen.add(key)
    return errors


def _validate_regex_list(value: Any, *, path_label: str) -> list[str]:
    errors = _validate_string_list(value, path_label=path_label)
    if errors:
        return errors
    for index, pattern in enumerate(value):
        try:
            re.compile(str(pattern))
        except re.error as exc:
            errors.append(f"{path_label}[{index}]: invalid regex ({exc})")
    return errors


def _validate_string_map_of_lists(value: Any, *, path_label: str, regex_values: bool = False) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, dict):
        return [f"{path_label}: expected dict[str, list[str]]"]
    for key, bucket in value.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            errors.append(f"{path_label}: empty key")
            continue
        validator = _validate_regex_list if regex_values else _validate_string_list
        errors.extend(validator(bucket, path_label=f"{path_label}.{clean_key}"))
    return errors


def _validate_uom_aliases(value: Any, *, path_label: str) -> list[str]:
    return _validate_string_map_of_lists(value, path_label=path_label)


def _validate_uom_labels(value: Any, *, path_label: str) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, dict):
        return [f"{path_label}: expected dict[str, dict[str, str]]"]
    for canonical, localized_values in value.items():
        canonical_key = str(canonical or "").strip()
        if not canonical_key:
            errors.append(f"{path_label}: empty canonical key")
            continue
        if not isinstance(localized_values, dict):
            errors.append(f"{path_label}.{canonical_key}: expected dict[str, str]")
            continue
        for lang, label in localized_values.items():
            clean_lang = str(lang or "").strip()
            clean_label = str(label or "").strip()
            if not clean_lang:
                errors.append(f"{path_label}.{canonical_key}: empty language key")
            elif not _LANG_FILENAME_RE.match(clean_lang):
                errors.append(f"{path_label}.{canonical_key}.{clean_lang}: invalid language code")
            if not clean_label:
                errors.append(f"{path_label}.{canonical_key}.{clean_lang}: empty label")
    return errors


def _validate_defaults(value: Any, *, path_label: str) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, dict):
        return [f"{path_label}: expected dict[str, scalar]"]
    for key, item in value.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            errors.append(f"{path_label}: empty key")
            continue
        if isinstance(item, (dict, list, tuple, set)):
            errors.append(f"{path_label}.{clean_key}: expected scalar value")
    return errors


def _validate_payload_keys(payload: dict[str, Any], *, allowed_keys: set[str], path_label: str) -> list[str]:
    errors: list[str] = []
    for key in payload.keys():
        if key not in allowed_keys:
            errors.append(f"{path_label}: unknown top-level key {key!r}")
    return errors


def validate_lead_lexicon_file(path: str | Path) -> list[str]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    path_label = str(Path(path).name)
    if not isinstance(payload, dict):
        return [f"{path_label}: expected top-level object"]
    errors = _validate_payload_keys(payload, allowed_keys=_LEAD_ALLOWED_KEYS, path_label=path_label)
    for section in _LEAD_LIST_SECTIONS:
        if section in payload:
            errors.extend(_validate_string_list(payload.get(section), path_label=f"{path_label}.{section}"))
    for section in _LEAD_DICT_LIST_SECTIONS:
        if section in payload:
            errors.extend(
                _validate_string_map_of_lists(
                    payload.get(section),
                    path_label=f"{path_label}.{section}",
                    regex_values=section == "signal_regexes",
                )
            )
    if "defaults" in payload:
        errors.extend(_validate_defaults(payload.get("defaults"), path_label=f"{path_label}.defaults"))
    return errors


def validate_uom_lexicon_file(path: str | Path) -> list[str]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    path_label = str(Path(path).name)
    if not isinstance(payload, dict):
        return [f"{path_label}: expected top-level object"]
    errors = _validate_payload_keys(payload, allowed_keys=_UOM_ALLOWED_KEYS, path_label=path_label)
    if "aliases" in payload:
        errors.extend(_validate_uom_aliases(payload.get("aliases"), path_label=f"{path_label}.aliases"))
    if "labels" in payload:
        errors.extend(_validate_uom_labels(payload.get("labels"), path_label=f"{path_label}.labels"))
    return errors


def validate_conversation_lexicon_file(path: str | Path) -> list[str]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    path_label = str(Path(path).name)
    if not isinstance(payload, dict):
        return [f"{path_label}: expected top-level object"]
    errors = _validate_payload_keys(payload, allowed_keys=_CONVERSATION_ALLOWED_KEYS, path_label=path_label)
    for section in _CONVERSATION_TERM_SECTIONS:
        if section in payload:
            errors.extend(_validate_string_list(payload.get(section), path_label=f"{path_label}.{section}"))
    for section in _CONVERSATION_REGEX_SECTIONS | _CONVERSATION_PATTERN_SECTIONS:
        if section in payload:
            errors.extend(_validate_regex_list(payload.get(section), path_label=f"{path_label}.{section}"))
    return errors


def validate_all_lexicons() -> list[str]:
    errors: list[str] = []
    families: list[tuple[str, Path, Any]] = [
        ("lead_management", _LEXICON_ROOT / "lead_management", validate_lead_lexicon_file),
        ("uom", _LEXICON_ROOT / "uom", validate_uom_lexicon_file),
        ("conversation_flow", _LEXICON_ROOT / "conversation_flow", validate_conversation_lexicon_file),
    ]
    for family_name, directory, validator in families:
        if not directory.exists():
            errors.append(f"{family_name}: missing directory {directory}")
            continue
        json_files = sorted(directory.glob("*.json"))
        if not json_files:
            errors.append(f"{family_name}: expected at least one json lexicon file")
            continue
        for path in json_files:
            if not _LANG_FILENAME_RE.match(path.stem):
                errors.append(f"{family_name}: invalid lexicon filename {path.name!r}")
            errors.extend(f"{family_name}:{error}" for error in validator(path))
    return errors
