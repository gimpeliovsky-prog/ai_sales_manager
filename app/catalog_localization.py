from __future__ import annotations

from typing import Any

from app.i18n import normalize_lang, text as i18n_text
from app.uom_semantics import canonical_uom, localize_available_uom_options, localize_uom_label


def catalog_lang(lang: str | None) -> str | None:
    normalized = normalize_lang(lang)
    return normalized if normalized and normalized != "auto" else None


def _catalog_name_fallback_languages(lang: str | None, ai_policy: dict[str, Any] | None = None) -> list[str]:
    requested_lang = catalog_lang(lang)
    languages: list[str] = []
    if requested_lang:
        languages.append(requested_lang)
    catalog_policy = ai_policy.get("catalog") if isinstance(ai_policy, dict) and isinstance(ai_policy.get("catalog"), dict) else {}
    configured = catalog_policy.get("item_name_fallback_languages") or catalog_policy.get("name_fallback_languages")
    if isinstance(configured, list):
        languages.extend(str(item) for item in configured)
    languages.extend(["en", "default"])

    normalized_languages: list[str] = []
    for language in languages:
        normalized = normalize_lang(language)
        if normalized and normalized != "auto" and normalized not in normalized_languages:
            normalized_languages.append(normalized)
    return normalized_languages


def _clean_catalog_name(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _uom_label(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _extract_catalog_translation_text(value: Any) -> str | None:
    if isinstance(value, str):
        return _clean_catalog_name(value)
    if isinstance(value, dict):
        for key in ("display_item_name", "translated_item_name", "translation", "item_name", "name", "value", "text"):
            text = _clean_catalog_name(value.get(key))
            if text:
                return text
    return None


def _translation_map_from_item(item: dict[str, Any]) -> dict[str, str]:
    translations: dict[str, str] = {}
    for field in ("item_name_translations", "translated_item_names", "translated_names", "translations", "item_translations"):
        payload = item.get(field)
        if isinstance(payload, dict):
            for language, value in payload.items():
                normalized = normalize_lang(language)
                text = _extract_catalog_translation_text(value)
                if normalized and text:
                    translations.setdefault(normalized, text)
        elif isinstance(payload, list):
            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                language = (
                    entry.get("lang")
                    or entry.get("language")
                    or entry.get("language_code")
                    or entry.get("locale")
                )
                normalized = normalize_lang(language)
                text = _extract_catalog_translation_text(entry)
                if normalized and text:
                    translations.setdefault(normalized, text)
    return translations


def _select_catalog_display_name(
    item: dict[str, Any],
    lang: str,
    ai_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_lang = catalog_lang(lang)
    canonical_name = _clean_catalog_name(item.get("item_name"))
    existing_display = _clean_catalog_name(
        item.get("display_item_name")
        or item.get("translated_item_name")
        or item.get("translated_name")
    )
    existing_display_lang = catalog_lang(
        item.get("display_item_name_lang")
        or item.get("translated_item_name_lang")
        or item.get("translation_lang")
        or item.get("language")
    )

    if existing_display:
        selected_lang = existing_display_lang or requested_lang
        return {
            "name": existing_display,
            "lang": selected_lang,
            "source": "license_server_display_name",
            "missing_requested_translation": bool(requested_lang and selected_lang and selected_lang != requested_lang),
            "canonical_name": canonical_name,
        }

    translations = _translation_map_from_item(item)
    for candidate_lang in _catalog_name_fallback_languages(lang, ai_policy):
        translated = translations.get(candidate_lang)
        if translated:
            return {
                "name": translated,
                "lang": candidate_lang,
                "source": "requested_translation" if candidate_lang == requested_lang else "fallback_translation",
                "missing_requested_translation": bool(requested_lang and candidate_lang != requested_lang),
                "canonical_name": canonical_name,
            }

    any_lang, any_translation = next(iter(translations.items()), (None, None))
    if any_translation:
        return {
            "name": any_translation,
            "lang": any_lang,
            "source": "any_available_translation",
            "missing_requested_translation": bool(requested_lang and any_lang != requested_lang),
            "canonical_name": canonical_name,
        }

    return {
        "name": canonical_name,
        "lang": None,
        "source": "canonical_item_name",
        "missing_requested_translation": bool(requested_lang and canonical_name),
        "canonical_name": canonical_name,
    }


def localize_catalog_result(result: dict[str, Any], lang: str, ai_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result
    items = result.get("items")
    if not isinstance(items, list):
        return result

    localized_items: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            localized_items.append(item)
            continue
        localized = dict(item)
        display_name = _select_catalog_display_name(localized, lang, ai_policy)
        if display_name.get("canonical_name"):
            localized.setdefault("canonical_item_name", display_name["canonical_name"])
        if display_name.get("name"):
            localized["item_name"] = display_name["name"]
            localized["display_item_name"] = display_name["name"]
        localized["display_item_name_lang"] = display_name.get("lang")
        localized["display_item_name_source"] = display_name.get("source")
        localized["missing_requested_item_name_translation"] = bool(display_name.get("missing_requested_translation"))
        available_uoms = localized.get("available_uoms")
        if isinstance(available_uoms, list):
            updated_uoms: list[dict[str, Any]] = []
            for uom in available_uoms:
                if not isinstance(uom, dict):
                    updated_uoms.append(uom)
                    continue
                updated = dict(uom)
                localized_label = localize_uom_label(updated.get("display_name") or updated.get("uom"), lang, ai_policy)
                updated["display_name"] = localized_label or _uom_label(updated.get("display_name") or updated.get("uom"))
                updated["uom_semantic"] = canonical_uom(updated.get("uom") or updated.get("display_name"), ai_policy)
                updated_uoms.append(updated)
            localized["available_uoms"] = updated_uoms
            localized["non_stock_uoms"] = [uom for uom in updated_uoms if isinstance(uom, dict) and not uom.get("is_stock_uom")]
        if localized.get("stock_uom") or localized.get("stock_uom_label"):
            stock_label = localize_uom_label(
                localized.get("stock_uom_label") or localized.get("stock_uom"),
                lang,
                ai_policy,
            )
            if stock_label:
                localized["stock_uom_label"] = stock_label
        if localized.get("sales_uom"):
            sales_uom_label = localize_uom_label(localized.get("sales_uom_label") or localized.get("sales_uom"), lang, ai_policy)
            if sales_uom_label:
                localized["sales_uom_label"] = sales_uom_label
        labels = localize_available_uom_options(
            localized.get("stock_uom_label") or localized.get("stock_uom"),
            localized.get("non_stock_uoms"),
            lang=lang,
            config=ai_policy,
        )
        if labels:
            localized["customer_uom_options"] = labels
            localized["customer_uom_summary_key"] = "catalog.sold_in"
            localized["customer_uom_summary"] = i18n_text(
                "catalog.sold_in",
                lang,
                {"options": ", ".join(labels)},
                ai_policy=ai_policy,
            )
        localized_items.append(localized)

    localized_result = dict(result)
    localized_result["items"] = localized_items
    return localized_result
