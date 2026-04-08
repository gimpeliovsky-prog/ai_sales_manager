from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.uom_semantics import canonical_uom, uom_aliases

LEAD_STATUSES = {
    "none",
    "new_lead",
    "qualified",
    "quote_needed",
    "order_ready",
    "order_created",
    "won",
    "lost",
    "service",
    "handoff",
    "stalled",
    "merged",
}

_QTY_RE = re.compile(r"\b\d+(?:[.,]\d+)?\b")
_COMPACT_QTY_UOM_RE = re.compile(r"(?<!\w)(?P<qty>\d+(?:[.,]\d+)?)(?P<uom>[^\W\d_]{1,16})(?!\w)", re.UNICODE)
_ITEM_QTY_SEGMENT_RE = re.compile(r"^\s*(?P<name>.+?)\s+(?P<qty>\d+(?:[.,]\d+)?)(?:\s+(?P<uom>[^\d,;]+))?\s*$")
_TOKEN_RE = re.compile(r"[^\W\d_]+(?:[-'][^\W\d_]+)*", re.UNICODE)
_BROWSE_SCAFFOLDING_RE = re.compile(
    r"\b(?:show\s+me|what\s+do\s+you\s+have|which\s+do\s+you\s+have|what\s+options\s+do\s+you\s+have|"
    r"какие\s+есть|что\s+есть|покажи|покажите|"
    r"מה\s+יש|תראה\s+לי|הראה\s+לי|"
    r"ما\s+عندكم|اعرض\s+لي|أرني)\b",
    re.IGNORECASE,
)
_YES_RE = re.compile(
    r"(?:\byes\b|\byeah\b|\byep\b|\bok(?:ay)?\b|\bcorrect\b|\bright\b|\bconfirmed?\b|"
    r"\u0434\u0430|\u0430\u0433\u0430|\u0432\u0435\u0440\u043d\u043e|\u043f\u0440\u0430\u0432\u0438\u043b\u044c\u043d\u043e)",
    re.IGNORECASE,
)
_GENERIC_PRODUCT_TOKENS = {
    "a", "an", "the", "i", "im", "i'm", "me", "my", "you", "your", "we", "our",
    "want", "need", "looking", "look", "for", "show", "have", "has", "do", "does",
    "what", "which", "who", "where", "when", "how", "please", "can", "could", "would",
    "product", "products", "item", "items", "model", "models", "variant", "variants",
    "option", "options", "type", "types", "name", "exact", "another", "until", "know",
    "dont", "don't", "not",
    "что", "какие", "какой", "покажи", "покажите", "есть", "товар", "товары", "позиция", "позиции",
    "модель", "модели", "вариант", "варианты", "название", "точное", "не", "знаю",
    "מה", "איזה", "יש", "תראה", "הראה", "אפשרויות", "אפשרות", "מוצר", "מוצרים", "דגם", "דגמים",
    "וריאנט", "וריאנטים", "שם", "מדויק", "לא", "יודע",
    "ما", "أي", "عندكم", "اعرض", "أرني", "خيارات", "خيار", "منتج", "منتجات", "صنف", "اصناف",
    "موديل", "موديلات", "نوع", "أنواع", "اسم", "دقيق", "لا", "اعرف",
}
_PRODUCT_INTEREST_NOISE_TERMS = [
    "price", "prices", "cost", "costs", "quote", "quotation", "discount", "discounts",
    "urgent", "asap", "today", "tomorrow", "fast", "soon", "delivery", "deliver", "shipping", "ship",
    "please", "pls",
    "цена", "цены", "стоимость", "скидка", "скидки", "кп", "срочно", "сегодня", "завтра", "быстро", "доставка",
    "пожалуйста",
    "מחיר", "מחירים", "הנחה", "הצעת", "הצעת מחיר", "דחוף", "היום", "מחר", "מהר", "משלוח",
    "בבקשה",
    "سعر", "الاسعار", "السعر", "خصم", "عرض سعر", "عاجل", "اليوم", "غدا", "توصيل", "شحن",
    "من فضلك", "رجاء",
]
_PRODUCT_INTEREST_FILLER_TERMS = [
    "ok", "okay", "hi", "hello", "hey", "thanks", "thank you",
    "ок", "хорошо", "ладно", "спасибо", "привет", "здравствуйте",
    "בסדר", "אוקי", "אוקיי", "שלום", "היי", "תודה",
    "تمام", "اوكي", "مرحبا", "اهلا", "شكرا",
]
_CONTACT_PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
_CONTACT_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_CONTACT_INTRO_RE = re.compile(
    r"(?is)\b(?:my\s+name\s+is|name\s+is|i\s+am|i'm|call\s+me|tel(?:ephone)?|phone|mobile|whatsapp|contact)\b[:\s-]*"
)
_COMMERCIAL_CUE_RE = re.compile(
    r"(?is)\b(?:i\s+want|want|need|looking\s+for|show\s+me|what\s+do\s+you\s+have|which\s+do\s+you\s+have|buy|order)\b.*"
)


def normalize_telegram_username(username: Any) -> str:
    return str(username or "").strip().lstrip("@").casefold()


DEFAULT_SIGNAL_TERMS: dict[str, list[str]] = {
    "urgency": [
        "asap",
        "urgent",
        "today",
        "tomorrow",
        "now",
        "fast",
        "rush",
        "soon",
        "срочно",
        "сегодня",
        "завтра",
        "сейчас",
        "быстро",
        "דחוף",
        "היום",
        "מחר",
        "עכשיו",
        "بسرعة",
        "عاجل",
        "اليوم",
        "غدا",
        "الآن",
    ],
    "delivery": [
        "delivery",
        "deliver",
        "shipping",
        "ship",
        "pickup",
        "address",
        "доставка",
        "доставить",
        "адрес",
        "самовывоз",
        "משלוח",
        "כתובת",
        "איסוף",
        "توصيل",
        "شحن",
        "عنوان",
        "استلام",
    ],
    "quote": [
        "quote",
        "proposal",
        "price offer",
        "commercial offer",
        "quotation",
        "кп",
        "коммерческое предложение",
        "предложение",
        "הצעת מחיר",
        "عرض سعر",
        "عرض تجاري",
    ],
    "price": [
        "price",
        "cost",
        "discount",
        "cheap",
        "cheaper",
        "цена",
        "стоимость",
        "скидка",
        "дешевле",
        "מחיר",
        "עלות",
        "הנחה",
        "سعر",
        "تكلفة",
        "خصم",
        "ارخص",
        "أرخص",
    ],
    "opt_out": [
        "stop",
        "unsubscribe",
        "do not contact",
        "don't contact",
        "remove me",
        "не пишите",
        "не писать",
        "стоп",
    ],
    "not_interested": [
        "not interested",
        "too expensive",
        "competitor",
        "no need",
        "не интересно",
        "дорого",
        "купил у других",
    ],
    "order_correction": [
        "change order",
        "update order",
        "modify order",
        "correct order",
        "change my order",
        "update my order",
        "edit order",
        "change delivery",
        "change quantity",
        "remove item",
        "replace item",
        "изменить заказ",
        "обновить заказ",
        "скорректировать заказ",
        "поменять заказ",
        "изменить количество",
        "убрать позицию",
        "добавить в заказ",
        "изменить доставку",
        "לשנות הזמנה",
        "לעדכן הזמנה",
        "לתקן הזמנה",
        "לשנות כמות",
        "להסיר פריט",
        "להוסיף להזמנה",
        "تعديل الطلب",
        "تغيير الطلب",
        "تحديث الطلب",
        "تغيير الكمية",
        "إزالة صنف",
        "إضافة إلى الطلب",
    ],
    "correction_delivery": [
        "date",
        "delivery",
        "shipping",
        "дата",
        "доставка",
        "срок",
        "תאריך",
        "משלוח",
        "אספקה",
        "تاريخ",
        "توصيل",
        "شحن",
    ],
    "correction_quantity": [
        "qty",
        "quantity",
        "amount",
        "количество",
        "кол-во",
        "כמות",
        "الكمية",
    ],
    "correction_remove": [
        "remove",
        "delete",
        "cancel item",
        "убрать",
        "удалить",
        "отменить позицию",
        "להסיר",
        "לבטל פריט",
        "إزالة",
        "حذف",
    ],
    "correction_add": [
        "add",
        "more",
        "добавить",
        "еще",
        "ещё",
        "להוסיף",
        "עוד",
        "إضافة",
        "المزيد",
    ],
}
_CURRENCY_RE = re.compile(r"[$€₪£¥₽]|(?:\b(?:usd|eur|ils|nis|rub|aed|sar)\b)", re.IGNORECASE)


def empty_lead_profile() -> dict[str, Any]:
    return {
        "status": "none",
        "lead_id": None,
        "created_at": None,
        "qualified_at": None,
        "quote_needed_at": None,
        "order_ready_at": None,
        "order_created_at": None,
        "won_at": None,
        "lost_at": None,
        "stalled_at": None,
        "hot_at": None,
        "handoff_at": None,
        "score": 0,
        "temperature": "cold",
        "next_action": "ask_need",
        "followup_strategy": "generic_stalled",
        "qualification_priority": "product_need",
        "qualification_priority_reason": None,
        "source_channel": None,
        "source_campaign": None,
        "source_utm_source": None,
        "source_utm_medium": None,
        "source_utm_campaign": None,
        "source_utm_term": None,
        "source_utm_content": None,
        "source_referrer": None,
        "source_landing_page": None,
        "source_product_page": None,
        "first_source_channel": None,
        "first_source_context": None,
        "need": None,
        "product_interest": None,
        "product_resolution_status": "unknown",
        "catalog_item_code": None,
        "catalog_item_name": None,
        "catalog_candidate_count": 0,
        "catalog_lookup_query": None,
        "catalog_lookup_status": "unknown",
        "catalog_lookup_match_count": 0,
        "catalog_lookup_at": None,
        "availability_item_code": None,
        "availability_item_name": None,
        "availability_in_stock": None,
        "availability_total_available_qty": None,
        "availability_stock_uom": None,
        "availability_warehouse": None,
        "availability_default_warehouse": None,
        "availability_known_warehouses": [],
        "availability_needs_warehouse_selection": False,
        "availability_checked_at": None,
        "quantity": None,
        "uom": None,
        "requested_items": [],
        "requested_item_count": 0,
        "requested_items_have_quantities": False,
        "requested_items_need_uom_confirmation": False,
        "requested_items_assumed_uom": None,
        "requested_items_uom_assumption_status": None,
        "urgency": None,
        "delivery_need": None,
        "price_sensitivity": False,
        "decision_status": "unknown",
        "duplicate_of_lead_id": None,
        "dedupe_reason": None,
        "dedupe_score": None,
        "dedupe_checked_at": None,
        "merged_into_lead_id": None,
        "merged_duplicate_lead_ids": [],
        "merged_at": None,
        "merged_by": None,
        "quote_status": "none",
        "quote_id": None,
        "quote_total": None,
        "quote_currency": None,
        "quote_pdf_url": None,
        "quote_requested_at": None,
        "quote_prepared_at": None,
        "quote_sent_at": None,
        "quote_accepted_at": None,
        "quote_rejected_at": None,
        "quote_last_actor_id": None,
        "quote_last_comment": None,
        "quote_last_updated_at": None,
        "expected_revenue": None,
        "order_total": None,
        "currency": None,
        "won_revenue": None,
        "active_order_state": None,
        "active_order_can_modify": None,
        "active_order_checked_at": None,
        "order_correction_status": "none",
        "target_order_id": None,
        "correction_type": None,
        "correction_requested_at": None,
        "correction_confirmed_at": None,
        "correction_applied_at": None,
        "correction_rejected_at": None,
        "correction_last_actor_id": None,
        "correction_last_comment": None,
        "followup_count": 0,
        "last_followup_attempt_at": None,
        "last_followup_at": None,
        "last_followup_delivery": None,
        "do_not_contact": False,
        "do_not_contact_reason": None,
        "do_not_contact_until": None,
        "lost_reason": None,
        "sales_owner_status": None,
        "sales_owner_action_by": None,
        "sales_owner_action_at": None,
        "sales_owner_notified_at": None,
        "sales_owner_delivery": None,
        "last_sales_event": None,
        "last_updated_at": None,
        "playbook_version": None,
    }


def normalize_lead_profile(raw_profile: Any) -> dict[str, Any]:
    profile = empty_lead_profile()
    if isinstance(raw_profile, dict):
        profile.update(raw_profile)
    if profile.get("status") not in LEAD_STATUSES:
        profile["status"] = "none"
    try:
        profile["score"] = max(0, min(100, int(profile.get("score") or 0)))
    except (TypeError, ValueError):
        profile["score"] = 0
    if profile.get("temperature") not in {"cold", "warm", "hot"}:
        profile["temperature"] = "cold"
    if not isinstance(profile.get("merged_duplicate_lead_ids"), list):
        profile["merged_duplicate_lead_ids"] = []
    if not isinstance(profile.get("requested_items"), list):
        profile["requested_items"] = []
    try:
        profile["requested_item_count"] = max(0, int(profile.get("requested_item_count") or len(profile["requested_items"])))
    except (TypeError, ValueError):
        profile["requested_item_count"] = len(profile["requested_items"])
    if profile["requested_items"] and not profile.get("requested_items_have_quantities"):
        profile["requested_items_have_quantities"] = all(isinstance(item, dict) and item.get("qty") for item in profile["requested_items"])
    else:
        profile["requested_items_have_quantities"] = bool(profile.get("requested_items_have_quantities"))
    if profile["requested_items"] and "requested_items_need_uom_confirmation" not in (raw_profile or {}):
        profile["requested_items_need_uom_confirmation"] = not (profile.get("uom") or all(isinstance(item, dict) and item.get("uom") for item in profile["requested_items"]))
    else:
        profile["requested_items_need_uom_confirmation"] = bool(profile.get("requested_items_need_uom_confirmation"))
    if profile["requested_items_need_uom_confirmation"] and not profile.get("requested_items_assumed_uom"):
        profile["requested_items_assumed_uom"] = "box"
    if profile.get("requested_items_assumed_uom") and not profile.get("requested_items_uom_assumption_status"):
        profile["requested_items_uom_assumption_status"] = "likely" if profile.get("requested_items_need_uom_confirmation") else "confirmed"
    try:
        profile["followup_count"] = max(0, int(profile.get("followup_count") or 0))
    except (TypeError, ValueError):
        profile["followup_count"] = 0
    return profile


def ensure_lead_identity(
    *,
    current_profile: Any,
    company_code: str | None,
    channel: str | None,
    channel_uid: str | None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    if not profile.get("lead_id"):
        profile["lead_id"] = f"lead_{uuid.uuid4().hex[:16]}"
    profile["company_code"] = company_code
    profile["channel"] = channel
    profile["channel_uid"] = channel_uid
    return profile


def update_lead_profile_source(
    *,
    current_profile: Any,
    channel: str | None,
    channel_context: dict[str, Any] | None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    context = channel_context if isinstance(channel_context, dict) else {}
    resolved_channel = str(channel or context.get("channel") or "").strip() or None
    if resolved_channel:
        profile["source_channel"] = profile.get("source_channel") or resolved_channel
        profile["first_source_channel"] = profile.get("first_source_channel") or resolved_channel
    source_map = {
        "source_campaign": ("campaign", "source_campaign"),
        "source_utm_source": ("utm_source",),
        "source_utm_medium": ("utm_medium",),
        "source_utm_campaign": ("utm_campaign",),
        "source_utm_term": ("utm_term",),
        "source_utm_content": ("utm_content",),
        "source_referrer": ("referrer", "referer"),
        "source_landing_page": ("landing_page", "page_url", "url"),
        "source_product_page": ("product_page", "product_url"),
    }
    for profile_key, context_keys in source_map.items():
        if profile.get(profile_key):
            continue
        for context_key in context_keys:
            value = _clean_text(context.get(context_key), limit=300)
            if value:
                profile[profile_key] = value
                break
    if not profile.get("first_source_context"):
        source_context = {
            key: value
            for key, value in context.items()
            if key
            in {
                "campaign",
                "source_campaign",
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "referrer",
                "referer",
                "landing_page",
                "page_url",
                "url",
                "product_page",
                "product_url",
                "webchat_company_code",
            }
            and value not in (None, "", [])
        }
        profile["first_source_context"] = source_context or None
    return profile


def apply_llm_lead_patch(
    *,
    current_profile: Any,
    patch: dict[str, Any] | None,
    lead_config: dict[str, Any] | None = None,
    intent: str | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    if not isinstance(patch, dict) or not patch:
        return profile

    previous_interest = _clean_text(profile.get("product_interest"))
    if "product_interest" in patch:
        normalized_interest = _normalize_single_item_interest(patch.get("product_interest"), lead_config) or _clean_text(patch.get("product_interest"))
        current_priority = str(profile.get("qualification_priority") or "product_need")
        patch_qty = _first_number(patch.get("quantity")) if "quantity" in patch else None
        patch_uom = canonical_uom(patch.get("uom"), _uom_config(lead_config, "single_item_uom_terms")) or _clean_text(patch.get("uom"), limit=40)
        allow_interest_update = bool(
            normalized_interest and (
                not previous_interest
                or _should_replace_product_interest(
                    current_interest=previous_interest,
                    normalized_text=normalized_interest,
                    current_priority=current_priority,
                    resolved_intent=str(intent or "find_product"),
                    extracted_uom=patch_uom,
                    qty=patch_qty,
                    config=lead_config,
                )
            )
        )
        if allow_interest_update:
            profile["product_interest"] = normalized_interest
            profile["need"] = profile.get("need") or normalized_interest
    if "quantity" in patch:
        quantity = _first_number(patch.get("quantity"))
        if quantity is not None:
            profile["quantity"] = quantity
    if "uom" in patch:
        resolved_uom = canonical_uom(patch.get("uom"), _uom_config(lead_config, "single_item_uom_terms")) or _clean_text(patch.get("uom"), limit=40)
        if resolved_uom:
            profile["uom"] = resolved_uom
    if "urgency" in patch and _clean_text(patch.get("urgency"), limit=40):
        profile["urgency"] = _clean_text(patch.get("urgency"), limit=40)
    if "delivery_need" in patch and _clean_text(patch.get("delivery_need"), limit=80):
        profile["delivery_need"] = _clean_text(patch.get("delivery_need"), limit=80)
    if "price_sensitivity" in patch:
        profile["price_sensitivity"] = bool(patch.get("price_sensitivity"))
    if "decision_status" in patch and _clean_text(patch.get("decision_status"), limit=40):
        profile["decision_status"] = _clean_text(patch.get("decision_status"), limit=40)

    current_interest = _clean_text(profile.get("product_interest"))
    if current_interest and current_interest != previous_interest:
        _reset_catalog_lookup_state(profile)
    _set_product_resolution_state(profile)
    return profile


def _clean_text(value: Any, *, limit: int = 160) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return None
    return text[:limit]


def _semantic_message_text(text: str) -> str:
    original = str(text or "")
    stripped = _CONTACT_EMAIL_RE.sub(" ", original)
    stripped = _CONTACT_PHONE_RE.sub(" ", stripped)
    stripped = _CONTACT_INTRO_RE.sub(" ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip(" ,.;:-")
    commercial_match = _COMMERCIAL_CUE_RE.search(stripped)
    if commercial_match:
        stripped = commercial_match.group(0).strip(" ,.;:-")
    tokens = re.findall(_TOKEN_RE, stripped)
    has_contact_payload = bool(
        _CONTACT_PHONE_RE.search(original) or _CONTACT_EMAIL_RE.search(original) or _CONTACT_INTRO_RE.search(original)
    )
    if has_contact_payload and len(tokens) <= 2:
        return ""
    return stripped


def _lead_config(config: dict[str, Any] | None) -> dict[str, Any]:
    return config if isinstance(config, dict) else {}


def _uom_config(config: dict[str, Any] | None, legacy_terms_key: str | None = None) -> dict[str, Any]:
    lead_config = _lead_config(config)
    merged: dict[str, Any] = {}
    configured_aliases = lead_config.get("uom_aliases")
    if isinstance(configured_aliases, dict):
        merged["uom_aliases"] = {key: list(values) for key, values in configured_aliases.items() if isinstance(values, list)}
    if legacy_terms_key:
        legacy_terms = lead_config.get(legacy_terms_key)
        if isinstance(legacy_terms, dict):
            merged.setdefault("uom_aliases", {})
            for canonical, values in legacy_terms.items():
                if not isinstance(values, list):
                    continue
                merged["uom_aliases"].setdefault(str(canonical), [])
                merged["uom_aliases"][str(canonical)].extend(values)
    configured_labels = lead_config.get("uom_labels")
    if isinstance(configured_labels, dict):
        merged["uom_labels"] = configured_labels
    return merged


def _multi_item_default_uom(config: dict[str, Any] | None) -> str:
    raw_value = str(_lead_config(config).get("multi_item_default_uom") or "box").strip() or "box"
    return canonical_uom(raw_value, _uom_config(config, "multi_item_uom_terms")) or raw_value


def _multi_item_uom_terms(config: dict[str, Any] | None) -> dict[str, list[str]]:
    return uom_aliases(_uom_config(config, "multi_item_uom_terms"))


def _single_item_uom_terms(config: dict[str, Any] | None) -> dict[str, list[str]]:
    return uom_aliases(_uom_config(config, "single_item_uom_terms"))


def _strip_product_interest_noise(text: str, config: dict[str, Any] | None) -> str:
    normalized = str(text or "")
    dynamic_terms: list[str] = []
    for signal in ("urgency", "delivery", "quote"):
        dynamic_terms.extend(_configured_terms(config, signal))
    terms = [term for term in [*_PRODUCT_INTEREST_NOISE_TERMS, *dynamic_terms] if str(term or "").strip()]
    for term in sorted({str(term).strip() for term in terms}, key=len, reverse=True):
        normalized = re.sub(rf"(?<!\w){re.escape(term)}(?!\w)", " ", normalized, flags=re.IGNORECASE)
    return normalized


def _extract_single_item_uom(text: str, config: dict[str, Any] | None) -> str | None:
    normalized = (text or "").casefold()
    if not normalized:
        return None
    compact_match = _COMPACT_QTY_UOM_RE.search(text or "")
    compact_uom = str(compact_match.group("uom") or "").strip() if compact_match else ""
    for uom, terms in _single_item_uom_terms(config).items():
        for term in terms:
            clean_term = str(term or "").strip()
            if not clean_term:
                continue
            if compact_uom and clean_term.casefold() == compact_uom.casefold():
                return str(uom)
            if re.search(rf"(?<!\w){re.escape(clean_term)}(?!\w)", normalized, re.IGNORECASE):
                return str(uom)
    return None


def _normalize_single_item_interest(text: str, config: dict[str, Any] | None) -> str | None:
    raw = str(text or "")
    if not raw.strip():
        return None
    normalized = raw
    normalized = re.sub(_BROWSE_SCAFFOLDING_RE, " ", normalized)
    normalized = re.sub(r"\b(?:i am looking for|i'm looking for|looking for|i want|want|need)\b", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\b(?:a|an|the)\b", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\d+(?:[.,]\d+)?", " ", normalized)
    for terms in _single_item_uom_terms(config).values():
        for term in terms:
            clean_term = str(term or "").strip()
            if clean_term:
                normalized = re.sub(rf"(?<!\w){re.escape(clean_term)}(?!\w)", " ", normalized, flags=re.IGNORECASE)
    normalized = _strip_product_interest_noise(normalized, config)
    normalized = re.sub(r"\s+", " ", normalized).strip(" ,.;:-")
    return _clean_text(normalized, limit=160)


def normalize_catalog_lookup_query(text: str | None, config: dict[str, Any] | None = None) -> str | None:
    semantic = _semantic_message_text(str(text or ""))
    candidate = _normalize_single_item_interest(semantic or str(text or ""), config)
    if candidate:
        return candidate
    fallback = _strip_product_interest_noise(semantic or str(text or ""), config)
    fallback = re.sub(_BROWSE_SCAFFOLDING_RE, " ", fallback)
    fallback = re.sub(r"\b(?:i am looking for|i'm looking for|looking for|i want|want|need)\b", " ", fallback, flags=re.IGNORECASE)
    fallback = re.sub(r"\b(?:a|an|the)\b", " ", fallback, flags=re.IGNORECASE)
    fallback = re.sub(r"\s+", " ", fallback).strip(" ,.;:-")
    return _clean_text(fallback, limit=160)


def _same_interest(a: str | None, b: str | None) -> bool:
    left = str(a or "").strip().casefold()
    right = str(b or "").strip().casefold()
    if not left or not right:
        return False
    return left == right or left in right or right in left


def _interest_tokens(text: str | None) -> list[str]:
    return [token.casefold() for token in _TOKEN_RE.findall(str(text or ""))]


def _substantive_product_tokens(text: str | None) -> list[str]:
    tokens = _interest_tokens(text)
    return [token for token in tokens if token not in _GENERIC_PRODUCT_TOKENS and len(token) > 1]


def _refines_interest(candidate: str | None, current_interest: str | None) -> bool:
    candidate_text = str(candidate or "").strip().casefold()
    current_text = str(current_interest or "").strip().casefold()
    if not candidate_text or not current_text:
        return False
    candidate_tokens = set(_substantive_product_tokens(candidate))
    current_tokens = set(_substantive_product_tokens(current_interest))
    if candidate_tokens and current_tokens and current_tokens.issubset(candidate_tokens) and candidate_tokens != current_tokens:
        return True
    return _same_interest(candidate_text, current_text) and len(candidate_text) > len(current_text) + 3


def _should_replace_product_interest(
    *,
    current_interest: str | None,
    normalized_text: str | None,
    current_priority: str,
    resolved_intent: str,
    extracted_uom: str | None,
    qty: float | None,
    config: dict[str, Any] | None,
) -> bool:
    if not normalized_text:
        return False
    if resolved_intent not in {"find_product", "browse_catalog", "order_detail"}:
        return False
    candidate = _normalize_single_item_interest(normalized_text, config)
    if not candidate:
        return False
    if not _substantive_product_tokens(candidate):
        return False
    if not current_interest:
        return True
    if _same_interest(candidate, current_interest):
        return _refines_interest(candidate, current_interest)
    if resolved_intent == "browse_catalog" and not _refines_interest(candidate, current_interest):
        return False
    if extracted_uom and candidate.casefold() == str(extracted_uom).casefold():
        return False
    if qty is not None and len(candidate) <= 4:
        return False
    if current_priority == "product_need":
        return True
    if qty is None and not extracted_uom:
        return True
    return False


def _configured_terms(config: dict[str, Any] | None, signal: str) -> list[str]:
    terms = list(DEFAULT_SIGNAL_TERMS.get(signal, []))
    configured_terms = _lead_config(config).get("signal_terms")
    if isinstance(configured_terms, dict):
        extra_terms = configured_terms.get(signal)
        if isinstance(extra_terms, list):
            terms.extend(str(term).strip() for term in extra_terms if str(term).strip())
    return terms


def _configured_regexes(config: dict[str, Any] | None, signal: str) -> list[str]:
    configured_regexes = _lead_config(config).get("signal_regexes")
    if not isinstance(configured_regexes, dict):
        return []
    regexes = configured_regexes.get(signal)
    if not isinstance(regexes, list):
        return []
    return [str(pattern).strip() for pattern in regexes if str(pattern).strip()]


def _order_correction_requested(*, user_text: str, intent: str, active_order_name: str | None, config: dict[str, Any] | None) -> bool:
    if not active_order_name:
        return False
    if intent in {"add_to_order", "service_request"}:
        return True
    if _signal_matches(user_text, "order_correction", config):
        return True
    return False


def _signal_matches(text: str, signal: str, config: dict[str, Any] | None = None) -> bool:
    normalized = (text or "").casefold()
    if not normalized:
        return False
    if signal == "price" and _CURRENCY_RE.search(text or ""):
        return True
    for term in _configured_terms(config, signal):
        if term.casefold() in normalized:
            return True
    for pattern in _configured_regexes(config, signal):
        try:
            if re.search(pattern, text or "", re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def _first_qty(text: str) -> float | None:
    match = _QTY_RE.search(text or "")
    if not match:
        compact_match = _COMPACT_QTY_UOM_RE.search(text or "")
        if compact_match:
            try:
                return float(str(compact_match.group("qty")).replace(",", "."))
            except ValueError:
                return None
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", "."))
    except ValueError:
        return None


def _parse_requested_items(text: str) -> list[dict[str, Any]]:
    parts = [part.strip() for part in re.split(r"[,;\n]+", text or "") if part.strip()]
    if len(parts) < 2:
        return []
    items: list[dict[str, Any]] = []
    for part in parts:
        match = _ITEM_QTY_SEGMENT_RE.match(part)
        if not match:
            return []
        name = _clean_text(match.group("name"), limit=120)
        if not name:
            return []
        try:
            qty = float(str(match.group("qty")).replace(",", "."))
        except ValueError:
            return []
        uom = _clean_text(match.group("uom"), limit=40)
        items.append({"item_text": name, "qty": qty, "uom": uom})
    return items


def _confirmed_multi_item_uom(text: str, profile: dict[str, Any], config: dict[str, Any] | None) -> str | None:
    normalized = (text or "").casefold()
    if not normalized:
        return None
    for uom, terms in _multi_item_uom_terms(config).items():
        if any(term.casefold() in normalized for term in terms):
            return str(uom)
    assumed_uom = str(profile.get("requested_items_assumed_uom") or "").strip()
    if assumed_uom and _YES_RE.search(text or ""):
        return assumed_uom
    return None


def _apply_multi_item_uom(profile: dict[str, Any], uom: str) -> None:
    clean_uom = str(uom or "").strip()
    if not clean_uom:
        return
    profile["uom"] = clean_uom
    profile["requested_items"] = [
        {**item, "uom": item.get("uom") or clean_uom} if isinstance(item, dict) else item
        for item in profile.get("requested_items") or []
    ]
    profile["requested_items_need_uom_confirmation"] = False
    profile["requested_items_uom_assumption_status"] = "confirmed"


def _has_quantity_detail(profile: dict[str, Any]) -> bool:
    return bool(profile.get("quantity")) or bool(profile.get("requested_items_have_quantities") and profile.get("requested_item_count"))


def _has_unit_detail(profile: dict[str, Any]) -> bool:
    if profile.get("uom"):
        return True
    return bool(profile.get("requested_item_count") and not profile.get("requested_items_need_uom_confirmation"))


def _has_specific_item_selection(profile: dict[str, Any]) -> bool:
    if profile.get("catalog_item_code"):
        return True
    return bool(profile.get("requested_items"))


def _needs_specific_item_selection(profile: dict[str, Any]) -> bool:
    return profile.get("product_resolution_status") == "broad" and not _has_specific_item_selection(profile)


def _set_product_resolution_state(profile: dict[str, Any]) -> dict[str, Any]:
    if _has_specific_item_selection(profile):
        profile["product_resolution_status"] = "specific"
    elif profile.get("product_interest"):
        profile["product_resolution_status"] = "broad"
    else:
        profile["product_resolution_status"] = "unknown"
        profile["catalog_candidate_count"] = 0
    return profile


def _reset_catalog_lookup_state(profile: dict[str, Any]) -> None:
    profile["catalog_item_code"] = None
    profile["catalog_item_name"] = None
    profile["catalog_candidate_count"] = 0
    profile["catalog_lookup_query"] = None
    profile["catalog_lookup_status"] = "unknown"
    profile["catalog_lookup_match_count"] = 0
    profile["catalog_lookup_at"] = None


def _first_number(*values: Any) -> float | None:
    for value in values:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        text = str(value or "").strip().replace(",", ".")
        if not text:
            continue
        match = re.search(r"\d+(?:\.\d+)?", text)
        if match:
            try:
                return float(match.group(0))
            except ValueError:
                continue
    return None


def _first_text(*values: Any, limit: int = 160) -> str | None:
    for value in values:
        text = _clean_text(value, limit=limit)
        if text:
            return text
    return None


def _correction_type(text: str) -> str:
    if _signal_matches(text, "correction_delivery"):
        return "delivery_or_date"
    if _signal_matches(text, "correction_quantity"):
        return "quantity"
    if _signal_matches(text, "correction_remove"):
        return "remove_item"
    if _signal_matches(text, "correction_add"):
        return "add_item"
    return "general"


def _temperature(score: int) -> str:
    if score >= 70:
        return "hot"
    if score >= 35:
        return "warm"
    return "cold"


def _score_profile(*, profile: dict[str, Any], customer_identified: bool, stage: str, intent: str) -> int:
    score = 0
    if customer_identified:
        score += 20
    if profile.get("product_interest") or intent in {"find_product", "browse_catalog"}:
        score += 20
    if _has_quantity_detail(profile):
        score += 15
    if _has_unit_detail(profile):
        score += 5
    if profile.get("urgency"):
        score += 10
    if profile.get("price_sensitivity"):
        score += 10
    if intent in {"confirm_order", "add_to_order"} or stage in {"order_build", "confirm"}:
        score += 20
    if stage in {"invoice", "closed"}:
        score += 15
    if stage == "handoff":
        score += 10
    return max(0, min(100, score))


def _status_for(*, stage: str, intent: str, profile: dict[str, Any], active_order_name: str | None) -> str:
    if profile.get("lost_reason"):
        return "lost"
    if stage == "handoff":
        return "handoff"
    if stage == "service" or intent == "service_request":
        return "service"
    if stage == "closed" and active_order_name:
        return "won"
    if profile.get("quote_status") == "requested":
        return "quote_needed"
    if active_order_name or stage == "invoice":
        return "order_created"
    if stage == "confirm" or intent == "confirm_order":
        return "order_ready"
    if intent in {"find_product", "browse_catalog"} and profile.get("price_sensitivity"):
        return "quote_needed"
    if profile.get("product_interest") and (_has_quantity_detail(profile) or _has_unit_detail(profile)):
        return "qualified"
    if stage == "lead_capture" or profile.get("product_interest"):
        return "new_lead"
    return "none"


def _next_action_for(*, status: str, stage: str, intent: str, profile: dict[str, Any], customer_identified: bool) -> str:
    if status == "handoff":
        return "handoff_manager"
    if status in {"order_created", "won"}:
        return "send_order_or_offer_invoice"
    if status == "service":
        return "fulfill_service_request"
    if not profile.get("product_interest"):
        return "ask_need"
    if _needs_specific_item_selection(profile):
        if intent == "browse_catalog" or (
            stage in {"discover", "lead_capture"} and not _has_quantity_detail(profile) and not _has_unit_detail(profile)
        ):
            return "show_matching_options"
        return "select_specific_item"
    if not _has_quantity_detail(profile):
        return "ask_quantity"
    if not _has_unit_detail(profile):
        return "ask_unit"
    if stage in {"order_build", "confirm"} and not (profile.get("urgency") or profile.get("delivery_need")):
        return "ask_delivery_timing"
    if not customer_identified:
        return "ask_contact"
    if status == "quote_needed":
        return "quote_or_clarify_price"
    if stage in {"order_build", "confirm"}:
        return "confirm_order"
    return "recommend_next_step"


def _qualification_priority_for(*, status: str, stage: str, profile: dict[str, Any], customer_identified: bool) -> tuple[str, str]:
    if not profile.get("product_interest"):
        return "product_need", "Clarify what the customer wants before asking for commercial parameters."
    if _needs_specific_item_selection(profile):
        return "specific_item_selection", "When the customer named only a broad category, first resolve the exact catalog item or show matching options before collecting more commercial parameters."
    if not _has_quantity_detail(profile):
        return "quantity", "After the product is known, quantity is the next most important sales qualification detail."
    if not _has_unit_detail(profile):
        return "unit_or_variant", "After quantity, clarify unit, package, or variant so price and order details are meaningful."
    if stage in {"order_build", "confirm"} and not (profile.get("urgency") or profile.get("delivery_need")):
        return "timing_or_delivery", "Before final order confirmation, clarify timing or delivery needs when they are still missing."
    if not customer_identified:
        return "contact", "Ask for contact only after product and core order parameters are clear."
    if status == "quote_needed":
        return "price_or_quote", "With product, quantity, and unit known, continue with price or quote handling."
    if stage in {"order_build", "confirm"}:
        return "confirmation", "Core details are known, so the next priority is explicit confirmation."
    return "next_best_action", "Core qualification is sufficient; choose the next concrete sales step."


def _llm_next_action_allowed(
    *,
    next_action: str,
    profile: dict[str, Any],
    customer_identified: bool,
    status: str,
) -> bool:
    if not next_action:
        return False
    if next_action in {"handoff_manager", "recommend_next_step"}:
        return True
    if next_action == "fulfill_service_request":
        return status == "service"
    if next_action == "ask_need":
        return not profile.get("product_interest")
    if next_action in {"show_matching_options", "select_specific_item"}:
        return bool(profile.get("product_interest")) and not bool(profile.get("catalog_item_code"))
    if next_action == "ask_quantity":
        return bool(profile.get("product_interest")) and not _has_quantity_detail(profile)
    if next_action == "ask_unit":
        return bool(profile.get("product_interest")) and _has_quantity_detail(profile) and not _has_unit_detail(profile)
    if next_action == "ask_delivery_timing":
        return bool(profile.get("product_interest")) and _has_quantity_detail(profile) and _has_unit_detail(profile)
    if next_action == "ask_contact":
        return not customer_identified and bool(profile.get("product_interest"))
    if next_action == "quote_or_clarify_price":
        return bool(profile.get("product_interest"))
    if next_action == "confirm_order":
        return bool(profile.get("product_interest")) and _has_quantity_detail(profile) and _has_unit_detail(profile)
    return False


def _set_qualification_priority(
    *,
    profile: dict[str, Any],
    status: str,
    stage: str,
    customer_identified: bool,
) -> dict[str, Any]:
    priority, reason = _qualification_priority_for(
        status=status,
        stage=stage,
        profile=profile,
        customer_identified=customer_identified,
    )
    profile["qualification_priority"] = priority
    profile["qualification_priority_reason"] = reason
    return profile


def _followup_strategy_for(*, status: str, stage: str, profile: dict[str, Any]) -> str:
    next_action = str(profile.get("next_action") or "")
    if next_action in {"show_matching_options", "select_specific_item"}:
        return "product_selection_missing"
    if profile.get("requested_items_need_uom_confirmation") or next_action == "ask_unit":
        return "uom_confirmation"
    if profile.get("price_sensitivity") or next_action == "quote_or_clarify_price":
        return "price_objection"
    if profile.get("quote_status") in {"requested", "prepared", "sent"} or next_action == "quote_followup":
        return "quote_followup"
    if next_action == "ask_quantity":
        return "catalog_browse_no_quantity"
    if next_action == "ask_contact":
        return "contact_missing"
    if next_action == "confirm_order":
        return "order_confirmation_missing"
    if next_action == "ask_delivery_timing":
        return "delivery_timing_missing"
    if status == "stalled" or next_action == "follow_up_or_handoff":
        return "generic_stalled"
    if stage in {"discover", "lead_capture"} and profile.get("product_interest") and not profile.get("quantity"):
        return "catalog_browse_no_quantity"
    return "generic_stalled"


def _set_followup_strategy(*, profile: dict[str, Any], status: str, stage: str) -> dict[str, Any]:
    profile["followup_strategy"] = _followup_strategy_for(status=status, stage=stage, profile=profile)
    return profile


def _mark_lifecycle(
    *,
    profile: dict[str, Any],
    previous_status: str | None,
    previous_temperature: str | None,
    now: datetime,
) -> dict[str, Any]:
    status = str(profile.get("status") or "none")
    temperature = str(profile.get("temperature") or "cold")
    now_iso = now.isoformat()
    if status != "none" and not profile.get("created_at"):
        profile["created_at"] = now_iso
    if status in {"qualified", "quote_needed", "order_ready", "order_created", "won"} and not profile.get("qualified_at"):
        profile["qualified_at"] = now_iso
    if status in {"order_ready", "order_created", "won"} and not profile.get("order_ready_at"):
        profile["order_ready_at"] = now_iso
    if status in {"order_created", "won"} and not profile.get("order_created_at"):
        profile["order_created_at"] = now_iso
    status_timestamp_fields = {
        "qualified": "qualified_at",
        "quote_needed": "quote_needed_at",
        "order_ready": "order_ready_at",
        "order_created": "order_created_at",
        "won": "won_at",
        "lost": "lost_at",
        "stalled": "stalled_at",
        "handoff": "handoff_at",
    }
    timestamp_field = status_timestamp_fields.get(status)
    if timestamp_field and previous_status != status and not profile.get(timestamp_field):
        profile[timestamp_field] = now_iso
    if temperature == "hot" and previous_temperature != "hot" and not profile.get("hot_at"):
        profile["hot_at"] = now_iso
    profile["last_updated_at"] = now_iso
    return profile


def update_lead_profile_from_message(
    *,
    current_profile: Any,
    user_text: str,
    stage: str | None,
    behavior_class: str | None,
    intent: str | None,
    customer_identified: bool,
    active_order_name: str | None,
    lead_config: dict[str, Any] | None = None,
    llm_state_update: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    resolved_now = datetime.now(UTC)
    resolved_stage = str(stage or "")
    resolved_intent = str(intent or "")
    current_priority = str(profile.get("qualification_priority") or "")
    if not current_priority:
        current_priority, _ = _qualification_priority_for(
            status=previous_status,
            stage=resolved_stage,
            profile=profile,
            customer_identified=customer_identified,
        )
    semantic_text = _semantic_message_text(user_text)
    normalized_text = _clean_text(semantic_text)
    requested_items = _parse_requested_items(semantic_text)
    qty = _first_qty(semantic_text)
    extracted_uom = _extract_single_item_uom(semantic_text, lead_config)

    if requested_items:
        assumed_uom = _multi_item_default_uom(lead_config)
        product_summary = "; ".join(str(item.get("item_text") or "") for item in requested_items if item.get("item_text"))
        profile["requested_items"] = requested_items
        profile["requested_item_count"] = len(requested_items)
        profile["requested_items_have_quantities"] = True
        profile["requested_items_need_uom_confirmation"] = not all(item.get("uom") for item in requested_items)
        profile["requested_items_assumed_uom"] = None if not profile["requested_items_need_uom_confirmation"] else assumed_uom
        profile["requested_items_uom_assumption_status"] = "confirmed" if not profile["requested_items_need_uom_confirmation"] else "likely"
        if product_summary:
            profile["product_interest"] = product_summary
            _reset_catalog_lookup_state(profile)
        if normalized_text and not profile.get("need"):
            profile["need"] = normalized_text
    elif profile.get("requested_items") and profile.get("requested_items_need_uom_confirmation"):
        confirmed_uom = _confirmed_multi_item_uom(user_text, profile, lead_config)
        if confirmed_uom:
            _apply_multi_item_uom(profile, confirmed_uom)
    elif _should_replace_product_interest(
        current_interest=_clean_text(profile.get("product_interest")),
        normalized_text=normalized_text,
        current_priority=current_priority,
        resolved_intent=resolved_intent,
        extracted_uom=extracted_uom,
        qty=qty,
        config=lead_config,
    ):
        normalized_interest = _normalize_single_item_interest(normalized_text, lead_config) or normalized_text
        profile["product_interest"] = normalized_interest
        profile["need"] = profile.get("need") or normalized_interest
        _reset_catalog_lookup_state(profile)
    if resolved_intent == "order_detail" and normalized_text and not profile.get("need"):
        profile["need"] = normalized_text

    if qty is not None and not requested_items:
        profile["quantity"] = qty
    if extracted_uom and not requested_items:
        profile["uom"] = extracted_uom
    _set_product_resolution_state(profile)
    if _signal_matches(user_text, "urgency", lead_config):
        profile["urgency"] = "soon"
    if _signal_matches(user_text, "delivery", lead_config):
        profile["delivery_need"] = "mentioned"
    if _signal_matches(user_text, "opt_out", lead_config):
        profile["do_not_contact"] = True
        profile["do_not_contact_reason"] = "customer_opt_out"
        profile["lost_reason"] = "opt_out"
    elif _signal_matches(user_text, "not_interested", lead_config):
        profile["lost_reason"] = "not_interested"
    if (
        behavior_class == "price_sensitive"
        or _signal_matches(user_text, "price", lead_config)
        or _signal_matches(user_text, "quote", lead_config)
    ):
        profile["price_sensitivity"] = True
    if _signal_matches(user_text, "quote", lead_config) or profile.get("price_sensitivity"):
        if profile.get("quote_status") in {None, "none"}:
            profile["quote_status"] = "requested"
            profile["quote_requested_at"] = resolved_now.isoformat()
    if resolved_intent in {"confirm_order", "add_to_order"}:
        profile["decision_status"] = "ready_to_buy"
    elif resolved_intent in {"find_product", "browse_catalog"}:
        profile["decision_status"] = "evaluating"
    if active_order_name and (
        resolved_intent == "order_detail"
        or _order_correction_requested(
            user_text=user_text,
            intent=resolved_intent,
            active_order_name=active_order_name,
            config=lead_config,
        )
    ):
        profile["order_correction_status"] = "requested"
        profile["target_order_id"] = profile.get("target_order_id") or active_order_name
        profile["correction_type"] = profile.get("correction_type") or _correction_type(user_text)
        profile["correction_requested_at"] = profile.get("correction_requested_at") or resolved_now.isoformat()
        profile["next_action"] = "clarify_order_correction"
    if profile.get("status") == "stalled" and not profile.get("lost_reason"):
        profile["status"] = profile.get("previous_status_before_stall") or "new_lead"
        profile["previous_status_before_stall"] = None
    profile["last_customer_reply_at"] = resolved_now.isoformat()

    status = _status_for(
        stage=resolved_stage,
        intent=resolved_intent,
        profile=profile,
        active_order_name=active_order_name,
    )
    profile["status"] = status
    profile["score"] = _score_profile(
        profile=profile,
        customer_identified=customer_identified,
        stage=resolved_stage,
        intent=resolved_intent,
    )
    profile["temperature"] = _temperature(int(profile["score"]))
    profile["next_action"] = _next_action_for(
        status=status,
        stage=resolved_stage,
        intent=resolved_intent,
        profile=profile,
        customer_identified=customer_identified,
    )
    llm_next_action = str((llm_state_update or {}).get("next_action") or "").strip()
    if _llm_next_action_allowed(
        next_action=llm_next_action,
        profile=profile,
        customer_identified=customer_identified,
        status=status,
    ):
        profile["next_action"] = llm_next_action
    if profile.get("order_correction_status") == "requested":
        profile["next_action"] = "clarify_order_correction"
    _set_qualification_priority(
        profile=profile,
        status=status,
        stage=resolved_stage,
        customer_identified=customer_identified,
    )
    _set_followup_strategy(profile=profile, status=status, stage=resolved_stage)
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def update_lead_profile_from_tool(
    *,
    current_profile: Any,
    tool_name: str,
    inputs: dict[str, Any],
    tool_result: dict[str, Any],
    stage: str | None,
    customer_identified: bool,
    active_order_name: str | None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    resolved_now = datetime.now(UTC)

    if tool_name == "get_product_catalog":
        interest = normalize_catalog_lookup_query(inputs.get("item_name") or inputs.get("item_group"))
        items = tool_result.get("items") if isinstance(tool_result, dict) else None
        profile["catalog_lookup_query"] = interest
        profile["catalog_lookup_at"] = resolved_now.isoformat()
        if not interest and isinstance(items, list) and items and isinstance(items[0], dict):
            interest = _clean_text(items[0].get("item_name") or items[0].get("display_item_name"))
        current_interest = _clean_text(profile.get("product_interest"))
        if interest and (
            not current_interest
            or _same_interest(interest, current_interest)
            or _refines_interest(interest, current_interest)
        ):
            profile["product_interest"] = interest
            profile["need"] = profile.get("need") or interest
        if isinstance(items, list):
            profile["catalog_candidate_count"] = len(items)
            profile["catalog_lookup_match_count"] = len(items)
            profile["catalog_lookup_status"] = "found" if items else "no_match"
            if len(items) == 1 and isinstance(items[0], dict):
                profile["catalog_item_code"] = _clean_text(items[0].get("item_code"), limit=64)
                profile["catalog_item_name"] = _clean_text(
                    items[0].get("display_item_name") or items[0].get("item_name"),
                    limit=160,
                )
            elif len(items) > 1:
                profile["catalog_item_code"] = None
                profile["catalog_item_name"] = None
            else:
                profile["catalog_item_code"] = None
                profile["catalog_item_name"] = None
        else:
            profile["catalog_lookup_status"] = "error" if tool_result.get("error") else "unknown"
            profile["catalog_lookup_match_count"] = 0

    items = inputs.get("items")
    if isinstance(items, list) and items:
        first_item = items[0] if isinstance(items[0], dict) else {}
        if first_item.get("qty"):
            profile["quantity"] = first_item.get("qty")
        if first_item.get("uom"):
            profile["uom"] = first_item.get("uom")
        if first_item.get("item_code"):
            profile["catalog_item_code"] = str(first_item.get("item_code"))
            profile["catalog_item_name"] = profile.get("catalog_item_name") or profile.get("product_interest") or str(first_item.get("item_code"))
            profile["product_interest"] = profile.get("product_interest") or str(first_item.get("item_code"))

    if tool_name == "register_buyer" and tool_result.get("erp_customer_id"):
        customer_identified = True
    if tool_name in {"create_sales_order", "update_sales_order"} and tool_result.get("name"):
        active_order_name = str(tool_result.get("name"))
    if tool_name == "get_sales_order_status" and not tool_result.get("error"):
        profile["target_order_id"] = tool_result.get("sales_order_name") or profile.get("target_order_id") or active_order_name
        profile["active_order_state"] = tool_result.get("order_state")
        profile["active_order_can_modify"] = bool(tool_result.get("can_modify"))
        profile["active_order_checked_at"] = resolved_now.isoformat()
        if profile.get("order_correction_status") == "requested":
            profile["next_action"] = "apply_order_correction" if tool_result.get("can_modify") else "handoff_manager"
    if tool_name == "get_item_availability" and not tool_result.get("error"):
        profile["availability_item_code"] = _clean_text(tool_result.get("item_code"), limit=64)
        profile["availability_item_name"] = _clean_text(tool_result.get("item_name"), limit=160)
        profile["availability_in_stock"] = tool_result.get("in_stock")
        profile["availability_total_available_qty"] = _first_number(tool_result.get("total_available_qty"))
        profile["availability_stock_uom"] = _clean_text(tool_result.get("stock_uom"), limit=32)
        profile["availability_warehouse"] = _clean_text(
            tool_result.get("effective_warehouse") or tool_result.get("requested_warehouse"),
            limit=160,
        )
        profile["availability_default_warehouse"] = _clean_text(tool_result.get("default_warehouse"), limit=160)
        known_warehouses = tool_result.get("known_warehouses")
        profile["availability_known_warehouses"] = (
            [str(item).strip() for item in known_warehouses if str(item).strip()]
            if isinstance(known_warehouses, list)
            else []
        )
        profile["availability_needs_warehouse_selection"] = bool(tool_result.get("needs_warehouse_selection"))
        profile["availability_checked_at"] = resolved_now.isoformat()
    if tool_name == "create_invoice" and tool_result.get("name"):
        profile["status"] = "won"
    if tool_name == "create_sales_order" and tool_result.get("name"):
        profile["quote_status"] = "accepted"
        profile["quote_accepted_at"] = profile.get("quote_accepted_at") or resolved_now.isoformat()
        profile["order_total"] = _first_number(
            tool_result.get("grand_total"),
            tool_result.get("rounded_total"),
            tool_result.get("total"),
            tool_result.get("net_total"),
            tool_result.get("base_grand_total"),
        )
        profile["currency"] = _first_text(tool_result.get("currency"), tool_result.get("company_currency"), limit=16)
        profile["won_revenue"] = profile.get("order_total")
    if tool_name == "update_sales_order" and tool_result.get("name"):
        profile["order_total"] = _first_number(
            tool_result.get("grand_total"),
            tool_result.get("rounded_total"),
            tool_result.get("total"),
            tool_result.get("net_total"),
            tool_result.get("base_grand_total"),
        ) or profile.get("order_total")
        profile["currency"] = _first_text(tool_result.get("currency"), tool_result.get("company_currency"), limit=16) or profile.get("currency")
    if tool_name == "create_invoice" and tool_result.get("name"):
        profile["won_revenue"] = _first_number(
            tool_result.get("grand_total"),
            tool_result.get("rounded_total"),
            tool_result.get("total"),
            tool_result.get("net_total"),
            tool_result.get("base_grand_total"),
        ) or profile.get("won_revenue") or profile.get("order_total")
        profile["currency"] = _first_text(tool_result.get("currency"), tool_result.get("company_currency"), limit=16) or profile.get("currency")
    if tool_name in {"create_quotation", "send_quote", "create_quote"} and tool_result.get("name"):
        profile["quote_id"] = tool_result.get("name")
        profile["quote_total"] = _first_number(
            tool_result.get("grand_total"),
            tool_result.get("rounded_total"),
            tool_result.get("total"),
            tool_result.get("net_total"),
            tool_result.get("base_grand_total"),
        )
        profile["quote_currency"] = _first_text(tool_result.get("currency"), tool_result.get("company_currency"), limit=16)
        profile["quote_pdf_url"] = _first_text(tool_result.get("quote_pdf_url"), tool_result.get("pdf_url"), tool_result.get("print_url"), limit=500)
        profile["quote_status"] = "sent" if profile.get("quote_pdf_url") else "prepared"
        if profile["quote_status"] == "sent":
            profile["quote_sent_at"] = profile.get("quote_sent_at") or resolved_now.isoformat()
        else:
            profile["quote_prepared_at"] = profile.get("quote_prepared_at") or resolved_now.isoformat()
        profile["expected_revenue"] = profile.get("quote_total")

    if profile.get("status") != "won":
        profile["status"] = _status_for(
            stage=str(stage or ""),
            intent="",
            profile=profile,
            active_order_name=active_order_name,
        )
    profile["score"] = _score_profile(
        profile=profile,
        customer_identified=customer_identified,
        stage=str(stage or ""),
        intent="",
    )
    profile["temperature"] = _temperature(int(profile["score"]))
    _set_product_resolution_state(profile)
    profile["next_action"] = _next_action_for(
        status=str(profile.get("status") or "none"),
        stage=str(stage or ""),
        intent="tool_result",
        profile=profile,
        customer_identified=customer_identified,
    )
    if profile.get("order_correction_status") == "requested" and profile.get("active_order_can_modify") is not None:
        profile["next_action"] = "apply_order_correction" if profile.get("active_order_can_modify") else "handoff_manager"
    _set_qualification_priority(
        profile=profile,
        status=str(profile.get("status") or "none"),
        stage=str(stage or ""),
        customer_identified=customer_identified,
    )
    _set_followup_strategy(
        profile=profile,
        status=str(profile.get("status") or "none"),
        stage=str(stage or ""),
    )
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def sales_event_type(previous_profile: Any, current_profile: Any) -> str | None:
    previous = normalize_lead_profile(previous_profile)
    current = normalize_lead_profile(current_profile)
    previous_status = previous.get("status")
    current_status = current.get("status")
    if current_status == previous_status or current_status == "none":
        return None
    return {
        "new_lead": "lead_created",
        "qualified": "lead_qualified",
        "quote_needed": "quote_requested",
        "order_ready": "order_confirmation_requested",
        "order_created": "sales_pipeline_order_created",
        "won": "sales_pipeline_won",
        "lost": "lead_lost",
        "service": "sales_pipeline_service",
        "handoff": "sales_pipeline_handoff",
        "stalled": "lead_stalled",
    }.get(str(current_status))


def sales_alert_event_types(previous_profile: Any, current_profile: Any) -> list[str]:
    previous = normalize_lead_profile(previous_profile)
    current = normalize_lead_profile(current_profile)
    events: list[str] = []
    if previous.get("temperature") != "hot" and current.get("temperature") == "hot":
        events.append("hot_lead_detected")
    if previous.get("quote_status") != "requested" and current.get("quote_status") == "requested":
        events.append("quote_followup_due")
    if previous.get("next_action") != current.get("next_action") and current.get("next_action") == "handoff_manager":
        events.append("manager_attention_required")
    return events


def build_lead_event_payload(
    *,
    session: dict[str, Any],
    previous_profile: Any | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(session.get("lead_profile"))
    previous = normalize_lead_profile(previous_profile) if previous_profile is not None else None
    payload: dict[str, Any] = {
        "lead_profile": profile,
        "lead_id": profile.get("lead_id"),
        "lead_status": profile.get("status"),
        "lead_score": profile.get("score"),
        "lead_temperature": profile.get("temperature"),
        "next_action": profile.get("next_action"),
        "followup_strategy": profile.get("followup_strategy"),
        "qualification_priority": profile.get("qualification_priority"),
        "qualification_priority_reason": profile.get("qualification_priority_reason"),
        "requested_items": profile.get("requested_items"),
        "requested_item_count": profile.get("requested_item_count"),
        "requested_items_have_quantities": profile.get("requested_items_have_quantities"),
        "requested_items_need_uom_confirmation": profile.get("requested_items_need_uom_confirmation"),
        "requested_items_assumed_uom": profile.get("requested_items_assumed_uom"),
        "requested_items_uom_assumption_status": profile.get("requested_items_uom_assumption_status"),
        "created_at": profile.get("created_at"),
        "qualified_at": profile.get("qualified_at"),
        "quote_needed_at": profile.get("quote_needed_at"),
        "order_ready_at": profile.get("order_ready_at"),
        "order_created_at": profile.get("order_created_at"),
        "won_at": profile.get("won_at"),
        "lost_at": profile.get("lost_at"),
        "stalled_at": profile.get("stalled_at"),
        "hot_at": profile.get("hot_at"),
        "handoff_at": profile.get("handoff_at"),
        "source_channel": profile.get("source_channel"),
        "source_campaign": profile.get("source_campaign"),
        "source_utm_source": profile.get("source_utm_source"),
        "source_utm_medium": profile.get("source_utm_medium"),
        "source_utm_campaign": profile.get("source_utm_campaign"),
        "source_utm_term": profile.get("source_utm_term"),
        "source_utm_content": profile.get("source_utm_content"),
        "source_referrer": profile.get("source_referrer"),
        "source_landing_page": profile.get("source_landing_page"),
        "source_product_page": profile.get("source_product_page"),
        "stage": session.get("stage"),
        "behavior_class": session.get("behavior_class"),
        "intent": session.get("last_intent"),
        "erp_customer_id": session.get("erp_customer_id"),
        "buyer_name": session.get("buyer_name"),
        "buyer_phone": session.get("buyer_phone"),
        "active_order_name": session.get("last_sales_order_name"),
        "quote_status": profile.get("quote_status"),
        "quote_id": profile.get("quote_id"),
        "quote_total": profile.get("quote_total"),
        "quote_currency": profile.get("quote_currency"),
        "quote_pdf_url": profile.get("quote_pdf_url"),
        "quote_prepared_at": profile.get("quote_prepared_at"),
        "quote_sent_at": profile.get("quote_sent_at"),
        "quote_accepted_at": profile.get("quote_accepted_at"),
        "quote_rejected_at": profile.get("quote_rejected_at"),
        "order_correction_status": profile.get("order_correction_status"),
        "target_order_id": profile.get("target_order_id"),
        "correction_type": profile.get("correction_type"),
        "correction_requested_at": profile.get("correction_requested_at"),
        "correction_confirmed_at": profile.get("correction_confirmed_at"),
        "correction_applied_at": profile.get("correction_applied_at"),
        "correction_rejected_at": profile.get("correction_rejected_at"),
        "expected_revenue": profile.get("expected_revenue"),
        "order_total": profile.get("order_total"),
        "currency": profile.get("currency"),
        "won_revenue": profile.get("won_revenue"),
        "active_order_state": profile.get("active_order_state"),
        "active_order_can_modify": profile.get("active_order_can_modify"),
        "active_order_checked_at": profile.get("active_order_checked_at"),
        "lost_reason": profile.get("lost_reason"),
        "followup_count": profile.get("followup_count"),
        "sales_owner_status": profile.get("sales_owner_status"),
        "sales_owner_action_by": profile.get("sales_owner_action_by"),
        "sales_owner_action_at": profile.get("sales_owner_action_at"),
        "playbook_version": profile.get("playbook_version"),
        "source_context": session.get("channel_context") if isinstance(session.get("channel_context"), dict) else {},
        "conversation_quality_score": session.get("conversation_quality_score"),
        "quality_flags": session.get("quality_flags") if isinstance(session.get("quality_flags"), list) else [],
        "sla_breaches": session.get("sla_breaches") if isinstance(session.get("sla_breaches"), list) else [],
    }
    if previous is not None:
        payload["previous_lead_status"] = previous.get("status")
        payload["previous_lead_score"] = previous.get("score")
        payload["previous_lead_temperature"] = previous.get("temperature")
    return payload


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def can_send_followup(
    *,
    current_profile: Any,
    lead_config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    profile = normalize_lead_profile(current_profile)
    config = _lead_config(lead_config)
    if profile.get("do_not_contact"):
        return False, "do_not_contact"
    do_not_contact_until = _parse_dt(profile.get("do_not_contact_until"))
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=UTC)
    if do_not_contact_until:
        if do_not_contact_until.tzinfo is None:
            do_not_contact_until = do_not_contact_until.replace(tzinfo=UTC)
        if resolved_now < do_not_contact_until:
            return False, "do_not_contact_until"
    try:
        max_followups = int(config.get("max_followups_per_lead", 2) or 2)
    except (TypeError, ValueError):
        max_followups = 2
    if int(profile.get("followup_count") or 0) >= max_followups:
        return False, "max_followups_reached"
    last_followup_at = _parse_dt(profile.get("last_followup_at"))
    if last_followup_at:
        if last_followup_at.tzinfo is None:
            last_followup_at = last_followup_at.replace(tzinfo=UTC)
        try:
            cooldown_minutes = int(config.get("followup_cooldown_minutes", 240) or 240)
        except (TypeError, ValueError):
            cooldown_minutes = 240
        if resolved_now - last_followup_at < timedelta(minutes=max(1, cooldown_minutes)):
            return False, "followup_cooldown"
    return True, None


def mark_lost_if_followup_exhausted(
    *,
    current_profile: Any,
    reason: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    if reason != "max_followups_reached":
        return profile
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    resolved_now = now or datetime.now(UTC)
    profile["status"] = "lost"
    profile["lost_reason"] = profile.get("lost_reason") or "no_response"
    profile["next_action"] = "stop_followup"
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def apply_sales_owner_action(
    *,
    current_profile: Any,
    action: str,
    actor_id: str | None = None,
    lost_reason: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    resolved_now = now or datetime.now(UTC)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    normalized_action = str(action or "").strip().lower()
    if normalized_action == "accept":
        profile["sales_owner_status"] = "accepted"
        profile["next_action"] = "human_owner_followup"
    elif normalized_action == "reassign":
        profile["sales_owner_status"] = "reassigned_requested"
        profile["next_action"] = "reassign_owner"
    elif normalized_action == "close":
        profile["sales_owner_status"] = "closed_not_target"
        profile["status"] = "lost"
        profile["lost_reason"] = lost_reason or "not_target"
        profile["next_action"] = "stop_followup"
        profile["do_not_contact"] = True
        profile["do_not_contact_reason"] = f"closed_by_owner:{profile['lost_reason']}"
    else:
        return profile
    profile["sales_owner_action_by"] = actor_id
    profile["sales_owner_action_at"] = resolved_now.isoformat()
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def apply_manual_close(
    *,
    current_profile: Any,
    outcome: str,
    actor_id: str | None = None,
    lost_reason: str | None = None,
    comment: str | None = None,
    order_total: Any = None,
    won_revenue: Any = None,
    currency: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    resolved_now = now or datetime.now(UTC)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    resolved_outcome = str(outcome or "").strip().casefold()
    if resolved_outcome not in {"won", "lost"}:
        return profile

    profile["status"] = resolved_outcome
    profile["manual_close_actor_id"] = actor_id
    profile["manual_closed_at"] = resolved_now.isoformat()
    profile["manual_close_comment"] = _clean_text(comment, limit=500)
    if currency:
        profile["currency"] = _clean_text(currency, limit=16)
    if order_total is not None:
        profile["order_total"] = _first_number(order_total)
    if resolved_outcome == "won":
        profile["lost_reason"] = None
        profile["won_revenue"] = _first_number(won_revenue, order_total) or profile.get("won_revenue") or profile.get("order_total")
        if profile.get("quote_status") in {"requested", "prepared", "sent"}:
            profile["quote_status"] = "accepted"
            profile["quote_accepted_at"] = profile.get("quote_accepted_at") or resolved_now.isoformat()
        profile["next_action"] = "closed_won"
    else:
        profile["lost_reason"] = _clean_text(lost_reason, limit=80) or "manual_close"
        if profile.get("quote_status") in {"requested", "prepared", "sent"}:
            profile["quote_status"] = "rejected"
            profile["quote_rejected_at"] = profile.get("quote_rejected_at") or resolved_now.isoformat()
        profile["do_not_contact"] = True
        profile["do_not_contact_reason"] = f"manual_close:{profile['lost_reason']}"
        profile["next_action"] = "stop_followup"
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def apply_quote_update(
    *,
    current_profile: Any,
    quote_status: str,
    actor_id: str | None = None,
    quote_id: Any = None,
    quote_total: Any = None,
    quote_currency: Any = None,
    quote_pdf_url: Any = None,
    comment: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    resolved_now = now or datetime.now(UTC)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    resolved_status = str(quote_status or "").strip().casefold()
    if resolved_status not in {"sent", "accepted", "rejected"}:
        return profile

    if quote_id is not None:
        profile["quote_id"] = _clean_text(quote_id, limit=120)
    if quote_total is not None:
        profile["quote_total"] = _first_number(quote_total)
        profile["expected_revenue"] = profile.get("quote_total")
    if quote_currency is not None:
        profile["quote_currency"] = _clean_text(quote_currency, limit=16)
    if quote_pdf_url is not None:
        profile["quote_pdf_url"] = _clean_text(quote_pdf_url, limit=500)

    profile["quote_status"] = resolved_status
    profile["quote_last_actor_id"] = actor_id
    profile["quote_last_comment"] = _clean_text(comment, limit=500)
    profile["quote_last_updated_at"] = resolved_now.isoformat()
    if resolved_status == "sent":
        profile["quote_prepared_at"] = profile.get("quote_prepared_at") or resolved_now.isoformat()
        profile["quote_sent_at"] = resolved_now.isoformat()
        if profile.get("status") in {"none", "new_lead", "qualified", "quote_needed"}:
            profile["status"] = "quote_needed"
        profile["next_action"] = "quote_followup"
    elif resolved_status == "accepted":
        profile["quote_accepted_at"] = resolved_now.isoformat()
        profile["next_action"] = "confirm_order"
        if profile.get("status") in {"none", "new_lead", "qualified", "quote_needed", "stalled"}:
            profile["status"] = "order_ready"
    elif resolved_status == "rejected":
        profile["quote_rejected_at"] = resolved_now.isoformat()
        if not profile.get("lost_reason"):
            profile["lost_reason"] = "quote_rejected"
        profile["status"] = "lost"
        profile["do_not_contact"] = True
        profile["do_not_contact_reason"] = "quote_rejected"
        profile["next_action"] = "stop_followup"

    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def apply_lead_merge(
    *,
    current_profile: Any,
    target_lead_id: str,
    actor_id: str | None = None,
    comment: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    resolved_now = now or datetime.now(UTC)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    profile["status"] = "merged"
    profile["merged_into_lead_id"] = _clean_text(target_lead_id, limit=120)
    profile["duplicate_of_lead_id"] = profile.get("duplicate_of_lead_id") or profile["merged_into_lead_id"]
    profile["dedupe_reason"] = profile.get("dedupe_reason") or "manual_merge"
    profile["merged_at"] = resolved_now.isoformat()
    profile["merged_by"] = actor_id
    profile["manual_close_comment"] = _clean_text(comment, limit=500)
    profile["do_not_contact"] = True
    profile["do_not_contact_reason"] = "lead_merged"
    profile["next_action"] = "merged_into_existing_lead"
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def record_merged_duplicate(
    *,
    current_profile: Any,
    duplicate_lead_id: str,
    actor_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    duplicate = _clean_text(duplicate_lead_id, limit=120)
    merged_ids = [str(item) for item in profile.get("merged_duplicate_lead_ids") or [] if str(item)]
    if duplicate and duplicate not in merged_ids:
        merged_ids.append(duplicate)
    profile["merged_duplicate_lead_ids"] = merged_ids[-100:]
    profile["merged_at"] = profile.get("merged_at") or (now or datetime.now(UTC)).isoformat()
    profile["merged_by"] = actor_id or profile.get("merged_by")
    return profile


def apply_order_correction_update(
    *,
    current_profile: Any,
    correction_status: str,
    target_order_id: Any = None,
    correction_type: Any = None,
    actor_id: str | None = None,
    comment: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    resolved_now = now or datetime.now(UTC)
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    status = str(correction_status or "").strip().casefold()
    if status not in {"requested", "confirmed", "applied", "rejected"}:
        return profile
    profile["order_correction_status"] = status
    if target_order_id is not None:
        profile["target_order_id"] = _clean_text(target_order_id, limit=120)
    if correction_type is not None:
        profile["correction_type"] = _clean_text(correction_type, limit=80)
    profile["correction_last_actor_id"] = actor_id
    profile["correction_last_comment"] = _clean_text(comment, limit=500)
    timestamp_key = {
        "requested": "correction_requested_at",
        "confirmed": "correction_confirmed_at",
        "applied": "correction_applied_at",
        "rejected": "correction_rejected_at",
    }[status]
    profile[timestamp_key] = resolved_now.isoformat()
    if status == "requested":
        profile["status"] = "service" if profile.get("status") in {"none", "new_lead"} else profile.get("status")
        profile["next_action"] = "clarify_order_correction"
    elif status == "confirmed":
        profile["next_action"] = "apply_order_correction"
    elif status == "applied":
        profile["next_action"] = "confirm_correction_applied"
    else:
        profile["next_action"] = "correction_rejected"
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def mark_stalled_if_needed(
    *,
    current_profile: Any,
    last_interaction_at: Any,
    now: datetime | None = None,
    idle_after: timedelta = timedelta(minutes=60),
) -> dict[str, Any]:
    profile = normalize_lead_profile(current_profile)
    if profile.get("sales_owner_status") in {"accepted", "closed_not_target"}:
        return profile
    if profile.get("status") in {"none", "order_created", "won", "lost", "closed", "service", "handoff", "stalled", "merged"}:
        return profile
    if profile.get("do_not_contact"):
        return profile
    if profile.get("temperature") == "cold":
        return profile
    last_seen_at = _parse_dt(last_interaction_at)
    if not last_seen_at:
        return profile
    resolved_now = now or datetime.now(UTC)
    if last_seen_at.tzinfo is None:
        last_seen_at = last_seen_at.replace(tzinfo=UTC)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=UTC)
    if resolved_now - last_seen_at < idle_after:
        return profile
    previous_status = str(profile.get("status") or "none")
    previous_temperature = str(profile.get("temperature") or "cold")
    profile["previous_status_before_stall"] = profile.get("status")
    profile["status"] = "stalled"
    profile["next_action"] = "follow_up_or_handoff"
    _set_followup_strategy(profile=profile, status="stalled", stage="")
    return _mark_lifecycle(
        profile=profile,
        previous_status=previous_status,
        previous_temperature=previous_temperature,
        now=resolved_now,
    )


def build_handoff_summary(session: dict[str, Any], *, reason: str | None = None) -> dict[str, Any]:
    profile = normalize_lead_profile(session.get("lead_profile"))
    return {
        "reason": reason,
        "lead_status": profile.get("status"),
        "lead_score": profile.get("score"),
        "lead_temperature": profile.get("temperature"),
        "next_action": profile.get("next_action"),
        "followup_strategy": profile.get("followup_strategy"),
        "qualification_priority": profile.get("qualification_priority"),
        "qualification_priority_reason": profile.get("qualification_priority_reason"),
        "created_at": profile.get("created_at"),
        "qualified_at": profile.get("qualified_at"),
        "quote_needed_at": profile.get("quote_needed_at"),
        "order_ready_at": profile.get("order_ready_at"),
        "order_created_at": profile.get("order_created_at"),
        "won_at": profile.get("won_at"),
        "lost_at": profile.get("lost_at"),
        "stalled_at": profile.get("stalled_at"),
        "hot_at": profile.get("hot_at"),
        "handoff_at": profile.get("handoff_at"),
        "source_channel": profile.get("source_channel"),
        "source_campaign": profile.get("source_campaign"),
        "source_utm_source": profile.get("source_utm_source"),
        "source_utm_medium": profile.get("source_utm_medium"),
        "source_utm_campaign": profile.get("source_utm_campaign"),
        "source_referrer": profile.get("source_referrer"),
        "source_landing_page": profile.get("source_landing_page"),
        "source_product_page": profile.get("source_product_page"),
        "buyer_name": session.get("buyer_name"),
        "buyer_phone": session.get("buyer_phone"),
        "erp_customer_id": session.get("erp_customer_id"),
        "product_interest": profile.get("product_interest"),
        "need": profile.get("need"),
        "quantity": profile.get("quantity"),
        "uom": profile.get("uom"),
        "requested_items": profile.get("requested_items"),
        "requested_item_count": profile.get("requested_item_count"),
        "requested_items_have_quantities": profile.get("requested_items_have_quantities"),
        "requested_items_need_uom_confirmation": profile.get("requested_items_need_uom_confirmation"),
        "requested_items_assumed_uom": profile.get("requested_items_assumed_uom"),
        "requested_items_uom_assumption_status": profile.get("requested_items_uom_assumption_status"),
        "urgency": profile.get("urgency"),
        "delivery_need": profile.get("delivery_need"),
        "price_sensitivity": profile.get("price_sensitivity"),
        "decision_status": profile.get("decision_status"),
        "duplicate_of_lead_id": profile.get("duplicate_of_lead_id"),
        "dedupe_reason": profile.get("dedupe_reason"),
        "dedupe_score": profile.get("dedupe_score"),
        "merged_into_lead_id": profile.get("merged_into_lead_id"),
        "quote_status": profile.get("quote_status"),
        "quote_id": profile.get("quote_id"),
        "quote_total": profile.get("quote_total"),
        "quote_currency": profile.get("quote_currency"),
        "expected_revenue": profile.get("expected_revenue"),
        "order_total": profile.get("order_total"),
        "currency": profile.get("currency"),
        "won_revenue": profile.get("won_revenue"),
        "active_order_state": profile.get("active_order_state"),
        "active_order_can_modify": profile.get("active_order_can_modify"),
        "lost_reason": profile.get("lost_reason"),
        "followup_count": profile.get("followup_count"),
        "sales_owner_status": profile.get("sales_owner_status"),
        "active_order_name": session.get("last_sales_order_name"),
        "recent_sales_orders": session.get("recent_sales_orders") or [],
        "recent_sales_invoices": session.get("recent_sales_invoices") or [],
    }
