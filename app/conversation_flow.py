from __future__ import annotations

import re
from typing import Any

from app.inbound_policy import has_product_context

from app.conversation_boundary import is_short_greeting_message
from app.conversation_lexicon import (
    add_to_order_regex,
    contact_details_regex,
    direct_buy_regex,
    explore_regex,
    frustrated_regex,
    human_regex,
    order_regex,
    price_regex,
    service_regex,
)
from app.lead_lexicon import commercial_cue_regex, generic_product_tokens
from app.i18n import text as i18n_text

DEFAULT_STAGE = "discover"
DEFAULT_BEHAVIOR_CLASS = "unclear_request"
DEFAULT_SIGNAL_TYPE = "deal_progress"
SIGNAL_TYPES = {
    "deal_progress",
    "small_talk",
    "price_objection",
    "discount_request",
    "analogs_request",
    "comparison_request",
    "delivery_question",
    "availability_question",
    "topic_shift",
    "frustration",
    "confirmation",
    "service_request",
    "stalling",
    "resume_previous_context",
    "low_signal",
    "handoff_request",
}
_SUPPORTED_REGEX_FLAGS = {
    "IGNORECASE": re.IGNORECASE,
    "MULTILINE": re.MULTILINE,
    "DOTALL": re.DOTALL,
}

STAGE_PROMPTS: dict[str, list[str]] = {
    "new": [
        "Treat this as a new conversation and establish context quickly.",
        "Move the customer to identification or product discovery without filler.",
    ],
    "identify": [
        "Your goal is to identify the customer before continuing the sales flow.",
        "Ask only for the minimum missing contact details and do not move to order creation yet.",
    ],
    "lead_capture": [
        "The buyer is not identified yet, but the message has commercial intent.",
        "Give useful low-risk product guidance when tool-backed data is available, then ask for the minimum contact details needed to continue.",
        "Do not create orders, invoices, licenses, or delivery commitments until the buyer is identified.",
    ],
    "discover": [
        "Your goal is to understand which product or service the customer wants.",
        "Offer at most two or three relevant options when the request is broad.",
    ],
    "clarify": [
        "Your goal is to clarify missing product, quantity, variant, or UOM details.",
        "Ask one focused clarification question at a time.",
        "Use the qualification order: product/need first, then quantity, then unit/package/variant, then timing or delivery when needed, then contact details.",
        "If the customer sent a list of item names with numbers, treat quantities as provided; boxes are the likely UOM when no other rule is known, but clarify the unit/package before order confirmation.",
    ],
    "order_build": [
        "Your goal is to assemble or update the customer's order.",
        "Make sure item, quantity, and unit are clear before creating or updating an order.",
    ],
    "confirm": [
        "Your goal is to get a clear confirmation for the current order contents.",
        "Do not create a new order until the customer clearly confirms.",
    ],
    "invoice": [
        "Your goal is to help the customer move from confirmed order to invoice.",
        "Keep the reply short and operational.",
    ],
    "service": [
        "Your goal is to fulfill a service request such as sending an order PDF or status.",
        "Do not push the customer back into product discovery unless they ask for it.",
    ],
    "handoff": [
        "Your goal is to hand the conversation to a human manager.",
        "Be transparent, concise, and stop improvising on uncertain business details.",
    ],
    "closed": [
        "The main task is complete.",
        "Only handle follow-up service requests or reopen the flow if the customer starts a new request.",
    ],
}

BEHAVIOR_PROMPTS: dict[str, list[str]] = {
    "direct_buyer": [
        "The customer is acting like a direct buyer.",
        "Be brief, transactional, and move quickly to the next concrete step.",
    ],
    "explorer": [
        "The customer is exploring options.",
        "Help compare choices, but avoid dumping large catalogs.",
    ],
    "unclear_request": [
        "The customer request is still unclear.",
        "Prefer a single clarifying question over long explanations.",
    ],
    "price_sensitive": [
        "The customer is focused on price or discounts.",
        "Anchor the conversation on the exact item and unit before discussing price details.",
    ],
    "frustrated": [
        "The customer appears frustrated or impatient.",
        "Acknowledge friction once, then move directly to resolution or handoff.",
    ],
    "service_request": [
        "The customer is asking for an operational service action rather than a new sale.",
        "Prioritize the requested document or status update.",
    ],
    "returning_customer": [
        "The customer appears to be returning with prior context.",
        "Reuse known context and avoid re-asking solved questions.",
    ],
    "silent_or_low_signal": [
        "The customer message has very little signal.",
        "Keep the response short and ask for the single most important missing detail.",
    ],
}

CHANNEL_PROMPTS: dict[str, list[str]] = {
    "telegram": [
        "Use short chat-style paragraphs suitable for Telegram.",
    ],
    "whatsapp": [
        "Keep replies especially compact for WhatsApp.",
    ],
    "webchat": [
        "Web chat allows slightly more structure, but stay concise.",
    ],
}

HANDOFF_MESSAGES: dict[str, str] = {
    "ru": "Подключаю менеджера. Чтобы не ошибиться в деталях, дальше вам поможет человек.",
    "en": "I'm connecting a manager so we don't make a mistake on the details. A human will continue from here.",
    "he": "אני מעביר את השיחה למנהל כדי שלא נטעה בפרטים. נציג אנושי ימשיך מכאן.",
    "ar": "سأحوّل المحادثة إلى مدير حتى لا نخطئ في التفاصيل. سيتابع معك شخص من الفريق.",
}

_SERVICE_RE = re.compile(
    r"\b(send (?:me )?order|send my order|order pdf|order file|invoice|pdf|status|subscription|renew|"
    r"пришли заказ|отправь заказ|счет|инвойс|статус|продли|"
    r"אישור|חשבונית|סטטוס|"
    r"فاتورة|حالة|اشتراك)\b",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(
    r"\b(price|cost|discount|cheap|cheaper|скидка|цена|дешевле|"
    r"מחיר|הנחה|"
    r"سعر|خصم|ارخص)\b",
    re.IGNORECASE,
)
_DIRECT_BUY_RE = re.compile(
    r"\b(confirm|go ahead|place order|buy now|оформляй|заказываю|беру|"
    r"אשר|תזמין|"
    r"أكد|اطلب)\b",
    re.IGNORECASE,
)
_EXPLORE_RE = re.compile(
    r"\b(what do you have|show me|options|variants|что есть|какие есть|варианты|"
    r"מה יש|אפשרויות|"
    r"ما عندكم|خيارات)\b",
    re.IGNORECASE,
)
_FRUSTRATED_RE = re.compile(
    r"\b(not helping|why so long|бред|не понял|"
    r"לא עוזר|"
    r"لا يساعد)\b",
    re.IGNORECASE,
)
_QTY_RE = re.compile(r"\b\d+(?:[.,]\d+)?\b")
_ORDER_RE = re.compile(
    r"\b(confirm|create order|place order|оформ|заказ|заказываю|"
    r"אשר|הזמנה|"
    r"أكد|طلب)\b",
    re.IGNORECASE,
)
_ADD_TO_ORDER_RE = re.compile(
    r"\b(add to order|append|add more|добавь|добавить|еще|ещё|"
    r"הוסף|עוד|"
    r"أضف|المزيد)\b",
    re.IGNORECASE,
)
_HUMAN_RE = re.compile(
    r"\b(manager\w*|human\w*|operator\w*|менедж\w*|оператор\w*|человек\w*|נציג|מנהל|موظف|مدير)\b",
    re.IGNORECASE,
)
_PRODUCT_TOKEN_RE = re.compile(r"[^\W\d_]+(?:[-'][^\W\d_]+)*", re.UNICODE)
_COMMERCIAL_CUE_RE = commercial_cue_regex()
_GENERIC_PRODUCT_TOKENS = generic_product_tokens()


_CONTACT_DETAILS_RE = re.compile(
    r"(?is)(?:\b(?:my\s+name\s+is|name\s+is|i\s+am|i'm|tel|phone|mobile|call\s+me)\b|\b[+0]?\d[\d\s().-]{7,}\d\b)"
)
_SMALL_TALK_RE = re.compile(
    r"(?:\b(?:how are you|how're you|how are u|how is it going|how's it going|what's up|whats up|how have you been)\b|"
    r"\b(?:как дела|как ты|как поживаешь|что нового)\b|"
    r"(?:מה נשמע|מה שלומך|מה העניינים)|"
    r"(?:كيف الحال|شلونك|كيفك|شو الأخبار))",
    re.IGNORECASE,
)

# Override legacy hardcoded regexes with data-driven lexicon-backed patterns.
_SERVICE_RE = service_regex()
_PRICE_RE = price_regex()
_DIRECT_BUY_RE = direct_buy_regex()
_EXPLORE_RE = explore_regex()
_FRUSTRATED_RE = frustrated_regex()
_ORDER_RE = order_regex()
_ADD_TO_ORDER_RE = add_to_order_regex()
_HUMAN_RE = human_regex()
_CONTACT_DETAILS_RE = contact_details_regex()


def looks_like_small_talk(text: str | None) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    return bool(is_short_greeting_message(normalized) or _SMALL_TALK_RE.search(normalized))


def _substantive_message_tokens(text: str | None) -> list[str]:
    tokens = [token.casefold() for token in _PRODUCT_TOKEN_RE.findall(str(text or ""))]
    return [token for token in tokens if token not in _GENERIC_PRODUCT_TOKENS and len(token) > 1]


def _has_positive_product_evidence(text: str | None) -> bool:
    normalized = _normalize_text(str(text or ""))
    if not normalized or looks_like_small_talk(normalized):
        return False
    if any(
        regex.search(normalized)
        for regex in (
            _SERVICE_RE,
            _PRICE_RE,
            _HUMAN_RE,
            _CONTACT_DETAILS_RE,
        )
    ):
        return False
    substantive_tokens = _substantive_message_tokens(normalized)
    if not substantive_tokens:
        return False
    if _COMMERCIAL_CUE_RE.search(normalized):
        return True
    if len(substantive_tokens) >= 2:
        return True
    raw_tokens = [token.casefold() for token in _PRODUCT_TOKEN_RE.findall(normalized)]
    return len(substantive_tokens) == 1 and len(raw_tokens) == 1 and len(substantive_tokens[0]) >= 3


def _lead_profile_dict(profile: Any) -> dict[str, Any]:
    return profile if isinstance(profile, dict) else {}


def _has_value(value: Any) -> bool:
    if isinstance(value, list):
        return bool(value)
    return value not in (None, "", False)


def _has_quantity_detail(profile: dict[str, Any]) -> bool:
    return bool(profile.get("quantity")) or bool(profile.get("requested_items_have_quantities") and profile.get("requested_item_count"))


def _has_unit_detail(profile: dict[str, Any]) -> bool:
    return bool(profile.get("uom")) or bool(profile.get("requested_item_count") and not profile.get("requested_items_need_uom_confirmation"))


def _clarification_progress_made(
    *,
    previous_profile: dict[str, Any],
    current_profile: dict[str, Any],
    user_text: str,
    customer_identified: bool,
) -> bool:
    if _CONTACT_DETAILS_RE.search(user_text or ""):
        return True
    tracked_transitions = (
        ("product_interest", lambda profile: _has_value(profile.get("product_interest"))),
        ("quantity", _has_quantity_detail),
        ("uom", _has_unit_detail),
        ("urgency", lambda profile: _has_value(profile.get("urgency"))),
        ("delivery_need", lambda profile: _has_value(profile.get("delivery_need"))),
        ("target_order_id", lambda profile: _has_value(profile.get("target_order_id"))),
    )
    for _, resolver in tracked_transitions:
        if not resolver(previous_profile) and resolver(current_profile):
            return True
    if customer_identified and not previous_profile.get("customer_identified"):
        return True
    if str(previous_profile.get("next_action") or "") != str(current_profile.get("next_action") or ""):
        return True
    return False


def _derive_stage_from_state(
    *,
    session: dict[str, Any],
    intent: str,
    signal_type: str,
    customer_identified: bool,
    needs_intro: bool,
    active_order_name: str | None,
    lead_profile: dict[str, Any],
) -> tuple[str, float]:
    previous_stage = str(session.get("stage") or "")
    status = str(lead_profile.get("status") or "none")
    next_action = str(lead_profile.get("next_action") or "")
    separate_order_requested = bool(lead_profile.get("separate_order_requested"))

    if signal_type == "handoff_request" or intent == "human_handoff":
        return "handoff", 0.98
    if signal_type == "small_talk":
        if previous_stage in STAGE_PROMPTS and previous_stage not in {"", "identify"}:
            return previous_stage, 0.86
        return "new", 0.86
    if signal_type == "service_request" or status == "service" or intent == "service_request":
        return "service", 0.95
    if signal_type in {"price_objection", "discount_request", "analogs_request", "comparison_request", "delivery_question", "availability_question", "frustration", "stalling", "resume_previous_context"} and previous_stage in STAGE_PROMPTS:
        return previous_stage, 0.87
    if separate_order_requested:
        if next_action in {"ask_quantity", "ask_unit", "ask_delivery_timing", "confirm_order"} or lead_profile.get("product_interest"):
            return "order_build", 0.92
        return "discover", 0.84
    if signal_type == "topic_shift":
        return "discover", 0.91
    if status in {"order_created", "won"}:
        return ("closed", 0.93) if previous_stage == "closed" else ("invoice", 0.93)
    if status == "handoff":
        return "handoff", 0.97
    if not customer_identified and not has_product_context(lead_profile):
        if intent in {"low_signal", "service_request"} and not lead_profile.get("product_interest"):
            return "identify", 0.95
        return "lead_capture", 0.88
    if lead_profile.get("order_correction_status") == "requested":
        return "order_build", 0.88
    if active_order_name and intent == "add_to_order":
        return "order_build", 0.9
    if status == "order_ready" or next_action == "confirm_order" or intent == "confirm_order":
        return "confirm", 0.9
    if next_action in {"ask_delivery_timing", "clarify_order_correction"}:
        return "order_build", 0.86
    if next_action in {"ask_quantity", "ask_unit"}:
        return "clarify", 0.82
    if next_action in {"show_matching_options", "select_specific_item"}:
        return "discover", 0.84
    if status in {"qualified", "quote_needed"}:
        return ("order_build", 0.8) if customer_identified else ("clarify", 0.78)
    if intent == "browse_catalog":
        return "discover", 0.83
    if lead_profile.get("product_interest"):
        return "discover", 0.72
    if signal_type == "low_signal" or intent == "low_signal":
        return "clarify", 0.72
    if previous_stage in STAGE_PROMPTS:
        return previous_stage, 0.58
    return DEFAULT_STAGE, 0.5


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def _classification_config(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(ai_policy, dict):
        return {}
    return ai_policy.get("classification") if isinstance(ai_policy.get("classification"), dict) else {}


def _prompt_overrides(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(ai_policy, dict):
        return {}
    return ai_policy.get("prompt_overrides") if isinstance(ai_policy.get("prompt_overrides"), dict) else {}


def _rule_matches(*, text: str, normalized: str, rule: dict[str, Any]) -> bool:
    pattern = str(rule.get("pattern") or "").strip()
    if not pattern:
        return False
    match_type = str(rule.get("match_type") or "regex").strip().lower()
    if match_type == "contains":
        return pattern.lower() in normalized
    if match_type == "exact":
        return pattern.lower() == normalized

    flags = 0
    for flag_name in rule.get("flags") or ["IGNORECASE"]:
        flags |= _SUPPORTED_REGEX_FLAGS.get(str(flag_name).upper(), 0)
    try:
        return bool(re.search(pattern, text, flags))
    except re.error:
        return False


def _classify_with_overrides(
    *,
    text: str,
    normalized: str,
    rules: list[dict[str, Any]] | None,
) -> tuple[str, float] | None:
    if not isinstance(rules, list):
        return None
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        if _rule_matches(text=text, normalized=normalized, rule=raw_rule):
            target = str(raw_rule.get("target") or "").strip()
            if not target:
                continue
            try:
                confidence = float(raw_rule.get("confidence", 0.8))
            except (TypeError, ValueError):
                confidence = 0.8
            return target, round(max(0.0, min(1.0, confidence)), 2)
    return None


def _merged_prompt_map(base: dict[str, list[str]], overrides: dict[str, Any]) -> dict[str, list[str]]:
    merged = {key: list(value) for key, value in base.items()}
    for key, lines in overrides.items():
        if not isinstance(lines, list):
            continue
        normalized_lines = [str(item).strip() for item in lines if str(item).strip()]
        if not normalized_lines:
            continue
        merged.setdefault(key, [])
        merged[key].extend(normalized_lines)
    return merged


def classify_behavior(text: str, session: dict[str, Any], ai_policy: dict[str, Any] | None = None) -> tuple[str, float]:
    normalized = _normalize_text(text)
    if not normalized:
        return "silent_or_low_signal", 0.95
    configured = _classify_with_overrides(
        text=text,
        normalized=normalized,
        rules=_classification_config(ai_policy).get("behavior_rules"),
    )
    if configured:
        return configured
    if _FRUSTRATED_RE.search(normalized):
        return "frustrated", 0.9
    if _PRICE_RE.search(normalized):
        return "price_sensitive", 0.82
    intent, intent_confidence = classify_intent(text, ai_policy=ai_policy)
    behavior_from_intent = _behavior_from_intent(
        intent=intent,
        customer_identified=bool(session.get("erp_customer_id")),
    )
    if behavior_from_intent is not None:
        return behavior_from_intent, max(0.7, intent_confidence)
    if session.get("erp_customer_id") and len(normalized) < 20:
        return "returning_customer", 0.63
    if len(normalized) < 8:
        return "silent_or_low_signal", 0.75
    return DEFAULT_BEHAVIOR_CLASS, 0.55


def classify_intent(text: str, ai_policy: dict[str, Any] | None = None) -> tuple[str, float]:
    normalized = _normalize_text(text)
    if not normalized:
        return "low_signal", 0.95
    configured = _classify_with_overrides(
        text=text,
        normalized=normalized,
        rules=_classification_config(ai_policy).get("intent_rules"),
    )
    if configured:
        return configured
    if _HUMAN_RE.search(normalized):
        return "human_handoff", 0.92
    if _SERVICE_RE.search(normalized):
        return "service_request", 0.94
    if _ADD_TO_ORDER_RE.search(normalized):
        return "add_to_order", 0.88
    if _ORDER_RE.search(normalized):
        return "confirm_order", 0.82
    if looks_like_small_talk(normalized):
        return "small_talk", 0.92
    if _QTY_RE.search(normalized):
        return "order_detail", 0.7
    if _EXPLORE_RE.search(normalized):
        return "browse_catalog", 0.76
    if _has_positive_product_evidence(normalized):
        return "find_product", 0.68
    return "low_signal", 0.5


def classify_stage(
    *,
    session: dict[str, Any],
    intent: str,
    signal_type: str | None = None,
    customer_identified: bool,
    needs_intro: bool,
    active_order_name: str | None,
    lead_profile: dict[str, Any] | None = None,
) -> tuple[str, float]:
    return _derive_stage_from_state(
        session=session,
        intent=intent,
        signal_type=str(signal_type or intent or DEFAULT_SIGNAL_TYPE).strip() or DEFAULT_SIGNAL_TYPE,
        customer_identified=customer_identified,
        needs_intro=needs_intro,
        active_order_name=active_order_name,
        lead_profile=_lead_profile_dict(lead_profile),
    )


def _active_context_type(session: dict[str, Any]) -> str | None:
    contexts = session.get("contexts")
    if not isinstance(contexts, dict):
        return None
    active_context_id = str(session.get("active_context_id") or "").strip()
    if not active_context_id:
        return None
    active_context = contexts.get(active_context_id)
    if not isinstance(active_context, dict):
        return None
    value = str(active_context.get("context_type") or "").strip()
    return value or None


def _signal_emotion(behavior_class: str) -> str:
    if behavior_class == "frustrated":
        return "impatient"
    if behavior_class == "price_sensitive":
        return "skeptical"
    if behavior_class in {"direct_buyer", "returning_customer"}:
        return "positive"
    return "neutral"


def _behavior_from_intent(*, intent: str, customer_identified: bool) -> str | None:
    if intent == "service_request":
        return "service_request"
    if intent == "human_handoff":
        return "frustrated"
    if intent in {"confirm_order", "add_to_order", "order_detail"}:
        return "direct_buyer"
    if intent in {"browse_catalog", "find_product"}:
        return "explorer"
    if intent == "small_talk":
        return "returning_customer" if customer_identified else "silent_or_low_signal"
    if intent == "low_signal":
        return "silent_or_low_signal"
    return None


def llm_signal_soft_override_types() -> set[str]:
    return {
        "small_talk",
        "price_objection",
        "discount_request",
        "analogs_request",
        "comparison_request",
        "delivery_question",
        "availability_question",
        "topic_shift",
        "frustration",
        "confirmation",
        "service_request",
        "stalling",
        "resume_previous_context",
        "low_signal",
        "handoff_request",
    }


def fallback_intent_can_override_llm(fallback_intent: str) -> bool:
    return fallback_intent in {"find_product", "browse_catalog", "confirm_order", "add_to_order"}


def behavior_from_signal_classifier(
    *,
    signal_type: str,
    current_behavior_class: str,
    customer_identified: bool,
) -> str:
    if signal_type == "small_talk":
        return "returning_customer" if customer_identified else "silent_or_low_signal"
    if signal_type in {"price_objection", "discount_request", "analogs_request", "comparison_request"}:
        return "price_sensitive"
    if signal_type in {"service_request", "delivery_question", "availability_question"}:
        return "service_request"
    if signal_type in {"frustration", "handoff_request"}:
        return "frustrated"
    if signal_type in {"low_signal", "stalling"}:
        return "silent_or_low_signal"
    return current_behavior_class


def intent_from_signal_classifier(*, signal_type: str, current_intent: str) -> str:
    if signal_type == "small_talk":
        return "small_talk"
    if signal_type in {"service_request", "delivery_question", "availability_question"}:
        return "service_request"
    if signal_type == "handoff_request":
        return "human_handoff"
    if signal_type == "confirmation":
        return "confirm_order"
    if signal_type in {"low_signal", "frustration"}:
        return "low_signal"
    if signal_type in {"topic_shift", "deal_progress"}:
        return current_intent
    return current_intent


def _changed_product_topic(
    *,
    session: dict[str, Any],
    current_profile: dict[str, Any],
    previous_profile: dict[str, Any],
    intent: str,
    active_order_name: str | None,
) -> bool:
    if bool(current_profile.get("separate_order_requested")):
        return True
    if intent not in {"find_product", "browse_catalog"}:
        return False
    current_interest = str(
        current_profile.get("catalog_item_name")
        or current_profile.get("catalog_item_code")
        or current_profile.get("product_interest")
        or ""
    ).strip()
    previous_interest = str(
        previous_profile.get("catalog_item_name")
        or previous_profile.get("catalog_item_code")
        or previous_profile.get("product_interest")
        or ""
    ).strip()
    if not current_interest or not previous_interest:
        return False
    if current_interest.casefold() == previous_interest.casefold():
        return False
    return bool(
        active_order_name
        or str(current_profile.get("target_order_id") or previous_profile.get("target_order_id") or "").strip()
        or str(current_profile.get("order_correction_status") or "").strip() == "requested"
        or str(previous_profile.get("order_correction_status") or "").strip() == "requested"
        or _active_context_type(session) == "order_edit"
    )


def classify_signal(
    *,
    session: dict[str, Any],
    user_text: str,
    intent: str,
    behavior_class: str,
    active_order_name: str | None,
    lead_profile: dict[str, Any] | None = None,
    previous_lead_profile: dict[str, Any] | None = None,
) -> tuple[str, float, bool, str]:
    current_profile = _lead_profile_dict(lead_profile)
    previous_profile = _lead_profile_dict(previous_lead_profile)

    if intent == "human_handoff":
        return "handoff_request", 0.98, False, _signal_emotion(behavior_class)
    if intent == "small_talk":
        return "small_talk", 0.96, True, "positive"
    if behavior_class == "frustrated":
        return "frustration", 0.92, True, "impatient"
    if intent == "service_request":
        return "service_request", 0.95, True, _signal_emotion(behavior_class)
    if behavior_class == "price_sensitive" and (
        current_profile.get("product_interest")
        or current_profile.get("catalog_item_code")
        or current_profile.get("catalog_item_name")
    ):
        return "price_objection", 0.8, True, "skeptical"
    if intent == "confirm_order":
        return "confirmation", 0.9, True, _signal_emotion(behavior_class)
    if active_order_name and intent == "order_detail" and "?" in str(user_text or ""):
        return "availability_question", 0.72, True, _signal_emotion(behavior_class)
    if _changed_product_topic(
        session=session,
        current_profile=current_profile,
        previous_profile=previous_profile,
        intent=intent,
        active_order_name=active_order_name,
    ):
        return "topic_shift", 0.86, False, _signal_emotion(behavior_class)
    if intent == "low_signal":
        return "low_signal", 0.86, True, _signal_emotion(behavior_class)
    if intent in {"find_product", "browse_catalog", "order_detail", "add_to_order"}:
        return "deal_progress", 0.8, True, _signal_emotion(behavior_class)
    return DEFAULT_SIGNAL_TYPE, 0.6, True, _signal_emotion(behavior_class)


def derive_conversation_state(
    *,
    session: dict[str, Any],
    user_text: str,
    channel: str,
    needs_intro: bool,
    customer_identified: bool,
    active_order_name: str | None,
    ai_policy: dict[str, Any] | None = None,
    lead_profile: dict[str, Any] | None = None,
    previous_lead_profile: dict[str, Any] | None = None,
    behavior_class: str | None = None,
    behavior_confidence: float | None = None,
    intent: str | None = None,
    intent_confidence: float | None = None,
    signal_type: str | None = None,
    signal_confidence: float | None = None,
    signal_preserves_deal: bool | None = None,
    signal_emotion: str | None = None,
) -> dict[str, Any]:
    ai_policy = ai_policy if isinstance(ai_policy, dict) else {}
    handoff_rules = ai_policy.get("handoff_rules") if isinstance(ai_policy.get("handoff_rules"), dict) else {}
    handoff_enabled = bool(handoff_rules.get("enabled", True))
    clarification_failure_limit = int(handoff_rules.get("clarification_failure_limit", 2) or 2)
    allow_customer_requested_handoff = bool(handoff_rules.get("allow_customer_requested_handoff", True))
    frustrated_customer_handoff = bool(handoff_rules.get("frustrated_customer_handoff", True))

    resolved_behavior_class = behavior_class
    resolved_behavior_confidence = behavior_confidence
    if not resolved_behavior_class or resolved_behavior_confidence is None:
        resolved_behavior_class, resolved_behavior_confidence = classify_behavior(user_text, session, ai_policy=ai_policy)
    resolved_intent = intent
    resolved_intent_confidence = intent_confidence
    if not resolved_intent or resolved_intent_confidence is None:
        resolved_intent, resolved_intent_confidence = classify_intent(user_text, ai_policy=ai_policy)
    if resolved_behavior_class == "silent_or_low_signal" and resolved_intent == "find_product":
        resolved_intent = "low_signal"
        resolved_intent_confidence = max(float(resolved_intent_confidence), float(resolved_behavior_confidence))
    current_profile = _lead_profile_dict(lead_profile)
    previous_profile = _lead_profile_dict(previous_lead_profile)
    current_profile.setdefault("customer_identified", customer_identified)
    previous_profile.setdefault("customer_identified", bool(session.get("erp_customer_id")))
    resolved_signal_type = str(signal_type or "").strip()
    resolved_signal_emotion = str(signal_emotion or "").strip()
    resolved_signal_confidence = float(signal_confidence or 0.0)
    resolved_signal_preserves_deal = signal_preserves_deal if isinstance(signal_preserves_deal, bool) else None
    if resolved_signal_type not in SIGNAL_TYPES:
        (
            resolved_signal_type,
            resolved_signal_confidence,
            resolved_signal_preserves_deal,
            resolved_signal_emotion,
        ) = classify_signal(
            session=session,
            user_text=user_text,
            intent=resolved_intent,
            behavior_class=resolved_behavior_class,
            active_order_name=active_order_name,
            lead_profile=current_profile,
            previous_lead_profile=previous_profile,
        )
    if not resolved_signal_emotion:
        resolved_signal_emotion = _signal_emotion(resolved_behavior_class)
    if resolved_signal_preserves_deal is None:
        resolved_signal_preserves_deal = resolved_signal_type != "topic_shift"
    stage, stage_confidence = classify_stage(
        session=session,
        intent=resolved_intent,
        signal_type=resolved_signal_type,
        customer_identified=customer_identified,
        needs_intro=needs_intro,
        active_order_name=active_order_name,
        lead_profile=current_profile,
    )

    previous_stage = str(session.get("stage") or "")
    if resolved_intent == "human_handoff" and not (handoff_enabled and allow_customer_requested_handoff):
        if previous_stage in STAGE_PROMPTS:
            stage = previous_stage
        elif needs_intro or not customer_identified:
            stage = "identify"
        else:
            stage = DEFAULT_STAGE
        stage_confidence = 0.61
    previous_failures = int(session.get("failed_clarification_count") or 0)
    progress_made = _clarification_progress_made(
        previous_profile=previous_profile,
        current_profile=current_profile,
        user_text=user_text,
        customer_identified=customer_identified,
    )
    if stage == "clarify" and not progress_made:
        failed_clarification_count = previous_failures + 1 if previous_stage == "clarify" else 1
    else:
        failed_clarification_count = 0

    handoff_required = False
    handoff_reason: str | None = None
    if handoff_enabled and allow_customer_requested_handoff and resolved_intent == "human_handoff":
        handoff_required = True
        handoff_reason = "customer_requested_human"
    elif handoff_enabled and frustrated_customer_handoff and resolved_behavior_class == "frustrated":
        handoff_required = True
        handoff_reason = "frustrated_customer"
    elif handoff_enabled and failed_clarification_count >= clarification_failure_limit:
        handoff_required = True
        handoff_reason = "repeated_clarification_failure"

    if handoff_required:
        stage = "handoff"
        stage_confidence = 0.99

    return {
        "stage": stage,
        "stage_confidence": round(stage_confidence, 2),
        "behavior_class": resolved_behavior_class,
        "behavior_confidence": round(float(resolved_behavior_confidence), 2),
        "last_intent": resolved_intent,
        "last_intent_confidence": round(float(resolved_intent_confidence), 2),
        "signal_type": resolved_signal_type,
        "signal_confidence": round(float(resolved_signal_confidence), 2),
        "signal_preserves_deal": bool(resolved_signal_preserves_deal),
        "signal_emotion": resolved_signal_emotion,
        "failed_clarification_count": failed_clarification_count,
        "handoff_required": handoff_required,
        "handoff_reason": handoff_reason,
        "last_channel": channel,
    }


def build_prompt_overlay(
    *,
    stage: str | None,
    behavior_class: str | None,
    channel: str,
    handoff_required: bool = False,
    handoff_reason: str | None = None,
    ai_policy: dict[str, Any] | None = None,
) -> str:
    prompt_overrides = _prompt_overrides(ai_policy)
    stage_prompts = _merged_prompt_map(STAGE_PROMPTS, prompt_overrides.get("stage_prompts") or {})
    behavior_prompts = _merged_prompt_map(BEHAVIOR_PROMPTS, prompt_overrides.get("behavior_prompts") or {})
    channel_prompts = _merged_prompt_map(CHANNEL_PROMPTS, prompt_overrides.get("channel_prompts") or {})
    resolved_stage = stage if stage in stage_prompts else DEFAULT_STAGE
    resolved_behavior = behavior_class if behavior_class in behavior_prompts else DEFAULT_BEHAVIOR_CLASS
    channel_rules = channel_prompts.get(channel, [])

    lines = [
        "",
        "Conversation routing context:",
        f"- Current stage: {resolved_stage}",
        f"- Current behavior class: {resolved_behavior}",
        f"- Channel: {channel}",
        "- Follow the stage guidance below in addition to the main policy.",
        "",
        "Stage guidance:",
    ]
    lines.extend(f"- {rule}" for rule in stage_prompts[resolved_stage])
    lines.append("")
    lines.append("Behavior guidance:")
    lines.extend(f"- {rule}" for rule in behavior_prompts[resolved_behavior])
    if channel_rules:
        lines.append("")
        lines.append("Channel guidance:")
        lines.extend(f"- {rule}" for rule in channel_rules)
    if handoff_required:
        lines.append("")
        lines.append("Handoff policy:")
        lines.append("- A human handoff is required in this conversation.")
        if handoff_reason:
            lines.append(f"- Handoff reason: {handoff_reason}.")
        lines.append("- Do not improvise on uncertain pricing, delivery, or policy details.")
        lines.append("- Offer to connect the customer with a manager and keep the reply short.")
    return "\n".join(lines)


def get_handoff_message(lang: str, reason: str | None = None, ai_policy: dict[str, Any] | None = None) -> str:
    handoff_messages = dict(HANDOFF_MESSAGES)
    configured_messages = _prompt_overrides(ai_policy).get("handoff_messages")
    if isinstance(configured_messages, dict):
        for key, value in configured_messages.items():
            normalized_key = str(key or "").strip()
            normalized_value = str(value or "").strip()
            if normalized_key and normalized_value:
                handoff_messages[normalized_key] = normalized_value
    message = handoff_messages.get(lang, handoff_messages["en"])
    if reason == "customer_requested_human":
        return i18n_text("handoff.customer_requested_human", lang, ai_policy=ai_policy)
    return message


def advance_stage_after_tool(session: dict[str, Any], tool_name: str, tool_result: dict[str, Any]) -> None:
    if not isinstance(tool_result, dict):
        return

    if tool_name == "register_buyer" and tool_result.get("erp_customer_id"):
        session["stage"] = "discover"
        session["stage_confidence"] = 0.96
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name == "create_sales_order" and tool_result.get("name"):
        session["stage"] = "invoice"
        session["stage_confidence"] = 0.97
        session["failed_clarification_count"] = 0
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name == "update_sales_order" and tool_result.get("name"):
        session["stage"] = "invoice"
        session["stage_confidence"] = 0.93
        session["failed_clarification_count"] = 0
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name == "create_invoice":
        session["stage"] = "closed"
        session["stage_confidence"] = 0.95
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name == "send_sales_order_pdf" and tool_result.get("name"):
        session["stage"] = "service"
        session["stage_confidence"] = 0.94
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name == "get_sales_order_status" and not tool_result.get("error"):
        session["stage"] = "service"
        session["stage_confidence"] = 0.9
        session["handoff_required"] = False
        session["handoff_reason"] = None
        return

    if tool_name in {"create_license", "extend_subscription"} and not tool_result.get("error"):
        session["stage"] = "closed"
        session["stage_confidence"] = 0.9
        session["handoff_required"] = False
        session["handoff_reason"] = None
