from __future__ import annotations

import re
from typing import Any

DEFAULT_STAGE = "discover"
DEFAULT_BEHAVIOR_CLASS = "unclear_request"
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
    "discover": [
        "Your goal is to understand which product or service the customer wants.",
        "Offer at most two or three relevant options when the request is broad.",
    ],
    "clarify": [
        "Your goal is to clarify missing product, quantity, variant, or UOM details.",
        "Ask one focused clarification question at a time.",
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
    if _SERVICE_RE.search(normalized):
        return "service_request", 0.94
    if _FRUSTRATED_RE.search(normalized):
        return "frustrated", 0.9
    if _PRICE_RE.search(normalized):
        return "price_sensitive", 0.82
    if _DIRECT_BUY_RE.search(normalized):
        return "direct_buyer", 0.84
    if _EXPLORE_RE.search(normalized):
        return "explorer", 0.8
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
    if _QTY_RE.search(normalized):
        return "order_detail", 0.7
    if _EXPLORE_RE.search(normalized):
        return "browse_catalog", 0.76
    return "find_product", 0.52


def classify_stage(
    *,
    session: dict[str, Any],
    intent: str,
    customer_identified: bool,
    needs_intro: bool,
    active_order_name: str | None,
) -> tuple[str, float]:
    if intent == "human_handoff":
        return "handoff", 0.98
    if needs_intro or not customer_identified:
        return "identify", 0.97
    if intent == "service_request":
        return "service", 0.95
    if active_order_name and intent == "add_to_order":
        return "order_build", 0.9
    if intent == "confirm_order":
        return "confirm", 0.82
    if intent == "order_detail":
        return "clarify", 0.7
    if intent == "browse_catalog":
        return "discover", 0.83
    if intent == "low_signal":
        return "clarify", 0.72
    previous_stage = str(session.get("stage") or "")
    if previous_stage in STAGE_PROMPTS:
        return previous_stage, 0.58
    return DEFAULT_STAGE, 0.5


def derive_conversation_state(
    *,
    session: dict[str, Any],
    user_text: str,
    channel: str,
    needs_intro: bool,
    customer_identified: bool,
    active_order_name: str | None,
    ai_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ai_policy = ai_policy if isinstance(ai_policy, dict) else {}
    handoff_rules = ai_policy.get("handoff_rules") if isinstance(ai_policy.get("handoff_rules"), dict) else {}
    handoff_enabled = bool(handoff_rules.get("enabled", True))
    clarification_failure_limit = int(handoff_rules.get("clarification_failure_limit", 2) or 2)
    allow_customer_requested_handoff = bool(handoff_rules.get("allow_customer_requested_handoff", True))
    frustrated_customer_handoff = bool(handoff_rules.get("frustrated_customer_handoff", True))

    behavior_class, behavior_confidence = classify_behavior(user_text, session, ai_policy=ai_policy)
    intent, intent_confidence = classify_intent(user_text, ai_policy=ai_policy)
    stage, stage_confidence = classify_stage(
        session=session,
        intent=intent,
        customer_identified=customer_identified,
        needs_intro=needs_intro,
        active_order_name=active_order_name,
    )

    previous_stage = str(session.get("stage") or "")
    if intent == "human_handoff" and not (handoff_enabled and allow_customer_requested_handoff):
        if previous_stage in STAGE_PROMPTS:
            stage = previous_stage
        elif needs_intro or not customer_identified:
            stage = "identify"
        else:
            stage = DEFAULT_STAGE
        stage_confidence = 0.61
    previous_failures = int(session.get("failed_clarification_count") or 0)
    if stage == "clarify":
        failed_clarification_count = previous_failures + 1 if previous_stage == "clarify" else 1
    else:
        failed_clarification_count = 0

    handoff_required = False
    handoff_reason: str | None = None
    if handoff_enabled and allow_customer_requested_handoff and intent == "human_handoff":
        handoff_required = True
        handoff_reason = "customer_requested_human"
    elif handoff_enabled and frustrated_customer_handoff and behavior_class == "frustrated":
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
        "behavior_class": behavior_class,
        "behavior_confidence": round(behavior_confidence, 2),
        "last_intent": intent,
        "last_intent_confidence": round(intent_confidence, 2),
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
        if lang == "ru":
            return "Хорошо, подключаю менеджера. Дальше с вами продолжит человек."
        if lang == "he":
            return "בסדר, אני מעביר למנהל. נציג אנושי ימשיך איתך מכאן."
        if lang == "ar":
            return "حسنًا، سأحوّل المحادثة إلى مدير. سيتابع معك شخص من الفريق."
        return "Okay, I'm connecting a manager. A human will continue with you from here."
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

    if tool_name in {"create_license", "extend_subscription"} and not tool_result.get("error"):
        session["stage"] = "closed"
        session["stage_confidence"] = 0.9
        session["handoff_required"] = False
        session["handoff_reason"] = None
