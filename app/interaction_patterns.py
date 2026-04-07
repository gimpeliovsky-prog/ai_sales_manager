from __future__ import annotations

import re

CONFIRM_RE = re.compile(
    r"\b(confirm|confirmed|place order|place the order|create order|create the order|buy now|order it|"
    r"yes|ok|okay|sure|go ahead|"
    r"оформляй|оформить|подтверждаю|заказываю|подтверждено|да|ок|хорошо|"
    r"כן|אישור|תאשר|"
    r"نعم|أكد|موافق)\b",
    re.IGNORECASE,
)

NEGATIVE_CONFIRM_RE = re.compile(
    r"(?:\b(no|not now|do not|don't|cancel|stop)\b|"
    r"\u043d\u0435\s+(?:\u043d\u0430\u0434\u043e|\u0437\u0430\u043a\u0430\u0437\u044b\u0432\u0430\u0439|\u043e\u0444\u043e\u0440\u043c\u043b\u044f\u0439)|"
    r"\u05dc\u05d0|"
    r"\u0644\u0627)",
    re.IGNORECASE,
)

CONVERSATIONAL_CONFIRM_RE = re.compile(
    r"(?:\b(?:let'?s do it|do it|sounds good|looks good|that works|proceed|send it|book it)\b|"
    r"\b(?:ok|okay|yes|sure)\b.{0,24}\b(?:go ahead|do it|proceed|send it|book it)\b|"
    r"\u043e\u043a(?:\u0435\u0439)?(?:\s*,?\s*)?\u0434\u0430\u0432\u0430\u0439|"
    r"\u043e\u043a(?:\u0435\u0439)?(?:\s*,?\s*)?\u043e\u0444\u043e\u0440\u043c\u043b\u044f\u0439|"
    r"\u043d\u0443\s+\u043b\u0430\u0434\u043d\u043e|"
    r"\u043b\u0430\u0434\u043d\u043e(?:\s*,?\s*)?\u0434\u0430\u0432\u0430\u0439|"
    r"\u0434\u0430\u0432\u0430\u0439(?:\s*,?\s*)?(?:\u043e\u0444\u043e\u0440\u043c\u043b\u044f\u0439|\u0437\u0430\u043a\u0430\u0437\u044b\u0432\u0430\u0439|\u0434\u0435\u043b\u0430\u0439)|"
    r"\u043c\u043e\u0436\u043d\u043e\s+\u043e\u0444\u043e\u0440\u043c\u043b\u044f\u0442\u044c|"
    r"\u0434\u0430(?:\s*,?\s*)?\u043e\u0444\u043e\u0440\u043c\u043b\u044f\u0439|"
    r"\u0431\u0435\u0440\u0443|"
    r"\u05d9\u05d0\u05dc\u05dc\u05d4|"
    r"\u05e1\u05d1\u05d1\u05d4|"
    r"\u05d1\u05e1\u05d3\u05e8|"
    r"\u05d0\u05d5\u05e7\u05d9|"
    r"\u05d0\u05d5\u05e7\u05d9\u05d9|"
    r"\u05d8\u05d5\u05d1|"
    r"\u05de\u05e2\u05d5\u05dc\u05d4|"
    r"\u05e1\u05d2\u05d5\u05e8|"
    r"\u05e1\u05d2\u05e8\u05e0\u05d5|"
    r"\u05de\u05d0\u05e9\u05e8|"
    r"\u062a\u0645\u0627\u0645|"
    r"\u0645\u0648\u0627\u0641\u0642|"
    r"\u064a\u0644\u0627)",
    re.IGNORECASE,
)

ADD_TO_ORDER_RE = re.compile(
    r"\b(add to order|append|add more|добавь|добавить|добавь в заказ|еще|ещё)\b",
    re.IGNORECASE,
)


def has_explicit_confirmation(user_text: str) -> bool:
    text = user_text or ""
    if NEGATIVE_CONFIRM_RE.search(text):
        return False
    return bool(CONFIRM_RE.search(text) or CONVERSATIONAL_CONFIRM_RE.search(text))


def has_add_to_order_intent(user_text: str) -> bool:
    return bool(ADD_TO_ORDER_RE.search(user_text or ""))
