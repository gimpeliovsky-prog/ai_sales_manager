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

ADD_TO_ORDER_RE = re.compile(
    r"\b(add to order|append|add more|добавь|добавить|добавь в заказ|еще|ещё)\b",
    re.IGNORECASE,
)


def has_explicit_confirmation(user_text: str) -> bool:
    return bool(CONFIRM_RE.search(user_text or ""))


def has_add_to_order_intent(user_text: str) -> bool:
    return bool(ADD_TO_ORDER_RE.search(user_text or ""))
