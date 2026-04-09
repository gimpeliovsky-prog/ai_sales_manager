from __future__ import annotations

import re

from app.i18n import text as i18n_text

_COMPANY_PREFIX_RE = re.compile(
    r"(?is)^\s*(?:"
    r"i\s+work\s+(?:at|for)|i(?:\s*am|'m)\s+from|from|company|my\s+company\s+is|"
    r"я\s+работаю\s+в|я\s+из|я\s+из\s+компании|компания|из\s+компании|"
    r"אני\s+עובד(?:ת)?\s+ב|אני\s+מ|אני\s+מחברת|מחברת|מהחברה|שם\s+החברה(?:\s+שלי)?|"
    r"أعمل\s+في|أنا\s+من|أنا\s+من\s+شركة|الشركة|شركة"
    r")[\s,:-]*"
)
_GENERIC_COMPANY_VALUES = {
    "company",
    "my company",
    "компания",
    "из компании",
    "חברה",
    "החברה",
    "شركة",
    "الشركة",
}
_KNOWN_BUYER_GREETING = {
    "en": "Hello, {buyer_name}. How can I help?",
    "ru": "Здравствуйте, {buyer_name}. Чем могу помочь?",
    "he": "שלום, {buyer_name}. איך אפשר לעזור?",
    "ar": "مرحبًا، {buyer_name}. كيف أستطيع المساعدة؟",
}
_BUYER_COMPANY_REQUEST = {
    "en": "Thanks, {buyer_name}. I couldn't match your phone to an existing customer yet. Which company do you work for?",
    "ru": "Спасибо, {buyer_name}. Я пока не нашёл клиента по вашему номеру. В какой компании вы работаете?",
    "he": "תודה, {buyer_name}. עדיין לא הצלחתי לזהות לקוח קיים לפי מספר הטלפון שלך. באיזו חברה אתה עובד?",
    "ar": "شكرًا، {buyer_name}. لم أتمكن بعد من לזהות العميل الحالي לפי رقم الهاتف. في أي شركة تعمل؟",
}
_BUYER_IDENTITY_REVIEW = {
    "en": "Thanks. I saved your details and sent them to a manager so they can link your contact to the correct company in ERP before we continue.",
    "ru": "Спасибо. Я сохранил ваши данные и передал их менеджеру, чтобы он связал ваш контакт с нужной компанией в ERP перед продолжением.",
    "he": "תודה. שמרתי את הפרטים שלך והעברתי אותם למנהל כדי שיקשר את איש הקשר שלך לחברה הנכונה ב-ERP לפני שנמשיך.",
    "ar": "شكرًا. حفظت تفاصيلك وأرسلتها إلى المدير ليربط جهة الاتصال الخاصة بك بالشركة الصحيحة في ERP قبل أن نتابع.",
}


def clean_company_candidate(text: str) -> str | None:
    candidate = re.sub(r"\s+", " ", str(text or "")).strip(" ,.;:-")
    if not candidate:
        return None
    candidate = _COMPANY_PREFIX_RE.sub("", candidate).strip(" ,.;:-")
    if not candidate:
        return None
    normalized = candidate.casefold()
    if normalized in _GENERIC_COMPANY_VALUES:
        return None
    if len(candidate) > 120:
        candidate = candidate[:120].rstrip(" ,.;:-")
    return candidate or None


def get_known_buyer_greeting(lang: str, buyer_name: str | None = None) -> str:
    display_name = str(buyer_name or "").strip()
    if not display_name:
        return i18n_text("welcome.generic", lang)
    template = _KNOWN_BUYER_GREETING.get(lang, _KNOWN_BUYER_GREETING["en"])
    return template.format(buyer_name=display_name)


def buyer_company_request_message(lang: str, buyer_name: str | None = None) -> str:
    template = _BUYER_COMPANY_REQUEST.get(lang, _BUYER_COMPANY_REQUEST["en"])
    display_name = str(buyer_name or "").strip() or "there"
    return template.format(buyer_name=display_name)


def buyer_identity_review_message(lang: str) -> str:
    return _BUYER_IDENTITY_REVIEW.get(lang, _BUYER_IDENTITY_REVIEW["en"])
