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
_INTRO_SALES_CONTACT = {
    "en": "Hello. To get started, please send your name and phone number.",
    "ru": "Здравствуйте. Чтобы начать, отправьте, пожалуйста, ваше имя и номер телефона.",
    "he": "שלום. כדי להתחיל, שלח לי בבקשה את השם ומספר הטלפון שלך.",
    "ar": "مرحبًا. للبدء، أرسل لي من فضلك اسمك ورقم هاتفك.",
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
_BUYER_COMPANY_RETRY = {
    "en": "I couldn't find this company in the official registry. Please send the full official company name or the company number.",
    "ru": "Я не нашёл такую компанию в официальном реестре. Пришлите, пожалуйста, полное официальное название компании или номер компании.",
    "he": "לא מצאתי את החברה הזאת במרשם הרשמי. שלח בבקשה את השם הרשמי המלא של החברה או את מספר החברה / ח.פ.",
    "ar": "لم أجد هذه الشركة في السجل الرسمي. أرسل من فضلك الاسم الرسمي الكامل للشركة أو رقم الشركة.",
}
_BUYER_COMPANY_AMBIGUOUS = {
    "en": "I found several companies. Reply with the exact company number or copy the official company name:\n{options}",
    "ru": "Я нашёл несколько компаний. Ответьте точным номером компании или скопируйте официальное название:\n{options}",
    "he": "מצאתי כמה חברות. שלח את מספר החברה המדויק או העתק את השם הרשמי:\n{options}",
    "ar": "وجدت عدة شركات. أرسل رقم الشركة الدقيق أو انسخ الاسم الرسمي:\n{options}",
}


_BUYER_COMPANY_LOOKUP_ERROR = {
    "en": "I couldn't verify the company right now because of a temporary error. Please send the official company name or number again in a moment.",
    "ru": "Сейчас не получилось проверить компанию из-за временной ошибки. Через минуту отправьте ещё раз официальное название компании или её номер.",
    "he": "לא הצלחתי לאמת עכשיו את החברה בגלל שגיאה זמנית. שלח שוב בעוד רגע את השם הרשמי של החברה או את מספר החברה.",
    "ar": "لم أتمكن من التحقق من الشركة الآن بسبب خطأ مؤقت. أرسل بعد قليل الاسم الرسمي للشركة أو رقمها مرة أخرى.",
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


def get_intro_sales_contact_message(lang: str) -> str:
    return _INTRO_SALES_CONTACT.get(lang, _INTRO_SALES_CONTACT["en"])


def buyer_company_request_message(lang: str, buyer_name: str | None = None) -> str:
    display_name = str(buyer_name or "").strip() or "there"
    if lang == "he":
        return f"תודה, {display_name}. עדיין לא הצלחתי לזהות לקוח קיים לפי מספר הטלפון שלך. שלח בבקשה את השם הרשמי של החברה או את מספר החברה / ח.פ."
    if lang == "ru":
        return (
            f"Спасибо, {display_name}. Я пока не нашёл клиента по вашему номеру. "
            "Пришлите, пожалуйста, официальное название компании или номер компании."
        )
    if lang == "ar":
        return (
            f"شكرًا، {display_name}. لم أتمكن بعد من تحديد العميل الحالي من رقم الهاتف. "
            "أرسل من فضلك الاسم الرسمي للشركة أو رقم الشركة."
        )
    template = _BUYER_COMPANY_REQUEST.get(lang, _BUYER_COMPANY_REQUEST["en"])
    return template.format(buyer_name=display_name)


def buyer_identity_review_message(lang: str) -> str:
    return _BUYER_IDENTITY_REVIEW.get(lang, _BUYER_IDENTITY_REVIEW["en"])


def buyer_company_retry_message(lang: str) -> str:
    return _BUYER_COMPANY_RETRY.get(lang, _BUYER_COMPANY_RETRY["en"])


def buyer_company_ambiguous_message(lang: str, options: list[str]) -> str:
    template = _BUYER_COMPANY_AMBIGUOUS.get(lang, _BUYER_COMPANY_AMBIGUOUS["en"])
    rendered = "\n".join(str(option or "").strip() for option in options if str(option or "").strip())
    return template.format(options=rendered)


def buyer_company_lookup_error_message(lang: str) -> str:
    return _BUYER_COMPANY_LOOKUP_ERROR.get(lang, _BUYER_COMPANY_LOOKUP_ERROR["en"])
