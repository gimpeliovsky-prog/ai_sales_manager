from __future__ import annotations

from string import Formatter
from typing import Any

BASE_LANGUAGE = "en"

# Specialized keys do not need a full language matrix. If a specific key is
# missing for the requested language, prefer the localized generic key before
# falling back to the English specialized copy.
FALLBACK_KEYS_BY_PREFIX: dict[str, str] = {
    "followup.": "followup.default",
    "next_step.": "next_step.default",
}

# This is a product-owned fallback catalog, not a required full matrix of
# key x language. Tenants can add any language through ai_policy.i18n, and
# missing specialized keys fall back through FALLBACK_KEYS_BY_PREFIX.
DEFAULT_TRANSLATIONS: dict[str, dict[str, str]] = {
    "intro.sales_contact": {
        "en": "Hello! I'm a sales manager. Before we continue, could you please tell me your name and phone number?",
        "ru": "Здравствуйте! Я менеджер по продажам. Прежде чем продолжить, скажите, пожалуйста, ваше имя и номер телефона.",
        "he": "שלום! אני מנהל מכירות. לפני שנמשיך, אפשר בבקשה את השם ומספר הטלפון שלך?",
        "ar": "مرحبًا! أنا مدير مبيعات. قبل أن نتابع، هل يمكنك من فضلك إرسال اسمك ورقم هاتفك؟",
    },
    "welcome.generic": {
        "en": "Hello! How can I help you today?",
        "ru": "Здравствуйте! Чем могу помочь?",
        "he": "שלום! איך אפשר לעזור לך היום?",
        "ar": "مرحبا! كيف يمكنني مساعدتك اليوم؟",
    },
    "order.already_created": {
        "en": "Your order has already been created. The current active order is {order_name}.",
        "ru": "Заказ уже создан. Текущий активный заказ: {order_name}.",
        "he": "ההזמנה כבר נוצרה. ההזמנה הפעילה כרגע היא {order_name}.",
        "ar": "تم إنشاء الطلب بالفعل. الطلب النشط الحالي هو {order_name}.",
    },
    "returning_customer.prefix": {
        "en": "Glad to help again{customer_suffix}.",
        "ru": "Рад помочь снова{customer_suffix}.",
        "he": "שמח לעזור שוב{customer_suffix}.",
        "ar": "سعيد بمساعدتك مرة أخرى{customer_suffix}.",
    },
    "handoff.customer_requested_human": {
        "en": "Okay, I'm connecting a manager. A human will continue with you from here.",
        "ru": "Хорошо, подключаю менеджера. Дальше с вами продолжит человек.",
        "he": "בסדר, אני מעביר למנהל. נציג אנושי ימשיך איתך מכאן.",
        "ar": "حسنًا، سأحوّل المحادثة إلى مدير. سيتابع معك شخص من الفريق.",
    },
    "catalog.sold_in": {
        "en": "This product is sold in: {options}.",
        "ru": "Товар продается в единицах: {options}.",
        "he": "המוצר נמכר ביחידות: {options}.",
        "ar": "يباع المنتج بوحدات: {options}.",
    },
    "followup.default": {
        "en": "Is your request about {product_interest} still relevant? If yes, please send {next_step} and I'll continue.",
        "ru": "Подскажите, пожалуйста, запрос по {product_interest} еще актуален? Если да, уточните {next_step}, и я продолжу.",
        "he": "האם הבקשה לגבי {product_interest} עדיין רלוונטית? אם כן, שלחו {next_step} ואמשיך משם.",
        "ar": "هل ما زال طلب {product_interest} مناسبًا؟ إذا نعم، أرسل {next_step} وسأتابع.",
    },
    "followup.ask_quantity": {
        "en": "Is your request about {product_interest} still relevant? Send the quantity you need and I will continue.",
    },
    "followup.catalog_browse_no_quantity": {
        "en": "I can continue with {product_interest}. Send the quantity you need and I will check the right option.",
    },
    "followup.ask_unit": {
        "en": "Is your request about {product_interest} still relevant? Send the unit or package size and I will continue.",
    },
    "followup.uom_confirmation": {
        "en": "I have {product_interest}. Please confirm the unit or package, for example boxes, pieces, or kg, and I will continue.",
    },
    "followup.ask_delivery_timing": {
        "en": "Is your request about {product_interest} still relevant? Send the delivery timing you need and I will continue.",
    },
    "followup.delivery_timing_missing": {
        "en": "I can continue with {product_interest}. Send the delivery timing you need and I will continue.",
    },
    "followup.ask_contact": {
        "en": "I can continue with {product_interest}. Please send your name and phone number so we can secure the offer.",
    },
    "followup.contact_missing": {
        "en": "I can continue with {product_interest}. Please send your name and phone number so we can secure the offer.",
    },
    "followup.confirm_order": {
        "en": "I can prepare the order for {product_interest}. Please confirm the details and I will continue.",
    },
    "followup.order_confirmation_missing": {
        "en": "I can prepare the order for {product_interest}. Please confirm the details and I will continue.",
    },
    "followup.quote_or_clarify_price": {
        "en": "I can continue with a price offer for {product_interest}. Please confirm the quantity and package you need.",
    },
    "followup.price_objection": {
        "en": "I can help adjust {product_interest} by quantity, package, or alternative item. Send the quantity and unit you prefer and I will continue.",
    },
    "followup.quote_followup": {
        "en": "I can continue with the offer for {product_interest}. If it is still relevant, send the quantity and package you want to proceed with.",
    },
    "followup.follow_up_or_handoff": {
        "en": "Is your request about {product_interest} still relevant? If yes, send {next_step} and I will continue.",
    },
    "followup.generic_stalled": {
        "en": "Is your request about {product_interest} still relevant? If yes, please send {next_step} and I'll continue.",
    },
    "next_step.ask_quantity": {
        "en": "the quantity you need",
        "ru": "нужное количество",
        "he": "את הכמות المطلوبة",
        "ar": "الكمية المطلوبة",
    },
    "next_step.ask_unit": {
        "en": "the unit or package",
        "ru": "единицу/упаковку",
        "he": "את היחידה או האריזה",
        "ar": "الوحدة أو العبوة",
    },
    "next_step.ask_delivery_timing": {
        "en": "the delivery timing you need",
    },
    "next_step.ask_contact": {
        "en": "your name and phone number",
        "ru": "имя и телефон",
        "he": "שם ומספר טלפון",
        "ar": "الاسم ورقم الهاتف",
    },
    "next_step.confirm_order": {
        "en": "your order confirmation",
        "ru": "подтверждение заказа",
        "he": "אישור הזמנה",
        "ar": "تأكيد الطلب",
    },
    "next_step.default": {
        "en": "the missing details",
        "ru": "недостающие детали",
        "he": "את הפרטים החסרים",
        "ar": "التفاصيل الناقصة",
    },
    "tool_error.buyer_not_identified": {
        "en": "Buyer is not identified yet. Register the buyer first.",
        "ru": "Покупатель не определен. Сначала зарегистрируйте покупателя.",
        "he": "הקונה עדיין לא זוהה. יש לרשום את הקונה קודם.",
        "ar": "لم يتم تحديد المشتري بعد. سجّل المشتري أولاً.",
    },
    "tool_error.order_qty_required": {
        "en": "A product quantity is required to create the order.",
        "ru": "Для создания заказа нужно указать количество товара.",
        "he": "נדרשת כמות מוצר כדי ליצור את ההזמנה.",
        "ar": "يجب تحديد كمية المنتج لإنشاء الطلب.",
    },
    "tool_error.order_confirmation_required": {
        "en": "An order can be created only after explicit customer confirmation.",
        "ru": "Заказ можно создать только после явного подтверждения клиента.",
        "he": "אפשר ליצור הזמנה רק לאחר אישור מפורש של הלקוח.",
        "ar": "لا يمكن إنشاء الطلب إلا بعد تأكيد واضح من العميل.",
    },
    "tool_error.no_active_order": {
        "en": "There is no active order yet. Create an order first.",
        "ru": "Нет активного заказа. Сначала создайте заказ.",
        "he": "אין עדיין הזמנה פעילה. יש ליצור הזמנה קודם.",
        "ar": "لا يوجد طلب نشط بعد. أنشئ طلباً أولاً.",
    },
    "tool_error.add_to_order_qty_required": {
        "en": "A product quantity is required to add an item to the order.",
        "ru": "Чтобы добавить товар в заказ, нужно указать количество.",
        "he": "נדרשת כמות מוצר כדי להוסיף פריט להזמנה.",
        "ar": "يجب تحديد كمية المنتج لإضافة عنصر إلى الطلب.",
    },
    "tool_error.add_to_order_confirmation_required": {
        "en": "To add an item to the order, the customer must explicitly ask to add it to the current order.",
        "ru": "Чтобы добавить товар в заказ, клиент должен явно попросить добавить позицию в текущий заказ.",
        "he": "כדי להוסיף פריט להזמנה, הלקוח צריך לבקש זאת במפורש.",
        "ar": "لإضافة عنصر إلى الطلب، يجب أن يطلب العميل ذلك بوضوح.",
    },
}


def normalize_lang(lang: str | None) -> str:
    text = str(lang or "").strip().casefold().replace("_", "-")
    if not text:
        return "en"
    return text.split("-", 1)[0]


def _tenant_translations(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(ai_policy, dict):
        return {}
    i18n = ai_policy.get("i18n")
    if not isinstance(i18n, dict):
        return {}
    translations = i18n.get("translations")
    return translations if isinstance(translations, dict) else {}


def _lookup_tenant_template(
    *,
    key: str,
    lang: str,
    ai_policy: dict[str, Any] | None,
) -> str | None:
    translations = _tenant_translations(ai_policy)
    for lang_key in (lang, "default", BASE_LANGUAGE):
        bucket = translations.get(lang_key)
        if isinstance(bucket, dict):
            for candidate_key in _candidate_keys(key):
                template = str(bucket.get(candidate_key) or "").strip()
                if template:
                    return template
    return None


def _lookup_default_template(*, key: str, lang: str) -> str | None:
    for candidate_lang in (lang, BASE_LANGUAGE):
        for candidate_key in _candidate_keys(key):
            bucket = DEFAULT_TRANSLATIONS.get(candidate_key)
            if isinstance(bucket, dict):
                template = bucket.get(candidate_lang)
                if template:
                    return template
    return None


def _candidate_keys(key: str) -> list[str]:
    keys = [key]
    for prefix, fallback_key in FALLBACK_KEYS_BY_PREFIX.items():
        if key.startswith(prefix) and key != fallback_key:
            keys.append(fallback_key)
            break
    return keys


def _safe_format(template: str, variables: dict[str, Any] | None) -> str:
    values = variables or {}
    safe_values = {field_name: values.get(field_name, "") for _, field_name, _, _ in Formatter().parse(template) if field_name}
    safe_values.update(values)
    try:
        return template.format(**safe_values)
    except (KeyError, ValueError):
        return template


def text(
    key: str,
    lang: str | None,
    variables: dict[str, Any] | None = None,
    *,
    ai_policy: dict[str, Any] | None = None,
) -> str:
    normalized_lang = normalize_lang(lang)
    template = (
        _lookup_tenant_template(key=key, lang=normalized_lang, ai_policy=ai_policy)
        or _lookup_default_template(key=key, lang=normalized_lang)
        or key
    )
    return _safe_format(template, variables)


def template(
    key: str,
    lang: str | None,
    *,
    ai_policy: dict[str, Any] | None = None,
) -> str:
    normalized_lang = normalize_lang(lang)
    return (
        _lookup_tenant_template(key=key, lang=normalized_lang, ai_policy=ai_policy)
        or _lookup_default_template(key=key, lang=normalized_lang)
        or key
    )
