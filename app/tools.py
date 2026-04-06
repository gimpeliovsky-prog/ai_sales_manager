import json
from datetime import date
import re
from typing import Any

from app.interaction_patterns import has_add_to_order_intent, has_explicit_confirmation
from app.license_client import LicenseClient

TOOLS: list[dict] = [
    {
        "type": "function",
        "name": "get_product_catalog",
        "description": "Получить каталог товаров и услуг по группе или по названию товара вместе с базовой единицей stock_uom и доступными коммерческими UOM, например коробками.",
        "parameters": {
            "type": "object",
            "properties": {
                "item_group": {"type": "string"},
                "item_name": {"type": "string"},
            },
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_sales_order",
        "description": "Оформить заказ после подтверждения состава. Если клиент просит коробки или другую не stock UOM, передай нужную UOM и conversion_factor из каталога. Если дата доставки не указана, используй ближайшую возможную дату автоматически.",
        "parameters": {
            "type": "object",
            "properties": {
                "delivery_date": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string"},
                            "qty": {"type": "number"},
                            "rate": {"type": "number"},
                            "uom": {"type": "string"},
                            "conversion_factor": {"type": "number"},
                        },
                        "required": ["item_code", "qty"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_invoice",
        "description": "Выставить счет по номеру заказа.",
        "parameters": {
            "type": "object",
            "properties": {"sales_order_name": {"type": "string"}},
            "required": ["sales_order_name"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "update_sales_order",
        "description": "Добавить товар в уже созданный draft Sales Order. Если номер заказа не указан, используй текущий активный заказ из переписки.",
        "parameters": {
            "type": "object",
            "properties": {
                "sales_order_name": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string"},
                            "qty": {"type": "number"},
                            "rate": {"type": "number"},
                            "uom": {"type": "string"},
                            "conversion_factor": {"type": "number"},
                        },
                        "required": ["item_code", "qty"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "send_sales_order_pdf",
        "description": "Повторно отправить клиенту PDF текущего заказа. Используй это, когда клиент просит отправить заказ, order PDF, order file или current order. Не используй для создания счета.",
        "parameters": {
            "type": "object",
            "properties": {
                "sales_order_name": {"type": "string"},
            },
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "register_buyer",
        "description": "Зарегистрировать нового покупателя после получения имени и телефона.",
        "parameters": {
            "type": "object",
            "properties": {"full_name": {"type": "string"}, "phone": {"type": "string"}},
            "required": ["full_name"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_buyer_sales_history",
        "description": "Получить недавнюю историю заказов и счетов покупателя, чтобы использовать ее как контекст для повторного клиента.",
        "parameters": {
            "type": "object",
            "properties": {"erp_customer_id": {"type": "string"}},
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_license",
        "description": "Создать лицензионный ключ.",
        "parameters": {
            "type": "object",
            "properties": {"description": {"type": "string"}},
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "extend_subscription",
        "description": "Продлить подписку.",
        "parameters": {
            "type": "object",
            "properties": {"add_days": {"type": "integer"}},
            "required": ["add_days"],
            "additionalProperties": False,
        },
    },
]

_PRODUCT_ALIASES: dict[str, list[str]] = {
    "ноутбук": ["laptop"],
    "ноутбуки": ["laptop"],
    "лэптоп": ["laptop"],
    "лэптопы": ["laptop"],
    "компьютер": ["computer", "laptop"],
    "телевизор": ["television", "tv"],
    "телевизоры": ["television", "tv"],
    "рюкзак": ["backpack"],
    "рюкзаки": ["backpack"],
    "книга": ["book"],
    "книги": ["book"],
    "смартфон": ["smartphone", "phone"],
    "смартфоны": ["smartphone", "phone"],
    "телефон": ["smartphone", "phone"],
    "телефоны": ["smartphone", "phone"],
    "кружка": ["coffee mug", "mug"],
    "кружки": ["coffee mug", "mug"],
    "наушники": ["headphones"],
    "камера": ["camera"],
    "футболка": ["t-shirt", "shirt"],
    "футболки": ["t-shirt", "shirt"],
    "кроссовки": ["sneakers"],
    "מחשב": ["computer", "laptop"],
    "מחשבים": ["computer", "laptop"],
    "מחשב נייד": ["laptop"],
    "מחשבים ניידים": ["laptop"],
    "לפטופ": ["laptop"],
    "לפטופים": ["laptop"],
    "טלוויזיה": ["television", "tv"],
    "טלוויזיות": ["television", "tv"],
    "תרמיל": ["backpack"],
    "תרמילים": ["backpack"],
    "ספר": ["book"],
    "ספרים": ["book"],
    "סמארטפון": ["smartphone", "phone"],
    "סמארטפונים": ["smartphone", "phone"],
    "טלפון": ["smartphone", "phone"],
    "טלפונים": ["smartphone", "phone"],
    "ספל": ["coffee mug", "mug"],
    "ספלים": ["coffee mug", "mug"],
    "אוזניות": ["headphones"],
    "מצלמה": ["camera"],
    "מצלמות": ["camera"],
    "חולצה": ["t-shirt", "shirt"],
    "חולצות": ["t-shirt", "shirt"],
    "נעליים": ["sneakers"],
    "לابتوب": ["laptop"],
    "لابتوبات": ["laptop"],
    "حاسوب": ["computer", "laptop"],
    "حاسوب محمول": ["laptop"],
    "كمبيوتر": ["computer", "laptop"],
    "تلفزيون": ["television", "tv"],
    "تلفزيونات": ["television", "tv"],
    "حقيبة": ["backpack"],
    "حقائب": ["backpack"],
    "كتاب": ["book"],
    "كتب": ["book"],
    "هاتف": ["smartphone", "phone"],
    "هواتف": ["smartphone", "phone"],
    "هاتف ذكي": ["smartphone", "phone"],
    "أكواب": ["coffee mug", "mug"],
    "كوب": ["coffee mug", "mug"],
    "سماعات": ["headphones"],
    "كاميرا": ["camera"],
    "كاميرات": ["camera"],
    "قميص": ["t-shirt", "shirt"],
    "قمصان": ["t-shirt", "shirt"],
    "حذاء": ["sneakers"],
    "أحذية": ["sneakers"],
}


def _has_explicit_confirmation(user_text: str) -> bool:
    return has_explicit_confirmation(user_text)


def _has_add_to_order_intent(user_text: str) -> bool:
    return has_add_to_order_intent(user_text)


def _items_have_qty(items: list[dict[str, Any]] | None) -> bool:
    if not isinstance(items, list) or not items:
        return False
    for item in items:
        qty = item.get("qty") if isinstance(item, dict) else None
        if isinstance(qty, (int, float)) and qty > 0:
            return True
    return False


def _normalize_match_text(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9а-яА-ЯёЁ\u0590-\u05FF\u0600-\u06FF]+", " ", text or "").strip().lower()


def _search_aliases(text: str | None) -> list[str]:
    normalized = _normalize_match_text(text or "")
    if not normalized:
        return []
    return [candidate for candidate in normalized.split() if len(candidate) >= 3]
    aliases: list[str] = []
    for token in normalized.split():
        for alias in _PRODUCT_ALIASES.get(token, []):
            if alias not in aliases:
                aliases.append(alias)
    if normalized in _PRODUCT_ALIASES:
        for alias in _PRODUCT_ALIASES[normalized]:
            if alias not in aliases:
                aliases.append(alias)
    return aliases


def _translate_uom_label(label: str | None, lang: str) -> str | None:
    if not label:
        return label
    normalized = str(label).strip().lower()
    translations = {
        "ru": {
            "штуки": "штуки",
            "коробки": "коробки",
            "упаковки": "упаковки",
            "килограммы": "килограммы",
            "граммы": "граммы",
            "литры": "литры",
            "метры": "метры",
        },
        "en": {
            "штуки": "pieces",
            "коробки": "boxes",
            "упаковки": "packs",
            "килограммы": "kilograms",
            "граммы": "grams",
            "литры": "liters",
            "метры": "meters",
        },
        "he": {
            "штуки": "יחידות",
            "коробки": "קרטון",
            "упаковки": "חבילות",
            "килограммы": "קילוגרמים",
            "граммы": "גרמים",
            "литры": "ליטרים",
            "метры": "מטרים",
        },
        "ar": {
            "штуки": "قطع",
            "коробки": "صناديق",
            "упаковки": "عبوات",
            "килограммы": "كيلوغرامات",
            "граммы": "غرامات",
            "литры": "لترات",
            "метры": "أمتار",
        },
    }
    return translations.get(lang, translations["en"]).get(normalized, label)


def _localize_catalog_result(result: dict[str, Any], lang: str) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result
    items = result.get("items")
    if not isinstance(items, list):
        return result

    if lang == "ru":
        sold_in_prefix = "Товар продается в единицах: "
    elif lang == "he":
        sold_in_prefix = "המוצר נמכר ביחידות: "
    elif lang == "ar":
        sold_in_prefix = "يباع المنتج بوحدات: "
    else:
        sold_in_prefix = "This product is sold in: "

    localized_items: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            localized_items.append(item)
            continue
        localized = dict(item)
        if localized.get("display_item_name"):
            localized["item_name"] = localized.get("display_item_name")
        if localized.get("stock_uom_label"):
            localized["stock_uom_label"] = _translate_uom_label(localized.get("stock_uom_label"), lang)
        if localized.get("sales_uom_label"):
            localized["sales_uom_label"] = _translate_uom_label(localized.get("sales_uom_label"), lang)
        available_uoms = localized.get("available_uoms")
        if isinstance(available_uoms, list):
            updated_uoms: list[dict[str, Any]] = []
            for uom in available_uoms:
                if not isinstance(uom, dict):
                    updated_uoms.append(uom)
                    continue
                updated = dict(uom)
                updated["display_name"] = _translate_uom_label(updated.get("display_name") or updated.get("uom"), lang)
                updated_uoms.append(updated)
            localized["available_uoms"] = updated_uoms
            localized["non_stock_uoms"] = [uom for uom in updated_uoms if isinstance(uom, dict) and not uom.get("is_stock_uom")]
        labels = []
        if localized.get("stock_uom_label"):
            labels.append(str(localized["stock_uom_label"]))
        for uom in localized.get("non_stock_uoms", []):
            if isinstance(uom, dict):
                label = str(uom.get("display_name") or uom.get("uom") or "").strip()
                if label and label not in labels:
                    labels.append(label)
        if labels:
            localized["customer_uom_summary"] = sold_in_prefix + ", ".join(labels) + "."
        localized_items.append(localized)

    localized_result = dict(result)
    localized_result["items"] = localized_items
    return localized_result


async def execute_tool(
    name: str,
    inputs: dict[str, Any],
    company_code: str,
    erp_customer_id: str | None,
    active_sales_order_name: str | None,
    current_lang: str,
    user_text: str,
    channel: str,
    channel_uid: str,
    lc: LicenseClient,
) -> str:
    try:
        result = await _dispatch(
            name,
            inputs,
            company_code,
            erp_customer_id,
            active_sales_order_name,
            current_lang,
            user_text,
            channel,
            channel_uid,
            lc,
        )
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


async def _dispatch(name, inp, company_code, erp_customer_id, active_sales_order_name, current_lang, user_text, channel, channel_uid, lc):
    from app.buyer_resolver import create_buyer_from_intro

    if name == "get_product_catalog":
        item_group = inp.get("item_group")
        item_name = inp.get("item_name")
        try:
            result = await lc.get_items(company_code, item_group, item_name, current_lang)
        except Exception:
            result = {"items": []}
        if not result.get("items"):
            candidates: list[str] = []
            for raw_text in (item_name, item_group):
                normalized = _normalize_match_text(raw_text or "")
                if not normalized:
                    continue
                if normalized not in candidates:
                    candidates.append(normalized)
                for token in normalized.split():
                    if len(token) >= 3 and token not in candidates:
                        candidates.append(token)
            for candidate in candidates[:6]:
                try:
                    result = await lc.get_items(company_code, None, candidate, current_lang)
                except Exception:
                    result = {"items": []}
                if result.get("items"):
                    break
        return _localize_catalog_result(result, current_lang)
    if name == "create_sales_order":
        if not erp_customer_id:
            return {"error": "Покупатель не определён. Сначала зарегистрируйте покупателя."}
        if not _items_have_qty(inp.get("items")):
            return {"error": "Для создания заказа нужно указать количество товара."}
        if not _has_explicit_confirmation(user_text):
            return {"error": "Заказ можно создать только после явного подтверждения клиента."}
        delivery_date = inp.get("delivery_date") or date.today().isoformat()
        return await lc.create_sales_order(company_code, erp_customer_id, delivery_date, inp["items"])
    if name == "create_invoice":
        return await lc.create_invoice(company_code, inp["sales_order_name"])
    if name == "update_sales_order":
        sales_order_name = inp.get("sales_order_name") or active_sales_order_name
        if not sales_order_name:
            return {"error": "Нет активного заказа. Сначала создайте заказ."}
        if not _items_have_qty(inp.get("items")):
            return {"error": "Чтобы добавить товар в заказ, нужно указать количество."}
        if not _has_add_to_order_intent(user_text) and not _has_explicit_confirmation(user_text):
            return {"error": "Чтобы добавить товар в заказ, клиент должен явно попросить добавить позицию в текущий заказ."}
        return await lc.update_sales_order_items(company_code, sales_order_name, inp["items"])
    if name == "send_sales_order_pdf":
        sales_order_name = inp.get("sales_order_name") or active_sales_order_name
        if not sales_order_name:
            return {"error": "Нет активного заказа. Сначала создайте заказ."}
        return await lc.get_sales_order(company_code, sales_order_name)
    if name == "register_buyer":
        buyer_result = await create_buyer_from_intro(
            session={},
            company_code=company_code,
            channel=channel,
            channel_uid=channel_uid,
            full_name=inp["full_name"],
            phone=inp.get("phone"),
            lc=lc,
        )
        customer_id = buyer_result.get("erp_customer_id") if isinstance(buyer_result, dict) else None
        return {
            "erp_customer_id": customer_id,
            "registered": customer_id is not None,
            "buyer_identity_id": buyer_result.get("buyer_identity_id") if isinstance(buyer_result, dict) else None,
            "recent_sales_orders": buyer_result.get("recent_sales_orders") if isinstance(buyer_result, dict) else [],
        }
    if name == "get_buyer_sales_history":
        target_customer_id = erp_customer_id or inp.get("erp_customer_id")
        if not target_customer_id:
            return {"error": "Buyer is not identified yet."}
        return await lc.get_buyer_sales_history(company_code, target_customer_id)
    if name == "create_license":
        return await lc.create_license(company_code, inp.get("description"))
    if name == "extend_subscription":
        return await lc.extend_subscription(company_code, inp["add_days"])
    return {"error": f"Unknown tool: {name}"}
