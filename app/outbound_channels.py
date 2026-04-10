from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from app.conversation_contexts import active_lead_profile, set_active_lead_profile
from app.i18n import template as i18n_template, text as i18n_text
from app.lead_management import normalize_lead_profile, normalize_telegram_username
from app.lead_runtime_config import lead_config_from_ai_policy
from app.sales_policy import sales_policy


def _clean_text(value: Any, limit: int = 80) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _lead_config(ai_policy: dict[str, Any] | None) -> dict[str, Any]:
    return lead_config_from_ai_policy(ai_policy)


def _template_from_bucket(bucket: Any, key: str, lang: str) -> str | None:
    if not isinstance(bucket, dict):
        return None
    templates = bucket.get(key)
    if not isinstance(templates, dict):
        return None
    template = str(templates.get(lang) or templates.get("default") or "").strip()
    return template or None


def _routing_candidates(lead_config: dict[str, Any], session: dict[str, Any], *, escalation: bool = False) -> tuple[str, list[Any]]:
    if escalation:
        return "escalation", [
            lead_config.get("sales_escalation_telegram_chat_id"),
            lead_config.get("sales_escalation_telegram_username"),
        ]
    routing = lead_config.get("sales_owner_routing") if isinstance(lead_config.get("sales_owner_routing"), dict) else {}
    profile = normalize_lead_profile(active_lead_profile(session))
    lang = str(session.get("lang") or "").casefold()
    if profile.get("temperature") == "hot" and isinstance(routing.get("hot_leads"), list) and routing.get("hot_leads"):
        return "hot_leads", list(routing.get("hot_leads") or [])
    languages = routing.get("languages") if isinstance(routing.get("languages"), dict) else {}
    if lang and isinstance(languages.get(lang), list) and languages.get(lang):
        return f"language:{lang}", list(languages.get(lang) or [])
    if isinstance(routing.get("default_queue"), list) and routing.get("default_queue"):
        return "default_queue", list(routing.get("default_queue") or [])
    return "default_owner", [
        lead_config.get("sales_owner_telegram_chat_id"),
        lead_config.get("sales_owner_telegram_username"),
    ]


def _candidate_chat_id(candidate: Any) -> str:
    if isinstance(candidate, dict):
        return str(candidate.get("chat_id") or "").strip()
    text = str(candidate or "").strip()
    return text if text and not text.startswith("@") and text.lstrip("-").isdigit() else ""


def _candidate_username(candidate: Any) -> str:
    if isinstance(candidate, dict):
        return normalize_telegram_username(candidate.get("username"))
    text = str(candidate or "").strip()
    return "" if not text or text.lstrip("-").isdigit() else normalize_telegram_username(text)


async def _select_owner_target(
    *,
    lead_config: dict[str, Any],
    session: dict[str, Any],
    escalation: bool = False,
) -> dict[str, Any]:
    route_key, candidates = _routing_candidates(lead_config, session, escalation=escalation)
    clean_candidates = [candidate for candidate in candidates if candidate not in (None, "", [])]
    if not clean_candidates:
        return {"route_key": route_key}
    selected = clean_candidates[0]
    if len(clean_candidates) > 1:
        from app.session_store import next_sales_owner_route_index

        route_index = await next_sales_owner_route_index(
            company_code=str(session.get("company_code") or ""),
            route_key=route_key,
            modulo=len(clean_candidates),
        )
        selected = clean_candidates[route_index]
    return {
        "route_key": route_key,
        "chat_id": _candidate_chat_id(selected),
        "username": _candidate_username(selected),
    }


def lost_reason_buttons(lead_id: str) -> list[list[dict[str, str]]]:
    reasons = [
        ("\u0414\u043e\u0440\u043e\u0433\u043e", "price_too_high"),
        ("\u041d\u0435\u0442 \u043d\u0430\u043b\u0438\u0447\u0438\u044f", "no_stock"),
        ("\u041d\u0435 \u0442\u043e\u0442 \u0442\u043e\u0432\u0430\u0440", "wrong_product"),
        ("\u0414\u0443\u0431\u043b\u044c", "duplicate"),
        ("\u041a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442", "competitor"),
        ("\u041d\u0435\u0442 \u043e\u0442\u0432\u0435\u0442\u0430", "no_response"),
        ("\u0414\u0440\u0443\u0433\u043e\u0435", "other"),
    ]
    return [
        [{"text": label, "callback_data": f"lead_close_reason:{reason}:{lead_id}"} for label, reason in reasons[index : index + 2]]
        for index in range(0, len(reasons), 2)
    ]


def _template_for(lang: str, lead_config: dict[str, Any], profile: dict[str, Any]) -> str:
    followup_strategy = str(profile.get("followup_strategy") or "")
    next_action = str(profile.get("next_action") or "")
    status = str(profile.get("status") or "")
    specific_template = (
        _template_from_bucket(lead_config.get("followup_templates_by_strategy"), followup_strategy, lang)
        or _template_from_bucket(lead_config.get("followup_templates_by_next_action"), next_action, lang)
        or _template_from_bucket(lead_config.get("followup_templates_by_status"), status, lang)
    )
    if specific_template:
        return specific_template
    templates = lead_config.get("followup_templates")
    if isinstance(templates, dict):
        template = str(templates.get(lang) or templates.get("default") or "").strip()
        if template:
            return template
    template_key = f"followup.{followup_strategy}" if followup_strategy else f"followup.{next_action}" if next_action else "followup.default"
    return i18n_template(template_key, lang)


def _next_step_label(lang: str, next_action: str) -> str:
    key = f"next_step.{next_action}" if next_action else "next_step.default"
    return i18n_text(key, lang)


def build_followup_message(session: dict[str, Any], ai_policy: dict[str, Any] | None = None) -> str:
    profile = normalize_lead_profile(active_lead_profile(session))
    lang = str(session.get("lang") or "en")
    lead_config = _lead_config(ai_policy)
    product_interest = _clean_text(profile.get("product_interest") or profile.get("need") or "your request")
    next_step = _next_step_label(lang, str(profile.get("next_action") or ""))
    template = _template_for(lang, lead_config, profile)
    return template.format(
        product_interest=product_interest,
        next_step=next_step,
        lead_status=profile.get("status"),
        lead_score=profile.get("score"),
        lead_temperature=profile.get("temperature"),
    ).strip()


def build_sales_owner_message(session: dict[str, Any], reason: str | None = None) -> str:
    profile = normalize_lead_profile(active_lead_profile(session))
    lines = [
        "Р“РѕСЂСЏС‡РёР№ Р»РёРґ С‚СЂРµР±СѓРµС‚ РІРЅРёРјР°РЅРёСЏ.",
        f"РџСЂРёС‡РёРЅР°: {reason or 'hot_lead'}",
        f"Lead ID: {profile.get('lead_id') or '-'}",
        f"РљР°РЅР°Р»: {session.get('last_channel') or 'unknown'}",
        f"РЎС‚Р°С‚СѓСЃ Р»РёРґР°: {profile.get('status')} / {profile.get('temperature')} / score {profile.get('score')}",
        f"РЎР»РµРґСѓСЋС‰РёР№ С€Р°Рі: {profile.get('next_action')}",
    ]
    if session.get("buyer_name"):
        lines.append(f"РљР»РёРµРЅС‚: {session.get('buyer_name')}")
    if session.get("buyer_phone"):
        lines.append(f"РўРµР»РµС„РѕРЅ: {session.get('buyer_phone')}")
    if session.get("buyer_company_name"):
        lines.append(f"Компания: {session.get('buyer_company_name')}")
    if session.get("buyer_company_registry_number"):
        lines.append(f"Номер компании: {session.get('buyer_company_registry_number')}")
    if session.get("buyer_review_case_id"):
        lines.append(f"Review case: {session.get('buyer_review_case_id')}")
    if session.get("erp_customer_id"):
        lines.append(f"ERP customer: {session.get('erp_customer_id')}")
    if profile.get("source_channel") or profile.get("source_campaign") or profile.get("source_utm_campaign"):
        source_parts = [
            _clean_text(profile.get("source_channel")),
            _clean_text(profile.get("source_campaign") or profile.get("source_utm_campaign")),
            _clean_text(profile.get("source_utm_source")),
        ]
        lines.append(f"Source: {' / '.join(part for part in source_parts if part)}")
    if profile.get("product_interest"):
        lines.append(f"РРЅС‚РµСЂРµСЃ: {profile.get('product_interest')}")
    if profile.get("quantity"):
        lines.append(f"РљРѕР»РёС‡РµСЃС‚РІРѕ: {profile.get('quantity')} {profile.get('uom') or ''}".strip())
    if profile.get("urgency"):
        lines.append(f"РЎСЂРѕС‡РЅРѕСЃС‚СЊ: {profile.get('urgency')}")
    if profile.get("quote_status"):
        lines.append(f"РљРџ: {profile.get('quote_status')}")
    if profile.get("lost_reason"):
        lines.append(f"РџСЂРёС‡РёРЅР° РїРѕС‚РµСЂРё: {profile.get('lost_reason')}")
    if session.get("last_sales_order_name"):
        lines.append(f"РђРєС‚РёРІРЅС‹Р№ Р·Р°РєР°Р·: {session.get('last_sales_order_name')}")
    if profile.get("order_correction_status") not in {None, "", "none"}:
        lines.append(f"Order correction: {profile.get('order_correction_status')} / {profile.get('target_order_id') or '-'}")
    return "\n".join(str(line) for line in lines if str(line).strip())


async def notify_sales_owner(
    *,
    session: dict[str, Any],
    ai_policy: dict[str, Any] | None = None,
    reason: str | None = None,
    escalation: bool = False,
) -> dict[str, Any]:
    lead_config = _lead_config(ai_policy)
    profile = normalize_lead_profile(active_lead_profile(session))
    if profile.get("sales_owner_status") in {"accepted", "closed_not_target"}:
        return {"sent": False, "status": "sales_owner_already_resolved"}
    owner_target = await _select_owner_target(lead_config=lead_config, session=session, escalation=escalation)
    chat_id = str(owner_target.get("chat_id") or "").strip()
    owner_username = normalize_telegram_username(owner_target.get("username"))
    if not chat_id and owner_username:
        from app.session_store import resolve_sales_owner_telegram_chat

        owner_chat = await resolve_sales_owner_telegram_chat(
            company_code=str(session.get("company_code") or ""),
            username=owner_username,
        )
        chat_id = str((owner_chat or {}).get("chat_id") or "").strip()
    if not chat_id:
        status = "sales_owner_username_not_registered" if owner_username else "missing_sales_owner_telegram_chat_id"
        return {
            "sent": False,
            "status": status,
            "sales_owner_telegram_username": owner_username or None,
            "route_key": owner_target.get("route_key"),
        }
    channel_context = session.get("channel_context") if isinstance(session.get("channel_context"), dict) else {}
    bot_token = str(
        lead_config.get("sales_owner_telegram_bot_token")
        or lead_config.get("telegram_bot_token")
        or channel_context.get("telegram_bot_token")
        or ""
    ).strip()
    if not bot_token:
        return {"sent": False, "status": "missing_sales_owner_telegram_bot_token"}
    text = build_sales_owner_message(session, reason=reason)
    lead_id = str(profile.get("lead_id") or "").strip()
    reply_markup = None
    if lead_id:
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "РџСЂРёРЅСЏР»", "callback_data": f"lead_owner:accept:{lead_id}"},
                    {"text": "РџРµСЂРµРґР°С‚СЊ", "callback_data": f"lead_owner:reassign:{lead_id}"},
                ],
                [
                    {"text": "РќРµ С†РµР»РµРІРѕР№", "callback_data": f"lead_owner:close_menu:{lead_id}"},
                ],
            ]
        }
    import httpx

    async with httpx.AsyncClient(timeout=30.0) as client:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        response = await client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json=payload,
        )
        response.raise_for_status()
    return {
        "sent": True,
        "status": "delivered_to_sales_escalation_owner" if escalation else "delivered_to_sales_owner",
        "channel": "telegram",
        "text": text,
        "sales_owner_telegram_username": owner_username or None,
        "route_key": owner_target.get("route_key"),
        "escalation": escalation,
    }


async def send_followup_message(
    *,
    channel: str,
    channel_uid: str,
    session: dict[str, Any],
    ai_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lead_config = _lead_config(ai_policy)
    if not bool(lead_config.get("proactive_followup_enabled", True)):
        return {"sent": False, "status": "disabled", "channel": channel}
    resolved_sales_policy = sales_policy({"sales_policy": lead_config.get("sales_policy")} if isinstance(lead_config.get("sales_policy"), dict) else ai_policy)
    allowed_channels = resolved_sales_policy.get("proactive_followup_channels")
    if isinstance(allowed_channels, list) and allowed_channels and channel not in {str(item) for item in allowed_channels}:
        return {"sent": False, "status": "channel_blocked_by_sales_policy", "channel": channel}

    text = build_followup_message(session, ai_policy)
    channel_context = session.get("channel_context") if isinstance(session.get("channel_context"), dict) else {}
    webhook_url = str(lead_config.get("outbound_webhook_url") or "").strip()
    if webhook_url:
        import httpx

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                webhook_url,
                json={
                    "channel": channel,
                    "channel_uid": channel_uid,
                    "company_code": session.get("company_code"),
                    "text": text,
                    "lead_profile": active_lead_profile(session),
                    "channel_context": channel_context,
                },
            )
            response.raise_for_status()
        return {"sent": True, "status": "sent", "channel": channel, "via": "outbound_webhook", "text": text}

    if channel == "telegram":
        bot_token = str(channel_context.get("telegram_bot_token") or "").strip()
        if not bot_token:
            return {"sent": False, "status": "missing_telegram_bot_token", "channel": channel, "text": text}
        import httpx

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": channel_uid, "text": text},
            )
            response.raise_for_status()
        return {"sent": True, "status": "sent", "channel": channel, "via": "telegram", "text": text}

    return {
        "sent": False,
        "status": "unsupported_channel_without_outbound_webhook",
        "channel": channel,
        "text": text,
    }


def mark_followup_attempt(session: dict[str, Any], delivery: dict[str, Any]) -> None:
    profile = normalize_lead_profile(active_lead_profile(session))
    now_iso = datetime.now(UTC).isoformat()
    profile["last_followup_attempt_at"] = now_iso
    if delivery.get("sent"):
        profile["last_followup_at"] = now_iso
        profile["followup_count"] = int(profile.get("followup_count") or 0) + 1
    profile["last_followup_delivery"] = {
        "sent": bool(delivery.get("sent")),
        "status": delivery.get("status"),
        "channel": delivery.get("channel"),
        "via": delivery.get("via"),
    }
    set_active_lead_profile(session, profile)


def mark_sales_owner_notification(session: dict[str, Any], delivery: dict[str, Any]) -> None:
    profile = normalize_lead_profile(active_lead_profile(session))
    if delivery.get("escalation"):
        profile["sales_owner_escalated_at"] = datetime.now(UTC).isoformat()
        profile["sales_owner_escalation_delivery"] = {
            "sent": bool(delivery.get("sent")),
            "status": delivery.get("status"),
            "channel": delivery.get("channel"),
            "sales_owner_telegram_username": delivery.get("sales_owner_telegram_username"),
        }
        set_active_lead_profile(session, profile)
        return
    profile["sales_owner_status"] = "delivered" if delivery.get("sent") else "delivery_failed"
    profile["sales_owner_notified_at"] = datetime.now(UTC).isoformat()
    profile["sales_owner_delivery"] = {
        "sent": bool(delivery.get("sent")),
        "status": delivery.get("status"),
        "channel": delivery.get("channel"),
        "sales_owner_telegram_username": delivery.get("sales_owner_telegram_username"),
    }
    set_active_lead_profile(session, profile)

