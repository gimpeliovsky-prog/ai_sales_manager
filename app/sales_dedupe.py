from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any


_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _parse_dt(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _first_dt(*values: Any) -> datetime | None:
    for value in values:
        parsed = _parse_dt(value)
        if parsed:
            return parsed
    return None


def _phone_digits(value: Any) -> str:
    return re.sub(r"\D+", "", _text(value))


def _tokens(value: Any) -> set[str]:
    return {token.casefold() for token in _TOKEN_RE.findall(_text(value)) if len(token) > 1}


def _similarity(left: Any, right: Any) -> float:
    left_text = _text(left).casefold()
    right_text = _text(right).casefold()
    if not left_text or not right_text:
        return 0.0
    if left_text == right_text:
        return 1.0
    if len(left_text) >= 4 and left_text in right_text:
        return 0.9
    if len(right_text) >= 4 and right_text in left_text:
        return 0.9
    left_tokens = _tokens(left_text)
    right_tokens = _tokens(right_text)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _product_similarity(current: dict[str, Any], candidate: dict[str, Any]) -> float:
    current_product = _text(current.get("product_interest") or current.get("need"))
    candidate_product = _text(candidate.get("product_interest") or candidate.get("need"))
    return _similarity(current_product, candidate_product)


def _same_company(current: dict[str, Any], candidate: dict[str, Any]) -> bool:
    current_company = _text(current.get("company_code")).casefold()
    candidate_company = _text(candidate.get("company_code")).casefold()
    return not current_company or not candidate_company or current_company == candidate_company


def _within_window(candidate: dict[str, Any], *, now: datetime, window: timedelta) -> bool:
    candidate_at = _first_dt(
        candidate.get("last_interaction_at"),
        candidate.get("updated_at"),
        candidate.get("created_at"),
    )
    if not candidate_at:
        return False
    if candidate_at.tzinfo is None:
        candidate_at = candidate_at.replace(tzinfo=UTC)
    return now - candidate_at <= window


def detect_duplicate_lead(
    *,
    current: dict[str, Any],
    candidates: list[dict[str, Any]],
    now: datetime | None = None,
    window_days: int = 7,
    product_similarity_threshold: float = 0.55,
) -> dict[str, Any] | None:
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=UTC)
    window = timedelta(days=max(1, int(window_days or 7)))
    current_lead_id = _text(current.get("lead_id"))
    current_order = _text(current.get("active_order_name"))
    current_phone = _phone_digits(current.get("buyer_phone"))
    current_customer = _text(current.get("erp_customer_id")).casefold()
    current_channel = _text(current.get("channel")).casefold()
    current_uid = _text(current.get("channel_uid")).casefold()

    best: dict[str, Any] | None = None
    best_score = 0.0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_lead_id = _text(candidate.get("lead_id"))
        if not candidate_lead_id or candidate_lead_id == current_lead_id:
            continue
        if not _same_company(current, candidate):
            continue
        candidate_order = _text(candidate.get("active_order_name"))
        if current_order and candidate_order and current_order.casefold() == candidate_order.casefold():
            return {
                "duplicate_of_lead_id": candidate_lead_id,
                "dedupe_reason": "same_active_order",
                "dedupe_score": 1.0,
                "dedupe_checked_at": resolved_now.isoformat(),
            }
        if not _within_window(candidate, now=resolved_now, window=window):
            continue
        candidate_status = _text(candidate.get("status")).casefold()
        if candidate_status in {"won", "lost"} and not candidate_order:
            continue

        candidate_phone = _phone_digits(candidate.get("buyer_phone"))
        candidate_customer = _text(candidate.get("erp_customer_id")).casefold()
        candidate_channel = _text(candidate.get("channel")).casefold()
        candidate_uid = _text(candidate.get("channel_uid")).casefold()
        same_phone = bool(current_phone and candidate_phone and current_phone == candidate_phone)
        same_customer = bool(current_customer and candidate_customer and current_customer == candidate_customer)
        same_channel_uid = bool(
            current_channel and current_uid and candidate_channel and candidate_uid
            and current_channel == candidate_channel
            and current_uid == candidate_uid
        )
        if not (same_phone or same_customer or same_channel_uid):
            continue

        similarity = _product_similarity(current, candidate)
        if candidate_order and same_customer and similarity >= product_similarity_threshold:
            reason = "same_customer_existing_active_order_similar_product"
            score = max(0.85, similarity)
        elif same_customer and similarity >= product_similarity_threshold:
            reason = "same_customer_similar_product"
            score = max(0.75, similarity)
        elif same_phone and similarity >= product_similarity_threshold:
            reason = "same_phone_similar_product"
            score = max(0.7, similarity)
        elif same_channel_uid and similarity >= product_similarity_threshold:
            reason = "same_channel_similar_product"
            score = max(0.65, similarity)
        else:
            continue

        if score > best_score:
            best_score = score
            best = {
                "duplicate_of_lead_id": candidate_lead_id,
                "dedupe_reason": reason,
                "dedupe_score": round(score, 4),
                "dedupe_checked_at": resolved_now.isoformat(),
            }
    return best
