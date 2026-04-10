from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import get_settings
from app.conversation_contexts import active_lead_profile, set_active_lead_profile
from app.lead_management import (
    build_lead_event_payload,
    can_send_followup,
    mark_lost_if_followup_exhausted,
    mark_stalled_if_needed,
    normalize_lead_profile,
)
from app.lead_runtime_config import lead_config_from_ai_policy
from app.license_client import get_license_client
from app.outbound_channels import mark_followup_attempt, mark_sales_owner_notification, notify_sales_owner, send_followup_message
from app.sales_governance import evaluate_sla_breaches, record_new_sla_breaches
from app.sales_quality import update_session_quality
from app.sales_timeline import append_lead_timeline_event
from app.session_store import iter_session_snapshots, save_session_snapshot

logger = logging.getLogger(__name__)


class LeadFollowupWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._policy_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}

    def start(self) -> None:
        settings = get_settings()
        if not settings.lead_followup_worker_enabled:
            logger.info("Lead follow-up worker disabled")
            return
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="lead-followup-worker")

    async def stop(self) -> None:
        self._stop_event.set()
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def run_once(self) -> int:
        settings = get_settings()
        processed = 0
        async for channel, uid, session in iter_session_snapshots(batch_size=settings.lead_followup_scan_batch_size):
            company_code = str(session.get("company_code") or "").strip()
            if not company_code:
                continue
            lead_config = await self._get_lead_config(company_code)
            stalled_detection_enabled = bool(lead_config.get("stalled_detection_enabled", True))
            previous_profile = normalize_lead_profile(active_lead_profile(session))
            session_changed = False
            previous_quality_flags = list(session.get("quality_flags") or []) if isinstance(session.get("quality_flags"), list) else []
            quality = update_session_quality(session, ai_policy={"sales_policy": lead_config.get("sales_policy")} if isinstance(lead_config.get("sales_policy"), dict) else None)
            if quality.get("quality_flags") != previous_quality_flags:
                append_lead_timeline_event(
                    session,
                    event_type="conversation_quality_flagged" if quality.get("quality_flags") else "conversation_quality_cleared",
                    payload={
                        "quality_score": quality.get("conversation_quality_score"),
                        "quality_flags": quality.get("quality_flags"),
                        "source": "lead_followup_worker",
                    },
                )
                session_changed = True
            new_sla_breaches = record_new_sla_breaches(
                session,
                evaluate_sla_breaches(session=session, lead_config=lead_config),
            )
            for breach in new_sla_breaches:
                append_lead_timeline_event(
                    session,
                    event_type="sales_sla_breached",
                    payload={
                        "sla_rule": breach.get("rule"),
                        "sla_minutes": breach.get("sla_minutes"),
                        "source": "lead_followup_worker",
                    },
                )
                await self._emit_sla_breach_event(
                    company_code=company_code,
                    channel=channel,
                    uid=uid,
                    session=session,
                    breach=breach,
                )
                if breach.get("rule") == "hot_lead_owner_accept":
                    escalation_delivery = await self._notify_sales_owner(
                        session=session,
                        lead_config=lead_config,
                        reason="owner_accept_sla_breached",
                        escalation=True,
                    )
                    mark_sales_owner_notification(session, escalation_delivery)
                    if escalation_delivery.get("sent"):
                        append_lead_timeline_event(
                            session,
                            event_type="sales_owner_escalated",
                            payload={
                                "reason": "owner_accept_sla_breached",
                                "sales_owner_delivery": escalation_delivery,
                                "source": "lead_followup_worker",
                            },
                        )
                session_changed = True
            if previous_profile.get("sales_owner_status") in {"accepted", "closed_not_target"}:
                if session_changed:
                    await save_session_snapshot(channel, uid, session)
                    processed += 1
                continue
            if not stalled_detection_enabled:
                if session_changed:
                    await save_session_snapshot(channel, uid, session)
                    processed += 1
                continue
            set_active_lead_profile(session, mark_stalled_if_needed(
                current_profile=previous_profile,
                last_interaction_at=session.get("last_interaction_at"),
                idle_after=self._stalled_after(lead_config),
            ))
            current_profile = normalize_lead_profile(active_lead_profile(session))
            if current_profile.get("status") != "stalled" or previous_profile.get("status") == "stalled":
                if session_changed:
                    await save_session_snapshot(channel, uid, session)
                    processed += 1
                continue
            current_profile["last_sales_event"] = "lead_stalled"
            set_active_lead_profile(session, current_profile)
            append_lead_timeline_event(
                session,
                event_type="lead_stalled",
                payload={
                    "previous_status": previous_profile.get("status"),
                    "status": current_profile.get("status"),
                    "source": "lead_followup_worker",
                },
            )
            can_followup, blocked_reason = can_send_followup(current_profile=current_profile, lead_config=lead_config)
            if can_followup:
                delivery = await self._send_followup(
                    channel=channel,
                    uid=uid,
                    session=session,
                    lead_config=lead_config,
                )
            else:
                set_active_lead_profile(session, mark_lost_if_followup_exhausted(
                    current_profile=current_profile,
                    reason=blocked_reason,
                ))
                delivery = {"sent": False, "status": blocked_reason or "followup_blocked", "channel": channel}
            mark_followup_attempt(session, delivery)
            append_lead_timeline_event(
                session,
                event_type="followup_sent" if delivery.get("sent") else "followup_blocked",
                payload={"followup_delivery": delivery, "source": "lead_followup_worker"},
            )
            if delivery.get("sent"):
                session.setdefault("messages", []).append({"role": "assistant", "content": delivery.get("text")})
                session["messages"] = session.get("messages", [])[-40:]
            owner_delivery = await self._notify_sales_owner(
                session=session,
                lead_config=lead_config,
                reason="lead_stalled",
            )
            mark_sales_owner_notification(session, owner_delivery)
            if owner_delivery.get("sent"):
                append_lead_timeline_event(
                    session,
                    event_type="sales_owner_notified",
                    payload={
                        "reason": "lead_stalled",
                        "sales_owner_delivery": owner_delivery,
                        "source": "lead_followup_worker",
                    },
                )
            await save_session_snapshot(channel, uid, session)
            await self._emit_stalled_event(
                company_code=company_code,
                channel=channel,
                uid=uid,
                session=session,
                previous_profile=previous_profile,
                delivery=delivery,
                owner_delivery=owner_delivery,
            )
            processed += 1
        return processed

    @staticmethod
    async def _emit_sla_breach_event(
        *,
        company_code: str,
        channel: str,
        uid: str,
        session: dict[str, Any],
        breach: dict[str, Any],
    ) -> None:
        payload = build_lead_event_payload(session=session)
        payload["source"] = "lead_followup_worker"
        payload["sla_breach"] = breach
        try:
            await get_license_client().create_conversation_event(
                company_code,
                event_type="sales_sla_breached",
                session_id=f"{channel}:{uid}",
                channel_type=channel,
                channel_user_id=uid,
                payload_json=payload,
                buyer_identity_id=session.get("buyer_identity_id"),
            )
        except Exception as exc:
            logger.warning("Failed to emit SLA breach for %s:%s: %s", channel, uid, exc)

    async def _run(self) -> None:
        settings = get_settings()
        interval_seconds = max(30, int(settings.lead_followup_scan_interval_seconds or 300))
        logger.info("Lead follow-up worker started with %ss scan interval", interval_seconds)
        while not self._stop_event.is_set():
            try:
                processed = await self.run_once()
                if processed:
                    logger.info("Lead follow-up worker marked %s stalled leads", processed)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lead follow-up worker scan failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

    async def _get_lead_config(self, company_code: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        cached = self._policy_cache.get(company_code)
        if cached and now - cached[0] < timedelta(minutes=5):
            return cached[1]
        settings = get_settings()
        config: dict[str, Any] = {"stalled_after_minutes": settings.lead_stalled_after_minutes}
        try:
            policy_response = await get_license_client().get_ai_policy(company_code)
            ai_policy = policy_response.get("ai_policy") if isinstance(policy_response.get("ai_policy"), dict) else {}
            lead_config = lead_config_from_ai_policy(ai_policy)
            config.update(lead_config)
            if isinstance(ai_policy.get("sales_policy"), dict):
                config["sales_policy"] = ai_policy["sales_policy"]
        except Exception as exc:
            logger.warning("Failed to load lead management policy for %s: %s", company_code, exc)
        self._policy_cache[company_code] = (now, config)
        return config

    @staticmethod
    def _stalled_after(lead_config: dict[str, Any]) -> timedelta:
        settings = get_settings()
        try:
            minutes = int(lead_config.get("stalled_after_minutes", settings.lead_stalled_after_minutes) or settings.lead_stalled_after_minutes)
        except (TypeError, ValueError):
            minutes = settings.lead_stalled_after_minutes
        return timedelta(minutes=max(5, minutes))

    async def _emit_stalled_event(
        self,
        *,
        company_code: str,
        channel: str,
        uid: str,
        session: dict[str, Any],
        previous_profile: dict[str, Any],
        delivery: dict[str, Any],
        owner_delivery: dict[str, Any],
    ) -> None:
        payload = build_lead_event_payload(session=session, previous_profile=previous_profile)
        payload["source"] = "lead_followup_worker"
        payload["followup_delivery"] = {
            "sent": bool(delivery.get("sent")),
            "status": delivery.get("status"),
            "channel": delivery.get("channel"),
            "via": delivery.get("via"),
        }
        payload["sales_owner_delivery"] = {
            "sent": bool(owner_delivery.get("sent")),
            "status": owner_delivery.get("status"),
            "channel": owner_delivery.get("channel"),
        }
        await get_license_client().create_conversation_event(
            company_code,
            event_type="lead_stalled",
            session_id=f"{channel}:{uid}",
            channel_type=channel,
            channel_user_id=uid,
            payload_json=payload,
            buyer_identity_id=session.get("buyer_identity_id"),
        )
        if delivery.get("sent"):
            await get_license_client().create_transcript_message(
                company_code,
                f"{channel}:{uid}",
                message_id=f"followup-{datetime.now(UTC).timestamp()}",
                channel_type=channel,
                channel_user_id=uid,
                role="assistant",
                message_type="followup",
                content=str(delivery.get("text") or ""),
                stage=session.get("stage"),
                behavior_class=session.get("behavior_class"),
                payload_json=payload,
                buyer_identity_id=session.get("buyer_identity_id"),
                erp_customer_id=session.get("erp_customer_id"),
                buyer_name=session.get("buyer_name"),
                buyer_phone=session.get("buyer_phone"),
            )

    @staticmethod
    async def _send_followup(
        *,
        channel: str,
        uid: str,
        session: dict[str, Any],
        lead_config: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            return await send_followup_message(
                channel=channel,
                channel_uid=uid,
                session=session,
                ai_policy={"lead_management": lead_config},
            )
        except Exception as exc:
            logger.warning("Failed to send proactive follow-up to %s:%s: %s", channel, uid, exc)
            return {"sent": False, "status": "send_failed", "channel": channel, "error": str(exc)}

    @staticmethod
    async def _notify_sales_owner(
        *,
        session: dict[str, Any],
        lead_config: dict[str, Any],
        reason: str,
        escalation: bool = False,
    ) -> dict[str, Any]:
        try:
            return await notify_sales_owner(
                session=session,
                ai_policy={"lead_management": lead_config},
                reason=reason,
                escalation=escalation,
            )
        except Exception as exc:
            logger.warning("Failed to notify sales owner for %s: %s", session.get("company_code"), exc)
            return {"sent": False, "status": "send_failed", "error": str(exc)}


_worker: LeadFollowupWorker | None = None


def get_lead_followup_worker() -> LeadFollowupWorker:
    global _worker
    if _worker is None:
        _worker = LeadFollowupWorker()
    return _worker
