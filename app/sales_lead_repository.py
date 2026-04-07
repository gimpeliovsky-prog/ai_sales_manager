from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from app.sales_reporting import lead_snapshot

logger = logging.getLogger(__name__)


class SalesLeadRepository(Protocol):
    async def init(self) -> None: ...

    async def close(self) -> None: ...

    async def upsert_from_session(self, *, channel: str, uid: str, session: dict[str, Any]) -> None: ...

    async def upsert_record(self, record: dict[str, Any]) -> None: ...

    async def enqueue_crm_sync_event(self, record: dict[str, Any], event_type: str = "lead_upserted") -> None: ...

    async def claim_crm_sync_events(self, *, limit: int) -> list[dict[str, Any]]: ...

    async def mark_crm_sync_event_sent(self, event: dict[str, Any], delivery: dict[str, Any]) -> None: ...

    async def mark_crm_sync_event_failed(self, event: dict[str, Any], delivery: dict[str, Any], *, max_attempts: int) -> None: ...

    async def crm_sync_summary(self, *, company_code: str | None = None, limit: int = 20) -> dict[str, Any]: ...

    async def storage_summary(self, *, company_code: str | None = None) -> dict[str, Any]: ...

    async def prune_storage(
        self,
        *,
        company_code: str | None = None,
        retention_days: int | None = None,
        max_per_company: int | None = None,
    ) -> dict[str, Any]: ...

    async def get(self, lead_id: str) -> dict[str, Any] | None: ...

    async def list_by_company(self, *, company_code: str, offset: int = 0, limit: int = 500) -> list[dict[str, Any]]: ...

    async def prune_company(self, company_code: str) -> int: ...


def _lead_key(lead_id: str) -> str:
    return f"ai_sales_lead:{lead_id}"


def _company_index_key(company_code: str) -> str:
    return f"ai_sales_leads_by_company:{company_code}"


def _prune_lock_key(company_code: str) -> str:
    return f"ai_sales_leads_prune_lock:{company_code}"


def _outbox_event_key(event_id: str) -> str:
    return f"ai_sales_crm_outbox:{event_id}"


def _outbox_pending_key() -> str:
    return "ai_sales_crm_outbox_pending"


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp(value: Any) -> float:
    parsed = _parse_dt(value)
    if parsed:
        return parsed.timestamp()
    return datetime.now(UTC).timestamp()


def _setting(name: str, default: Any) -> Any:
    try:
        from app.config import get_settings

        return getattr(get_settings(), name, default)
    except Exception:
        return default


def _backend() -> str:
    return str(_setting("sales_lead_repository_backend", "redis") or "redis").strip().casefold()


def _retention_seconds() -> int:
    try:
        days = int(_setting("sales_lead_retention_days", 180) or 180)
    except (TypeError, ValueError):
        days = 180
    return max(1, days) * 86400


def _retention_seconds_for(days_override: int | None = None) -> int:
    if days_override is not None:
        return max(1, int(days_override)) * 86400
    return _retention_seconds()


def _timeline_limit() -> int:
    try:
        limit = int(_setting("sales_lead_timeline_limit", 100) or 100)
    except (TypeError, ValueError):
        limit = 100
    return max(1, min(1000, limit))


def _max_per_company() -> int:
    try:
        limit = int(_setting("sales_lead_max_per_company", 50000) or 50000)
    except (TypeError, ValueError):
        limit = 50000
    return max(100, limit)


def _max_per_company_for(limit_override: int | None = None) -> int:
    if limit_override is not None:
        return max(100, int(limit_override))
    return _max_per_company()


def _outbox_retention_seconds() -> int:
    try:
        days = int(_setting("sales_crm_sync_outbox_retention_days", 30) or 30)
    except (TypeError, ValueError):
        days = 30
    return max(1, days) * 86400


def _sync_enabled() -> bool:
    return bool(_setting("sales_crm_sync_enabled", False))


def _backoff_seconds(attempts: int) -> int:
    return min(3600, 60 * (2 ** max(0, attempts - 1)))


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return default
        return parsed if parsed is not None else default
    return value if value is not None else default


def _compact_session_context(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "company_code": session.get("company_code"),
        "buyer_name": session.get("buyer_name"),
        "buyer_phone": session.get("buyer_phone"),
        "buyer_identity_id": session.get("buyer_identity_id"),
        "erp_customer_id": session.get("erp_customer_id"),
        "last_sales_order_name": session.get("last_sales_order_name"),
        "stage": session.get("stage"),
        "behavior_class": session.get("behavior_class"),
        "last_intent": session.get("last_intent"),
        "last_interaction_at": session.get("last_interaction_at"),
        "conversation_quality_score": session.get("conversation_quality_score"),
        "quality_flags": session.get("quality_flags") if isinstance(session.get("quality_flags"), list) else [],
        "coaching_notes": session.get("coaching_notes") if isinstance(session.get("coaching_notes"), list) else [],
        "quality_evaluated_at": session.get("quality_evaluated_at"),
        "sla_breaches": session.get("sla_breaches") if isinstance(session.get("sla_breaches"), list) else [],
    }


def compact_lead_record(*, channel: str, uid: str, session: dict[str, Any]) -> dict[str, Any] | None:
    profile = session.get("lead_profile") if isinstance(session.get("lead_profile"), dict) else {}
    lead_id = str(profile.get("lead_id") or "").strip()
    company_code = str(session.get("company_code") or "").strip()
    if not lead_id or not company_code:
        return None
    timeline = session.get("lead_timeline") if isinstance(session.get("lead_timeline"), list) else []
    snapshot = lead_snapshot(channel=channel, uid=uid, session=session)
    updated_at = str(session.get("last_interaction_at") or profile.get("last_updated_at") or datetime.now(UTC).isoformat())
    return {
        "schema_version": 1,
        "lead_id": lead_id,
        "company_code": company_code,
        "session_id": f"{channel}:{uid}",
        "channel": channel,
        "channel_uid": uid,
        "updated_at": updated_at,
        "lead": snapshot,
        "lead_profile": profile,
        "session_context": _compact_session_context(session),
        "timeline": timeline[-_timeline_limit():],
    }


def _crm_event_from_record(record: dict[str, Any], event_type: str) -> dict[str, Any]:
    from app.sales_crm_sync import build_sales_crm_outbox_event

    now_iso = datetime.now(UTC).isoformat()
    event = build_sales_crm_outbox_event(record, event_type=event_type)
    safe_lead_id = str(record.get("lead_id") or "unknown").replace(":", "_")
    event["event_id"] = f"crm_sync_{event_type}_{safe_lead_id}"
    event["created_at"] = now_iso
    event["updated_at"] = now_iso
    event["next_attempt_at"] = now_iso
    event["last_error"] = None
    return event


class RedisSalesLeadRepository:
    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def upsert_from_session(self, *, channel: str, uid: str, session: dict[str, Any]) -> None:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return
        record = compact_lead_record(channel=channel, uid=uid, session=session)
        if not record:
            return
        await self.upsert_record(record)

    async def upsert_record(self, record: dict[str, Any]) -> None:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return
        if not isinstance(record, dict) or not record.get("lead_id") or not record.get("company_code"):
            return
        from app.session_store import redis_client

        client = redis_client()
        retention_seconds = _retention_seconds()
        lead_id = str(record["lead_id"])
        company_code = str(record["company_code"])
        await client.setex(
            _lead_key(lead_id),
            retention_seconds,
            json.dumps(record, ensure_ascii=False, default=str),
        )
        await client.zadd(_company_index_key(company_code), {lead_id: _timestamp(record.get("updated_at"))})
        if await client.set(_prune_lock_key(company_code), "1", ex=3600, nx=True):
            await self.prune_company(company_code)
        await self.enqueue_crm_sync_event(record)

    async def enqueue_crm_sync_event(self, record: dict[str, Any], event_type: str = "lead_upserted") -> None:
        if not _sync_enabled():
            return
        if not isinstance(record, dict) or not record.get("lead_id"):
            return
        from app.session_store import redis_client

        event = _crm_event_from_record(record, event_type)
        client = redis_client()
        event_id = str(event["event_id"])
        ttl = _outbox_retention_seconds()
        await client.setex(_outbox_event_key(event_id), ttl, json.dumps(event, ensure_ascii=False, default=str))
        await client.zadd(_outbox_pending_key(), {event_id: _timestamp(event.get("next_attempt_at"))})

    async def claim_crm_sync_events(self, *, limit: int) -> list[dict[str, Any]]:
        if not _sync_enabled():
            return []
        from app.session_store import redis_client

        client = redis_client()
        now_score = datetime.now(UTC).timestamp()
        event_ids = await client.zrangebyscore(_outbox_pending_key(), "-inf", now_score, start=0, num=max(1, int(limit or 100)))
        claimed: list[dict[str, Any]] = []
        for event_id in event_ids:
            event_id_text = str(event_id)
            removed = await client.zrem(_outbox_pending_key(), event_id_text)
            if not removed:
                continue
            raw = await client.get(_outbox_event_key(event_id_text))
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            event["status"] = "processing"
            event["updated_at"] = datetime.now(UTC).isoformat()
            await client.setex(_outbox_event_key(event_id_text), _outbox_retention_seconds(), json.dumps(event, ensure_ascii=False, default=str))
            claimed.append(event)
        return claimed

    async def mark_crm_sync_event_sent(self, event: dict[str, Any], delivery: dict[str, Any]) -> None:
        from app.session_store import redis_client

        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            return
        event = dict(event)
        event["status"] = "sent"
        event["attempts"] = int(event.get("attempts") or 0) + 1
        event["updated_at"] = datetime.now(UTC).isoformat()
        event["delivery"] = delivery
        await redis_client().setex(_outbox_event_key(event_id), _outbox_retention_seconds(), json.dumps(event, ensure_ascii=False, default=str))

    async def mark_crm_sync_event_failed(self, event: dict[str, Any], delivery: dict[str, Any], *, max_attempts: int) -> None:
        from app.session_store import redis_client

        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            return
        attempts = int(event.get("attempts") or 0) + 1
        event = dict(event)
        event["attempts"] = attempts
        event["last_error"] = delivery.get("error") or delivery.get("status")
        event["updated_at"] = datetime.now(UTC).isoformat()
        client = redis_client()
        if attempts >= max(1, int(max_attempts or 1)):
            event["status"] = "dead"
            await client.setex(_outbox_event_key(event_id), _outbox_retention_seconds(), json.dumps(event, ensure_ascii=False, default=str))
            return
        next_attempt_at = datetime.now(UTC) + timedelta(seconds=_backoff_seconds(attempts))
        event["status"] = "retry"
        event["next_attempt_at"] = next_attempt_at.isoformat()
        await client.setex(_outbox_event_key(event_id), _outbox_retention_seconds(), json.dumps(event, ensure_ascii=False, default=str))
        await client.zadd(_outbox_pending_key(), {event_id: next_attempt_at.timestamp()})

    async def crm_sync_summary(self, *, company_code: str | None = None, limit: int = 20) -> dict[str, Any]:
        from collections import Counter

        from app.session_store import redis_client

        client = redis_client()
        status_counts: Counter[str] = Counter()
        failed_events: list[dict[str, Any]] = []
        async for key in client.scan_iter(match="ai_sales_crm_outbox:*", count=500):
            raw = await client.get(str(key))
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            if company_code and str(event.get("company_code") or "") != company_code:
                continue
            status = str(event.get("status") or "unknown")
            status_counts[status] += 1
            if status in {"retry", "dead", "processing"}:
                failed_events.append(event)
        failed_events.sort(key=lambda event: str(event.get("updated_at") or ""), reverse=True)
        return {
            "by_status": dict(status_counts),
            "failed_or_pending_attention": failed_events[: max(1, int(limit or 20))],
            "backend": "redis",
        }

    async def storage_summary(self, *, company_code: str | None = None) -> dict[str, Any]:
        from collections import Counter

        from app.session_store import redis_client

        client = redis_client()
        safe_company_code = str(company_code or "").strip()
        lead_count = 0
        companies: dict[str, int] = {}
        if safe_company_code:
            lead_count = int(await client.zcard(_company_index_key(safe_company_code)) or 0)
            companies[safe_company_code] = lead_count
        else:
            async for key in client.scan_iter(match="ai_sales_leads_by_company:*", count=500):
                company = str(key).removeprefix("ai_sales_leads_by_company:")
                count = int(await client.zcard(str(key)) or 0)
                companies[company] = count
                lead_count += count
        outbox_status: Counter[str] = Counter()
        async for key in client.scan_iter(match="ai_sales_crm_outbox:*", count=500):
            raw = await client.get(str(key))
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            if safe_company_code and str(event.get("company_code") or "") != safe_company_code:
                continue
            outbox_status[str(event.get("status") or "unknown")] += 1
        return {
            "backend": "redis",
            "company_code": safe_company_code or None,
            "lead_count": lead_count,
            "companies": companies,
            "crm_outbox_by_status": dict(outbox_status),
            "retention_days": int(_retention_seconds() / 86400),
            "max_per_company": _max_per_company(),
        }

    async def prune_storage(
        self,
        *,
        company_code: str | None = None,
        retention_days: int | None = None,
        max_per_company: int | None = None,
    ) -> dict[str, Any]:
        from app.session_store import redis_client

        client = redis_client()
        safe_company_code = str(company_code or "").strip()
        removed_leads = 0
        companies = [safe_company_code] if safe_company_code else []
        if not companies:
            async for key in client.scan_iter(match="ai_sales_leads_by_company:*", count=500):
                companies.append(str(key).removeprefix("ai_sales_leads_by_company:"))
        for company in companies:
            index_key = _company_index_key(company)
            cutoff = (datetime.now(UTC) - timedelta(seconds=_retention_seconds_for(retention_days))).timestamp()
            old_ids = [str(lead_id) for lead_id in await client.zrangebyscore(index_key, "-inf", cutoff)]
            if old_ids:
                await client.delete(*[_lead_key(lead_id) for lead_id in old_ids])
                removed_leads += await client.zrem(index_key, *old_ids)
            over_limit = await client.zcard(index_key) - _max_per_company_for(max_per_company)
            if over_limit > 0:
                trim_ids = [str(lead_id) for lead_id in await client.zrange(index_key, 0, over_limit - 1)]
                if trim_ids:
                    await client.delete(*[_lead_key(lead_id) for lead_id in trim_ids])
                    removed_leads += await client.zrem(index_key, *trim_ids)
        pending_ids = [str(event_id) for event_id in await client.zrange(_outbox_pending_key(), 0, -1)]
        stale_pending = [event_id for event_id in pending_ids if not await client.exists(_outbox_event_key(event_id))]
        removed_outbox_index = await client.zrem(_outbox_pending_key(), *stale_pending) if stale_pending else 0
        return {
            "backend": "redis",
            "company_code": safe_company_code or None,
            "removed_leads": int(removed_leads or 0),
            "removed_outbox_index_entries": int(removed_outbox_index or 0),
            "retention_days": retention_days,
            "max_per_company": max_per_company,
        }

    async def get(self, lead_id: str) -> dict[str, Any] | None:
        from app.session_store import redis_client

        raw = await redis_client().get(_lead_key(str(lead_id or "").strip()))
        if not raw:
            return None
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return record if isinstance(record, dict) else None

    async def list_by_company(self, *, company_code: str, offset: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        from app.session_store import redis_client

        safe_offset = max(0, int(offset or 0))
        safe_limit = max(1, min(50000, int(limit or 500)))
        client = redis_client()
        lead_ids = await client.zrevrange(
            _company_index_key(str(company_code or "").strip()),
            safe_offset,
            safe_offset + safe_limit - 1,
        )
        records: list[dict[str, Any]] = []
        stale_ids: list[str] = []
        for lead_id in lead_ids:
            record = await self.get(str(lead_id))
            if isinstance(record, dict):
                records.append(record)
            else:
                stale_ids.append(str(lead_id))
        if stale_ids:
            await client.zrem(_company_index_key(str(company_code or "").strip()), *stale_ids)
        return records

    async def prune_company(self, company_code: str) -> int:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return 0
        from app.session_store import redis_client

        safe_company_code = str(company_code or "").strip()
        if not safe_company_code:
            return 0
        client = redis_client()
        index_key = _company_index_key(safe_company_code)
        cutoff = (datetime.now(UTC) - timedelta(seconds=_retention_seconds())).timestamp()
        old_ids = [str(lead_id) for lead_id in await client.zrangebyscore(index_key, "-inf", cutoff)]
        removed = 0
        if old_ids:
            await client.delete(*[_lead_key(lead_id) for lead_id in old_ids])
            removed += await client.zrem(index_key, *old_ids)

        over_limit = await client.zcard(index_key) - _max_per_company()
        if over_limit > 0:
            trim_ids = [str(lead_id) for lead_id in await client.zrange(index_key, 0, over_limit - 1)]
            if trim_ids:
                await client.delete(*[_lead_key(lead_id) for lead_id in trim_ids])
                removed += await client.zrem(index_key, *trim_ids)
        return int(removed or 0)


class PostgresSalesLeadRepository:
    def __init__(self) -> None:
        self._pool: Any | None = None

    async def init(self) -> None:
        if self._pool is not None:
            return
        dsn = str(_setting("sales_lead_postgres_dsn", "") or "").strip()
        if not dsn:
            raise RuntimeError("SALES_LEAD_POSTGRES_DSN is required when SALES_LEAD_REPOSITORY_BACKEND=postgres")
        import asyncpg

        self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._ensure_schema()
        logger.info("Postgres sales lead repository initialized")

    async def close(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    async def _client(self) -> Any:
        if self._pool is None:
            await self.init()
        return self._pool

    async def _ensure_schema(self) -> None:
        pool = await self._client()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_sales_leads (
                    lead_id TEXT PRIMARY KEY,
                    company_code TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    channel_uid TEXT NOT NULL,
                    status TEXT,
                    temperature TEXT,
                    sales_owner_status TEXT,
                    updated_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ,
                    last_interaction_at TIMESTAMPTZ,
                    lead JSONB NOT NULL,
                    lead_profile JSONB NOT NULL,
                    session_context JSONB NOT NULL,
                    timeline JSONB NOT NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_leads_company_updated ON ai_sales_leads (company_code, updated_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_leads_company_status ON ai_sales_leads (company_code, status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_leads_company_owner ON ai_sales_leads (company_code, sales_owner_status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_leads_company_temperature ON ai_sales_leads (company_code, temperature)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_sales_crm_outbox (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    lead_id TEXT NOT NULL,
                    company_code TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    last_error TEXT,
                    payload JSONB NOT NULL,
                    delivery JSONB
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_crm_outbox_status_next ON ai_sales_crm_outbox (status, next_attempt_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_sales_crm_outbox_company ON ai_sales_crm_outbox (company_code, created_at DESC)"
            )

    async def upsert_from_session(self, *, channel: str, uid: str, session: dict[str, Any]) -> None:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return
        record = compact_lead_record(channel=channel, uid=uid, session=session)
        if not record:
            return
        await self.upsert_record(record)

    async def upsert_record(self, record: dict[str, Any]) -> None:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return
        if not isinstance(record, dict) or not record.get("lead_id") or not record.get("company_code"):
            return
        pool = await self._client()
        lead = record.get("lead") if isinstance(record.get("lead"), dict) else {}
        profile = record.get("lead_profile") if isinstance(record.get("lead_profile"), dict) else {}
        context = record.get("session_context") if isinstance(record.get("session_context"), dict) else {}
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_sales_leads (
                    lead_id, company_code, session_id, channel, channel_uid,
                    status, temperature, sales_owner_status, updated_at, created_at, last_interaction_at,
                    lead, lead_profile, session_context, timeline
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10, $11,
                    $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb
                )
                ON CONFLICT (lead_id) DO UPDATE SET
                    company_code = EXCLUDED.company_code,
                    session_id = EXCLUDED.session_id,
                    channel = EXCLUDED.channel,
                    channel_uid = EXCLUDED.channel_uid,
                    status = EXCLUDED.status,
                    temperature = EXCLUDED.temperature,
                    sales_owner_status = EXCLUDED.sales_owner_status,
                    updated_at = EXCLUDED.updated_at,
                    created_at = COALESCE(ai_sales_leads.created_at, EXCLUDED.created_at),
                    last_interaction_at = EXCLUDED.last_interaction_at,
                    lead = EXCLUDED.lead,
                    lead_profile = EXCLUDED.lead_profile,
                    session_context = EXCLUDED.session_context,
                    timeline = EXCLUDED.timeline
                """,
                str(record["lead_id"]),
                str(record["company_code"]),
                str(record["session_id"]),
                str(record["channel"]),
                str(record["channel_uid"]),
                str(profile.get("status") or lead.get("status") or ""),
                str(profile.get("temperature") or lead.get("temperature") or ""),
                str(profile.get("sales_owner_status") or lead.get("sales_owner_status") or ""),
                _parse_dt(record.get("updated_at")) or datetime.now(UTC),
                _parse_dt(profile.get("created_at") or lead.get("created_at")),
                _parse_dt(context.get("last_interaction_at") or lead.get("last_interaction_at")),
                json.dumps(lead, ensure_ascii=False, default=str),
                json.dumps(profile, ensure_ascii=False, default=str),
                json.dumps(context, ensure_ascii=False, default=str),
                json.dumps(record.get("timeline") if isinstance(record.get("timeline"), list) else [], ensure_ascii=False, default=str),
            )
        await self.enqueue_crm_sync_event(record)

    async def enqueue_crm_sync_event(self, record: dict[str, Any], event_type: str = "lead_upserted") -> None:
        if not _sync_enabled():
            return
        if not isinstance(record, dict) or not record.get("lead_id"):
            return
        event = _crm_event_from_record(record, event_type)
        pool = await self._client()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_sales_crm_outbox (
                    event_id, event_type, lead_id, company_code, status, attempts,
                    next_attempt_at, created_at, updated_at, last_error, payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                ON CONFLICT (event_id) DO UPDATE SET
                    status = 'pending',
                    attempts = 0,
                    company_code = EXCLUDED.company_code,
                    next_attempt_at = EXCLUDED.next_attempt_at,
                    updated_at = EXCLUDED.updated_at,
                    last_error = NULL,
                    payload = EXCLUDED.payload
                """,
                str(event["event_id"]),
                str(event["event_type"]),
                str(event.get("lead_id") or ""),
                str(event.get("company_code") or ""),
                str(event["status"]),
                int(event.get("attempts") or 0),
                _parse_dt(event.get("next_attempt_at")) or datetime.now(UTC),
                _parse_dt(event.get("created_at")) or datetime.now(UTC),
                _parse_dt(event.get("updated_at")) or datetime.now(UTC),
                event.get("last_error"),
                json.dumps(event.get("payload") or {}, ensure_ascii=False, default=str),
            )

    async def claim_crm_sync_events(self, *, limit: int) -> list[dict[str, Any]]:
        if not _sync_enabled():
            return []
        pool = await self._client()
        safe_limit = max(1, min(500, int(limit or 100)))
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM ai_sales_crm_outbox WHERE status IN ('sent', 'dead') AND updated_at < $1",
                datetime.now(UTC) - timedelta(seconds=_outbox_retention_seconds()),
            )
            rows = await conn.fetch(
                """
                WITH candidate AS (
                    SELECT event_id
                    FROM ai_sales_crm_outbox
                    WHERE status IN ('pending', 'retry') AND next_attempt_at <= now()
                    ORDER BY next_attempt_at ASC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ai_sales_crm_outbox
                SET status = 'processing', updated_at = now()
                WHERE event_id IN (SELECT event_id FROM candidate)
                RETURNING *
                """,
                safe_limit,
            )
        return [self._outbox_row_to_event(row) for row in rows]

    async def mark_crm_sync_event_sent(self, event: dict[str, Any], delivery: dict[str, Any]) -> None:
        pool = await self._client()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE ai_sales_crm_outbox
                SET status = 'sent',
                    attempts = attempts + 1,
                    updated_at = now(),
                    delivery = $2::jsonb
                WHERE event_id = $1
                  AND status = 'processing'
                """,
                str(event.get("event_id") or ""),
                json.dumps(delivery, ensure_ascii=False, default=str),
            )

    async def mark_crm_sync_event_failed(self, event: dict[str, Any], delivery: dict[str, Any], *, max_attempts: int) -> None:
        pool = await self._client()
        attempts = int(event.get("attempts") or 0) + 1
        status = "dead" if attempts >= max(1, int(max_attempts or 1)) else "retry"
        next_attempt_at = datetime.now(UTC) if status == "dead" else datetime.now(UTC) + timedelta(seconds=_backoff_seconds(attempts))
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE ai_sales_crm_outbox
                SET status = $2,
                    attempts = $3,
                    next_attempt_at = $4,
                    updated_at = now(),
                    last_error = $5,
                    delivery = $6::jsonb
                WHERE event_id = $1
                  AND status = 'processing'
                """,
                str(event.get("event_id") or ""),
                status,
                attempts,
                next_attempt_at,
                str(delivery.get("error") or delivery.get("status") or ""),
                json.dumps(delivery, ensure_ascii=False, default=str),
            )

    async def crm_sync_summary(self, *, company_code: str | None = None, limit: int = 20) -> dict[str, Any]:
        pool = await self._client()
        safe_limit = max(1, min(200, int(limit or 20)))
        company_filter = str(company_code or "").strip() or None
        async with pool.acquire() as conn:
            if company_filter:
                status_rows = await conn.fetch(
                    "SELECT status, count(*) AS count FROM ai_sales_crm_outbox WHERE company_code = $1 GROUP BY status",
                    company_filter,
                )
                attention_rows = await conn.fetch(
                    """
                    SELECT * FROM ai_sales_crm_outbox
                    WHERE company_code = $1 AND status IN ('retry', 'dead', 'processing')
                    ORDER BY updated_at DESC
                    LIMIT $2
                    """,
                    company_filter,
                    safe_limit,
                )
            else:
                status_rows = await conn.fetch("SELECT status, count(*) AS count FROM ai_sales_crm_outbox GROUP BY status")
                attention_rows = await conn.fetch(
                    """
                    SELECT * FROM ai_sales_crm_outbox
                    WHERE status IN ('retry', 'dead', 'processing')
                    ORDER BY updated_at DESC
                    LIMIT $1
                    """,
                    safe_limit,
                )
        return {
            "by_status": {str(row["status"]): int(row["count"]) for row in status_rows},
            "failed_or_pending_attention": [self._outbox_row_to_event(row) for row in attention_rows],
            "backend": "postgres",
        }

    async def storage_summary(self, *, company_code: str | None = None) -> dict[str, Any]:
        pool = await self._client()
        company_filter = str(company_code or "").strip() or None
        async with pool.acquire() as conn:
            if company_filter:
                total = await conn.fetchval("SELECT count(*) FROM ai_sales_leads WHERE company_code = $1", company_filter)
                company_rows = await conn.fetch(
                    "SELECT company_code, count(*) AS count FROM ai_sales_leads WHERE company_code = $1 GROUP BY company_code",
                    company_filter,
                )
                outbox_rows = await conn.fetch(
                    "SELECT status, count(*) AS count FROM ai_sales_crm_outbox WHERE company_code = $1 GROUP BY status",
                    company_filter,
                )
                bounds = await conn.fetchrow(
                    "SELECT min(updated_at) AS oldest, max(updated_at) AS newest FROM ai_sales_leads WHERE company_code = $1",
                    company_filter,
                )
            else:
                total = await conn.fetchval("SELECT count(*) FROM ai_sales_leads")
                company_rows = await conn.fetch(
                    "SELECT company_code, count(*) AS count FROM ai_sales_leads GROUP BY company_code ORDER BY count DESC"
                )
                outbox_rows = await conn.fetch("SELECT status, count(*) AS count FROM ai_sales_crm_outbox GROUP BY status")
                bounds = await conn.fetchrow("SELECT min(updated_at) AS oldest, max(updated_at) AS newest FROM ai_sales_leads")
        return {
            "backend": "postgres",
            "company_code": company_filter,
            "lead_count": int(total or 0),
            "companies": {str(row["company_code"]): int(row["count"]) for row in company_rows},
            "crm_outbox_by_status": {str(row["status"]): int(row["count"]) for row in outbox_rows},
            "oldest_lead_updated_at": bounds["oldest"].isoformat() if bounds and bounds["oldest"] else None,
            "newest_lead_updated_at": bounds["newest"].isoformat() if bounds and bounds["newest"] else None,
            "retention_days": int(_retention_seconds() / 86400),
            "max_per_company": _max_per_company(),
        }

    async def prune_storage(
        self,
        *,
        company_code: str | None = None,
        retention_days: int | None = None,
        max_per_company: int | None = None,
    ) -> dict[str, Any]:
        pool = await self._client()
        safe_company_code = str(company_code or "").strip()
        removed_leads = 0
        if safe_company_code:
            cutoff = datetime.now(UTC) - timedelta(seconds=_retention_seconds_for(retention_days))
            async with pool.acquire() as conn:
                old_deleted = await conn.fetchval(
                    "WITH deleted AS (DELETE FROM ai_sales_leads WHERE company_code = $1 AND updated_at < $2 RETURNING 1) SELECT count(*) FROM deleted",
                    safe_company_code,
                    cutoff,
                )
                overflow_deleted = await conn.fetchval(
                    """
                    WITH overflow AS (
                        SELECT lead_id
                        FROM ai_sales_leads
                        WHERE company_code = $1
                        ORDER BY updated_at DESC
                        OFFSET $2
                    ),
                    deleted AS (
                        DELETE FROM ai_sales_leads
                        WHERE lead_id IN (SELECT lead_id FROM overflow)
                        RETURNING 1
                    )
                    SELECT count(*) FROM deleted
                    """,
                    safe_company_code,
                    _max_per_company_for(max_per_company),
                )
            removed_leads = int(old_deleted or 0) + int(overflow_deleted or 0)
        else:
            cutoff = datetime.now(UTC) - timedelta(seconds=_retention_seconds_for(retention_days))
            async with pool.acquire() as conn:
                removed_leads = await conn.fetchval(
                    "WITH deleted AS (DELETE FROM ai_sales_leads WHERE updated_at < $1 RETURNING 1) SELECT count(*) FROM deleted",
                    cutoff,
                )
        async with pool.acquire() as conn:
            removed_outbox = await conn.fetchval(
                """
                WITH deleted AS (
                    DELETE FROM ai_sales_crm_outbox
                    WHERE status IN ('sent', 'dead') AND updated_at < $1
                    RETURNING 1
                )
                SELECT count(*) FROM deleted
                """,
                datetime.now(UTC) - timedelta(seconds=_outbox_retention_seconds()),
            )
        return {
            "backend": "postgres",
            "company_code": safe_company_code or None,
            "removed_leads": int(removed_leads or 0),
            "removed_outbox_events": int(removed_outbox or 0),
            "retention_days": retention_days,
            "max_per_company": max_per_company,
        }

    async def get(self, lead_id: str) -> dict[str, Any] | None:
        pool = await self._client()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ai_sales_leads WHERE lead_id = $1", str(lead_id or "").strip())
        return self._row_to_record(row) if row else None

    async def list_by_company(self, *, company_code: str, offset: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        safe_offset = max(0, int(offset or 0))
        safe_limit = max(1, min(50000, int(limit or 500)))
        pool = await self._client()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM ai_sales_leads
                WHERE company_code = $1
                ORDER BY updated_at DESC
                OFFSET $2 LIMIT $3
                """,
                str(company_code or "").strip(),
                safe_offset,
                safe_limit,
            )
        return [record for row in rows if (record := self._row_to_record(row))]

    async def prune_company(self, company_code: str) -> int:
        if not bool(_setting("sales_lead_persistence_enabled", True)):
            return 0
        pool = await self._client()
        safe_company_code = str(company_code or "").strip()
        cutoff = datetime.now(UTC) - timedelta(seconds=_retention_seconds())
        async with pool.acquire() as conn:
            old_deleted = await conn.fetchval(
                "WITH deleted AS (DELETE FROM ai_sales_leads WHERE company_code = $1 AND updated_at < $2 RETURNING 1) SELECT count(*) FROM deleted",
                safe_company_code,
                cutoff,
            )
            overflow_deleted = await conn.fetchval(
                """
                WITH overflow AS (
                    SELECT lead_id
                    FROM ai_sales_leads
                    WHERE company_code = $1
                    ORDER BY updated_at DESC
                    OFFSET $2
                ),
                deleted AS (
                    DELETE FROM ai_sales_leads
                    WHERE lead_id IN (SELECT lead_id FROM overflow)
                    RETURNING 1
                )
                SELECT count(*) FROM deleted
                """,
                safe_company_code,
                _max_per_company(),
            )
        return int(old_deleted or 0) + int(overflow_deleted or 0)

    @staticmethod
    def _row_to_record(row: Any) -> dict[str, Any] | None:
        if not row:
            return None
        data = dict(row)
        lead = _json_value(data.get("lead"), {})
        profile = _json_value(data.get("lead_profile"), {})
        context = _json_value(data.get("session_context"), {})
        timeline = _json_value(data.get("timeline"), [])
        return {
            "schema_version": 1,
            "lead_id": data.get("lead_id"),
            "company_code": data.get("company_code"),
            "session_id": data.get("session_id"),
            "channel": data.get("channel"),
            "channel_uid": data.get("channel_uid"),
            "updated_at": data.get("updated_at").isoformat() if data.get("updated_at") else None,
            "lead": lead if isinstance(lead, dict) else {},
            "lead_profile": profile if isinstance(profile, dict) else {},
            "session_context": context if isinstance(context, dict) else {},
            "timeline": timeline if isinstance(timeline, list) else [],
        }

    @staticmethod
    def _outbox_row_to_event(row: Any) -> dict[str, Any]:
        data = dict(row)
        payload = _json_value(data.get("payload"), {})
        delivery = _json_value(data.get("delivery"), {})
        return {
            "event_id": data.get("event_id"),
            "event_type": data.get("event_type"),
            "lead_id": data.get("lead_id"),
            "company_code": data.get("company_code"),
            "status": data.get("status"),
            "attempts": int(data.get("attempts") or 0),
            "next_attempt_at": data.get("next_attempt_at").isoformat() if data.get("next_attempt_at") else None,
            "created_at": data.get("created_at").isoformat() if data.get("created_at") else None,
            "updated_at": data.get("updated_at").isoformat() if data.get("updated_at") else None,
            "last_error": data.get("last_error"),
            "payload": payload if isinstance(payload, dict) else {},
            "delivery": delivery if isinstance(delivery, dict) else {},
        }


_repo: SalesLeadRepository | None = None


def get_sales_lead_repository() -> SalesLeadRepository:
    global _repo
    if _repo is None:
        _repo = PostgresSalesLeadRepository() if _backend() == "postgres" else RedisSalesLeadRepository()
    return _repo


async def init_sales_lead_repository() -> None:
    await get_sales_lead_repository().init()


async def close_sales_lead_repository() -> None:
    await get_sales_lead_repository().close()
