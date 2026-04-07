from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.config import get_settings
from app.sales_crm_sync import deliver_sales_crm_outbox_event
from app.sales_lead_repository import get_sales_lead_repository

logger = logging.getLogger(__name__)


class SalesCrmSyncWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        settings = get_settings()
        if not settings.sales_crm_sync_worker_enabled:
            logger.info("Sales CRM sync worker disabled")
            return
        if not settings.sales_crm_sync_enabled:
            logger.info("Sales CRM sync disabled")
            return
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="sales-crm-sync-worker")

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
        repo = get_sales_lead_repository()
        events = await repo.claim_crm_sync_events(limit=settings.sales_crm_sync_batch_size)
        processed = 0
        for event in events:
            delivery = await self._deliver(event)
            if delivery.get("synced"):
                await repo.mark_crm_sync_event_sent(event, delivery)
            else:
                await repo.mark_crm_sync_event_failed(
                    event,
                    delivery,
                    max_attempts=settings.sales_crm_sync_max_attempts,
                )
            processed += 1
        return processed

    async def _run(self) -> None:
        settings = get_settings()
        interval_seconds = max(10, int(settings.sales_crm_sync_scan_interval_seconds or 60))
        logger.info("Sales CRM sync worker started with %ss scan interval", interval_seconds)
        while not self._stop_event.is_set():
            try:
                processed = await self.run_once()
                if processed:
                    logger.info("Sales CRM sync worker processed %s outbox events", processed)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Sales CRM sync worker scan failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

    @staticmethod
    async def _deliver(event: dict[str, Any]) -> dict[str, Any]:
        try:
            return await deliver_sales_crm_outbox_event(event)
        except Exception as exc:
            return {"synced": False, "status": "send_failed", "error": str(exc)}


_worker: SalesCrmSyncWorker | None = None


def get_sales_crm_sync_worker() -> SalesCrmSyncWorker:
    global _worker
    if _worker is None:
        _worker = SalesCrmSyncWorker()
    return _worker
