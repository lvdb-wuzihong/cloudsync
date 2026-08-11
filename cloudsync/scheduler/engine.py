"""Sync engine main loop: cron triggering, hot reload, round orchestration.

Design doc sections 2.1/2.2: table-driven scheduling, 60s hot reload,
single replica (v1), per-task error containment, slow-round warnings.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from croniter import croniter

from cloudsync.adapters.base import get_adapter
from cloudsync.core.config import settings
from cloudsync.core.db import session_factory
from cloudsync.core.exceptions import CloudSyncError, CredentialError
from cloudsync.normalize.hashing import compute_resource_version
from cloudsync.reconcile.soft_delete import reconcile_deleted
from cloudsync.scheduler.locks import release_task_lock, try_acquire_task_lock
from cloudsync.scheduler.tasks import SyncTask, load_cloud_tasks, mark_task_synced
from cloudsync.schemas.messages import build_upsert_message

if TYPE_CHECKING:
    from cloudsync.core.accounts import AccountRegistry
    from cloudsync.kafka.producer import CloudSyncProducer

logger = logging.getLogger("cloudsync.scheduler.engine")

TICK_INTERVAL = 1.0  # main loop resolution in seconds


class SyncEngine:
    """Drives all cloud sync tasks (single-replica, v1)."""

    def __init__(self, accounts: AccountRegistry, producer: CloudSyncProducer) -> None:
        self._accounts = accounts
        self._producer = producer
        self._tasks: dict[int, SyncTask] = {}
        self._next_fire: dict[int, datetime] = {}
        self._stopped = asyncio.Event()

    # ── lifecycle ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the engine until shutdown() is called."""
        await self._reload_tasks()
        last_reload = time.monotonic()
        while not self._stopped.is_set():
            now = datetime.now(UTC)
            if time.monotonic() - last_reload >= settings.task_reload_interval:
                await self._reload_tasks()
                last_reload = time.monotonic()
            for task in self._due_tasks(now):
                asyncio.create_task(self._run_task(task))
            await asyncio.sleep(TICK_INTERVAL)

    async def shutdown(self) -> None:
        """Stop the engine loop and release resources."""
        self._stopped.set()
        await self._producer.close()

    # ── task table hot reload ────────────────────────────────────────────

    async def _reload_tasks(self) -> None:
        """Reload cmdb_sync_tasks (task_type='cloud'); add/remove/enable hot-applied."""
        try:
            async with session_factory() as session:
                tasks = await load_cloud_tasks(session)
        except Exception:
            logger.exception("Failed to reload sync tasks")
            return
        self._tasks = {t.id: t for t in tasks}
        now = datetime.now(UTC)
        for task in tasks:
            if task.id not in self._next_fire:
                self._next_fire[task.id] = self._compute_next_fire(task, now)
        # Drop fire slots for removed tasks
        for task_id in list(self._next_fire):
            if task_id not in self._tasks:
                del self._next_fire[task_id]

    def _compute_next_fire(self, task: SyncTask, base: datetime) -> datetime:
        """Next cron fire time at or after base; falls back to base+1h on bad cron."""
        try:
            cron = croniter(task.schedule, base)
            return cron.get_next(datetime).astimezone(UTC)
        except (ValueError, KeyError):
            logger.error("Invalid cron expression in sync task",
                         extra={"task_id": task.id, "schedule": task.schedule})
            return base.replace(minute=0, second=0, microsecond=0)

    def _due_tasks(self, now: datetime) -> list[SyncTask]:
        """Tasks whose fire time has passed; reschedule immediately (in-flight guard)."""
        due: list[SyncTask] = []
        for task_id, fire_at in self._next_fire.items():
            task = self._tasks.get(task_id)
            if task is None or not task.enabled:
                continue
            if now >= fire_at:
                due.append(task)
                # Reschedule before executing so a long round cannot double-fire
                self._next_fire[task_id] = self._compute_next_fire(task, now)
        return due

    # ── round orchestration ──────────────────────────────────────────────

    async def _run_task(self, task: SyncTask) -> None:
        """Execute one task round with advisory-lock ownership and containment.

        Instances race for pg_try_advisory_lock(task.id); losers skip so
        multi-replica deployments never double-execute a round (design doc
        section 2.2). The lock session stays open for the whole round and is
        released in finally; a crashed instance loses its connection and PG
        drops the lock automatically.
        """
        started = time.perf_counter()
        async with session_factory() as lock_session:
            if not await try_acquire_task_lock(lock_session, task.id):
                logger.info("Sync task lock held by another instance, skipped",
                            extra={"task_id": task.id, "provider": task.provider})
                return
            try:
                await self._run_round(task, started)
            except CloudSyncError as e:
                logger.error("Sync task failed with business error",
                             extra={"task_id": task.id, "provider": task.provider,
                                    "error_code": e.error_code})
            except Exception:
                logger.exception("Sync task failed with unexpected error",
                                 extra={"task_id": task.id, "provider": task.provider})
            finally:
                await release_task_lock(lock_session, task.id)

    async def _run_round(self, task: SyncTask, started: float) -> None:
        """One full round: per resource type fetch -> publish -> reconcile."""
        account = self._accounts.get(task.provider, task.target_id)
        if account is None:
            # Default-deny: task without credential errors out but never blocks others
            raise CredentialError(
                f"No credential for task target_id={task.target_id} provider={task.provider}"
            )

        adapter = get_adapter(task.provider)
        resource_types = task.resource_types or adapter.default_resource_types()

        upserted_total = 0
        deleted_total = 0
        async with session_factory() as session:
            for resource_type in resource_types:
                upserted, deleted = await self._sync_resource_type(
                    session, task, account, adapter, resource_type
                )
                upserted_total += upserted
                deleted_total += deleted

        async with session_factory() as session:
            await mark_task_synced(session, task.id)

        duration_ms = (time.perf_counter() - started) * 1000
        logger.info("Sync round completed",
                    extra={"task_id": task.id, "provider": task.provider,
                           "account": task.target_id,
                           "upserted": upserted_total, "deleted": deleted_total,
                           "duration_ms": round(duration_ms, 2)})
        self._warn_if_slow(task, duration_ms)

    async def _sync_resource_type(self, session, task: SyncTask, account, adapter,
                                  resource_type: str) -> tuple[int, int]:
        """Fetch one resource type, publish upserts, then diff-reconcile deletes.

        Raises on any fetch failure so the round aborts WITHOUT emitting deletes
        (design doc section 6: prevents mass mis-deletion on API flapping).
        """
        started = time.perf_counter()
        resources = []
        async for resource in adapter.list_resources(account, resource_type):
            resources.append(resource)
        seen_ids = {r.provider_id for r in resources}

        messages = [
            build_upsert_message(r, compute_resource_version(r)) for r in resources
        ]
        await self._producer.send_batch(task.provider, messages)

        delete_messages = await reconcile_deleted(
            session,
            provider=task.provider,
            cloud_account=task.target_id,
            resource_type=resource_type,
            seen_ids=seen_ids,
        )
        await self._producer.send_batch(task.provider, delete_messages)

        duration_ms = (time.perf_counter() - started) * 1000
        logger.info("Resource type synced",
                    extra={"task_id": task.id, "provider": task.provider,
                           "account": task.target_id, "resource_type": resource_type,
                           "upserted": len(messages), "deleted": len(delete_messages),
                           "duration_ms": round(duration_ms, 2)})
        return len(messages), len(delete_messages)

    def _warn_if_slow(self, task: SyncTask, duration_ms: float) -> None:
        """Warn when a round consumed more than 80% of its schedule interval."""
        try:
            base = datetime.now(UTC)
            cron = croniter(task.schedule, base)
            interval_s = cron.get_next(datetime).timestamp() - cron.get_prev(datetime).timestamp()
        except (ValueError, KeyError):
            return
        if duration_ms / 1000 > interval_s * settings.slow_round_ratio:
            logger.warning("Sync round slower than schedule budget",
                           extra={"task_id": task.id, "provider": task.provider,
                                  "duration_ms": round(duration_ms, 2),
                                  "interval_s": interval_s})
