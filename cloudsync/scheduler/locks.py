"""PostgreSQL advisory locks for multi-instance task ownership.

Design doc section 2.2 (v2 horizontal scaling): instances race for
pg_try_advisory_lock keyed by cmdb_sync_tasks.id; the winner executes the
round, losers skip. Advisory locks are session-scoped: a crashed instance's
locks are released automatically when its connection drops.

With replicas=1 the lock adds one cheap query per round and changes nothing
else, so it is safe to enable from day one.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("cloudsync.scheduler.locks")


async def try_acquire_task_lock(session: AsyncSession, task_id: int) -> bool:
    """Try to acquire the advisory lock for a sync task (non-blocking).

    Args:
        session: Session whose underlying connection will own the lock.
        task_id: cmdb_sync_tasks.id used as the lock key.

    Returns:
        True when this instance now owns the lock and may execute the round.
    """
    result = await session.execute(
        text("SELECT pg_try_advisory_lock(:key)"), {"key": task_id}
    )
    return bool(result.scalar())


async def release_task_lock(session: AsyncSession, task_id: int) -> None:
    """Release the advisory lock; must run on the same session that acquired it.

    Args:
        session: Session owning the lock.
        task_id: cmdb_sync_tasks.id used as the lock key.
    """
    await session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": task_id})
