"""Task table access: load cloud sync tasks, hot reload, write back last_synced_at.

Business policy (who/what/frequency/on-off) has a single source of truth:
cmdb_sync_tasks. This module never keeps a second copy of that policy.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from sqlalchemy import select, update

from cloudsync.scheduler.models import SyncTaskRow

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("cloudsync.scheduler.tasks")


class SyncTask(BaseModel):
    """One cloud sync task row (task_type='cloud')."""

    id: int
    target_id: str  # cloud account ID, must equal accounts.yaml account_id
    provider: str  # aliyun / gcp; selects adapter and topic
    enabled: bool = True
    schedule: str  # cron expression
    resource_types: list[str] = Field(default_factory=list)  # empty = provider default set
    last_synced_at: datetime | None = None


async def load_cloud_tasks(session: AsyncSession) -> list[SyncTask]:
    """Load enabled-relevant cloud tasks from cmdb_sync_tasks.

    Args:
        session: Async session on the bingops shared database.

    Returns:
        Tasks with task_type='cloud'; disabled rows are included and filtered
        by the engine so hot toggling off is visible in logs.
    """
    stmt = select(SyncTaskRow).where(SyncTaskRow.task_type == "cloud")
    rows = (await session.execute(stmt)).scalars().all()
    tasks = [
        SyncTask(
            id=row.id,
            target_id=row.target_id,
            provider=row.provider,
            enabled=row.enabled,
            schedule=row.schedule,
            resource_types=list(row.resource_types or []),
            last_synced_at=row.last_synced_at,
        )
        for row in rows
    ]
    logger.info("Cloud sync tasks loaded", extra={"count": len(tasks)})
    return tasks


async def mark_task_synced(session: AsyncSession, task_id: int) -> None:
    """Write back last_synced_at after a successful round (the only UPDATE here).

    Args:
        session: Async session on the bingops shared database.
        task_id: cmdb_sync_tasks.id of the completed task.
    """
    stmt = (
        update(SyncTaskRow)
        .where(SyncTaskRow.id == task_id)
        .values(last_synced_at=datetime.now(UTC))
    )
    await session.execute(stmt)
    await session.commit()
