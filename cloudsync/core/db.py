"""Async access to the bingops shared PostgreSQL (design doc section 2.3).

Access is minimized: SELECT on cmdb_sync_tasks / cmdb_resources plus
UPDATE(last_synced_at) on cmdb_sync_tasks. No DDL is ever issued here.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from cloudsync.core.config import settings

engine: AsyncEngine = create_async_engine(settings.database_url, pool_pre_ping=True)

session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def dispose_engine() -> None:
    """Dispose the engine pool on shutdown."""
    await engine.dispose()
