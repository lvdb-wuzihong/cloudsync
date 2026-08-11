"""Read-only ORM mappings for the bingops shared tables.

No DDL is ever issued from this project; both tables are owned by bingops.
The only write performed anywhere in cloudsync is UPDATE(last_synced_at)
on cmdb_sync_tasks (design doc sections 2.1 / 2.3).
"""

from __future__ import annotations

# Kept at runtime on purpose: SQLAlchemy resolves stringified Mapped[datetime]
# annotations against module globals, so TYPE_CHECKING would break mapping.
from datetime import datetime  # noqa: TC003

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for shared-table mappings."""


class SyncTaskRow(Base):
    """cmdb_sync_tasks mapping (SELECT + UPDATE last_synced_at only)."""

    __tablename__ = "cmdb_sync_tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    task_type: Mapped[str] = mapped_column(String(32))
    target_id: Mapped[str] = mapped_column(String(128))
    provider: Mapped[str] = mapped_column(String(32))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    schedule: Mapped[str] = mapped_column(String(64))
    resource_types: Mapped[list | None] = mapped_column(JSON, default=list)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResourceRow(Base):
    """cmdb_resources mapping (SELECT only, used by diff reconciliation)."""

    __tablename__ = "cmdb_resources"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    model_id: Mapped[int] = mapped_column(Integer)
    provider: Mapped[str] = mapped_column(String(32))
    provider_id: Mapped[str] = mapped_column(String(256))
    cloud_account: Mapped[str] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(32))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
