"""Diff soft-delete reconciliation (design doc section 6, decision D4).

After each (account x resource_type) fetch round, resources present in the
CMDB but absent from the fetched set are emitted as delete events. Discipline:

1. Only rows with source='discovery' are soft-deleted; manual rows untouched.
2. Any fetch failure aborts the round before reconciliation, so deletes are
   never emitted on partial data (prevents mass mis-deletion on API flapping).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, text

from cloudsync.scheduler.models import ResourceRow
from cloudsync.schemas.messages import CloudResourceMessage

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("cloudsync.reconcile.soft_delete")


async def resolve_model_id(session: AsyncSession, model_code: str) -> int | None:
    """Resolve a CMDB model code to its model id via cmdb_models (read-only).

    Args:
        session: Async session on the bingops shared database.
        model_code: Model code equal to the message resource_type.

    Returns:
        Model id, or None when the model is not registered.
    """
    stmt = text("SELECT id FROM cmdb_models WHERE code = :code")
    row = (await session.execute(stmt, {"code": model_code})).first()
    return row[0] if row else None


async def reconcile_deleted(
    session: AsyncSession,
    *,
    provider: str,
    cloud_account: str,
    resource_type: str,
    seen_ids: set[str],
) -> list[CloudResourceMessage]:
    """Compute disappeared resources and build delete messages.

    Args:
        session: Async session on the bingops shared database.
        provider: Cloud vendor, e.g. "aliyun".
        cloud_account: Account ID (cmdb_sync_tasks.target_id).
        resource_type: CMDB model code of the round.
        seen_ids: provider_ids fetched in the current round.

    Returns:
        Delete messages for stored-but-unseen discovery resources.
    """
    model_id = await resolve_model_id(session, resource_type)
    if model_id is None:
        logger.warning("Model code not registered, skip reconciliation",
                       extra={"provider": provider, "resource_type": resource_type})
        return []

    stmt = (
        select(ResourceRow.provider_id)
        .where(
            ResourceRow.model_id == model_id,
            ResourceRow.provider == provider,
            ResourceRow.cloud_account == cloud_account,
            ResourceRow.source == "discovery",
            ResourceRow.deleted_at.is_(None),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    stored_ids = set(rows)

    disappeared = stored_ids - seen_ids
    messages = [
        CloudResourceMessage(
            provider=provider,
            resource_type=resource_type,
            provider_id=pid,
            cloud_account=cloud_account,
            event_type="delete",
            resource_version="",  # identity-only event; consumer deletes by key
        )
        for pid in sorted(disappeared)
    ]
    if messages:
        logger.info("Disappeared resources detected",
                    extra={"provider": provider, "account": cloud_account,
                           "resource_type": resource_type, "deleted": len(messages)})
    return messages
