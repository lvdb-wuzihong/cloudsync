"""Aliyun adapter package (one module per resource type, to be filled in P1/P2)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cloudsync.adapters.base import register_adapter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource

PROVIDER = "aliyun"

# Default resource set when cmdb_sync_tasks.resource_types is empty
# (design doc section 5.2 frequency tiers).
DEFAULT_RESOURCE_TYPES = [
    # compute tier (5-10min) + account root node
    "aliyun_account",
    "aliyun_ecs",
    # database/storage tier (30min)
    "aliyun_rds",
    "aliyun_redis",
    "aliyun_oss",
    # network tier (1h)
    "aliyun_vpc",
    "aliyun_vswitch",
    "aliyun_security_group",
    "aliyun_clb",
    "aliyun_nlb",
    "aliyun_nat_gateway",
    "aliyun_eip",
    "aliyun_disk",
    "aliyun_nas",
]


class AliyunAdapter:
    """Placeholder adapter; real SDK fetching lands in P1/P2 per resource module."""

    provider: str = PROVIDER

    def default_resource_types(self) -> list[str]:
        """Return the provider default resource type set."""
        return list(DEFAULT_RESOURCE_TYPES)

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Not implemented yet (skeleton stage); raises so rounds abort cleanly."""
        raise NotImplementedError(
            f"aliyun adapter not implemented yet for {resource_type}"
        )
        yield  # pragma: no cover - makes this an async generator


register_adapter(AliyunAdapter())
