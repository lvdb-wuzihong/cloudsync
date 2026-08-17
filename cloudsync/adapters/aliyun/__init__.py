"""Aliyun adapter package (one module per resource type, dispatched below)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cloudsync.adapters.aliyun.clb import list_clb
from cloudsync.adapters.aliyun.disk import list_disk
from cloudsync.adapters.aliyun.ecs import list_ecs
from cloudsync.adapters.aliyun.eip import list_eip
from cloudsync.adapters.aliyun.nas import list_nas
from cloudsync.adapters.aliyun.nat_gateway import list_nat_gateway
from cloudsync.adapters.aliyun.nlb import list_nlb
from cloudsync.adapters.aliyun.oss import list_oss
from cloudsync.adapters.aliyun.rds import list_rds
from cloudsync.adapters.aliyun.security_group import list_security_group
from cloudsync.adapters.aliyun.vpc import list_vpc
from cloudsync.adapters.aliyun.vswitch import list_vswitch
from cloudsync.adapters.base import register_adapter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource

type Fetcher = Callable[[AccountConfig], AsyncIterator[NormalizedResource]]

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


# resource_type (model code) -> fetcher coroutine; grows per phase
_FETCHERS: dict[str, Fetcher] = {
    "aliyun_ecs": list_ecs,
    "aliyun_vpc": list_vpc,
    "aliyun_vswitch": list_vswitch,
    "aliyun_security_group": list_security_group,
    "aliyun_eip": list_eip,
    "aliyun_clb": list_clb,
    "aliyun_nlb": list_nlb,
    "aliyun_nat_gateway": list_nat_gateway,
    "aliyun_oss": list_oss,
    "aliyun_disk": list_disk,
    "aliyun_nas": list_nas,
    "aliyun_rds": list_rds,
}


class AliyunAdapter:
    """Dispatches per resource type; unfetched types raise NotImplementedError."""

    provider: str = PROVIDER

    def default_resource_types(self) -> list[str]:
        """Return the provider default resource type set."""
        return list(DEFAULT_RESOURCE_TYPES)

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Yield normalized resources via the per-type fetcher module."""
        fetcher = _FETCHERS.get(resource_type)
        if fetcher is None:
            raise NotImplementedError(
                f"aliyun adapter not implemented yet for {resource_type}"
            )
        async for resource in fetcher(account):
            yield resource


register_adapter(AliyunAdapter())
