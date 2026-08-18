"""GCP adapter package (one module per resource type, dispatched below)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cloudsync.adapters.base import register_adapter
from cloudsync.adapters.gcp.compute import list_compute

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource

type Fetcher = Callable[[AccountConfig], AsyncIterator[NormalizedResource]]

PROVIDER = "gcp"

# Planned model codes per design doc section 5.2 frequency tiers:
#   gcp_account / gcp_compute          (compute tier, 5-10min)
#   gcp_cloudsql                       (database tier, 30min)
#   gcp_vpc / gcp_subnet / gcp_firewall / gcp_disk  (network tier, 1h)

# resource_type (model code) -> fetcher coroutine; grows per resource module
_FETCHERS: dict[str, Fetcher] = {
    "gcp_compute": list_compute,
}


class GcpAdapter:
    """Dispatches per resource type; unfetched types raise NotImplementedError."""

    provider: str = PROVIDER

    def default_resource_types(self) -> list[str]:
        """Default set when cmdb_sync_tasks.resource_types is empty.

        Derived from the registered fetchers so the default set can never
        contain an unimplemented type (empty whitelist = all implemented).
        """
        return sorted(_FETCHERS)

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Yield normalized resources via the per-type fetcher module."""
        fetcher = _FETCHERS.get(resource_type)
        if fetcher is None:
            raise NotImplementedError(
                f"gcp adapter not implemented yet for {resource_type}"
            )
        async for resource in fetcher(account):
            yield resource


register_adapter(GcpAdapter())
