"""GCP adapter package (one module per resource type, to be filled in P3)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cloudsync.adapters.base import register_adapter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource

PROVIDER = "gcp"

# Default resource set when cmdb_sync_tasks.resource_types is empty
# (design doc section 5.2 frequency tiers).
DEFAULT_RESOURCE_TYPES = [
    # compute tier (5-10min) + account root node
    "gcp_account",
    "gcp_compute",
    # database tier (30min)
    "gcp_cloudsql",
    # network tier (1h)
    "gcp_vpc",
    "gcp_subnet",
    "gcp_firewall",
    "gcp_disk",
]


class GcpAdapter:
    """Placeholder adapter; real SDK fetching lands in P3 per resource module."""

    provider: str = PROVIDER

    def default_resource_types(self) -> list[str]:
        """Return the provider default resource type set."""
        return list(DEFAULT_RESOURCE_TYPES)

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Not implemented yet (skeleton stage); raises so rounds abort cleanly."""
        raise NotImplementedError(f"gcp adapter not implemented yet for {resource_type}")
        yield  # pragma: no cover - makes this an async generator


register_adapter(GcpAdapter())
