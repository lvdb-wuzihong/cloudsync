"""ProviderAdapter protocol and registry (design doc section 5.1).

Cross-vendor isomorphic resources (VPC/subnet/security group/compute/cloud DB)
must align field codes strictly with cmdb-model-presets.md (e.g. cidr_block,
memory_gb, rules + rules_hash shared across providers).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from cloudsync.core.exceptions import ConfigError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource


@runtime_checkable
class ProviderAdapter(Protocol):
    """Fetches and normalizes resources for one cloud provider."""

    provider: str

    def default_resource_types(self) -> list[str]:
        """Default resource type set when cmdb_sync_tasks.resource_types is empty."""
        ...

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Yield normalized resources of the given type for the account.

        Must raise (never yield nothing silently) on any fetch failure so the
        engine can abort the round without emitting deletes.
        """
        ...


_REGISTRY: dict[str, ProviderAdapter] = {}


def register_adapter(adapter: ProviderAdapter) -> None:
    """Register an adapter instance under its provider name."""
    _REGISTRY[adapter.provider] = adapter


def get_adapter(provider: str) -> ProviderAdapter:
    """Return the registered adapter for a provider.

    Raises:
        ConfigError: No adapter registered for the provider.
    """
    adapter = _REGISTRY.get(provider)
    if adapter is None:
        raise ConfigError(f"No adapter registered for provider '{provider}'")
    return adapter


def registered_providers() -> list[str]:
    """Names of all registered providers (drives default topic derivation)."""
    return sorted(_REGISTRY)
