"""Synthetic provider IDs for resources without a native cloud ID.

Placeholder module: concrete rules follow cmdb-model-presets.md appendix B #19
(e.g. gcp_firewall = "fw:{project}:{vpc}"). To be completed once the presets
document is available; adapters must never invent ad-hoc formats meanwhile.
"""

from __future__ import annotations


def synthetic_id(resource_type: str, *parts: str) -> str:
    """Build a synthetic provider ID from typed parts.

    Args:
        resource_type: CMDB model code owning the ID scheme.
        *parts: Stable identity parts in the scheme-defined order.

    Returns:
        Synthetic ID string, e.g. "fw:{project}:{vpc}" for gcp_firewall.

    Raises:
        NotImplementedError: The scheme for this resource_type is not defined yet.
    """
    # TODO(P2/P3): populate schemes from cmdb-model-presets.md appendix B #19.
    raise NotImplementedError(f"Synthetic ID scheme not defined for {resource_type}")
