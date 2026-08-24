"""GCP persistent disk adapter: DisksClient.aggregated_list.

Disk is a zonal resource; aggregated_list covers the whole project in one
call (scope keys "zones/{zone}"), region scope comes from accounts.yaml via
the zone→region map (empty = all zones). Fetching rules identical to the
other adapters: raise on any failure so the engine aborts the round without
emitting deletes.

provider_id uses "{zone}/{name}": disk names are only unique per zone (same
collision lesson as gcp_subnet). The users URLs carry zone+instance name,
which the consumer parses to rebuild gcp_disk → gcp_compute "挂载于" edges.

Field codes align with the CMDB model gcp_disk (disk_type / size_gb /
encrypted / users). Proto field names verified against the SDK wheel:
the type field is exposed as ``type_`` (trailing underscore).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_disks_client,
    build_zones_client,
    fetch,
    last_segment,
    project_of,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.disk")

RESOURCE_TYPE = "gcp_disk"
PAGE_SIZE = 500  # AggregatedList upper bound


def _is_encrypted(disk: Any) -> bool:
    """Encryption key presence (CMEK/CSEK); unset proto messages yield empty strs."""
    for attr in (
        "disk_encryption_key",
        "source_image_encryption_key",
        "source_snapshot_encryption_key",
    ):
        enc = getattr(disk, attr, None)
        if enc is not None and (
            getattr(enc, "kms_key_name", "") or getattr(enc, "sha256", "")
        ):
            return True
    return False


def _parse_users(disk: Any) -> list[dict[str, str]]:
    """users = attached instance URLs; keep zone+name for edge matching."""
    entries: list[dict[str, str]] = []
    for url in getattr(disk, "users", None) or []:
        parts = (url or "").split("/")
        name = last_segment(url)
        if not name:
            continue
        zone = parts[parts.index("zones") + 1] if "zones" in parts else ""
        entries.append({"name": name, "zone": zone})
    entries.sort(key=lambda e: (e["zone"], e["name"]))
    return entries


def map_disk(disk: Any, account_id: str, region: str) -> NormalizedResource:
    """Map one Disk (proto message) to NormalizedResource."""
    zone = last_segment(getattr(disk, "zone", "") or "")
    name = getattr(disk, "name", "") or ""
    disk_type = last_segment(getattr(disk, "type_", "") or "")
    size_gb = getattr(disk, "size_gb", 0) or 0
    users = _parse_users(disk)

    attributes = {
        "disk_type": disk_type or None,
        "size_gb": int(size_gb) if size_gb else None,
        "encrypted": _is_encrypted(disk),
        "users": users or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        # zone-scoped name uniqueness (cross-zone same names must not collide)
        provider_id=f"{zone}/{name}" if zone else name,
        cloud_account=account_id,
        name=name,
        region=region,
        zone=zone,
        status=normalize_status(getattr(disk, "status", "") or ""),
        attributes=attributes,
        cloud_tags=dict(getattr(disk, "labels", None) or {}),
        parent_provider_id=account_id,
        parent_resource_type="gcp_account",
    )


async def _discover_zone_region(account: AccountConfig) -> dict[str, str]:
    """All UP zones -> region (disk carries zone URL only)."""
    client = build_zones_client(account)
    pager = await fetch(
        lambda: client.list({"project": project_of(account)}),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="ZonesClient.list",
    )
    return {
        zone.name: last_segment(zone.region)
        for zone in pager
        if zone.name and zone.status == "UP"
    }


async def list_disk(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all disks of the project within the configured regions."""
    started = time.perf_counter()
    zone_region = await _discover_zone_region(account)
    wanted: set[str] | None = set(account.regions) if account.regions else None

    client = build_disks_client(account)
    pager = await fetch(
        lambda: client.aggregated_list(
            {"project": project_of(account), "max_results": PAGE_SIZE},
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DisksClient.aggregated_list",
    )
    count = 0
    for scope_key, scoped in pager:
        if not scope_key.startswith("zones/"):
            continue
        zone = scope_key.removeprefix("zones/")
        region = zone_region.get(zone, "")
        if wanted is not None and region not in wanted:
            continue  # outside the configured region scope
        for disk in getattr(scoped, "disks", None) or []:
            count += 1
            yield map_disk(disk, account.account_id, region)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Disk fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
