"""GCP subnetwork adapter: SubnetworksClient.aggregated_list.

Subnetwork is a regional resource; aggregated_list covers the whole project
in one call (same pattern as InstancesClient.aggregated_list in compute.py),
scope keys are "regions/{region}" and get filtered against the accounts.yaml
region scope (empty = all). Fetching rules identical to the other adapters:
raise on any failure (never yield a partial set) so the engine aborts the
round without emitting deletes.

Field codes align with the CMDB model gcp_subnet (cidr_block /
secondary_ranges / private_google_access / vpc_id). Proto field names are
read defensively (getattr): message structure drifts across SDK versions and
must not take the whole round down.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_subnetworks_client,
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

logger = logging.getLogger("cloudsync.adapters.gcp.subnet")

RESOURCE_TYPE = "gcp_subnet"
PAGE_SIZE = 500  # AggregatedList upper bound


def map_subnet(
    subnetwork: Any, account_id: str, region: str,
) -> NormalizedResource:
    """Map one Subnetwork (proto message) to NormalizedResource.

    provider_id uses "{region}/{name}"：子网名只在 VPC 内唯一，跨地域同名
    （如各 region 的 default）会撞键互相覆盖导致反复 update；同时 GCP
    子资源只携带父资源名称，边匹配靠这个键联 gcp_compute.subnet_id。
    """
    # secondary_ip_ranges: GKE pod/service ranges; normalized snake_case and
    # sorted for a stable content hash
    secondary = [
        {
            "range_name": getattr(r, "range_name", "") or "",
            "ip_cidr_range": getattr(r, "ip_cidr_range", "") or "",
        }
        for r in (getattr(subnetwork, "secondary_ip_ranges", None) or [])
    ]
    secondary.sort(key=lambda e: (e["range_name"], e["ip_cidr_range"]))

    vpc_name = last_segment(getattr(subnetwork, "network", "") or "")
    attributes = {
        "cidr_block": getattr(subnetwork, "ip_cidr_range", "") or None,
        "secondary_ranges": secondary or None,
        "private_google_access": bool(
            getattr(subnetwork, "private_ip_google_access", False)
        ),
        "vpc_id": vpc_name or None,
    }
    # Drop unset fields so the content hash stays stable across API shapes
    attributes = {k: v for k, v in attributes.items() if v is not None}

    subnet_name = getattr(subnetwork, "name", "") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=f"{region}/{subnet_name}" if region else subnet_name,
        cloud_account=account_id,
        name=subnet_name,
        region=region,
        zone="",
        status=normalize_status("available"),  # no lifecycle; alive = running
        attributes=attributes,
        cloud_tags={},  # GCP subnetwork has no labels
        parent_provider_id=vpc_name or None,
        parent_resource_type="gcp_vpc" if vpc_name else None,
    )


async def _discover_region_map(account: AccountConfig) -> dict[str, str]:
    """All UP zones -> region; used to expand/validate the region scope."""
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


async def _fetch_scoped(
    account: AccountConfig, wanted_regions: set[str] | None,
) -> list[tuple[str, Any]]:
    """AggregatedList across the project, scoped to the configured regions."""
    client = build_subnetworks_client(account)
    pager = await fetch(
        lambda: client.aggregated_list(
            {"project": project_of(account), "max_results": PAGE_SIZE},
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="SubnetworksClient.aggregated_list",
    )
    results: list[tuple[str, Any]] = []
    for scope_key, scoped in pager:
        if not scope_key.startswith("regions/"):
            continue
        region = scope_key.removeprefix("regions/")
        if wanted_regions is not None and region not in wanted_regions:
            continue  # outside the configured region scope
        for subnetwork in scoped.subnetworks:
            results.append((region, subnetwork))
    return results


async def list_subnet(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all subnetworks of the project within the configured regions."""
    started = time.perf_counter()
    wanted: set[str] | None = None
    if account.regions:
        zone_regions = await _discover_region_map(account)
        wanted = {r for r in zone_regions.values() if r in set(account.regions)}

    items = await _fetch_scoped(account, wanted)
    count = 0
    for region, subnetwork in items:
        count += 1
        yield map_subnet(subnetwork, account.account_id, region)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Subnet fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
