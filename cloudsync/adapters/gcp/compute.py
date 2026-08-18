"""Aliyun-style discipline applied to GCP: GCE instances via aggregated_list.

GCE is zonal; the accounts.yaml region scope is expanded to zones through
Zones.list (regions empty = all UP zones of the project). Instances come from
a single AggregatedList call (pager handles page tokens); results are then
scoped to the zone set before enrichment so a region-scoped account never
leaks instances from other regions.

Enrichment (N+1, cached): MachineTypes.get supplies cpu / memory_gb (the
instance resource only carries the machine type URL), and the boot disk
source image name yields a best-effort os label. Machine specs are cached per
(zone, machine type) since fleets typically share a handful of shapes.

Fetching rules identical to the other adapters: raise on any failure (never
yield a partial set) so the engine aborts the round without emitting deletes.

Field codes align with the CMDB model gcp_compute (machine_type / cpu /
memory_gb / os / private_ip / public_ip / disk_size_gb / spot / subnet_id /
vpc_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_instances_client,
    build_machine_types_client,
    build_zones_client,
    fetch,
    last_segment,
    project_of,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.compute")

RESOURCE_TYPE = "gcp_compute"
PAGE_SIZE = 500  # AggregatedList upper bound

# Best-effort os label from the boot disk source image name (GCE exposes no
# dedicated OS field); matched in order, first hit wins, else left unset.
_OS_KEYWORDS = (
    ("windows", "windows"),
    ("ubuntu", "ubuntu"),
    ("debian", "debian"),
    ("centos", "centos"),
    ("rhel", "rhel"),
    ("rocky", "rocky-linux"),
    ("almalinux", "almalinux"),
    ("sles", "sles"),
    ("cos-", "cos"),
)


def _guess_os(source_image: str | None) -> str | None:
    """Derive an os label from the boot disk source image URL/name."""
    if not source_image:
        return None
    image = last_segment(source_image).lower()
    for keyword, os_name in _OS_KEYWORDS:
        if keyword in image:
            return os_name
    return None


def map_compute(
    instance: Any,
    account_id: str,
    *,
    zone: str,
    region: str,
    machine_type: str | None = None,
    cpu: int | None = None,
    memory_gb: int | None = None,
    os_name: str | None = None,
) -> NormalizedResource:
    """Map one GCE Instance (proto message) to NormalizedResource.

    Attribute keys are model field codes; common-layer fields (name/region/
    zone/status/labels) stay out of attributes. machine_type is the baselined
    type name (e.g. "e2-medium"); cpu/memory_gb come from MachineTypes.get.
    """
    nic = instance.network_interfaces[0] if instance.network_interfaces else None
    private_ip = nic.network_i_p if nic else None
    public_ip = None
    subnet_id = None
    vpc_id = None
    if nic:
        for access_config in nic.access_configs:
            if access_config.nat_i_p:
                public_ip = access_config.nat_i_p
                break
        subnet_id = last_segment(nic.subnetwork) if nic.subnetwork else None
        vpc_id = last_segment(nic.network) if nic.network else None

    boot_image: str | None = None
    disk_total = 0
    for disk in instance.disks:
        if disk.disk_size_gb:
            disk_total += disk.disk_size_gb
        if disk.boot and disk.source:
            boot_image = disk.source

    scheduling = instance.scheduling
    spot = bool(scheduling.preemptible) or scheduling.provisioning_model == "SPOT"

    attributes = {
        "machine_type": machine_type,
        "cpu": cpu,
        "memory_gb": memory_gb,
        "os": os_name,
        "private_ip": private_ip,
        "public_ip": public_ip,
        "disk_size_gb": disk_total or None,
        "spot": spot,
        "subnet_id": subnet_id,
        "vpc_id": vpc_id,
    }
    # Drop unset fields so the content hash stays stable across API shapes
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=str(instance.id),
        cloud_account=account_id,
        name=instance.name or "",
        region=region,
        zone=zone,
        status=normalize_status(instance.status),
        attributes=attributes,
        cloud_tags=normalize_tags(dict(instance.labels)),
        parent_provider_id=subnet_id,
        parent_resource_type="gcp_subnet" if subnet_id else None,
    )


async def _discover_zones(account: AccountConfig) -> dict[str, str]:
    """All UP zones of the project -> {zone: region}; used when scope is empty."""
    client = build_zones_client(account)
    pager = await fetch(
        lambda: client.list(project=project_of(account)),
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
    account: AccountConfig, zone_regions: dict[str, str],
) -> list[tuple[str, str, Any]]:
    """AggregatedList across the project, scoped to the configured zones."""
    client = build_instances_client(account)
    # max_results 只在请求消息里（方法扁平 kwarg 仅 project），dict 形式传参
    pager = await fetch(
        lambda: client.aggregated_list(
            {"project": project_of(account), "max_results": PAGE_SIZE},
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="InstancesClient.aggregated_list",
    )
    results: list[tuple[str, str, Any]] = []
    for scope_key, scoped in pager:
        if not scope_key.startswith("zones/"):
            continue
        zone = scope_key.removeprefix("zones/")
        region = zone_regions.get(zone)
        if region is None:
            continue  # outside the configured region scope
        for instance in scoped.instances:
            results.append((zone, region, instance))
    return results


async def _enrich(
    account: AccountConfig, items: list[tuple[str, str, Any]],
) -> list[NormalizedResource]:
    """Machine-type specs (cached) + boot-disk OS guess per instance."""
    mt_client = build_machine_types_client(account)
    project = project_of(account)
    specs_cache: dict[tuple[str, str], tuple[int | None, int | None]] = {}
    resources: list[NormalizedResource] = []

    for zone, region, instance in items:
        mt_name = last_segment(instance.machine_type) if instance.machine_type else None
        cpu: int | None = None
        memory_gb: int | None = None
        if mt_name:
            key = (zone, mt_name)
            if key not in specs_cache:
                specs_cache[key] = await _machine_specs(mt_client, account, project, zone, mt_name)
            cpu, memory_gb = specs_cache[key]
        boot_image = next(
            (disk.source for disk in instance.disks if disk.boot and disk.source), None,
        )
        resources.append(map_compute(
            instance, account.account_id,
            zone=zone, region=region,
            machine_type=mt_name, cpu=cpu, memory_gb=memory_gb,
            os_name=_guess_os(boot_image),
        ))
    return resources


async def _machine_specs(
    client: Any, account: AccountConfig, project: str, zone: str, machine_type: str,
) -> tuple[int | None, int | None]:
    """(guest_cpus, memory_mb -> GB) for one machine type; (None, None) on miss."""
    try:
        mt = await fetch(
            lambda: client.get(project=project, zone=zone, machine_type=machine_type),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="MachineTypesClient.get",
        )
    except Exception:
        logger.warning("Machine type lookup failed, cpu/memory left unset",
                       extra={"provider": PROVIDER, "account": account.account_id,
                              "resource_type": RESOURCE_TYPE, "zone": zone,
                              "machine_type": machine_type})
        return None, None
    return mt.guest_cpus, round(mt.memory_mb / 1024, 2)


async def list_compute(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all GCE instances of the project within the configured regions."""
    started = time.perf_counter()
    if account.regions:
        zones = await _discover_zones(account)
        wanted = set(account.regions)
        zone_regions = {z: r for z, r in zones.items() if r in wanted}
    else:
        zone_regions = await _discover_zones(account)

    items = await _fetch_scoped(account, zone_regions)
    resources = await _enrich(account, items)
    for resource in resources:
        yield resource

    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("GCE fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": len(resources),
                       "duration_ms": round(duration_ms, 2)})
