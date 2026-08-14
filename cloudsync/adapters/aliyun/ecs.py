"""Aliyun ECS adapter: DescribeInstances across configured regions (P1).

Fetching rules (design doc section 5):
- regions come from accounts.yaml; empty means all regions via DescribeRegions;
- pagination by page_number/page_size until total_count is reached;
- any SDK failure raises (never yields a partial set) so the engine aborts
  the round without emitting deletes (design doc section 6).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_ecs20140526 import models as ecs_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_ecs_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_ecs20140526.client import Client as EcsClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.ecs")

RESOURCE_TYPE = "aliyun_ecs"
API_NAME = "DescribeInstances"
PAGE_SIZE = 100  # DescribeInstances upper bound
# Client config needs some region; DescribeRegions works from any endpoint.
DISCOVERY_REGION = "cn-hangzhou"


def _safe_div(value: int | None, divisor: int) -> int | None:
    """Safely divide, returning None if value is None."""
    if value is None:
        return None
    return value // divisor  # integer division, e.g. 16384 MB → 16 GB


def _extract_disk_size(raw: dict[str, Any]) -> int | None:
    """Extract system disk size (GB) from DescribeInstances DiskDeviceMappings."""
    mappings = (raw.get("DiskDeviceMappings") or {}).get("DiskDeviceMapping") or []
    for disk in mappings:
        if disk.get("Type") == "system":
            size = disk.get("Size")
            return int(size) if size else None
    # Fallback: first disk if no system disk marked
    if mappings:
        size = mappings[0].get("Size")
        return int(size) if size else None
    return None


def map_instance(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeInstances item (to_map dict) to NormalizedResource.

    Attribute keys are model field codes; common-layer fields (name/region/
    zone/status/tags) stay out of attributes (design doc section 4).
    """
    vpc_attrs = raw.get("VpcAttributes") or {}
    private_ips = (vpc_attrs.get("PrivateIpAddress") or {}).get("IpAddress") or []
    public_ips = (raw.get("PublicIpAddress") or {}).get("IpAddress") or []
    charge_type = raw.get("InstanceChargeType")
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    attributes = {
        "instance_class": raw.get("InstanceType"),
        "cpu": raw.get("Cpu"),
        "memory_gb": _safe_div(raw.get("Memory"), 1024),
        "os": raw.get("OSName"),
        "os_type": raw.get("OSType"),
        "host_name": raw.get("HostName"),
        "image_id": raw.get("ImageId"),
        "key_pair": raw.get("KeyPairName"),
        "charge_type": charge_type,
        "internet_charge_type": raw.get("InternetChargeType"),
        "disk_size_gb": _extract_disk_size(raw),
        "creation_time": raw.get("CreationTime"),
        # 后付费无到期概念（阿里云返回 2999-12-31 占位值），仅预付费采集到期时间
        "expired_at": raw.get("ExpiredTime") if charge_type == "PrePaid" else None,
        "vpc_id": vpc_attrs.get("VpcId"),
        "vswitch_id": vpc_attrs.get("VSwitchId"),
        "private_ip": private_ips[0] if private_ips else None,
        "public_ip": public_ips[0] if public_ips else None,
        "eip": (raw.get("EipAddress") or {}).get("IpAddress") or None,
        "security_group_ids": (raw.get("SecurityGroupIds") or {}).get("SecurityGroupId") or [],
    }
    # Drop unset fields so the content hash stays stable across API shapes
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vswitch_id = vpc_attrs.get("VSwitchId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("InstanceId", ""),
        cloud_account=account_id,
        name=raw.get("InstanceName") or "",
        region=raw.get("RegionId") or "",
        zone=raw.get("ZoneId") or "",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vswitch_id or None,
        parent_resource_type="aliyun_vswitch" if vswitch_id else None,
    )


async def _discover_regions(account: AccountConfig, client: EcsClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(ecs_models.DescribeRegionsRequest()),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeRegions",
    )
    body = response.body.to_map()
    return [
        r["RegionId"]
        for r in (body.get("Regions") or {}).get("Region") or []
        if r.get("RegionId")
    ]


async def _list_region(
    account: AccountConfig, client: EcsClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeInstances for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = ecs_models.DescribeInstancesRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_instances(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Instances") or {}).get("Instance") or []
        for item in items:
            yield map_instance(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_ecs(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all ECS instances of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_ecs_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_ecs_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("ECS fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
