"""Aliyun disk adapter: DescribeDisks across configured regions.

Same fetching discipline as the ECS module (same product SDK): config-driven
region scope, page_number pagination, raise-on-failure. Field codes align
with the CMDB model aliyun_disk (category / size_gb / is_system /
performance_level / encrypted / charge_type / expired_at / instance_id).
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

logger = logging.getLogger("cloudsync.adapters.aliyun.disk")

RESOURCE_TYPE = "aliyun_disk"
API_NAME = "DescribeDisks"
PAGE_SIZE = 100  # DescribeDisks upper bound
DISCOVERY_REGION = "cn-hangzhou"

# API DiskChargeType -> model enum value (charge_type options: prepaid / postpaid)
_CHARGE_TYPE_MAP = {"PrePaid": "prepaid", "PostPaid": "postpaid"}


def map_disk(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeDisks item (to_map dict) to NormalizedResource.

    Disk is owned by the cloud account (belongs_to 账号归属); the attached
    ECS instance rides along as instance_id so the consumer can rebuild
    disk -> ECS relates_to edges.
    """
    charge_type = raw.get("DiskChargeType")
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "category": raw.get("Category"),
        "size_gb": raw.get("Size"),
        "is_system": raw.get("Type") == "system",
        "performance_level": raw.get("PerformanceLevel") or None,
        "encrypted": raw.get("Encrypted"),
        "charge_type": _CHARGE_TYPE_MAP.get(charge_type or ""),
        # 后付费无到期概念（阿里云返回 2999-12-31 占位值），仅预付费采集到期时间
        "expired_at": raw.get("ExpiredTime") if charge_type == "PrePaid" else None,
        "instance_id": raw.get("InstanceId") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("DiskId", ""),
        cloud_account=account_id,
        name=raw.get("DiskName") or "",
        region=raw.get("RegionId") or "",
        zone=raw.get("ZoneId") or "",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=account_id,
        parent_resource_type="aliyun_account",
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
    """Paginate DescribeDisks for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = ecs_models.DescribeDisksRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_disks(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Disks") or {}).get("Disk") or []
        for item in items:
            yield map_disk(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_disk(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all disks of the account across its region scope."""
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
    logger.info("Disk fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
