"""Aliyun NAS adapter: DescribeFileSystems across configured regions.

NAS is its own product SDK; DescribeFileSystems returns mount targets inline,
so no per-instance enrichment call is needed. Fetching discipline identical
to the other aliyun modules: config-driven region scope, page_number
pagination, raise-on-failure.

Field codes align with the CMDB model aliyun_nas (protocol_type /
storage_type / used_size_gb / mount_targets / charge_type / vpc_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_nas20170626 import models as nas_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_nas_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_nas20170626.client import Client as NasClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.nas")

RESOURCE_TYPE = "aliyun_nas"
API_NAME = "DescribeFileSystems"
PAGE_SIZE = 100  # DescribeFileSystems upper bound
DISCOVERY_REGION = "cn-hangzhou"

# API ChargeType -> model enum value (charge_type options: prepaid / postpaid)
_CHARGE_TYPE_MAP = {"Subscription": "prepaid", "PayAsYouGo": "postpaid"}


def _normalize_mount_targets(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """MountTargets -> deterministic snake_case dicts (stable hash)."""
    targets = [
        {k: v for k, v in {
            "mount_target_domain": t.get("MountTargetDomain"),
            "vpc_id": t.get("VpcId") or None,
            "vswitch_id": t.get("VswId") or None,
            "access_group": t.get("AccessGroupName"),
            "network_type": t.get("NetworkType"),
            "status": t.get("Status"),
        }.items() if v is not None}
        for t in (raw.get("MountTargets") or {}).get("MountTarget") or []
    ]
    targets.sort(key=lambda t: t.get("mount_target_domain") or "")
    return targets


def map_nas(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeFileSystems item (to_map dict) to NormalizedResource.

    NAS is owned by the cloud account (belongs_to 账号归属); mount-target VPC
    ids ride along as internal metadata (_mount_vpc_ids, underscore prefix =
    not rendered) so the consumer can rebuild NAS -> VPC relates_to edges.
    """
    raw_tags = {
        (t.get("Key") or t.get("TagKey") or ""): (t.get("Value") or t.get("TagValue") or "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("Key") or t.get("TagKey")
    }
    mount_targets = _normalize_mount_targets(raw)
    mount_vpc_ids = sorted({t["vpc_id"] for t in mount_targets if t.get("vpc_id")})
    metered_size = raw.get("MeteredSize")
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "protocol_type": raw.get("ProtocolType"),
        "storage_type": raw.get("StorageType"),
        "used_size_gb": round(metered_size / (1024 ** 3), 2) if metered_size else None,
        "charge_type": _CHARGE_TYPE_MAP.get(raw.get("ChargeType") or ""),
        "mount_targets": mount_targets or None,
        "vpc_id": mount_vpc_ids[0] if mount_vpc_ids else None,
    }
    if mount_vpc_ids:
        attributes["_mount_vpc_ids"] = mount_vpc_ids
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("FileSystemId", ""),
        cloud_account=account_id,
        # NAS 实例无独立名称，以描述作为展示名
        name=raw.get("Description") or "",
        region=raw.get("RegionId") or "",
        zone=raw.get("ZoneId") or "",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=account_id,
        parent_resource_type="aliyun_account",
    )


async def _discover_regions(account: AccountConfig, client: NasClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(nas_models.DescribeRegionsRequest()),
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
    account: AccountConfig, client: NasClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeFileSystems for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = nas_models.DescribeFileSystemsRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_file_systems(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("FileSystems") or {}).get("FileSystem") or []
        for item in items:
            yield map_nas(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_nas(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all NAS file systems of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_nas_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_nas_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("NAS fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
