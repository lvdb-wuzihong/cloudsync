"""Aliyun Redis (R-KVStore) adapter: DescribeInstances across regions.

Unlike RDS, the Redis list API already returns connection domain / port /
capacity / vswitch inline, so no per-instance enrichment is needed. Fetching
discipline identical to the other aliyun modules: config-driven region
scope, page_number pagination, raise-on-failure.

Field codes align with the CMDB model aliyun_redis (engine_version /
instance_class / capacity_mb / connection_string / port / vswitch_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_r_kvstore20150101 import models as redis_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_redis_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_r_kvstore20150101.client import Client as RedisClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.redis")

RESOURCE_TYPE = "aliyun_redis"
API_NAME = "DescribeInstances"
PAGE_SIZE = 50  # DescribeInstances (R-KVStore) upper bound
DISCOVERY_REGION = "cn-hangzhou"


def _safe_int(value: Any) -> int | None:
    """Capacity/port may arrive as strings; coerce defensively."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def map_redis(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeInstances item (to_map dict) to NormalizedResource.

    Redis belongs to its VSwitch; parent_provider_id points to the VSwitchId
    so the consumer can rebuild Redis -> VSwitch belongs_to edges.
    """
    raw_tags = {
        (t.get("Key") or t.get("TagKey") or ""): (t.get("Value") or t.get("TagValue") or "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("Key") or t.get("TagKey")
    }
    vswitch_id = raw.get("VSwitchId") or None
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "engine_version": raw.get("EngineVersion"),
        "instance_class": raw.get("InstanceClass"),
        "capacity_mb": _safe_int(raw.get("Capacity")),
        "connection_string": raw.get("ConnectionDomain") or None,
        "port": _safe_int(raw.get("Port")),
        "vswitch_id": vswitch_id,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("InstanceId", ""),
        cloud_account=account_id,
        name=raw.get("InstanceName") or "",
        region=raw.get("RegionId") or "",
        zone=raw.get("ZoneId") or "",
        status=normalize_status(raw.get("InstanceStatus")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vswitch_id or None,
        parent_resource_type="aliyun_vswitch" if vswitch_id else None,
    )


async def _discover_regions(account: AccountConfig, client: RedisClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty.

    R-KVStore returns Regions.KVStoreRegion with zone-level entries;
    dedupe by RegionId.
    """
    response = await fetch(
        lambda: client.describe_regions(redis_models.DescribeRegionsRequest()),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeRegions",
    )
    body = response.body.to_map()
    return sorted({
        r["RegionId"]
        for r in (body.get("Regions") or {}).get("KVStoreRegion") or []
        if r.get("RegionId")
    })


async def _list_region(
    account: AccountConfig, client: RedisClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeInstances for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = redis_models.DescribeInstancesRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_instances(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Instances") or {}).get("KVStoreInstance") or []
        for item in items:
            yield map_redis(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_redis(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all Redis instances of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_redis_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_redis_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Redis fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
