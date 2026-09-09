"""Aliyun Redis (R-KVStore) adapter: DescribeInstances across regions.

The list API returns connection domain / port / capacity / vswitch inline
(all intranet-scoped); the public connection address only appears in
DescribeDBInstanceNetInfo (IPType=Public, wordlist per official docs:
Public / Inner / Private), so it is fetched per instance as a best-effort
enhancement (failures degrade to no public endpoint without aborting the
round). Fetching discipline identical to the other aliyun modules:
config-driven region scope, page_number pagination, raise-on-failure.

Field codes align with the CMDB model aliyun_redis (engine_version /
instance_class / capacity_mb / connection_string / public_connection_string
/ port / vswitch_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_r_kvstore20150101 import models as redis_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_redis_client, fetch
from cloudsync.core.exceptions import AdapterError, RateLimitError
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


def map_redis(
    raw: dict[str, Any], account_id: str, public_connection_string: str | None = None
) -> NormalizedResource:
    """Map one DescribeInstances item (+ NetInfo enrichment) to NormalizedResource.

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
        # 内网带宽(MB/s)，列表 API 内联返回（DescribeIntranetAttribute 仅在需要
        # 突发带宽/带宽计费状态时才必要，暂不引入）
        "bandwidth": _safe_int(raw.get("Bandwidth")),
        "connection_string": raw.get("ConnectionDomain") or None,
        # 公网连接地址（DescribeDBInstanceNetInfo IPType=Public）；未开通公网不落
        "public_connection_string": public_connection_string,
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


async def _fetch_public_connection(
    account: AccountConfig, client: RedisClient, instance_id: str
) -> str | None:
    """DescribeDBInstanceNetInfo -> 公网连接地址（best-effort 增强字段）。

    列表 API 只返回内网 ConnectionDomain；公网地址仅在本 API 的
    IPType=Public 条目（词表 Public/Inner/Private，照官方文档）。未开通
    公网的实例无 Public 条目或直接报错：降级返回 None（记 WARNING），
    不拖垮整轮同步。
    """
    try:
        response = await fetch(
            lambda: client.describe_dbinstance_net_info(
                redis_models.DescribeDBInstanceNetInfoRequest(instance_id=instance_id)
            ),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="DescribeDBInstanceNetInfo",
        )
    except RateLimitError as exc:
        # 限流重试耗尽：跳过本实例公网增强，保住主数据（seen_ids 完整不误删）
        logger.warning(
            "DescribeDBInstanceNetInfo throttled, public endpoint skipped",
            extra={
                "provider": PROVIDER,
                "account": account.account_id,
                "resource_type": RESOURCE_TYPE,
                "instance_id": instance_id,
                "error_code": exc.error_code,
                "detail": exc.message,
            },
        )
        return None
    except AdapterError as exc:
        # 未开通公网/接口不支持等 API 错误：该实例视为无公网端点
        logger.warning(
            "DescribeDBInstanceNetInfo failed, public endpoint skipped",
            extra={
                "provider": PROVIDER,
                "account": account.account_id,
                "resource_type": RESOURCE_TYPE,
                "instance_id": instance_id,
                "error_code": exc.error_code,
                "detail": exc.message,
            },
        )
        return None
    body = response.body.to_map()
    public: str | None = None
    for item in (body.get("NetInfoItems") or {}).get("InstanceNetInfo") or []:
        if item.get("IPType") == "Public" and item.get("ConnectionString"):
            public = item["ConnectionString"]
            break
    # TEMP triage log: dump IPType list to verify public endpoint extraction.
    # Remove once public endpoint wiring is confirmed.
    logger.info(
        "Redis net info for public endpoint triage",
        extra={
            "provider": PROVIDER,
            "account": account.account_id,
            "resource_type": RESOURCE_TYPE,
            "instance_id": instance_id,
            "ip_types": [
                i.get("IPType")
                for i in (body.get("NetInfoItems") or {}).get("InstanceNetInfo") or []
            ],
            "public": public,
        },
    )
    return public


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
            # TEMP triage log: CMDB shows empty public endpoint for Redis;
            # dump raw keys to confirm whether the list API carries any public
            # connection field. Remove once field wiring is confirmed.
            logger.info(
                "Redis list item raw fields for endpoint triage",
                extra={
                    "provider": PROVIDER,
                    "account": account.account_id,
                    "resource_type": RESOURCE_TYPE,
                    "instance_id": item.get("InstanceId"),
                    "raw_keys": sorted(item.keys()),
                    "connection_domain": item.get("ConnectionDomain"),
                    "network_type": item.get("NetworkType"),
                },
            )
            instance_id = item.get("InstanceId") or ""
            # 公网地址不在列表 API：每实例查 NetInfo（best-effort）
            public = (
                await _fetch_public_connection(account, client, instance_id)
                if instance_id else None
            )
            yield map_redis(item, account.account_id, public)
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
