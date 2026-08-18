"""Aliyun AMQP (RabbitMQ) adapter: ListInstances + per-instance GetInstance.

The list API's response surface is incomplete in practice (VswitchIds etc.
may be absent), so each instance is enriched with GetInstance — the
authoritative per-instance source (same N+1 pattern as RDS net info;
AMQP instance counts are small). The AMQP product SDK has no
region-discovery API and ListInstances carries no region_id parameter
(region scope comes from the client endpoint), so when accounts.yaml leaves
the scope empty we borrow the ECS DescribeRegions discovery. Pagination is
NextToken-based. SupportNode / port are not returned by any AMQP API and
stay unset (never fabricated).

Field codes align with the CMDB model aliyun_amqp (instance_type /
max_queues / max_tps / endpoint / charge_type / expired_at / vswitch_id).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from alibabacloud_amqp_open20191212 import models as amqp_models
from alibabacloud_ecs20140526 import models as ecs_models

from cloudsync.adapters.aliyun.client import (
    PROVIDER,
    build_amqp_client,
    build_ecs_client,
    fetch,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_amqp_open20191212.client import Client as AmqpClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.amqp")

RESOURCE_TYPE = "aliyun_amqp"
API_NAME = "ListInstances"
PAGE_SIZE = 100  # ListInstances max_results
DISCOVERY_REGION = "cn-hangzhou"

# API InstanceType -> model enum value。
# 官方值域：PROFESSIONAL / ENTERPRISE / VIP（铂金版）/ SERVERLESS；
# serverless 需模型枚举扩展 option 后才有意义，未扩展前 consumer 渲染为空。
# 注意：Edition 是 serverless 部署架构（shared/dedicated），不是实例系列，不作回退。
_INSTANCE_TYPE_MAP = {
    "professional": "professional",
    "enterprise": "enterprise",
    "vip": "platinum",
    "serverless": "serverless",
}

# API OrderType -> model enum value (charge_type options: prepaid / postpaid)
_CHARGE_TYPE_MAP = {
    "pre_paid": "prepaid",
    "post_paid": "postpaid",
    "subscription": "prepaid",
    "payasyougo": "postpaid",
}


def _ms_to_iso(value: Any) -> str | None:
    """ExpireTime arrives as a millisecond epoch; model field is date."""
    if value in (None, "", -1):
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _safe_int(value: Any) -> int | None:
    """Quota fields may arrive as strings; coerce defensively."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def map_amqp(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one ListInstances item (to_map dict) to NormalizedResource.

    AMQP belongs to its VSwitch; parent_provider_id points to the first
    VSwitchId so the consumer can rebuild AMQP -> VSwitch belongs_to edges.
    Items carry no RegionId, so the region comes from the caller's endpoint.
    """
    instance_type = _INSTANCE_TYPE_MAP.get((raw.get("InstanceType") or "").lower())
    charge_type = _CHARGE_TYPE_MAP.get((raw.get("OrderType") or "").lower())
    vswitch_ids = sorted(raw.get("VswitchIds") or [])
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "instance_type": instance_type,
        "max_queues": _safe_int(raw.get("MaxQueue")),
        "max_tps": _safe_int(raw.get("MaxTps")),
        "endpoint": raw.get("PrivateEndpoint") or raw.get("PublicEndpoint") or None,
        "charge_type": charge_type,
        # ExpireTime 是毫秒时间戳，转 ISO 日期；仅预付费采集
        "expired_at": _ms_to_iso(raw.get("ExpireTime"))
        if charge_type == "prepaid" else None,
        # serverless 实例 API 不返回 VswitchIds（仅创建时填 privateLink 才有），空即真相
        "vswitch_id": vswitch_ids[0] if vswitch_ids else None,
        # support_node / port: AMQP API 不返回，不臆造，留空待人工补充
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vswitch_id = vswitch_ids[0] if vswitch_ids else ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("InstanceId", ""),
        cloud_account=account_id,
        name=raw.get("InstanceName") or "",
        region=region,
        zone="",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags={},  # AMQP tagging not exposed on ListInstances
        parent_provider_id=vswitch_id or None,
        parent_resource_type="aliyun_vswitch" if vswitch_id else None,
    )


async def _discover_regions(account: AccountConfig) -> list[str]:
    """AMQP has no region API; borrow the ECS discovery (same account scope)."""
    client = build_ecs_client(account, DISCOVERY_REGION)
    response = await fetch(
        lambda: client.describe_regions(ecs_models.DescribeRegionsRequest()),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeRegions(ecs-fallback)",
    )
    body = response.body.to_map()
    return [
        r["RegionId"]
        for r in (body.get("Regions") or {}).get("Region") or []
        if r.get("RegionId")
    ]


async def _fetch_instance(
    account: AccountConfig, client: AmqpClient, instance_id: str
) -> dict[str, Any]:
    """GetInstance: authoritative per-instance detail (VswitchIds etc.)."""
    response = await fetch(
        lambda: client.get_instance(
            amqp_models.GetInstanceRequest(instance_id=instance_id)
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="GetInstance",
    )
    return response.body.to_map().get("Data") or {}


async def _list_region(
    account: AccountConfig, client: AmqpClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate ListInstances for one region (endpoint-bound); raise on failure."""
    token: str | None = None
    while True:
        request = amqp_models.ListInstancesRequest(
            max_results=PAGE_SIZE, next_token=token,
        )
        response = await fetch(
            lambda req=request: client.list_instances(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        data = body.get("Data") or {}
        items = data.get("Instances") or []
        for item in items:
            instance_id = item.get("InstanceId") or ""
            # 列表接口响应面不全（VswitchIds 等可能缺失），详情为准
            detail = (
                await _fetch_instance(account, client, instance_id)
                if instance_id else {}
            )
            yield map_amqp({**item, **detail}, account.account_id, region)
        token = data.get("NextToken")
        if not token or not items:
            break


async def list_amqp(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all AMQP instances of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(account)
    count = 0
    for region in regions:
        client = build_amqp_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("AMQP fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
