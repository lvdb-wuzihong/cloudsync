"""Aliyun CLB adapter: DescribeLoadBalancers + per-LB attribute enrichment.

CLB (classic SLB) entry form is an IP address (design decision: aliyun_clb /
aliyun_nlb stay separate models). Fetching discipline identical to the other
aliyun modules: config-driven region scope, page_number pagination,
raise-on-failure. Per-LB DescribeLoadBalancerAttribute supplies listeners and
backend server IDs so the consumer can rebuild CLB -> ECS relates_to edges.

Field codes align with the CMDB model aliyun_clb (address / address_type /
spec / listeners / charge_type / vpc_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_slb20140515 import models as slb_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_slb_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_slb20140515.client import Client as SlbClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.clb")

RESOURCE_TYPE = "aliyun_clb"
API_NAME = "DescribeLoadBalancers"
ATTRIBUTE_API_NAME = "DescribeLoadBalancerAttribute"
PAGE_SIZE = 50  # DescribeLoadBalancers page size
DISCOVERY_REGION = "cn-hangzhou"

# API PayType -> model enum value (charge_type options: prepaid / postpaid)
_CHARGE_TYPE_MAP = {"PrePay": "prepaid", "PayOnDemand": "postpaid"}


def _normalize_listeners(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """ListenerPortsAndProtocol -> deterministic listener dicts (stable hash)."""
    listeners = [
        {"protocol": item.get("ListenerProtocol"), "port": item.get("ListenerPort")}
        for item in (raw.get("ListenerPortsAndProtocol") or {})
        .get("ListenerPortAndProtocol") or []
        if item.get("ListenerPort") is not None
    ]
    listeners.sort(key=lambda l: (l.get("port") or 0, l.get("protocol") or ""))
    return listeners


def map_clb(
    raw: dict[str, Any],
    account_id: str,
    attribute: dict[str, Any] | None = None,
) -> NormalizedResource:
    """Map one DescribeLoadBalancers item (+ attribute) to NormalizedResource.

    CLB belongs to VPC; parent_provider_id points to the VpcId so the consumer
    can rebuild CLB -> VPC belongs_to edges. Backend server IDs ride along as
    internal metadata (_backend_ecs_ids, underscore prefix = not rendered).
    """
    attribute = attribute or {}
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    backend_ecs_ids = sorted({
        item.get("ServerId")
        for item in (attribute.get("BackendServers") or {}).get("BackendServer") or []
        if item.get("ServerId")
    })
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "address": raw.get("Address"),
        "address_type": raw.get("AddressType"),
        "spec": raw.get("LoadBalancerSpec"),
        "charge_type": _CHARGE_TYPE_MAP.get(raw.get("PayType") or ""),
        "vpc_id": attribute.get("VpcId") or None,
        "listeners": _normalize_listeners(attribute),
    }
    if backend_ecs_ids:
        attributes["_backend_ecs_ids"] = backend_ecs_ids
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = attribute.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("LoadBalancerId", ""),
        cloud_account=account_id,
        name=raw.get("LoadBalancerName") or "",
        region=raw.get("RegionId") or "",
        zone="",
        status=normalize_status(raw.get("LoadBalancerStatus")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aliyun_vpc" if vpc_id else None,
    )


async def _fetch_attribute(
    account: AccountConfig, client: SlbClient, region: str, lb_id: str
) -> dict[str, Any]:
    """DescribeLoadBalancerAttribute for one LB (listeners + backend servers)."""
    response = await fetch(
        lambda: client.describe_load_balancer_attribute(
            slb_models.DescribeLoadBalancerAttributeRequest(
                region_id=region, load_balancer_id=lb_id,
            )
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api=ATTRIBUTE_API_NAME,
    )
    return response.body.to_map()


async def _discover_regions(account: AccountConfig, client: SlbClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(slb_models.DescribeRegionsRequest()),
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
    account: AccountConfig, client: SlbClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeLoadBalancers for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = slb_models.DescribeLoadBalancersRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_load_balancers(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("LoadBalancers") or {}).get("LoadBalancer") or []
        for item in items:
            lb_id = item.get("LoadBalancerId") or ""
            attribute = (
                await _fetch_attribute(account, client, region, lb_id)
                if lb_id
                else None
            )
            yield map_clb(item, account.account_id, attribute)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_clb(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all CLBs of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_slb_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_slb_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("CLB fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
