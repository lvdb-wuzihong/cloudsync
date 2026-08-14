"""Aliyun EIP adapter: DescribeEipAddresses across configured regions.

Same fetching discipline as the VPC module: config-driven region scope,
page_number pagination, raise-on-failure (no partial sets for the diff).
Field codes align with the CMDB model aliyun_eip (ip_address / bandwidth /
charge_type / bind_instance_type / bind_instance_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_vpc20160428 import models as vpc_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_vpc_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_vpc20160428.client import Client as VpcClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.eip")

RESOURCE_TYPE = "aliyun_eip"
API_NAME = "DescribeEipAddresses"
PAGE_SIZE = 50  # DescribeEipAddresses page size
DISCOVERY_REGION = "cn-hangzhou"


def _safe_int(value: Any) -> int | None:
    """Bandwidth comes back as a string ("5"); coerce defensively."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def map_eip(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeEipAddresses item (to_map dict) to NormalizedResource.

    Bind info (InstanceType/InstanceId) is carried as attributes so the
    consumer can rebuild EIP -> ECS relates_to edges; EIP itself is a
    network root with no belongs_to parent.
    """
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "ip_address": raw.get("IpAddress"),
        "bandwidth": _safe_int(raw.get("Bandwidth")),
        "charge_type": raw.get("ChargeType"),
        "bind_instance_type": raw.get("InstanceType") or None,
        "bind_instance_id": raw.get("InstanceId") or None,
        "internet_charge_type": raw.get("InternetChargeType"),
        "allocation_time": raw.get("AllocationTime"),
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("AllocationId", ""),
        cloud_account=account_id,
        name=raw.get("Name") or "",
        region=raw.get("RegionId") or "",
        zone="",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
    )


async def _discover_regions(account: AccountConfig, client: VpcClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(vpc_models.DescribeRegionsRequest()),
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
    account: AccountConfig, client: VpcClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeEipAddresses for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = vpc_models.DescribeEipAddressesRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_eip_addresses(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("EipAddresses") or {}).get("EipAddress") or []
        for item in items:
            yield map_eip(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_eip(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all EIPs of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_vpc_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_vpc_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("EIP fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
