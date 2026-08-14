"""Aliyun SecurityGroup adapter: DescribeSecurityGroups across configured regions.

SecurityGroup is an ECS product resource; uses the ECS client (not VPC).
Same fetching discipline: config-driven region scope, page_number pagination,
raise-on-failure (no partial sets for the diff).
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

logger = logging.getLogger("cloudsync.adapters.aliyun.security_group")

RESOURCE_TYPE = "aliyun_security_group"
API_NAME = "DescribeSecurityGroups"
PAGE_SIZE = 50  # DescribeSecurityGroups upper bound
DISCOVERY_REGION = "cn-hangzhou"


def map_security_group(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeSecurityGroups item (to_map dict) to NormalizedResource.

    SecurityGroup belongs to VPC; parent_provider_id points to the VpcId
    so the consumer can rebuild SecurityGroup → VPC belongs_to edges.
    """
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    attributes = {
        "security_group_type": raw.get("SecurityGroupType"),
        "description": raw.get("Description"),
        "creation_time": raw.get("CreationTime"),
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("SecurityGroupId", ""),
        cloud_account=account_id,
        name=raw.get("SecurityGroupName") or "",
        region=raw.get("RegionId") or "",
        zone="",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aliyun_vpc" if vpc_id else None,
    )


async def _list_region(
    account: AccountConfig, client: EcsClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeSecurityGroups for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = ecs_models.DescribeSecurityGroupsRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_security_groups(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("SecurityGroups") or {}).get("SecurityGroup") or []
        for item in items:
            yield map_security_group(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_security_group(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all SecurityGroups of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions)
    if not regions:
        client = build_ecs_client(account, DISCOVERY_REGION)
        response = await fetch(
            lambda: client.describe_regions(ecs_models.DescribeRegionsRequest()),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="DescribeRegions",
        )
        body = response.body.to_map()
        regions = [
            r["RegionId"]
            for r in (body.get("Regions") or {}).get("Region") or []
            if r.get("RegionId")
        ]
    count = 0
    for region in regions:
        client = build_ecs_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("SecurityGroup fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
