"""Aliyun VPC adapter: DescribeVpcs across configured regions (P1).

Same fetching discipline as the ECS module: config-driven region scope,
page_number pagination, raise-on-failure (no partial sets for the diff).
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

logger = logging.getLogger("cloudsync.adapters.aliyun.vpc")

RESOURCE_TYPE = "aliyun_vpc"
API_NAME = "DescribeVpcs"
PAGE_SIZE = 50  # DescribeVpcs upper bound
DISCOVERY_REGION = "cn-hangzhou"


def map_vpc(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeVpcs item (to_map dict) to NormalizedResource.

    cidr_block follows the cross-vendor field code convention (design doc
    section 5.1); no parent since VPC is the account-level network root.
    """
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    attributes = {
        # cross-vendor aligned field codes
        "cidr_block": raw.get("CidrBlock"),
        "secondary_cidr_blocks": (raw.get("SecondaryCidrBlocks") or {})
        .get("SecondaryCidrBlock") or [],
        "vswitch_ids": (raw.get("VSwitchIds") or {}).get("VSwitchId") or [],
        "is_default": raw.get("IsDefault"),
        "description": raw.get("Description"),
        "creation_time": raw.get("CreationTime"),
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("VpcId", ""),
        cloud_account=account_id,
        name=raw.get("VpcName") or "",
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
    """Paginate DescribeVpcs for one region; raise on any failure."""
    page = 1
    collected = 0
    while True:
        request = vpc_models.DescribeVpcsRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_vpcs(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Vpcs") or {}).get("Vpc") or []
        for item in items:
            yield map_vpc(item, account.account_id)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_vpc(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all VPCs of the account across its region scope."""
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
    logger.info("VPC fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
