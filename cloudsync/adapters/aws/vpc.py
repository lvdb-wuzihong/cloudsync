"""AWS VPC adapter: DescribeVpcs across configured regions.

Same fetching discipline as the other adapters: config-driven region scope,
raise-on-failure. DescribeVpcs has no paginator (single full-list call per
region, verified against the botocore wheel data). VPCs have a real state
(available/pending); the display name comes from the Name tag (AWS VPC has
no native name, presets appendix A2).

Field codes align with the CMDB model aws_vpc (cidr_block / is_default).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import (
    PROVIDER,
    build_client,
    fetch,
    normalized_tags,
    region_scope,
    tag_name,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.vpc")

RESOURCE_TYPE = "aws_vpc"
API_NAME = "describe_vpcs"


def map_vpc(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one DescribeVpcs item to NormalizedResource."""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（跨云同构：aliyun_vpc/gcp_vpc 同 code）
        "cidr_block": raw.get("CidrBlock") or None,
        "is_default": raw.get("IsDefault") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("VpcId", ""),
        cloud_account=account_id,
        name=tag_name(raw.get("Tags")) or raw.get("VpcId", ""),
        region=region,
        zone="",
        status=normalize_status(raw.get("State") or ""),
        attributes=attributes,
        cloud_tags=normalized_tags(raw.get("Tags")),
    )


async def list_vpc(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield VPCs of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "ec2", region)
        response = await fetch(
            lambda: client.describe_vpcs(),
            account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
        )
        count = 0
        for raw in response.get("Vpcs", []):
            count += 1
            yield map_vpc(raw, account.account_id, region)
        logger.info("VPC fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
