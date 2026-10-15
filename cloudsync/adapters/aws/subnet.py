"""AWS Subnet adapter: DescribeSubnets across configured regions.

Same fetching discipline as the other EC2 modules: config-driven region
scope, NextToken pagination (verified against the botocore wheel
paginators-1.json: limit MaxResults / token NextToken, result Subnets).
provider_id = SubnetId (globally unique, no region prefix needed).

Field codes align with the CMDB model aws_subnet (cidr_block / vpc_id /
available_ip_count); AZ goes into the zone column (subnets are AZ-scoped,
same discipline as aliyun_vswitch). Subnets have a real state
(available/pending).

Hierarchy: aws_subnet belongs_to aws_vpc (#72) and aws_ec2 hangs off the
subnet (#60 retargeted from aws_vpc in the same batch).
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger("cloudsync.adapters.aws.subnet")

RESOURCE_TYPE = "aws_subnet"
API_NAME = "describe_subnets"
PAGE_SIZE = 100


def map_subnet(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one DescribeSubnets item to NormalizedResource."""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（跨云同构：aliyun_vswitch/gcp_subnet 同 code）
        "cidr_block": raw.get("CidrBlock") or None,
        "vpc_id": raw.get("VpcId") or None,
        "available_ip_count": raw.get("AvailableIpAddressCount"),
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("SubnetId", ""),
        cloud_account=account_id,
        name=tag_name(raw.get("Tags")) or raw.get("SubnetId", ""),
        region=region,
        zone=raw.get("AvailabilityZone") or "",
        status=normalize_status(raw.get("State") or ""),
        attributes=attributes,
        cloud_tags=normalized_tags(raw.get("Tags")),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aws_vpc" if vpc_id else None,
    )


async def list_subnet(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield subnets of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "ec2", region)
        next_token: str | None = None
        count = 0
        while True:
            kwargs = {"MaxResults": PAGE_SIZE}
            if next_token:
                kwargs["NextToken"] = next_token
            response = await fetch(
                lambda kw=kwargs: client.describe_subnets(**kw),
                account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
            )
            for raw in response.get("Subnets", []):
                count += 1
                yield map_subnet(raw, account.account_id, region)
            next_token = response.get("NextToken")
            if not next_token:
                break
        logger.info("Subnet fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
