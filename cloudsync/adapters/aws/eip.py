"""AWS EIP (Elastic IP) adapter: DescribeAddresses across configured regions.

Same fetching discipline as the EC2 modules (same service client): config-
driven region scope, raise-on-failure. DescribeAddresses has no paginator
(single full-list call per region, wheel-verified). provider_id = AllocationId.

Field codes align with the CMDB model aws_eip (ip_address / private_ip /
bind_instance_id); AWS EIP has no bandwidth/prepaid concept so no such field
is fabricated. The bind edge (aws_eip -> aws_ec2, kind=bind) is rebuilt by
the consumer from bind_instance_id.
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
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.eip")

RESOURCE_TYPE = "aws_eip"
API_NAME = "describe_addresses"


def map_eip(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one DescribeAddresses item to NormalizedResource.

    EIP is account-level (no VPC binding); parent_provider_id stays None.
    instance_id comes from the top-level InstanceId (present only when
    associated, wheel-verified shape).
    """
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（对齐 aliyun_eip 同 code）
        "ip_address": raw.get("PublicIp") or None,
        "private_ip": raw.get("PrivateIpAddress") or None,
        "bind_instance_id": raw.get("InstanceId") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("AllocationId", ""),
        cloud_account=account_id,
        name=raw.get("PublicIp") or raw.get("AllocationId", ""),
        region=region,
        zone="",
        status=None,  # EIP 无生命周期状态，不硬塞
        attributes=attributes,
        cloud_tags=normalized_tags(raw.get("Tags")),
    )


async def list_eip(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield EIPs of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "ec2", region)
        response = await fetch(
            lambda: client.describe_addresses(),
            account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
        )
        count = 0
        for raw in response.get("Addresses", []):
            count += 1
            yield map_eip(raw, account.account_id, region)
        logger.info("EIP fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
