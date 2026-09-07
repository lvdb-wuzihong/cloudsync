"""AWS EC2 adapter: DescribeInstances across configured regions.

Same fetching discipline as the other EC2 modules: config-driven region
scope, NextToken pagination, raise-on-failure. provider_id = InstanceId
(globally unique, no region prefix needed).

Field codes align with the CMDB model aws_ec2 (instance_type / os / spot /
private_ip / public_ip / creation_time / vpc_id / subnet_id / image_id);
os is derived from PlatformDetails (windows/linux), spot from
InstanceLifecycle="spot". The security group ids go into the _security_group_ids
internal key (underscore-prefixed, never rendered) for the consumer to rebuild
aws_ec2 -> aws_security_group "绑定安全组" edges.
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

logger = logging.getLogger("cloudsync.adapters.aws.ec2")

RESOURCE_TYPE = "aws_ec2"
API_NAME = "describe_instances"
PAGE_SIZE = 100


def _derive_os(raw: dict[str, Any]) -> str | None:
    """os from PlatformDetails ("Windows" / "Linux/UNIX") or Platform fallback."""
    details = (raw.get("PlatformDetails") or "").lower()
    if details.startswith("windows"):
        return "windows"
    if details.startswith("linux"):
        return "linux"
    platform = (raw.get("Platform") or "").lower()
    if platform.startswith("windows"):
        return "windows"
    return "linux" if raw.get("PlatformDetails") or raw.get("Platform") else None


def map_ec2(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one DescribeInstances item to NormalizedResource."""
    sg_ids = sorted(sg.get("GroupId", "") for sg in raw.get("SecurityGroups") or [])
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（跨云同构：aliyun_ecs/gcp_compute 同 code）
        "instance_type": raw.get("InstanceType") or None,
        "os": _derive_os(raw),
        "spot": raw.get("InstanceLifecycle") == "spot" or None,
        "private_ip": raw.get("PrivateIpAddress") or None,
        "public_ip": raw.get("PublicIpAddress") or None,
        "creation_time": raw.get("LaunchTime").isoformat()
        if raw.get("LaunchTime") else None,
        "vpc_id": raw.get("VpcId") or None,
        "subnet_id": raw.get("SubnetId") or None,
        "image_id": raw.get("ImageId") or None,
        # 下划线内部键：消费端建 aws_ec2 -> aws_security_group 边用，前端不渲染
        "_security_group_ids": sg_ids or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("InstanceId", ""),
        cloud_account=account_id,
        name=tag_name(raw.get("Tags")) or raw.get("InstanceId", ""),
        region=region,
        zone=(raw.get("Placement") or {}).get("AvailabilityZone") or "",
        status=normalize_status((raw.get("State") or {}).get("Name") or ""),
        attributes=attributes,
        cloud_tags=normalized_tags(raw.get("Tags")),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aws_vpc" if vpc_id else None,
    )


async def list_ec2(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield EC2 instances of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "ec2", region)
        next_token: str | None = None
        count = 0
        while True:
            kwargs = {"MaxResults": PAGE_SIZE}
            if next_token:
                kwargs["NextToken"] = next_token
            response = await fetch(
                lambda kw=kwargs: client.describe_instances(**kw),
                account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
            )
            for reservation in response.get("Reservations", []):
                for raw in reservation.get("Instances", []):
                    count += 1
                    yield map_ec2(raw, account.account_id, region)
            next_token = response.get("NextToken")
            if not next_token:
                break
        logger.info("EC2 fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
