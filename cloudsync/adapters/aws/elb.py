"""AWS ELB (Classic Load Balancer) adapter: DescribeLoadBalancers.

Classic ELB is regional; pagination is Marker/PageSize -> NextMarker
(wheel-verified). provider_id uses "{region}/{name}" — classic LB names are
only unique per region+account (same collision discipline as gcp_subnet /
gcp_disk). Classic ELB exposes no status field, so status stays None
(never fabricated).

Field codes align with the CMDB model aws_elb (dns_name / scheme / listeners /
vpc_id). Backend instance ids go into the _backend_instance_ids internal key
for the consumer to rebuild aws_elb -> aws_ec2 "负载均衡后端" edges.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import PROVIDER, build_client, fetch, region_scope
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.elb")

RESOURCE_TYPE = "aws_elb"
API_NAME = "describe_load_balancers"
PAGE_SIZE = 400  # DescribeLoadBalancers PageSize upper bound


def normalize_listeners(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """ListenerDescriptions -> [{protocol, load_balancer_port, instance_port}] sorted."""
    listeners = [
        {
            "protocol": (ld.get("Listener") or {}).get("Protocol"),
            "load_balancer_port": (ld.get("Listener") or {}).get("LoadBalancerPort"),
            "instance_port": (ld.get("Listener") or {}).get("InstancePort"),
        }
        for ld in raw.get("ListenerDescriptions") or []
    ]
    listeners = [
        {k: v for k, v in l.items() if v is not None} for l in listeners
    ]
    listeners.sort(key=lambda l: (l.get("load_balancer_port") or 0, l.get("protocol") or ""))
    return listeners


def map_elb(raw: dict[str, Any], account_id: str, region: str) -> NormalizedResource:
    """Map one LoadBalancerDescription to NormalizedResource."""
    name = raw.get("LoadBalancerName", "")
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（dns_name 对齐 aliyun_nlb 同 code）
        "dns_name": raw.get("DNSName") or None,
        "scheme": raw.get("Scheme") or None,
        "listeners": normalize_listeners(raw) or None,
        "vpc_id": raw.get("VPCId") or None,
        # 下划线内部键：消费端建 aws_elb -> aws_ec2 边用，前端不渲染
        "_backend_instance_ids": sorted(
            i.get("InstanceId", "") for i in raw.get("Instances") or []
        ) or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VPCId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        # 名字仅 region 内唯一，provider_id 带 region 前缀防跨域撞键
        provider_id=f"{region}/{name}",
        cloud_account=account_id,
        name=name,
        region=region,
        zone="",
        status=None,  # Classic ELB 无状态字段，不硬塞
        attributes=attributes,
        cloud_tags={},  # classic ELB tags need a separate DescribeTags call; not a model field
        parent_provider_id=vpc_id or None,
        parent_resource_type="aws_vpc" if vpc_id else None,
    )


async def list_elb(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield classic load balancers of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "elb", region)
        marker: str | None = None
        count = 0
        while True:
            kwargs: dict[str, Any] = {"PageSize": PAGE_SIZE}
            if marker:
                kwargs["Marker"] = marker
            response = await fetch(
                lambda kw=kwargs: client.describe_load_balancers(**kw),
                account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
            )
            for raw in response.get("LoadBalancerDescriptions", []):
                count += 1
                yield map_elb(raw, account.account_id, region)
            marker = response.get("NextMarker")
            if not marker:
                break
        logger.info("ELB fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
