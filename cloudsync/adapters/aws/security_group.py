"""AWS security group adapter: DescribeSecurityGroups across regions.

Rules come inline with the group (IpPermissions / IpPermissionsEgress), so no
per-group enrichment call is needed (unlike aliyun's separate attribute API).
Each IpRange / Ipv6Range / UserIdGroupPair of a permission expands to one
normalized rule entry, snake_case codes aligned cross-vendor with the aliyun
adapter (direction / ip_protocol / source_cidr_ip ...); the sorted list feeds
compute_rules_hash so unchanged rule sets keep the resource_version stable
(design doc section 5.3).

Field codes align with the CMDB model aws_security_group (rules / rules_hash /
description / vpc_id); status is None (no lifecycle concept, never fabricated).
"""

from __future__ import annotations

import json
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
from cloudsync.normalize.hashing import compute_rules_hash
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.security_group")

RESOURCE_TYPE = "aws_security_group"
API_NAME = "describe_security_groups"
PAGE_SIZE = 100


def _expand_permission(
    raw: dict[str, Any], direction: str,
) -> list[dict[str, Any]]:
    """One IpPermission -> one rule dict per range/group (cross-vendor codes)."""
    rules: list[dict[str, Any]] = []
    cidr_key = "dest_cidr_ip" if direction == "egress" else "source_cidr_ip"
    group_key = "dest_group_id" if direction == "egress" else "source_group_id"
    base = {
        "direction": direction,
        "ip_protocol": raw.get("IpProtocol"),
        "from_port": raw.get("FromPort"),
        "to_port": raw.get("ToPort"),
    }
    for rng in raw.get("IpRanges") or []:
        rules.append({
            **base, cidr_key: rng.get("CidrIp"),
            "description": rng.get("Description"),
        })
    for rng in raw.get("Ipv6Ranges") or []:
        rules.append({
            **base, cidr_key: rng.get("CidrIpv6"),
            "description": rng.get("Description"),
        })
    for pair in raw.get("UserIdGroupPairs") or []:
        rules.append({
            **base, group_key: pair.get("GroupId"),
            "description": raw.get("IpRanges", [{}])[0].get("Description"),
        })
    if not rules:
        # all-protocol / all-targets permission (e.g. protocol "-1" with no ranges)
        rules.append(dict(base))
    return [{k: v for k, v in r.items() if v is not None} for r in rules]


def normalize_rules(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """IpPermissions + IpPermissionsEgress -> sorted cross-vendor rule list."""
    rules: list[dict[str, Any]] = []
    for p in raw.get("IpPermissions") or []:
        rules.extend(_expand_permission(p, "ingress"))
    for p in raw.get("IpPermissionsEgress") or []:
        rules.extend(_expand_permission(p, "egress"))
    rules.sort(key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False, default=str))
    return rules


def map_security_group(
    raw: dict[str, Any], account_id: str, region: str,
) -> NormalizedResource:
    """Map one DescribeSecurityGroups item to NormalizedResource.

    SecurityGroup belongs to VPC; parent_provider_id points to the VpcId
    so the consumer can rebuild SecurityGroup -> VPC belongs_to edges.
    """
    vpc_id = raw.get("VpcId") or ""
    rules = normalize_rules(raw)
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（对齐 aliyun_security_group 同 code）
        "rules": rules,
        "rules_hash": compute_rules_hash(rules),
        "description": raw.get("Description"),
        "vpc_id": vpc_id or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("GroupId", ""),
        cloud_account=account_id,
        name=raw.get("GroupName") or "",
        region=region,
        zone="",
        status=None,  # 无生命周期状态：不硬塞，落库 NULL（不适用）
        attributes=attributes,
        cloud_tags=normalized_tags(raw.get("Tags")),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aws_vpc" if vpc_id else None,
    )


async def list_security_group(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield security groups of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "ec2", region)
        next_token: str | None = None
        count = 0
        while True:
            kwargs = {"MaxResults": PAGE_SIZE}
            if next_token:
                kwargs["NextToken"] = next_token
            response = await fetch(
                lambda kw=kwargs: client.describe_security_groups(**kw),
                account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
            )
            for raw in response.get("SecurityGroups", []):
                count += 1
                yield map_security_group(raw, account.account_id, region)
            next_token = response.get("NextToken")
            if not next_token:
                break
        logger.info("SG fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
