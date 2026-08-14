"""Aliyun SecurityGroup adapter: DescribeSecurityGroups + per-group rules.

Same fetching discipline: config-driven region scope, page_number pagination,
raise-on-failure. Design doc section 5.3: each group's rules are normalized
and hashed into attributes (rules + rules_hash) so unchanged rule sets keep
the whole content hash stable and the consumer skips the update.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_ecs20140526 import models as ecs_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_ecs_client, fetch
from cloudsync.normalize.hashing import compute_rules_hash
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
RULES_API_NAME = "DescribeSecurityGroupAttribute"
PAGE_SIZE = 50  # DescribeSecurityGroups upper bound
RULES_PAGE_SIZE = 1000  # DescribeSecurityGroupAttribute upper bound
DISCOVERY_REGION = "cn-hangzhou"


def _normalize_rule(raw: dict[str, Any]) -> dict[str, Any]:
    """One permission entry -> cross-vendor rule dict (snake_case codes)."""
    rule = {
        "direction": raw.get("Direction"),
        "ip_protocol": raw.get("IpProtocol"),
        "port_range": raw.get("PortRange"),
        "source_cidr_ip": raw.get("SourceCidrIp"),
        "source_group_id": raw.get("SourceGroupId"),
        "dest_cidr_ip": raw.get("DestCidrIp"),
        "dest_group_id": raw.get("DestGroupId"),
        "policy": raw.get("Policy"),
        "priority": raw.get("Priority"),
        "description": raw.get("Description"),
        "create_time": raw.get("CreateTime"),
    }
    return {k: v for k, v in rule.items() if v is not None}


def _sort_key(rule: dict[str, Any]) -> str:
    """Deterministic ordering for the rules list (stable content hash)."""
    return json.dumps(rule, sort_keys=True, ensure_ascii=False, default=str)


async def _list_rules(
    account: AccountConfig, client: EcsClient, region: str, group_id: str
) -> list[dict[str, Any]]:
    """Paginate DescribeSecurityGroupAttribute; raise on any failure."""
    rules: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        request = ecs_models.DescribeSecurityGroupAttributeRequest(
            region_id=region,
            security_group_id=group_id,
            direction="all",
            max_results=RULES_PAGE_SIZE,
            next_token=next_token,
        )
        response = await fetch(
            lambda req=request: client.describe_security_group_attribute(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=RULES_API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Permissions") or {}).get("Permission") or []
        rules.extend(_normalize_rule(item) for item in items)
        next_token = body.get("NextToken")
        if not next_token:
            break
    rules.sort(key=_sort_key)
    return rules


def map_security_group(
    raw: dict[str, Any], account_id: str, rules: list[dict[str, Any]] | None = None
) -> NormalizedResource:
    """Map one DescribeSecurityGroups item + its rules to NormalizedResource.

    SecurityGroup belongs to VPC; parent_provider_id points to the VpcId
    so the consumer can rebuild SecurityGroup -> VPC belongs_to edges.
    """
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    vpc_id = raw.get("VpcId") or ""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（sg_type / vpc_id）
        "sg_type": raw.get("SecurityGroupType"),
        "description": raw.get("Description"),
        "creation_time": raw.get("CreationTime"),
        "resource_group_id": raw.get("ResourceGroupId"),
        "available_instance_amount": raw.get("AvailableInstanceAmount"),
        "ecs_count": raw.get("EcsCount"),
        "vpc_id": vpc_id or None,
    }
    if rules is not None:
        attributes["rules"] = rules
        attributes["rules_hash"] = compute_rules_hash(rules)
    attributes = {k: v for k, v in attributes.items() if v is not None}

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
    """Paginate DescribeSecurityGroups; fetch rules per group; raise on failure."""
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
            group_id = item.get("SecurityGroupId") or ""
            rules = (
                await _list_rules(account, client, region, group_id)
                if group_id
                else None
            )
            yield map_security_group(item, account.account_id, rules)
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
