"""Aliyun NAT Gateway adapter: DescribeNatGateways + SNAT/DNAT enrichment.

Same fetching discipline as the other Vpc modules: config-driven region
scope, page_number pagination, raise-on-failure. Per-NAT DescribeSnatTableEntries
/ DescribeForwardTableEntries supply the SNAT/DNAT entries; each entry list
carries a content hash (same pattern as security-group rules) so unchanged
entries keep the resource_version stable.

Field codes align with the CMDB model aliyun_nat_gateway (nat_type / spec /
snat_entries / snat_hash / dnat_entries / dnat_hash / eip_ids / vpc_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_vpc20160428 import models as vpc_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_vpc_client, fetch
from cloudsync.normalize.hashing import compute_rules_hash
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_vpc20160428.client import Client as VpcClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.nat_gateway")

RESOURCE_TYPE = "aliyun_nat_gateway"
API_NAME = "DescribeNatGateways"
PAGE_SIZE = 50
DISCOVERY_REGION = "cn-hangzhou"


def _normalize_snat_entry(item: dict[str, Any]) -> dict[str, Any]:
    """One DescribeSnatTableEntries item -> deterministic snake_case dict."""
    entry = {
        "snat_entry_id": item.get("SnatEntryId"),
        "snat_entry_name": item.get("SnatEntryName"),
        "source_cidr": item.get("SourceCIDR") or None,
        "source_vswitch_id": item.get("SourceVSwitchId") or None,
        # API returns comma-joined IPs; split + sort for a stable hash
        "snat_ips": sorted(
            ip.strip() for ip in (item.get("SnatIp") or "").split(",") if ip.strip()
        ),
        "status": item.get("Status"),
    }
    return {k: v for k, v in entry.items() if v is not None}


def _normalize_dnat_entry(item: dict[str, Any]) -> dict[str, Any]:
    """One DescribeForwardTableEntries item -> deterministic snake_case dict."""
    entry = {
        "dnat_entry_id": item.get("ForwardEntryId"),
        "dnat_entry_name": item.get("ForwardEntryName"),
        "public_ip": item.get("ExternalIp"),
        "public_port": item.get("ExternalPort"),
        "private_ip": item.get("InternalIp"),
        "private_port": item.get("InternalPort"),
        "protocol": item.get("IpProtocol"),
        "status": item.get("Status"),
    }
    return {k: v for k, v in entry.items() if v is not None}


def map_nat_gateway(
    raw: dict[str, Any],
    account_id: str,
    snat_entries: list[dict[str, Any]] | None = None,
    dnat_entries: list[dict[str, Any]] | None = None,
) -> NormalizedResource:
    """Map one DescribeNatGateways item (+ SNAT/DNAT entries) to NormalizedResource.

    NAT belongs to VPC; parent_provider_id points to the VpcId so the consumer
    can rebuild NAT -> VPC belongs_to edges. Bound EIP allocation ids ride
    along as eip_ids for NAT -> EIP relates_to edge rebuilding.
    """
    raw_tags = {
        t.get("TagKey", ""): t.get("TagValue", "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("TagKey")
    }
    snat_entries = sorted(
        snat_entries or [],
        key=lambda e: (e.get("source_cidr") or "", e.get("source_vswitch_id") or "",
                       ",".join(e.get("snat_ips") or [])),
    )
    dnat_entries = sorted(
        dnat_entries or [],
        key=lambda e: (e.get("public_ip") or "", str(e.get("public_port") or ""),
                       e.get("private_ip") or "", str(e.get("private_port") or ""),
                       e.get("protocol") or ""),
    )
    # IpLists 携带绑定的 EIP AllocationId（DescribeNatGateways 直接返回）
    eip_ids = sorted({
        ip.get("AllocationId")
        for ip in (raw.get("IpLists") or {}).get("IpList") or []
        if ip.get("AllocationId")
    })
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "nat_type": raw.get("NatType"),
        "spec": raw.get("Spec") or raw.get("NatGatewaySpec"),
        "vpc_id": raw.get("VpcId") or None,
        "eip_ids": eip_ids or None,
        "snat_entries": snat_entries or None,
        "dnat_entries": dnat_entries or None,
        # 条目内容哈希（同安全组 rules_hash 模式），条目不变则整体哈希稳定
        "snat_hash": compute_rules_hash(snat_entries) if snat_entries else None,
        "dnat_hash": compute_rules_hash(dnat_entries) if dnat_entries else None,
        "network_type": raw.get("NetworkType"),
        "instance_charge_type": raw.get("InstanceChargeType"),
        "creation_time": raw.get("CreationTime"),
        "expired_time": raw.get("ExpiredTime"),
        "description": raw.get("Description") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("NatGatewayId", ""),
        cloud_account=account_id,
        name=raw.get("Name") or "",
        region=raw.get("RegionId") or "",
        zone="",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aliyun_vpc" if vpc_id else None,
    )


async def _list_snat_entries(
    account: AccountConfig, client: VpcClient, snat_table_ids: list[str]
) -> list[dict[str, Any]]:
    """All SNAT entries of the NAT's tables (usually one table per NAT)."""
    entries: list[dict[str, Any]] = []
    for table_id in snat_table_ids:
        page = 1
        collected = 0
        while True:
            request = vpc_models.DescribeSnatTableEntriesRequest(
                snat_table_id=table_id, page_number=page, page_size=PAGE_SIZE,
            )
            response = await fetch(
                lambda req=request: client.describe_snat_table_entries(req),
                account=account,
                resource_type=RESOURCE_TYPE,
                api="DescribeSnatTableEntries",
            )
            body = response.body.to_map()
            items = (body.get("SnatTableEntries") or {}).get("SnatTableEntry") or []
            entries.extend(_normalize_snat_entry(i) for i in items)
            collected += len(items)
            total = body.get("TotalCount") or 0
            if collected >= total or not items:
                break
            page += 1
    return entries


async def _list_dnat_entries(
    account: AccountConfig, client: VpcClient, forward_table_ids: list[str]
) -> list[dict[str, Any]]:
    """All DNAT (forward) entries of the NAT's tables."""
    entries: list[dict[str, Any]] = []
    for table_id in forward_table_ids:
        page = 1
        collected = 0
        while True:
            request = vpc_models.DescribeForwardTableEntriesRequest(
                forward_table_id=table_id, page_number=page, page_size=PAGE_SIZE,
            )
            response = await fetch(
                lambda req=request: client.describe_forward_table_entries(req),
                account=account,
                resource_type=RESOURCE_TYPE,
                api="DescribeForwardTableEntries",
            )
            body = response.body.to_map()
            items = (body.get("ForwardTableEntries") or {}).get("ForwardTableEntry") or []
            entries.extend(_normalize_dnat_entry(i) for i in items)
            collected += len(items)
            total = body.get("TotalCount") or 0
            if collected >= total or not items:
                break
            page += 1
    return entries


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
    """Paginate DescribeNatGateways for one region; enrich with SNAT/DNAT."""
    page = 1
    collected = 0
    while True:
        request = vpc_models.DescribeNatGatewaysRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_nat_gateways(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("NatGateways") or {}).get("NatGateway") or []
        for item in items:
            snat_table_ids = (item.get("SnatTableIds") or {}).get("SnatTableId") or []
            forward_table_ids = (
                (item.get("ForwardTableIds") or {}).get("ForwardTableId") or []
            )
            snat_entries = (
                await _list_snat_entries(account, client, snat_table_ids)
                if snat_table_ids else []
            )
            dnat_entries = (
                await _list_dnat_entries(account, client, forward_table_ids)
                if forward_table_ids else []
            )
            yield map_nat_gateway(item, account.account_id, snat_entries, dnat_entries)
        collected += len(items)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_nat_gateway(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all NAT gateways of the account across its region scope."""
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
    logger.info("NAT gateway fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
