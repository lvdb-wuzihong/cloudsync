"""Aliyun NLB adapter: ListLoadBalancers + listener/server-group enrichment.

NLB entry form is a DNS name (design decision: aliyun_clb / aliyun_nlb stay
separate models; dns_record -> NLB matches by hostname). Fetching discipline
identical to the other aliyun modules: config-driven region scope, token
pagination, raise-on-failure. Backend ECS IDs are resolved through the
listener -> server group -> servers chain so the consumer can rebuild
NLB -> ECS relates_to edges.

Field codes align with the CMDB model aliyun_nlb (dns_name / address_type /
zone_mappings / server_groups / vpc_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_nlb20220430 import models as nlb_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_nlb_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_nlb20220430.client import Client as NlbClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.nlb")

RESOURCE_TYPE = "aliyun_nlb"
API_NAME = "ListLoadBalancers"
PAGE_SIZE = 50  # max_results for the NLB List* APIs
DISCOVERY_REGION = "cn-hangzhou"


def _normalize_zone_mappings(raw_zones: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """ZoneMappings -> deterministic snake_case dicts (stable hash)."""
    zones = []
    for z in raw_zones or []:
        addresses = [
            {k: v for k, v in {
                "private_ipv4": a.get("PrivateIPv4Address"),
                "public_ip": a.get("PublicIpAddress"),
                "eip_allocation_id": a.get("AllocationId"),
            }.items() if v}
            for a in z.get("LoadBalancerAddresses") or []
        ]
        addresses.sort(key=lambda a: (a.get("private_ipv4") or "", a.get("public_ip") or ""))
        zones.append({
            "zone_id": z.get("ZoneId"),
            "vswitch_id": z.get("VSwitchId"),
            "addresses": addresses,
        })
    zones.sort(key=lambda z: z.get("zone_id") or "")
    return zones


def _normalize_server_groups(
    listeners: list[dict[str, Any]] | None,
    server_group_meta: dict[str, dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Group listeners by server group id; deterministic order by group id."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in listeners or []:
        sg_id = item.get("ServerGroupId")
        if not sg_id:
            continue
        grouped.setdefault(sg_id, []).append(
            {"port": item.get("ListenerPort"), "protocol": item.get("ListenerProtocol")}
        )
    entries = []
    for sg_id in sorted(grouped):
        meta = (server_group_meta or {}).get(sg_id) or {}
        listener_list = sorted(
            grouped[sg_id], key=lambda ln: (ln.get("port") or 0, ln.get("protocol") or "")
        )
        entries.append({
            "server_group_id": sg_id,
            "server_group_name": meta.get("ServerGroupName"),
            "server_group_type": meta.get("ServerGroupType"),
            "listeners": listener_list,
        })
    return entries


def map_nlb(
    raw: dict[str, Any],
    account_id: str,
    listeners: list[dict[str, Any]] | None = None,
    server_group_meta: dict[str, dict[str, Any]] | None = None,
    backend_ecs_ids: list[str] | None = None,
) -> NormalizedResource:
    """Map one ListLoadBalancers item (+ listener chain) to NormalizedResource.

    NLB belongs to VPC; parent_provider_id points to the VpcId so the consumer
    can rebuild NLB -> VPC belongs_to edges. Backend ECS IDs ride along as
    internal metadata (_backend_ecs_ids, underscore prefix = not rendered).
    """
    raw_tags = {
        t.get("Key", ""): t.get("Value", "")
        for t in raw.get("Tags") or []
        if t.get("Key")
    }
    # API AddressType is capitalized (Internet / Intranet); enum expects lowercase
    address_type = (raw.get("AddressType") or "").lower() or None
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "dns_name": raw.get("DNSName") or raw.get("DnsName"),
        "address_type": address_type,
        "zone_mappings": _normalize_zone_mappings(raw.get("ZoneMappings")) or None,
        "server_groups": _normalize_server_groups(listeners, server_group_meta) or None,
        "vpc_id": raw.get("VpcId") or None,
    }
    backend_ids = sorted({i for i in backend_ecs_ids or [] if i})
    if backend_ids:
        attributes["_backend_ecs_ids"] = backend_ids
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("LoadBalancerId", ""),
        cloud_account=account_id,
        name=raw.get("LoadBalancerName") or "",
        region=raw.get("RegionId") or "",
        zone="",
        status=normalize_status(raw.get("LoadBalancerStatus")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vpc_id or None,
        parent_resource_type="aliyun_vpc" if vpc_id else None,
    )


async def _list_listeners(
    account: AccountConfig, client: NlbClient, lb_id: str
) -> list[dict[str, Any]]:
    """All listeners of one NLB (token pagination); carries ServerGroupId."""
    listeners: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        request = nlb_models.ListListenersRequest(
            load_balancer_ids=[lb_id], max_results=PAGE_SIZE, next_token=token,
        )
        response = await fetch(
            lambda req=request: client.list_listeners(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="ListListeners",
        )
        body = response.body.to_map()
        listeners.extend(body.get("Listeners") or [])
        token = body.get("NextToken")
        if not token:
            break
    return listeners


async def _list_server_groups(
    account: AccountConfig, client: NlbClient
) -> list[dict[str, Any]]:
    """All server groups of the region (name/type metadata for display)."""
    groups: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        request = nlb_models.ListServerGroupsRequest(
            max_results=PAGE_SIZE, next_token=token,
        )
        response = await fetch(
            lambda req=request: client.list_server_groups(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="ListServerGroups",
        )
        body = response.body.to_map()
        groups.extend(body.get("ServerGroups") or [])
        token = body.get("NextToken")
        if not token:
            break
    return groups


async def _list_server_group_servers(
    account: AccountConfig, client: NlbClient, server_group_id: str
) -> list[dict[str, Any]]:
    """All backend servers of one server group (token pagination)."""
    servers: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        request = nlb_models.ListServerGroupServersRequest(
            server_group_id=server_group_id, max_results=PAGE_SIZE, next_token=token,
        )
        response = await fetch(
            lambda req=request: client.list_server_group_servers(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api="ListServerGroupServers",
        )
        body = response.body.to_map()
        servers.extend(body.get("Servers") or [])
        token = body.get("NextToken")
        if not token:
            break
    return servers


async def _discover_regions(account: AccountConfig, client: NlbClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(nlb_models.DescribeRegionsRequest()),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeRegions",
    )
    body = response.body.to_map()
    return [
        r["RegionId"]
        for r in body.get("Regions") or []
        if r.get("RegionId")
    ]


async def _list_region(
    account: AccountConfig, client: NlbClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Fetch one region: LBs, then the listener -> server group chain."""
    lbs: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        request = nlb_models.ListLoadBalancersRequest(
            region_id=region, max_results=PAGE_SIZE, next_token=token,
        )
        response = await fetch(
            lambda req=request: client.list_load_balancers(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        lbs.extend(body.get("LoadBalancers") or [])
        token = body.get("NextToken")
        if not token:
            break

    sg_meta = {
        sg["ServerGroupId"]: sg
        for sg in await _list_server_groups(account, client)
        if sg.get("ServerGroupId")
    }
    # Shared across LBs: several listeners/LBs may reference the same group
    sg_backend_cache: dict[str, list[str]] = {}

    for item in lbs:
        lb_id = item.get("LoadBalancerId") or ""
        listeners = await _list_listeners(account, client, lb_id) if lb_id else []
        backend_ecs_ids: list[str] = []
        sg_ids = {ln.get("ServerGroupId") for ln in listeners if ln.get("ServerGroupId")}
        for sg_id in sg_ids:
            if sg_id not in sg_backend_cache:
                servers = await _list_server_group_servers(account, client, sg_id)
                sg_backend_cache[sg_id] = [
                    s.get("ServerId") for s in servers
                    if s.get("ServerType") == "Ecs" and s.get("ServerId")
                ]
            backend_ecs_ids.extend(sg_backend_cache[sg_id])
        yield map_nlb(item, account.account_id, listeners, sg_meta, backend_ecs_ids)


async def list_nlb(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all NLBs of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_nlb_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_nlb_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("NLB fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
