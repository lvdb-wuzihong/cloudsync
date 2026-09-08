"""Aliyun RDS adapter: DescribeDBInstances + per-instance enrichment.

The list API omits storage size and network endpoints; enrichment follows
the design doc N+1 pattern: DescribeDBInstanceAttribute supplies storage /
private connection / vswitch, DescribeDBInstanceNetInfo is the authoritative
source for the public address (main APIs never return it), and
DescribeDBProxy splits the database-proxy endpoint into its intranet /
internet addresses (NetType InnerString / OuterString) — it is only called
when NetInfo carries a Proxy entry, i.e. the proxy is actually enabled.
Fetching discipline identical to the other aliyun modules: config-driven
region scope, page_number pagination, raise-on-failure.

Field codes align with the CMDB model aliyun_rds (engine / engine_version /
instance_class / storage_gb / private/public/proxy connection strings /
port / charge_type / expired_at / vswitch_id).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_rds20140815 import models as rds_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_rds_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.normalize.tags import normalize_tags
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from alibabacloud_rds20140815.client import Client as RdsClient

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.rds")

RESOURCE_TYPE = "aliyun_rds"
API_NAME = "DescribeDBInstances"
PAGE_SIZE = 100  # DescribeDBInstances upper bound
DISCOVERY_REGION = "cn-hangzhou"

# API PayType -> model enum value (charge_type options: prepaid / postpaid)
_CHARGE_TYPE_MAP = {"Prepaid": "prepaid", "Postpaid": "postpaid"}


def _safe_int(value: Any) -> int | None:
    """Storage/port may arrive as strings; coerce defensively."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_attribute(attribute: dict[str, Any]) -> dict[str, Any]:
    """DescribeDBInstanceAttribute body -> flat instance dict (first item)."""
    items = (attribute.get("Items") or {}).get("DBInstanceAttribute") or []
    return items[0] if items else {}


def _extract_endpoints(
    net_info: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """DescribeDBInstanceNetInfo -> (端点 dict, 是否开通数据库代理)。

    IPType 枚举：Private / Public / Proxy；Proxy 条目存在 = 实例已开通
    数据库代理（触发 DescribeDBProxy 细化代理内/外网地址）。
    """
    endpoints: dict[str, Any] = {}
    has_proxy = False
    for item in (net_info.get("DBInstanceNetInfos") or {}).get("DBInstanceNetInfo") or []:
        ip_type = item.get("IPType")
        if ip_type == "Private":
            endpoints["private"] = item.get("ConnectionString")
            endpoints["private_port"] = _safe_int(item.get("Port"))
        elif ip_type == "Public":
            endpoints["public"] = item.get("ConnectionString")
        elif ip_type == "Proxy":
            has_proxy = True
    return endpoints, has_proxy


def map_rds(
    raw: dict[str, Any],
    account_id: str,
    attribute: dict[str, Any] | None = None,
    endpoints: dict[str, Any] | None = None,
) -> NormalizedResource:
    """Map one DescribeDBInstances item (+ enrichment) to NormalizedResource.

    RDS belongs to its VSwitch; parent_provider_id points to the VSwitchId so
    the consumer can rebuild RDS -> VSwitch belongs_to edges.

    endpoints keys: private / private_port / public / proxy / proxy_public。
    内外网分列（公网覆盖语义已废弃——内网地址不再被公网顶掉），代理地址
    内/外网分列，未开启时不落（不硬塞）。
    """
    attribute = attribute or {}
    raw_tags = {
        (t.get("Key") or t.get("TagKey") or ""): (t.get("Value") or t.get("TagValue") or "")
        for t in (raw.get("Tags") or {}).get("Tag", [])
        if t.get("Key") or t.get("TagKey")
    }
    pay_type = raw.get("PayType")
    vswitch_id = attribute.get("VSwitchId") or raw.get("VSwitchId") or None

    endpoints = endpoints or {}
    # NetInfo 的 Private 条目比 Attribute 更权威（含 port 拆分）
    private_conn = endpoints.get("private") or attribute.get("ConnectionString") or None
    private_port = endpoints.get("private_port") or _safe_int(attribute.get("Port"))

    attributes = {
        # 字段 code 对齐 CMDB 模型定义（内外网分列，公网覆盖语义已废弃）
        "engine": raw.get("Engine"),
        "engine_version": raw.get("EngineVersion"),
        "instance_class": raw.get("DBInstanceClass"),
        "storage_gb": _safe_int(attribute.get("DBInstanceStorage")),
        "private_connection_string": private_conn,
        "public_connection_string": endpoints.get("public"),
        # 代理地址内外网分列（DescribeDBProxy NetType InnerString/OuterString）
        "proxy_endpoint": endpoints.get("proxy"),
        "proxy_public_endpoint": endpoints.get("proxy_public"),
        "port": private_port,
        "charge_type": _CHARGE_TYPE_MAP.get(pay_type or ""),
        # 后付费无到期概念（阿里云返回 2999-12-31 占位值），仅预付费采集到期时间
        "expired_at": raw.get("ExpireTime") if pay_type == "Prepaid" else None,
        "vswitch_id": vswitch_id,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("DBInstanceId", ""),
        cloud_account=account_id,
        name=raw.get("DBInstanceDescription") or "",
        region=raw.get("RegionId") or "",
        zone=raw.get("ZoneId") or "",
        status=normalize_status(raw.get("DBInstanceStatus")),
        attributes=attributes,
        cloud_tags=normalize_tags(raw_tags),
        parent_provider_id=vswitch_id or None,
        parent_resource_type="aliyun_vswitch" if vswitch_id else None,
    )


async def _fetch_attribute(
    account: AccountConfig, client: RdsClient, db_instance_id: str
) -> dict[str, Any]:
    """DescribeDBInstanceAttribute for one instance (storage / conn / vswitch)."""
    response = await fetch(
        lambda: client.describe_dbinstance_attribute(
            rds_models.DescribeDBInstanceAttributeRequest(dbinstance_id=db_instance_id)
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeDBInstanceAttribute",
    )
    return _extract_attribute(response.body.to_map())


async def _fetch_net_info(
    account: AccountConfig, client: RdsClient, db_instance_id: str
) -> tuple[dict[str, Any], bool]:
    """DescribeDBInstanceNetInfo -> (端点 dict, 是否开通数据库代理)。"""
    response = await fetch(
        lambda: client.describe_dbinstance_net_info(
            rds_models.DescribeDBInstanceNetInfoRequest(dbinstance_id=db_instance_id)
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeDBInstanceNetInfo",
    )
    return _extract_endpoints(response.body.to_map())


async def _fetch_proxy(
    account: AccountConfig, client: RdsClient, db_instance_id: str
) -> dict[str, Any]:
    """DescribeDBProxy -> 代理内/外网连接地址（仅开通代理的实例调用）。

    DBProxyConnectStringItems 条目按 NetType 区分：InnerString（内网）/
    OuterString（外网）；DBProxyServiceStatus != Active 视为未开通返回空。
    """
    response = await fetch(
        lambda: client.describe_dbproxy(
            rds_models.DescribeDBProxyRequest(dbinstance_id=db_instance_id)
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeDBProxy",
    )
    body = response.body.to_map()
    endpoints: dict[str, Any] = {}
    if (body.get("DBProxyServiceStatus") or "") != "Active":
        return endpoints
    for item in body.get("DBProxyConnectStringItems") or []:
        net = item.get("DBProxyConnectStringNetType")
        if net == "InnerString":
            endpoints["proxy"] = item.get("DBProxyConnectString")
        elif net == "OuterString":
            endpoints["proxy_public"] = item.get("DBProxyConnectString")
    return endpoints


async def _discover_regions(account: AccountConfig, client: RdsClient) -> list[str]:
    """All account regions when accounts.yaml leaves the scope empty."""
    response = await fetch(
        lambda: client.describe_regions(rds_models.DescribeRegionsRequest()),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="DescribeRegions",
    )
    body = response.body.to_map()
    # RDS returns Regions.RDSRegion with zone-level entries; dedupe by RegionId
    return sorted({
        r["RegionId"]
        for r in (body.get("Regions") or {}).get("RDSRegion") or []
        if r.get("RegionId")
    })


async def _list_region(
    account: AccountConfig, client: RdsClient, region: str
) -> AsyncIterator[NormalizedResource]:
    """Paginate DescribeDBInstances for one region; enrich per instance."""
    page = 1
    collected = 0
    while True:
        request = rds_models.DescribeDBInstancesRequest(
            region_id=region, page_number=page, page_size=PAGE_SIZE
        )
        response = await fetch(
            lambda req=request: client.describe_dbinstances(req),
            account=account,
            resource_type=RESOURCE_TYPE,
            api=API_NAME,
        )
        body = response.body.to_map()
        items = (body.get("Items") or {}).get("DBInstance") or []
        for item in items:
            db_instance_id = item.get("DBInstanceId") or ""
            attribute = (
                await _fetch_attribute(account, client, db_instance_id)
                if db_instance_id else {}
            )
            endpoints: dict[str, Any] = {}
            if db_instance_id:
                endpoints, has_proxy = await _fetch_net_info(account, client, db_instance_id)
                if has_proxy:
                    # 代理已开通：DescribeDBProxy 细化内/外网代理地址
                    endpoints |= await _fetch_proxy(account, client, db_instance_id)
            yield map_rds(item, account.account_id, attribute, endpoints)
        collected += len(items)
        total = body.get("TotalRecordCount") or 0
        if collected >= total or not items:
            break
        page += 1


async def list_rds(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all RDS instances of the account across its region scope."""
    started = time.perf_counter()
    regions = list(account.regions) or await _discover_regions(
        account, build_rds_client(account, DISCOVERY_REGION)
    )
    count = 0
    for region in regions:
        client = build_rds_client(account, region)
        async for resource in _list_region(account, client, region):
            count += 1
            yield resource
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("RDS fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
