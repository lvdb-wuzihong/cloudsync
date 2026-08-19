"""Aliyun public DNS adapter: DescribeDomains + per-zone DescribeDomainRecords.

DNS is a global service; the accounts.yaml region scope does not apply (any
endpoint works, same pattern as OSS). Zone = public hosted domain; record
normalization follows the cross-vendor conventions (presets appendix B #19
and the DNS modeling decision):

- dns_record.name stores the FQDN ("@" -> bare zone, "*" -> "*.<zone>");
- provider_id is the cloud RecordId (same FQDN may carry many records);
- aliyun Line (解析线路) maps to policy_type simple/line + policy_key;
- the raw API item is preserved in the `raw` json field for audit.

PrivateZone (pvtz) is deliberately out of scope (presets: 用到再录).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_alidns20150109 import models as dns_models

from cloudsync.adapters.aliyun.client import PROVIDER, build_dns_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.dns")

ZONE_TYPE = "dns_zone"
RECORD_TYPE = "dns_record"
DOMAINS_API = "DescribeDomains"
RECORDS_API = "DescribeDomainRecords"
DOMAINS_PAGE_SIZE = 100  # DescribeDomains upper bound
RECORDS_PAGE_SIZE = 500  # DescribeDomainRecords upper bound
DISCOVERY_REGION = "cn-hangzhou"

# record_type enum options on the CMDB model; other API types stay in `raw`
# only (model enum renders blank, never fabricated)
_RECORD_TYPE_OPTIONS = {"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV"}


def fqdn(rr: str, zone: str) -> str:
    """Relative RR + zone -> FQDN (presets: @ 存裸域，泛解析存 *.example.com)."""
    if not rr or rr == "@":
        return zone
    if rr == "*":
        return f"*.{zone}"
    if rr == zone or rr.endswith(f".{zone}"):
        return rr  # already absolute
    return f"{rr}.{zone}"


def map_dns_zone(raw: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one DescribeDomains item to NormalizedResource."""
    domain = raw.get("DomainName") or ""
    # 付费实例才有到期时间；免费版两个键都缺席 -> None（不臆造）
    expire_at = raw.get("ExpireTime") or raw.get("InstanceEndTime") or None
    attributes = {
        "zone_type": "public",
        "expire_at": expire_at if expire_at and not str(expire_at).startswith("2999") else None,
        "record_count": raw.get("RecordCount"),
        "dns_servers": (raw.get("DnsServers") or {}).get("DnsServer") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=ZONE_TYPE,
        provider_id=domain,  # domain name is globally unique
        cloud_account=account_id,
        name=domain,
        region="",  # DNS is global
        zone="",
        status=normalize_status("available"),
        attributes=attributes,
        cloud_tags={},
    )


def map_dns_record(
    raw: dict[str, Any], zone_name: str, account_id: str,
) -> NormalizedResource:
    """Map one DescribeDomainRecords item to NormalizedResource."""
    rr = raw.get("RR") or ""
    line = raw.get("Line") or "default"
    record_type = raw.get("Type") or ""
    attributes = {
        "rr": rr,
        "record_type": record_type if record_type in _RECORD_TYPE_OPTIONS else None,
        "value": raw.get("Value"),
        "ttl": raw.get("TTL"),
        "priority": raw.get("Priority") or None,
        "record_status": (raw.get("Status") or "").lower() or None,
        # aliyun 线路解析：default=普通，其余=线路（policy_key 存线路名）
        "policy_type": "simple" if line == "default" else "line",
        "policy_key": line if line != "default" else None,
        "raw": raw,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RECORD_TYPE,
        provider_id=raw.get("RecordId", ""),
        cloud_account=account_id,
        name=fqdn(rr, zone_name),
        region="",
        zone="",
        status=normalize_status(raw.get("Status")),
        attributes=attributes,
        cloud_tags={},
        parent_provider_id=zone_name,
        parent_resource_type=ZONE_TYPE,
    )


async def _list_domains(account: AccountConfig) -> list[dict[str, Any]]:
    """All hosted domains of the account; raise on any failure."""
    client = build_dns_client(account, DISCOVERY_REGION)
    domains: list[dict[str, Any]] = []
    page = 1
    while True:
        request = dns_models.DescribeDomainsRequest(
            page_number=page, page_size=DOMAINS_PAGE_SIZE,
        )
        response = await fetch(
            lambda req=request: client.describe_domains(req),
            account=account,
            resource_type=ZONE_TYPE,
            api=DOMAINS_API,
        )
        body = response.body.to_map()
        items = (body.get("Domains") or {}).get("Domain") or []
        domains.extend(items)
        collected = len(domains)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1
    return domains


async def list_dns_zone(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all hosted domains (public zones) of the account."""
    started = time.perf_counter()
    count = 0
    for raw in await _list_domains(account):
        count += 1
        yield map_dns_zone(raw, account.account_id)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("DNS zone fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": ZONE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})


async def _list_zone_records(
    account: AccountConfig, zone_name: str,
) -> list[dict[str, Any]]:
    """All records of one domain; raise on any failure."""
    client = build_dns_client(account, DISCOVERY_REGION)
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        request = dns_models.DescribeDomainRecordsRequest(
            domain_name=zone_name, page_number=page, page_size=RECORDS_PAGE_SIZE,
        )
        response = await fetch(
            lambda req=request: client.describe_domain_records(req),
            account=account,
            resource_type=RECORD_TYPE,
            api=RECORDS_API,
        )
        body = response.body.to_map()
        items = (body.get("DomainRecords") or {}).get("Record") or []
        records.extend(items)
        collected = len(records)
        total = body.get("TotalCount") or 0
        if collected >= total or not items:
            break
        page += 1
    return records


async def list_dns_record(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all records of every hosted domain of the account."""
    started = time.perf_counter()
    count = 0
    for zone in await _list_domains(account):
        zone_name = zone.get("DomainName") or ""
        if not zone_name:
            continue
        for raw in await _list_zone_records(account, zone_name):
            count += 1
            yield map_dns_record(raw, zone_name, account.account_id)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("DNS record fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RECORD_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
