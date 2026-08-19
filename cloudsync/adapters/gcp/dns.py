"""GCP Cloud DNS adapter: ManagedZones + per-zone ResourceRecordSets.

DNS is a global service; the accounts.yaml region scope does not apply.
Cross-vendor conventions match the aliyun DNS adapter (presets appendix B
#19 and the DNS modeling decision):

- dns_record.name stores the FQDN (trailing dot stripped);
- GCP rrsets carry no id, so provider_id is synthesized
  {zone}:{fqdn}:{type}:{value} (one CMDB record per rrdata value);
- MX/SRV priority is embedded in rrdatas ("10 mail.example.com.") and split
  out; TXT values arrive quoted and get unquoted;
- SOA rrsets are zone-level system records and never become CIs.

Proto fields are read defensively (getattr): message structure drifts across
SDK versions and must not take the round down.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_dns_client,
    fetch,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.dns")

ZONE_TYPE = "dns_zone"
RECORD_TYPE = "dns_record"

# SOA is auto-created zone metadata, never a CI
_SKIP_TYPES = {"SOA"}

# record_type enum options on the CMDB model; others stay in `raw` only
_RECORD_TYPE_OPTIONS = {"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV"}


def _strip_dot(value: str) -> str:
    """GCP DNS names carry a trailing dot; CMDB stores bare FQDNs."""
    return (value or "").rstrip(".")


def _relative_rr(fqdn: str, zone: str) -> str:
    """FQDN -> relative RR ("@" for apex, "*" for wildcard, prefix otherwise)."""
    if fqdn == zone:
        return "@"
    if fqdn.endswith(f".{zone}"):
        return fqdn[: -(len(zone) + 1)]
    return fqdn


def _normalize_value(
    record_type: str, rrdata: str,
) -> tuple[str, int | None]:
    """rrdata -> (value, priority); type-specific shape normalization."""
    value = (rrdata or "").strip()
    if record_type in ("MX", "SRV"):
        parts = value.split(None, 1)
        try:
            priority = int(parts[0])
        except (ValueError, IndexError):
            priority = None
        rest = parts[1].strip() if len(parts) > 1 else ""
        return rest.rstrip("."), priority
    if record_type == "TXT" and len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1], None
    if record_type in ("CNAME", "NS"):
        return value.rstrip("."), None
    return value, None


def _zone_visibility(zone: Any) -> str:
    """public/private; the hand-written SDK exposes no property, the raw
    API field lives in _properties (GAPIC-style protos carry it directly)."""
    vis = getattr(zone, "visibility", None)
    if not vis:
        vis = (getattr(zone, "_properties", None) or {}).get("visibility")
    return (vis or "public").lower()


def _rrset_type(rrset: Any) -> str:
    """rrset type: proto objects use .type, hand-written SDK .record_type."""
    return getattr(rrset, "type", None) or getattr(rrset, "record_type", "") or ""


def map_dns_zone(zone: Any, account_id: str) -> NormalizedResource:
    """Map one ManagedZone to NormalizedResource."""
    dns_name = _strip_dot(getattr(zone, "dns_name", "") or "")
    attributes = {
        "zone_type": _zone_visibility(zone),
        "dns_servers": [
            _strip_dot(ns) for ns in (getattr(zone, "name_servers", None) or [])
        ] or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=ZONE_TYPE,
        provider_id=dns_name,  # domain name is globally unique
        cloud_account=account_id,
        name=dns_name,
        region="",  # DNS is global
        zone="",
        status=normalize_status("available"),
        attributes=attributes,
        cloud_tags={},
    )


def map_dns_record(
    rrset: Any, value: str, priority: int | None, zone_name: str, account_id: str,
) -> NormalizedResource:
    """Map one rrdata value of an rrset to NormalizedResource."""
    fqdn = _strip_dot(getattr(rrset, "name", "") or "")
    record_type = _rrset_type(rrset)
    attributes = {
        "rr": _relative_rr(fqdn, zone_name),
        "record_type": record_type if record_type in _RECORD_TYPE_OPTIONS else None,
        "value": value,
        "ttl": getattr(rrset, "ttl", 0) or None,
        "priority": priority,
        "policy_type": "simple",
        "raw": {
            "name": getattr(rrset, "name", ""),
            "type": record_type,
            "ttl": getattr(rrset, "ttl", 0),
            "rrdatas": list(getattr(rrset, "rrdatas", None) or []),
        },
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RECORD_TYPE,
        provider_id=f"{zone_name}:{fqdn}:{record_type}:{value}",
        cloud_account=account_id,
        name=fqdn,
        region="",
        zone="",
        status=normalize_status("available"),  # rrsets have no enable/disable
        attributes=attributes,
        cloud_tags={},
        parent_provider_id=zone_name,
        parent_resource_type=ZONE_TYPE,
    )


async def list_dns_zone(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all managed zones of the project."""
    started = time.perf_counter()
    client = build_dns_client(account)
    # materialize inside the worker thread: the SDK iterator does lazy HTTP
    zones = await fetch(
        lambda: list(client.list_zones()),
        account=account,
        resource_type=ZONE_TYPE,
        api="dns.Client.list_zones",
    )
    count = 0
    for zone in zones:
        count += 1
        yield map_dns_zone(zone, account.account_id)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("DNS zone fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": ZONE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})


async def list_dns_record(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all record sets of every managed zone (SOA skipped)."""
    started = time.perf_counter()
    client = build_dns_client(account)
    zones = await fetch(
        lambda: list(client.list_zones()),
        account=account,
        resource_type=RECORD_TYPE,
        api="dns.Client.list_zones",
    )
    count = 0
    for zone in zones:
        zone_name = _strip_dot(getattr(zone, "dns_name", "") or "")
        if not zone_name:
            continue
        rrsets = await fetch(
            lambda z=zone: list(z.list_resource_record_sets()),
            account=account,
            resource_type=RECORD_TYPE,
            api="ManagedZone.list_resource_record_sets",
        )
        for rrset in rrsets:
            record_type = _rrset_type(rrset)
            if record_type in _SKIP_TYPES:
                continue
            for rrdata in getattr(rrset, "rrdatas", None) or []:
                value, priority = _normalize_value(record_type, rrdata)
                count += 1
                yield map_dns_record(
                    rrset, value, priority, zone_name, account.account_id,
                )
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("DNS record fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RECORD_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
