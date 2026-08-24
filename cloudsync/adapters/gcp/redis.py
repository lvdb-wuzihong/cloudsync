"""GCP Memorystore for Redis adapter: CloudRedisClient.list_instances.

locations='-' lists all regions in one call (global parent), so the
accounts.yaml region scope filters on the parsed location afterwards.
Instance names are only guaranteed unique per location, hence provider_id
uses "{region}/{name}" (same collision lesson as gcp_subnet/gcp_disk).

Field codes align with the CMDB model gcp_redis (engine_version / tier /
capacity_mb / connection_string / port / vpc_id — the last three land once
the model fields are registered; filter_by_model_fields drops them until
then). Proto enum fields are read via .name with a str fallback so tests can
simulate them with plain strings.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_redis_client,
    fetch,
    last_segment,
    project_of,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.redis")

RESOURCE_TYPE = "gcp_redis"


def _enum_name(value: Any) -> str:
    """Proto enum -> name string; plain strings (tests) pass through."""
    if value is None:
        return ""
    return getattr(value, "name", "") or str(value)


def _parse_location(full_name: str) -> str:
    """projects/p/locations/asia-east2/instances/x -> asia-east2."""
    parts = (full_name or "").split("/")
    if "locations" in parts:
        return parts[parts.index("locations") + 1]
    return ""


def map_redis(instance: Any, account_id: str) -> NormalizedResource:
    """Map one Memorystore Instance (proto message) to NormalizedResource."""
    full_name = getattr(instance, "name", "") or ""
    name = last_segment(full_name)
    region = _parse_location(full_name)

    redis_version = getattr(instance, "redis_version", "") or ""
    engine_version = redis_version.split("_", 1)[1].replace("_", ".") \
        if "_" in redis_version else (redis_version or None)

    memory_gb = getattr(instance, "memory_size_gb", 0) or 0
    authorized_network = getattr(instance, "authorized_network", "") or ""
    vpc_name = last_segment(authorized_network) or None

    attributes = {
        "engine_version": engine_version,
        "tier": _enum_name(getattr(instance, "tier", None)) or None,
        "capacity_mb": int(memory_gb) * 1024 if memory_gb else None,
        "connection_string": getattr(instance, "host", "") or None,
        "port": getattr(instance, "port", 0) or None,
        "vpc_id": vpc_name,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=f"{region}/{name}" if region else name,
        cloud_account=account_id,
        name=name,
        region=region,
        zone="",
        status=normalize_status(_enum_name(getattr(instance, "state", None))),
        attributes=attributes,
        cloud_tags=dict(getattr(instance, "labels", None) or {}),
        parent_provider_id=vpc_name,
        parent_resource_type="gcp_vpc" if vpc_name else None,
    )


async def list_redis(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """All Memorystore instances of the project, scoped to configured regions."""
    started = time.perf_counter()
    wanted: set[str] | None = set(account.regions) if account.regions else None
    client = build_redis_client(account)
    pager = await fetch(
        lambda: client.list_instances(
            {"parent": f"projects/{project_of(account)}/locations/-"},
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="CloudRedisClient.list_instances",
    )
    count = 0
    for instance in pager:
        region = _parse_location(getattr(instance, "name", "") or "")
        if wanted is not None and region not in wanted:
            continue
        count += 1
        yield map_redis(instance, account.account_id)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Redis fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
