"""Aliyun-style discipline applied to GCP: VPC networks via NetworksClient.

VPC is a global resource (no zone/region binding), so the accounts.yaml
region scope does NOT apply here (same pattern as OSS). Fetching rules
identical to the other adapters: raise on any failure (never yield a partial
set) so the engine aborts the round without emitting deletes.

GCP VPC has no labels; os-ish enrichment does not apply. Field codes align
with the CMDB model gcp_vpc (subnet_mode / routing_mode / mtu).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_networks_client,
    fetch,
    project_of,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.vpc")

RESOURCE_TYPE = "gcp_vpc"
PAGE_SIZE = 500  # ListNetworks upper bound


def map_vpc(network: Any, account_id: str) -> NormalizedResource:
    """Map one Network (proto message) to NormalizedResource.

    Attribute keys are model field codes; parent points at the gcp_account
    root node (provider_id = project id) for the 项目归属 belongs_to edge.
    """
    attributes = {
        # auto_create_subnetworks = legacy auto-subnet mode ("Auto" in console)
        "subnet_mode": "auto" if network.auto_create_subnetworks else "custom",
        # routing_mode lives inside routing_config (not top-level); field
        # name verified via types.Network.meta.fields on the deployed SDK
        "routing_mode": (network.routing_config.routing_mode or "").lower() or None,
        "mtu": network.mtu or None,
    }
    # Drop unset fields so the content hash stays stable across API shapes
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        # provider_id uses the NAME (not the numeric id): GCP children
        # (subnetworks) only carry the network name, so edge matching must
        # join on names
        provider_id=network.name or "",
        cloud_account=account_id,
        name=network.name or "",
        region="",  # VPC is global
        zone="",
        status=normalize_status("available"),  # no lifecycle; alive = running
        attributes=attributes,
        cloud_tags={},  # GCP VPC has no labels
        parent_provider_id=account_id,  # project id
        parent_resource_type="gcp_account",
    )


async def list_vpc(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all VPC networks of the project (global, region scope N/A)."""
    started = time.perf_counter()
    client = build_networks_client(account)
    # dict 形式传参：方法扁平 kwarg 签名各产品不一，请求消息字段恒稳
    pager = await fetch(
        lambda: client.list(
            {"project": project_of(account), "max_results": PAGE_SIZE},
        ),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="NetworksClient.list",
    )
    count = 0
    for network in pager:
        count += 1
        yield map_vpc(network, account.account_id)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("VPC fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
