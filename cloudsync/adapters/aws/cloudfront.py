"""AWS CloudFront adapter: ListDistributions (global service).

CloudFront is global (no region binding, region column stays empty — same
discipline as gcp_vpc). Pagination is DistributionList.Marker/MaxItems with
IsTruncated as the more-results flag (wheel-verified). provider_id =
DistributionId; status is Enabled/Disabled -> running/stopped (the real
switch, per presets appendix A2).

Field codes align with the CMDB model aws_cloudfront (domain_name / aliases /
origins / http_version). origins normalized to [{origin_id, domain_name}]
sorted for a stable content hash; the consumer resolves S3 bucket names and
ALB DNS names out of origin domains for the "分发源" (#71) edges.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import PROVIDER, build_client, fetch
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.cloudfront")

RESOURCE_TYPE = "aws_cloudfront"
API_NAME = "list_distributions"


def normalize_origins(origins: dict[str, Any] | None) -> list[dict[str, str]]:
    """Origins.Items -> [{origin_id, domain_name}] sorted (stable hash)."""
    items = (origins or {}).get("Items") or []
    normalized = [
        {
            "origin_id": o.get("Id") or "",
            "domain_name": o.get("DomainName") or "",
        }
        for o in items
    ]
    normalized.sort(key=lambda o: (o["origin_id"], o["domain_name"]))
    return [o for o in normalized if o["origin_id"] or o["domain_name"]]


def map_distribution(
    raw: dict[str, Any], account_id: str,
) -> NormalizedResource:
    """Map one DistributionSummary to NormalizedResource."""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "domain_name": raw.get("DomainName") or None,
        "aliases": (raw.get("Aliases") or {}).get("Items") or None,
        "origins": normalize_origins(raw.get("Origins")) or None,
        "http_version": raw.get("HttpVersion") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.get("Id", ""),
        cloud_account=account_id,
        name=raw.get("DomainName") or raw.get("Id", ""),
        region="",  # CloudFront is global
        zone="",
        status=normalize_status("running" if raw.get("Enabled") else "stopped"),
        attributes=attributes,
        cloud_tags={},  # distribution tags need ListTagsForResource; not a model field
        parent_provider_id=account_id,  # 账号归属：挂项目根节点
        parent_resource_type="aws_account",
    )


async def list_cloudfront(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield every distribution of the account (global, marker pagination)."""
    client = build_client(account, "cloudfront")
    marker: str | None = None
    count = 0
    while True:
        kwargs: dict[str, Any] = {"MaxItems": "100"}
        if marker:
            kwargs["Marker"] = marker
        response = await fetch(
            lambda kw=kwargs: client.list_distributions(**kw),
            account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
        )
        dist_list = response.get("DistributionList") or {}
        for raw in dist_list.get("Items") or []:
            count += 1
            yield map_distribution(raw, account.account_id)
        if not dist_list.get("IsTruncated"):
            break
        marker = dist_list.get("NextMarker")
        if not marker:
            break
    logger.info("CloudFront fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "count": count})
