"""AWS Lambda adapter: ListFunctions across configured regions.

Same fetching discipline as the other adapters: config-driven region
scope, raise-on-failure. ListFunctions paginates with Marker/NextMarker
(verified against the botocore wheel paginators-1.json: limit MaxItems /
token Marker -> NextMarker, result Functions).

provider_id = "{region}/{FunctionName}" — function names are unique
per-region only, so the region scope goes into the key (same collision
discipline as elb/alb).

Field codes align with the CMDB model aws_lambda (runtime / handler /
timeout / memory_size / version / last_modified). Lambda has no
lifecycle state to show (State is Active once published) — status stays
None, same discipline as s3/eip. Hierarchy: aws_lambda belongs_to
aws_account (账号归属, #73) — serverless has no hosting layer.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import (
    PROVIDER,
    build_client,
    fetch,
    normalized_tags,
    region_scope,
)
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.lambda_function")

RESOURCE_TYPE = "aws_lambda"
API_NAME = "list_functions"
PAGE_SIZE = 50


def map_lambda_function(
    raw: dict[str, Any], account_id: str, region: str,
) -> NormalizedResource:
    """Map one ListFunctions item to NormalizedResource."""
    name = raw.get("FunctionName", "")
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（对齐函数计算类跨云约定）
        "runtime": raw.get("Runtime") or None,
        "handler": raw.get("Handler") or None,
        "timeout": raw.get("Timeout"),
        "memory_size": raw.get("MemorySize"),
        "version": raw.get("Version") or None,
        "last_modified": raw.get("LastModified") or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=f"{region}/{name}",  # 函数名仅 region 内唯一，撞键纪律
        cloud_account=account_id,
        name=name,
        region=region,
        zone="",  # serverless 无 AZ
        status=None,  # 无生命周期状态，不硬塞（对齐 s3/eip 惯例）
        attributes=attributes,
        cloud_tags={},  # list_functions 不含 tags；逐函数 list_tags 不值当
        parent_provider_id=account_id,  # 账号归属：挂项目根节点（对齐 s3）
        parent_resource_type="aws_account",
    )


async def list_lambda_function(
    account: AccountConfig,
) -> AsyncIterator[NormalizedResource]:
    """Yield Lambda functions of every region in the configured scope."""
    for region in await region_scope(account):
        client = build_client(account, "lambda", region)
        marker: str | None = None
        count = 0
        while True:
            kwargs = {"MaxItems": PAGE_SIZE}
            if marker:
                kwargs["Marker"] = marker
            response = await fetch(
                lambda kw=kwargs: client.list_functions(**kw),
                account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
            )
            for raw in response.get("Functions", []):
                count += 1
                yield map_lambda_function(raw, account.account_id, region)
            marker = response.get("NextMarker")
            if not marker:
                break
        logger.info("Lambda fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": count})
