"""Aliyun account root node adapter: config-driven, zero cloud API calls.

The account is the cloud topology tree root (design doc: provider_id =
account ID, 建树根). Unlike every other resource type its data comes from
accounts.yaml itself, not from any Aliyun API: the fetcher emits exactly one
node per configured account so OSS / disk / NAS / VPC "账号归属" edges have a
parent to attach to.

Field codes align with the CMDB model aliyun_account (alias). owner /
account_type are manual-enrichment fields and stay unset by discovery.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from cloudsync.adapters.aliyun.client import PROVIDER
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.account")

RESOURCE_TYPE = "aliyun_account"


def map_account(account: AccountConfig) -> NormalizedResource:
    """Map one accounts.yaml entry to the topology root node."""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（display_name 落 alias）
        "alias": account.display_name or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=account.account_id,
        cloud_account=account.account_id,
        name=account.display_name or account.account_id,
        region="",  # account is global, not bound to any region
        zone="",
        status=normalize_status("available"),  # configured = alive
        attributes=attributes,
        cloud_tags={},
    )


async def list_account(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Emit the single root node for this account (no API calls)."""
    yield map_account(account)
