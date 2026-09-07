"""AWS account root node adapter: config-driven, zero cloud API calls.

The account (12-digit account ID) is the AWS topology tree root; vpc / s3 /
cloudfront / eip "账号归属" edges hang off it. Data comes from accounts.yaml
itself (same pattern as aliyun/gcp account): one root node per configured
account. alias comes from display_name; owner/account_type stay unset by
discovery (manual-enrichment fields, model has no other fields to fetch).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from cloudsync.adapters.aws.client import PROVIDER
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.account")

RESOURCE_TYPE = "aws_account"


def map_account(account: AccountConfig) -> NormalizedResource:
    """Map one accounts.yaml entry to the topology root node."""
    attributes = {
        "alias": account.display_name or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=account.account_id,
        cloud_account=account.account_id,
        name=account.display_name or account.account_id,
        region="",  # account is global
        zone="",
        status=None,  # 账号根节点无生命周期状态，不硬塞
        attributes=attributes,
        cloud_tags={},
    )


async def list_account(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Emit the single root node for this account (no API calls)."""
    yield map_account(account)
