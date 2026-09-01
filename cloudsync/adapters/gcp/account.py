"""GCP account root node adapter: config-driven, zero cloud API calls.

The account (project) is the GCP topology tree root; OSS-ish resources
(gcp_vpc / gcp_disk) hang "账号归属" edges off it. Data comes from
accounts.yaml itself (same pattern as aliyun account): one root node per
configured project. project_number / owner are manual-enrichment fields
and stay unset by discovery.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from cloudsync.adapters.gcp.client import PROVIDER
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.account")

RESOURCE_TYPE = "gcp_account"


def map_account(account: AccountConfig) -> NormalizedResource:
    """One root node per configured project; provider_id = project ID."""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=account.account_id,
        cloud_account=account.account_id,
        name=account.display_name or account.account_id,
        region="",  # project is global
        zone="",
        status=None,  # project root has no lifecycle status; never fabricate one
        attributes={},
        cloud_tags={},
    )


async def list_account(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield the project root node (no cloud API involved)."""
    yield map_account(account)
