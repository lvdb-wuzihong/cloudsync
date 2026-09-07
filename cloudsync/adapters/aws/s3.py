"""AWS S3 adapter: ListBuckets + per-bucket enrichment (global service).

S3 is global: one ListBuckets call covers every bucket of the account, so the
accounts.yaml region scope does NOT apply (each bucket carries its own
BucketRegion in the list response — wheel-verified field). provider_id is the
bucket name (globally unique, aligned with aliyun_oss).

Per-bucket enrichment: get_bucket_acl (canned ACL inferred from grants),
get_bucket_versioning (Enabled -> true), get_public_access_block (four-way
block all enabled -> true; NoSuchPublicAccessBlockConfiguration is benign =
nothing configured -> False). Status is None (buckets have no lifecycle).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import (
    PROVIDER,
    build_client,
    fetch,
)
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.s3")

RESOURCE_TYPE = "aws_s3"
API_NAME = "list_buckets"

# acl inference: AllUsers grants map to the well-known canned ACL names
_PUBLIC_READ = "http://acs.amazonaws.com/groups/global/AllUsers"


def infer_acl(grants: list[dict[str, Any]] | None) -> str | None:
    """Infer the canned-ACL name from GetBucketAcl grants (never fabricated).

    AllUsers READ / WRITE grants -> public-read family; otherwise private.
    Non-standard grant combinations stay None (the grants themselves are not
    a model field; the block_public_access boolean is the audit signal).
    """
    if not grants:
        return None
    public = [g for g in grants
              if (g.get("Grantee") or {}).get("URI", "").endswith(_PUBLIC_READ)]
    if not public:
        return "private"
    perms = {g.get("Permission") for g in public}
    if perms == {"READ"}:
        return "public-read"
    if {"READ", "WRITE"} <= perms:
        return "public-read-write"
    return None


def map_s3(
    name: str, account_id: str, region: str, creation_time: Any,
    acl: str | None, versioning: bool, block_public_access: bool,
) -> NormalizedResource:
    """Assemble the bucket NormalizedResource from list + enrichment results."""
    attributes = {
        # 字段 code 对齐 CMDB 模型定义（对齐 aliyun_oss 同 code）
        "acl": acl,
        "versioning": versioning or None,
        "block_public_access": block_public_access or None,
        "endpoint": f"{name}.s3.{region}.amazonaws.com" if region
        else f"{name}.s3.amazonaws.com",
        "creation_time": creation_time.isoformat() if creation_time else None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=name,
        cloud_account=account_id,
        name=name,
        region=region,  # regional resource (bucket data location), global namespace
        zone="",
        status=None,  # 桶无生命周期状态，不硬塞
        attributes=attributes,
        cloud_tags={},  # S3 tags need get_bucket_tagging per bucket; not a model field
        parent_provider_id=account_id,  # 账号归属：挂项目根节点（对齐 aliyun_oss #52）
        parent_resource_type="aws_account",
    )


async def list_s3(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield every bucket of the account with acl/versioning/PAB enrichment."""
    client = build_client(account, "s3")
    continuation: str | None = None
    while True:
        kwargs: dict[str, Any] = {}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = await fetch(
            lambda kw=kwargs: client.list_buckets(**kw),
            account=account, resource_type=RESOURCE_TYPE, api=API_NAME,
        )
        for raw in response.get("Buckets", []):
            name = raw.get("Name", "")
            region = raw.get("BucketRegion") or ""

            acl_resp = await fetch(
                lambda b=name: client.get_bucket_acl(Bucket=b),
                account=account, resource_type=RESOURCE_TYPE, api="get_bucket_acl",
            )
            ver_resp = await fetch(
                lambda b=name: client.get_bucket_versioning(Bucket=b),
                account=account, resource_type=RESOURCE_TYPE, api="get_bucket_versioning",
            )
            # NoSuchPublicAccessBlockConfiguration = nothing configured -> False
            pab_resp = await fetch(
                lambda b=name: client.get_public_access_block(Bucket=b),
                account=account, resource_type=RESOURCE_TYPE, api="get_public_access_block",
                benign_codes=frozenset({"NoSuchPublicAccessBlockConfiguration"}),
            )
            pab_cfg = (pab_resp or {}).get("PublicAccessBlockConfiguration") or {}
            block_all = all(pab_cfg.get(k) for k in (
                "BlockPublicAcls", "IgnorePublicAcls",
                "BlockPublicPolicy", "RestrictPublicBuckets",
            )) if pab_resp else False

            yield map_s3(
                name, account.account_id, region, raw.get("CreationTime"),
                infer_acl(acl_resp.get("Grants")),
                ver_resp.get("Status") == "Enabled",
                block_all,
            )
        continuation = response.get("ContinuationToken")
        if not continuation:
            break
    logger.info("S3 fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id})
