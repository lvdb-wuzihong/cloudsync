"""Aliyun OSS adapter: ListBuckets + per-bucket enrichment (oss2 SDK).

OSS is a global service: ListBuckets returns every bucket of the account
across all regions, so the accounts.yaml region scope does NOT apply here
(each bucket carries its own Location). The oss2 SDK is synchronous and not
tea-based, hence a dedicated fetch wrapper mapping oss2 exceptions onto the
engine hierarchy with the same discipline: throttling retried, auth never
retried, everything else aborts the round (no partial sets for the diff).

Per-bucket enrichment: GetBucketInfo (acl / redundancy / versioning /
endpoints), GetBucketStat (storage usage), GetBucketLifecycle (rules;
NoSuchLifecycle is benign = no rules). Field codes align with the CMDB
model aliyun_oss (acl / storage_class / redundancy_type / versioning /
endpoint / intranet_endpoint / used_size_gb / lifecycle_rules).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

import oss2
from oss2.exceptions import NoSuchLifecycle, OssError

from cloudsync.adapters.aliyun.client import PROVIDER
from cloudsync.core.exceptions import (
    AdapterError,
    AuthFailedError,
    CredentialError,
    RateLimitError,
)
from cloudsync.core.retry import cloud_api_retry
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.oss")

RESOURCE_TYPE = "aliyun_oss"
API_NAME = "ListBuckets"
PAGE_SIZE = 100  # ListBuckets max_keys
SERVICE_REGION = "cn-hangzhou"  # ListBuckets is global; any region endpoint works

# oss2 error codes normalized to AUTH_FAILED (mirrors client.py for tea SDKs)
_AUTH_ERROR_CODES = {
    "InvalidAccessKeyId",
    "SecurityTokenExpired",
    "SignatureDoesNotMatch",
    "AccessDenied",
}


def map_oss_exception(exc: OssError) -> AdapterError:
    """Normalize an oss2 error into the engine hierarchy."""
    detail = f"code={exc.code} status={exc.status} message={exc.message}"
    if exc.code in _AUTH_ERROR_CODES or exc.status in (401, 403):
        return AuthFailedError(PROVIDER, detail)
    if exc.code == "TooManyRequests" or exc.status == 429:
        return RateLimitError(PROVIDER, detail)
    return AdapterError(PROVIDER, detail)


@cloud_api_retry
async def fetch_oss(
    call: Callable[[], Any],
    *,
    account: AccountConfig,
    api: str,
) -> Any:
    """Run one synchronous oss2 call off the event loop with throttling retries."""
    try:
        return await asyncio.to_thread(call)
    except OssError as exc:
        mapped = map_oss_exception(exc)
        if isinstance(mapped, RateLimitError):
            logger.warning("Cloud API throttled, will retry",
                           extra={"provider": PROVIDER, "account": account.account_id,
                                  "resource_type": RESOURCE_TYPE, "api": api})
        raise mapped from exc


def build_service(account: AccountConfig) -> oss2.Service:
    """Account-level OSS service handle for the global ListBuckets call."""
    if not account.access_key_id or not account.access_key_secret:
        raise CredentialError("missing access_key_id/access_key_secret")
    auth = oss2.Auth(account.access_key_id, account.access_key_secret)
    return oss2.Service(auth, f"https://oss-{SERVICE_REGION}.aliyuncs.com")


def _build_bucket(account: AccountConfig, raw: Any) -> oss2.Bucket:
    """Bucket handle bound to the bucket's own region endpoint."""
    auth = oss2.Auth(account.access_key_id, account.access_key_secret)
    location = raw.location or f"oss-{SERVICE_REGION}"
    return oss2.Bucket(auth, f"https://{location}.aliyuncs.com", raw.name)


def _extract_region(location: str | None) -> str:
    """Location 'oss-cn-hangzhou' -> 'cn-hangzhou'."""
    return (location or "").removeprefix("oss-")


def _normalize_lifecycle_rules(rules: list[Any]) -> list[dict[str, Any]]:
    """LifecycleRule objects -> deterministic dicts (stable hash)."""
    normalized = []
    for rule in rules:
        entry: dict[str, Any] = {
            "id": rule.id or None,
            "prefix": rule.prefix or None,
            "status": rule.status,
        }
        if rule.expiration is not None:
            entry["expiration"] = {k: v for k, v in {
                "days": rule.expiration.days,
                "date": rule.expiration.date,
                "created_before_date": rule.expiration.created_before_date,
                "expired_object_delete_marker": rule.expiration.expired_object_delete_marker,
            }.items() if v is not None}
        if rule.storage_transitions:
            entry["storage_transitions"] = sorted(
                ({k: v for k, v in {
                    "days": t.days,
                    "created_before_date": t.created_before_date,
                    "storage_class": t.storage_class,
                }.items() if v is not None} for t in rule.storage_transitions),
                key=str,
            )
        normalized.append({k: v for k, v in entry.items() if v is not None})
    normalized.sort(key=str)
    return normalized


def map_oss(
    raw: Any,
    account_id: str,
    info: Any | None = None,
    storage_bytes: int | None = None,
    lifecycle_rules: list[dict[str, Any]] | None = None,
) -> NormalizedResource:
    """Map one ListBuckets item (+ enrichment) to NormalizedResource.

    OSS is a network root owned by the cloud account; parent points to
    aliyun_account so the consumer rebuilds OSS -> account belongs_to edges.
    """
    attributes: dict[str, Any] = {
        # 字段 code 对齐 CMDB 模型定义
        "storage_class": getattr(raw, "storage_class", None),
        "endpoint": None,
        "intranet_endpoint": None,
    }
    if info is not None:
        attributes.update({
            "acl": info.acl,
            "redundancy_type": info.data_redundancy_type or None,
            "versioning": (info.versioning_status or "").lower() == "enabled",
            "endpoint": info.extranet_endpoint or None,
            "intranet_endpoint": info.intranet_endpoint or None,
        })
    if storage_bytes is not None:
        attributes["used_size_gb"] = round(storage_bytes / (1024 ** 3), 2)
    if lifecycle_rules:
        attributes["lifecycle_rules"] = lifecycle_rules
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=raw.name,  # bucket name is the globally unique id
        cloud_account=account_id,
        name=raw.name,
        region=_extract_region(getattr(raw, "region", None) or raw.location),
        zone="",
        status=normalize_status("available"),  # buckets have no status; alive = running
        attributes=attributes,
        cloud_tags={},  # OSS tagging is a separate API; out of scope for v1
        parent_provider_id=account_id,
        parent_resource_type="aliyun_account",
    )


async def _bucket_info(account: AccountConfig, bucket: oss2.Bucket) -> Any:
    """GetBucketInfo: acl / redundancy / versioning / endpoints.

    The SDK wraps the BucketInfo object inside result.bucket.
    """
    response = await fetch_oss(
        lambda: bucket.get_bucket_info(), account=account, api="GetBucketInfo",
    )
    return response.bucket


async def _bucket_storage_bytes(account: AccountConfig, bucket: oss2.Bucket) -> int:
    """GetBucketStat: current storage usage in bytes."""
    response = await fetch_oss(
        lambda: bucket.get_bucket_stat(), account=account, api="GetBucketStat",
    )
    return response.storage_size


def _lifecycle_or_empty(bucket: oss2.Bucket) -> Any:
    """GetBucketLifecycle; NoSuchLifecycle is benign (bucket has no rules)."""
    try:
        return bucket.get_bucket_lifecycle()
    except NoSuchLifecycle:
        return None


async def _bucket_lifecycle(
    account: AccountConfig, bucket: oss2.Bucket
) -> list[dict[str, Any]]:
    """Fetch + normalize lifecycle rules; empty when none configured."""
    response = await fetch_oss(
        lambda: _lifecycle_or_empty(bucket), account=account,
        api="GetBucketLifecycle",
    )
    if response is None:
        return []
    return _normalize_lifecycle_rules(response.rules)


async def list_oss(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all buckets of the account (global, region scope not applicable)."""
    started = time.perf_counter()
    service = build_service(account)
    count = 0
    marker = ""
    while True:
        result = await fetch_oss(
            lambda m=marker: service.list_buckets(marker=m, max_keys=PAGE_SIZE),
            account=account,
            api=API_NAME,
        )
        for raw in result.buckets:
            bucket = _build_bucket(account, raw)
            info = await _bucket_info(account, bucket)
            storage_bytes = await _bucket_storage_bytes(account, bucket)
            lifecycle = await _bucket_lifecycle(account, bucket)
            count += 1
            yield map_oss(raw, account.account_id, info, storage_bytes, lifecycle)
        if not result.is_truncated:
            break
        marker = result.next_marker
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("OSS fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
