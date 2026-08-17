"""Aliyun OSS adapter: ListBuckets + per-bucket enrichment (OSS 2.0 SDK).

OSS is a global service: ListBuckets returns every bucket of the account
across all regions, so the accounts.yaml region scope does NOT apply here
(each bucket carries its own region/Location). The v2 SDK ships a native
async client (aio.AsyncClient); ServiceError is normalized onto the engine
hierarchy with the same discipline as client.py: throttling retried, auth
never retried, everything else aborts the round (no partial sets for diff).

Per-bucket enrichment: GetBucketInfo (acl / redundancy / versioning /
endpoints), GetBucketStat (storage usage), GetBucketLifecycle (rules;
NoSuchLifecycle is benign = no rules). Field codes align with the CMDB
model aliyun_oss (acl / storage_class / redundancy_type / versioning /
endpoint / intranet_endpoint / used_size_gb / lifecycle_rules).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from alibabacloud_oss_v2 import Config
from alibabacloud_oss_v2 import models as oss_models
from alibabacloud_oss_v2.aio import AsyncClient
from alibabacloud_oss_v2.credentials import StaticCredentialsProvider
from alibabacloud_oss_v2.exceptions import ServiceError

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

# v2 ServiceError codes normalized to AUTH_FAILED (mirrors client.py)
_AUTH_ERROR_CODES = {
    "InvalidAccessKeyId",
    "SecurityTokenExpired",
    "SignatureDoesNotMatch",
    "AccessDenied",
}


def map_oss_exception(exc: ServiceError) -> AdapterError:
    """Normalize a v2 SDK service error into the engine hierarchy."""
    detail = f"code={exc.code} status={exc.status_code} message={exc.message}"
    if exc.code in _AUTH_ERROR_CODES or exc.status_code in (401, 403):
        return AuthFailedError(PROVIDER, detail)
    if exc.code == "TooManyRequests" or exc.status_code == 429:
        return RateLimitError(PROVIDER, detail)
    return AdapterError(PROVIDER, detail)


@cloud_api_retry
async def fetch_oss(
    call: Callable[[], Any],
    *,
    account: AccountConfig,
    api: str,
) -> Any:
    """Run one v2 SDK async call with throttling retries and error mapping."""
    try:
        return await call()
    except ServiceError as exc:
        mapped = map_oss_exception(exc)
        if isinstance(mapped, RateLimitError):
            logger.warning("Cloud API throttled, will retry",
                           extra={"provider": PROVIDER, "account": account.account_id,
                                  "resource_type": RESOURCE_TYPE, "api": api})
        raise mapped from exc


def build_client(account: AccountConfig, region: str) -> AsyncClient:
    """Region-bound async OSS client from the account's static credentials."""
    if not account.access_key_id or not account.access_key_secret:
        raise CredentialError("missing access_key_id/access_key_secret")
    config = Config(
        credentials_provider=StaticCredentialsProvider(
            access_key_id=account.access_key_id,
            access_key_secret=account.access_key_secret,
        ),
        region=region,
    )
    return AsyncClient(config)


def _extract_region(location: str | None) -> str:
    """Location 'oss-cn-hangzhou' -> 'cn-hangzhou'."""
    return (location or "").removeprefix("oss-")


def _normalize_lifecycle_rules(rules: list[Any]) -> list[dict[str, Any]]:
    """v2 LifecycleRule objects -> deterministic dicts (stable hash)."""
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
                "created_before_date": rule.expiration.created_before_date,
                "expired_object_delete_marker":
                    rule.expiration.expired_object_delete_marker,
            }.items() if v is not None}
        if rule.transitions:
            entry["transitions"] = sorted(
                ({k: v for k, v in {
                    "days": t.days,
                    "created_before_date": t.created_before_date,
                    "storage_class": t.storage_class,
                }.items() if v is not None} for t in rule.transitions),
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
    }
    if info is not None:
        attributes.update({
            "acl": info.acl,
            "redundancy_type": info.data_redundancy_type or None,
            "versioning": (info.versioning or "").lower() == "enabled",
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
        region=getattr(raw, "region", None) or _extract_region(raw.location),
        zone="",
        status=normalize_status("available"),  # buckets have no status; alive = running
        attributes=attributes,
        cloud_tags={},  # OSS tagging is a separate API; out of scope for v1
        parent_provider_id=account_id,
        parent_resource_type="aliyun_account",
    )


async def _bucket_info(account: AccountConfig, client: AsyncClient, name: str) -> Any:
    """GetBucketInfo: acl / redundancy / versioning / endpoints."""
    result = await fetch_oss(
        lambda: client.get_bucket_info(oss_models.GetBucketInfoRequest(bucket=name)),
        account=account, api="GetBucketInfo",
    )
    return result.bucket_info


async def _bucket_storage_bytes(
    account: AccountConfig, client: AsyncClient, name: str
) -> int | None:
    """GetBucketStat: current storage usage in bytes."""
    result = await fetch_oss(
        lambda: client.get_bucket_stat(oss_models.GetBucketStatRequest(bucket=name)),
        account=account, api="GetBucketStat",
    )
    return result.storage


async def _bucket_lifecycle(
    account: AccountConfig, client: AsyncClient, name: str
) -> list[dict[str, Any]]:
    """Fetch + normalize lifecycle rules; empty when none configured."""
    try:
        result = await fetch_oss(
            lambda: client.get_bucket_lifecycle(
                oss_models.GetBucketLifecycleRequest(bucket=name)
            ),
            account=account, api="GetBucketLifecycle",
        )
    except AdapterError as exc:
        # NoSuchLifecycle is benign: the bucket simply has no rules
        cause = exc.__cause__
        if isinstance(cause, ServiceError) and cause.code == "NoSuchLifecycle":
            return []
        raise
    config = result.lifecycle_configuration
    if config is None or not config.rules:
        return []
    return _normalize_lifecycle_rules(config.rules)


async def list_oss(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all buckets of the account (global; region scope not applicable)."""
    started = time.perf_counter()
    clients: dict[str, AsyncClient] = {}

    def client_for(region: str) -> AsyncClient:
        """One async client per region, reused across buckets, closed at end."""
        if region not in clients:
            clients[region] = build_client(account, region)
        return clients[region]

    count = 0
    try:
        service = client_for(SERVICE_REGION)
        marker: str | None = None
        while True:
            result = await fetch_oss(
                lambda m=marker: service.list_buckets(
                    oss_models.ListBucketsRequest(marker=m, max_keys=PAGE_SIZE)
                ),
                account=account, api=API_NAME,
            )
            for props in result.buckets or []:
                region = props.region or _extract_region(props.location)
                client = client_for(region)
                info = await _bucket_info(account, client, props.name)
                storage_bytes = await _bucket_storage_bytes(account, client, props.name)
                lifecycle = await _bucket_lifecycle(account, client, props.name)
                count += 1
                yield map_oss(props, account.account_id, info, storage_bytes, lifecycle)
            if not result.is_truncated:
                break
            marker = result.next_marker
    finally:
        for client in clients.values():
            await client.close()
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("OSS fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
