"""AWS client factory (boto3) + API error normalization + async fetch wrapper.

boto3 is synchronous; every call goes through fetch() which offloads it to a
worker thread and maps botocore exceptions onto the engine hierarchy:

- Throttling family / HTTP 429+503 -> RateLimitError (RATE_LIMITED, retried)
- AccessDenied family / HTTP 401+403 -> AuthFailedError (AUTH_FAILED, never retried)
- anything else                     -> AdapterError (API_ERROR, round aborts, no deletes)

benign_codes lets a caller declare per-call error codes that mean "empty
result" (e.g. S3 NoSuchPublicAccessBlockConfiguration): fetch returns None
instead of raising, keeping the raise-on-failure discipline for real errors.

Method names / pagination tokens / response shapes verified against the
botocore 1.43.89 wheel data files (service-2.json.gz + paginators-1.json):
EC2 NextToken, ELB/ELBv2 Marker+PageSize, CloudFront Marker/MaxItems,
S3 ListBuckets ContinuationToken. DescribeVpcs / DescribeAddresses /
DescribeRegions have no paginator (single full-list call).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ReadTimeoutError,
)

from cloudsync.core.exceptions import (
    AdapterError,
    AuthFailedError,
    CloudSyncError,
    RateLimitError,
)
from cloudsync.core.retry import cloud_api_retry
from cloudsync.normalize.tags import normalize_tags

if TYPE_CHECKING:
    from collections.abc import Callable

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.client")

PROVIDER = "aws"

# Error codes normalized to RATE_LIMITED (retried with backoff)
_THROTTLE_CODES = {
    "Throttling", "ThrottlingException", "RequestThrottled",
    "RequestThrottledException", "RequestLimitExceeded",
    "TooManyRequestsException", "LimitExceededException",
    "TransactionInProgressException",
}

# Error codes normalized to AUTH_FAILED (never retried)
_AUTH_CODES = {
    "UnauthorizedOperation", "AccessDenied", "AccessDeniedException",
    "AccessDeniedException", "InvalidClientTokenId",
    "SignatureDoesNotMatch", "AuthFailure", "AuthAccessDeniedException",
    "ExpiredToken", "ExpiredTokenException", "UnrecognizedClientException",
    "InvalidAccessKeyId",
}

# Read-only posture: caller IAM still enforces permissions, the botocore
# retry config only shapes transport-level retries (we add our own on top).
_BOTO_CONFIG = BotoConfig(retries={"max_attempts": 0}, connect_timeout=30, read_timeout=60)


def build_session(account: AccountConfig) -> boto3.Session:
    """boto3 session for one account; credential fields never logged.

    Raises:
        AuthFailedError: Access key fields missing.
    """
    if not account.access_key_id or not account.secret_access_key:
        raise AuthFailedError(PROVIDER, "missing access_key_id/secret_access_key")
    return boto3.Session(
        aws_access_key_id=account.access_key_id,
        aws_secret_access_key=account.secret_access_key,
        aws_session_token=account.session_token or None,
    )


def build_client(account: AccountConfig, service: str, region: str = "") -> Any:
    """Low-level service client for one account+region ("" = global endpoint)."""
    session = build_session(account)
    kwargs: dict[str, Any] = {"config": _BOTO_CONFIG}
    if region:
        kwargs["region_name"] = region
    return session.client(service, **kwargs)


def map_sdk_exception(exc: Exception, resource_type: str) -> CloudSyncError:
    """Normalize a botocore exception into the engine hierarchy.

    Returns:
        RateLimitError / AuthFailedError / AdapterError instance (not raised).
    """
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        status = (exc.response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        detail = f"code={code} status={status} message={error.get('Message', '')}".rstrip()
        if code in _THROTTLE_CODES or status in (429, 503):
            return RateLimitError(PROVIDER, detail)
        if code in _AUTH_CODES or status in (401, 403):
            return AuthFailedError(PROVIDER, detail)
        return AdapterError(PROVIDER, detail)
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return AuthFailedError(PROVIDER, f"credentials: {exc}")
    if isinstance(exc, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
        return AdapterError(PROVIDER, f"connection: {exc}")
    return AdapterError(PROVIDER, f"{type(exc).__name__}: {exc}")


@cloud_api_retry
async def fetch(
    call: Callable[[], Any],
    *,
    account: AccountConfig,
    resource_type: str,
    api: str,
    benign_codes: frozenset[str] = frozenset(),
) -> Any:
    """Run one synchronous boto3 call off the event loop with throttling retries.

    Args:
        call: Zero-arg closure invoking the SDK method (runs in a thread).
        account: Account context for log fields (credential never logged).
        resource_type: Model code being fetched.
        api: SDK API name, e.g. "describe_instances" (log context).
        benign_codes: Error codes that mean "empty result" — fetch returns
            None instead of raising (callers treat None as the empty case).

    Raises:
        RateLimitError: Throttled, re-raised after retries are exhausted.
        AuthFailedError: Credential/permission rejected (never retried).
        AdapterError: Any other SDK/network failure.
    """
    try:
        return await asyncio.to_thread(call)
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code", "")
        if code in benign_codes:
            return None
        mapped = map_sdk_exception(exc, resource_type)
        if isinstance(mapped, RateLimitError):
            logger.warning("Cloud API throttled, will retry",
                           extra={"provider": PROVIDER, "account": account.account_id,
                                  "resource_type": resource_type, "api": api})
        else:
            # 鉴权/其他 API 失败不再静默上抛，引擎日志之外保留一层现场细节
            logger.error("Cloud API call failed",
                         extra={"provider": PROVIDER, "account": account.account_id,
                                "resource_type": resource_type, "api": api,
                                "error_code": mapped.error_code, "detail": mapped.message})
        raise mapped from exc
    except Exception as exc:
        mapped = map_sdk_exception(exc, resource_type)
        logger.error("Cloud API call failed with non-API exception",
                     extra={"provider": PROVIDER, "account": account.account_id,
                            "resource_type": resource_type, "api": api,
                            "detail": mapped.message})
        raise mapped from exc


async def discover_regions(account: AccountConfig) -> list[str]:
    """All opted-in regions when accounts.yaml leaves the scope empty."""
    client = build_client(account, "ec2", region="us-east-1")
    response = await fetch(
        lambda: client.describe_regions(AllRegions=False),
        account=account, resource_type="region", api="describe_regions",
    )
    return sorted(
        r["RegionName"] for r in response.get("Regions", [])
        if r.get("RegionName") and r.get("OptInStatus") != "not-opted-in"
    )


async def region_scope(account: AccountConfig) -> list[str]:
    """accounts.yaml regions, or all opted-in regions when empty."""
    return list(account.regions) or await discover_regions(account)


def tags_dict(items: Any) -> dict[str, str]:
    """AWS Tags=[{Key,Value}] -> flat dict for normalize_tags."""
    return {t.get("Key", ""): t.get("Value", "") for t in items or [] if t.get("Key")}


def normalized_tags(items: Any) -> dict[str, str]:
    """AWS Tags list normalized straight to the consumer contract."""
    return normalize_tags(tags_dict(items))


def tag_name(items: Any) -> str:
    """AWS resources have no native name; display name comes from the Name tag."""
    return tags_dict(items).get("Name", "")


def last_segment(arn_or_url: str) -> str:
    """Last segment after '/' of an ARN or URL (empty-safe)."""
    return arn_or_url.rstrip("/").rsplit("/", 1)[-1] if arn_or_url else ""
