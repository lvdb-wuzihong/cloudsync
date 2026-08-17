"""Aliyun V2 SDK client factory + SDK error normalization + async fetch wrapper.

The V2 SDK is synchronous (tea runtime); every call goes through fetch() which
offloads it to a worker thread and maps SDK exceptions onto the engine
hierarchy (bingops-error-handling skill error_code normalization):

- Throttling*            -> RateLimitError (RATE_LIMITED, retried by callers)
- auth/permission codes  -> AuthFailedError (AUTH_FAILED, never retried)
- anything else          -> AdapterError (API_ERROR, round aborts, no deletes)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_nas20170626.client import Client as NasClient
from alibabacloud_nlb20220430.client import Client as NlbClient
from alibabacloud_slb20140515.client import Client as SlbClient
from alibabacloud_tea_openapi.models import Config as OpenApiConfig
from alibabacloud_vpc20160428.client import Client as VpcClient
from Tea.exceptions import TeaException, UnretryableException

from cloudsync.core.exceptions import AdapterError, AuthFailedError, RateLimitError
from cloudsync.core.retry import cloud_api_retry

if TYPE_CHECKING:
    from collections.abc import Callable

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aliyun.client")

PROVIDER = "aliyun"

# SDK error codes normalized to AUTH_FAILED (not retryable)
_AUTH_ERROR_CODES = {
    "InvalidAccessKeyId.NotFound",
    "InvalidAccessKeyId.Malformed",
    "InvalidAccessKey.NotMatch",
    "SignatureDoesNotMatch",
    "Forbidden",
    "Forbidden.RAM",
    "NoPermission",
    "InvalidSecurityToken.Expired",
    "InvalidSecurityToken.Malformed",
}

# Prefixes that identify throttling (SDK returns Throttling / Throttling.User /
# Throttling.Api etc. depending on the throttled dimension)
_THROTTLE_PREFIXES = ("Throttling",)


def build_openapi_config(account: AccountConfig, region: str) -> OpenApiConfig:
    """OpenAPI config for one account+region; endpoint resolved from region_id."""
    if not account.access_key_id or not account.access_key_secret:
        raise AuthFailedError(PROVIDER, "missing access_key_id/access_key_secret")
    return OpenApiConfig(
        access_key_id=account.access_key_id,
        access_key_secret=account.access_key_secret,
        region_id=region,
    )


def build_ecs_client(account: AccountConfig, region: str) -> EcsClient:
    """ECS product client for one account+region."""
    return EcsClient(build_openapi_config(account, region))


def build_vpc_client(account: AccountConfig, region: str) -> VpcClient:
    """VPC product client for one account+region."""
    return VpcClient(build_openapi_config(account, region))


def build_slb_client(account: AccountConfig, region: str) -> SlbClient:
    """SLB (CLB) product client for one account+region."""
    return SlbClient(build_openapi_config(account, region))


def build_nlb_client(account: AccountConfig, region: str) -> NlbClient:
    """NLB product client for one account+region."""
    return NlbClient(build_openapi_config(account, region))


def build_nas_client(account: AccountConfig, region: str) -> NasClient:
    """NAS (file storage) product client for one account+region."""
    return NasClient(build_openapi_config(account, region))


def map_sdk_exception(exc: Exception, resource_type: str) -> AdapterError:
    """Normalize an aliyun SDK exception into the engine hierarchy.

    Args:
        exc: TeaException / UnretryableException raised by the SDK.
        resource_type: Model code being fetched (log context only).

    Returns:
        RateLimitError / AuthFailedError / AdapterError instance (not raised).
    """
    if isinstance(exc, TeaException):
        code = exc.code or ""
        detail = f"code={code} message={exc.message}"
        if any(code.startswith(p) for p in _THROTTLE_PREFIXES):
            return RateLimitError(PROVIDER, detail)
        if code in _AUTH_ERROR_CODES:
            return AuthFailedError(PROVIDER, detail)
        return AdapterError(PROVIDER, detail)
    # UnretryableException wraps network/timeout failures inside the tea runtime
    return AdapterError(PROVIDER, f"{type(exc).__name__}: {exc}")


@cloud_api_retry
async def fetch(
    call: Callable[[], Any],
    *,
    account: AccountConfig,
    resource_type: str,
    api: str,
) -> Any:
    """Run one synchronous SDK call off the event loop with throttling retries.

    Args:
        call: Zero-arg closure invoking the SDK method (runs in a thread).
        account: Account context for log fields (credential never logged).
        resource_type: Model code being fetched.
        api: SDK API name, e.g. "DescribeInstances" (log context).

    Returns:
        Raw SDK response object.

    Raises:
        RateLimitError: Throttled, re-raised after retries are exhausted.
        AuthFailedError: Credential rejected (never retried).
        AdapterError: Any other SDK/network failure.
    """
    try:
        return await asyncio.to_thread(call)
    except (TeaException, UnretryableException) as exc:
        mapped = map_sdk_exception(exc, resource_type)
        if isinstance(mapped, RateLimitError):
            logger.warning("Cloud API throttled, will retry",
                           extra={"provider": PROVIDER, "account": account.account_id,
                                  "resource_type": resource_type, "api": api})
        raise mapped from exc
