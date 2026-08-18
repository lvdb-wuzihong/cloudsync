"""GCP SDK client factory + API error normalization + async fetch wrapper.

The google-cloud SDKs are synchronous (gRPC/HTTP transport); every call goes
through fetch() which offloads it to a worker thread and maps SDK exceptions
onto the engine hierarchy (bingops-error-handling skill error_code
normalization):

- HTTP 429 / 503       -> RateLimitError (RATE_LIMITED, retried by callers)
- HTTP 401 / 403       -> AuthFailedError (AUTH_FAILED, never retried)
- anything else        -> AdapterError (API_ERROR, round aborts, no deletes)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.compute_v1 import (
    DisksClient,
    FirewallsClient,
    InstancesClient,
    NetworksClient,
    SubnetworksClient,
)
from google.oauth2 import service_account

from cloudsync.core.exceptions import (
    AdapterError,
    AuthFailedError,
    CloudSyncError,
    RateLimitError,
)
from cloudsync.core.retry import cloud_api_retry

if TYPE_CHECKING:
    from collections.abc import Callable

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.client")

PROVIDER = "gcp"

# Read-only scope covering Compute Engine resources (GCE/disks/VPC/subnets/firewalls)
_COMPUTE_SCOPES = ["https://www.googleapis.com/auth/cloud-platform.read-only"]

# HTTP status codes normalized to RATE_LIMITED (retried with backoff); 503 is
# included because GCP signals quota exhaustion via RESOURCE_EXHAUSTED/503 too
_THROTTLE_STATUS_CODES = {429, 503}

# HTTP status codes normalized to AUTH_FAILED (never retried)
_AUTH_STATUS_CODES = {401, 403}


def build_credentials(account: AccountConfig) -> service_account.Credentials:
    """Parse the service account JSON from accounts.yaml into credentials.

    Args:
        account: GCP account entry; service_account_json holds the key file body.

    Returns:
        Read-only credentials bound to the service account.

    Raises:
        AuthFailedError: Field missing or JSON unparsable (detail never
            includes credential content).
    """
    if not account.service_account_json:
        raise AuthFailedError(PROVIDER, "missing service_account_json")
    try:
        info = json.loads(account.service_account_json)
    except json.JSONDecodeError as exc:
        raise AuthFailedError(PROVIDER, f"service_account_json unparsable: {exc.msg}") from exc
    try:
        credentials = service_account.Credentials.from_service_account_info(  # type: ignore[no-untyped-call]
            info, scopes=_COMPUTE_SCOPES
        )
    except (KeyError, ValueError) as exc:
        raise AuthFailedError(PROVIDER, f"service_account_json invalid: {exc}") from exc
    return credentials  # type: ignore[no-any-return]


def project_of(account: AccountConfig) -> str:
    """GCP project ID; by contract account_id equals the project ID."""
    return account.account_id


def build_instances_client(account: AccountConfig) -> InstancesClient:
    """Compute Engine instances client for one account."""
    return InstancesClient(credentials=build_credentials(account))


def build_disks_client(account: AccountConfig) -> DisksClient:
    """Compute Engine disks client for one account."""
    return DisksClient(credentials=build_credentials(account))


def build_networks_client(account: AccountConfig) -> NetworksClient:
    """Compute Engine networks (VPC) client for one account."""
    return NetworksClient(credentials=build_credentials(account))


def build_subnetworks_client(account: AccountConfig) -> SubnetworksClient:
    """Compute Engine subnetworks client for one account."""
    return SubnetworksClient(credentials=build_credentials(account))


def build_firewalls_client(account: AccountConfig) -> FirewallsClient:
    """Compute Engine firewalls client for one account."""
    return FirewallsClient(credentials=build_credentials(account))


def map_sdk_exception(exc: Exception, resource_type: str) -> CloudSyncError:
    """Normalize a GCP SDK exception into the engine hierarchy.

    Args:
        exc: GoogleAPICallError raised by the SDK (covers HTTP/gRPC failures).
        resource_type: Model code being fetched (log context only).

    Returns:
        RateLimitError / AuthFailedError / AdapterError instance (not raised).
    """
    if isinstance(exc, GoogleAPICallError):
        detail = f"status={exc.code} message={exc.message}"
        if exc.code in _THROTTLE_STATUS_CODES:
            return RateLimitError(PROVIDER, detail)
        if exc.code in _AUTH_STATUS_CODES:
            return AuthFailedError(PROVIDER, detail)
        return AdapterError(PROVIDER, detail)
    # Network/timeout/credential-refresh failures outside the API call path
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
        api: SDK API name, e.g. "InstancesClient.list" (log context).

    Returns:
        Raw SDK response (page object).

    Raises:
        RateLimitError: Throttled, re-raised after retries are exhausted.
        AuthFailedError: Credential rejected (never retried).
        AdapterError: Any other SDK/network failure.
    """
    try:
        return await asyncio.to_thread(call)
    except GoogleAPICallError as exc:
        mapped = map_sdk_exception(exc, resource_type)
        if isinstance(mapped, RateLimitError):
            logger.warning("Cloud API throttled, will retry",
                           extra={"provider": PROVIDER, "account": account.account_id,
                                  "resource_type": resource_type, "api": api})
        raise mapped from exc
