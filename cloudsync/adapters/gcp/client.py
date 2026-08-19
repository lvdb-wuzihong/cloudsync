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
from google.cloud import dns
from google.cloud.compute_v1 import (
    DisksClient,
    FirewallsClient,
    InstancesClient,
    MachineTypesClient,
    NetworksClient,
    SubnetworksClient,
    ZonesClient,
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

# Read-only scopes covering Compute Engine and Cloud DNS resources.
# NOTE: the Compute API rejects cloud-platform.read-only ("insufficient
# authentication scopes" even with a valid token and enough IAM); it only
# accepts compute.readonly / compute / cloud-platform. Cloud DNS needs its
# own read-only scope. A multi-scope token satisfies both APIs.
_COMPUTE_SCOPES = [
    "https://www.googleapis.com/auth/compute.readonly",
    "https://www.googleapis.com/auth/ndev.clouddns.readonly",
]

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


def build_zones_client(account: AccountConfig) -> ZonesClient:
    """Compute Engine zones client for one account (region scope discovery)."""
    return ZonesClient(credentials=build_credentials(account))


def build_machine_types_client(account: AccountConfig) -> MachineTypesClient:
    """Compute Engine machine types client for one account (cpu/memory specs)."""
    return MachineTypesClient(credentials=build_credentials(account))


def build_dns_client(account: AccountConfig) -> dns.Client:
    """Cloud DNS client for one account (hand-written 0.x SDK; no GAPIC dns_v1)."""
    return dns.Client(project=project_of(account), credentials=build_credentials(account))


def last_segment(url: str) -> str:
    """Last path segment of a GCP resource URL (or the value itself).

    Compute fields like machine_type / network / subnetwork / disk source are
    full URLs (.../projects/p/regions/r/subnetworks/name); model fields and
    edge matching need the bare resource name.
    """
    return url.rstrip("/").rsplit("/", 1)[-1] if url else ""


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
