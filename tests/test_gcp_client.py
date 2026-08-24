"""Tests for GCP SDK error normalization, credentials, fetch retries and dispatch."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import (
    BadRequest,
    Forbidden,
    InternalServerError,
    ServiceUnavailable,
    TooManyRequests,
    Unauthorized,
)
from google.cloud.compute_v1.services.instances.transports import InstancesRestTransport
from google.cloud.redis_v1.services.cloud_redis.transports import CloudRedisRestTransport

import cloudsync.adapters.gcp.client as client_mod
from cloudsync.adapters.gcp import _FETCHERS, GcpAdapter
from cloudsync.adapters.gcp.client import (
    build_credentials,
    build_instances_client,
    build_redis_client,
    fetch,
    map_sdk_exception,
    project_of,
)
from cloudsync.core.accounts import AccountConfig
from cloudsync.core.exceptions import AdapterError, AuthFailedError, RateLimitError


def _account(**overrides) -> AccountConfig:
    defaults = {"provider": "gcp", "account_id": "my-gcp-project",
                "service_account_json": '{"type": "service_account"}'}
    defaults.update(overrides)
    return AccountConfig(**defaults)


def test_throttle_statuses_map_to_rate_limit():
    for exc in (TooManyRequests("quota"), ServiceUnavailable("overloaded")):
        mapped = map_sdk_exception(exc, "gcp_compute")
        assert isinstance(mapped, RateLimitError)
        assert mapped.error_code == "RATE_LIMITED"


def test_auth_statuses_map_to_auth_failed():
    for exc in (Unauthorized("bad token"), Forbidden("denied")):
        mapped = map_sdk_exception(exc, "gcp_compute")
        assert isinstance(mapped, AuthFailedError)
        assert mapped.error_code == "AUTH_FAILED"


def test_other_statuses_map_to_adapter_error():
    for exc in (BadRequest("bad param"), InternalServerError("boom")):
        mapped = map_sdk_exception(exc, "gcp_vpc")
        assert isinstance(mapped, AdapterError)
        assert mapped.error_code == "API_ERROR"


def test_non_sdk_exception_maps_to_adapter_error():
    exc = map_sdk_exception(ConnectionError("reset"), "gcp_disk")
    assert isinstance(exc, AdapterError)
    assert exc.error_code == "API_ERROR"


def test_build_credentials_missing_json_raises_auth_failed():
    with pytest.raises(AuthFailedError):
        build_credentials(_account(service_account_json=""))


def test_build_credentials_unparsable_json_raises_auth_failed():
    with pytest.raises(AuthFailedError, match="unparsable"):
        build_credentials(_account(service_account_json="{not json"))


def test_build_credentials_invalid_structure_raises_auth_failed():
    # Valid JSON but missing the service account fields -> ValueError inside SDK
    with pytest.raises(AuthFailedError, match="invalid"):
        build_credentials(_account(service_account_json='{"type": "service_account"}'))


def test_project_of_returns_account_id():
    assert project_of(_account()) == "my-gcp-project"


def test_gapic_clients_use_rest_transport(monkeypatch):
    # gRPC transport ignores HTTPS_PROXY and fails with 503 "failed to connect
    # to all addresses" in the proxy-egress deployment; REST is mandatory.
    monkeypatch.setattr(client_mod, "build_credentials", lambda account: MagicMock())
    assert isinstance(build_instances_client(_account()).transport, InstancesRestTransport)
    assert isinstance(build_redis_client(_account()).transport, CloudRedisRestTransport)


async def test_fetch_raises_auth_error_without_retry():
    calls = 0

    def failing():
        nonlocal calls
        calls += 1
        raise Forbidden("denied")

    with pytest.raises(AuthFailedError):
        await fetch(failing, account=_account(), resource_type="gcp_compute", api="X")
    assert calls == 1  # AUTH_FAILED is never retried


async def test_fetch_retries_throttling_then_reraises():
    calls = 0

    def throttled():
        nonlocal calls
        calls += 1
        raise TooManyRequests("quota")

    with pytest.raises(RateLimitError):
        await fetch(throttled, account=_account(), resource_type="gcp_compute", api="X")
    assert calls == 3  # cloud_api_retry stop_after_attempt(3)


async def test_fetch_returns_sdk_result():
    sentinel = object()

    def ok():
        return sentinel

    assert await fetch(ok, account=_account(), resource_type="gcp_vpc",
                       api="NetworksClient.list") is sentinel


def test_dispatch_default_set_tracks_fetchers():
    adapter = GcpAdapter()
    assert adapter.default_resource_types() == sorted(_FETCHERS)
    assert all(callable(f) for f in _FETCHERS.values())


async def test_adapter_dispatch_unimplemented_type_raises():
    adapter = GcpAdapter()
    agen = adapter.list_resources(_account(), "gcp_lb")  # not in _FETCHERS
    with pytest.raises(NotImplementedError):
        await agen.__anext__()
