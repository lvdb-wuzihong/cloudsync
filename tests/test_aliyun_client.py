"""Tests for aliyun SDK error normalization, fetch retries and dispatch."""

from __future__ import annotations

import pytest
from Tea.exceptions import TeaException

import cloudsync.adapters.aliyun.ecs as ecs_mod
from cloudsync.adapters.aliyun import _FETCHERS, AliyunAdapter
from cloudsync.adapters.aliyun.client import fetch, map_sdk_exception
from cloudsync.core.accounts import AccountConfig
from cloudsync.core.exceptions import AdapterError, AuthFailedError, RateLimitError


def _tea(code: str | None, message: str = "boom") -> TeaException:
    return TeaException({"code": code, "message": message})


def test_throttling_codes_map_to_rate_limit():
    for code in ("Throttling", "Throttling.User", "Throttling.Api"):
        assert isinstance(map_sdk_exception(_tea(code), "aliyun_ecs"), RateLimitError)


def test_auth_codes_map_to_auth_failed():
    for code in ("InvalidAccessKeyId.NotFound", "SignatureDoesNotMatch", "Forbidden.RAM"):
        exc = map_sdk_exception(_tea(code), "aliyun_ecs")
        assert isinstance(exc, AuthFailedError)
        assert exc.error_code == "AUTH_FAILED"


def test_other_codes_map_to_adapter_error():
    exc = map_sdk_exception(_tea("InvalidParameter"), "aliyun_ecs")
    assert isinstance(exc, AdapterError)
    assert exc.error_code == "API_ERROR"


def test_codeless_tea_exception_maps_to_adapter_error():
    # UnretryableException (network failures) subclasses TeaException with
    # code=None and must land in API_ERROR, never RATE_LIMITED/AUTH_FAILED
    exc = map_sdk_exception(_tea(None), "aliyun_vpc")
    assert isinstance(exc, AdapterError)


async def test_fetch_raises_auth_error_without_retry():
    calls = 0

    def failing():
        nonlocal calls
        calls += 1
        raise _tea("InvalidAccessKeyId.NotFound")

    account = AccountConfig(provider="aliyun", account_id="acc",
                            access_key_id="k", access_key_secret="s")
    with pytest.raises(AuthFailedError):
        await fetch(failing, account=account, resource_type="aliyun_ecs", api="X")
    assert calls == 1  # AUTH_FAILED is never retried


async def test_fetch_retries_throttling_then_reraises():
    calls = 0

    def throttled():
        nonlocal calls
        calls += 1
        raise _tea("Throttling.User")

    account = AccountConfig(provider="aliyun", account_id="acc",
                            access_key_id="k", access_key_secret="s")
    with pytest.raises(RateLimitError):
        await fetch(throttled, account=account, resource_type="aliyun_ecs", api="X")
    assert calls == 3  # cloud_api_retry stop_after_attempt(3)


def test_fetcher_registry_covers_p1_types():
    assert set(_FETCHERS) == {"aliyun_ecs", "aliyun_vpc"}


async def test_adapter_dispatch_unimplemented_type_raises():
    adapter = AliyunAdapter()
    account = AccountConfig(provider="aliyun", account_id="acc")
    agen = adapter.list_resources(account, "aliyun_rds")
    with pytest.raises(NotImplementedError):
        await agen.__anext__()


# ── pagination with a fake fetch (no network) ───────────────────────────────


class _FakeBody:
    def __init__(self, body: dict):
        self._body = body

    def to_map(self) -> dict:
        return self._body


class _FakeResponse:
    def __init__(self, body: dict):
        self.body = _FakeBody(body)


def _page(instances: list[dict], total: int) -> dict:
    return {"Instances": {"Instance": instances}, "TotalCount": total}


async def test_list_ecs_paginates_until_total(monkeypatch):
    account = AccountConfig(provider="aliyun", account_id="acc",
                            access_key_id="k", access_key_secret="s",
                            regions=["cn-beijing"])
    pages = [
        _page([{"InstanceId": "i-1"}, {"InstanceId": "i-2"}], 3),
        _page([{"InstanceId": "i-3"}], 3),
    ]
    fetch_calls: list[str] = []

    async def fake_fetch(call, *, account, resource_type, api):
        fetch_calls.append(api)
        return _FakeResponse(pages.pop(0))

    monkeypatch.setattr(ecs_mod, "fetch", fake_fetch)

    resources = [r async for r in ecs_mod.list_ecs(account)]
    assert [r.provider_id for r in resources] == ["i-1", "i-2", "i-3"]
    assert all(r.cloud_account == "acc" for r in resources)
    assert fetch_calls == ["DescribeInstances", "DescribeInstances"]
