"""Tests for aliyun OSS bucket mapping (alibabacloud_oss_v2 shapes simulated)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from alibabacloud_oss_v2.exceptions import OperationError, ServiceError

import cloudsync.adapters.aliyun.oss as oss_mod
from cloudsync.adapters.aliyun.oss import map_oss
from cloudsync.core.accounts import AccountConfig
from cloudsync.core.exceptions import AdapterError, AuthFailedError


def _service_error(code: str, status: int) -> ServiceError:
    """ServiceError carries all fmt kwargs (mirrors SDK construction)."""
    return ServiceError(code=code, status_code=status, request_id="req-1",
                        message="boom", ec="", timestamp="", request_target="")


def _account() -> AccountConfig:
    return AccountConfig(provider="aliyun", account_id="acc")

_OSS_RAW = SimpleNamespace(
    name="web-bucket",
    location="oss-cn-hangzhou",
    region="cn-hangzhou",
    storage_class="Standard",
    creation_date="2026-01-01T00:00:00.000Z",
)

_OSS_INFO = SimpleNamespace(
    acl="private",
    data_redundancy_type="LRS",
    versioning="Enabled",
    extranet_endpoint="web-bucket.oss-cn-hangzhou.aliyuncs.com",
    intranet_endpoint="web-bucket.oss-cn-hangzhou-internal.aliyuncs.com",
)


def test_map_oss_fields():
    lifecycle = [{"id": "rule-1", "prefix": "logs/", "status": "Enabled",
                  "expiration": {"days": 30}}]
    r = map_oss(_OSS_RAW, "acc", _OSS_INFO, storage_bytes=5 * 1024 ** 3,
                lifecycle_rules=lifecycle)
    assert r.resource_type == "aliyun_oss"
    assert r.provider_id == "web-bucket"  # bucket name is the id
    assert r.name == "web-bucket"
    assert r.region == "cn-hangzhou"
    assert r.status == "running"
    assert r.attributes["acl"] == "private"
    assert r.attributes["storage_class"] == "Standard"
    assert r.attributes["redundancy_type"] == "LRS"
    assert r.attributes["versioning"] is True  # Enabled -> True
    assert r.attributes["endpoint"] == "web-bucket.oss-cn-hangzhou.aliyuncs.com"
    assert r.attributes["intranet_endpoint"] == \
        "web-bucket.oss-cn-hangzhou-internal.aliyuncs.com"
    assert r.attributes["used_size_gb"] == 5.0
    assert r.attributes["lifecycle_rules"] == lifecycle
    # OSS is owned by the cloud account (belongs_to 账号归属)
    assert r.parent_provider_id == "acc"
    assert r.parent_resource_type == "aliyun_account"


def test_map_oss_versioning_suspended_is_false():
    info = SimpleNamespace(**{**vars(_OSS_INFO), "versioning": "Suspended"})
    r = map_oss(_OSS_RAW, "acc", info)
    assert r.attributes["versioning"] is False


def test_map_oss_without_enrichment():
    r = map_oss(_OSS_RAW, "acc")
    assert r.attributes["storage_class"] == "Standard"
    assert "acl" not in r.attributes
    assert "versioning" not in r.attributes
    assert "used_size_gb" not in r.attributes
    assert "lifecycle_rules" not in r.attributes
    assert r.parent_provider_id == "acc"


def test_map_oss_region_from_location_when_region_missing():
    raw = SimpleNamespace(**{**vars(_OSS_RAW), "region": None})
    r = map_oss(raw, "acc")
    assert r.region == "cn-hangzhou"  # oss- prefix stripped from Location


async def test_fetch_oss_unwraps_operation_error_to_auth_failed():
    """invoke_operation wraps ServiceError in OperationError; normalize the inner."""
    op = OperationError(name="GetBucketInfo", error=_service_error("AccessDenied", 403))

    def boom():
        raise op

    with pytest.raises(AuthFailedError):
        await oss_mod.fetch_oss(boom, account=_account(), api="GetBucketInfo")


async def test_fetch_oss_non_service_operation_error_maps_to_adapter_error():
    op = OperationError(name="ListBuckets", error=ConnectionError("net down"))

    def boom():
        raise op

    with pytest.raises(AdapterError):
        await oss_mod.fetch_oss(boom, account=_account(), api="ListBuckets")


async def test_bucket_lifecycle_no_such_lifecycle_is_benign(monkeypatch):
    """404 NoSuchLifecycle (wrapped in OperationError) means no rules, not failure."""
    op = OperationError(name="GetBucketLifecycle",
                        error=_service_error("NoSuchLifecycle", 404))

    async def fake_fetch(call, *, account, api):
        try:
            raise op
        except OperationError as exc:
            raise AdapterError("aliyun", "wrapped") from exc

    monkeypatch.setattr(oss_mod, "fetch_oss", fake_fetch)
    assert await oss_mod._bucket_lifecycle(_account(), None, "b") == []


async def test_bucket_lifecycle_other_api_error_reraises(monkeypatch):
    op = OperationError(name="GetBucketLifecycle",
                        error=_service_error("InternalError", 500))

    async def fake_fetch(call, *, account, api):
        try:
            raise op
        except OperationError as exc:
            raise AdapterError("aliyun", "wrapped") from exc

    monkeypatch.setattr(oss_mod, "fetch_oss", fake_fetch)
    with pytest.raises(AdapterError):
        await oss_mod._bucket_lifecycle(_account(), None, "b")


def test_sdk_surface_covers_required_ops():
    """Pin the v2 sync Client surface the adapter relies on.

    The adapter deliberately uses the sync Client (aio AsyncClient has an
    incomplete surface: no lifecycle ops in 1.3.x). If the SDK ever drops or
    renames one of these, this guard fails locally instead of in prod.
    """
    from alibabacloud_oss_v2.client import Client

    for op in ("list_buckets", "get_bucket_info", "get_bucket_stat",
               "get_bucket_lifecycle"):
        assert hasattr(Client, op), f"sync Client lost {op}"


def test_normalize_lifecycle_rules_sdk_shapes():
    """Fakes mirror alibabacloud_oss_v2 model attribute names exactly."""
    from cloudsync.adapters.aliyun.oss import _normalize_lifecycle_rules

    expiration = SimpleNamespace(
        days=30, created_before_date=None, expired_object_delete_marker=None
    )
    transition = SimpleNamespace(
        days=60, created_before_date=None, storage_class="IA"
    )
    rule = SimpleNamespace(
        id="rule-1", prefix="logs/", status="Enabled",
        expiration=expiration, transitions=[transition],
    )
    normalized = _normalize_lifecycle_rules([rule])
    assert normalized == [{
        "id": "rule-1", "prefix": "logs/", "status": "Enabled",
        "expiration": {"days": 30},
        "transitions": [{"days": 60, "storage_class": "IA"}],
    }]
