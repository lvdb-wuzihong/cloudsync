"""Tests for content hashing (decision D3)."""

from __future__ import annotations

from cloudsync.normalize.hashing import compute_resource_version
from cloudsync.schemas.normalized import NormalizedResource


def _resource(**overrides) -> NormalizedResource:
    base = {
        "provider": "aliyun",
        "resource_type": "aliyun_ecs",
        "provider_id": "i-abc123",
        "cloud_account": "1234567890",
        "name": "web-01",
        "region": "cn-beijing",
        "status": "running",
        "attributes": {"cpu_cores": 4, "memory_gb": 8},
        "cloud_tags": {"env": "prod"},
    }
    base.update(overrides)
    return NormalizedResource(**base)


def test_hash_is_stable_for_identical_content():
    assert compute_resource_version(_resource()) == compute_resource_version(_resource())


def test_hash_length_is_16_hex_chars():
    version = compute_resource_version(_resource())
    assert len(version) == 16
    int(version, 16)  # must parse as hex


def test_hash_changes_when_attributes_change():
    before = compute_resource_version(_resource())
    after = compute_resource_version(_resource(attributes={"cpu_cores": 8, "memory_gb": 8}))
    assert before != after


def test_hash_ignores_identity_fields():
    # Identity fields (provider_id etc.) are outside the content hash by design
    a = compute_resource_version(_resource(provider_id="i-abc123"))
    b = compute_resource_version(_resource(provider_id="i-other"))
    assert a == b


def test_hash_is_insensitive_to_dict_insertion_order():
    a = compute_resource_version(_resource(attributes={"cpu_cores": 4, "memory_gb": 8}))
    b = compute_resource_version(_resource(attributes={"memory_gb": 8, "cpu_cores": 4}))
    assert a == b
