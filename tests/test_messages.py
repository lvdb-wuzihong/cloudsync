"""Tests for message building from normalized resources."""

from __future__ import annotations

from cloudsync.normalize.hashing import compute_resource_version
from cloudsync.schemas.messages import build_delete_message, build_upsert_message
from cloudsync.schemas.normalized import NormalizedResource


def _resource() -> NormalizedResource:
    return NormalizedResource(
        provider="aliyun",
        resource_type="aliyun_ecs",
        provider_id="i-abc123",
        cloud_account="1234567890",
        name="web-01",
        region="cn-beijing",
        status="running",
        attributes={"cpu_cores": 4},
        cloud_tags={"env": "prod"},
        parent_provider_id="vsw-xyz",
        parent_resource_type="aliyun_vswitch",
    )


def test_upsert_message_carries_all_fields():
    resource = _resource()
    message = build_upsert_message(resource, compute_resource_version(resource))
    assert message.event_type == "upsert"
    assert message.resource_type == "aliyun_ecs"
    assert message.provider_id == "i-abc123"
    assert message.cloud_account == "1234567890"
    assert message.parent_provider_id == "vsw-xyz"
    assert len(message.resource_version) == 16


def test_delete_message_keeps_identity():
    resource = _resource()
    message = build_delete_message(resource, "")
    assert message.event_type == "delete"
    assert message.provider_id == "i-abc123"


def test_json_dict_timestamp_is_iso_utc():
    resource = _resource()
    message = build_upsert_message(resource, "0" * 16)
    dumped = message.to_json_dict()
    assert dumped["timestamp"].endswith("Z") or "+00:00" in dumped["timestamp"]
    assert dumped["attributes"] == {"cpu_cores": 4}
