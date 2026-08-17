"""Tests for aliyun disk mapping (DescribeDisks)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.disk import map_disk

_DISK_RAW = {
    "DiskId": "d-abc",
    "DiskName": "web-data",
    "RegionId": "cn-hangzhou",
    "ZoneId": "cn-hangzhou-h",
    "Status": "In_use",
    "Size": 100,
    "Category": "cloud_essd",
    "Type": "data",
    "InstanceId": "i-abc",
    "Encrypted": True,
    "PerformanceLevel": "PL1",
    "DiskChargeType": "PrePaid",
    "ExpiredTime": "2026-12-31T16:00Z",
    "CreationTime": "2026-01-01T00:00Z",
    "Tags": {"Tag": [{"TagKey": "env", "TagValue": "prod"}]},
}


def test_map_disk_fields():
    r = map_disk(_DISK_RAW, "acc")
    assert r.resource_type == "aliyun_disk"
    assert r.provider_id == "d-abc"
    assert r.name == "web-data"
    assert r.region == "cn-hangzhou"
    assert r.zone == "cn-hangzhou-h"
    assert r.status == "running"  # In_use -> running
    assert r.attributes["category"] == "cloud_essd"
    assert r.attributes["size_gb"] == 100
    assert r.attributes["is_system"] is False  # Type=data
    assert r.attributes["performance_level"] == "PL1"
    assert r.attributes["encrypted"] is True
    assert r.attributes["charge_type"] == "prepaid"  # PrePaid -> enum value
    assert r.attributes["expired_at"] == "2026-12-31T16:00Z"
    assert r.attributes["instance_id"] == "i-abc"
    assert r.cloud_tags == {"env": "prod"}
    # disk is owned by the cloud account (belongs_to 账号归属)
    assert r.parent_provider_id == "acc"
    assert r.parent_resource_type == "aliyun_account"


def test_map_disk_unattached_postpaid():
    raw = dict(
        _DISK_RAW, Status="Available", Type="system", InstanceId="",
        DiskChargeType="PostPaid", PerformanceLevel="",
    )
    r = map_disk(raw, "acc")
    assert r.attributes["is_system"] is True
    assert "instance_id" not in r.attributes  # unattached
    assert "expired_at" not in r.attributes  # postpaid has no expiry
    assert "performance_level" not in r.attributes  # empty string dropped
    assert r.attributes["charge_type"] == "postpaid"
