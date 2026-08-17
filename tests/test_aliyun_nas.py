"""Tests for aliyun NAS mapping (DescribeFileSystems)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.nas import map_nas

_NAS_RAW = {
    "FileSystemId": "fs-abc",
    "Description": "web share",
    "RegionId": "cn-hangzhou",
    "ZoneId": "cn-hangzhou-h",
    "Status": "Running",
    "ProtocolType": "NFS",
    "StorageType": "Performance",
    "MeteredSize": 2 * 1024 ** 3,
    "ChargeType": "PayAsYouGo",
    "CreateTime": "2026-01-01T00:00:00Z",
    "Tags": {"Tag": [{"Key": "env", "Value": "prod"}]},
    "MountTargets": {"MountTarget": [
        {"MountTargetDomain": "fs-abc-b.nas.aliyuncs.com", "VpcId": "vpc-2",
         "VswId": "vsw-2", "AccessGroupName": "DEFAULT_VPC_GROUP_NAME",
         "NetworkType": "Vpc", "Status": "Active"},
        {"MountTargetDomain": "fs-abc-a.nas.aliyuncs.com", "VpcId": "vpc-1",
         "VswId": "vsw-1", "AccessGroupName": "DEFAULT_VPC_GROUP_NAME",
         "NetworkType": "Vpc", "Status": "Active"},
    ]},
}


def test_map_nas_fields():
    r = map_nas(_NAS_RAW, "acc")
    assert r.resource_type == "aliyun_nas"
    assert r.provider_id == "fs-abc"
    assert r.name == "web share"  # NAS has no name; description is the display name
    assert r.region == "cn-hangzhou"
    assert r.zone == "cn-hangzhou-h"
    assert r.status == "running"
    assert r.attributes["protocol_type"] == "NFS"
    assert r.attributes["storage_type"] == "Performance"
    assert r.attributes["used_size_gb"] == 2.0  # MeteredSize bytes -> GB
    assert r.attributes["charge_type"] == "postpaid"  # PayAsYouGo -> postpaid
    # mount targets sorted by domain for stable hash
    assert [t["mount_target_domain"] for t in r.attributes["mount_targets"]] == [
        "fs-abc-a.nas.aliyuncs.com", "fs-abc-b.nas.aliyuncs.com",
    ]
    assert r.attributes["vpc_id"] == "vpc-1"  # first sorted mount-target VPC
    # internal metadata for NAS -> VPC relates_to edge building
    assert r.attributes["_mount_vpc_ids"] == ["vpc-1", "vpc-2"]
    assert r.cloud_tags == {"env": "prod"}
    # NAS is owned by the cloud account (belongs_to 账号归属)
    assert r.parent_provider_id == "acc"
    assert r.parent_resource_type == "aliyun_account"


def test_map_nas_without_mount_targets():
    raw = dict(_NAS_RAW, ChargeType="Subscription", MountTargets={})
    r = map_nas(raw, "acc")
    assert r.attributes["charge_type"] == "prepaid"
    assert "mount_targets" not in r.attributes
    assert "vpc_id" not in r.attributes
    assert "_mount_vpc_ids" not in r.attributes
