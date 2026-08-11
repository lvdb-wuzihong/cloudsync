"""Tests for aliyun ECS/VPC normalization mapping (pure dict in, model out)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.ecs import map_instance
from cloudsync.adapters.aliyun.vpc import map_vpc

_ECS_RAW = {
    "InstanceId": "i-abc123",
    "InstanceName": "web-01",
    "RegionId": "cn-beijing",
    "ZoneId": "cn-beijing-h",
    "Status": "Running",
    "InstanceType": "ecs.g7.large",
    "Cpu": 2,
    "Memory": 8192,
    "OSName": "Alibaba Cloud Linux 3",
    "OSType": "linux",
    "HostName": "web-01",
    "ImageId": "aliyun_3_x64_20G",
    "InstanceChargeType": "PostPaid",
    "InternetChargeType": "PayByTraffic",
    "CreationTime": "2026-01-01T00:00Z",
    "ExpiredTime": "2099-12-31T00:00Z",
    "VpcAttributes": {
        "VpcId": "vpc-1",
        "VSwitchId": "vsw-1",
        "PrivateIpAddress": {"IpAddress": ["10.0.0.5"]},
    },
    "PublicIpAddress": {"IpAddress": ["1.2.3.4"]},
    "EipAddress": {"IpAddress": ""},
    "SecurityGroupIds": {"SecurityGroupId": ["sg-1", "sg-2"]},
    "Tags": {"Tag": [{"TagKey": "Env_Type", "TagValue": "prod"}, {"TagKey": "", "TagValue": "x"}]},
}


def test_map_instance_common_fields():
    r = map_instance(_ECS_RAW, "1234567890")
    assert r.provider == "aliyun"
    assert r.resource_type == "aliyun_ecs"
    assert r.provider_id == "i-abc123"
    assert r.cloud_account == "1234567890"
    assert r.name == "web-01"
    assert r.region == "cn-beijing"
    assert r.zone == "cn-beijing-h"
    assert r.status == "running"  # vocabulary member


def test_map_instance_attributes_exclude_common_layer():
    r = map_instance(_ECS_RAW, "1234567890")
    assert r.attributes["instance_type"] == "ecs.g7.large"
    assert r.attributes["cpu"] == 2
    assert r.attributes["memory_mb"] == 8192
    assert r.attributes["private_ip"] == "10.0.0.5"
    assert r.attributes["public_ip"] == "1.2.3.4"
    assert r.attributes["vpc_id"] == "vpc-1"
    assert r.attributes["security_group_ids"] == ["sg-1", "sg-2"]
    # common-layer fields never duplicated into attributes
    for banned in ("name", "region", "zone", "status", "provider"):
        assert banned not in r.attributes
    # empty eip dropped (None-filtered)
    assert "eip" not in r.attributes


def test_map_instance_tags_normalized():
    r = map_instance(_ECS_RAW, "1234567890")
    assert r.cloud_tags == {"env-type": "prod"}  # lowercased + hyphenated


def test_map_instance_parent_is_vswitch():
    r = map_instance(_ECS_RAW, "1234567890")
    assert r.parent_provider_id == "vsw-1"
    assert r.parent_resource_type == "aliyun_vswitch"


def test_map_instance_without_vswitch_has_no_parent():
    raw = dict(_ECS_RAW)
    raw["VpcAttributes"] = {"VpcId": "vpc-1"}
    r = map_instance(raw, "acc")
    assert r.parent_provider_id is None
    assert r.parent_resource_type is None


def test_map_instance_unknown_status_falls_back():
    raw = dict(_ECS_RAW, Status="WeirdState")
    assert map_instance(raw, "acc").status == "unknown"


_VPC_RAW = {
    "VpcId": "vpc-abc",
    "VpcName": "prod-vpc",
    "RegionId": "cn-shanghai",
    "Status": "Available",
    "CidrBlock": "172.16.0.0/12",
    "SecondaryCidrBlocks": {"SecondaryCidrBlock": ["192.168.0.0/16"]},
    "VSwitchIds": {"VSwitchId": ["vsw-a", "vsw-b"]},
    "IsDefault": False,
    "Description": "main",
    "CreationTime": "2026-01-02T00:00Z",
    "Tags": {"Tag": [{"TagKey": "team", "TagValue": "ops"}]},
}


def test_map_vpc_fields():
    r = map_vpc(_VPC_RAW, "1234567890")
    assert r.resource_type == "aliyun_vpc"
    assert r.provider_id == "vpc-abc"
    assert r.name == "prod-vpc"
    assert r.status == "running"  # Available -> running
    assert r.zone == ""
    assert r.attributes["cidr_block"] == "172.16.0.0/12"
    assert r.attributes["secondary_cidr_blocks"] == ["192.168.0.0/16"]
    assert r.attributes["vswitch_ids"] == ["vsw-a", "vsw-b"]
    assert r.cloud_tags == {"team": "ops"}
    assert r.parent_provider_id is None  # VPC is a network root
