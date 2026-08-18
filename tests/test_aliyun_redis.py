"""Tests for aliyun Redis mapping (R-KVStore DescribeInstances)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.redis import map_redis

_REDIS_RAW = {
    "InstanceId": "r-abc",
    "InstanceName": "web cache",
    "RegionId": "cn-hangzhou",
    "ZoneId": "cn-hangzhou-h",
    "InstanceStatus": "Normal",
    "EngineVersion": "7.0",
    "InstanceClass": "redis.master.small.default",
    "Capacity": 1024,
    "ConnectionDomain": "r-abc.redis.rds.aliyuncs.com",
    "Port": 6379,
    "VSwitchId": "vsw-1",
    "ChargeType": "PostPaid",
    "Tags": {"Tag": [{"Key": "env", "Value": "prod"}]},
}


def test_map_redis_fields():
    r = map_redis(_REDIS_RAW, "acc")
    assert r.resource_type == "aliyun_redis"
    assert r.provider_id == "r-abc"
    assert r.name == "web cache"
    assert r.region == "cn-hangzhou"
    assert r.zone == "cn-hangzhou-h"
    assert r.status == "running"  # Normal -> running
    assert r.attributes["engine_version"] == "7.0"
    assert r.attributes["instance_class"] == "redis.master.small.default"
    assert r.attributes["capacity_mb"] == 1024
    assert r.attributes["connection_string"] == "r-abc.redis.rds.aliyuncs.com"
    assert r.attributes["port"] == 6379
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.cloud_tags == {"env": "prod"}
    # Redis belongs to VSwitch (belongs_to 网络归属)
    assert r.parent_provider_id == "vsw-1"
    assert r.parent_resource_type == "aliyun_vswitch"


def test_map_redis_without_vswitch():
    raw = dict(_REDIS_RAW, VSwitchId="", InstanceStatus="Changing")
    r = map_redis(raw, "acc")
    assert "vswitch_id" not in r.attributes
    assert r.status == "maintenance"  # Changing -> maintenance
    assert r.parent_provider_id is None
