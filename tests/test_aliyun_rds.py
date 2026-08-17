"""Tests for aliyun RDS mapping (DescribeDBInstances + enrichment)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.rds import map_rds

_RDS_RAW = {
    "DBInstanceId": "rm-abc",
    "DBInstanceDescription": "web db",
    "RegionId": "cn-hangzhou",
    "ZoneId": "cn-hangzhou-h",
    "DBInstanceStatus": "Running",
    "Engine": "MySQL",
    "EngineVersion": "8.0",
    "DBInstanceClass": "mysql.n2.medium.1",
    "PayType": "Prepaid",
    "ExpireTime": "2026-12-31T16:00Z",
    "CreateTime": "2026-01-01T00:00Z",
    "VSwitchId": "vsw-1",
    "Tags": {"Tag": [{"Key": "env", "Value": "prod"}]},
}

_RDS_ATTR = {
    "DBInstanceStorage": 100,
    "ConnectionString": "rm-abc.mysql.rds.aliyuncs.com",
    "Port": "3306",
    "VSwitchId": "vsw-1",
}


def test_map_rds_fields():
    r = map_rds(_RDS_RAW, "acc", _RDS_ATTR)
    assert r.resource_type == "aliyun_rds"
    assert r.provider_id == "rm-abc"
    assert r.name == "web db"
    assert r.region == "cn-hangzhou"
    assert r.zone == "cn-hangzhou-h"
    assert r.status == "running"
    assert r.attributes["engine"] == "MySQL"
    assert r.attributes["engine_version"] == "8.0"
    assert r.attributes["instance_class"] == "mysql.n2.medium.1"
    assert r.attributes["storage_gb"] == 100  # attribute-only field
    assert r.attributes["connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert r.attributes["port"] == 3306  # string -> int
    assert r.attributes["charge_type"] == "prepaid"  # Prepaid -> enum value
    assert r.attributes["expired_at"] == "2026-12-31T16:00Z"
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.cloud_tags == {"env": "prod"}
    # RDS belongs to VSwitch (belongs_to 网络归属)
    assert r.parent_provider_id == "vsw-1"
    assert r.parent_resource_type == "aliyun_vswitch"


def test_map_rds_public_endpoint_overrides_private():
    r = map_rds(
        _RDS_RAW, "acc", _RDS_ATTR,
        public_endpoint=("rm-abc-pub.mysql.rds.aliyuncs.com", 3307),
    )
    assert r.attributes["connection_string"] == "rm-abc-pub.mysql.rds.aliyuncs.com"
    assert r.attributes["port"] == 3307


def test_map_rds_postpaid_without_enrichment():
    raw = dict(_RDS_RAW, PayType="Postpaid")
    r = map_rds(raw, "acc")
    assert r.attributes["charge_type"] == "postpaid"
    assert "expired_at" not in r.attributes  # postpaid has no expiry
    assert "storage_gb" not in r.attributes
    assert "connection_string" not in r.attributes
    # vswitch falls back to the list-API field
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.parent_provider_id == "vsw-1"
