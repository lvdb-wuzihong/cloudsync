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

# NetInfo：内网 + 公网 + 代理三形态并存（公网/代理未开时无对应条目）
_NET_INFO_FULL = (
    "rm-abc.mysql.rds.aliyuncs.com", 3306,
    "rm-abc.mysql.pub.rds.aliyuncs.com",
    "rm-abc.proxy.rds.aliyuncs.com",
)

_NET_INFO_PRIVATE_ONLY = (
    "rm-abc.mysql.rds.aliyuncs.com", 3306, None, None,
)


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
    # attribute 的连接串兜底为内网地址（NetInfo 未增强时）
    assert r.attributes["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert r.attributes["port"] == 3306  # string -> int
    assert r.attributes["charge_type"] == "prepaid"  # Prepaid -> enum value
    assert r.attributes["expired_at"] == "2026-12-31T16:00Z"
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.cloud_tags == {"env": "prod"}
    # RDS belongs to VSwitch (belongs_to 网络归属)
    assert r.parent_provider_id == "vsw-1"
    assert r.parent_resource_type == "aliyun_vswitch"


def test_map_rds_endpoints_split():
    """NetInfo 增强：内网/公网/代理分列，公网不再覆盖内网。"""
    r = map_rds(_RDS_RAW, "acc", _RDS_ATTR, _NET_INFO_FULL)
    assert r.attributes["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert r.attributes["public_connection_string"] == "rm-abc.mysql.pub.rds.aliyuncs.com"
    assert r.attributes["proxy_endpoint"] == "rm-abc.proxy.rds.aliyuncs.com"
    assert "connection_string" not in r.attributes  # 旧单字段已废弃
    assert r.attributes["port"] == 3306  # 内网端口语义


def test_map_rds_private_only_no_public_proxy():
    """未开公网/代理的实例：两个字段不落，不硬塞。"""
    r = map_rds(_RDS_RAW, "acc", _RDS_ATTR, _NET_INFO_PRIVATE_ONLY)
    assert r.attributes["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert "public_connection_string" not in r.attributes
    assert "proxy_endpoint" not in r.attributes


def test_map_rds_postpaid_without_enrichment():
    raw = dict(_RDS_RAW, PayType="Postpaid")
    r = map_rds(raw, "acc")
    assert r.attributes["charge_type"] == "postpaid"
    assert "expired_at" not in r.attributes  # postpaid has no expiry
    assert "storage_gb" not in r.attributes
    assert r.attributes["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert "public_connection_string" not in r.attributes
    # vswitch falls back to the list-API field
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.parent_provider_id == "vsw-1"
