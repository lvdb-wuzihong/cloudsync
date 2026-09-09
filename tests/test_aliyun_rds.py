"""Tests for aliyun RDS mapping (DescribeDBInstances + enrichment)."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import cloudsync.adapters.aliyun.rds as rds_mod
from cloudsync.adapters.aliyun.rds import _fetch_proxy, map_rds
from cloudsync.core.exceptions import AdapterError

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

# NetInfo 端点（_extract_endpoints 产物）：内网 + 公网；has_proxy 触发代理增强
_ENDPOINTS_NET = {
    "private": "rm-abc.mysql.rds.aliyuncs.com",
    "private_port": 3306,
    "public": "rm-abc.mysql.pub.rds.aliyuncs.com",
}

# DescribeDBProxy 产物：代理内/外网分列（NetType InnerString/OuterString）
_ENDPOINTS_PROXY = {
    "proxy": "rm-abc.proxy.rds.aliyuncs.com",
    "proxy_public": "rm-abc.proxy.pub.rds.aliyuncs.com",
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
    """NetInfo + DescribeDBProxy 增强：内网/公网/代理内外网全分列。"""
    r = map_rds(_RDS_RAW, "acc", _RDS_ATTR, {**_ENDPOINTS_NET, **_ENDPOINTS_PROXY})
    assert r.attributes["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert r.attributes["public_connection_string"] == "rm-abc.mysql.pub.rds.aliyuncs.com"
    assert r.attributes["proxy_endpoint"] == "rm-abc.proxy.rds.aliyuncs.com"
    assert r.attributes["proxy_public_endpoint"] == "rm-abc.proxy.pub.rds.aliyuncs.com"
    assert "connection_string" not in r.attributes  # 旧单字段已废弃
    assert r.attributes["port"] == 3306  # 内网端口语义（NetInfo 权威）


def test_map_rds_private_only_no_public_proxy():
    """未开公网/代理的实例：三个字段不落，不硬塞。"""
    r = map_rds(_RDS_RAW, "acc", _RDS_ATTR, dict(_ENDPOINTS_NET, public=None))
    del_r = r.attributes
    assert del_r["private_connection_string"] == "rm-abc.mysql.rds.aliyuncs.com"
    assert "public_connection_string" not in del_r
    assert "proxy_endpoint" not in del_r
    assert "proxy_public_endpoint" not in del_r


def test_map_rds_postpaid_without_enrichment():
    """无增强兜底：内网地址/端口只信 NetInfo/Attribute，不硬塞。"""
    raw = dict(_RDS_RAW, PayType="Postpaid")
    r = map_rds(raw, "acc")
    assert r.attributes["charge_type"] == "postpaid"
    assert "expired_at" not in r.attributes  # postpaid has no expiry
    assert "storage_gb" not in r.attributes
    # 内网地址/端口不再从列表 API raw 兜底
    assert "private_connection_string" not in r.attributes
    assert "public_connection_string" not in r.attributes
    assert "port" not in r.attributes
    # vswitch falls back to the list-API field
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.parent_provider_id == "vsw-1"


# ---- _fetch_proxy（best-effort 增强字段）----

_PROXY_BODY = {
    "DBProxyServiceStatus": "Startup",
    "DBProxyConnectStringItems": [
        {
            "DBProxyConnectStringNetType": "InnerString",
            "DBProxyConnectString": "p.inner.rds.aliyuncs.com",
        },
        {
            "DBProxyConnectStringNetType": "OuterString",
            "DBProxyConnectString": "p.outer.rds.aliyuncs.com",
        },
    ],
}


def _account() -> SimpleNamespace:
    return SimpleNamespace(account_id="acc")


async def test_fetch_proxy_parses_startup_endpoints(monkeypatch):
    """DBProxyServiceStatus=Startup（官方开启取值）：解析内/外网代理端点。"""
    response = SimpleNamespace(body=SimpleNamespace(to_map=lambda: _PROXY_BODY))

    async def fake_fetch(call, **kwargs):
        return response

    monkeypatch.setattr(rds_mod, "fetch", fake_fetch)
    endpoints = await _fetch_proxy(_account(), None, "rm-abc")
    assert endpoints == {
        "proxy": "p.inner.rds.aliyuncs.com",
        "proxy_public": "p.outer.rds.aliyuncs.com",
    }


async def test_fetch_proxy_shutdown_yields_empty(monkeypatch):
    """DBProxyServiceStatus=Shutdown：未启用代理，不落端点。"""
    body = dict(_PROXY_BODY, DBProxyServiceStatus="Shutdown")
    response = SimpleNamespace(body=SimpleNamespace(to_map=lambda: body))

    async def fake_fetch(call, **kwargs):
        return response

    monkeypatch.setattr(rds_mod, "fetch", fake_fetch)
    assert await _fetch_proxy(_account(), None, "rm-abc") == {}


async def test_fetch_proxy_degrades_on_api_error(monkeypatch, caplog):
    """未开通代理实例报 API 错误：WARNING 降级返回空，不拖垮整轮。"""

    async def fake_fetch(call, **kwargs):
        raise AdapterError("aliyun", "code=IncorrectDBInstanceEngine")

    monkeypatch.setattr(rds_mod, "fetch", fake_fetch)
    with caplog.at_level(logging.WARNING, logger="cloudsync.adapters.aliyun.rds"):
        endpoints = await _fetch_proxy(_account(), None, "rm-abc")
    assert endpoints == {}
    assert any(r.levelno == logging.WARNING for r in caplog.records)
