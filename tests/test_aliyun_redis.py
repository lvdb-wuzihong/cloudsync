"""Tests for aliyun Redis mapping (R-KVStore DescribeInstances)."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import cloudsync.adapters.aliyun.redis as redis_mod
from cloudsync.adapters.aliyun.redis import _fetch_public_connection, map_redis
from cloudsync.core.exceptions import AdapterError

_REDIS_RAW = {
    "InstanceId": "r-abc",
    "InstanceName": "web cache",
    "RegionId": "cn-hangzhou",
    "ZoneId": "cn-hangzhou-h",
    "InstanceStatus": "Normal",
    "EngineVersion": "7.0",
    "InstanceClass": "redis.master.small.default",
    "Capacity": 1024,
    "Bandwidth": 32,
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
    assert r.attributes["bandwidth"] == 32  # 内网带宽(MB/s)，列表 API 内联
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


def test_map_redis_public_endpoint_enrichment():
    """NetInfo 增强：公网连接地址落入 attributes；未开通时不落。"""
    r = map_redis(_REDIS_RAW, "acc", "r-abc.pub.redis.rds.aliyuncs.com")
    assert r.attributes["public_connection_string"] == "r-abc.pub.redis.rds.aliyuncs.com"
    no_public = map_redis(_REDIS_RAW, "acc")
    assert "public_connection_string" not in no_public.attributes


# ---- _fetch_public_connection（best-effort 增强字段）----

def _account() -> SimpleNamespace:
    return SimpleNamespace(account_id="acc")


def _net_info_response(items: list[dict]) -> SimpleNamespace:
    return SimpleNamespace(
        body=SimpleNamespace(
            to_map=lambda: {"NetInfoItems": {"InstanceNetInfo": items}}
        )
    )


async def test_fetch_public_connection_parses_public_entry(monkeypatch):
    """IPType=Public（官方词表 Public/Inner/Private）条目 -> 公网连接地址。"""
    response = _net_info_response([
        {"IPType": "Private", "ConnectionString": "r-abc.redis.rds.aliyuncs.com"},
        {"IPType": "Public", "ConnectionString": "r-abc.pub.redis.rds.aliyuncs.com"},
    ])

    async def fake_fetch(call, **kwargs):
        return response

    monkeypatch.setattr(redis_mod, "fetch", fake_fetch)
    public = await _fetch_public_connection(_account(), None, "r-abc")
    assert public == "r-abc.pub.redis.rds.aliyuncs.com"


async def test_fetch_public_connection_without_public_entry(monkeypatch):
    """未开通公网：无 Public 条目 -> None。"""
    response = _net_info_response([
        {"IPType": "Private", "ConnectionString": "r-abc.redis.rds.aliyuncs.com"},
    ])

    async def fake_fetch(call, **kwargs):
        return response

    monkeypatch.setattr(redis_mod, "fetch", fake_fetch)
    assert await _fetch_public_connection(_account(), None, "r-abc") is None


async def test_fetch_public_connection_degrades_on_api_error(monkeypatch, caplog):
    """API 错误（未开通公网等）：WARNING 降级返回 None，不拖垮整轮。"""

    async def fake_fetch(call, **kwargs):
        raise AdapterError("aliyun", "code=InvalidInstanceId.NotFound")

    monkeypatch.setattr(redis_mod, "fetch", fake_fetch)
    with caplog.at_level(logging.WARNING, logger="cloudsync.adapters.aliyun.redis"):
        public = await _fetch_public_connection(_account(), None, "r-abc")
    assert public is None
    assert any(r.levelno == logging.WARNING for r in caplog.records)
