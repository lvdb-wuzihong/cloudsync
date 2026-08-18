"""Tests for aliyun AMQP (RabbitMQ) mapping (ListInstances)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.amqp import map_amqp

_AMQP_RAW = {
    "InstanceId": "amqp-abc",
    "InstanceName": "order mq",
    "InstanceType": "enterprise",
    "Status": "SERVING",
    "MaxQueue": 100,
    "MaxTps": 5000,
    "PrivateEndpoint": "amqp-abc.amqp.aliyuncs.com",
    "OrderType": "subscription",
    "ExpireTime": "2026-12-31T16:00Z",
    "VswitchIds": ["vsw-2", "vsw-1"],
}


def test_map_amqp_fields():
    r = map_amqp(_AMQP_RAW, "acc", "cn-hangzhou")
    assert r.resource_type == "aliyun_amqp"
    assert r.provider_id == "amqp-abc"
    assert r.name == "order mq"
    assert r.region == "cn-hangzhou"  # from caller endpoint, items carry none
    assert r.status == "running"  # SERVING -> running
    assert r.attributes["instance_type"] == "enterprise"
    assert r.attributes["max_queues"] == 100
    assert r.attributes["max_tps"] == 5000
    assert r.attributes["endpoint"] == "amqp-abc.amqp.aliyuncs.com"
    assert r.attributes["charge_type"] == "prepaid"  # subscription -> prepaid
    assert r.attributes["expired_at"] == "2026-12-31T16:00Z"
    # first sorted VSwitchId wins
    assert r.attributes["vswitch_id"] == "vsw-1"
    assert r.parent_provider_id == "vsw-1"
    assert r.parent_resource_type == "aliyun_vswitch"


def test_map_amqp_postpaid_without_vswitch():
    raw = dict(_AMQP_RAW, OrderType="payasyougo", VswitchIds=[],
               InstanceType="unknown-series", PrivateEndpoint="",
               PublicEndpoint="amqp-abc-pub.amqp.aliyuncs.com")
    r = map_amqp(raw, "acc", "cn-hangzhou")
    assert r.attributes["charge_type"] == "postpaid"
    assert "expired_at" not in r.attributes  # postpaid has no expiry
    assert "instance_type" not in r.attributes  # outside enum set dropped
    assert r.attributes["endpoint"] == "amqp-abc-pub.amqp.aliyuncs.com"
    assert "vswitch_id" not in r.attributes
    assert r.parent_provider_id is None
