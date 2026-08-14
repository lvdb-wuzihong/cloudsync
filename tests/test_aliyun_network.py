"""Tests for aliyun vswitch/security_group mapping and rules hashing."""

from __future__ import annotations

from cloudsync.adapters.aliyun.security_group import _normalize_rule, map_security_group
from cloudsync.adapters.aliyun.vswitch import map_vswitch
from cloudsync.normalize.hashing import compute_rules_hash

_VSWITCH_RAW = {
    "VSwitchId": "vsw-abc",
    "VSwitchName": "web-subnet",
    "VpcId": "vpc-1",
    "ZoneId": "cn-beijing-h",
    "CidrBlock": "10.0.1.0/24",
    "AvailableIpAddressCount": 250,
    "Ipv6CidrBlock": "",
    "IsDefault": False,
    "Status": "Available",
    "Description": "web tier",
    "CreationTime": "2026-01-01T00:00Z",
    "Tags": {"Tag": [{"TagKey": "tier", "TagValue": "web"}]},
}


def test_map_vswitch_fields():
    r = map_vswitch(_VSWITCH_RAW, "acc")
    assert r.resource_type == "aliyun_vswitch"
    assert r.provider_id == "vsw-abc"
    assert r.name == "web-subnet"
    assert r.region == ""  # DescribeVSwitches items may omit RegionId
    assert r.zone == "cn-beijing-h"
    assert r.status == "running"  # Available -> running
    assert r.attributes["cidr_block"] == "10.0.1.0/24"
    assert r.attributes["available_ip_address_count"] == 250
    assert "ipv6_cidr_block" not in r.attributes  # empty string dropped
    assert r.cloud_tags == {"tier": "web"}
    assert r.parent_provider_id == "vpc-1"
    assert r.parent_resource_type == "aliyun_vpc"


def test_map_vswitch_without_vpc_has_no_parent():
    raw = dict(_VSWITCH_RAW, VpcId=None)
    r = map_vswitch(raw, "acc")
    assert r.parent_provider_id is None
    assert r.parent_resource_type is None


def test_map_vswitch_with_region_id():
    raw = dict(_VSWITCH_RAW, RegionId="cn-beijing")
    assert map_vswitch(raw, "acc").region == "cn-beijing"


_SG_RAW = {
    "SecurityGroupId": "sg-abc",
    "SecurityGroupName": "web-sg",
    "SecurityGroupType": "normal",
    "VpcId": "vpc-1",
    "CreationTime": "2026-01-01T00:00Z",
    "Description": "web",
    "ResourceGroupId": "rg-1",
    "AvailableInstanceAmount": 10,
    "EcsCount": 3,
    "Tags": {"Tag": [{"TagKey": "app", "TagValue": "web"}]},
}


def test_map_security_group_without_rules():
    r = map_security_group(_SG_RAW, "acc")
    assert r.resource_type == "aliyun_security_group"
    assert r.provider_id == "sg-abc"
    assert r.attributes["security_group_type"] == "normal"
    assert r.attributes["ecs_count"] == 3
    assert "rules" not in r.attributes
    assert "rules_hash" not in r.attributes
    assert r.parent_provider_id == "vpc-1"
    assert r.parent_resource_type == "aliyun_vpc"


def test_normalize_rule_drops_unset_fields():
    rule = _normalize_rule({"Direction": "ingress", "IpProtocol": "TCP",
                            "PortRange": "22/22", "SourceCidrIp": "0.0.0.0/0"})
    assert rule == {"direction": "ingress", "ip_protocol": "TCP",
                    "port_range": "22/22", "source_cidr_ip": "0.0.0.0/0"}


def test_rules_hash_deterministic_after_canonical_sort():
    rules = [
        {"direction": "ingress", "ip_protocol": "TCP", "port_range": "22/22"},
        {"direction": "egress", "ip_protocol": "ALL", "port_range": "-1/-1"},
    ]
    # callers sort before hashing (_list_rules); same multiset -> same hash
    assert compute_rules_hash(sorted(rules, key=str)) == compute_rules_hash(
        sorted(list(reversed(rules)), key=str)
    )
    assert len(compute_rules_hash(rules)) == 16


def test_rules_hash_changes_when_rules_change():
    rules = [{"direction": "ingress", "port_range": "22/22"}]
    changed = [{"direction": "ingress", "port_range": "80/80"}]
    assert compute_rules_hash(rules) != compute_rules_hash(changed)


def test_map_security_group_with_rules():
    rules = [{"direction": "ingress", "ip_protocol": "TCP", "port_range": "443/443"}]
    r = map_security_group(_SG_RAW, "acc", rules)
    assert r.attributes["rules"] == rules
    assert r.attributes["rules_hash"] == compute_rules_hash(rules)
