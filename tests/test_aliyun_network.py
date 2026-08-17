"""Tests for aliyun vswitch/security_group mapping and rules hashing."""

from __future__ import annotations

from cloudsync.adapters.aliyun.clb import map_clb
from cloudsync.adapters.aliyun.eip import map_eip
from cloudsync.adapters.aliyun.nlb import map_nlb
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
    assert r.attributes["available_ip_count"] == 250
    assert r.attributes["vpc_id"] == "vpc-1"
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
    assert r.attributes["sg_type"] == "normal"
    assert r.attributes["vpc_id"] == "vpc-1"
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


_EIP_RAW = {
    "AllocationId": "eip-abc",
    "Name": "web-eip",
    "IpAddress": "47.96.1.1",
    "Status": "InUse",
    "Bandwidth": "100",
    "ChargeType": "PostPaid",
    "InternetChargeType": "PayByTraffic",
    "InstanceType": "EcsInstance",
    "InstanceId": "i-abc",
    "RegionId": "cn-hangzhou",
    "AllocationTime": "2026-01-01T00:00Z",
    "Tags": {"Tag": [{"TagKey": "env", "TagValue": "prod"}]},
}


def test_map_eip_fields():
    r = map_eip(_EIP_RAW, "acc")
    assert r.resource_type == "aliyun_eip"
    assert r.provider_id == "eip-abc"
    assert r.name == "web-eip"
    assert r.region == "cn-hangzhou"
    assert r.status == "running"  # InUse -> running
    assert r.attributes["ip_address"] == "47.96.1.1"
    assert r.attributes["bandwidth"] == 100  # string -> int
    assert r.attributes["charge_type"] == "PostPaid"
    assert r.attributes["bind_instance_type"] == "EcsInstance"
    assert r.attributes["bind_instance_id"] == "i-abc"
    assert r.cloud_tags == {"env": "prod"}
    assert r.parent_provider_id is None  # EIP is a network root


def test_map_eip_unbound_drops_bind_fields():
    raw = dict(_EIP_RAW, Status="Available", InstanceType="", InstanceId="")
    r = map_eip(raw, "acc")
    assert "bind_instance_type" not in r.attributes
    assert "bind_instance_id" not in r.attributes


_CLB_RAW = {
    "LoadBalancerId": "lb-abc",
    "LoadBalancerName": "web-lb",
    "RegionId": "cn-hangzhou",
    "Address": "47.96.2.2",
    "AddressType": "internet",
    "LoadBalancerSpec": "slb.s2.small",
    "LoadBalancerStatus": "active",
    "PayType": "PayOnDemand",
    "Tags": {"Tag": [{"TagKey": "env", "TagValue": "prod"}]},
}

_CLB_ATTR = {
    "VpcId": "vpc-1",
    "VSwitchId": "vsw-1",
    "BackendServers": {"BackendServer": [
        {"ServerId": "i-1"}, {"ServerId": "i-2"}, {"ServerId": "i-1"},
    ]},
    "ListenerPortsAndProtocol": {"ListenerPortAndProtocol": [
        {"ListenerPort": 443, "ListenerProtocol": "https"},
        {"ListenerPort": 80, "ListenerProtocol": "http"},
    ]},
}


def test_map_clb_fields():
    r = map_clb(_CLB_RAW, "acc", _CLB_ATTR)
    assert r.resource_type == "aliyun_clb"
    assert r.provider_id == "lb-abc"
    assert r.region == "cn-hangzhou"
    assert r.status == "running"  # active -> running
    assert r.attributes["address"] == "47.96.2.2"
    assert r.attributes["address_type"] == "internet"
    assert r.attributes["spec"] == "slb.s2.small"
    assert r.attributes["charge_type"] == "postpaid"  # PayOnDemand -> postpaid
    assert r.attributes["vpc_id"] == "vpc-1"
    # listeners sorted by port for stable hash
    assert r.attributes["listeners"] == [
        {"protocol": "http", "port": 80},
        {"protocol": "https", "port": 443},
    ]
    # backend ids deduped + sorted (internal metadata)
    assert r.attributes["_backend_ecs_ids"] == ["i-1", "i-2"]
    assert r.parent_provider_id == "vpc-1"
    assert r.parent_resource_type == "aliyun_vpc"


def test_map_clb_without_attribute():
    raw = dict(_CLB_RAW, PayType="PrePay")
    r = map_clb(raw, "acc")
    assert r.attributes["charge_type"] == "prepaid"
    assert r.attributes["listeners"] == []
    assert "_backend_ecs_ids" not in r.attributes
    assert "vpc_id" not in r.attributes
    assert r.parent_provider_id is None


_NLB_RAW = {
    "LoadBalancerId": "nlb-abc",
    "LoadBalancerName": "web-nlb",
    "RegionId": "cn-hangzhou",
    "DNSName": "nlb-abc.cn-hangzhou.nlb.aliyuncsslb.com",
    "AddressType": "Internet",
    "LoadBalancerStatus": "Active",
    "VpcId": "vpc-1",
    "ZoneMappings": [
        {"ZoneId": "cn-hangzhou-j", "VSwitchId": "vsw-2", "LoadBalancerAddresses": []},
        {"ZoneId": "cn-hangzhou-h", "VSwitchId": "vsw-1", "LoadBalancerAddresses": [
            {"PrivateIPv4Address": "10.0.1.5", "PublicIpAddress": "47.96.3.3",
             "AllocationId": "eip-1"},
        ]},
    ],
    "Tags": [{"Key": "env", "Value": "prod"}],
}

_NLB_LISTENERS = [
    {"ListenerPort": 443, "ListenerProtocol": "TCP", "ServerGroupId": "sgp-1"},
    {"ListenerPort": 80, "ListenerProtocol": "TCP", "ServerGroupId": "sgp-1"},
]

_NLB_SG_META = {
    "sgp-1": {"ServerGroupId": "sgp-1", "ServerGroupName": "web-group",
              "ServerGroupType": "Instance"},
}


def test_map_nlb_fields():
    r = map_nlb(_NLB_RAW, "acc", _NLB_LISTENERS, _NLB_SG_META, ["i-2", "i-1", "i-2"])
    assert r.resource_type == "aliyun_nlb"
    assert r.provider_id == "nlb-abc"
    assert r.region == "cn-hangzhou"
    assert r.status == "running"  # Active -> running
    assert r.attributes["dns_name"] == "nlb-abc.cn-hangzhou.nlb.aliyuncsslb.com"
    assert r.attributes["address_type"] == "internet"  # Internet -> lowercase enum
    assert r.attributes["vpc_id"] == "vpc-1"
    # zone mappings sorted by zone_id, snake_case keys
    assert r.attributes["zone_mappings"] == [
        {"zone_id": "cn-hangzhou-h", "vswitch_id": "vsw-1",
         "addresses": [{"private_ipv4": "10.0.1.5", "public_ip": "47.96.3.3",
                        "eip_allocation_id": "eip-1"}]},
        {"zone_id": "cn-hangzhou-j", "vswitch_id": "vsw-2", "addresses": []},
    ]
    # listeners grouped by server group, ports sorted for stable hash
    assert r.attributes["server_groups"] == [{
        "server_group_id": "sgp-1",
        "server_group_name": "web-group",
        "server_group_type": "Instance",
        "listeners": [{"port": 80, "protocol": "TCP"},
                      {"port": 443, "protocol": "TCP"}],
    }]
    # backend ids deduped + sorted (internal metadata)
    assert r.attributes["_backend_ecs_ids"] == ["i-1", "i-2"]
    assert r.cloud_tags == {"env": "prod"}
    assert r.parent_provider_id == "vpc-1"
    assert r.parent_resource_type == "aliyun_vpc"


def test_map_nlb_without_listeners():
    r = map_nlb(_NLB_RAW, "acc")
    assert r.attributes["dns_name"] == "nlb-abc.cn-hangzhou.nlb.aliyuncsslb.com"
    assert "server_groups" not in r.attributes
    assert "_backend_ecs_ids" not in r.attributes
