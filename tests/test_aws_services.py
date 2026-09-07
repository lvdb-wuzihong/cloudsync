"""Tests for AWS S3 / CloudFront / ELB / ALB mapping (boto3 dicts simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.aws.alb import map_alb, normalize_listeners
from cloudsync.adapters.aws.cloudfront import map_distribution, normalize_origins
from cloudsync.adapters.aws.elb import map_elb
from cloudsync.adapters.aws.s3 import infer_acl, map_s3

_ACL_GRANTS = [
    {"Grantee": {"Type": "CanonicalUser"}, "Permission": "FULL_CONTROL"},
    {"Grantee": {"Type": "Group",
                 "URI": "http://acs.amazonaws.com/groups/global/AllUsers"},
     "Permission": "READ"},
]


def test_infer_acl():
    assert infer_acl(None) is None
    assert infer_acl([{"Grantee": {"Type": "CanonicalUser"},
                       "Permission": "FULL_CONTROL"}]) == "private"
    assert infer_acl(_ACL_GRANTS) == "public-read"


def test_map_s3_fields():
    from datetime import datetime, timezone
    r = map_s3(
        "web-assets", "123456789012", "us-east-1",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        "private", versioning=True, block_public_access=True,
    )
    assert r.resource_type == "aws_s3"
    assert r.provider_id == "web-assets"
    assert r.name == "web-assets"
    assert r.region == "us-east-1"
    assert r.attributes["acl"] == "private"
    assert r.attributes["versioning"] is True
    assert r.attributes["block_public_access"] is True
    assert r.attributes["endpoint"] == "web-assets.s3.us-east-1.amazonaws.com"
    assert r.attributes["creation_time"] == "2026-01-01T00:00:00+00:00"
    assert r.status is None  # 桶无生命周期状态
    assert r.parent_provider_id == "123456789012"  # aws_account 根
    assert r.parent_resource_type == "aws_account"


_DISTRIBUTION = {
    "Id": "E1234ABC", "DomainName": "d1111.cloudfront.net",
    "Enabled": True, "HttpVersion": "http2",
    "Aliases": {"Quantity": 1, "Items": ["cdn.example.com"]},
    "Origins": {"Quantity": 1, "Items": [{
        "Id": "s3-origin", "DomainName": "web-assets.s3.us-east-1.amazonaws.com",
    }]},
}


def test_normalize_origins():
    origins = normalize_origins(_DISTRIBUTION["Origins"])
    assert origins == [{"origin_id": "s3-origin",
                        "domain_name": "web-assets.s3.us-east-1.amazonaws.com"}]


def test_map_distribution_fields():
    r = map_distribution(_DISTRIBUTION, "123456789012")
    assert r.resource_type == "aws_cloudfront"
    assert r.provider_id == "E1234ABC"
    assert r.name == "d1111.cloudfront.net"
    assert r.region == ""  # 全局资源
    assert r.status == "running"  # Enabled -> running
    assert r.attributes["domain_name"] == "d1111.cloudfront.net"
    assert r.attributes["aliases"] == ["cdn.example.com"]
    assert r.attributes["http_version"] == "http2"
    assert r.attributes["origins"][0]["domain_name"] == "web-assets.s3.us-east-1.amazonaws.com"
    assert r.parent_provider_id == "123456789012"
    assert r.parent_resource_type == "aws_account"


_CLB = {
    "LoadBalancerName": "web-clb", "DNSName": "web-clb-1.us-east-1.elb.amazonaws.com",
    "Scheme": "internet-facing", "VPCId": "vpc-0abc",
    "Instances": [{"InstanceId": "i-0abc"}, {"InstanceId": "i-0def"}],
    "ListenerDescriptions": [
        {"Listener": {"Protocol": "HTTPS", "LoadBalancerPort": 443, "InstancePort": 8443}},
        {"Listener": {"Protocol": "HTTP", "LoadBalancerPort": 80, "InstancePort": 8080}},
    ],
}


def test_map_elb_fields():
    r = map_elb(_CLB, "123456789012", "us-east-1")
    assert r.resource_type == "aws_elb"
    assert r.provider_id == "us-east-1/web-clb"  # region 前缀防撞键
    assert r.name == "web-clb"
    assert r.attributes["dns_name"] == "web-clb-1.us-east-1.elb.amazonaws.com"
    assert r.attributes["scheme"] == "internet-facing"
    assert r.attributes["vpc_id"] == "vpc-0abc"
    # listeners 按端口排序（稳定哈希）
    assert r.attributes["listeners"][0]["load_balancer_port"] == 80
    assert r.attributes["_backend_instance_ids"] == ["i-0abc", "i-0def"]
    assert r.parent_provider_id == "vpc-0abc"
    assert r.status is None  # Classic ELB 无状态字段


_ALB = SimpleNamespace(
    LoadBalancerArn="arn:aws:elasticloadbalancing:us-east-1:1:loadbalancer/app/web/abc",
    LoadBalancerName="web-alb", DNSName="web-alb-1.us-east-1.elb.amazonaws.com",
    Scheme="internet-facing", VpcId="vpc-0abc", IpAddressType="ipv4",
    State=SimpleNamespace(Code="active"),
)

_LISTENERS = [
    SimpleNamespace(ListenerArn="arn:listener/443", Port=443, Protocol="HTTPS"),
    SimpleNamespace(ListenerArn="arn:listener/80", Port=80, Protocol="HTTP"),
]

_RULES_443 = [{
    "Priority": "1", "IsDefault": False,
    "Conditions": [{"Field": "host-header",
                    "HostHeaderConfig": {"Values": ["api.example.com"]}}],
    "Actions": [{"Type": "forward", "TargetGroupArn": "arn:tg/api"}],
}]


def test_normalize_listeners_with_rules():
    tg_map = {"arn:tg/api": "api-tg"}
    listeners = normalize_listeners(_LISTENERS, {"arn:listener/443": _RULES_443}, tg_map)
    # 按端口排序：80 在前
    assert listeners[0]["port"] == 80
    assert listeners[0]["rules"] is None  # 无规则 listener
    assert listeners[1]["port"] == 443
    assert listeners[1]["rules"] == [{"priority": "1", "host": ["api.example.com"],
                                      "target_group": "api-tg"}]


def test_map_alb_fields():
    r = map_alb(
        _ALB, "123456789012", "us-east-1",
        normalize_listeners(_LISTENERS, {"arn:listener/443": _RULES_443},
                            {"arn:tg/api": "api-tg"}),
        [{"name": "api-tg", "target_type": "instance", "protocol": "HTTP", "port": 8080}],
        ["i-0abc", "i-0def", "i-0abc"],  # 去重前
    )
    assert r.resource_type == "aws_alb"
    assert r.provider_id == "us-east-1/web-alb"  # region 前缀防撞键
    assert r.name == "web-alb"
    assert r.status == "running"  # State.Code active
    assert r.attributes["dns_name"] == "web-alb-1.us-east-1.elb.amazonaws.com"
    assert r.attributes["ip_address_type"] == "ipv4"
    assert r.attributes["target_groups"][0]["name"] == "api-tg"
    assert r.attributes["_backend_instance_ids"] == ["i-0abc", "i-0def"]  # 去重排序
    assert r.parent_provider_id == "vpc-0abc"
    assert r.parent_resource_type == "aws_vpc"
