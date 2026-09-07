"""Tests for AWS EC2-family mapping (boto3 dicts simulated)."""

from __future__ import annotations

from cloudsync.adapters.aws.account import map_account
from cloudsync.adapters.aws.ec2 import map_ec2
from cloudsync.adapters.aws.eip import map_eip
from cloudsync.adapters.aws.security_group import map_security_group, normalize_rules
from cloudsync.adapters.aws.vpc import map_vpc
from cloudsync.core.accounts import AccountConfig

_ACCOUNT = AccountConfig(provider="aws", account_id="123456789012", display_name="main")

_VPC = {
    "VpcId": "vpc-0abc", "CidrBlock": "10.0.0.0/16", "IsDefault": True,
    "State": "available",
    "Tags": [{"Key": "Name", "Value": "prod-net"}],
}

_EIP = {
    "AllocationId": "eipalloc-0abc", "PublicIp": "52.1.2.3",
    "PrivateIpAddress": "10.0.0.5", "InstanceId": "i-0abc",
    "Tags": [{"Key": "env", "Value": "prod"}],
}

_SG = {
    "GroupId": "sg-0abc", "GroupName": "web-sg", "Description": "web",
    "VpcId": "vpc-0abc",
    "IpPermissions": [{
        "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "ssh"}],
    }],
    "IpPermissionsEgress": [{
        "IpProtocol": "-1",
        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
    }],
    "Tags": [],
}

_EC2 = {
    "InstanceId": "i-0abc", "ImageId": "ami-0abc",
    "InstanceType": "t3.medium",
    "State": {"Name": "running", "Code": 16},
    "PrivateIpAddress": "10.0.0.5", "PublicIpAddress": "52.1.2.3",
    "SubnetId": "subnet-0abc", "VpcId": "vpc-0abc",
    "PlatformDetails": "Linux/UNIX",
    "InstanceLifecycle": "spot",
    "Placement": {"AvailabilityZone": "us-east-1a"},
    "SecurityGroups": [{"GroupId": "sg-0abc", "GroupName": "web-sg"}],
    "LaunchTime": __import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").timezone.utc),
    "Tags": [{"Key": "Name", "Value": "web-1"}],
}


def test_map_account_root():
    r = map_account(_ACCOUNT)
    assert r.resource_type == "aws_account"
    assert r.provider_id == "123456789012"
    assert r.name == "main"
    assert r.attributes["alias"] == "main"
    assert r.region == ""
    assert r.status is None  # 根节点无状态概念
    assert r.parent_provider_id is None


def test_map_vpc_fields():
    r = map_vpc(_VPC, "123456789012", "us-east-1")
    assert r.resource_type == "aws_vpc"
    assert r.provider_id == "vpc-0abc"
    assert r.name == "prod-net"  # Name tag
    assert r.region == "us-east-1"
    assert r.status == "running"  # available -> running
    assert r.attributes["cidr_block"] == "10.0.0.0/16"
    assert r.attributes["is_default"] is True
    assert r.parent_provider_id == "123456789012"  # aws_account 根
    assert r.parent_resource_type == "aws_account"


def test_map_eip_fields():
    r = map_eip(_EIP, "123456789012", "us-east-1")
    assert r.resource_type == "aws_eip"
    assert r.provider_id == "eipalloc-0abc"
    assert r.name == "52.1.2.3"
    assert r.attributes["ip_address"] == "52.1.2.3"
    assert r.attributes["private_ip"] == "10.0.0.5"
    assert r.attributes["bind_instance_id"] == "i-0abc"
    assert r.status is None  # EIP 无生命周期状态
    assert r.cloud_tags == {"env": "prod"}


def test_map_security_group_rules():
    r = map_security_group(_SG, "123456789012", "us-east-1")
    assert r.resource_type == "aws_security_group"
    assert r.provider_id == "sg-0abc"
    assert r.status is None  # 无生命周期状态，不硬塞
    assert r.attributes["vpc_id"] == "vpc-0abc"
    assert r.parent_provider_id == "vpc-0abc"
    rules = r.attributes["rules"]
    # ingress tcp/22 展开一条 + egress -1 一条；排序稳定供哈希
    assert {x["direction"] for x in rules} == {"ingress", "egress"}
    ingress = next(x for x in rules if x["direction"] == "ingress")
    assert ingress["ip_protocol"] == "tcp"
    assert ingress["from_port"] == 22
    assert ingress["source_cidr_ip"] == "0.0.0.0/0"
    assert ingress["description"] == "ssh"
    egress = next(x for x in rules if x["direction"] == "egress")
    assert egress["ip_protocol"] == "-1"
    assert egress["dest_cidr_ip"] == "0.0.0.0/0"
    assert r.attributes["rules_hash"]


def test_normalize_rules_empty_permission():
    # protocol -1 with no ranges -> single all-allow entry, never empty
    rules = normalize_rules({"IpPermissions": [{"IpProtocol": "-1"}]})
    assert rules == [{"direction": "ingress", "ip_protocol": "-1"}]


def test_map_ec2_fields():
    r = map_ec2(_EC2, "123456789012", "us-east-1")
    assert r.resource_type == "aws_ec2"
    assert r.provider_id == "i-0abc"
    assert r.name == "web-1"
    assert r.region == "us-east-1"
    assert r.zone == "us-east-1a"
    assert r.status == "running"
    assert r.attributes["instance_type"] == "t3.medium"
    assert r.attributes["os"] == "linux"  # PlatformDetails derived
    assert r.attributes["spot"] is True
    assert r.attributes["private_ip"] == "10.0.0.5"
    assert r.attributes["public_ip"] == "52.1.2.3"
    assert r.attributes["creation_time"] == "2026-01-01T00:00:00+00:00"
    assert r.attributes["vpc_id"] == "vpc-0abc"
    assert r.attributes["subnet_id"] == "subnet-0abc"
    assert r.attributes["image_id"] == "ami-0abc"
    assert r.attributes["_security_group_ids"] == ["sg-0abc"]  # 内部键供建边
    assert r.parent_provider_id == "vpc-0abc"
    assert r.parent_resource_type == "aws_vpc"
