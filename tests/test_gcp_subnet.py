"""Tests for GCP subnet mapping (google-cloud-compute Subnetwork simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.subnet import map_subnet

_SUBNET = SimpleNamespace(
    id=111222333,
    name="web-subnet",
    region="https://www.googleapis.com/compute/v1/projects/p/regions/asia-east2",
    network="https://www.googleapis.com/compute/v1/projects/p/global/networks/prod-net",
    ip_cidr_range="10.0.0.0/24",
    private_ip_google_access=True,
    secondary_ip_ranges=[
        SimpleNamespace(range_name="svc", ip_cidr_range="10.4.0.0/20"),
        SimpleNamespace(range_name="pods", ip_cidr_range="10.2.0.0/16"),
    ],
)


def test_map_subnet_fields():
    r = map_subnet(_SUBNET, "my-gcp-project", "asia-east2")
    assert r.resource_type == "gcp_subnet"
    assert r.provider_id == "asia-east2/web-subnet"  # region/name 键（防跨域重名撞键）
    assert r.name == "web-subnet"
    assert r.region == "asia-east2"  # from caller scope
    assert r.zone == ""
    assert r.status == "running"  # alive = running
    assert r.attributes["cidr_block"] == "10.0.0.0/24"
    assert r.attributes["private_google_access"] is True
    # sorted by range_name for stable hash
    assert r.attributes["secondary_ranges"] == [
        {"range_name": "pods", "ip_cidr_range": "10.2.0.0/16"},
        {"range_name": "svc", "ip_cidr_range": "10.4.0.0/20"},
    ]
    assert r.attributes["vpc_id"] == "prod-net"  # URL baselined
    assert r.cloud_tags == {}  # GCP subnetwork has no labels
    # Subnet belongs to VPC (网络归属), joined by network name
    assert r.parent_provider_id == "prod-net"
    assert r.parent_resource_type == "gcp_vpc"


def test_map_subnet_minimal():
    subnet = SimpleNamespace(
        id=1, name="default", region="", network="",
        ip_cidr_range="", private_ip_google_access=False,
        secondary_ip_ranges=[],
    )
    r = map_subnet(subnet, "proj", "us-east1")
    assert "cidr_block" not in r.attributes
    assert r.attributes["private_google_access"] is False
    assert "secondary_ranges" not in r.attributes
    assert "vpc_id" not in r.attributes
    assert r.parent_provider_id is None
