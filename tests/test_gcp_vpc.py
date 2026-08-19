"""Tests for GCP VPC mapping (google-cloud-compute Network simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.vpc import map_vpc

_NETWORK = SimpleNamespace(
    id=987654321,
    name="prod-net",
    auto_create_subnetworks=False,
    routing_config=SimpleNamespace(routing_mode="REGIONAL"),
    mtu=1460,
)


def test_map_vpc_fields():
    r = map_vpc(_NETWORK, "my-gcp-project")
    assert r.resource_type == "gcp_vpc"
    assert r.provider_id == "prod-net"  # name-based id (children carry names)
    assert r.name == "prod-net"
    assert r.region == ""  # VPC is global
    assert r.zone == ""
    assert r.status == "running"  # alive = running
    assert r.attributes["subnet_mode"] == "custom"
    assert r.attributes["routing_mode"] == "regional"
    assert r.attributes["mtu"] == 1460
    assert r.cloud_tags == {}  # GCP VPC has no labels
    # VPC belongs to the project account root (项目归属)
    assert r.parent_provider_id == "my-gcp-project"
    assert r.parent_resource_type == "gcp_account"


def test_map_vpc_auto_mode_network():
    net = SimpleNamespace(
        id=1, name="default", auto_create_subnetworks=True,
        routing_config=SimpleNamespace(routing_mode="GLOBAL"), mtu=0,
    )
    r = map_vpc(net, "proj")
    assert r.attributes["subnet_mode"] == "auto"
    assert r.attributes["routing_mode"] == "global"
    assert "mtu" not in r.attributes  # 0 dropped
