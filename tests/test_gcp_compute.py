"""Tests for GCP GCE mapping (google-cloud-compute proto messages simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.compute import _guess_os, map_compute

_INSTANCE = SimpleNamespace(
    id=123456789,
    name="web-1",
    status="RUNNING",
    machine_type="https://www.googleapis.com/compute/v1/projects/p/zones/asia-east2-a/machineTypes/e2-medium",
    labels={"env": "prod"},
    scheduling=SimpleNamespace(preemptible=False, provisioning_model="STANDARD"),
    disks=[
        SimpleNamespace(
            boot=True, disk_size_gb=20,
            source="https://www.googleapis.com/compute/v1/projects/p/zones/asia-east2-a/disks/web-1",
        ),
        SimpleNamespace(boot=False, disk_size_gb=100, source=""),
    ],
    network_interfaces=[SimpleNamespace(
        network_i_p="10.0.0.5",
        network="https://www.googleapis.com/compute/v1/projects/p/global/networks/default",
        subnetwork="https://www.googleapis.com/compute/v1/projects/p/regions/asia-east2/subnetworks/web-subnet",
        access_configs=[SimpleNamespace(nat_i_p="34.1.2.3")],
    )],
)


def test_map_compute_fields():
    r = map_compute(
        _INSTANCE, "my-gcp-project",
        zone="asia-east2-a", region="asia-east2",
        machine_type="e2-medium", cpu=2, memory_gb=4,
        os_name="ubuntu",
    )
    assert r.resource_type == "gcp_compute"
    assert r.provider_id == "123456789"  # numeric id stringified
    assert r.name == "web-1"
    assert r.region == "asia-east2"
    assert r.zone == "asia-east2-a"
    assert r.status == "running"  # RUNNING -> running
    assert r.attributes["machine_type"] == "e2-medium"
    assert r.attributes["cpu"] == 2
    assert r.attributes["memory_gb"] == 4
    assert r.attributes["os"] == "ubuntu"
    assert r.attributes["private_ip"] == "10.0.0.5"
    assert r.attributes["public_ip"] == "34.1.2.3"
    assert r.attributes["disk_size_gb"] == 120  # boot + data disks summed
    assert r.attributes["spot"] is False
    assert r.attributes["subnet_id"] == "web-subnet"  # URL baselined
    assert r.attributes["vpc_id"] == "default"
    assert r.cloud_tags == {"env": "prod"}
    # GCE belongs to subnet (belongs_to 网络归属)
    assert r.parent_provider_id == "web-subnet"
    assert r.parent_resource_type == "gcp_subnet"


def test_map_compute_spot_and_no_public_ip():
    instance = SimpleNamespace(
        id=2, name="spot-1", status="TERMINATED",
        machine_type="", labels={},
        scheduling=SimpleNamespace(preemptible=False, provisioning_model="SPOT"),
        disks=[], network_interfaces=[SimpleNamespace(
            network_i_p="10.0.0.9", network="", subnetwork="",
            access_configs=[],
        )],
    )
    r = map_compute(instance, "proj", zone="z", region="r")
    assert r.status == "stopped"  # TERMINATED -> stopped
    assert r.attributes["spot"] is True
    assert "public_ip" not in r.attributes
    assert "machine_type" not in r.attributes
    assert "subnet_id" not in r.attributes
    assert r.parent_provider_id is None


def test_map_compute_no_disks_drops_size():
    instance = SimpleNamespace(
        id=3, name="bare", status="STAGING", machine_type="", labels={},
        scheduling=SimpleNamespace(preemptible=True, provisioning_model="STANDARD"),
        disks=[], network_interfaces=[],
    )
    r = map_compute(instance, "proj", zone="z", region="r")
    assert r.status == "maintenance"  # STAGING -> maintenance
    assert r.attributes["spot"] is True  # preemptible flag counts as spot
    assert "disk_size_gb" not in r.attributes
    assert "private_ip" not in r.attributes


def test_guess_os_from_image_url():
    url = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy"
    assert _guess_os(url) == "ubuntu"
    assert _guess_os("windows-server-2022-dc") == "windows"
    assert _guess_os("cos-109") == "cos"
    assert _guess_os("my-custom-image") is None
    assert _guess_os(None) is None
