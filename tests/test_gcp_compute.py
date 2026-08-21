"""Tests for GCP GCE mapping (google-cloud-compute proto messages simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.compute import (
    _guess_os,
    _guess_os_from_attached,
    map_compute,
)

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
    assert r.attributes["subnet_id"] == "asia-east2/web-subnet"  # region/name 键，同 gcp_subnet provider_id
    assert r.attributes["vpc_id"] == "default"
    assert r.cloud_tags == {"env": "prod"}
    # GCE belongs to subnet (belongs_to 网络归属)
    assert r.parent_provider_id == "asia-east2/web-subnet"
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


def test_guess_os_from_boot_disk_licenses():
    # licenses 末段带版本号，识别出 OS 家族项目时直接取完整 slug
    licenses = ["https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-2204-lts"]
    assert _guess_os(licenses, None) == "ubuntu-2204-lts"
    win = ["https://www.googleapis.com/compute/v1/projects/windows-cloud/global/licenses/windows-server-2022-dc"]
    assert _guess_os(win, None) == "windows-server-2022-dc"


def test_guess_os_gke_license_slug_keyword():
    # GKE 发行项目不在家族映射里，但许可证 slug 含关键词 → 取整个 slug（带版本）
    licenses = ["https://www.googleapis.com/compute/v1/projects/gke-node-images/global/licenses/ubuntu-gke-2404-1-33-amd64"]
    assert _guess_os(licenses, None) == "ubuntu-gke-2404-1-33-amd64"


def test_guess_os_falls_back_to_source_image():
    # licenses 无信号时回退镜像名（命中关键词时返回完整镜像名，带版本）
    licenses = ["https://www.googleapis.com/compute/v1/projects/my-proj/global/licenses/custom"]
    image = "https://www.googleapis.com/compute/v1/projects/p/global/images/ubuntu-gke-2404-1-33-amd64-v20260325"
    assert _guess_os(licenses, image) == "ubuntu-gke-2404-1-33-amd64-v20260325"


def test_guess_os_none_when_no_signal():
    assert _guess_os(None, None) is None
    assert _guess_os([], "") is None


def test_guess_os_from_attached_disk():
    disk = SimpleNamespace(
        licenses=["https://www.googleapis.com/compute/v1/projects/debian-cloud/global/licenses/debian-12-bookworm"],
        initialize_params=SimpleNamespace(source_image=""),
    )
    assert _guess_os_from_attached(disk) == "debian-12-bookworm"
    assert _guess_os_from_attached(None) is None
