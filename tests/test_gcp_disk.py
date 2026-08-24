"""Tests for GCP disk / account mapping (google-cloud-compute proto simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.account import map_account
from cloudsync.adapters.gcp.disk import map_disk
from cloudsync.core.accounts import AccountConfig

_DISK = SimpleNamespace(
    id=444555,
    name="web-1-disk",
    zone="https://www.googleapis.com/compute/v1/projects/p/zones/asia-east2-a",
    status="READY",
    type_="https://www.googleapis.com/compute/v1/projects/p/zones/asia-east2-a/diskTypes/pd-ssd",
    size_gb=100,
    labels={"env": "prod"},
    users=[
        "https://www.googleapis.com/compute/v1/projects/p/zones/asia-east2-a/instances/web-1",
    ],
    disk_encryption_key=SimpleNamespace(
        kms_key_name="projects/p/locations/x/keyRings/r/cryptoKeys/k", sha256=""
    ),
    source_image_encryption_key=None,
    source_snapshot_encryption_key=None,
)


def test_map_disk_fields():
    r = map_disk(_DISK, "my-proj", "asia-east2")
    assert r.resource_type == "gcp_disk"
    # zone-scoped provider_id (cross-zone same names must not collide)
    assert r.provider_id == "asia-east2-a/web-1-disk"
    assert r.name == "web-1-disk"
    assert r.region == "asia-east2"
    assert r.zone == "asia-east2-a"
    assert r.status == "running"  # READY -> running
    assert r.attributes["disk_type"] == "pd-ssd"  # type_ URL baselined
    assert r.attributes["size_gb"] == 100
    assert r.attributes["encrypted"] is True  # CMEK key present
    assert r.attributes["users"] == [{"name": "web-1", "zone": "asia-east2-a"}]
    assert r.cloud_tags == {"env": "prod"}
    # Disk belongs to the project account root (账号归属)
    assert r.parent_provider_id == "my-proj"
    assert r.parent_resource_type == "gcp_account"


def test_map_disk_unencrypted_no_users():
    disk = SimpleNamespace(
        id=1, name="data", zone="", status="CREATING", type_="",
        size_gb=0, labels={}, users=[],
        disk_encryption_key=SimpleNamespace(kms_key_name="", sha256=""),
        source_image_encryption_key=None,
        source_snapshot_encryption_key=None,
    )
    r = map_disk(disk, "p", "us-east1")
    assert r.status == "maintenance"  # CREATING -> maintenance
    assert r.attributes["encrypted"] is False
    assert "users" not in r.attributes
    assert "disk_type" not in r.attributes
    assert "size_gb" not in r.attributes


def test_map_account_root_node():
    acc = AccountConfig(provider="gcp", account_id="povison-pord", display_name="prod")
    r = map_account(acc)
    assert r.resource_type == "gcp_account"
    assert r.provider_id == "povison-pord"  # project id is the tree root id
    assert r.name == "prod"
    assert r.region == ""
    assert r.parent_provider_id is None
