"""Tests for GCP Cloud SQL / Memorystore mapping."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.cloudsql import _split_database_version, map_cloudsql
from cloudsync.adapters.gcp.redis import map_redis

_SQL_ITEM = {
    "name": "prod-db",
    "region": "asia-east2",
    "databaseVersion": "MYSQL_8_0",
    "state": "RUNNABLE",
    "settings": {
        "tier": "db-custom-4-15360",
        "dataDiskSizeGb": "200",
        "userLabels": {"env": "prod"},
        "ipConfiguration": {
            "privateNetwork": "https://www.googleapis.com/compute/v1/projects/p/global/networks/prod-net",
        },
    },
    "ipAddresses": [
        {"type": "PRIMARY", "ipAddress": "35.1.2.3"},
        {"type": "PRIVATE", "ipAddress": "10.0.0.8"},
    ],
}


def test_split_database_version():
    assert _split_database_version("MYSQL_8_0") == ("MYSQL", "8.0")
    assert _split_database_version("POSTGRES_15") == ("POSTGRES", "15")
    assert _split_database_version("") == (None, None)


def test_map_cloudsql_fields():
    r = map_cloudsql(_SQL_ITEM, "p")
    assert r.resource_type == "gcp_cloudsql"
    assert r.provider_id == "prod-db"
    assert r.region == "asia-east2"
    assert r.status == "running"  # RUNNABLE -> running
    assert r.attributes["engine"] == "MYSQL"
    assert r.attributes["engine_version"] == "8.0"
    assert r.attributes["tier"] == "db-custom-4-15360"
    assert r.attributes["storage_gb"] == 200
    assert r.attributes["public_ip"] == "35.1.2.3"
    assert r.attributes["private_ip"] == "10.0.0.8"
    assert r.attributes["vpc_id"] == "prod-net"
    assert r.cloud_tags == {"env": "prod"}
    assert r.parent_provider_id == "prod-net"
    assert r.parent_resource_type == "gcp_vpc"


def test_map_cloudsql_no_public_no_vpc():
    item = {"name": "db2", "region": "us-east1", "databaseVersion": "POSTGRES_15",
            "state": "PENDING_CREATE", "settings": {"tier": "db-g1"},
            "ipAddresses": []}
    r = map_cloudsql(item, "p")
    assert r.status == "maintenance"
    assert "public_ip" not in r.attributes
    assert "private_ip" not in r.attributes
    assert "vpc_id" not in r.attributes
    assert r.parent_provider_id is None


_REDIS = SimpleNamespace(
    name="projects/p/locations/asia-east2/instances/prod-cache",
    redis_version="REDIS_7_2",
    tier="STANDARD_HA",
    memory_size_gb=5,
    host="10.0.1.5",
    port=6379,
    authorized_network="projects/p/global/networks/prod-net",
    state="READY",
    labels={"env": "prod"},
)


def test_map_redis_fields():
    r = map_redis(_REDIS, "p")
    assert r.resource_type == "gcp_redis"
    assert r.provider_id == "asia-east2/prod-cache"  # location-scoped key
    assert r.name == "prod-cache"
    assert r.region == "asia-east2"
    assert r.status == "running"  # READY -> running
    assert r.attributes["engine_version"] == "7.2"
    assert r.attributes["tier"] == "STANDARD_HA"
    assert r.attributes["capacity_mb"] == 5 * 1024
    assert r.attributes["connection_string"] == "10.0.1.5"
    assert r.attributes["port"] == 6379
    assert r.attributes["vpc_id"] == "prod-net"
    assert r.parent_provider_id == "prod-net"
    assert r.parent_resource_type == "gcp_vpc"
