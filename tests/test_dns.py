"""Tests for DNS zone/record mapping (aliyun dicts + GCP protos simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.aliyun.dns import (
    fqdn as aliyun_fqdn,
    map_dns_record as aliyun_map_record,
    map_dns_zone as aliyun_map_zone,
)
from cloudsync.adapters.gcp.dns import (
    _normalize_value,
    map_dns_record as gcp_map_record,
    map_dns_zone as gcp_map_zone,
)

_ALIYUN_ZONE = {
    "DomainName": "example.com",
    "RecordCount": 12,
    "DnsServers": {"DnsServer": ["dns1.hichina.com", "dns2.hichina.com"]},
    "InstanceId": "dns-cn-xxx",
}

_ALIYUN_RECORD = {
    "RecordId": "123456",
    "RR": "www",
    "Type": "A",
    "Value": "1.2.3.4",
    "TTL": 600,
    "Line": "default",
    "Status": "ENABLE",
}


def test_aliyun_fqdn_rules():
    assert aliyun_fqdn("@", "example.com") == "example.com"
    assert aliyun_fqdn("*", "example.com") == "*.example.com"
    assert aliyun_fqdn("www", "example.com") == "www.example.com"
    # already absolute stays untouched
    assert aliyun_fqdn("www.example.com", "example.com") == "www.example.com"


def test_aliyun_map_zone():
    r = aliyun_map_zone(_ALIYUN_ZONE, "acc")
    assert r.resource_type == "dns_zone"
    assert r.provider_id == "example.com"
    assert r.attributes["zone_type"] == "public"
    assert r.attributes["record_count"] == 12
    assert r.attributes["dns_servers"] == ["dns1.hichina.com", "dns2.hichina.com"]
    assert "expire_at" not in r.attributes  # free instance: no expiry


def test_aliyun_map_record():
    r = aliyun_map_record(_ALIYUN_RECORD, "example.com", "acc")
    assert r.resource_type == "dns_record"
    assert r.provider_id == "123456"  # cloud RecordId
    assert r.name == "www.example.com"  # FQDN
    assert r.status == "running"  # ENABLE -> running
    assert r.attributes["rr"] == "www"
    assert r.attributes["record_type"] == "A"
    assert r.attributes["value"] == "1.2.3.4"
    assert r.attributes["ttl"] == 600
    assert r.attributes["policy_type"] == "simple"
    assert r.attributes["raw"]["RR"] == "www"
    assert r.parent_provider_id == "example.com"
    assert r.parent_resource_type == "dns_zone"


def test_aliyun_map_record_line_policy():
    raw = dict(_ALIYUN_RECORD, Line="telecom", Type="CNAME", Value="cdn.example.com")
    r = aliyun_map_record(raw, "example.com", "acc")
    assert r.attributes["policy_type"] == "line"
    assert r.attributes["policy_key"] == "telecom"


_GCP_ZONE = SimpleNamespace(
    name="example-com",
    dns_name="example.com.",
    visibility="public",
    name_servers=["ns-cloud-a1.googledomains.com.", "ns-cloud-a2.googledomains.com."],
)

_GCP_RRSET_MX = SimpleNamespace(
    name="example.com.",
    type="MX",
    ttl=3600,
    rrdatas=["10 mail.example.com."],
)


def test_gcp_map_zone():
    r = gcp_map_zone(_GCP_ZONE, "proj")
    assert r.resource_type == "dns_zone"
    assert r.provider_id == "example.com"  # trailing dot stripped
    assert r.attributes["zone_type"] == "public"
    assert r.attributes["dns_servers"] == [
        "ns-cloud-a1.googledomains.com", "ns-cloud-a2.googledomains.com",
    ]


def test_normalize_value_mx():
    value, priority = _normalize_value("MX", "10 mail.example.com.")
    assert value == "mail.example.com"
    assert priority == 10


def test_normalize_value_txt_and_cname():
    assert _normalize_value("TXT", '"v=spf1 -all"') == ("v=spf1 -all", None)
    assert _normalize_value("CNAME", "target.example.com.") == ("target.example.com", None)


def test_gcp_map_record_synthesized_id():
    value, priority = _normalize_value("MX", "10 mail.example.com.")
    r = gcp_map_record(_GCP_RRSET_MX, value, priority, "example.com", "proj")
    assert r.resource_type == "dns_record"
    assert r.provider_id == "example.com:example.com:MX:mail.example.com"
    assert r.name == "example.com"  # apex FQDN
    assert r.attributes["rr"] == "@"
    assert r.attributes["record_type"] == "MX"
    assert r.attributes["priority"] == 10
    assert r.attributes["value"] == "mail.example.com"
    assert r.attributes["ttl"] == 3600
    assert r.attributes["policy_type"] == "simple"
    assert r.parent_provider_id == "example.com"
