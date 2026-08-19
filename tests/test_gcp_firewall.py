"""Tests for GCP firewall synthesized mapping (Firewall proto simulated)."""

from __future__ import annotations

from types import SimpleNamespace

from cloudsync.adapters.gcp.firewall import _normalize_rule, map_firewall

_FW_ALLOW = SimpleNamespace(
    name="allow-ssh",
    network="https://www.googleapis.com/compute/v1/projects/p/global/networks/prod-net",
    priority=1000,
    direction="INGRESS",
    allowed=[SimpleNamespace(i_p_protocol="tcp", ports=["22"])],
    denied=[],
    source_ranges=["0.0.0.0/0"],
    destination_ranges=[],
    source_tags=[],
    target_tags=["web"],
    source_service_accounts=[],
    target_service_accounts=[],
    disabled=False,
    log_config=SimpleNamespace(enable=True),
    description="ssh access",
)

_FW_DENY = SimpleNamespace(
    name="deny-egress",
    network="https://www.googleapis.com/compute/v1/projects/p/global/networks/prod-net",
    priority=2000,
    direction="EGRESS",
    allowed=[],
    denied=[SimpleNamespace(i_p_protocol="all", ports=[])],
    source_ranges=[],
    destination_ranges=["10.0.0.0/8"],
    source_tags=[],
    target_tags=[],
    source_service_accounts=[],
    target_service_accounts=[],
    disabled=True,
    log_config=SimpleNamespace(enable=False),
    description="",
)


def test_normalize_rule_allow():
    rule = _normalize_rule(_FW_ALLOW)
    assert rule["name"] == "allow-ssh"
    assert rule["priority"] == 1000
    assert rule["direction"] == "ingress"
    assert rule["action"] == "allow"
    assert rule["protocols"] == [{"ip_protocol": "tcp", "ports": ["22"]}]
    assert rule["source_ranges"] == ["0.0.0.0/0"]
    assert rule["target_tags"] == ["web"]
    assert rule["disabled"] is False
    assert rule["log_enabled"] is True


def test_normalize_rule_deny():
    rule = _normalize_rule(_FW_DENY)
    assert rule["action"] == "deny"
    assert rule["protocols"] == [{"ip_protocol": "all"}]  # i_p_protocol 取值，empty ports 丢弃
    assert rule["disabled"] is True
    assert rule["log_enabled"] is False
    assert "description" not in rule  # empty dropped


def test_map_firewall_synthesized():
    rules = [_normalize_rule(_FW_DENY), _normalize_rule(_FW_ALLOW)]
    r = map_firewall("prod-net", "povison-pord", rules)
    assert r.resource_type == "gcp_firewall"
    assert r.provider_id == "fw:povison-pord:prod-net"  # appendix B #19
    assert r.name == "fw:prod-net"
    assert r.region == "" and r.zone == ""  # network-scoped
    assert r.status == "running"
    # rules sorted deterministically (allow-ssh < deny-egress by canonical json)
    assert [x["name"] for x in r.attributes["rules"]] == ["allow-ssh", "deny-egress"]
    assert len(r.attributes["rules_hash"]) == 16
    assert r.attributes["vpc_id"] == "prod-net"
    # parent = VPC by name (防火墙归属)
    assert r.parent_provider_id == "prod-net"
    assert r.parent_resource_type == "gcp_vpc"


def test_map_firewall_hash_stable_across_input_order():
    rules_a = [_normalize_rule(_FW_ALLOW), _normalize_rule(_FW_DENY)]
    rules_b = [_normalize_rule(_FW_DENY), _normalize_rule(_FW_ALLOW)]
    assert (
        map_firewall("prod-net", "p", rules_a).attributes["rules_hash"]
        == map_firewall("prod-net", "p", rules_b).attributes["rules_hash"]
    )
