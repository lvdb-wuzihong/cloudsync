"""GCP firewall adapter: FirewallsClient.list -> synthesized per-VPC instance.

GCP firewall rules are individual resources attached to a network; the CMDB
models ONE synthesized instance per VPC (presets appendix B #19:
provider_id = fw:{project}:{vpc_name}, relation #13 is 1:1). All rules of a
network are aggregated into the rules json field with a rules_hash (design
doc section 5.3: unchanged rule sets keep the content hash stable).

Firewalls are global (no region binding), so the accounts.yaml region scope
does not apply. Proto fields are read defensively (getattr): message
structure drifts across SDK versions and must not take the round down.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_firewalls_client,
    fetch,
    last_segment,
    project_of,
)
from cloudsync.normalize.hashing import compute_rules_hash
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.firewall")

RESOURCE_TYPE = "gcp_firewall"
PAGE_SIZE = 500  # ListFirewalls upper bound


def _normalize_protocols(entries: Any) -> list[dict[str, Any]]:
    """Allowed/Denied entries -> [{ip_protocol, ports}] (snake_case)."""
    normalized = []
    for entry in entries or []:
        item = {
            "ip_protocol": getattr(entry, "i_p_protocol", "") or getattr(entry, "ip_protocol", "") or None,
            "ports": list(getattr(entry, "ports", None) or []) or None,
        }
        normalized.append({k: v for k, v in item.items() if v is not None})
    return normalized


def _normalize_rule(firewall: Any) -> dict[str, Any]:
    """One GCP firewall resource -> cross-vendor rule dict (snake_case)."""
    allowed = _normalize_protocols(getattr(firewall, "allowed", None))
    denied = _normalize_protocols(getattr(firewall, "denied", None))
    rule = {
        "name": getattr(firewall, "name", "") or None,
        "priority": getattr(firewall, "priority", 0) or None,
        "direction": (getattr(firewall, "direction", "") or "").lower() or None,
        # GCP rules are allow-XOR-deny; action derived from which list is set
        "action": "allow" if allowed else ("deny" if denied else None),
        "protocols": allowed or denied or None,
        "source_ranges": list(getattr(firewall, "source_ranges", None) or []) or None,
        "destination_ranges": list(getattr(firewall, "destination_ranges", None) or []) or None,
        "source_tags": list(getattr(firewall, "source_tags", None) or []) or None,
        "target_tags": list(getattr(firewall, "target_tags", None) or []) or None,
        "source_service_accounts": list(getattr(firewall, "source_service_accounts", None) or []) or None,
        "target_service_accounts": list(getattr(firewall, "target_service_accounts", None) or []) or None,
        "disabled": bool(getattr(firewall, "disabled", False)),
        "log_enabled": bool(
            getattr(getattr(firewall, "log_config", None), "enable", False)
        ),
        "description": getattr(firewall, "description", "") or None,
    }
    return {k: v for k, v in rule.items() if v is not None}


def _sort_key(rule: dict[str, Any]) -> str:
    """Deterministic ordering for the rules list (stable content hash)."""
    return json.dumps(rule, sort_keys=True, ensure_ascii=False, default=str)


def map_firewall(
    vpc_name: str, project: str, rules: list[dict[str, Any]],
) -> NormalizedResource:
    """Synthesize one gcp_firewall instance per VPC (appendix B #19).

    provider_id = fw:{project}:{vpc_name}; parent points at gcp_vpc by name
    (name-based provider_id convention for GCP edge joining).
    """
    rules = sorted(rules, key=_sort_key)
    attributes = {
        "rules": rules,
        "rules_hash": compute_rules_hash(rules),
        "vpc_id": vpc_name,
    }
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=f"fw:{project}:{vpc_name}",
        cloud_account=project,
        name=f"fw:{vpc_name}",
        region="",  # firewalls are network-scoped (global)
        zone="",
        status=normalize_status("available"),  # policy object; alive = running
        attributes=attributes,
        cloud_tags={},
        parent_provider_id=vpc_name,
        parent_resource_type="gcp_vpc",
    )


async def list_firewall(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Fetch all firewalls, aggregate per VPC, yield synthesized instances."""
    started = time.perf_counter()
    project = project_of(account)
    client = build_firewalls_client(account)
    pager = await fetch(
        lambda: client.list({"project": project, "max_results": PAGE_SIZE}),
        account=account,
        resource_type=RESOURCE_TYPE,
        api="FirewallsClient.list",
    )
    by_network: dict[str, list[dict[str, Any]]] = {}
    total = 0
    for firewall in pager:
        total += 1
        network_name = last_segment(getattr(firewall, "network", "") or "")
        if not network_name:
            continue
        by_network.setdefault(network_name, []).append(_normalize_rule(firewall))

    count = 0
    for vpc_name in sorted(by_network):
        count += 1
        yield map_firewall(vpc_name, project, by_network[vpc_name])

    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Firewall fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "rules": total, "duration_ms": round(duration_ms, 2)})
