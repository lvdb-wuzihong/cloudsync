"""AWS ALB (Application Load Balancer) adapter: ELBv2 + listener/target chains.

Filter Type="application" from describe_load_balancers (ELBv2 returns ALB and
NLB together; NLB is a separate model, not yet implemented). The seven-layer
routing summary is the model's core: per listener describe_rules resolves
host/path conditions and the forward action's target group; target groups
carry the backend resolution chain — instance-type targets are resolved via
describe_target_health into _backend_instance_ids for the consumer's
aws_alb -> aws_ec2 "负载均衡后端" edges.

provider_id uses "{region}/{name}" (name unique per region+account only,
collision discipline). status comes from State.Code (active -> running).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.aws.client import (
    PROVIDER,
    build_client,
    fetch,
    last_segment,
    region_scope,
)
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.aws.alb")

RESOURCE_TYPE = "aws_alb"
API_NAME = "describe_load_balancers"
PAGE_SIZE = 400  # ELBv2 PageSize upper bound


def _condition_values(rule: dict[str, Any], field: str) -> list[str]:
    """host-header / path-pattern condition values (both field shapes)."""
    values: list[str] = []
    for cond in rule.get("Conditions") or []:
        if (cond.get("Field") or "") != field:
            continue
        values.extend(cond.get("Values") or [])
        values.extend((cond.get(f"{field.title().replace('-', '')}Config") or {}).get("Values") or [])
    return sorted(v for v in values if v)


def _forward_target(rule: dict[str, Any]) -> str | None:
    """TargetGroupArn of the first forward action (empty-safe)."""
    for action in rule.get("Actions") or []:
        if action.get("Type") == "forward":
            return action.get("TargetGroupArn")
    return None


def normalize_listeners(
    listeners: list[dict[str, Any]],
    rules_by_listener: dict[str, list[dict[str, Any]]],
    tg_name_by_arn: dict[str, str],
) -> list[dict[str, Any]]:
    """Listeners + rules -> [{port, protocol, rules: [{host, path, target_group}]}]."""
    result: list[dict[str, Any]] = []
    for ln in sorted(listeners, key=lambda l: (l.get("Port") or 0, l.get("Protocol") or "")):
        rules: list[dict[str, Any]] = []
        for rule in rules_by_listener.get(ln.get("ListenerArn", "") or "", []):
            target_arn = _forward_target(rule)
            rules.append({
                "priority": rule.get("Priority"),
                "host": _condition_values(rule, "host-header") or None,
                "path": _condition_values(rule, "path-pattern") or None,
                "target_group": tg_name_by_arn.get(target_arn or ""),
            })
        rules = [{k: v for k, v in r.items() if v is not None} for r in rules]
        result.append({
            "port": ln.get("Port"),
            "protocol": ln.get("Protocol"),
            "rules": rules or None,
        })
    return result


def map_alb(
    raw: dict[str, Any], account_id: str, region: str,
    listeners: list[dict[str, Any]], target_groups: list[dict[str, Any]],
    backend_instance_ids: list[str],
) -> NormalizedResource:
    """Assemble the ALB NormalizedResource from the full collection chain."""
    name = raw.get("LoadBalancerName", "")
    attributes = {
        # 字段 code 对齐 CMDB 模型定义
        "dns_name": raw.get("DNSName") or None,
        "scheme": raw.get("Scheme") or None,
        "listeners": listeners or None,
        "target_groups": target_groups or None,
        "ip_address_type": raw.get("IpAddressType") or None,
        "vpc_id": raw.get("VpcId") or None,
        # 下划线内部键：消费端建 aws_alb -> aws_ec2 边用，前端不渲染
        "_backend_instance_ids": sorted(set(backend_instance_ids)) or None,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    vpc_id = raw.get("VpcId") or ""
    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        # 名字仅 region 内唯一，provider_id 带 region 前缀防跨域撞键
        provider_id=f"{region}/{name}",
        cloud_account=account_id,
        name=name,
        region=region,
        zone="",
        status=normalize_status((raw.get("State") or {}).get("Code") or ""),
        attributes=attributes,
        cloud_tags={},  # ALB tags need a separate DescribeTags call; not a model field
        parent_provider_id=vpc_id or None,
        parent_resource_type="aws_vpc" if vpc_id else None,
    )


async def _paginate(client: Any, api: str, account: AccountConfig,
                    resource_type: str, **kwargs: Any) -> list[dict[str, Any]]:
    """ELBv2 marker pagination helper (PageSize/Marker -> NextMarker)."""
    items: list[dict[str, Any]] = []
    marker: str | None = None
    while True:
        call_kwargs = {**kwargs, "PageSize": PAGE_SIZE}
        if marker:
            call_kwargs["Marker"] = marker
        response = await fetch(
            lambda kw=call_kwargs: getattr(client, api)(**kw),
            account=account, resource_type=resource_type, api=api,
        )
        items.extend(response.get(_RESULT_KEY[api], []))
        marker = response.get("NextMarker")
        if not marker:
            break
    return items


_RESULT_KEY = {
    "describe_load_balancers": "LoadBalancers",
    "describe_listeners": "Listeners",
    "describe_target_groups": "TargetGroups",
    "describe_rules": "Rules",
}


async def list_alb(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Yield ALBs (Type=application) of every region with the routing chain."""
    for region in await region_scope(account):
        client = build_client(account, "elbv2", region)
        lbs = await _paginate(client, "describe_load_balancers", account, RESOURCE_TYPE)
        for raw in lbs:
            if raw.get("Type") != "application":
                continue  # ELBv2 returns ALB and NLB together; NLB is a separate model
            lb_arn = raw.get("LoadBalancerArn", "")
            listeners = await _paginate(
                client, "describe_listeners", account, RESOURCE_TYPE,
                LoadBalancerArn=lb_arn,
            )
            tgs = await _paginate(
                client, "describe_target_groups", account, RESOURCE_TYPE,
                LoadBalancerArn=lb_arn,
            )
            tg_name_by_arn = {tg.get("TargetGroupArn", ""): tg.get("TargetGroupName", "")
                              for tg in tgs}

            # per-listener rules, pulled exactly once per listener
            rules_by_listener: dict[str, list[dict[str, Any]]] = {}
            for ln in listeners:
                rules_by_listener[ln.get("ListenerArn", "")] = await _paginate(
                    client, "describe_rules", account, RESOURCE_TYPE,
                    ListenerArn=ln.get("ListenerArn", ""),
                )

            backend_ids: list[str] = []
            for tg in tgs:
                if tg.get("TargetType") != "instance":
                    continue
                # instance targets: describe_target_health resolves Target.Id = instance id
                health = await fetch(
                    lambda a=tg.get("TargetGroupArn", ""): client.describe_target_health(
                        TargetGroupArn=a),
                    account=account, resource_type=RESOURCE_TYPE,
                    api="describe_target_health",
                )
                for desc in health.get("TargetHealthDescriptions", []):
                    target_id = (desc.get("Target") or {}).get("Id") or ""
                    if target_id.startswith("i-"):
                        backend_ids.append(target_id)

            yield map_alb(
                raw, account.account_id, region,
                normalize_listeners(listeners, rules_by_listener, tg_name_by_arn),
                [
                    {
                        "name": tg.get("TargetGroupName"),
                        "target_type": tg.get("TargetType"),
                        "protocol": tg.get("Protocol"),
                        "port": tg.get("Port"),
                    }
                    for tg in sorted(tgs, key=lambda t: t.get("TargetGroupName", ""))
                ],
                backend_ids,
            )
        logger.info("ALB fetch completed",
                    extra={"provider": PROVIDER, "account": account.account_id,
                           "region": region, "count": sum(
                               1 for r in lbs if r.get("Type") == "application")})
