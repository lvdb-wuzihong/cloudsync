"""Tests for the aliyun account root node adapter (config-driven)."""

from __future__ import annotations

from cloudsync.adapters.aliyun.account import map_account
from cloudsync.core.accounts import AccountConfig


def test_map_account_with_display_name():
    acc = AccountConfig(
        provider="aliyun", account_id="1266681915973270", display_name="prod-main",
    )
    r = map_account(acc)
    assert r.resource_type == "aliyun_account"
    assert r.provider_id == "1266681915973270"  # account id is the tree root id
    assert r.name == "prod-main"
    assert r.attributes["alias"] == "prod-main"
    assert r.region == ""  # account is global
    assert r.status == "running"
    assert r.parent_provider_id is None  # root node has no parent


def test_map_account_falls_back_to_id_as_name():
    acc = AccountConfig(provider="aliyun", account_id="123456")
    r = map_account(acc)
    assert r.name == "123456"
    assert "alias" not in r.attributes  # empty display_name dropped
