"""Tests for accounts.yaml loading and credential masking."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cloudsync.core.accounts import load_accounts
from cloudsync.core.exceptions import ConfigError

if TYPE_CHECKING:
    from pathlib import Path


def test_loads_all_providers(accounts_file: Path):
    registry = load_accounts(accounts_file)
    assert len(registry) == 2
    assert registry.get("aliyun", "1234567890") is not None
    assert registry.get("gcp", "my-gcp-project") is not None


def test_registry_miss_returns_none(accounts_file: Path):
    registry = load_accounts(accounts_file)
    assert registry.get("aliyun", "unknown-account") is None
    assert registry.get("aws", "1234567890") is None


def test_missing_file_raises_config_error(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_accounts(tmp_path / "nope.yaml")


def test_invalid_structure_raises_config_error(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("providers: not-a-mapping", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_accounts(bad)


def test_repr_never_leaks_credentials(accounts_file: Path):
    registry = load_accounts(accounts_file)
    account = registry.get("aliyun", "1234567890")
    text = repr(account)
    assert "FAKE_SECRET" not in text
    assert "LTAI_FAKE_ID" not in text
    assert "account_id='1234567890'" in text
