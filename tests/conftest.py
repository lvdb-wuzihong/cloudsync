"""Shared test fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

SAMPLE_ACCOUNTS_YAML = """
providers:
  aliyun:
    - account_id: "1234567890"
      display_name: prod-main
      access_key_id: "LTAI_FAKE_ID"
      access_key_secret: "FAKE_SECRET"
      regions: [cn-beijing]
  gcp:
    - account_id: "my-gcp-project"
      display_name: gcp-main
      service_account_json: '{"type": "service_account"}'
      regions: [asia-east2]
"""


@pytest.fixture
def accounts_file(tmp_path: Path) -> Path:
    """Write a sample accounts.yaml and return its path."""
    path = tmp_path / "accounts.yaml"
    path.write_text(SAMPLE_ACCOUNTS_YAML, encoding="utf-8")
    return path
