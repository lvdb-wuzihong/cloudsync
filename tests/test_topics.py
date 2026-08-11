"""Tests for topic set resolution."""

from __future__ import annotations

import cloudsync.adapters.aliyun  # noqa: F401  (registers adapter)
import cloudsync.adapters.gcp  # noqa: F401  (registers adapter)
from cloudsync.adapters.base import registered_providers
from cloudsync.kafka.producer import resolve_topics


def test_default_topics_derived_from_registered_adapters():
    topics = resolve_topics("", registered_providers())
    assert topics == ["cloud-sync-aliyun", "cloud-sync-gcp"]


def test_env_override_wins_over_derivation():
    topics = resolve_topics("custom-a, custom-b", registered_providers())
    assert topics == ["custom-a", "custom-b"]


def test_blank_override_falls_back_to_derivation():
    topics = resolve_topics("   ", ["aliyun"])
    assert topics == ["cloud-sync-aliyun"]


def test_registered_providers_sorted():
    assert registered_providers() == ["aliyun", "gcp"]
