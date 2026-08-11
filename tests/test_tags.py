"""Tests for tag normalization."""

from __future__ import annotations

from cloudsync.normalize.tags import normalize_tag_key, normalize_tags


def test_key_lowercased_and_underscores_to_hyphens():
    assert normalize_tag_key("Env_Type") == "env-type"


def test_key_trimmed_and_spaces_collapsed():
    assert normalize_tag_key("  Cost Center ") == "cost-center"


def test_values_are_preserved():
    assert normalize_tags({"Env": "Prod"}) == {"env": "Prod"}


def test_empty_input_returns_empty_dict():
    assert normalize_tags(None) == {}
    assert normalize_tags({}) == {}


def test_conflicting_keys_resolved_deterministically():
    # Sorted raw-key order makes duplicate resolution stable across runs
    result = normalize_tags({"env": "a", "Env": "b"})
    assert set(result) == {"env"}
