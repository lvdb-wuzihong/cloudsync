"""Tests for status vocabulary normalization."""

from __future__ import annotations

import pytest

from cloudsync.normalize.status import STATUS_VOCABULARY, normalize_status


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Running", "running"),
        ("available", "running"),
        ("STOPPED", "stopped"),
        ("migrating", "maintenance"),
        ("Starting", "maintenance"),
        ("some-future-state", "unknown"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_normalize_status(raw, expected):
    assert normalize_status(raw) == expected


def test_result_always_in_vocabulary():
    for raw in ("Running", "weird", "", None):
        assert normalize_status(raw) in STATUS_VOCABULARY
