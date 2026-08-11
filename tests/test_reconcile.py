"""Tests for diff soft-delete computation (mocked storage access)."""

from __future__ import annotations

from cloudsync.reconcile.soft_delete import reconcile_deleted


class _FakeScalars:
    def __init__(self, rows: list):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeResult:
    def __init__(self, rows: list, first_row=None):
        self._rows = rows
        self._first_row = first_row

    def first(self):
        return self._first_row

    def scalars(self):
        return _FakeScalars(self._rows)


class _FakeSession:
    """Returns (model_id row, stored provider_ids) for the two queries in order."""

    def __init__(self, model_id: int | None, stored_ids: list[str]):
        self._model_id = model_id
        self._stored_ids = stored_ids
        self._calls = 0

    async def execute(self, stmt, params=None):
        self._calls += 1
        if self._calls == 1:
            return _FakeResult([], first_row=(self._model_id,) if self._model_id else None)
        return _FakeResult(self._stored_ids)


async def test_disappeared_resources_emit_delete_messages():
    session = _FakeSession(model_id=7, stored_ids=["i-1", "i-2", "i-3"])
    messages = await reconcile_deleted(
        session,
        provider="aliyun",
        cloud_account="1234567890",
        resource_type="aliyun_ecs",
        seen_ids={"i-1", "i-3"},
    )
    assert [m.provider_id for m in messages] == ["i-2"]
    assert messages[0].event_type == "delete"
    assert messages[0].resource_type == "aliyun_ecs"


async def test_no_disappearance_emits_nothing():
    session = _FakeSession(model_id=7, stored_ids=["i-1", "i-2"])
    messages = await reconcile_deleted(
        session,
        provider="aliyun",
        cloud_account="1234567890",
        resource_type="aliyun_ecs",
        seen_ids={"i-1", "i-2", "i-new"},
    )
    assert messages == []


async def test_unregistered_model_skips_reconciliation():
    session = _FakeSession(model_id=None, stored_ids=["i-1"])
    messages = await reconcile_deleted(
        session,
        provider="aliyun",
        cloud_account="1234567890",
        resource_type="aliyun_unknown",
        seen_ids=set(),
    )
    assert messages == []
