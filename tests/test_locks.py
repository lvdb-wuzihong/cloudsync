"""Tests for advisory lock wrappers (mocked session)."""

from __future__ import annotations

from cloudsync.scheduler.locks import release_task_lock, try_acquire_task_lock


class _FakeResult:
    def __init__(self, value: bool):
        self._value = value

    def scalar(self):
        return self._value


class _FakeSession:
    """Records executed statements and returns a preset lock result."""

    def __init__(self, lock_granted: bool):
        self.lock_granted = lock_granted
        self.executed: list[str] = []

    async def execute(self, stmt, params=None):
        self.executed.append(str(stmt))
        return _FakeResult(self.lock_granted)


async def test_acquire_returns_true_when_lock_granted():
    session = _FakeSession(lock_granted=True)
    assert await try_acquire_task_lock(session, 42) is True
    assert "pg_try_advisory_lock" in session.executed[0]


async def test_acquire_returns_false_when_lock_held_elsewhere():
    session = _FakeSession(lock_granted=False)
    assert await try_acquire_task_lock(session, 42) is False


async def test_release_issues_unlock_with_same_key():
    session = _FakeSession(lock_granted=True)
    await release_task_lock(session, 42)
    assert "pg_advisory_unlock" in session.executed[0]
