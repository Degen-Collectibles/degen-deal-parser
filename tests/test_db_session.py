from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

import app.db as db_module
from app.db import managed_session


def test_managed_session_preserves_caller_operational_error_type() -> None:
    original = OperationalError("SELECT broken", {}, Exception("database is locked"))

    with pytest.raises(OperationalError) as raised:
        with managed_session():
            raise original

    assert raised.value is original


def test_managed_session_closes_session_when_postgres_health_check_fails(monkeypatch) -> None:
    error = OperationalError("SELECT 1", {}, Exception("db unavailable"))
    sessions: list[FakeSession] = []

    class FakeSession:
        def __init__(self) -> None:
            self.closed = False
            sessions.append(self)

        def exec(self, _statement):
            raise error

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(db_module, "database_url", "postgresql+psycopg://test/db")
    monkeypatch.setattr(db_module, "Session", lambda _engine: FakeSession())
    monkeypatch.setattr(db_module, "recent_db_failure", lambda: False)
    monkeypatch.setattr(db_module, "mark_db_failure", lambda: None)
    monkeypatch.setattr(db_module, "clear_db_failure", lambda: None)
    monkeypatch.setattr(db_module.time, "sleep", lambda _seconds: None)

    with pytest.raises(OperationalError) as raised:
        with db_module.managed_session():
            pytest.fail("health check failure should happen before yield")

    assert raised.value is error
    assert len(sessions) == 5
    assert all(session.closed for session in sessions)
