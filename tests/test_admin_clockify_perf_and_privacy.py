from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-clockify-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-clockify-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-clockify-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-clockify-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class _FakeRequest:
    def __init__(self, current_user):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(path="/team/admin/clockify", scheme="http", netloc="testserver")


class _FakeClockifyClient:
    def __init__(self, entries=None):
        self.entries = entries or []
        self.entry_calls = 0

    def workspace_info(self):
        return {"id": "workspace", "name": "Test Workspace"}

    def list_workspace_users(self, status="ALL"):
        return [{"id": "clock-1", "name": "Alice", "email": "alice@example.com", "status": status}]

    def get_user_time_entries(self, user_id, *, start_utc, end_utc, **_kw):
        self.entry_calls += 1
        return list(self.entries)

    def user_week_summary(self, user_id, *, today=None, settings=None):
        from app.clockify import build_week_summary, clockify_week_bounds

        start, end = clockify_week_bounds(today, settings=settings)
        return build_week_summary(
            self.get_user_time_entries(
                user_id,
                start_utc=start.astimezone(timezone.utc),
                end_utc=end.astimezone(timezone.utc),
            ),
            week_start_local=start,
            week_end_local=end,
            settings=settings,
        )


class _CountingSummaryClient:
    def __init__(self):
        self.calls = 0

    def user_week_summary(self, user_id, *, today=None, settings=None):
        self.calls += 1
        return SimpleNamespace(user_id=user_id, call_number=self.calls)


class ClockifyAdminPerfPrivacyTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app.db import seed_employee_portal_defaults
        from app.models import User
        from app.routers import team_admin_clockify as mod

        cfg.get_settings.cache_clear()
        mod._CLOCKIFY_WEEK_CACHE.clear()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=1,
            username="clockify-admin",
            password_hash="x",
            password_salt="x",
            display_name="Clockify Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        from app.routers import team_admin_clockify as mod

        mod._CLOCKIFY_WEEK_CACHE.clear()
        self.session.close()

    def _settings(self):
        return SimpleNamespace(
            employee_portal_enabled=True,
            clockify_api_key="key",
            clockify_workspace_id="workspace",
            clockify_timezone="America/Los_Angeles",
        )

    def _render(self, *, include_hours="0", entries=None):
        from app.routers import team_admin_clockify as mod

        fake_client = _FakeClockifyClient(entries=entries)
        with patch.object(mod, "get_settings", return_value=self._settings()), \
             patch.object(mod, "clockify_client_from_settings", return_value=fake_client):
            response = mod.admin_clockify_page(
                _FakeRequest(self.admin),
                include_hours=include_hours,
                session=self.session,
            )
        return response.body.decode("utf-8"), fake_client

    def test_page_does_not_fetch_hours_by_default(self):
        html, client = self._render(include_hours="0")
        self.assertIn("Not loaded", html)
        self.assertEqual(client.entry_calls, 0)

    def test_page_fetches_hours_when_explicitly_requested(self):
        entries = [
            {
                "id": "entry-1",
                "description": "Open",
                "timeInterval": {
                    "start": "2026-04-20T16:00:00Z",
                    "end": "2026-04-20T18:00:00Z",
                },
            }
        ]
        html, client = self._render(include_hours="1", entries=entries)
        self.assertIn("2h", html)
        self.assertEqual(client.entry_calls, 1)

    def test_week_summary_is_cached_within_60_seconds(self):
        from app.routers import team_admin_clockify as mod

        client = _CountingSummaryClient()
        first = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        second = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        self.assertIs(first, second)
        self.assertEqual(client.calls, 1)

    def test_week_summary_cache_expires_after_60_seconds(self):
        from app.routers import team_admin_clockify as mod

        client = _CountingSummaryClient()
        first = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        key = next(iter(mod._CLOCKIFY_WEEK_CACHE))
        mod._CLOCKIFY_WEEK_CACHE[key] = (0.0, first)
        with patch.object(mod.time, "time", return_value=120.0):
            second = mod._cached_user_week_summary(
                client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
            )
        self.assertIsNot(first, second)
        self.assertEqual(client.calls, 2)

    def test_displayed_emails_are_masked(self):
        html, _client = self._render(include_hours="0")
        self.assertNotIn("alice@example.com", html)
        self.assertIn("ali***@example.com", html)
