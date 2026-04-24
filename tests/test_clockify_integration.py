from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-clockify")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-clockify")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-clockify")


class ClockifyServiceTests(unittest.TestCase):
    def test_week_summary_totals_entries_and_daily_rollup(self):
        from app.clockify import build_week_summary, format_hours

        tz = ZoneInfo("America/Los_Angeles")
        week_start = datetime(2026, 4, 20, 0, 0, tzinfo=tz)
        week_end = datetime(2026, 4, 27, 0, 0, tzinfo=tz)
        entries = [
            {
                "id": "entry-1",
                "description": "Open store",
                "timeInterval": {
                    "start": "2026-04-20T16:00:00Z",
                    "end": "2026-04-20T20:30:00Z",
                },
            },
            {
                "id": "entry-2",
                "description": "Close store",
                "timeInterval": {
                    "start": "2026-04-21T00:00:00Z",
                    "end": "2026-04-21T03:00:00Z",
                },
            },
        ]

        summary = build_week_summary(
            entries,
            week_start_local=week_start,
            week_end_local=week_end,
            now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.week_start.isoformat(), "2026-04-20")
        self.assertEqual(summary.week_end_inclusive.isoformat(), "2026-04-26")
        self.assertEqual(summary.total_seconds, 27000)
        self.assertEqual(format_hours(summary.total_seconds), "7h 30m")
        self.assertEqual(summary.daily_totals[0].duration_seconds, 27000)
        self.assertEqual(summary.daily_totals[1].duration_seconds, 0)
        self.assertEqual([row.description for row in summary.entries], ["Open store", "Close store"])

    def test_running_entry_uses_now_for_duration(self):
        from app.clockify import build_week_summary

        tz = ZoneInfo("America/Los_Angeles")
        week_start = datetime(2026, 4, 20, 0, 0, tzinfo=tz)
        week_end = datetime(2026, 4, 27, 0, 0, tzinfo=tz)

        summary = build_week_summary(
            [
                {
                    "id": "running",
                    "description": "Live timer",
                    "timeInterval": {"start": "2026-04-20T16:00:00Z", "end": None},
                }
            ],
            week_start_local=week_start,
            week_end_local=week_end,
            now=datetime(2026, 4, 20, 18, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.running_count, 1)
        self.assertEqual(summary.total_seconds, 9000)

    def test_hours_template_renders_summary(self):
        from types import SimpleNamespace

        from app.clockify import build_week_summary, format_hours
        from app.shared import templates

        tz = ZoneInfo("America/Los_Angeles")
        summary = build_week_summary(
            [
                {
                    "id": "entry-1",
                    "description": "Inventory count",
                    "timeInterval": {
                        "start": "2026-04-20T16:00:00Z",
                        "end": "2026-04-20T18:00:00Z",
                    },
                }
            ],
            week_start_local=datetime(2026, 4, 20, 0, 0, tzinfo=tz),
            week_end_local=datetime(2026, 4, 27, 0, 0, tzinfo=tz),
            now=datetime(2026, 4, 20, 20, 0, tzinfo=timezone.utc),
        )
        html = templates.env.get_template("team/hours.html").render(
            {
                "request": SimpleNamespace(url=SimpleNamespace(path="/team/hours")),
                "title": "My Hours",
                "active": "hours",
                "current_user": SimpleNamespace(role="employee", username="emp", display_name="Emp"),
                "clockify_ready": True,
                "clockify_user_id": "clock-user",
                "clockify_summary": summary,
                "clockify_error": None,
                "format_hours": format_hours,
                "csrf_token": "token",
                "nav_items": [],
                "admin_nav_items": [],
                "tools_nav_items": [],
                "schedule_href": "/team/schedule",
            }
        )

        self.assertIn("Inventory count", html)
        self.assertIn("2h", html)
        self.assertIn("Daily totals", html)

    def test_client_filters_entries_to_requested_range(self):
        from app.clockify import ClockifyClient

        class FakeClockifyClient(ClockifyClient):
            def __init__(self):
                super().__init__(api_key="key", workspace_id="workspace")
                self.params_seen = []

            def _request(self, method, path, *, params=None, json_body=None):
                self.params_seen.append(params or {})
                if params and params.get("page") == 1:
                    return [
                        {
                            "id": "in-range",
                            "timeInterval": {
                                "start": "2026-04-20T16:00:00Z",
                                "end": "2026-04-20T17:00:00Z",
                            },
                        },
                        {
                            "id": "outside",
                            "timeInterval": {
                                "start": "2026-04-28T16:00:00Z",
                                "end": "2026-04-28T17:00:00Z",
                            },
                        },
                    ]
                return []

        client = FakeClockifyClient()
        rows = client.get_user_time_entries(
            "clock-user",
            start_utc=datetime(2026, 4, 20, 7, 0, tzinfo=timezone.utc),
            end_utc=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
        )

        self.assertEqual([row["id"] for row in rows], ["in-range"])
        self.assertEqual(client.params_seen[0]["start"], "2026-04-20T07:00:00Z")
        self.assertEqual(client.params_seen[0]["page-size"], 50)


class ClockifyAdminSyncTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app.models import SQLModel

        cfg.get_settings.cache_clear()
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def test_sync_maps_by_email_without_overwriting_conflicts(self):
        from app.models import AuditLog, EmployeeProfile, User
        from app.pii import encrypt_pii
        from app.routers.team_admin_clockify import sync_clockify_user_ids_by_email

        admin = User(
            id=99,
            username="admin",
            password_hash="x",
            password_salt="x",
            role="admin",
            is_active=True,
        )
        alice = User(
            id=1,
            username="alice",
            password_hash="x",
            password_salt="x",
            role="employee",
            is_active=True,
        )
        bob = User(
            id=2,
            username="bob",
            password_hash="x",
            password_salt="x",
            role="employee",
            is_active=True,
        )
        self.session.add_all([admin, alice, bob])
        self.session.add(
            EmployeeProfile(
                user_id=1,
                email_ciphertext=encrypt_pii("alice@example.com"),
            )
        )
        self.session.add(
            EmployeeProfile(
                user_id=2,
                email_ciphertext=encrypt_pii("bob@example.com"),
                clockify_user_id="existing-bob",
            )
        )
        self.session.commit()

        counts = sync_clockify_user_ids_by_email(
            self.session,
            current_user=admin,
            clockify_users=[
                {"id": "clock-alice", "email": "alice@example.com"},
                {"id": "clock-bob", "email": "bob@example.com"},
            ],
            ip_address="127.0.0.1",
        )

        alice_profile = self.session.get(EmployeeProfile, 1)
        bob_profile = self.session.get(EmployeeProfile, 2)
        self.assertEqual(alice_profile.clockify_user_id, "clock-alice")
        self.assertEqual(bob_profile.clockify_user_id, "existing-bob")
        self.assertEqual(counts["mapped"], 1)
        self.assertEqual(counts["conflicts"], 1)
        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.clockify.sync_users")
        ).first()
        self.assertIsNotNone(audit)
        self.assertNotIn("alice@example.com", audit.details_json)


if __name__ == "__main__":
    unittest.main()
