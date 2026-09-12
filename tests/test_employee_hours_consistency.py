"""The dashboard widget and /team/hours must report the same week.

The dashboard excluded break entries and applied the missed-break deduction;
/team/hours summed raw Clockify durations. Same week, one click apart, two
different numbers -- and the larger one was on the page called "My Hours",
while the smaller one was what payroll actually pays.

Both now read employee_week_hours(). These tests pin the agreement rather than
either individual number, so the two cannot drift apart again.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "hours-consistency-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "hours-consistency-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "hours-consistency-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "hours-consistency-admin-password")

LA = ZoneInfo("America/Los_Angeles")
MONDAY = date(2026, 4, 20)


def _settings():
    return SimpleNamespace(
        employee_portal_enabled=True,
        clockify_api_key="key",
        clockify_workspace_id="workspace",
        clockify_timezone="America/Los_Angeles",
        clockify_base_url="https://api.clockify.me/api/v1",
        clockify_timeout_seconds=5.0,
    )


class EmployeeHoursConsistencyTests(unittest.TestCase):
    def setUp(self):
        from app.db import seed_employee_portal_defaults
        from app.models import EmployeeProfile, SQLModel, User

        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        self.user = User(
            id=7,
            username="worker",
            password_hash="x",
            password_salt="x",
            display_name="Worker",
            role="employee",
            is_active=True,
        )
        self.session.add(self.user)
        self.session.add(EmployeeProfile(user_id=7, clockify_user_id="ck-7"))
        self.session.commit()
        self.session.refresh(self.user)

    def tearDown(self):
        self.session.close()

    def _cache_entry(self, start_local, end_local, description="Shift"):
        from app.models import ClockifyTimeEntry

        start = start_local.astimezone(timezone.utc)
        end = end_local.astimezone(timezone.utc)
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id=f"e-{description}-{start.isoformat()}",
                clockify_user_id="ck-7",
                user_id=7,
                description=description,
                start_at=start,
                end_at=end,
                duration_seconds=int((end - start).total_seconds()),
                is_running=False,
                is_deleted=False,
            )
        )
        self.session.commit()

    def _week(self, today=MONDAY):
        from app.routers import team as mod

        with patch.object(mod, "get_settings", return_value=_settings()):
            return mod.employee_week_hours(
                self.session, self.user, today=today, settings=_settings()
            )

    def _dashboard(self, today=MONDAY):
        from app.routers import team as mod

        with patch.object(mod, "get_settings", return_value=_settings()):
            return mod._employee_dashboard_pay_summary(
                self.session, self.user, today=today
            )

    # -- agreement -----------------------------------------------------------

    def test_totals_agree_when_a_break_is_clocked(self):
        """A clocked break is unpaid; both surfaces must exclude it."""
        self._cache_entry(
            datetime(2026, 4, 20, 9, 0, tzinfo=LA),
            datetime(2026, 4, 20, 12, 0, tzinfo=LA),
            description="Open store",
        )
        self._cache_entry(
            datetime(2026, 4, 20, 12, 0, tzinfo=LA),
            datetime(2026, 4, 20, 12, 30, tzinfo=LA),
            description="Lunch break",
        )
        self._cache_entry(
            datetime(2026, 4, 20, 12, 30, tzinfo=LA),
            datetime(2026, 4, 20, 17, 0, tzinfo=LA),
            description="Afternoon",
        )

        week = self._week()
        dashboard = self._dashboard()

        from app.team.clockify import format_hours

        self.assertEqual(dashboard["hours_label"], format_hours(week["total_work_seconds"]))
        self.assertEqual(week["total_work_seconds"], int(7.5 * 3600))
        self.assertEqual(week["total_break_seconds"], 30 * 60)

    def test_totals_agree_when_the_break_is_auto_deducted(self):
        """Over 5h with no break clocked: 30m comes off paid time on both."""
        self._cache_entry(
            datetime(2026, 4, 20, 9, 0, tzinfo=LA),
            datetime(2026, 4, 20, 17, 0, tzinfo=LA),
            description="Long day",
        )

        week = self._week()
        dashboard = self._dashboard()

        from app.team.clockify import format_hours

        self.assertEqual(week["total_auto_break_seconds"], 30 * 60)
        self.assertEqual(week["total_work_seconds"], int(7.5 * 3600))
        self.assertEqual(dashboard["hours_label"], format_hours(week["total_work_seconds"]))

    def test_paid_total_is_below_the_raw_clockify_total(self):
        """Guards the specific regression: the page must not show raw hours."""
        self._cache_entry(
            datetime(2026, 4, 20, 9, 0, tzinfo=LA),
            datetime(2026, 4, 20, 17, 0, tzinfo=LA),
            description="Long day",
        )

        week = self._week()
        raw_seconds = sum(entry.duration_seconds for entry in week["entries"])

        self.assertEqual(raw_seconds, 8 * 3600)
        self.assertLess(week["total_work_seconds"], raw_seconds)

    def test_days_sum_to_the_week_total(self):
        self._cache_entry(
            datetime(2026, 4, 20, 9, 0, tzinfo=LA),
            datetime(2026, 4, 20, 13, 0, tzinfo=LA),
        )
        self._cache_entry(
            datetime(2026, 4, 22, 9, 0, tzinfo=LA),
            datetime(2026, 4, 22, 12, 0, tzinfo=LA),
        )

        week = self._week()

        self.assertEqual(len(week["days"]), 7)
        self.assertEqual(
            sum(day["work_seconds"] for day in week["days"]),
            week["total_work_seconds"],
        )

    def test_week_bounds_cover_monday_to_sunday(self):
        from app.routers import team as mod

        # An empty current-week cache uses the live fallback. Keep this calendar
        # fixture synthetic rather than calling Clockify with dummy credentials.
        with patch.object(mod, "clockify_client_from_settings") as client:
            client.return_value.get_user_time_entries.return_value = []
            week = self._week()
            client.return_value.get_user_time_entries.assert_called_once_with(
                "ck-7",
                start_utc=datetime(2026, 4, 20, 7, tzinfo=timezone.utc),
                end_utc=datetime(2026, 4, 27, 7, tzinfo=timezone.utc),
            )

        self.assertEqual(week["week_start"], MONDAY)
        self.assertEqual(week["week_end_inclusive"], MONDAY + timedelta(days=6))
        self.assertEqual(week["timezone_name"], "America/Los_Angeles")

    def test_unlinked_employee_reports_nothing_without_error(self):
        from app.models import EmployeeProfile

        profile = self.session.get(EmployeeProfile, 7)
        profile.clockify_user_id = None
        self.session.add(profile)
        self.session.commit()

        week = self._week()

        self.assertFalse(week["linked"])
        self.assertEqual(week["total_work_seconds"], 0)
        self.assertEqual(week["error"], "")


class EmployeeHoursPageTests(unittest.TestCase):
    """The rendered page must show the paid figure, not the raw one."""

    def setUp(self):
        from app.db import seed_employee_portal_defaults
        from app.models import ClockifyTimeEntry, EmployeeProfile, SQLModel, User

        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.user = User(
            id=7,
            username="worker",
            password_hash="x",
            password_salt="x",
            display_name="Worker",
            role="employee",
            is_active=True,
        )
        self.session.add(self.user)
        self.session.add(EmployeeProfile(user_id=7, clockify_user_id="ck-7"))
        start = datetime(2026, 4, 20, 9, 0, tzinfo=LA).astimezone(timezone.utc)
        end = datetime(2026, 4, 20, 17, 0, tzinfo=LA).astimezone(timezone.utc)
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id="e-1",
                clockify_user_id="ck-7",
                user_id=7,
                description="Long day",
                start_at=start,
                end_at=end,
                duration_seconds=8 * 3600,
                is_deleted=False,
            )
        )
        self.session.commit()
        self.session.refresh(self.user)

    def tearDown(self):
        self.session.close()

    def _render(self):
        from app.routers import team as mod

        request = SimpleNamespace(
            state=SimpleNamespace(current_user=self.user),
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path="/team/hours"),
            scope={"path": "/team/hours"},
            headers={},
            cookies={},
            session={},
            query_params={},
        )
        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "_portal_today", return_value=MONDAY
        ):
            response = mod.team_hours(request, session=self.session)
        return response.body.decode("utf-8")

    def test_page_shows_paid_hours_not_raw_hours(self):
        html = self._render()

        self.assertIn("Paid hours this week", html)
        self.assertIn("7h 30m", html)

    def test_page_explains_the_automatic_deduction(self):
        html = self._render()

        self.assertIn("Breaks this week", html)
        self.assertIn("deducted automatically", html)

    def test_page_shows_estimated_pay_it_promises(self):
        """The unlinked empty state advertises estimated pay; deliver it."""
        html = self._render()

        self.assertIn("Estimated pay", html)

    def test_page_does_not_leak_raw_api_errors(self):
        from app.routers import team as mod
        from app.team.clockify import ClockifyApiError

        request = SimpleNamespace(
            state=SimpleNamespace(current_user=self.user),
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path="/team/hours"),
            scope={"path": "/team/hours"},
            headers={},
            cookies={},
            session={},
            query_params={},
        )
        boom = ClockifyApiError("Clockify said: {internal trace 12345}")
        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "_portal_today", return_value=MONDAY
        ), patch.object(
            mod, "employee_week_hours", side_effect=None
        ) as fake:
            fake.return_value = {
                "linked": True,
                "error": str(boom),
                "start_local": datetime(2026, 4, 20, tzinfo=LA),
                "total_work_seconds": 0,
                "adjusted_by_day": {},
                "entries": [],
                "days": [],
                "week_start": MONDAY,
                "week_end_inclusive": MONDAY,
                "timezone_name": "America/Los_Angeles",
                "total_break_seconds": 0,
                "total_auto_break_seconds": 0,
                "running_count": 0,
            }
            html = mod.team_hours(request, session=self.session).body.decode("utf-8")

        self.assertNotIn("internal trace 12345", html)
        self.assertIn("could not be loaded", html)


if __name__ == "__main__":
    unittest.main()
