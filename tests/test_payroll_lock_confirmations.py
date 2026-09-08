"""Irreversible lock actions must say so before the click.

Two affordances permanently lock timecard approval status with no way back --
there is no unlock route, and set_timecard_day_status refuses to touch a locked
approval row afterwards. Cached hours and rates remain live, so the warnings
must not promise frozen hours or pay:

  1. "Lock payroll window" on /team/admin/payroll -- a primary-styled button,
     one click, which also locks days still marked Pending (lock_payroll_window
     skips only rejected ones).
  2. "Locked" in the per-day status dropdown on the timecard page, sitting one
     row below "Needs fix" in the same control used for routine triage.

These tests pin the warnings, not the styling.
"""
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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "lock-confirm-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "lock-confirm-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "lock-confirm-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "lock-confirm-admin-password")

WEEK = date(2026, 4, 20)


class _FakeRequest:
    def __init__(self, user, path="/team/admin/employees/2/timecards"):
        self.state = SimpleNamespace(current_user=user)
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(path=path)
        self.scope = {"path": path}
        self.headers = {}
        self.cookies = {}
        self.session = {}
        self.query_params = {}


class _FakeClockifyClient:
    def __init__(self, entries=None):
        self.entries = entries or []

    def get_user_time_entries(self, user_id, *, start_utc, end_utc, **_kw):
        return list(self.entries)

    def user_week_summary(self, user_id, *, today=None, settings=None):
        from app.team.clockify import build_week_summary, clockify_week_bounds

        start, end = clockify_week_bounds(today, settings=settings)
        return build_week_summary(
            list(self.entries),
            week_start_local=start,
            week_end_local=end,
            settings=settings,
        )


def _settings():
    return SimpleNamespace(
        employee_portal_enabled=True,
        clockify_api_key="key",
        clockify_workspace_id="workspace",
        clockify_timezone="America/Los_Angeles",
        clockify_base_url="https://api.clockify.me/api/v1",
        clockify_timeout_seconds=5.0,
    )


class _Base(unittest.TestCase):
    def setUp(self):
        from app.db import seed_employee_portal_defaults
        from app.models import SQLModel

        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = self._seed_user(1, "admin1", "admin")
        self.employee = self._seed_user(2, "worker", "employee")

    def tearDown(self):
        self.session.close()

    def _seed_user(self, uid, username, role):
        from app.models import User

        user = User(
            id=uid,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username.title(),
            role=role,
            is_active=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _seed_approval(self, day, status):
        from app.models import TimecardApproval

        self.session.add(
            TimecardApproval(user_id=self.employee.id, work_date=day, status=status)
        )
        self.session.commit()


class TimecardLockDropdownTests(_Base):
    def _render(self, week=WEEK):
        from app.routers import team_admin_employees_timecards as mod

        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "clockify_client_from_settings", return_value=_FakeClockifyClient()
        ):
            response = mod.admin_employee_timecards(
                _FakeRequest(self.admin),
                self.employee.id,
                week=week.isoformat(),
                session=self.session,
            )
        return response.body.decode("utf-8")

    def test_editable_rows_carry_a_lock_confirmation(self):
        html = self._render()
        self.assertIn("data-confirm-lock", html)
        self.assertIn("no unlock", html)
        self.assertIn("Hours and pay are not frozen", html)
        self.assertNotIn("hours can never be edited again", html)

    def test_confirmation_only_fires_for_the_locked_value(self):
        """Routine approve/reject must stay a single click."""
        html = self._render()
        self.assertIn("select.value !== 'locked'", html)

    def test_locked_row_shows_a_note_instead_of_dead_controls(self):
        self._seed_approval(WEEK, "locked")
        html = self._render()

        self.assertIn("Locked for payroll", html)
        self.assertIn("Hours and pay can still change", html)
        self.assertNotIn("Day totals can no longer be edited", html)
        # The row's editable form must be gone -- saving it could only error.
        self.assertNotIn(
            f'name="work_date" value="{WEEK.isoformat()}"',
            html,
            "a locked day must not render an editable status form",
        )

    def test_unlocked_rows_still_render_their_form(self):
        self._seed_approval(WEEK, "approved")
        html = self._render()

        self.assertIn(f'name="work_date" value="{WEEK.isoformat()}"', html)
        self.assertNotIn("Locked for payroll", html)


class PayrollLockButtonTests(_Base):
    def _render(self):
        from app.routers import team_admin_clockify as mod

        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "clockify_is_configured", return_value=False
        ):
            # Called directly, so FastAPI's Query defaults are not resolved.
            response = mod.admin_payroll_page(
                _FakeRequest(self.admin, path="/team/admin/payroll"),
                range_key="custom",
                start=WEEK.isoformat(),
                end=WEEK.isoformat(),
                show_inactive="1",
                flash=None,
                error=None,
                session=self.session,
            )
        return response.body.decode("utf-8")

    def test_lock_button_requires_confirmation(self):
        html = self._render()
        self.assertIn("data-confirm-lock", html)
        self.assertIn("window.confirm(form.getAttribute('data-confirm-lock'))", html)

    def test_confirmation_states_it_cannot_be_undone(self):
        html = self._render()
        self.assertIn("cannot be undone", html)
        self.assertIn("Hours and pay are not frozen", html)
        self.assertIn("can change later exports", html)
        self.assertNotIn("locked days can never be edited again", html)

    def test_confirm_message_has_no_raw_newline_in_js_source(self):
        """A newline inside an inline onsubmit string literal is a JS syntax
        error -- the handler would throw and the form would submit unconfirmed.
        The message must ride in a data attribute instead."""
        import re

        import html as html_mod

        markup = self._render()
        self.assertNotIn("onsubmit=", markup)
        attr = re.search(r'data-confirm-lock="([^"]*)"', markup, re.S)
        self.assertIsNotNone(attr)
        # Encoded in the source, a real newline once the browser decodes the
        # attribute -- which is why it is safe here and would not be in JS.
        self.assertNotIn(chr(10), attr.group(1))
        self.assertIn(chr(10), html_mod.unescape(attr.group(1)))

    def test_helper_text_states_permanence_and_no_unlock(self):
        html = self._render()
        self.assertIn("Permanent.", html)
        self.assertIn("There is no unlock for approval status.", html)
        self.assertIn("later exports can change", html)


class PayrollLockPendingDisclosureTests(_Base):
    """lock_payroll_window skips only rejected days, so Pending approvals also
    get locked. That was invisible before the click."""

    def setUp(self):
        super().setUp()
        from app.models import EmployeeProfile

        # A salaried employee produces active payroll days without needing any
        # Clockify entries (see _active_payroll_days_by_user).
        self.session.add(
            EmployeeProfile(
                user_id=self.employee.id,
                compensation_type="monthly_salary",
                hire_date=date(2026, 1, 1),
            )
        )
        self.session.commit()

    def _render(self):
        from app.routers import team_admin_clockify as mod

        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "clockify_is_configured", return_value=False
        ):
            response = mod.admin_payroll_page(
                _FakeRequest(self.admin, path="/team/admin/payroll"),
                range_key="custom",
                start=WEEK.isoformat(),
                end=WEEK.isoformat(),
                show_inactive="1",
                flash=None,
                error=None,
                session=self.session,
            )
        return response.body.decode("utf-8")

    def test_window_has_active_days_to_lock(self):
        """Guards the fixture: without active days the disclosure is moot."""
        self.assertIn("Locks all 1 active day(s)", self._render())

    def test_pending_days_are_called_out_before_locking(self):
        html = self._render()
        self.assertIn("still Pending", html)
        self.assertIn("are still Pending review", html)

    def test_pending_count_appears_in_the_confirm_message(self):
        import html as html_mod
        import re

        attr = re.search(
            r'data-confirm-lock="([^"]*)"', self._render(), re.S
        ).group(1)
        message = html_mod.unescape(attr)

        self.assertIn("including 1 still marked Pending", message)
        self.assertIn("Review the pending days first", message)

    def test_approved_days_do_not_raise_the_pending_warning(self):
        self._seed_approval(WEEK, "approved")
        html = self._render()

        self.assertNotIn("are still Pending review", html)


if __name__ == "__main__":
    unittest.main()
