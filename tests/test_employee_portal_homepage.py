"""Homepage data and copy regression tests for the employee portal.

These render the dashboard template directly instead of using TestClient,
matching the Wave A portal tests and avoiding sandbox hangs on app routes.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta
from html import unescape
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-homepage")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-homepage")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-homepage")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeePortalHomepageTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        from app.db import seed_employee_portal_defaults

        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _seed_user(
        self,
        user_id: int,
        *,
        username: str | None = None,
        display_name: str | None = None,
        role: str = "employee",
    ):
        from app.models import User

        user = User(
            id=user_id,
            username=username or f"user{user_id}",
            password_hash="x",
            password_salt="x",
            display_name=display_name or username or f"User {user_id}",
            role=role,
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _login_as(self, role: str, user_id: int = 100, username: str = "u"):
        from app.models import User

        existing = self.session.get(User, user_id)
        if existing is not None:
            self._current_user = existing
            return existing
        self._current_user = self._seed_user(
            user_id,
            username=username,
            display_name=username,
            role=role,
        )
        return self._current_user

    def _seed_shift(
        self,
        user_id: int,
        shift_date: date,
        label: str,
        *,
        sort_order: int = 0,
        kind: str | None = None,
    ):
        from app.models import ShiftEntry, classify_shift_label

        row = ShiftEntry(
            user_id=user_id,
            shift_date=shift_date,
            label=label,
            kind=kind or classify_shift_label(label),
            sort_order=sort_order,
            created_by_user_id=user_id,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _seed_day_note(self, shift_date: date, location_label: str):
        from app.models import ScheduleDayNote

        row = ScheduleDayNote(day_date=shift_date, location_label=location_label)
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    # A fixed Wednesday keeps week-strip assertions deterministic.
    TODAY = date(2026, 9, 23)
    NOW = datetime(2026, 9, 23, 12, 0)

    def _dashboard_html(self, *, now: datetime | None = None) -> str:
        from app import permissions as perms
        from app.routers.team import (
            _employee_home_context,
            _nav_context,
            _today_staffing_for,
        )
        from app.shared import templates

        today = self.TODAY
        user = self._current_user
        request = SimpleNamespace(url=SimpleNamespace(path="/team/"))
        nav_ctx = _nav_context(self.session, user)
        context = {
            "request": request,
            "title": "Home",
            "active": "dashboard",
            "current_user": user,
            "widgets": perms.allowed_widgets_for(self.session, user),
            "clockify_ready": False,
            "supply_queue_count": 0,
            "today_staffing": _today_staffing_for(self.session, today=today),
            "csrf_token": "test-token",
            **_employee_home_context(
                self.session,
                user,
                today=today,
                now_local=now or self.NOW,
                clockify_ready=False,
                nav_ctx=nav_ctx,
            ),
            **nav_ctx,
        }
        return templates.env.get_template("team/dashboard.html").render(context)

    @staticmethod
    def _section(html: str, marker: str) -> str:
        start = html.index(marker)
        return html[start:html.index("</section>", start)]

    def _week_strip(self, html: str) -> str:
        start = html.index('<ol class="pt-week">')
        return html[start:html.index("</ol>", start)]

    def test_hero_shows_next_shift_and_week_strip_marks_own_days(self):
        # Replaces the old "Upcoming: next five shifts" list, which the
        # redesign folds into the hero (next shift) and the week strip.
        user = self._login_as("employee", user_id=101, username="emp")
        today = self.TODAY
        labels = [
            "9:00 AM - 1:00 PM",
            "10:00 AM - 2:00 PM",
            "11:00 AM - 3:00 PM",
            "12:00 PM - 4:00 PM",
            "1:00 PM - 5:00 PM",
        ]
        for offset, label in enumerate(labels, start=1):
            self._seed_shift(user.id, today + timedelta(days=offset), label)
        self._seed_day_note(today + timedelta(days=1), "Back room")

        html = self._dashboard_html()

        self.assertIn('class="pt-hero off"', html)
        self.assertIn("Tomorrow", html)
        self.assertIn("9:00 AM – 1:00 PM", html)
        self.assertIn("Storefront", html)
        self.assertIn("Back room", html)
        strip = self._week_strip(html)
        # Wed Sep 23 -> Thu..Sun are this week; next Monday is not.
        self.assertEqual(strip.count('class="pt-day-dot is-shift"'), 4)
        self.assertIn("Thu Sep 24: 9:00 AM – 1:00 PM", strip)
        self.assertNotIn("Mon Sep 28", strip)
        self.assertLess(strip.index("Thu Sep 24"), strip.index("Fri Sep 25"))

    def _seed_stream(self, user_id: int, day: date, start: str, end: str, *, overnight: bool):
        from app.models import StreamAccount, StreamSchedule, Streamer

        account = StreamAccount(name="Degen TikTok")
        streamer = Streamer(name=f"streamer-{user_id}", user_id=user_id)
        self.session.add(account)
        self.session.add(streamer)
        self.session.commit()
        self.session.add(
            StreamSchedule(
                streamer_id=streamer.id,
                stream_account_id=account.id,
                date=day.isoformat(),
                start_time=start,
                end_time=end,
                is_overnight=overnight,
            )
        )
        self.session.commit()

    def test_hero_counts_an_in_progress_overnight_stream(self):
        # Regression: Home only read ShiftEntry, so at 10:40 PM during a
        # 6 PM - 12 AM stream it said "Done for today".
        user = self._login_as("employee", user_id=150, username="streamer")
        self._seed_shift(user.id, self.TODAY, "11-7")
        self._seed_stream(user.id, self.TODAY, "18:00", "00:00", overnight=True)

        html = unescape(self._dashboard_html(now=datetime(2026, 9, 23, 22, 40)))

        self.assertIn('class="pt-hero later"', html)
        self.assertIn("On shift now · until 12:00 AM", html)
        self.assertNotIn("Done for today", html)
        strip = self._week_strip(html)
        self.assertIn("Wed Sep 23: 11:00 AM – 7:00 PM, 6:00 PM – 12:00 AM", strip)
        self.assertNotIn("(next day)", html)

    def test_next_shift_can_be_a_stream(self):
        user = self._login_as("employee", user_id=151, username="streamer2")
        self._seed_shift(user.id, self.TODAY + timedelta(days=3), "12-8")
        self._seed_stream(user.id, self.TODAY + timedelta(days=1), "18:00", "00:00", overnight=True)

        hero = self._section(unescape(self._dashboard_html()), 'class="pt-hero')

        self.assertIn("Tomorrow", hero)
        self.assertIn("6:00 PM – 12:00 AM", hero)
        self.assertIn("Stream", hero)

    def test_home_excludes_other_users_shifts(self):
        user_a = self._login_as("employee", user_id=201, username="alice")
        user_b = self._seed_user(202, username="bob", display_name="Bob")
        today = self.TODAY
        for offset in range(1, 4):
            self._seed_shift(user_a.id, today + timedelta(days=offset), f"A shift {offset}")
            self._seed_shift(user_b.id, today + timedelta(days=offset), f"B shift {offset}")

        html = self._dashboard_html()

        for offset in range(1, 4):
            self.assertIn(f"A shift {offset}", html)
            self.assertNotIn(f"B shift {offset}", html)

    def test_home_ignores_past_weeks_and_uses_today_for_hero(self):
        user = self._login_as("employee", user_id=301, username="past")
        today = self.TODAY
        self._seed_shift(user.id, today - timedelta(days=8), "Past shift one")
        self._seed_shift(user.id, today - timedelta(days=9), "Past shift two")
        self._seed_shift(user.id, today, "Today shift")
        self._seed_shift(user.id, today + timedelta(days=1), "Future shift")

        html = self._dashboard_html()

        self.assertNotIn("Past shift one", html)
        self.assertNotIn("Past shift two", html)
        # Unparseable label today -> "later" hero showing the label itself.
        self.assertIn('class="pt-hero later"', html)
        self.assertIn("Today shift", html)
        self.assertIn("Future shift", self._week_strip(html))

    def test_home_excludes_request_kind(self):
        from app.models import SHIFT_KIND_REQUEST, SHIFT_KIND_WORK

        user = self._login_as("employee", user_id=302, username="upcoming")
        today = self.TODAY
        self._seed_shift(user.id, today + timedelta(days=1), "10-6", kind=SHIFT_KIND_WORK)
        self._seed_shift(
            user.id, today + timedelta(days=2), "Approved leave", kind=SHIFT_KIND_REQUEST
        )

        html = self._dashboard_html()

        self.assertEqual(self._week_strip(html).count('class="pt-day-dot is-shift"'), 1)
        self.assertIn("10:00 AM – 6:00 PM", html)
        self.assertNotIn("Approved leave", html)

    def test_upcoming_shifts_empty_state(self):
        self._login_as("employee", user_id=401, username="empty")

        html = self._dashboard_html()

        self.assertIn("No upcoming shifts", html)
        self.assertIn("You&#39;re off today", html)
        self.assertNotIn("coming soon", html.lower())
        self.assertNotIn("lands soon", html.lower())
        self.assertNotIn("TBA", html)

    def test_week_strip_marks_approved_time_off(self):
        from app.models import TimeOffRequest

        user = self._login_as("employee", user_id=402, username="pto")
        self.session.add_all(
            [
                TimeOffRequest(
                    submitted_by_user_id=user.id,
                    start_date=date(2026, 9, 26),
                    end_date=date(2026, 9, 27),
                    status="approved",
                ),
                TimeOffRequest(
                    submitted_by_user_id=user.id,
                    start_date=date(2026, 9, 24),
                    end_date=date(2026, 9, 24),
                    status="submitted",
                ),
            ]
        )
        self.session.commit()

        strip = self._week_strip(self._dashboard_html())

        self.assertEqual(strip.count('class="pt-day-dot is-timeoff"'), 2)
        self.assertIn("Sat Sep 26: approved time off", strip)

    def test_needs_you_excludes_announcements(self):
        from app.models import TeamAnnouncement

        user = self._login_as("employee", user_id=403, username="reader")
        self.session.add(
            TeamAnnouncement(
                title="Surprise Set restock Friday",
                body="Heads up.",
                created_by_user_id=user.id,
            )
        )
        self.session.commit()

        html = self._dashboard_html()
        needs = self._section(html, 'id="pt-needs-you-h"')

        self.assertNotIn("Surprise Set restock Friday", needs)
        self.assertNotIn("announcement", needs.lower())
        # Seeded required policies are unsigned -> real to-dos show up.
        self.assertIn("Acknowledge", needs)
        self.assertIn("Add your phone number", needs)
        # The announcement is still on Home, as the "Latest" row.
        latest = self._section(html, 'id="pt-latest-h"')
        self.assertIn("Surprise Set restock Friday", latest)

    def test_my_requests_lists_pending_first_with_status_pills(self):
        from app.models import SupplyRequest, TimeOffRequest

        user = self._login_as("employee", user_id=404, username="asker")
        self.session.add_all(
            [
                TimeOffRequest(
                    submitted_by_user_id=user.id,
                    start_date=date(2026, 9, 27),
                    end_date=date(2026, 9, 27),
                    status="approved",
                    created_at=datetime(2026, 9, 22, 18, 0),
                ),
                SupplyRequest(
                    submitted_by_user_id=user.id,
                    title="Penny sleeves x10 packs",
                    status="submitted",
                    created_at=datetime(2026, 9, 20, 18, 0),
                ),
            ]
        )
        self.session.commit()

        section = self._section(self._dashboard_html(), 'id="pt-requests-h"')

        self.assertIn("Penny sleeves x10 packs", section)
        self.assertIn("Time off · Sun Sep 27", section)
        self.assertIn('<span class="pt-pill warn">Pending</span>', section)
        self.assertIn('<span class="pt-pill ok">Approved</span>', section)
        self.assertLess(section.index("Penny sleeves"), section.index("Time off ·"))

    def test_home_has_no_estimated_pay_or_duplicate_clock_blocks(self):
        self._login_as("employee", user_id=405, username="nopay")

        html = unescape(self._dashboard_html())

        self.assertNotIn("Estimated pay", html)
        self.assertNotIn("Clocked in today", html)
        self.assertNotIn("What do I need to do today?", html)
        self.assertEqual(html.count('class="pt-hero '), 1)

    def test_today_staffing_renders_for_admin(self):
        admin = self._login_as("admin", user_id=501, username="admin")
        worker_a = self._seed_user(502, username="amy", display_name="Amy")
        worker_b = self._seed_user(503, username="ben", display_name="Ben")
        today = self.TODAY
        self._seed_shift(worker_b.id, today, "12:00 PM - 4:00 PM")
        self._seed_shift(worker_a.id, today, "9:00 AM - 1:00 PM")

        html = self._dashboard_html()

        self.assertEqual(admin.role, "admin")
        self.assertIn("Who's on today", html)
        self.assertIn("Amy", html)
        self.assertIn("Ben", html)
        self.assertLess(html.index("Amy"), html.index("Ben"))

    def test_today_staffing_excludes_approved_timeoff(self):
        from app.models import SHIFT_KIND_REQUEST, SHIFT_KIND_WORK

        self._login_as("manager", user_id=504, username="manager")
        worker_a = self._seed_user(505, username="amy2", display_name="Amy Worker")
        worker_b = self._seed_user(506, username="ben2", display_name="Ben Off")
        today = self.TODAY
        self._seed_shift(worker_a.id, today, "10-6", kind=SHIFT_KIND_WORK)
        self._seed_shift(worker_b.id, today, "Approved leave", kind=SHIFT_KIND_REQUEST)

        html = self._dashboard_html()

        self.assertIn("Who's on today", html)
        self.assertEqual(html.count('pt-today-staff-row"'), 1)
        self.assertIn("Amy Worker", html)
        self.assertIn("10-6", html)
        self.assertNotIn("Ben Off", html)
        self.assertNotIn("Approved leave", html)

    def test_today_staffing_hidden_for_employee(self):
        employee = self._login_as("employee", user_id=601, username="employee")
        coworker = self._seed_user(602, username="coworker", display_name="Coworker")
        today = self.TODAY
        self._seed_shift(employee.id, today, "9:00 AM - 1:00 PM")
        self._seed_shift(coworker.id, today, "10:00 AM - 2:00 PM")

        html = self._dashboard_html()

        self.assertNotIn("Who's on today", html)

    def test_today_staffing_empty_message(self):
        self._login_as("manager", user_id=701, username="manager")

        html = self._dashboard_html()

        self.assertIn("Who's on today", html)
        self.assertIn("Nobody scheduled today", html)

    def test_placeholder_cards_are_hidden_until_wired(self):
        self._login_as("employee", user_id=801, username="cards")

        html = self._dashboard_html()
        upper = unescape(html).upper()

        self.assertNotIn("TODAY'S TASKS", upper)
        self.assertNotIn("Coming with payroll integration", html)
        self.assertNotIn("Task assignments pending", html)

    def test_placeholder_copy_hygiene(self):
        self._login_as("employee", user_id=901, username="copy")

        html = self._dashboard_html()

        for forbidden in (
            "Not connected",
            "isn't hooked up",
            "lands soon",
            "All clear",
            "No tasks assigned right now",
        ):
            self.assertNotIn(forbidden, html)


if __name__ == "__main__":
    unittest.main()
