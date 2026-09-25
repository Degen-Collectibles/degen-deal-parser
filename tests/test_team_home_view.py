"""Unit tests for the employee Home view-model (app/team/home.py) and the
/team/more page added in the 2026-09 portal redesign (Phase 1).

The hero-state, "Needs you" and "My requests" rules live in pure functions,
so they're tested here without a database or template.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-homeview")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-homeview")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-homeview")

TODAY = date(2026, 9, 23)  # a Wednesday


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(TODAY.year, TODAY.month, TODAY.day, hour, minute)


def _shift(label: str, *, day: date = TODAY, calendar_label: str = "Storefront"):
    return {
        "shift_date": day,
        "label": label,
        "kind": "work",
        "calendar_kind": "storefront",
        "calendar_label": calendar_label,
        "day_note": None,
    }


class HeroStateTests(unittest.TestCase):
    def _hero(self, now, today_shifts=(), upcoming=(), clock=None):
        from app.team.home import build_hero

        return build_hero(
            now_local=now,
            today=TODAY,
            today_shifts=list(today_shifts),
            upcoming_shifts=list(upcoming),
            clock=clock,
        )

    def test_on_the_clock_when_clockify_entry_is_running(self):
        hero = self._hero(
            _at(14, 14),
            today_shifts=[_shift("11-7")],
            clock={
                "linked": True,
                "running": True,
                "on_break": False,
                "since": _at(11, 2),
                "today_seconds": 3 * 3600 + 12 * 60,
            },
        )
        self.assertEqual(hero["state"], "on")
        self.assertTrue(hero["live"])
        self.assertEqual(hero["status"], "On the clock · Storefront")
        self.assertEqual(hero["big"], "3h 12m")
        self.assertEqual(
            hero["meta"],
            [
                {"label": "Since", "value": "11:02 AM"},
                {"label": "shift ends", "value": "7:00 PM"},
            ],
        )
        self.assertEqual(hero["progress"], 40)

    def test_running_break_shows_on_break(self):
        hero = self._hero(
            _at(13),
            clock={"linked": True, "running": True, "on_break": True, "since": _at(9)},
        )
        self.assertEqual(hero["state"], "on")
        self.assertTrue(hero["status"].startswith("On break"))
        self.assertIsNone(hero["progress"])

    def test_shift_later_today(self):
        hero = self._hero(
            _at(11, 20),
            today_shifts=[_shift("2-8")],
            upcoming=[_shift("2-8"), _shift("12-8", day=TODAY + timedelta(days=1))],
            clock={"linked": True, "running": False},
        )
        self.assertEqual(hero["state"], "later")
        self.assertEqual(hero["status"], "Next shift · starts in 2h 40m")
        self.assertEqual(hero["big"], "2 – 8 PM")
        self.assertEqual(hero["meta"], [{"label": "", "value": "Storefront"}])
        self.assertIn("Can't make it?", [a["label"] for a in hero["actions"]])

    def test_shift_in_progress_without_clock_in(self):
        hero = self._hero(
            _at(15),
            today_shifts=[_shift("2-8")],
            clock={"linked": True, "running": False},
        )
        self.assertEqual(hero["state"], "later")
        self.assertEqual(hero["status"], "Shift started 2:00 PM · not clocked in yet")

    def test_unparseable_label_today_is_later(self):
        hero = self._hero(_at(9), today_shifts=[_shift("ALL")])
        self.assertEqual(hero["state"], "later")
        self.assertEqual(hero["status"], "Scheduled today")
        self.assertEqual(hero["big"], "ALL")

    def test_off_today_shows_next_shift(self):
        tomorrow = TODAY + timedelta(days=1)
        hero = self._hero(
            _at(10),
            upcoming=[_shift("12:00 PM - 8:00 PM", day=tomorrow)],
        )
        self.assertEqual(hero["state"], "off")
        self.assertEqual(hero["status"], "You're off today")
        self.assertEqual(hero["big"], "Tomorrow")
        self.assertEqual(hero["meta"][0], {"label": "Next shift", "value": "12:00 – 8:00 PM"})

    def test_finished_shift_today_is_off_with_done_status(self):
        hero = self._hero(
            _at(12),
            today_shifts=[_shift("8-11")],
            upcoming=[_shift("8-11"), _shift("10-2", day=TODAY + timedelta(days=12))],
            clock={"linked": True, "running": False},
        )
        self.assertEqual(hero["state"], "off")
        self.assertEqual(hero["status"], "Done for today")
        self.assertEqual(hero["big"], "Mon, Oct 5")

    def test_off_with_nothing_posted(self):
        hero = self._hero(_at(10))
        self.assertEqual(hero["state"], "off")
        self.assertEqual(hero["big"], "No shifts posted")
        self.assertIn("No upcoming shifts", hero["meta"][0]["label"])


class NeedsYouTests(unittest.TestCase):
    def _completion(self, *, phone=True, emergency=True, clockify=True, missing=()):
        items = [
            {"key": "phone", "done": phone},
            {"key": "emergency", "done": emergency},
            {"key": "policies", "done": not missing},
            {"key": "clockify", "done": clockify},
        ]
        return {
            "items": items,
            "complete_count": sum(1 for item in items if item["done"]),
            "total_count": len(items),
            "missing_policies": list(missing),
        }

    def test_empty_when_everything_is_done(self):
        from app.team.home import build_needs_you

        self.assertEqual(
            build_needs_you(profile_completion=self._completion(), clockify_configured=True),
            [],
        )

    def test_lists_only_real_todos(self):
        from app.team.home import build_needs_you

        items = build_needs_you(
            profile_completion=self._completion(
                emergency=False,
                clockify=False,
                missing=[{"id": "p1", "title": "Break policy", "version": "v2"}],
            ),
            needs_fix_days=[{"day": date(2026, 9, 22), "status_note": "Missing clock-out"}],
            clockify_configured=True,
        )
        self.assertEqual(
            [item["key"] for item in items],
            ["timecard", "policy", "emergency", "clockify"],
        )
        self.assertEqual(items[0]["label"], "Fix your timecard for Tue Sep 22")
        self.assertEqual(items[0]["href"], "/team/hours?week=2026-09-21")
        self.assertEqual(items[1]["label"], "Acknowledge Break policy")
        for item in items:
            self.assertNotIn("announcement", item["label"].lower())

    def test_clockify_row_hidden_when_clockify_not_configured(self):
        from app.team.home import build_needs_you

        items = build_needs_you(
            profile_completion=self._completion(clockify=False),
            clockify_configured=False,
        )
        self.assertEqual(items, [])

    def test_many_unsigned_policies_collapse(self):
        from app.team.home import MAX_POLICY_ROWS, build_needs_you

        missing = [{"id": f"p{i}", "title": f"Policy {i}"} for i in range(5)]
        items = build_needs_you(profile_completion=self._completion(missing=missing))
        self.assertEqual(len(items), MAX_POLICY_ROWS + 1)
        self.assertEqual(items[-1]["label"], "2 more policies to acknowledge")


class RequestRowsTests(unittest.TestCase):
    def test_pending_first_then_newest_and_limited(self):
        from app.team.home import build_request_rows

        timeoff = [
            SimpleNamespace(
                start_date=date(2026, 10, 10),
                end_date=date(2026, 10, 12),
                status="denied",
                status_changed_at=datetime(2026, 9, 21, 9),
                created_at=datetime(2026, 9, 20, 9),
            ),
            SimpleNamespace(
                start_date=date(2026, 9, 27),
                end_date=date(2026, 9, 27),
                status="approved",
                status_changed_at=datetime(2026, 9, 22, 9),
                created_at=datetime(2026, 9, 22, 8),
            ),
        ]
        supply = [
            SimpleNamespace(
                title="Toploaders x200",
                status="submitted",
                status_changed_at=None,
                created_at=datetime(2026, 9, 1, 9),
            ),
        ]
        rows = build_request_rows(timeoff=timeoff, supply=supply, limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Toploaders x200")
        self.assertEqual((rows[0]["pill_tone"], rows[0]["pill"]), ("warn", "Pending"))
        self.assertEqual(rows[1]["title"], "Time off · Sun Sep 27")
        self.assertEqual(rows[1]["sub"], "Decided Sep 22")

    def test_date_span_labels(self):
        from app.team.home import date_span

        self.assertEqual(date_span(date(2026, 10, 10), date(2026, 10, 12)), "Oct 10 – 12")
        self.assertEqual(date_span(date(2026, 9, 30), date(2026, 10, 2)), "Sep 30 – Oct 2")


class MorePageTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        from app.db import seed_employee_portal_defaults
        from app.models import SQLModel

        rate_limit.reset()
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _user(self, user_id: int, role: str):
        from app.models import User

        user = User(
            id=user_id,
            username=f"{role}{user_id}",
            password_hash="x",
            password_salt="x",
            display_name=f"Pat {role.title()}",
            role=role,
            is_active=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _render(self, user):
        from app.routers import team

        captured = {}

        def fake_template_response(request, template, context):
            captured["template"] = template
            captured["context"] = context
            html = team.templates.env.get_template(template).render(context)
            return SimpleNamespace(status_code=200, body=html)

        request = SimpleNamespace(
            state=SimpleNamespace(current_user=user),
            session={},
            headers={},
            cookies={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path="/team/more"),
        )
        with patch.object(team.templates, "TemplateResponse", side_effect=fake_template_response), patch.object(
            team, "issue_token", return_value="tok"
        ):
            response = team.team_more(request, session=self.session)
        return response, captured

    def test_employee_more_page(self):
        user = self._user(301, "employee")
        response, captured = self._render(user)
        html = response.body

        self.assertEqual(captured["template"], "team/more.html")
        self.assertIn('href="/team/profile"', html)
        self.assertIn('href="/team/policies"', html)
        self.assertIn('href="/team/help"', html)
        self.assertIn('action="/team/logout"', html)
        self.assertIn('class="pt-row is-danger"', html)
        self.assertGreater(captured["context"]["unsigned_policy_count"], 0)
        self.assertNotIn("Back to Ops", html)
        self.assertNotIn(">Team admin<", html)
        self.assertNotIn("Photo to listing", html)

    def test_manager_more_page_lists_ops_tools_and_admin(self):
        user = self._user(302, "manager")
        response, _ = self._render(user)
        html = response.body

        self.assertIn(">Ops tools<", html)
        self.assertIn('href="/degen_eye?team_shell=1"', html)
        self.assertIn('href="/tiktok/streamer?team_shell=1"', html)
        self.assertIn("Photo to listing", html)
        self.assertIn(">Team admin<", html)
        self.assertIn('href="/team/admin/schedule"', html)
        self.assertIn("is-active", html.split('<nav class="pt-mobile-bottom-nav"')[1].split("</nav>")[0])

    def test_admin_more_page_has_back_to_ops(self):
        user = self._user(303, "admin")
        response, _ = self._render(user)
        self.assertIn('href="/dashboard"', response.body)


if __name__ == "__main__":
    unittest.main()
