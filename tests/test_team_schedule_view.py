"""Employee Schedule + Hours (2026-09 portal redesign, Phase 2).

Pure view-model rules in app/team/schedule_view.py are tested without a
database; the route tests render /team/schedule and /team/hours against an
in-memory SQLite DB with TemplateResponse captured, like the Home tests.
"""
from __future__ import annotations

import os
import re
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-schedview")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-schedview")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-schedview")

LA = ZoneInfo("America/Los_Angeles")
MONDAY = date(2026, 9, 21)
WEEK = [MONDAY + timedelta(days=i) for i in range(7)]
TODAY = MONDAY + timedelta(days=2)  # Wednesday
ME = 10


def _e(label, kind="work"):
    return {"label": label, "kind": kind}


def _cal(kind, entries, label=None):
    return {"kind": kind, "label": label or kind.title(), "entries": entries}


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


class MyWeekTests(unittest.TestCase):
    def _build(self, calendars, **kw):
        from app.team.schedule_view import build_my_week

        return build_my_week(
            week_days=WEEK,
            today=TODAY,
            me_id=ME,
            calendars=calendars,
            names={ME: "Maya Rodriguez", 11: "Jordan Lee", 12: "Priya Shah", 13: "Dana Kim"},
            **kw,
        )

    def test_one_row_per_day_with_off_days_and_today(self):
        week = self._build(
            [_cal("storefront", {(ME, WEEK[0].isoformat()): [_e("12-8")]})]
        )
        self.assertEqual([d["date"] for d in week["days"]], WEEK)
        self.assertEqual(week["days"][0]["state"], "work")
        self.assertEqual(week["days"][0]["shifts"][0]["time"], "12:00 – 8:00 PM")
        self.assertEqual(week["days"][0]["shifts"][0]["location_label"], "Storefront")
        self.assertEqual(week["days"][1]["state"], "off")
        self.assertTrue(week["days"][2]["is_today"])
        self.assertEqual(sum(d["is_today"] for d in week["days"]), 1)
        self.assertEqual(week["shift_count"], 1)
        self.assertEqual(week["scheduled_label"], "8")

    def test_only_current_user_rows_become_shifts(self):
        week = self._build(
            [_cal("storefront", {(11, WEEK[3].isoformat()): [_e("10-6")]})]
        )
        self.assertEqual(week["shift_count"], 0)
        self.assertEqual(week["days"][3]["state"], "off")

    def test_coworkers_are_same_location_and_day_only(self):
        iso = WEEK[3].isoformat()
        week = self._build(
            [
                _cal(
                    "storefront",
                    {
                        (ME, iso): [_e("10-6")],
                        (11, iso): [_e("12-8")],
                        (12, iso): [_e("OFF", "off")],  # not working
                        (13, WEEK[4].isoformat()): [_e("12-8")],  # other day
                    },
                ),
                _cal("packing", {(13, iso): [_e("9-1")]}),  # other location
            ]
        )
        coworkers = week["days"][3]["shifts"][0]["coworkers"]
        self.assertEqual([c["initials"] for c in coworkers], ["JL"])
        self.assertEqual(coworkers[0]["name"], "Jordan Lee")

    def test_approved_timeoff_from_requests_and_grid(self):
        week = self._build(
            [_cal("storefront", {(ME, WEEK[6].isoformat()): [_e("Time off", "request")]})],
            timeoff_days={WEEK[5]},
        )
        self.assertEqual(week["days"][5]["state"], "timeoff")
        self.assertEqual(week["days"][5]["timeoff_label"], "Time off · approved")
        self.assertEqual(week["days"][6]["state"], "timeoff")
        self.assertEqual(week["days"][6]["timeoff_label"], "Time off")
        self.assertEqual(week["shift_count"], 0)

    def test_stream_hints_become_stream_shifts(self):
        hint = {
            "label": "6:00 PM - 12:00 AM (next day)",
            "start_time": "18:00",
            "end_time": "00:00",
            "is_overnight": True,
            "account_name": "Degen TikTok",
        }
        week = self._build(
            [_cal("stream", {(ME, WEEK[4].isoformat()): [hint]})]
        )
        shift = week["days"][4]["shifts"][0]
        self.assertEqual(shift["location"], "stream")
        self.assertEqual(shift["time"], "6:00 PM – 12:00 AM")
        self.assertEqual(shift["hours"], 6.0)
        self.assertIn("Degen TikTok", shift["notes"])

    def test_hero_shifts_combine_calendars_and_carry_overnight(self):
        from app.team.schedule_view import build_my_week, hero_shifts

        days = [TODAY + timedelta(days=i) for i in range(-1, 8)]
        stream = {
            "label": "6:00 PM - 2:00 AM (next day)",
            "start_time": "18:00",
            "end_time": "02:00",
            "is_overnight": True,
            "account_name": "Degen TikTok",
        }
        my_days = build_my_week(
            week_days=days,
            today=TODAY,
            me_id=ME,
            calendars=[
                _cal("storefront", {
                    (ME, TODAY.isoformat()): [_e("11-7")],
                    (ME, (TODAY + timedelta(days=3)).isoformat()): [_e("12-8")],
                }),
                _cal("stream", {
                    (ME, (TODAY - timedelta(days=1)).isoformat()): [stream],
                    (ME, (TODAY + timedelta(days=1)).isoformat()): [stream],
                }),
            ],
        )["days"]
        today_rows, upcoming = hero_shifts(my_days, today=TODAY)
        # Yesterday's 6 PM - 2 AM stream still runs until 2 AM today.
        self.assertEqual([r["calendar_label"] for r in today_rows], ["Stream", "Storefront"])
        self.assertEqual(today_rows[0]["ranges"], [(18 * 60 - 1440, 2 * 60)])
        self.assertEqual(today_rows[1]["ranges"], [(11 * 60, 19 * 60)])
        self.assertEqual(
            [(r["shift_date"], r["calendar_label"]) for r in upcoming],
            [
                (TODAY, "Storefront"),
                (TODAY + timedelta(days=1), "Stream"),
                (TODAY + timedelta(days=3), "Storefront"),
            ],
        )
        self.assertEqual(upcoming[1]["time"], "6:00 PM – 2:00 AM")
        self.assertEqual(upcoming[1]["day_note"], "Degen TikTok")

    def test_day_note_attaches_to_storefront_shift(self):
        iso = WEEK[5].isoformat()
        week = self._build(
            [_cal("storefront", {(ME, iso): [_e("SHOW", "show")]})],
            day_notes={iso: "East Bay Santa Clara"},
        )
        shift = week["days"][5]["shifts"][0]
        self.assertEqual(shift["time"], "Show")
        self.assertEqual(shift["notes"], ["East Bay Santa Clara"])

    def test_eyebrow(self):
        from app.team.schedule_view import my_week_eyebrow

        self.assertEqual(my_week_eyebrow({"shift_count": 0}), "No shifts this week")
        self.assertEqual(
            my_week_eyebrow({"shift_count": 4, "scheduled_hours": 30.0, "scheduled_label": "30"}),
            "30h scheduled · 4 shifts",
        )


class TeamWeekTests(unittest.TestCase):
    def test_grouped_by_day_then_location_me_flagged(self):
        from app.team.schedule_view import build_team_week

        iso = TODAY.isoformat()
        days = build_team_week(
            week_days=WEEK,
            today=TODAY,
            me_id=ME,
            calendars=[
                _cal(
                    "storefront",
                    {(11, iso): [_e("12-8")], (ME, iso): [_e("11-7")], (12, iso): [_e("OFF", "off")]},
                ),
                _cal("packing", {(13, iso): [_e("9-1")]}),
                _cal("stream", {}),
            ],
            names={ME: "Maya", 11: "Jordan", 12: "Priya", 13: "Dana"},
        )
        self.assertEqual(len(days), 7)
        today = days[2]
        self.assertTrue(today["heading"].startswith("Today · Wed"))
        self.assertEqual([g["location"] for g in today["groups"]], ["storefront", "packing"])
        store = today["groups"][0]["people"]
        # Earliest start first; OFF rows are not listed.
        self.assertEqual([p["name"] for p in store], ["Maya", "Jordan"])
        self.assertTrue(store[0]["is_me"])
        self.assertFalse(store[1]["is_me"])
        self.assertEqual(days[0]["groups"], [])

    def test_normalize_view_and_week_label(self):
        from app.team.schedule_view import normalize_view, week_label

        self.assertEqual(normalize_view("team"), "team")
        self.assertEqual(normalize_view("TEAM "), "team")
        self.assertEqual(normalize_view("grid"), "mine")
        self.assertEqual(normalize_view(None), "mine")
        self.assertEqual(week_label(date(2026, 9, 21)), "Sep 21 – 27")
        self.assertEqual(week_label(date(2026, 9, 28)), "Sep 28 – Oct 4")


class HoursViewTests(unittest.TestCase):
    def _week(self, days):
        return {
            "days": [
                {
                    "day": WEEK[i],
                    "work_seconds": days.get(i, {}).get("work", 0),
                    "break_seconds": days.get(i, {}).get("brk", 0),
                    "auto_break_seconds": days.get(i, {}).get("auto", 0),
                    "status": days.get(i, {}).get("status", ""),
                    "status_label": days.get(i, {}).get("status_label", ""),
                    "status_tone": days.get(i, {}).get("status_tone", ""),
                    "status_note": days.get(i, {}).get("note", ""),
                }
                for i in range(7)
            ],
            "total_work_seconds": sum(v.get("work", 0) for v in days.values()),
            "total_break_seconds": sum(v.get("brk", 0) for v in days.values()),
            "total_auto_break_seconds": sum(v.get("auto", 0) for v in days.values()),
        }

    def _entry(self, day, h1, h2, desc="Shift", is_break=False, running=False, end=True):
        return {
            "start_local": datetime(day.year, day.month, day.day, h1, tzinfo=LA),
            "end_local": datetime(day.year, day.month, day.day, h2, tzinfo=LA) if end else None,
            "duration_seconds": (h2 - h1) * 3600,
            "running": running,
            "description": desc,
            "is_break": is_break,
        }

    def _sched(self, hours_by_index):
        return [{"date": d, "scheduled_hours": hours_by_index.get(i, 0.0)} for i, d in enumerate(WEEK)]

    def _build(self, week, sched, entries):
        from app.team.schedule_view import build_hours_view

        return build_hours_view(week=week, my_days=sched, entries=entries, today=TODAY)

    def test_worked_vs_scheduled_totals_and_bars(self):
        view = self._build(
            self._week({0: {"work": 8 * 3600}, 2: {"work": 3 * 3600}}),
            self._sched({0: 8.0, 2: 8.0, 4: 6.0}),
            [self._entry(WEEK[0], 12, 20), self._entry(WEEK[2], 11, 14)],
        )
        self.assertEqual(view["total_label"], "11")
        self.assertEqual(view["scheduled_label"], "22")
        self.assertEqual(len(view["bars"]), 7)
        mon = view["bars"][0]
        self.assertEqual(mon["worked_bucket"], mon["sched_bucket"])
        self.assertGreater(mon["worked_bucket"], 0)
        self.assertEqual(view["bars"][4]["worked_bucket"], 0)
        self.assertTrue(view["bars"][4]["has_sched"])
        self.assertEqual(view["days_worked"], 2)
        self.assertEqual(view["days_scheduled"], 3)
        # Newest first; future scheduled days stay on the chart only.
        self.assertEqual([r["date"] for r in view["rows"]], [WEEK[2], WEEK[0]])
        self.assertTrue(view["rows"][0]["heading"].startswith("Today · "))

    def test_flags_come_from_existing_rules_only(self):
        view = self._build(
            self._week(
                {
                    0: {"work": int(7.5 * 3600), "brk": 1800, "auto": 1800},
                    1: {"work": 0, "status": "rejected", "status_label": "Needs fix",
                        "status_tone": "danger", "note": "Missing clock-out"},
                }
            ),
            self._sched({0: 8.0, 1: 8.0}),
            [self._entry(WEEK[0], 9, 17), self._entry(WEEK[1], 9, 17, end=False)],
        )
        by_day = {r["date"]: r for r in view["rows"]}
        mon_flags = " ".join(f["text"] for f in by_day[WEEK[0]]["flags"])
        self.assertIn("No break logged", mon_flags)
        self.assertIn("30m", mon_flags)
        tue_flags = " ".join(f["text"] for f in by_day[WEEK[1]]["flags"])
        self.assertIn("Needs fix: Missing clock-out", tue_flags)
        self.assertIn("No clock-out recorded", tue_flags)
        self.assertEqual(by_day[WEEK[1]]["status_tone"], "err")

    def test_scheduled_past_day_with_nothing_logged_is_flagged(self):
        view = self._build(self._week({}), self._sched({0: 8.0, 3: 8.0}), [])
        self.assertEqual([r["date"] for r in view["rows"]], [WEEK[0]])
        self.assertIn("no hours logged", view["rows"][0]["flags"][0]["text"])

    def test_break_entries_listed_and_running_marked(self):
        view = self._build(
            self._week({2: {"work": 3 * 3600, "brk": 1800}}),
            self._sched({2: 8.0}),
            [
                self._entry(WEEK[2], 11, 13),
                self._entry(WEEK[2], 13, 14, desc="Lunch break", is_break=True),
                self._entry(WEEK[2], 14, 15, running=True),
            ],
        )
        row = view["rows"][0]
        self.assertTrue(row["running"])
        texts = [line["text"] for line in row["lines"]]
        self.assertTrue(any(t.startswith("Break ") for t in texts))
        self.assertTrue(any(t.endswith("– now · Shift") for t in texts))
        self.assertEqual(row["flags"], [])

    def test_bar_bucket(self):
        from app.team.schedule_view import bar_bucket

        self.assertEqual(bar_bucket(0, 9), 0)
        self.assertEqual(bar_bucket(9, 9), 100)
        self.assertEqual(bar_bucket(4.5, 9), 50)
        self.assertEqual(bar_bucket(0.05, 9), 5)
        self.assertEqual(bar_bucket(20, 9), 100)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


class _RouteHarness:
    def _setup_db(self):
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

    def _user(self, user_id, name, role="employee", clockify=None):
        from app.models import EmployeeProfile, User

        user = User(
            id=user_id,
            username=name.split()[0].lower(),
            password_hash="x",
            password_salt="x",
            display_name=name,
            role=role,
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.add(EmployeeProfile(user_id=user_id, clockify_user_id=clockify))
        self.session.commit()
        self.session.refresh(user)
        return user

    def _shift(self, user_id, day, label, calendar_kind="storefront", kind=None):
        from app.models import ShiftEntry, classify_shift_label

        self.session.add(
            ShiftEntry(
                user_id=user_id,
                shift_date=day,
                calendar_kind=calendar_kind,
                label=label,
                kind=kind or classify_shift_label(label),
                created_by_user_id=user_id,
            )
        )
        self.session.commit()

    def _request(self, user, path):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=user),
            session={},
            headers={},
            cookies={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path=path),
            scope={"path": path},
            query_params={},
        )

    def _capture(self, fn, *args, **kwargs):
        from app.routers import team

        captured = {}

        def fake_template_response(request, template, context):
            captured["template"] = template
            captured["context"] = context
            html = team.templates.env.get_template(template).render(context)
            return SimpleNamespace(status_code=200, body=html)

        with patch.object(team.templates, "TemplateResponse", side_effect=fake_template_response), patch.object(
            team, "issue_token", return_value="tok"
        ):
            response = fn(*args, **kwargs)
        return response, captured


class ScheduleRouteTests(unittest.TestCase, _RouteHarness):
    def setUp(self):
        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        self.jordan = self._user(11, "Jordan Lee")
        self.dana = self._user(13, "Dana Kim")
        self._shift(ME, WEEK[0], "12-8")
        self._shift(11, WEEK[0], "10-6")
        self._shift(11, WEEK[3], "11-7")  # Jordan only
        self._shift(13, WEEK[0], "9-1", calendar_kind="packing")

    def tearDown(self):
        self.session.close()

    def _render(self, *, view=None, week=MONDAY.isoformat(), today=TODAY):
        from app.routers import team

        with patch.object(team, "_portal_today", return_value=today):
            return self._capture(
                team.team_schedule,
                self._request(self.maya, "/team/schedule"),
                week=week,
                view=view,
                session=self.session,
            )

    def test_my_shifts_rows_are_for_the_current_user_only(self):
        response, captured = self._render()
        html = response.body
        ctx = captured["context"]
        self.assertEqual(captured["template"], "team/schedule.html")
        self.assertEqual(ctx["view"], "mine")
        days = ctx["my_week"]["days"]
        self.assertEqual(len(days), 7)
        self.assertEqual([d["state"] for d in days].count("work"), 1)
        self.assertEqual(days[0]["shifts"][0]["time"], "12:00 – 8:00 PM")
        # Jordan's Thursday shift is not in Maya's list; she's off.
        self.assertEqual(days[3]["state"], "off")
        # Coworker on the same location/day shows as initials; Dana is Packing.
        self.assertEqual([c["initials"] for c in days[0]["shifts"][0]["coworkers"]], ["JL"])
        self.assertEqual(html.count('class="pt-row pt-shift'), 7)
        self.assertIn('aria-current="page">My shifts', html)
        self.assertIn("pt-row pt-shift is-today", html)
        self.assertNotIn("Team total", html)
        self.assertNotIn("People working", html)
        self.assertNotIn("sch-table", html)

    def test_team_view_groups_by_day_then_location(self):
        response, captured = self._render(view="team")
        html = response.body
        team_days = captured["context"]["team_days"]
        self.assertEqual(len(team_days), 7)
        monday = team_days[0]
        self.assertEqual([g["location"] for g in monday["groups"]], ["storefront", "packing"])
        store_names = [p["name"] for p in monday["groups"][0]["people"]]
        self.assertEqual(store_names, ["Jordan Lee", "Maya Rodriguez"])
        self.assertIn('aria-current="page">Whole team', html)
        self.assertIn("pt-row pt-person is-me", html)
        self.assertIn("Maya Rodriguez (you)", html)
        self.assertNotIn("Jordan Lee (you)", html)
        self.assertIn('class="pt-tag is-packing"', html)

    def test_week_nav_keeps_week_and_view_params(self):
        response, captured = self._render(view="team", week=(MONDAY + timedelta(days=3)).isoformat())
        html = response.body
        prev_week = (MONDAY - timedelta(days=7)).isoformat()
        next_week = (MONDAY + timedelta(days=7)).isoformat()
        self.assertEqual(captured["context"]["week_start"], MONDAY)
        self.assertIn(f'href="/team/schedule?view=team&amp;week={prev_week}"', html)
        self.assertIn(f'href="/team/schedule?view=team&amp;week={next_week}"', html)
        self.assertIn(f'href="/team/schedule?view=mine&amp;week={MONDAY.isoformat()}"', html)

    def test_approved_time_off_shows_inline(self):
        from app.models import TimeOffRequest

        self.session.add(
            TimeOffRequest(
                submitted_by_user_id=ME,
                start_date=WEEK[5],
                end_date=WEEK[6],
                status="approved",
            )
        )
        self.session.add(
            TimeOffRequest(
                submitted_by_user_id=ME,
                start_date=WEEK[2],
                end_date=WEEK[2],
                status="submitted",
            )
        )
        self.session.commit()
        response, captured = self._render()
        days = captured["context"]["my_week"]["days"]
        self.assertEqual(days[5]["state"], "timeoff")
        self.assertEqual(days[6]["state"], "timeoff")
        self.assertEqual(days[2]["state"], "off")  # pending is not approved
        self.assertEqual(response.body.count("Time off · approved"), 2)

    def test_timeoff_link_prefills_next_shift_date(self):
        response, _ = self._render(today=MONDAY)
        self.assertIn(
            f'href="/team/requests?new=timeoff&amp;date={MONDAY.isoformat()}"', response.body
        )
        self.assertIn("Can't make a shift? Request time off", response.body)

    def test_template_has_no_inline_styles(self):
        source = Path("app/templates/team/schedule.html").read_text(encoding="utf-8")
        self.assertNotIn("<style", source)
        self.assertNotIn("style=", source)
        self.assertNotIn("<script", source)
        self.assertIsNone(re.search(r"#[0-9a-fA-F]{3,8}\b", source))


def _settings():
    return SimpleNamespace(
        employee_portal_enabled=True,
        clockify_api_key="key",
        clockify_workspace_id="workspace",
        clockify_timezone="America/Los_Angeles",
        clockify_base_url="https://api.clockify.me/api/v1",
        clockify_timeout_seconds=5.0,
    )


class HoursRouteTests(unittest.TestCase, _RouteHarness):
    def setUp(self):
        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez", clockify="ck-10")
        self._shift(ME, WEEK[0], "12-8")
        self._shift(ME, WEEK[4], "10-2", calendar_kind="packing")

    def tearDown(self):
        self.session.close()

    def _cache(self, start, end, description="Shift"):
        from app.models import ClockifyTimeEntry

        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id=f"e-{start.isoformat()}",
                clockify_user_id="ck-10",
                user_id=ME,
                description=description,
                start_at=start.astimezone(timezone.utc),
                end_at=end.astimezone(timezone.utc),
                duration_seconds=int((end - start).total_seconds()),
                is_deleted=False,
            )
        )
        self.session.commit()

    def _render(self, settings=None):
        from app.routers import team

        with patch.object(team, "get_settings", return_value=settings or _settings()), patch.object(
            team, "_portal_today", return_value=TODAY
        ):
            return self._capture(
                team.team_hours,
                self._request(self.maya, "/team/hours"),
                week=MONDAY.isoformat(),
                session=self.session,
            )

    def test_worked_vs_scheduled_totals(self):
        self._cache(datetime(2026, 9, 21, 12, 0, tzinfo=LA), datetime(2026, 9, 21, 16, 0, tzinfo=LA))
        response, captured = self._render()
        hours = captured["context"]["hours"]
        self.assertEqual(hours["total_label"], "4")
        self.assertEqual(hours["scheduled_label"], "12")  # 8h storefront + 4h packing
        html = response.body
        self.assertIn('<span class="pt-stat-v">4h</span>', html)
        self.assertIn("of 12h scheduled", html)
        self.assertEqual(html.count('<li class="pt-bar'), 7)
        self.assertIn("You clock in and out in <b>Clockify</b> on the shop iPad", html)
        self.assertNotIn("Clockify status", html)

    def test_no_estimated_pay_even_with_a_rate(self):
        from app.models import EmployeeProfile

        profile = self.session.get(EmployeeProfile, ME)
        for attr, value in (("hourly_rate_cents", 2000), ("pay_type", "hourly")):
            if hasattr(profile, attr):
                setattr(profile, attr, value)
        self.session.add(profile)
        self.session.commit()
        self._cache(datetime(2026, 9, 21, 12, 0, tzinfo=LA), datetime(2026, 9, 21, 16, 0, tzinfo=LA))
        response, captured = self._render()
        self.assertNotIn("pay", captured["context"])
        self.assertNotIn("estimated pay", response.body.lower())
        self.assertNotIn("$", response.body.split('<main class="pt-main">', 1)[1])

    def test_clockify_not_connected_state(self):
        settings = _settings()
        settings.clockify_api_key = ""
        response, captured = self._render(settings=settings)
        html = response.body
        self.assertIn("Hours are being set up.", html)
        self.assertEqual(captured["context"]["hours"], {})
        self.assertNotIn("pt-bars", html)

    def test_unlinked_employee_state(self):
        from app.models import EmployeeProfile

        profile = self.session.get(EmployeeProfile, ME)
        profile.clockify_user_id = None
        self.session.add(profile)
        self.session.commit()
        response, _ = self._render()
        self.assertIn("Your hours are not linked yet.", response.body)
        self.assertNotIn("estimated pay", response.body.lower())

    def test_empty_week_state(self):
        from app.routers import team

        with patch.object(team, "clockify_client_from_settings") as client:
            client.return_value.get_user_time_entries.return_value = []
            response, _ = self._render()
        self.assertIn("pt-bars", response.body)
        # Monday's 12-8 shift passed with nothing logged: a real flag.
        self.assertIn("Scheduled, but no hours logged", response.body)

    def test_clockify_fetch_error_shows_error_state_not_empty_week(self):
        from app.routers import team
        from app.team.clockify import ClockifyApiError

        with patch.object(team, "clockify_client_from_settings") as client:
            client.return_value.get_user_time_entries.side_effect = ClockifyApiError(
                "Clockify request failed with HTTP 503."
            )
            response, captured = self._render()
        self.assertTrue(captured["context"]["clockify_error"])
        self.assertIn("Your hours could not be loaded.", response.body)
        self.assertNotIn("No time entries", response.body)
        self.assertNotIn("pt-bars", response.body)

    def test_empty_past_week_is_not_an_error_and_makes_no_live_call(self):
        from app.routers import team

        past = (MONDAY - timedelta(days=21)).isoformat()
        with patch.object(team, "clockify_client_from_settings") as client, patch.object(
            team, "get_settings", return_value=_settings()
        ), patch.object(team, "_portal_today", return_value=TODAY):
            response, captured = self._capture(
                team.team_hours,
                self._request(self.maya, "/team/hours"),
                week=past,
                session=self.session,
            )
        client.assert_not_called()
        self.assertEqual(captured["context"]["clockify_error"], "")
        self.assertIn("No hours logged that week.", response.body)
        self.assertNotIn("this week yet", response.body)
        self.assertNotIn("could not be loaded", response.body)

    def test_template_has_no_inline_styles_or_small_type(self):
        source = Path("app/templates/team/hours.html").read_text(encoding="utf-8")
        self.assertNotIn("<style", source)
        self.assertNotIn("style=", source)
        self.assertNotIn("<script", source)
        self.assertIsNone(re.search(r"#[0-9a-fA-F]{3,8}\b", source))


class Phase2CssTests(unittest.TestCase):
    def test_phase2_css_uses_tokens_and_min_12px_type(self):
        css = Path("app/static/portal.css").read_text(encoding="utf-8")
        block = css.split("Schedule + Hours (employee portal redesign, Phase 2)", 1)[1]
        self.assertIsNone(re.search(r"#[0-9a-fA-F]{3,8}\b", block))
        for size in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", block):
            self.assertGreaterEqual(float(size), 12)
        self.assertIn(".pt-seg a", block)
        self.assertIn("min-height: var(--pt-tap)", block)
        self.assertIn(".pt-weeknav > .pt-btn { width: var(--pt-tap)", block)


if __name__ == "__main__":
    unittest.main()
