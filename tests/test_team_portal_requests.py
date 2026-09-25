"""Employee Requests page (2026-09 portal redesign, Phase 3).

Pure card / overlap rules in app/team/requests_view.py are tested without a
database. Route tests call the handlers directly against an in-memory SQLite
DB (same harness as the Schedule/Hours tests) and render the real template.
"""
from __future__ import annotations

import asyncio
import json
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlmodel import select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-requests")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-requests")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-requests")

from tests.test_team_schedule_view import _RouteHarness, _settings  # noqa: E402

LA = ZoneInfo("America/Los_Angeles")
TODAY = date(2026, 9, 23)  # Wednesday
MONDAY = date(2026, 9, 21)
ME = 10
OTHER = 11
MANAGER = 20


def _query(location: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(location).query)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


class RequestCardTests(unittest.TestCase):
    def _timeoff(self, **kw):
        base = dict(
            id=1,
            start_date=date(2026, 10, 10),
            end_date=date(2026, 10, 12),
            status="submitted",
            reason="Family trip",
            decision_notes="",
            created_at=datetime(2026, 9, 22, 9),
            status_changed_at=None,
            updated_at=datetime(2026, 9, 22, 9),
        )
        base.update(kw)
        return SimpleNamespace(**base)

    def test_pending_timeoff_card_mentions_days_sent_and_overlaps(self):
        from app.team.requests_view import build_timeoff_card

        card = build_timeoff_card(self._timeoff(), today=TODAY, overlap_count=2)
        self.assertEqual(card["title"], "Oct 10 – 12")
        self.assertEqual(card["sub"], "Time off · 3 days · sent yesterday · overlaps 2 shifts")
        self.assertEqual((card["pill_tone"], card["pill"]), ("warn", "Pending"))
        self.assertTrue(card["is_open"])
        self.assertTrue(card["can_edit"])

    def test_decided_timeoff_card_shows_decider_and_note(self):
        from app.team.requests_view import build_timeoff_card

        card = build_timeoff_card(
            self._timeoff(
                status="denied",
                decision_notes="We're short that weekend.",
                status_changed_at=datetime(2026, 9, 20, 10),
            ),
            today=TODAY,
            decided_by="Jef",
        )
        self.assertEqual(card["sub"], "Time off · 3 days · decided by Jef, Sep 20")
        self.assertEqual((card["pill_tone"], card["pill"]), ("err", "Declined"))
        self.assertEqual(card["note"], "We're short that weekend.")
        self.assertFalse(card["is_open"])
        self.assertFalse(card["can_edit"])

    def test_cancelled_card_has_cancelled_pill(self):
        from app.team.requests_view import build_supply_card

        card = build_supply_card(
            SimpleNamespace(
                id=3,
                title="Penny sleeves",
                description="",
                urgency="high",
                status="cancelled",
                notes="",
                created_at=datetime(2026, 9, 20, 9),
                status_changed_at=datetime(2026, 9, 21, 9),
                updated_at=datetime(2026, 9, 21, 9),
            ),
            today=TODAY,
        )
        self.assertEqual((card["pill_tone"], card["pill"]), ("neutral", "Cancelled"))
        self.assertEqual(card["sub"], "Supplies · ASAP · you cancelled Sep 21")
        self.assertFalse(card["can_edit"])

    def test_split_open_past(self):
        from app.team.requests_view import split_open_past

        cards = [
            {"is_open": True, "sort_at": datetime(2026, 9, 1)},
            {"is_open": False, "decided_at": datetime(2026, 9, 5), "sort_at": datetime(2026, 9, 1)},
            {"is_open": True, "sort_at": datetime(2026, 9, 3)},
            {"is_open": False, "decided_at": datetime(2026, 9, 9), "sort_at": datetime(2026, 9, 2)},
        ]
        lists = split_open_past(cards)
        self.assertEqual([c["sort_at"].day for c in lists["open"]], [3, 1])
        self.assertEqual([c["decided_at"].day for c in lists["past"]], [9, 5])

    def test_overlaps_from_my_week_days(self):
        from app.team.requests_view import overlap_summary, timeoff_overlaps

        days = [
            {
                "date": date(2026, 10, 10),
                "iso": "2026-10-10",
                "state": "work",
                "shifts": [{"time_compact": "12 – 8 PM", "location_label": "Storefront"}],
            },
            {"date": date(2026, 10, 11), "iso": "2026-10-11", "state": "off", "shifts": []},
            {"date": date(2026, 10, 12), "iso": "2026-10-12", "state": "timeoff", "shifts": []},
        ]
        overlaps = timeoff_overlaps(days)
        self.assertEqual(overlaps, [{"iso": "2026-10-10", "text": "Sat Oct 10 · 12 – 8 PM (Storefront)"}])
        self.assertEqual(overlap_summary(overlaps), "You're scheduled for 1 shift on these dates.")
        self.assertEqual(overlap_summary([]), "")

    def test_normalize_tab_and_kind(self):
        from app.team.requests_view import normalize_kind, normalize_tab

        self.assertEqual(normalize_tab("supply", ["timeoff", "supply"]), "supply")
        self.assertEqual(normalize_tab("supply", ["timeoff"]), "all")
        self.assertEqual(normalize_tab("bogus"), "all")
        self.assertEqual(normalize_kind("time-off"), "timeoff")
        self.assertEqual(normalize_kind("supplies"), "supply")
        self.assertEqual(normalize_kind(None), "")

    def test_home_request_rows_show_cancelled(self):
        from app.team.home import build_request_rows

        rows = build_request_rows(
            supply=[
                SimpleNamespace(
                    title="Tape",
                    status="cancelled",
                    status_changed_at=datetime(2026, 9, 22, 9),
                    created_at=datetime(2026, 9, 21, 9),
                )
            ]
        )
        self.assertEqual((rows[0]["pill_tone"], rows[0]["pill"]), ("neutral", "Cancelled"))
        self.assertEqual(rows[0]["sub"], "Cancelled Sep 22")
        self.assertFalse(rows[0]["pending"])
        self.assertEqual(rows[0]["href"], "/team/requests?tab=supply")

    def test_status_label_macro_has_cancelled(self):
        from app.shared import templates

        tmpl = templates.env.from_string(
            '{% from "_macros.html" import status_label %}{{ status_label("cancelled") }}'
        )
        self.assertEqual(tmpl.render().strip(), "Cancelled")


class RequestAlertWordingTests(unittest.TestCase):
    def _settings(self):
        return SimpleNamespace(
            public_base_url="https://ops.example.com",
            team_request_alert_email_to="alerts@example.com",
            team_request_alert_email_enabled=True,
            team_supply_discord_enabled=True,
            team_supply_discord_channel_id="123",
        )

    def test_timeoff_edit_and_cancel_wording(self):
        from app.team import request_alerts
        from app.team.email import EmailSendResult

        emails = []

        def fake_send_email(**kwargs):
            emails.append(kwargs)
            return EmailSendResult(provider="smtp", status="sent")

        with patch.object(request_alerts, "send_email", fake_send_email):
            for event in ("submitted", "edited", "cancelled"):
                request_alerts.send_timeoff_request_alert(
                    request_id=7,
                    employee_name="Maya",
                    employee_username="maya",
                    start_date="2026-10-10",
                    end_date="2026-10-12",
                    event=event,
                    settings=self._settings(),
                )
        self.assertTrue(emails[0]["subject"].startswith("[Degen] Time-off request: "))
        self.assertTrue(emails[0]["body"].startswith("New time-off request pending approval"))
        self.assertIn("Time-off request edited", emails[1]["subject"])
        self.assertIn("edited by the employee", emails[1]["body"])
        self.assertIn("Time-off request cancelled", emails[2]["subject"])
        self.assertIn("cancelled by the employee", emails[2]["body"])

    def test_supply_edit_and_cancel_wording_reaches_discord(self):
        from app.team import request_alerts
        from app.team.email import EmailSendResult

        emails, posts = [], []

        class _Resp:
            status_code = 200
            text = "{}"

            def json(self):
                return {"id": "1"}

        def fake_post(url, *, headers, json, timeout):
            posts.append(json["content"])
            return _Resp()

        with patch.object(
            request_alerts, "send_email", lambda **kw: emails.append(kw) or EmailSendResult(provider="smtp", status="sent")
        ), patch.object(request_alerts.httpx, "post", fake_post), patch.dict(
            os.environ, {"DEGEN_OPS_DISCORD_BOT_TOKEN": "t"}
        ):
            request_alerts.send_supply_request_alert(
                request_id=3, employee_name="Maya", title="Tape", event="edited", settings=self._settings()
            )
            request_alerts.send_supply_request_alert(
                request_id=3, employee_name="Maya", title="Tape", event="cancelled", settings=self._settings()
            )
        self.assertIn("Supply request edited", emails[0]["subject"])
        self.assertIn("**Supply Request Edited**", posts[0])
        self.assertIn("Supply request cancelled", emails[1]["subject"])
        self.assertIn("**Supply Request Cancelled**", posts[1])


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


class _RequestsHarness(_RouteHarness):
    def setUp(self):
        from app.routers import team_timeoff

        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        self.jordan = self._user(OTHER, "Jordan Lee")
        self.manager = self._user(MANAGER, "Jef Boss", role="manager")
        patcher = patch.object(team_timeoff, "clockify_today", return_value=TODAY)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.session.close)

    def _post_request(self, user, path):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=user),
            session={},
            headers={},
            cookies={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path=path, scheme="http", netloc="testserver"),
            scope={"path": path},
            query_params={},
        )

    def _timeoff(self, user_id=ME, start=None, end=None, status="submitted", **kw):
        from app.models import TimeOffRequest

        start = start or TODAY + timedelta(days=10)
        row = TimeOffRequest(
            submitted_by_user_id=user_id,
            start_date=start,
            end_date=end or start,
            reason=kw.pop("reason", "Trip"),
            status=status,
            created_at=datetime(2026, 9, 22, 9),
            **kw,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _supply(self, user_id=ME, status="submitted", **kw):
        from app.models import SupplyRequest

        row = SupplyRequest(
            submitted_by_user_id=user_id,
            title=kw.pop("title", "Penny sleeves"),
            description=kw.pop("description", "10 packs"),
            urgency=kw.pop("urgency", "normal"),
            status=status,
            created_at=datetime(2026, 9, 22, 9),
            **kw,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _page(self, user=None, **params):
        from app.routers import team_timeoff

        defaults = dict(tab=None, new=None, edit=None, id=None, date=None, flash=None, error=None)
        defaults.update(params)
        return self._capture(
            team_timeoff.team_requests,
            self._request(user or self.maya, "/team/requests"),
            session=self.session,
            **defaults,
        )

    def _set_permission(self, role, key, allowed):
        from app.models import RolePermission

        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == role, RolePermission.resource_key == key
            )
        ).first()
        self.assertIsNotNone(row, key)
        row.is_allowed = allowed
        self.session.add(row)
        self.session.commit()

    def _audits(self, action):
        from app.models import AuditLog

        return self.session.exec(select(AuditLog).where(AuditLog.action == action)).all()

    def _manager_notifications(self, kind):
        from app.models import AuditLog
        from app.team.team_notifications import EMPLOYEE_NOTIFICATION_ACTION

        rows = self.session.exec(
            select(AuditLog).where(AuditLog.action == EMPLOYEE_NOTIFICATION_ACTION)
        ).all()
        return [
            json.loads(r.details_json)
            for r in rows
            if json.loads(r.details_json).get("kind") == kind
        ]


class RequestsPageTests(_RequestsHarness, unittest.TestCase):
    def test_lists_open_and_past_as_cards_with_notes_and_pills(self):
        self._timeoff(start=TODAY + timedelta(days=10))
        self._timeoff(
            start=TODAY + timedelta(days=20),
            status="denied",
            decision_notes="Short-staffed that week.",
            approved_by_user_id=MANAGER,
            status_changed_at=datetime(2026, 9, 22, 12),
        )
        self._supply(status="cancelled", status_changed_at=datetime(2026, 9, 22, 13))
        self._supply(status="submitted", title="Toploaders")
        self._timeoff(user_id=OTHER)  # someone else's: never shown

        response, captured = self._page()
        ctx = captured["context"]
        html = response.body
        self.assertEqual(captured["template"], "team/requests.html")
        self.assertEqual(len(ctx["open_cards"]), 2)
        self.assertEqual(len(ctx["past_cards"]), 2)
        self.assertNotIn("<table", html)
        self.assertEqual(html.count('<li class="pt-req"'), 4)
        self.assertIn('<span class="pt-pill neutral">Cancelled</span>', html)
        self.assertIn('<span class="pt-pill err">Declined</span>', html)
        self.assertIn("Short-staffed that week.", html)
        self.assertIn("decided by Jef", html)
        # Edit / cancel only on pending rows.
        self.assertEqual(html.count("Cancel request</button>"), 2)
        self.assertEqual(html.count(">Edit</a>"), 2)
        self.assertIn('href="/team/requests?new=timeoff"', html)
        self.assertIn('href="/team/requests?new=supply"', html)
        self.assertIn('src="/static/portal-requests.js', html)

    def test_sheets_closed_by_default_and_rendered_for_no_js(self):
        response, captured = self._page()
        html = response.body
        self.assertEqual(captured["context"]["open_sheet"], "")
        self.assertIn('id="pt-sheet-new-timeoff" role="dialog"', html)
        self.assertIn('id="pt-sheet-new-supply" role="dialog"', html)
        self.assertNotIn('class="pt-sheet is-open"', html)

    def test_new_param_renders_that_sheet_open_without_js(self):
        response, captured = self._page(new="timeoff")
        html = response.body
        self.assertEqual(captured["context"]["open_sheet"], "pt-sheet-new-timeoff")
        self.assertIn('<section class="pt-sheet is-open" id="pt-sheet-new-timeoff"', html)
        self.assertIn('<a class="pt-sheet-bg is-open" href="/team/requests"', html)
        start = html.index('id="pt-sheet-new-timeoff"')
        self.assertNotIn("hidden", html[start : html.index(">", start)])
        self.assertIn('action="/team/timeoff"', html)
        self.assertIn(f'min="{TODAY.isoformat()}"', html)
        # The supply sheet stays hidden.
        start = html.index('id="pt-sheet-new-supply"')
        self.assertIn("hidden", html[start : html.index(">", start)])

    def test_date_prefill_and_server_side_overlap_warning(self):
        day = TODAY + timedelta(days=2)  # Friday
        self._shift(ME, day, "12-8")
        self._shift(OTHER, day, "10-6")  # not mine

        response, captured = self._page(new="timeoff", date=day.isoformat())
        html = response.body
        ctx = captured["context"]
        self.assertEqual(ctx["prefill_date"], day.isoformat())
        self.assertEqual(len(ctx["prefill_overlaps"]), 1)
        self.assertIn(f'value="{day.isoformat()}"', html)
        self.assertIn("You&#39;re scheduled for 1 shift on these dates.", html)
        self.assertIn("Fri Sep 25 · 12 – 8 PM (Storefront)", html)

    def test_past_prefill_date_is_ignored(self):
        _, captured = self._page(new="timeoff", date=(TODAY - timedelta(days=1)).isoformat())
        self.assertEqual(captured["context"]["prefill_date"], "")

    def test_open_card_counts_overlaps_including_stream_shifts(self):
        from app.models import StreamSchedule, Streamer

        start = TODAY + timedelta(days=3)
        self._shift(ME, start, "11-7")
        streamer = Streamer(name="maya", user_id=ME)
        self.session.add(streamer)
        self.session.commit()
        self.session.add(
            StreamSchedule(
                streamer_id=streamer.id,
                date=(start + timedelta(days=1)).isoformat(),
                start_time="18:00",
                end_time="23:00",
            )
        )
        self.session.commit()
        self._timeoff(start=start, end=start + timedelta(days=2))

        response, captured = self._page()
        card = captured["context"]["open_cards"][0]
        self.assertEqual(card["overlap_count"], 2)
        self.assertIn("overlaps 2 shifts", card["sub"])
        self.assertIn("(Stream)", " ".join(o["text"] for o in card["overlaps"]))

    def test_edit_param_opens_prefilled_edit_sheet(self):
        row = self._timeoff(reason="Wedding")
        response, captured = self._page(edit="timeoff", id=str(row.id))
        html = response.body
        sheet_id = f"pt-sheet-timeoff-{row.id}"
        self.assertEqual(captured["context"]["open_sheet"], sheet_id)
        self.assertIn(f'<section class="pt-sheet is-open" id="{sheet_id}"', html)
        self.assertIn(f'action="/team/timeoff/{row.id}/edit"', html)
        self.assertIn(">Wedding</textarea>", html)

    def test_edit_param_for_someone_elses_request_opens_nothing(self):
        row = self._timeoff(user_id=OTHER)
        _, captured = self._page(edit="timeoff", id=str(row.id))
        self.assertEqual(captured["context"]["open_sheet"], "")

    def test_tab_filters_list(self):
        self._timeoff()
        self._supply()
        _, captured = self._page(tab="supply")
        ctx = captured["context"]
        self.assertEqual([c["kind"] for c in ctx["open_cards"]], ["supply"])
        self.assertEqual(ctx["active"], "supply")
        response, _ = self._page()
        self.assertIn('<a href="/team/requests" aria-current="page">All</a>', response.body)

    def test_supply_only_user_sees_only_supply(self):
        self._set_permission("employee", "page.timeoff", False)
        self._timeoff()
        self._supply()
        response, captured = self._page()
        ctx = captured["context"]
        self.assertEqual(ctx["kinds"], ["supply"])
        self.assertEqual([c["kind"] for c in ctx["open_cards"]], ["supply"])
        self.assertNotIn("pt-sheet-new-timeoff", response.body)
        self.assertNotIn('aria-label="Show requests"', response.body)
        self.assertIn("pt-newreq is-single", response.body)

    def test_user_without_either_permission_gets_403(self):
        from app.routers import team_timeoff

        self._set_permission("employee", "page.timeoff", False)
        self._set_permission("employee", "page.supply_requests", False)
        response = team_timeoff.team_requests(
            self._request(self.maya, "/team/requests"),
            tab=None, new=None, edit=None, id=None, date=None, flash=None, error=None,
            session=self.session,
        )
        self.assertEqual(response.status_code, 403)

    def test_overlap_json_endpoint(self):
        from app.routers import team_timeoff

        day = TODAY + timedelta(days=2)
        self._shift(ME, day, "12-8")
        response = team_timeoff.team_requests_overlap(
            self._request(self.maya, "/team/requests/overlap"),
            start=day.isoformat(),
            end=(day + timedelta(days=1)).isoformat(),
            session=self.session,
        )
        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual([s["iso"] for s in payload["shifts"]], [day.isoformat()])
        self.assertIn("1 shift", payload["summary"])
        bad = team_timeoff.team_requests_overlap(
            self._request(self.maya, "/team/requests/overlap"),
            start=day.isoformat(),
            end=(day - timedelta(days=1)).isoformat(),
            session=self.session,
        )
        self.assertFalse(json.loads(bad.body)["ok"])

    def test_template_has_no_inline_styles_or_scripts(self):
        source = Path("app/templates/team/requests.html").read_text(encoding="utf-8")
        self.assertNotIn("style=", source)
        self.assertNotIn("<style", source)
        self.assertNotIn("<script>", source)


class OldUrlRedirectTests(unittest.TestCase):
    def test_timeoff_get_redirects_to_requests_keeping_date(self):
        from app.routers.team_timeoff import team_timeoff

        response = team_timeoff(SimpleNamespace(), date="2026-10-10", flash=None, error=None)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"], "/team/requests?new=timeoff&date=2026-10-10"
        )

    def test_timeoff_get_without_params(self):
        from app.routers.team_timeoff import team_timeoff

        response = team_timeoff(SimpleNamespace(), date=None, flash=None, error=None)
        self.assertEqual(response.headers["location"], "/team/requests?new=timeoff")

    def test_supply_get_redirects_to_requests_form(self):
        from app.routers.team import team_supply

        response = team_supply(flash="Request submitted.", error=None)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            _query(response.headers["location"]),
            {"new": ["supply"], "flash": ["Request submitted."]},
        )


class CancelEditRouteTests(_RequestsHarness, unittest.TestCase):
    def _call(self, fn, user, path, *args, **kwargs):
        return asyncio.run(fn(self._post_request(user, path), *args, session=self.session, **kwargs))

    # -- CSRF -------------------------------------------------------------

    def test_all_change_routes_require_csrf(self):
        from app.csrf import require_csrf
        from app.routers import team_timeoff

        wanted = {
            "/team/timeoff/{request_id}/cancel",
            "/team/timeoff/{request_id}/edit",
            "/team/supply/{request_id}/cancel",
            "/team/supply/{request_id}/edit",
        }
        found = {}
        for route in team_timeoff.router.routes:
            if getattr(route, "path", None) in wanted and "POST" in getattr(route, "methods", set()):
                found[route.path] = [dep.call for dep in route.dependant.dependencies]
        self.assertEqual(set(found), wanted)
        for path, calls in found.items():
            self.assertIn(require_csrf, calls, path)


    # -- time off: cancel -----------------------------------------------

    def test_owner_cancels_pending_timeoff(self):
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        row = self._timeoff()
        with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
            response = self._call(
                team_timeoff.team_timeoff_cancel, self.maya, f"/team/timeoff/{row.id}/cancel", row.id
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("flash=", response.headers["location"])
        self.session.expire_all()
        fresh = self.session.get(TimeOffRequest, row.id)
        self.assertEqual(fresh.status, "cancelled")
        self.assertIsNotNone(fresh.status_changed_at)
        audits = self._audits("timeoff.cancelled")
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0].actor_user_id, ME)
        self.assertEqual(json.loads(audits[0].details_json)["time_off_request_id"], row.id)
        alert.assert_called_once()
        self.assertEqual(alert.call_args.kwargs["event"], "cancelled")
        notes = self._manager_notifications("timeoff_cancelled")
        self.assertEqual(len(notes), 1)
        self.assertIn("cancelled", notes[0]["title"].lower())

    def test_other_user_cannot_cancel(self):
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        row = self._timeoff()
        with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
            response = self._call(
                team_timeoff.team_timeoff_cancel, self.jordan, f"/team/timeoff/{row.id}/cancel", row.id
            )
        self.assertEqual(response.status_code, 404)
        self.session.expire_all()
        self.assertEqual(self.session.get(TimeOffRequest, row.id).status, "submitted")
        alert.assert_not_called()
        self.assertEqual(self._audits("timeoff.cancelled"), [])

    def test_cannot_cancel_decided_timeoff(self):
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        for status in ("approved", "denied", "cancelled"):
            row = self._timeoff(status=status, start=TODAY + timedelta(days=30 + len(status)))
            with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
                response = self._call(
                    team_timeoff.team_timeoff_cancel, self.maya, "/x", row.id
                )
            self.assertEqual(response.status_code, 303, status)
            self.assertIn("already", _query(response.headers["location"])["error"][0], status)
            self.session.expire_all()
            self.assertEqual(self.session.get(TimeOffRequest, row.id).status, status)
            alert.assert_not_called()

    def test_cancel_racing_a_manager_decision_is_refused(self):
        """Owner + status are re-checked inside the UPDATE, not just on read."""
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        row = self._timeoff()
        stale = SimpleNamespace(**{k: getattr(row, k) for k in ("id", "submitted_by_user_id", "status")})
        self.session.exec(
            __import__("sqlalchemy").update(TimeOffRequest)
            .where(TimeOffRequest.id == row.id)
            .values(status="approved")
        )
        self.session.commit()
        # _owned_row hands back the stale 'submitted' snapshot, as if the
        # manager approved between our read and our write.
        with patch.object(team_timeoff, "_owned_row", return_value=stale), patch.object(
            team_timeoff, "send_timeoff_request_alert"
        ) as alert:
            response = self._call(team_timeoff.team_timeoff_cancel, self.maya, "/x", row.id)
        self.assertEqual(response.status_code, 303)
        self.assertIn("marked that request approved", _query(response.headers["location"])["error"][0])
        self.session.expire_all()
        self.assertEqual(self.session.get(TimeOffRequest, row.id).status, "approved")
        alert.assert_not_called()
        self.assertEqual(self._audits("timeoff.cancelled"), [])

    # -- time off: edit ---------------------------------------------------

    def test_owner_edits_pending_timeoff_and_managers_hear_it_was_edited(self):
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        row = self._timeoff(start=TODAY + timedelta(days=10))
        new_start = TODAY + timedelta(days=12)
        new_end = TODAY + timedelta(days=13)
        with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
            response = self._call(
                team_timeoff.team_timeoff_edit,
                self.maya,
                "/x",
                row.id,
                start_date=new_start.isoformat(),
                end_date=new_end.isoformat(),
                reason="  moved  ",
            )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(_query(response.headers["location"])["flash"], ["Time-off request updated."])
        self.session.expire_all()
        fresh = self.session.get(TimeOffRequest, row.id)
        self.assertEqual((fresh.start_date, fresh.end_date, fresh.reason), (new_start, new_end, "moved"))
        self.assertEqual(fresh.status, "submitted")
        audit = self._audits("timeoff.edited")
        self.assertEqual(len(audit), 1)
        details = json.loads(audit[0].details_json)
        self.assertEqual(details["after"]["start_date"], new_start.isoformat())
        self.assertEqual(details["before"]["reason"], "Trip")
        alert.assert_called_once_with(
            request_id=row.id,
            employee_name="Maya Rodriguez",
            employee_username="maya",
            start_date=new_start.isoformat(),
            end_date=new_end.isoformat(),
            reason="moved",
            event="edited",
        )
        notes = self._manager_notifications("timeoff_edited")
        self.assertEqual(len(notes), 1)
        self.assertIn("edited", notes[0]["body"])

    def test_edit_validates_dates_and_reopens_the_sheet(self):
        from app.models import TimeOffRequest
        from app.routers import team_timeoff

        row = self._timeoff()
        cases = [
            ((TODAY + timedelta(days=5)).isoformat(), (TODAY + timedelta(days=4)).isoformat(), "on or after"),
            ((TODAY - timedelta(days=1)).isoformat(), TODAY.isoformat(), "past"),
            ("nope", "", "valid"),
        ]
        for start, end, needle in cases:
            with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
                response = self._call(
                    team_timeoff.team_timeoff_edit, self.maya, "/x", row.id,
                    start_date=start, end_date=end, reason="",
                )
            q = _query(response.headers["location"])
            self.assertIn(needle, q["error"][0])
            self.assertEqual(q["edit"], ["timeoff"])
            self.assertEqual(q["id"], [str(row.id)])
            alert.assert_not_called()
        self.session.expire_all()
        self.assertEqual(self.session.get(TimeOffRequest, row.id).start_date, row.start_date)

    def test_edit_does_not_conflict_with_itself_but_does_with_others(self):
        from app.routers import team_timeoff

        row = self._timeoff(start=TODAY + timedelta(days=10), end=TODAY + timedelta(days=11))
        self._timeoff(start=TODAY + timedelta(days=20), status="approved")
        with patch.object(team_timeoff, "send_timeoff_request_alert"):
            ok = self._call(
                team_timeoff.team_timeoff_edit, self.maya, "/x", row.id,
                start_date=(TODAY + timedelta(days=11)).isoformat(),
                end_date=(TODAY + timedelta(days=12)).isoformat(),
                reason="Trip",
            )
            clash = self._call(
                team_timeoff.team_timeoff_edit, self.maya, "/x", row.id,
                start_date=(TODAY + timedelta(days=19)).isoformat(),
                end_date=(TODAY + timedelta(days=21)).isoformat(),
                reason="Trip",
            )
        self.assertIn("flash=", ok.headers["location"])
        self.assertIn("pending+request", clash.headers["location"])

    def test_other_user_cannot_edit_and_decided_cannot_be_edited(self):
        from app.routers import team_timeoff

        mine = self._timeoff()
        approved = self._timeoff(start=TODAY + timedelta(days=40), status="approved")
        kwargs = dict(
            start_date=(TODAY + timedelta(days=50)).isoformat(),
            end_date=(TODAY + timedelta(days=50)).isoformat(),
            reason="x",
        )
        with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
            other = self._call(team_timeoff.team_timeoff_edit, self.jordan, "/x", mine.id, **kwargs)
            decided = self._call(team_timeoff.team_timeoff_edit, self.maya, "/x", approved.id, **kwargs)
        self.assertEqual(other.status_code, 404)
        self.assertIn("already", _query(decided.headers["location"])["error"][0])
        alert.assert_not_called()
        self.assertEqual(self._audits("timeoff.edited"), [])

    def test_unchanged_edit_sends_no_alert(self):
        from app.routers import team_timeoff

        row = self._timeoff()
        with patch.object(team_timeoff, "send_timeoff_request_alert") as alert:
            response = self._call(
                team_timeoff.team_timeoff_edit, self.maya, "/x", row.id,
                start_date=row.start_date.isoformat(), end_date=row.end_date.isoformat(), reason="Trip",
            )
        self.assertEqual(_query(response.headers["location"])["flash"], ["No changes to save."])
        alert.assert_not_called()

    # -- supplies ---------------------------------------------------------

    def test_owner_cancels_pending_supply(self):
        from app.models import SupplyRequest
        from app.routers import team_timeoff

        row = self._supply()
        with patch.object(team_timeoff, "send_supply_request_alert") as alert:
            response = self._call(team_timeoff.team_supply_cancel, self.maya, "/x", row.id)
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        fresh = self.session.get(SupplyRequest, row.id)
        self.assertEqual(fresh.status, "cancelled")
        self.assertIsNotNone(fresh.status_changed_at)
        self.assertEqual(len(self._audits("supply.cancelled")), 1)
        self.assertEqual(alert.call_args.kwargs["event"], "cancelled")
        self.assertEqual(len(self._manager_notifications("supply_cancelled")), 1)

    def test_owner_edits_pending_supply(self):
        from app.models import SupplyRequest
        from app.routers import team_timeoff

        row = self._supply()
        with patch.object(team_timeoff, "send_supply_request_alert") as alert:
            response = self._call(
                team_timeoff.team_supply_edit, self.maya, "/x", row.id,
                title="Toploaders", description="200", urgency="high",
            )
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        fresh = self.session.get(SupplyRequest, row.id)
        self.assertEqual((fresh.title, fresh.description, fresh.urgency), ("Toploaders", "200", "high"))
        self.assertEqual(len(self._audits("supply.edited")), 1)
        alert.assert_called_once_with(
            request_id=row.id,
            employee_name="Maya Rodriguez",
            employee_username="maya",
            title="Toploaders",
            description="200",
            urgency="high",
            event="edited",
        )
        notes = self._manager_notifications("supply_edited")
        self.assertEqual(len(notes), 1)
        self.assertIn("edited", notes[0]["body"])

    def test_supply_edit_requires_title_and_owner_and_pending(self):
        from app.routers import team_timeoff

        row = self._supply()
        ordered = self._supply(status="ordered")
        with patch.object(team_timeoff, "send_supply_request_alert") as alert:
            blank = self._call(team_timeoff.team_supply_edit, self.maya, "/x", row.id, title="  ", description="", urgency="low")
            other = self._call(team_timeoff.team_supply_edit, self.jordan, "/x", row.id, title="x", description="", urgency="low")
            other_cancel = self._call(team_timeoff.team_supply_cancel, self.jordan, "/x", row.id)
            decided = self._call(team_timeoff.team_supply_cancel, self.maya, "/x", ordered.id)
        self.assertEqual(_query(blank.headers["location"])["edit"], ["supply"])
        self.assertEqual(other.status_code, 404)
        self.assertEqual(other_cancel.status_code, 404)
        self.assertIn("already", _query(decided.headers["location"])["error"][0])
        alert.assert_not_called()

    # -- new submissions land back on /team/requests ----------------------

    def test_new_timeoff_and_supply_redirect_to_requests(self):
        from app.routers import team, team_timeoff

        with patch.object(team_timeoff, "send_timeoff_request_alert"):
            ok = self._call(
                team_timeoff.team_timeoff_post, self.maya, "/team/timeoff",
                start_date=(TODAY + timedelta(days=3)).isoformat(),
                end_date=(TODAY + timedelta(days=3)).isoformat(),
                reason="",
            )
            bad = self._call(
                team_timeoff.team_timeoff_post, self.maya, "/team/timeoff",
                start_date="x", end_date="", reason="",
            )
        self.assertTrue(ok.headers["location"].startswith("/team/requests?"))
        self.assertEqual(_query(bad.headers["location"])["new"], ["timeoff"])
        with patch.object(team, "send_supply_request_alert"):
            ok = self._call(team.team_supply_post, self.maya, "/team/supply", title="Tape", description="", urgency="normal")
            bad = self._call(team.team_supply_post, self.maya, "/team/supply", title="", description="", urgency="normal")
        self.assertTrue(ok.headers["location"].startswith("/team/requests?"))
        self.assertEqual(_query(bad.headers["location"])["new"], ["supply"])

    def test_cancelled_request_frees_the_dates(self):
        from app.routers import team_timeoff

        row = self._timeoff(start=TODAY + timedelta(days=10))
        with patch.object(team_timeoff, "send_timeoff_request_alert"):
            self._call(team_timeoff.team_timeoff_cancel, self.maya, "/x", row.id)
            again = self._call(
                team_timeoff.team_timeoff_post, self.maya, "/team/timeoff",
                start_date=row.start_date.isoformat(), end_date=row.end_date.isoformat(), reason="",
            )
        self.assertIn("flash=", again.headers["location"])


class ManagerQueueCancelledTests(_RequestsHarness, unittest.TestCase):
    def _admin_request(self, path):
        from app.csrf import issue_token

        request = self._post_request(self.manager, path)
        return request, issue_token(request)

    def test_timeoff_queue_hides_cancelled_by_default(self):
        from app.routers import team_admin_timeoff

        live = self._timeoff()
        gone = self._timeoff(start=TODAY + timedelta(days=30), status="cancelled")
        request = self._post_request(self.manager, "/team/admin/timeoff")
        ctx = team_admin_timeoff.admin_timeoff_list(
            request, status=None, flash=None, error=None, session=self.session
        ).context
        self.assertEqual([r.id for r in ctx["requests"]], [live.id])
        self.assertEqual(ctx["counts"]["cancelled"], 1)
        self.assertIn("cancelled", ctx["statuses"])
        ctx = team_admin_timeoff.admin_timeoff_list(
            request, status="cancelled", flash=None, error=None, session=self.session
        ).context
        self.assertEqual([r.id for r in ctx["requests"]], [gone.id])

    def test_approve_or_deny_cancelled_timeoff_is_refused(self):
        from app.models import ShiftEntry, TimeOffRequest
        from app.routers import team_admin_timeoff

        row = self._timeoff(status="cancelled")
        for fn in (team_admin_timeoff.admin_timeoff_approve, team_admin_timeoff.admin_timeoff_deny):
            request, token = self._admin_request(f"/team/admin/timeoff/{row.id}/x")
            response = asyncio.run(
                fn(request, row.id, decision_notes="", csrf_token=token, session=self.session)
            )
            self.assertEqual(response.status_code, 303)
            self.assertIn("cancelled", _query(response.headers["location"])["error"][0])
        self.session.expire_all()
        self.assertEqual(self.session.get(TimeOffRequest, row.id).status, "cancelled")
        self.assertEqual(self.session.exec(select(ShiftEntry)).all(), [])

    def test_supply_queue_hides_cancelled_and_refuses_decisions(self):
        from app.models import SupplyRequest
        from app.routers import team_admin_supply

        live = self._supply()
        gone = self._supply(status="cancelled")
        with patch.object(team_admin_supply.templates, "TemplateResponse", side_effect=lambda r, t, c: SimpleNamespace(context=c)):
            ctx = team_admin_supply.admin_supply_list(
                self._post_request(self.manager, "/team/admin/supply"),
                status=None, flash=None, error=None, session=self.session,
            ).context
        self.assertEqual([r.id for r in ctx["requests"]], [live.id])
        self.assertEqual(ctx["counts"]["cancelled"], 1)

        for fn, kwargs in (
            (team_admin_supply.admin_supply_approve, {}),
            (team_admin_supply.admin_supply_deny, {"notes": ""}),
            (team_admin_supply.admin_supply_mark_ordered, {}),
        ):
            response = asyncio.run(
                fn(self._post_request(self.manager, "/x"), gone.id, session=self.session, **kwargs)
            )
            self.assertEqual(response.status_code, 303)
            self.assertIn("cancelled", _query(response.headers["location"])["error"][0])
        self.session.expire_all()
        self.assertEqual(self.session.get(SupplyRequest, gone.id).status, "cancelled")

    def test_supply_state_machine_treats_cancelled_as_final(self):
        from fastapi import HTTPException

        from app.routers.team_admin_supply import _validate_transition

        for target in ("approved", "denied", "ordered", "submitted"):
            with self.assertRaises(HTTPException):
                _validate_transition("cancelled", target)


class RequestsEndToEndTests(unittest.TestCase):
    """Through the real app + TestClient: CSRF and redirects on the wire."""

    def setUp(self):
        from tests.test_employee_portal_wave3 import SupplyAndPoliciesTests

        # Reuse wave3's harness (TestClient + in-memory DB + login patch).
        self.h = SupplyAndPoliciesTests("test_supply_post_without_csrf_is_403")
        self.h._setup_portal()
        self.addCleanup(self.h._teardown_portal)
        self.uid = self.h._seed_employee(user_id=61, username="emp_req")

    def _row(self):
        from app.models import TimeOffRequest

        row = TimeOffRequest(
            submitted_by_user_id=self.uid,
            start_date=date.today() + timedelta(days=20),
            end_date=date.today() + timedelta(days=20),
            status="submitted",
        )
        self.h.session.add(row)
        self.h.session.commit()
        self.h.session.refresh(row)
        return row

    def test_cancel_without_csrf_is_403_and_with_csrf_cancels(self):
        from app.models import TimeOffRequest

        row = self._row()
        r = self.h.client.post(f"/team/timeoff/{row.id}/cancel", data={}, follow_redirects=False)
        self.assertEqual(r.status_code, 403)
        self.h.session.expire_all()
        self.assertEqual(self.h.session.get(TimeOffRequest, row.id).status, "submitted")

        csrf = self.h._csrf()
        with patch("app.routers.team_timeoff.send_timeoff_request_alert"):
            r = self.h.client.post(
                f"/team/timeoff/{row.id}/cancel", data={"csrf_token": csrf}, follow_redirects=False
            )
        self.assertEqual(r.status_code, 303)
        self.h.session.expire_all()
        self.assertEqual(self.h.session.get(TimeOffRequest, row.id).status, "cancelled")

    def test_old_urls_redirect_and_page_renders(self):
        r = self.h.client.get("/team/timeoff?date=2030-01-02", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/requests?new=timeoff&date=2030-01-02")
        page = self.h.client.get("/team/requests?new=timeoff&date=2030-01-02")
        self.assertEqual(page.status_code, 200)
        self.assertIn('<section class="pt-sheet is-open" id="pt-sheet-new-timeoff"', page.text)
        self.assertIn('value="2030-01-02"', page.text)


class HomeMatchesHoursTests(_RequestsHarness, unittest.TestCase):
    """Home's 'This week … scheduled' uses the same helper as /team/hours."""

    def test_home_scheduled_equals_hours_scheduled_with_stream_shift(self):
        from app.models import ClockifyTimeEntry, EmployeeProfile, StreamSchedule, Streamer
        from app.routers import team

        profile = self.session.get(EmployeeProfile, ME)
        profile.clockify_user_id = "ck-10"
        self.session.add(profile)
        self._shift(ME, MONDAY, "12-8")
        streamer = Streamer(name="maya", user_id=ME)
        self.session.add(streamer)
        self.session.commit()
        self.session.add(
            StreamSchedule(
                streamer_id=streamer.id,
                date=(MONDAY + timedelta(days=1)).isoformat(),
                start_time="18:00",
                end_time="22:30",
            )
        )
        start = datetime(2026, 9, 21, 12, 0, tzinfo=LA)
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id="e-1",
                clockify_user_id="ck-10",
                user_id=ME,
                description="Shift",
                start_at=start.astimezone(timezone.utc),
                end_at=(start + timedelta(hours=4)).astimezone(timezone.utc),
                duration_seconds=4 * 3600,
                is_deleted=False,
            )
        )
        self.session.commit()

        with patch.object(team, "get_settings", return_value=_settings()), patch.object(
            team, "_portal_today", return_value=TODAY
        ):
            home = team._employee_home_context(
                self.session,
                self.maya,
                today=TODAY,
                now_local=datetime(2026, 9, 23, 9, 0, tzinfo=LA),
                settings=_settings(),
                clockify_ready=True,
            )["home"]
            _, captured = self._capture(
                team.team_hours,
                self._request(self.maya, "/team/hours"),
                week=MONDAY.isoformat(),
                session=self.session,
            )
        hours = captured["context"]["hours"]
        self.assertEqual(hours["scheduled_label"], "12.5")  # 8h storefront + 4.5h stream
        self.assertEqual(home["tiles"]["week_scheduled"], hours["scheduled_label"])
        # The stream day gets a dot on Home's week strip too.
        tuesday = home["week"][1]
        self.assertTrue(tuesday["has_shift"])


if __name__ == "__main__":
    unittest.main()
