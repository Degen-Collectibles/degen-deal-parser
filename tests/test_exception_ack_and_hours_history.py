"""Exception acknowledgement, employee week history, and approval visibility.

Three gaps from the /team tracking audit:

  * The exceptions page was a flat recomputed list with no way to say "seen,
    handled", so anything a manager had already decided re-rendered forever
    and buried the genuinely new rows.
  * Employees could only ever see the current week, so they could not check a
    past week against a paycheck.
  * An employee could not see whether their own day was approved, rejected or
    locked -- a manager marking a day "Needs fix" with a note was invisible to
    the one person who could act on it.
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
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "ack-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "ack-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "ack-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "ack-admin-password")

LA = ZoneInfo("America/Los_Angeles")
WEEK = date(2026, 4, 20)


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
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user


class ExceptionFingerprintTests(_Base):
    def test_fingerprint_is_stable_for_the_same_row(self):
        from app.routers.team_admin_clockify import exception_fingerprint

        args = dict(week_start=WEEK, user_id=2, category="Pending timecard", detail="x")
        self.assertEqual(exception_fingerprint(**args), exception_fingerprint(**args))

    def test_fingerprint_changes_when_the_detail_changes(self):
        """An ack covers the information as it stood; new facts must resurface."""
        from app.routers.team_admin_clockify import exception_fingerprint

        base = dict(week_start=WEEK, user_id=2, category="Pending timecard")
        self.assertNotEqual(
            exception_fingerprint(detail="2026-04-20 needs review", **base),
            exception_fingerprint(detail="2026-04-21 needs review", **base),
        )

    def test_fingerprint_is_scoped_per_employee_and_week(self):
        from app.routers.team_admin_clockify import exception_fingerprint

        base = dict(category="Pending timecard", detail="same text")
        self.assertNotEqual(
            exception_fingerprint(week_start=WEEK, user_id=2, **base),
            exception_fingerprint(week_start=WEEK, user_id=3, **base),
        )
        self.assertNotEqual(
            exception_fingerprint(week_start=WEEK, user_id=2, **base),
            exception_fingerprint(
                week_start=WEEK + timedelta(days=7), user_id=2, **base
            ),
        )


class ExceptionAckFilteringTests(_Base):
    def _exceptions(self):
        from app.routers import team_admin_clockify as mod

        return mod.build_timecard_exceptions(
            self.session,
            week_start=WEEK,
            settings=_settings(),
            include_inactive=True,
            now=datetime(2026, 4, 24, 20, 0, tzinfo=timezone.utc),
        )

    def _ack(self, row, note=""):
        from app.models import TimecardExceptionAck

        self.session.add(
            TimecardExceptionAck(
                fingerprint=row["fingerprint"],
                week_start=WEEK,
                user_id=row["user_id"],
                category=row["category"],
                detail=row["detail"],
                note=note,
                acked_by_user_id=self.admin.id,
            )
        )
        self.session.commit()

    def test_rows_carry_a_fingerprint(self):
        result = self._exceptions()
        self.assertTrue(result["all_rows"], "fixture should produce exceptions")
        for row in result["all_rows"]:
            self.assertTrue(row["fingerprint"])

    def test_acked_row_leaves_the_working_list(self):
        before = self._exceptions()
        target = before["rows"][0]
        self._ack(target)

        after = self._exceptions()

        self.assertEqual(len(after["rows"]), len(before["rows"]) - 1)
        self.assertEqual(after["acked_count"], 1)
        self.assertNotIn(
            target["fingerprint"], [row["fingerprint"] for row in after["rows"]]
        )

    def test_acked_row_is_still_computed_and_visible(self):
        """Hiding is not suppression -- it stays available under the fold."""
        target = self._exceptions()["rows"][0]
        self._ack(target, note="spoke to them")

        after = self._exceptions()
        acked = [
            row for row in after["acked_rows"] if row["fingerprint"] == target["fingerprint"]
        ]

        self.assertEqual(len(acked), 1)
        self.assertTrue(acked[0]["acknowledged"])
        self.assertEqual(acked[0]["ack_note"], "spoke to them")
        self.assertIn(target["fingerprint"], [r["fingerprint"] for r in after["all_rows"]])

    def test_counts_reflect_only_open_rows(self):
        before = self._exceptions()
        target = before["rows"][0]
        self._ack(target)
        after = self._exceptions()

        self.assertEqual(after["total_count"], before["total_count"] - 1)
        severity_total = (
            after["danger_count"] + after["warn_count"] + after["info_count"]
        )
        self.assertEqual(severity_total, after["total_count"])

    def test_ack_from_another_week_does_not_leak(self):
        from app.models import TimecardExceptionAck

        target = self._exceptions()["rows"][0]
        self.session.add(
            TimecardExceptionAck(
                fingerprint=target["fingerprint"],
                week_start=WEEK + timedelta(days=7),
                acked_by_user_id=self.admin.id,
            )
        )
        self.session.commit()

        after = self._exceptions()

        self.assertEqual(after["acked_count"], 0)


class EmployeeHoursHistoryTests(_Base):
    def _cache(self, day, hours=8):
        from app.models import ClockifyTimeEntry, EmployeeProfile

        if self.session.get(EmployeeProfile, 2) is None:
            self.session.add(EmployeeProfile(user_id=2, clockify_user_id="ck-2"))
            self.session.commit()
        start = datetime(day.year, day.month, day.day, 9, 0, tzinfo=LA).astimezone(
            timezone.utc
        )
        end = start + timedelta(hours=hours)
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id=f"e-{day.isoformat()}",
                clockify_user_id="ck-2",
                user_id=2,
                description="Shift",
                start_at=start,
                end_at=end,
                duration_seconds=hours * 3600,
                is_deleted=False,
            )
        )
        self.session.commit()

    def _week(self, week_of, today=date(2026, 4, 27)):
        from app.routers import team as mod

        with patch.object(mod, "get_settings", return_value=_settings()):
            return mod.employee_week_hours(
                self.session,
                self.employee,
                today=today,
                week_of=week_of,
                settings=_settings(),
            )

    def test_past_week_is_reportable(self):
        self._cache(WEEK, hours=4)

        week = self._week(WEEK)

        self.assertEqual(week["week_start"], WEEK)
        self.assertEqual(week["total_work_seconds"], 4 * 3600)

    def test_week_selector_does_not_move_today(self):
        """Paging back must not make a past day look like today."""
        self._cache(WEEK, hours=4)

        week = self._week(WEEK, today=date(2026, 4, 27))

        self.assertFalse(any(day["is_today"] for day in week["days"]))

    def test_parser_clamps_forward_paging(self):
        from app.routers.team import _parse_employee_week

        this_week = date(2026, 4, 27)
        self.assertEqual(
            _parse_employee_week("2026-12-31", this_week),
            this_week,
            "employees should not page into the future",
        )

    def test_parser_snaps_to_monday_and_survives_garbage(self):
        from app.routers.team import _parse_employee_week

        this_week = date(2026, 4, 27)
        self.assertEqual(_parse_employee_week("2026-04-22", this_week), WEEK)
        self.assertEqual(_parse_employee_week("not-a-date", this_week), this_week)
        self.assertEqual(_parse_employee_week(None, this_week), this_week)

    def test_parser_bounds_history(self):
        from app.routers.team import _parse_employee_week

        this_week = date(2026, 4, 27)
        result = _parse_employee_week("1999-01-04", this_week)
        self.assertGreaterEqual(result, this_week - timedelta(days=730))


class EmployeeHoursLiveFallbackTests(_Base):
    """Only the current week may fall through to a live Clockify call.

    Past weeks read cache-only: the cache is authoritative once the week is
    over, and falling through hit the API once per empty week while an employee
    paged back through history -- surfacing an API error as though the week
    itself had failed to load.
    """

    def _week(self, week_of, today):
        from app.routers import team as mod
        from app.models import EmployeeProfile

        if self.session.get(EmployeeProfile, 2) is None:
            self.session.add(EmployeeProfile(user_id=2, clockify_user_id="ck-2"))
            self.session.commit()

        calls = []

        class _Client:
            def get_user_time_entries(self, *a, **kw):
                calls.append(kw)
                return []

        with patch.object(mod, "get_settings", return_value=_settings()), patch.object(
            mod, "clockify_is_configured", return_value=True
        ), patch.object(mod, "clockify_client_from_settings", return_value=_Client()):
            result = mod.employee_week_hours(
                self.session,
                self.employee,
                today=today,
                week_of=week_of,
                settings=_settings(),
            )
        return result, calls

    def test_current_week_may_call_clockify(self):
        _week, calls = self._week(WEEK, today=date(2026, 4, 22))
        self.assertEqual(len(calls), 1)

    def test_past_week_never_calls_clockify(self):
        _week, calls = self._week(WEEK, today=date(2026, 5, 11))
        self.assertEqual(calls, [])

    def test_empty_past_week_reports_zero_not_an_error(self):
        week, _calls = self._week(WEEK, today=date(2026, 5, 11))
        self.assertEqual(week["error"], "")
        self.assertEqual(week["total_work_seconds"], 0)
        self.assertEqual(len(week["days"]), 7)


class EmployeeApprovalVisibilityTests(_Base):
    def _cache(self, day):
        from app.models import ClockifyTimeEntry, EmployeeProfile

        if self.session.get(EmployeeProfile, 2) is None:
            self.session.add(EmployeeProfile(user_id=2, clockify_user_id="ck-2"))
            self.session.commit()
        start = datetime(day.year, day.month, day.day, 9, 0, tzinfo=LA).astimezone(
            timezone.utc
        )
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id=f"e-{day.isoformat()}",
                clockify_user_id="ck-2",
                user_id=2,
                description="Shift",
                start_at=start,
                end_at=start + timedelta(hours=4),
                duration_seconds=4 * 3600,
                is_deleted=False,
            )
        )
        self.session.commit()

    def _approval(self, day, status, note=""):
        from app.models import TimecardApproval

        self.session.add(
            TimecardApproval(user_id=2, work_date=day, status=status, note=note)
        )
        self.session.commit()

    def _week(self):
        from app.routers import team as mod

        with patch.object(mod, "get_settings", return_value=_settings()):
            return mod.employee_week_hours(
                self.session,
                self.employee,
                today=date(2026, 4, 22),
                week_of=WEEK,
                settings=_settings(),
            )

    def test_employee_sees_their_own_rejection_and_note(self):
        self._cache(WEEK)
        self._approval(WEEK, "rejected", note="Clock-out looks wrong")

        week = self._week()
        monday = next(d for d in week["days"] if d["day"] == WEEK)

        self.assertEqual(monday["status"], "rejected")
        self.assertEqual(monday["status_label"], "Needs fix")
        self.assertEqual(monday["status_note"], "Clock-out looks wrong")
        self.assertEqual(len(week["needs_fix_days"]), 1)

    def test_approved_day_reads_as_approved(self):
        self._cache(WEEK)
        self._approval(WEEK, "approved")

        monday = next(d for d in self._week()["days"] if d["day"] == WEEK)

        self.assertEqual(monday["status_label"], "Approved")
        self.assertEqual(monday["status_tone"], "ok")

    def test_locked_day_reads_as_final_not_locked(self):
        """'Locked' is internal jargon; employees get plainer wording."""
        self._cache(WEEK)
        self._approval(WEEK, "locked")

        monday = next(d for d in self._week()["days"] if d["day"] == WEEK)

        self.assertEqual(monday["status_label"], "Final")

    def test_day_with_no_approval_row_has_no_status(self):
        self._cache(WEEK)

        monday = next(d for d in self._week()["days"] if d["day"] == WEEK)

        self.assertEqual(monday["status"], "")
        self.assertEqual(monday["status_label"], "")

    def test_only_the_employees_own_approvals_are_read(self):
        from app.models import TimecardApproval

        self._cache(WEEK)
        self.session.add(
            TimecardApproval(
                user_id=self.admin.id, work_date=WEEK, status="rejected", note="other"
            )
        )
        self.session.commit()

        monday = next(d for d in self._week()["days"] if d["day"] == WEEK)

        self.assertEqual(monday["status"], "")


class ExceptionAckRouteTests(_Base):
    """The POST handler itself: creates, undoes, audits, and refuses."""

    def _post(self, actor, **form):
        import asyncio

        from app.routers import team_admin_clockify as mod

        request = SimpleNamespace(
            state=SimpleNamespace(current_user=actor),
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path="/team/admin/exceptions/ack"),
            scope={"path": "/team/admin/exceptions/ack"},
            headers={},
            cookies={},
            session={},
            query_params={},
        )
        payload = {
            "fingerprint": "fp-1",
            "week": WEEK.isoformat(),
            "category": "Pending timecard",
            "detail": "2026-04-20 needs review",
            "user_id": "2",
            "note": "",
            "undo": "0",
        }
        payload.update(form)
        return asyncio.run(
            mod.admin_exception_ack(request, session=self.session, **payload)
        )

    def _acks(self):
        from app.models import TimecardExceptionAck

        return list(self.session.exec(select(TimecardExceptionAck)).all())

    def test_ack_persists_the_row(self):
        response = self._post(self.admin)

        self.assertEqual(response.status_code, 303)
        rows = self._acks()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fingerprint, "fp-1")
        self.assertEqual(rows[0].week_start, WEEK)
        self.assertEqual(rows[0].acked_by_user_id, self.admin.id)

    def test_ack_is_idempotent(self):
        self._post(self.admin)
        self._post(self.admin)

        self.assertEqual(len(self._acks()), 1)

    def test_undo_removes_the_ack(self):
        self._post(self.admin)
        self._post(self.admin, undo="1")

        self.assertEqual(self._acks(), [])

    def test_ack_and_undo_are_audit_logged(self):
        from app.models import AuditLog

        self._post(self.admin)
        self._post(self.admin, undo="1")

        actions = [
            row.action
            for row in self.session.exec(select(AuditLog)).all()
            if row.action.startswith("admin.exception.")
        ]
        self.assertIn("admin.exception.ack", actions)
        self.assertIn("admin.exception.unack", actions)

    def test_bad_week_is_rejected_without_writing(self):
        response = self._post(self.admin, week="not-a-date")

        self.assertEqual(response.status_code, 303)
        self.assertEqual(self._acks(), [])

    def test_empty_fingerprint_is_rejected_without_writing(self):
        self._post(self.admin, fingerprint="   ")

        self.assertEqual(self._acks(), [])

    def test_week_is_snapped_to_monday(self):
        self._post(self.admin, week="2026-04-22")

        self.assertEqual(self._acks()[0].week_start, WEEK)

    def test_employee_cannot_acknowledge(self):
        response = self._post(self.employee)

        self.assertEqual(self._acks(), [])
        self.assertNotEqual(getattr(response, "status_code", None), 303)


if __name__ == "__main__":
    unittest.main()
