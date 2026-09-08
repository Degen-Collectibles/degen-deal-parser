"""The labor-cache refresh must not delete hours it never asked about.

refresh_clockify_labor_cache reconciles the local ClockifyTimeEntry cache
against Clockify: anything cached for the window that Clockify no longer
returns is marked deleted. The Clockify endpoint filters on the entry START,
so a shift that began before the window and ran into it was never in the
response -- and got tombstoned. Its hours then vanished from labor stats,
timecards, and payroll every time an admin pressed Refresh on the window.

These tests drive the REAL ClockifyClient with a stubbed transport that
emulates the start-filter semantics, so both the lookback in the client and
the tombstone floor in the router are genuinely exercised.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "tombstone-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "tombstone-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "tombstone-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "tombstone-admin-password")

# Local Mon-Sun week; America/Los_Angeles puts the day boundary at 07:00Z.
START_DAY = date(2026, 4, 20)
END_DAY = date(2026, 4, 26)
WINDOW_START_UTC = datetime(2026, 4, 20, 7, 0, tzinfo=timezone.utc)
API_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(API_FORMAT)


def _parse(value: str) -> datetime:
    return datetime.strptime(value, API_FORMAT).replace(tzinfo=timezone.utc)


def _entry(entry_id: str, start: datetime, end: Optional[datetime]):
    return {
        "id": entry_id,
        "description": "Shift",
        "timeInterval": {"start": _iso(start), "end": _iso(end) if end else None},
    }


class _StubTransport:
    """Emulates the endpoint: returns only entries starting at/after `start`."""

    def __init__(self, entries):
        self.entries = entries
        self.requested_starts = []

    def __call__(self, method, path, *, params=None, json_body=None):
        params = params or {}
        raw_start = params.get("start")
        if raw_start is None:
            return []
        start = _parse(raw_start)
        page = params.get("page", 1)
        if page == 1:
            self.requested_starts.append(start)
        else:
            return []
        return [
            row
            for row in self.entries
            if _parse(row["timeInterval"]["start"]) >= start
        ]


class ClockifyRefreshTombstoneTests(unittest.TestCase):
    def setUp(self):
        from app.models import SQLModel

        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        self._seed_employee()

    def tearDown(self):
        self.session.close()

    def _settings(self):
        return SimpleNamespace(
            employee_portal_enabled=True,
            clockify_api_key="key",
            clockify_workspace_id="workspace",
            clockify_timezone="America/Los_Angeles",
            clockify_base_url="https://api.clockify.me/api/v1",
            clockify_timeout_seconds=5.0,
        )

    def _seed_employee(self):
        from app.models import EmployeeProfile, User

        self.session.add(
            User(
                id=20,
                username="alice",
                password_hash="x",
                password_salt="x",
                display_name="Alice",
                role="employee",
                is_active=True,
            )
        )
        self.session.add(EmployeeProfile(user_id=20, clockify_user_id="clock-1"))
        self.session.commit()

    def _cache(self, entry_id, start, end, is_running=False):
        from app.models import ClockifyTimeEntry

        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id=entry_id,
                clockify_user_id="clock-1",
                user_id=20,
                start_at=start,
                end_at=end,
                duration_seconds=int((end - start).total_seconds()) if end else 0,
                is_running=is_running,
                is_deleted=False,
            )
        )
        self.session.commit()

    def _refresh(self, api_entries):
        from app.routers import team_admin_clockify as mod
        from app.team.clockify import ClockifyClient

        transport = _StubTransport(api_entries)
        client = ClockifyClient(api_key="key", workspace_id="workspace")
        with patch.object(ClockifyClient, "_request", side_effect=transport):
            result = mod.refresh_clockify_labor_cache(
                self.session,
                client,
                start_day=START_DAY,
                end_day=END_DAY,
                settings=self._settings(),
                include_inactive=True,
            )
        return result, transport

    def _row(self, entry_id):
        from app.models import ClockifyTimeEntry

        return self.session.exec(
            select(ClockifyTimeEntry).where(
                ClockifyTimeEntry.clockify_entry_id == entry_id
            )
        ).first()

    # -- the bug ------------------------------------------------------------

    def test_overnight_shift_starting_before_window_is_not_deleted(self):
        """Sun 22:00 to Mon 02:00 local: starts before the window, runs into it."""
        start = WINDOW_START_UTC - timedelta(hours=3)
        end = WINDOW_START_UTC + timedelta(hours=2)
        self._cache("overnight-1", start, end)

        self._refresh([_entry("overnight-1", start, end)])

        self.assertFalse(
            self._row("overnight-1").is_deleted,
            "an overnight shift crossing into the window must survive a refresh",
        )

    def test_lookback_is_applied_to_the_api_query(self):
        from app.team.clockify import CLOCKIFY_ENTRY_FETCH_LOOKBACK_HOURS

        _result, transport = self._refresh([])
        requested = transport.requested_starts[0]

        # Asserted against a literal, not the constant: reading the constant
        # back would make this pass even if the lookback were zeroed out.
        self.assertLessEqual(
            requested,
            WINDOW_START_UTC - timedelta(hours=24),
            "the fetch must reach back far enough to see an overnight shift",
        )
        self.assertGreaterEqual(CLOCKIFY_ENTRY_FETCH_LOOKBACK_HOURS, 24)
        self.assertEqual(
            requested,
            WINDOW_START_UTC - timedelta(hours=CLOCKIFY_ENTRY_FETCH_LOOKBACK_HOURS),
        )

    def test_entry_older_than_the_lookback_is_left_alone(self):
        """Never queried, so absence proves nothing."""
        start = WINDOW_START_UTC - timedelta(days=5)
        self._cache("ancient-1", start, WINDOW_START_UTC + timedelta(hours=1))

        self._refresh([])

        self.assertFalse(self._row("ancient-1").is_deleted)

    # -- real deletions must still be caught --------------------------------

    def test_entry_deleted_upstream_is_still_tombstoned(self):
        kept_start = WINDOW_START_UTC + timedelta(hours=10)
        kept_end = kept_start + timedelta(hours=4)
        gone_start = WINDOW_START_UTC + timedelta(days=1, hours=10)
        self._cache("kept-1", kept_start, kept_end)
        self._cache("gone-1", gone_start, gone_start + timedelta(hours=4))

        self._refresh([_entry("kept-1", kept_start, kept_end)])

        self.assertFalse(self._row("kept-1").is_deleted)
        self.assertTrue(
            self._row("gone-1").is_deleted,
            "an entry inside the queried range that Clockify dropped is a real delete",
        )

    # -- empty-response guard ------------------------------------------------

    def test_empty_response_does_not_wipe_cached_hours(self):
        start = WINDOW_START_UTC + timedelta(hours=10)
        self._cache("payroll-1", start, start + timedelta(hours=8))

        result, _transport = self._refresh([])

        self.assertFalse(
            self._row("payroll-1").is_deleted,
            "an empty API response must not silently delete a week of payroll",
        )
        self.assertEqual(result["skipped_tombstone_count"], 1)
        self.assertIn("Alice", result["skipped_tombstones"][0])

    def test_empty_response_with_empty_cache_is_not_a_warning(self):
        result, _transport = self._refresh([])

        self.assertEqual(result["skipped_tombstone_count"], 0)
        self.assertEqual(result["errors"], [])

    def test_guard_does_not_mask_partial_responses(self):
        """Some entries returned means the sweep still runs normally."""
        kept_start = WINDOW_START_UTC + timedelta(hours=10)
        kept_end = kept_start + timedelta(hours=4)
        gone_start = WINDOW_START_UTC + timedelta(days=2, hours=10)
        self._cache("kept-2", kept_start, kept_end)
        self._cache("gone-2", gone_start, gone_start + timedelta(hours=4))

        result, _transport = self._refresh([_entry("kept-2", kept_start, kept_end)])

        self.assertEqual(result["skipped_tombstone_count"], 0)
        self.assertTrue(self._row("gone-2").is_deleted)

    # -- running timers ------------------------------------------------------

    def test_running_timer_started_before_the_window_survives(self):
        start = WINDOW_START_UTC - timedelta(hours=2)
        self._cache("running-1", start, None, is_running=True)

        self._refresh([_entry("running-1", start, None)])

        self.assertFalse(self._row("running-1").is_deleted)


if __name__ == "__main__":
    unittest.main()
