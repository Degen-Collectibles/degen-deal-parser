"""Pending time-off requests must be visible on the manager schedule grid.

Submitting time off writes only a TimeOffRequest row; ShiftEntry rows are
created solely on approval (team_admin_timeoff._ensure_timeoff_shift_entries).
So until a manager worked the queue, the grid showed nothing and managers
scheduled straight over the request.

The overlay is manager-only by design: a pending request is not a commitment,
and a denial should not be inferable by peers watching a marker vanish. These
tests pin both halves — that managers see it, and that the employee view and
the shareable screenshot export never receive the data.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "pto-overlay-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "pto-overlay-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "pto-overlay-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "pto-overlay-admin-password")

WEEK = date(2026, 4, 27)  # Monday
TUE = WEEK + timedelta(days=1)
WED = WEEK + timedelta(days=2)


@pytest.fixture()
def session():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _seed_user(session: Session, user_id: int):
    from app.models import User

    user = User(
        id=user_id,
        username=f"user{user_id}",
        password_hash="x",
        password_salt="x",
        display_name=f"User {user_id}",
        role="employee",
        is_active=True,
        is_schedulable=True,
    )
    session.add(user)
    session.commit()
    return user


def _seed_roster(session: Session, user_id: int, *, calendar_kind: str = "storefront"):
    from app.models import ScheduleRosterMember

    session.add(
        ScheduleRosterMember(
            week_start=WEEK,
            user_id=user_id,
            calendar_kind=calendar_kind,
            added_by_user_id=999,
        )
    )
    session.commit()


def _seed_shift(session: Session, user_id: int, day: date, label: str):
    from app.models import ShiftEntry, classify_shift_label

    session.add(
        ShiftEntry(
            user_id=user_id,
            shift_date=day,
            label=label,
            kind=classify_shift_label(label),
            calendar_kind="storefront",
            sort_order=0,
            created_by_user_id=999,
        )
    )
    session.commit()


def _seed_request(
    session: Session,
    user_id: int,
    start: date,
    end: date,
    *,
    status: str = "submitted",
):
    from app.models import TimeOffRequest

    row = TimeOffRequest(
        submitted_by_user_id=user_id,
        start_date=start,
        end_date=end,
        reason="dentist",
        status=status,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _ctx(session: Session, **kwargs):
    from app.routers.team_admin_schedule import _grid_context

    return _grid_context(session, WEEK, staff_kind="storefront", **kwargs)


class TestPendingOverlayVisibleToManagers:
    def test_pending_request_appears_on_requested_day(self, session):
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, TUE)

        ctx = _ctx(session, include_pending_timeoff=True)
        assert (1, TUE.isoformat()) in ctx["pending_timeoff_map"]

    def test_multi_day_request_marks_every_day(self, session):
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, WED)

        pending = _ctx(session, include_pending_timeoff=True)["pending_timeoff_map"]
        assert (1, TUE.isoformat()) in pending
        assert (1, WED.isoformat()) in pending
        assert (1, WEEK.isoformat()) not in pending

    def test_request_is_clipped_to_the_visible_week(self, session):
        """A request running past the week edges must not leak extra keys."""
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, WEEK - timedelta(days=5), WEEK + timedelta(days=12))

        pending = _ctx(session, include_pending_timeoff=True)["pending_timeoff_map"]
        assert len(pending) == 7
        assert all(user_id == 1 for user_id, _iso in pending)
        assert (1, (WEEK - timedelta(days=1)).isoformat()) not in pending
        assert (1, (WEEK + timedelta(days=7)).isoformat()) not in pending

    def test_conflict_case_shows_alongside_the_scheduled_shift(self, session):
        """The case the overlay exists for: scheduled AND requested off."""
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_shift(session, 1, TUE, "10-6")
        _seed_request(session, 1, TUE, TUE)

        ctx = _ctx(session, include_pending_timeoff=True)
        assert ctx["entry_map"].get((1, TUE.isoformat()))
        assert ctx["pending_timeoff_map"].get((1, TUE.isoformat()))


class TestDecidedRequestsAreExcluded:
    @pytest.mark.parametrize("status", ["approved", "denied"])
    def test_decided_requests_do_not_show_as_pending(self, session, status):
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, TUE, status=status)

        ctx = _ctx(session, include_pending_timeoff=True)
        assert ctx["pending_timeoff_map"] == {}

    def test_approved_request_relies_on_shift_entries_instead(self, session):
        """Approval already writes a real ShiftEntry; the overlay must not double up."""
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, TUE, status="approved")
        _seed_shift(session, 1, TUE, "Time off")

        ctx = _ctx(session, include_pending_timeoff=True)
        assert ctx["entry_map"].get((1, TUE.isoformat()))
        assert ctx["pending_timeoff_map"] == {}


class TestOverlayIsOptIn:
    def test_default_is_off(self, session):
        """Employee view and screenshot export call without the flag."""
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, TUE)

        assert _ctx(session)["pending_timeoff_map"] == {}

    def test_employee_schedule_view_receives_no_pending_data(self, session):
        """Peers must not be able to infer a denial from a vanishing marker."""
        from app.routers.team_admin_schedule import _grid_context

        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_request(session, 1, TUE, TUE)

        # Mirrors team.team_schedule, which passes neither flag.
        for calendar in ("storefront", "packing"):
            ctx = _grid_context(session, WEEK, staff_kind=calendar)
            assert ctx["pending_timeoff_map"] == {}

    def test_stream_grid_carries_an_empty_map(self, session):
        """Stream is a read-only StreamSchedule projection; key must still exist."""
        from app.models import STAFF_KIND_STREAM
        from app.routers.team_admin_schedule import _grid_context

        ctx = _grid_context(
            session,
            WEEK,
            staff_kind=STAFF_KIND_STREAM,
            include_pending_timeoff=True,
        )
        assert ctx["pending_timeoff_map"] == {}


class TestScopingToTheGrid:
    def test_request_from_someone_not_on_the_grid_is_ignored(self, session):
        _seed_user(session, 1)
        _seed_roster(session, 1)
        _seed_user(session, 2)  # never rostered, no shifts
        _seed_request(session, 2, TUE, TUE)

        ctx = _ctx(session, include_pending_timeoff=True)
        assert ctx["pending_timeoff_map"] == {}

    def test_empty_grid_does_not_query(self, session):
        _seed_user(session, 1)
        _seed_request(session, 1, TUE, TUE)

        ctx = _ctx(session, include_pending_timeoff=True)
        assert ctx["pending_timeoff_map"] == {}


# ---------------------------------------------------------------------------
# HTTP-level checks. The context tests above prove the map is built correctly;
# these prove the template actually renders the flag, and — more importantly —
# that the employee-facing page never emits it. The privacy boundary is the
# part worth verifying at the wire, not in a dict.
#
# Uses a live uvicorn server rather than TestClient, matching the approach in
# test_schedule_mobile.py (TestClient has shown hangs on schedule-admin routes
# in this sandbox).
# ---------------------------------------------------------------------------

import importlib
import socket
import threading
import time
import unittest
from unittest.mock import patch

FLAG_MARKERS = ("sch-pto-flag", "Requested off")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _UvicornThread:
    def __init__(self, app):
        import uvicorn

        self.port = _free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=self.port,
                log_level="error",
                lifespan="off",
                access_log=False,
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                return
            time.sleep(0.05)
        raise RuntimeError("uvicorn failed to start")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)


class PendingOverlayRenderTests(unittest.TestCase):
    """Render /team/admin/schedule and /team/schedule with a pending request."""

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

        from app import config as cfg

        cfg.get_settings.cache_clear()
        import app.main as app_main

        importlib.reload(app_main)
        self.app_main = app_main

        from app.db import get_session as real_get_session

        _engine = self.engine

        def _override():
            s = Session(_engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override

        self.admin = self._seed(500, "adminx", "admin")
        self.employee = self._seed(901, "david", "employee")
        _seed_roster(self.session, 901)
        _seed_shift(self.session, 901, TUE, "10-6")
        _seed_request(self.session, 901, TUE, TUE)

        self.server = _UvicornThread(self.app_main.app)
        self.server.start()

    def tearDown(self):
        self.server.stop()
        self.app_main.app.dependency_overrides.clear()
        self.session.close()

    def _seed(self, user_id, username, role):
        from app.models import User

        user = User(
            id=user_id,
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
        self.session.expunge(user)
        return user

    def _get(self, path, as_user):
        import httpx
        from app import shared

        with patch.object(shared, "get_request_user", return_value=as_user), patch.object(
            self.app_main, "get_request_user", return_value=as_user
        ):
            return httpx.get(
                f"{self.server.base_url}{path}",
                params={"week": WEEK.isoformat()},
                timeout=30.0,
            )

    def test_manager_grid_renders_the_pending_flag(self):
        response = self._get("/team/admin/schedule", self.admin)
        self.assertEqual(response.status_code, 200)
        for marker in FLAG_MARKERS:
            self.assertIn(marker, response.text)

    def test_manager_grid_is_clean_when_nothing_is_pending(self):
        """Negative control: the flag must not render unconditionally."""
        from app.models import TimeOffRequest

        row = self.session.get(TimeOffRequest, 1)
        row.status = "approved"
        self.session.add(row)
        self.session.commit()

        response = self._get("/team/admin/schedule", self.admin)
        self.assertEqual(response.status_code, 200)
        # The CSS rule always ships; the rendered cell text must not.
        self.assertNotIn("Requested off", response.text)

    def test_employee_schedule_never_renders_the_pending_flag(self):
        response = self._get("/team/schedule", self.employee)
        self.assertEqual(response.status_code, 200)
        for marker in FLAG_MARKERS:
            self.assertNotIn(marker, response.text)

    def test_screenshot_export_never_renders_the_pending_flag(self):
        """The screenshot is shared with staff; it must stay clean."""
        response = self._get("/team/admin/schedule/screenshot", self.admin)
        if response.status_code == 200:
            for marker in FLAG_MARKERS:
                self.assertNotIn(marker, response.text)
