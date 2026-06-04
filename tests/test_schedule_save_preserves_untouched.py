"""Regression tests for schedule-grid save data preservation.

These call the save handler directly with a fake Request object instead of
TestClient. The bug is in form payload semantics, and direct handler calls avoid
the TestClient hangs this sandbox has shown on schedule-admin routes.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "schedule-save-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "schedule-save-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "schedule-save-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "schedule-save-admin-password")


WEEK = date(2026, 4, 27)


class _FakeRequest:
    def __init__(self, form: dict[str, str], current_user):
        self._form = form
        self.state = SimpleNamespace(current_user=current_user)
        self.client = SimpleNamespace(host="testclient")

    async def form(self):
        return self._form


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


def _seed_user(session: Session, user_id: int, *, role: str = "employee"):
    from app.models import User

    user = User(
        id=user_id,
        username=f"user{user_id}",
        password_hash="x",
        password_salt="x",
        display_name=f"User {user_id}",
        role=role,
        is_active=True,
        is_schedulable=True,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _seed_shift(
    session: Session,
    user_id: int,
    shift_date: date,
    label: str,
    *,
    calendar_kind: str = "storefront",
    sort_order: int = 0,
):
    from app.models import ShiftEntry, User, classify_shift_label

    if session.get(User, user_id) is None:
        _seed_user(session, user_id)
    row = ShiftEntry(
        user_id=user_id,
        shift_date=shift_date,
        label=label,
        kind=classify_shift_label(label),
        calendar_kind=calendar_kind,
        sort_order=sort_order,
        created_by_user_id=999,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _seed_roster(
    session: Session,
    user_id: int,
    *,
    calendar_kind: str = "storefront",
    admin_id: int = 999,
):
    from app.models import ScheduleRosterMember

    row = ScheduleRosterMember(
        week_start=WEEK,
        user_id=user_id,
        calendar_kind=calendar_kind,
        added_by_user_id=admin_id,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _entries(
    session: Session,
    user_id: int,
    shift_date: date,
    *,
    calendar_kind: str = "storefront",
):
    from app.models import ShiftEntry

    return list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user_id)
            .where(ShiftEntry.shift_date == shift_date)
            .where(ShiftEntry.calendar_kind == calendar_kind)
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )


def _save(
    session: Session,
    admin,
    fields: dict[str, str],
    *,
    staff_kind: str = "storefront",
):
    from app.models import STAFF_KIND_STOREFRONT
    from app.routers import team_admin_schedule as schedule

    form = {
        "week": WEEK.isoformat(),
        "staff_kind": staff_kind or STAFF_KIND_STOREFRONT,
        **fields,
    }
    request = _FakeRequest(form, admin)
    with patch.object(schedule, "_permission_gate", return_value=(None, admin)):
        return asyncio.run(schedule.admin_schedule_save(request, session))


def test_absent_cell_key_preserves_existing_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    _seed_shift(session, 5, shift_date, "10-6")

    response = _save(session, admin, {})

    assert response.status_code == 303
    assert _build_cell_key(5, shift_date) not in {}
    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["10-6"]


def test_empty_cell_key_without_clear_marker_preserves_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(session, admin, {key: "", "cleared_cells": "[]"})

    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["10-6"]


def test_empty_cell_key_with_clear_marker_deletes_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(
        session,
        admin,
        {key: "", "cleared_cells": json.dumps([f"5__{shift_date.isoformat()}"])},
    )

    assert _entries(session, 5, shift_date) == []


def test_nonempty_cell_key_updates_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(session, admin, {key: "12-8", "cleared_cells": "[]"})

    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["12-8"]


def test_full_week_save_with_one_edit_preserves_four_untouched(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    untouched = []
    for offset, user_id in enumerate(range(1, 6)):
        shift_date = WEEK + timedelta(days=offset)
        label = f"original-{user_id}"
        _seed_shift(session, user_id, shift_date, label)
        if user_id != 1:
            untouched.append((user_id, shift_date, label))

    edited_key = _build_cell_key(1, WEEK)
    _save(session, admin, {edited_key: "edited-one-cell", "cleared_cells": "[]"})

    assert [row.label for row in _entries(session, 1, WEEK)] == ["edited-one-cell"]
    for user_id, shift_date, label in untouched:
        rows = _entries(session, user_id, shift_date)
        assert [row.label for row in rows] == [label], (
            "untouched cells must not be deleted when one schedule cell is edited"
        )


def test_same_employee_can_save_storefront_and_packing_same_day(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    employee = _seed_user(session, 5)
    _seed_roster(session, employee.id, calendar_kind="storefront", admin_id=admin.id)
    _seed_roster(session, employee.id, calendar_kind="packing", admin_id=admin.id)

    key = _build_cell_key(employee.id, WEEK)
    _save(session, admin, {key: "10 AM - 2 PM", "cleared_cells": "[]"})
    _save(
        session,
        admin,
        {key: "3 PM - 7 PM", "cleared_cells": "[]"},
        staff_kind="packing",
    )

    storefront_rows = _entries(
        session, employee.id, WEEK, calendar_kind="storefront"
    )
    packing_rows = _entries(session, employee.id, WEEK, calendar_kind="packing")
    assert [row.label for row in storefront_rows] == ["10 AM - 2 PM"]
    assert [row.label for row in packing_rows] == ["3 PM - 7 PM"]


def test_copy_previous_week_for_one_person_targets_one_calendar(session: Session):
    admin = _seed_user(session, 999, role="admin")
    employee = _seed_user(session, 5)
    _seed_roster(session, employee.id, calendar_kind="packing", admin_id=admin.id)
    _seed_shift(
        session,
        employee.id,
        WEEK - timedelta(days=7),
        "9 AM - 1 PM",
        calendar_kind="packing",
        sort_order=0,
    )
    _seed_shift(
        session,
        employee.id,
        WEEK - timedelta(days=7),
        "2 PM - 6 PM",
        calendar_kind="packing",
        sort_order=1,
    )
    _seed_shift(
        session,
        employee.id,
        WEEK - timedelta(days=7),
        "storefront stays separate",
        calendar_kind="storefront",
    )

    from app.routers import team_admin_schedule as schedule

    request = _FakeRequest(
        {
            "week": WEEK.isoformat(),
            "staff_kind": "packing",
            "user_id": str(employee.id),
        },
        admin,
    )
    with patch.object(schedule, "_permission_gate", return_value=(None, admin)):
        response = asyncio.run(
            schedule.admin_schedule_generate_person_from_previous(
                request, session
            )
        )

    assert response.status_code == 303
    packing_rows = _entries(session, employee.id, WEEK, calendar_kind="packing")
    storefront_rows = _entries(
        session, employee.id, WEEK, calendar_kind="storefront"
    )
    assert [row.label for row in packing_rows] == [
        "9 AM - 1 PM",
        "2 PM - 6 PM",
    ]
    assert [row.sort_order for row in packing_rows] == [0, 1]
    assert storefront_rows == []
