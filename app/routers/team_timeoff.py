"""
/team/requests — the employee Requests page (time off + supplies).

Also owns the time-off submit route and the owner-only cancel / edit routes
for both request types (redesign Phase 3). The supply *submit* route stays
in ``app/routers/team.py``; manager approval lives in the admin routers.

Cancel and edit only work while a request is still ``submitted``. The owner
and status checks run inside the same UPDATE (``WHERE id = … AND
submitted_by_user_id = … AND status = 'submitted'``), so an employee racing
a manager's decision gets a friendly "already decided" message rather than
overwriting it. Cancelling keeps the row (status ``cancelled``) for audit.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import update
from sqlmodel import Session, select

from ..team.clockify import clockify_today
from ..auth import has_permission
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, SupplyRequest, TimeOffRequest, User, utcnow
from ..rate_limit import rate_limited_or_429
from ..shared import templates
from ..team import requests_view
from ..team import schedule_view
from ..team.request_alerts import (
    EVENT_CANCELLED,
    EVENT_EDITED,
    send_supply_request_alert,
    send_timeoff_request_alert,
)
from ..team.team_notifications import notify_manager_admins
from .team import _nav_context, _require_employee

router = APIRouter()

REQUESTS_PATH = "/team/requests"
MAX_TIMEOFF_SPAN_DAYS = 90
# Overlap lookups never scan more than this many days (a 90-day request
# spans 91 calendar days).
MAX_OVERLAP_DAYS = MAX_TIMEOFF_SPAN_DAYS + 1
SUPPLY_URGENCIES = ("low", "normal", "high")

PAGE_KEYS = {
    requests_view.KIND_TIMEOFF: "page.timeoff",
    requests_view.KIND_SUPPLY: "page.supply_requests",
}
SUBMIT_KEYS = {
    requests_view.KIND_TIMEOFF: "action.timeoff.submit",
    requests_view.KIND_SUPPLY: "action.supply_request.submit",
}


# ---------------------------------------------------------------------------
# Redirect helpers
# ---------------------------------------------------------------------------

def _requests_url(**params: Any) -> str:
    query = {key: value for key, value in params.items() if value}
    return REQUESTS_PATH + (f"?{urlencode(query)}" if query else "")


def _requests_redirect(
    message: str = "",
    *,
    error: bool = False,
    **params: Any,
) -> RedirectResponse:
    if message:
        params["error" if error else "flash"] = message
    return RedirectResponse(_requests_url(**params), status_code=303)


def _timeoff_redirect(message: str, *, error: bool = False) -> RedirectResponse:
    """After a new time-off submit: back to Requests, reopening the form on error."""
    if error:
        return _requests_redirect(message, error=True, new=requests_view.KIND_TIMEOFF)
    return _requests_redirect(message)


def _parse_iso_date(value: str) -> Optional[date]:
    try:
        return date.fromisoformat((value or "").strip())
    except ValueError:
        return None


def _already_decided_message(status: str) -> str:
    if status == requests_view.CANCELLED_STATUS:
        return "That request was already cancelled."
    _tone, label = requests_view.status_pill(status)
    return (
        f"A manager already marked that request {label.lower()}, so it can't be "
        "changed. Send a new request instead."
    )


def _allowed_kinds(session: Session, user: User, keys: dict[str, str]) -> list[str]:
    cache: dict = {}
    return [
        kind
        for kind in requests_view.KINDS
        if has_permission(session, user, keys[kind], cache=cache)
    ]


# ---------------------------------------------------------------------------
# Scheduled shifts a time-off request would miss
# ---------------------------------------------------------------------------

def _timeoff_overlaps(
    session: Session,
    user: User,
    start: date,
    end: date,
    *,
    today: date,
) -> list[dict[str, Any]]:
    """The user's working shifts on [start, end] (from today on).

    Same sources and rules as /team/schedule and /team/hours (ShiftEntry per
    calendar + Stream Manager shifts, via ``_my_schedule_calendars`` and
    ``schedule_view.build_my_week``), so a warning here always matches what
    the Schedule tab shows.
    """
    from .team import _my_schedule_calendars

    first = max(start, today)
    if end < first:
        return []
    last = min(end, first + timedelta(days=MAX_OVERLAP_DAYS - 1))
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]
    my_week = schedule_view.build_my_week(
        week_days=days,
        today=today,
        me_id=user.id,
        calendars=_my_schedule_calendars(session, user, days),
    )
    return requests_view.timeoff_overlaps(my_week["days"])


# ---------------------------------------------------------------------------
# /team/requests
# ---------------------------------------------------------------------------

def _decider_names(session: Session, rows: list[Any]) -> dict[int, str]:
    ids = {
        row.approved_by_user_id
        for row in rows
        if getattr(row, "approved_by_user_id", None) is not None
    }
    if not ids:
        return {}
    return {
        u.id: (u.display_name or u.username or "").split(" ")[0]
        for u in session.exec(select(User).where(User.id.in_(ids))).all()
        if u.id is not None
    }


@router.get("/team/requests", response_class=HTMLResponse)
def team_requests(
    request: Request,
    tab: Optional[str] = Query(default=None),
    new: Optional[str] = Query(default=None),
    edit: Optional[str] = Query(default=None),
    id: Optional[str] = Query(default=None),
    date: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session)
    if denial:
        return denial
    kinds = _allowed_kinds(session, user, PAGE_KEYS)
    if not kinds:
        # Same 403 the old /team/timeoff page gave someone without access.
        denial, _ = _require_employee(request, session, resource_key="page.timeoff")
        return denial
    can_submit = set(_allowed_kinds(session, user, SUBMIT_KEYS)) & set(kinds)

    today = clockify_today()
    timeoff_rows: list[TimeOffRequest] = []
    supply_rows: list[SupplyRequest] = []
    if requests_view.KIND_TIMEOFF in kinds:
        timeoff_rows = list(
            session.exec(
                select(TimeOffRequest)
                .where(TimeOffRequest.submitted_by_user_id == user.id)
                .order_by(TimeOffRequest.created_at.desc())
            ).all()
        )
    if requests_view.KIND_SUPPLY in kinds:
        supply_rows = list(
            session.exec(
                select(SupplyRequest)
                .where(SupplyRequest.submitted_by_user_id == user.id)
                .order_by(SupplyRequest.created_at.desc())
            ).all()
        )
    deciders = _decider_names(session, [*timeoff_rows, *supply_rows])

    overlaps_by_id: dict[int, list[dict[str, Any]]] = {}
    cards: list[dict[str, Any]] = []
    for row in timeoff_rows:
        overlap_count = None
        if row.status == requests_view.EDITABLE_STATUS:
            overlaps_by_id[row.id] = _timeoff_overlaps(
                session, user, row.start_date, row.end_date, today=today
            )
            overlap_count = len(overlaps_by_id[row.id])
        card = requests_view.build_timeoff_card(
            row,
            today=today,
            overlap_count=overlap_count,
            decided_by=deciders.get(row.approved_by_user_id or -1, ""),
        )
        card["overlaps"] = overlaps_by_id.get(row.id, [])
        card["overlap_summary"] = requests_view.overlap_summary(card["overlaps"])
        cards.append(card)
    for row in supply_rows:
        cards.append(
            requests_view.build_supply_card(
                row,
                today=today,
                decided_by=deciders.get(row.approved_by_user_id or -1, ""),
            )
        )
    for card in cards:
        card["can_edit"] = card["can_edit"] and card["kind"] in can_submit
        card["sheet_id"] = f"pt-sheet-{card['kind']}-{card['id']}"

    active_tab = requests_view.normalize_tab(tab, kinds)
    shown = [c for c in cards if active_tab == requests_view.TAB_ALL or c["kind"] == active_tab]
    lists = requests_view.split_open_past(shown)
    editable = [c for c in cards if c["can_edit"]]

    # ?date=YYYY-MM-DD (Schedule's "Can't make a shift?" link) prefills the
    # new time-off form and gets the overlap warning without JavaScript.
    prefill = requests_view.parse_iso_date(date)
    if prefill is not None and prefill < today:
        prefill = None
    prefill_overlaps = (
        _timeoff_overlaps(session, user, prefill, prefill, today=today)
        if prefill is not None and requests_view.KIND_TIMEOFF in can_submit
        else []
    )

    # Which sheet the server renders open (the no-JS path).
    open_sheet = ""
    new_kind = requests_view.normalize_kind(new, can_submit)
    if new_kind:
        open_sheet = f"pt-sheet-new-{new_kind}"
    else:
        edit_kind = requests_view.normalize_kind(edit, can_submit)
        if edit_kind and (id or "").strip().isdigit():
            wanted = f"pt-sheet-{edit_kind}-{int(id)}"
            if any(c["sheet_id"] == wanted for c in editable):
                open_sheet = wanted

    tab_param = active_tab if active_tab != requests_view.TAB_ALL else ""
    active_nav = {
        requests_view.KIND_TIMEOFF: "time-off",
        requests_view.KIND_SUPPLY: "supply",
    }.get(active_tab, "requests")
    return templates.TemplateResponse(
        request,
        "team/requests.html",
        {
            "request": request,
            "title": "Requests",
            "active": active_nav,
            "current_user": user,
            "kinds": kinds,
            "can_submit": sorted(can_submit),
            "tab": active_tab,
            "tab_param": tab_param,
            "close_href": _requests_url(tab=tab_param),
            "open_cards": lists["open"],
            "past_cards": lists["past"],
            "editable_cards": editable,
            "open_sheet": open_sheet,
            "prefill_date": prefill.isoformat() if prefill else "",
            "prefill_overlaps": prefill_overlaps,
            "prefill_overlap_summary": requests_view.overlap_summary(prefill_overlaps),
            "today": today.isoformat(),
            "max_span_days": MAX_TIMEOFF_SPAN_DAYS,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/requests/overlap")
def team_requests_overlap(
    request: Request,
    start: str = Query(default=""),
    end: str = Query(default=""),
    session: Session = Depends(get_session),
):
    """JSON for the time-off sheet: scheduled shifts on the chosen dates."""
    denial, user = _require_employee(request, session, resource_key="page.timeoff")
    if denial:
        return denial
    parsed_start = _parse_iso_date(start)
    parsed_end = _parse_iso_date(end) or parsed_start
    if parsed_start is None or parsed_end is None or parsed_end < parsed_start:
        return JSONResponse({"ok": False, "summary": "", "shifts": []})
    overlaps = _timeoff_overlaps(
        session, user, parsed_start, parsed_end, today=clockify_today()
    )
    return JSONResponse(
        {
            "ok": True,
            "summary": requests_view.overlap_summary(overlaps),
            "shifts": overlaps,
        }
    )


@router.get("/team/timeoff")
def team_timeoff(
    request: Request,
    date: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    """Old page URL: opens the time-off form on /team/requests, keeping ?date=."""
    return _requests_redirect(
        new=requests_view.KIND_TIMEOFF,
        date=(date or "").strip(),
        flash=flash or "",
        error=error or "",
    )


# ---------------------------------------------------------------------------
# Time off: submit / cancel / edit
# ---------------------------------------------------------------------------

def _validate_timeoff(
    session: Session,
    user: User,
    start_date: str,
    end_date: str,
    *,
    exclude_id: Optional[int] = None,
) -> tuple[Optional[date], Optional[date], str]:
    parsed_start = _parse_iso_date(start_date)
    parsed_end = _parse_iso_date(end_date)
    if parsed_start is None or parsed_end is None:
        return None, None, "Start and end dates must be valid."
    if parsed_start > parsed_end:
        return None, None, "End date must be on or after start date."
    if parsed_start < clockify_today():
        return None, None, "Start date cannot be in the past."
    if (parsed_end - parsed_start).days > MAX_TIMEOFF_SPAN_DAYS:
        return None, None, "Time-off requests cannot span more than 90 days."

    stmt = (
        select(TimeOffRequest)
        .where(TimeOffRequest.submitted_by_user_id == user.id)
        .where(TimeOffRequest.status.in_(("submitted", "approved")))
        .where(TimeOffRequest.start_date <= parsed_end)
        .where(TimeOffRequest.end_date >= parsed_start)
    )
    if exclude_id is not None:
        stmt = stmt.where(TimeOffRequest.id != exclude_id)
    if session.exec(stmt).first() is not None:
        return None, None, "You already have a pending request for those dates."
    return parsed_start, parsed_end, ""


@router.post("/team/timeoff", dependencies=[Depends(require_csrf)])
async def team_timeoff_post(
    request: Request,
    start_date: str = Form(default=""),
    end_date: str = Form(default=""),
    reason: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.timeoff.submit"
    )
    if denial:
        return denial

    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:timeoff:{user.id}",
        max_requests=10,
        window_seconds=3600.0,
    ):
        return limited

    parsed_start, parsed_end, problem = _validate_timeoff(
        session, user, start_date, end_date
    )
    if problem:
        return _timeoff_redirect(problem, error=True)

    row = TimeOffRequest(
        submitted_by_user_id=user.id,
        start_date=parsed_start,
        end_date=parsed_end,
        reason=(reason or "").strip()[:2000],
        status="submitted",
    )
    session.add(row)
    session.flush()
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="timeoff.submitted",
            resource_key="action.timeoff.submit",
            details_json=json.dumps(
                {
                    "time_off_request_id": row.id,
                    "start_date": parsed_start.isoformat(),
                    "end_date": parsed_end.isoformat(),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.timeoff.view",
        kind="timeoff_submitted",
        title="New time-off request",
        body=(
            f"{user.display_name or user.username} requested "
            f"{parsed_start.isoformat()} to {parsed_end.isoformat()}."
        ),
        link_path="/team/admin/timeoff",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    send_timeoff_request_alert(
        request_id=row.id,
        employee_name=user.display_name or user.username,
        employee_username=user.username,
        start_date=parsed_start.isoformat(),
        end_date=parsed_end.isoformat(),
        reason=row.reason,
    )
    return _timeoff_redirect("Time-off request submitted.")


def _owned_row(session: Session, model: Any, request_id: int, user: User) -> Any:
    row = session.get(model, request_id)
    if row is None or row.submitted_by_user_id != user.id:
        return None
    return row


def _update_if_pending(
    session: Session,
    model: Any,
    request_id: int,
    user: User,
    values: dict[str, Any],
) -> bool:
    """Apply ``values`` only if the row is still the user's and still pending.

    Owner and status are part of the UPDATE's WHERE clause, so this is one
    atomic check-and-write on SQLite and Postgres alike: if a manager decided
    the request in the meantime, zero rows match and nothing is written.
    """
    result = session.exec(
        update(model)
        .where(
            model.id == request_id,
            model.submitted_by_user_id == user.id,
            model.status == requests_view.EDITABLE_STATUS,
        )
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if int(result.rowcount or 0) != 1:
        session.rollback()
        return False
    return True


def _current_status(session: Session, model: Any, request_id: int) -> str:
    """Status as it is in the DB now (after a lost race with a manager)."""
    session.expire_all()
    fresh = session.get(model, request_id)
    return fresh.status if fresh is not None else ""


def _change_rate_limited(request: Request, user: User):
    # Edits and cancels alert managers, so cap them like new submissions.
    return rate_limited_or_429(
        request,
        key_prefix=f"team:request-change:{user.id}",
        max_requests=20,
        window_seconds=3600.0,
    )


def _audit(
    session: Session,
    request: Request,
    user: User,
    *,
    action: str,
    resource_key: str,
    details: dict[str, Any],
) -> None:
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action=action,
            resource_key=resource_key,
            details_json=json.dumps(details),
            ip_address=(request.client.host if request.client else None),
        )
    )


@router.post("/team/timeoff/{request_id}/cancel", dependencies=[Depends(require_csrf)])
async def team_timeoff_cancel(
    request: Request,
    request_id: int,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.timeoff.submit"
    )
    if denial:
        return denial
    row = _owned_row(session, TimeOffRequest, request_id, user)
    if row is None:
        return HTMLResponse("Time-off request not found", status_code=404)
    if limited := _change_rate_limited(request, user):
        return limited
    if row.status != requests_view.EDITABLE_STATUS:
        return _requests_redirect(_already_decided_message(row.status), error=True)

    now = utcnow()
    if not _update_if_pending(
        session,
        TimeOffRequest,
        request_id,
        user,
        {"status": requests_view.CANCELLED_STATUS, "status_changed_at": now, "updated_at": now},
    ):
        return _requests_redirect(
            _already_decided_message(_current_status(session, TimeOffRequest, request_id)),
            error=True,
        )
    session.refresh(row)
    _audit(
        session,
        request,
        user,
        action="timeoff.cancelled",
        resource_key="action.timeoff.submit",
        details={
            "time_off_request_id": row.id,
            "start_date": row.start_date.isoformat(),
            "end_date": row.end_date.isoformat(),
            "previous_status": requests_view.EDITABLE_STATUS,
        },
    )
    name = user.display_name or user.username
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.timeoff.view",
        kind="timeoff_cancelled",
        title="Time-off request cancelled",
        body=(
            f"{name} cancelled their request for "
            f"{row.start_date.isoformat()} to {row.end_date.isoformat()}."
        ),
        link_path="/team/admin/timeoff",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    send_timeoff_request_alert(
        request_id=row.id,
        employee_name=name,
        employee_username=user.username,
        start_date=row.start_date.isoformat(),
        end_date=row.end_date.isoformat(),
        reason=row.reason,
        event=EVENT_CANCELLED,
    )
    return _requests_redirect("Time-off request cancelled.")


@router.post("/team/timeoff/{request_id}/edit", dependencies=[Depends(require_csrf)])
async def team_timeoff_edit(
    request: Request,
    request_id: int,
    start_date: str = Form(default=""),
    end_date: str = Form(default=""),
    reason: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.timeoff.submit"
    )
    if denial:
        return denial
    row = _owned_row(session, TimeOffRequest, request_id, user)
    if row is None:
        return HTMLResponse("Time-off request not found", status_code=404)
    if limited := _change_rate_limited(request, user):
        return limited
    if row.status != requests_view.EDITABLE_STATUS:
        return _requests_redirect(_already_decided_message(row.status), error=True)

    parsed_start, parsed_end, problem = _validate_timeoff(
        session, user, start_date, end_date, exclude_id=row.id
    )
    if problem:
        return _requests_redirect(
            problem, error=True, edit=requests_view.KIND_TIMEOFF, id=str(row.id)
        )
    clean_reason = (reason or "").strip()[:2000]
    before = {
        "start_date": row.start_date.isoformat(),
        "end_date": row.end_date.isoformat(),
        "reason": row.reason,
    }
    after = {
        "start_date": parsed_start.isoformat(),
        "end_date": parsed_end.isoformat(),
        "reason": clean_reason,
    }
    if before == after:
        return _requests_redirect("No changes to save.")

    if not _update_if_pending(
        session,
        TimeOffRequest,
        request_id,
        user,
        {
            "start_date": parsed_start,
            "end_date": parsed_end,
            "reason": clean_reason,
            "updated_at": utcnow(),
        },
    ):
        return _requests_redirect(
            _already_decided_message(_current_status(session, TimeOffRequest, request_id)),
            error=True,
        )
    session.refresh(row)
    _audit(
        session,
        request,
        user,
        action="timeoff.edited",
        resource_key="action.timeoff.submit",
        details={"time_off_request_id": row.id, "before": before, "after": after},
    )
    name = user.display_name or user.username
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.timeoff.view",
        kind="timeoff_edited",
        title="Time-off request edited",
        body=(
            f"{name} edited their pending request: now "
            f"{after['start_date']} to {after['end_date']}."
        ),
        link_path="/team/admin/timeoff",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    send_timeoff_request_alert(
        request_id=row.id,
        employee_name=name,
        employee_username=user.username,
        start_date=after["start_date"],
        end_date=after["end_date"],
        reason=clean_reason,
        event=EVENT_EDITED,
    )
    return _requests_redirect("Time-off request updated.")


# ---------------------------------------------------------------------------
# Supplies: cancel / edit (submit is POST /team/supply in team.py)
# ---------------------------------------------------------------------------

@router.post("/team/supply/{request_id}/cancel", dependencies=[Depends(require_csrf)])
async def team_supply_cancel(
    request: Request,
    request_id: int,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.supply_request.submit"
    )
    if denial:
        return denial
    row = _owned_row(session, SupplyRequest, request_id, user)
    if row is None:
        return HTMLResponse("Supply request not found", status_code=404)
    if limited := _change_rate_limited(request, user):
        return limited
    if row.status != requests_view.EDITABLE_STATUS:
        return _requests_redirect(_already_decided_message(row.status), error=True)

    now = utcnow()
    if not _update_if_pending(
        session,
        SupplyRequest,
        request_id,
        user,
        {"status": requests_view.CANCELLED_STATUS, "status_changed_at": now, "updated_at": now},
    ):
        return _requests_redirect(
            _already_decided_message(_current_status(session, SupplyRequest, request_id)),
            error=True,
        )
    session.refresh(row)
    _audit(
        session,
        request,
        user,
        action="supply.cancelled",
        resource_key="action.supply_request.submit",
        details={
            "supply_request_id": row.id,
            "title": row.title,
            "previous_status": requests_view.EDITABLE_STATUS,
        },
    )
    name = user.display_name or user.username
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.supply.view",
        kind="supply_cancelled",
        title="Supply request cancelled",
        body=f"{name} cancelled their request for {row.title}.",
        link_path="/team/admin/supply",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    send_supply_request_alert(
        request_id=row.id,
        employee_name=name,
        employee_username=user.username,
        title=row.title,
        description=row.description,
        urgency=row.urgency,
        event=EVENT_CANCELLED,
    )
    return _requests_redirect("Supply request cancelled.")


@router.post("/team/supply/{request_id}/edit", dependencies=[Depends(require_csrf)])
async def team_supply_edit(
    request: Request,
    request_id: int,
    title: str = Form(default=""),
    description: str = Form(default=""),
    urgency: str = Form(default="normal"),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.supply_request.submit"
    )
    if denial:
        return denial
    row = _owned_row(session, SupplyRequest, request_id, user)
    if row is None:
        return HTMLResponse("Supply request not found", status_code=404)
    if limited := _change_rate_limited(request, user):
        return limited
    if row.status != requests_view.EDITABLE_STATUS:
        return _requests_redirect(_already_decided_message(row.status), error=True)

    clean_title = (title or "").strip()[:200]
    if not clean_title:
        return _requests_redirect(
            "Title is required.", error=True, edit=requests_view.KIND_SUPPLY, id=str(row.id)
        )
    clean_urgency = urgency if urgency in SUPPLY_URGENCIES else "normal"
    before = {"title": row.title, "description": row.description, "urgency": row.urgency}
    after = {
        "title": clean_title,
        "description": (description or "")[:4000],
        "urgency": clean_urgency,
    }
    if before == after:
        return _requests_redirect("No changes to save.")

    if not _update_if_pending(
        session,
        SupplyRequest,
        request_id,
        user,
        {**after, "updated_at": utcnow()},
    ):
        return _requests_redirect(
            _already_decided_message(_current_status(session, SupplyRequest, request_id)),
            error=True,
        )
    session.refresh(row)
    _audit(
        session,
        request,
        user,
        action="supply.edited",
        resource_key="action.supply_request.submit",
        details={"supply_request_id": row.id, "before": before, "after": after},
    )
    name = user.display_name or user.username
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.supply.view",
        kind="supply_edited",
        title="Supply request edited",
        body=f"{name} edited their pending request: {row.title} ({row.urgency} urgency).",
        link_path="/team/admin/supply",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    send_supply_request_alert(
        request_id=row.id,
        employee_name=name,
        employee_username=user.username,
        title=row.title,
        description=row.description,
        urgency=row.urgency,
        event=EVENT_EDITED,
    )
    return _requests_redirect("Supply request updated.")
