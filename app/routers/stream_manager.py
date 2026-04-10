"""
Stream Manager routes — streamer profiles, schedule, and stream accounts.

Extracted from main.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..shared import (
    STREAMER_COLORS,
    StreamAccount,
    Streamer,
    StreamSchedule,
    _now_pacific,
    get_current_streamer,
    get_session,
    require_role_response,
    templates,
    utcnow,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Private helpers (only used by stream-manager routes)
# ---------------------------------------------------------------------------

def _format_time_12h(t24: str) -> str:
    """Convert 'HH:MM' 24-hour to '12:00 AM/PM'."""
    try:
        h, m = int(t24.split(":")[0]), t24.split(":")[1]
        suffix = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m} {suffix}"
    except Exception:
        return t24


def _ensure_default_stream_account(session: Session) -> None:
    """Create the default '@degencollectibles' stream account if none exist.

    Also backfills stream_account_id and is_overnight on legacy schedules.
    """
    existing = session.exec(select(StreamAccount).where(StreamAccount.is_active == True)).first()
    if not existing:
        acct = StreamAccount(
            name="Main Stream",
            platform="TikTok",
            handle="@degencollectibles",
            is_default=True,
            sort_order=0,
        )
        session.add(acct)
        session.commit()
        session.refresh(acct)

    default_acct = session.exec(
        select(StreamAccount).where(StreamAccount.is_default == True)
    ).first()

    if default_acct:
        needs_fix = session.exec(
            select(StreamSchedule).where(
                (StreamSchedule.stream_account_id == None) | (StreamSchedule.is_overnight == False)
            )
        ).all()
        changed = False
        for s in needs_fix:
            if s.stream_account_id is None:
                s.stream_account_id = default_acct.id
                changed = True
            if not s.is_overnight and s.end_time < s.start_time:
                s.is_overnight = True
                changed = True
            session.add(s)
        if changed:
            session.commit()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/stream-manager", response_class=HTMLResponse)
def stream_manager_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    _ensure_default_stream_account(session)

    streamers = session.exec(
        select(Streamer).where(Streamer.is_active == True).order_by(Streamer.name)
    ).all()

    stream_accounts = session.exec(
        select(StreamAccount)
        .where(StreamAccount.is_active == True)
        .order_by(StreamAccount.sort_order, StreamAccount.name)
    ).all()

    now_pst = _now_pacific()
    today = now_pst.strftime("%Y-%m-%d")
    end_date = (now_pst + timedelta(days=7)).strftime("%Y-%m-%d")
    schedules = session.exec(
        select(StreamSchedule)
        .where(StreamSchedule.date >= today, StreamSchedule.date <= end_date)
        .order_by(StreamSchedule.date, StreamSchedule.start_time)
    ).all()

    streamer_map = {s.id: s for s in streamers}

    schedule_by_account: dict[int, list[dict]] = {a.id: [] for a in stream_accounts}
    for sched in schedules:
        s = streamer_map.get(sched.streamer_id)
        acct_id = sched.stream_account_id or (stream_accounts[0].id if stream_accounts else 0)
        item = {
            "id": sched.id,
            "date": sched.date,
            "day_of_week": datetime.strptime(sched.date, "%Y-%m-%d").strftime("%A"),
            "start_time": sched.start_time,
            "end_time": sched.end_time,
            "start_display": _format_time_12h(sched.start_time),
            "end_display": _format_time_12h(sched.end_time),
            "is_overnight": sched.is_overnight,
            "title": sched.title or "",
            "notes": sched.notes or "",
            "streamer_id": sched.streamer_id,
            "streamer_name": (s.display_name or s.name) if s else "Unknown",
            "streamer_color": (s.color or "#fe2c55") if s else "#fe2c55",
            "streamer_emoji": (s.avatar_emoji or "\U0001f3ae") if s else "\U0001f3ae",
        }
        if acct_id in schedule_by_account:
            schedule_by_account[acct_id].append(item)
        else:
            schedule_by_account.setdefault(acct_id, []).append(item)

    current_streamer = get_current_streamer(session)

    return templates.TemplateResponse(request, "stream_manager.html", {
        "request": request,
        "title": "Stream Manager",
        "current_user": getattr(request.state, "current_user", None),
        "streamers": streamers,
        "streamer_colors": STREAMER_COLORS,
        "stream_accounts": stream_accounts,
        "schedule_by_account": schedule_by_account,
        "current_streamer": current_streamer,
        "today": today,
    })


@router.post("/stream-manager/streamer/add")
def stream_manager_add_streamer(
    request: Request,
    name: str = Form(...),
    display_name: Optional[str] = Form(default=None),
    color: Optional[str] = Form(default=None),
    avatar_emoji: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = Streamer(
        name=name.strip(),
        display_name=(display_name or "").strip() or None,
        color=(color or "").strip() or None,
        avatar_emoji=(avatar_emoji or "").strip() or None,
    )
    session.add(streamer)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+added", status_code=303)


@router.post("/stream-manager/streamer/{streamer_id}/edit")
def stream_manager_edit_streamer(
    request: Request,
    streamer_id: int,
    name: str = Form(...),
    display_name: Optional[str] = Form(default=None),
    color: Optional[str] = Form(default=None),
    avatar_emoji: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = session.get(Streamer, streamer_id)
    if not streamer:
        return RedirectResponse(url="/stream-manager?error=Streamer+not+found", status_code=303)

    streamer.name = name.strip()
    streamer.display_name = (display_name or "").strip() or None
    streamer.color = (color or "").strip() or None
    streamer.avatar_emoji = (avatar_emoji or "").strip() or None
    streamer.updated_at = utcnow()
    session.add(streamer)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+updated", status_code=303)


@router.post("/stream-manager/streamer/{streamer_id}/delete")
def stream_manager_delete_streamer(
    request: Request,
    streamer_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = session.get(Streamer, streamer_id)
    if streamer:
        streamer.is_active = False
        streamer.updated_at = utcnow()
        session.add(streamer)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+removed", status_code=303)


@router.post("/stream-manager/schedule/add")
def stream_manager_add_schedule(
    request: Request,
    streamer_id: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    stream_account_id: Optional[str] = Form(default=None),
    title: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    st = start_time.strip()
    et = end_time.strip()
    is_overnight = et < st  # e.g. start 18:00, end 06:00

    acct_id = int(stream_account_id) if stream_account_id else None
    if acct_id is None:
        default_acct = session.exec(
            select(StreamAccount).where(StreamAccount.is_default == True)
        ).first()
        if default_acct:
            acct_id = default_acct.id

    sched = StreamSchedule(
        streamer_id=int(streamer_id),
        stream_account_id=acct_id,
        date=date.strip(),
        start_time=st,
        end_time=et,
        is_overnight=is_overnight,
        title=(title or "").strip() or None,
        notes=(notes or "").strip() or None,
    )
    session.add(sched)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Shift+added", status_code=303)


@router.post("/stream-manager/schedule/{schedule_id}/delete")
def stream_manager_delete_schedule(
    request: Request,
    schedule_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    sched = session.get(StreamSchedule, schedule_id)
    if sched:
        session.delete(sched)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Shift+removed", status_code=303)


@router.post("/stream-manager/schedule/{schedule_id}/edit")
def stream_manager_edit_schedule(
    request: Request,
    schedule_id: int,
    streamer_id: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    sched = session.get(StreamSchedule, schedule_id)
    if sched:
        st = start_time.strip()
        et = end_time.strip()
        sched.streamer_id = int(streamer_id)
        sched.date = date.strip()
        sched.start_time = st
        sched.end_time = et
        sched.is_overnight = et < st
        sched.title = (title or "").strip() or None
        sched.notes = (notes or "").strip() or None
        sched.updated_at = utcnow()
        session.add(sched)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Shift+updated", status_code=303)


@router.post("/stream-manager/account/add")
def stream_manager_add_account(
    request: Request,
    name: str = Form(...),
    platform: str = Form(default="TikTok"),
    handle: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    max_order = session.exec(select(StreamAccount.sort_order).order_by(StreamAccount.sort_order.desc())).first() or 0
    acct = StreamAccount(
        name=name.strip(),
        platform=platform.strip(),
        handle=(handle or "").strip() or None,
        is_default=False,
        sort_order=max_order + 1,
    )
    session.add(acct)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Stream+added", status_code=303)


@router.post("/stream-manager/account/{account_id}/edit")
def stream_manager_edit_account(
    request: Request,
    account_id: int,
    name: str = Form(...),
    platform: str = Form(default="TikTok"),
    handle: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    acct = session.get(StreamAccount, account_id)
    if acct:
        acct.name = name.strip()
        acct.platform = platform.strip()
        acct.handle = (handle or "").strip() or None
        acct.updated_at = utcnow()
        session.add(acct)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Stream+updated", status_code=303)


@router.post("/stream-manager/account/{account_id}/delete")
def stream_manager_delete_account(
    request: Request,
    account_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    acct = session.get(StreamAccount, account_id)
    if acct:
        acct.is_active = False
        acct.updated_at = utcnow()
        session.add(acct)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Stream+removed", status_code=303)


@router.get("/api/stream-manager/current-streamer")
def api_current_streamer(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {"current_streamer": get_current_streamer(session)}
