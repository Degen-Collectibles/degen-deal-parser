"""Pure view-model builders for the employee Home screen (/team/).

The route in ``app/routers/team.py`` gathers rows (shifts, Clockify week,
requests, policies) and hands them to these functions, which decide what
the Home screen says. Keeping the decisions here -- with no DB or router
imports -- makes the hero state and the "Needs you" rules unit-testable
without rendering a template.

Design source: docs/design/team-portal-redesign/employee-mockup.html.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional

from .clockify import format_hours
from .shift_labels import parse_shift_hours, parse_shift_ranges

HERO_ON = "on"
HERO_LATER = "later"
HERO_OFF = "off"

_DAY_MINUTES = 24 * 60
_WEEKDAY_LETTERS = ("M", "T", "W", "T", "F", "S", "S")


# ---------------------------------------------------------------------------
# Formatting helpers (portable: no %-d, which Windows strftime lacks)
# ---------------------------------------------------------------------------

def month_day(value: date) -> str:
    """'Sep 24'."""
    return f"{value:%b} {value.day}"


def weekday_month_day(value: date) -> str:
    """'Wed Sep 24'."""
    return f"{value:%a} {value:%b} {value.day}"


def date_span(start: date, end: date) -> str:
    """'Sat Sep 27' for one day, 'Oct 10 – 12' or 'Sep 30 – Oct 2' for spans."""
    if start == end:
        return weekday_month_day(start)
    if start.month == end.month and start.year == end.year:
        return f"{month_day(start)} – {end.day}"
    return f"{month_day(start)} – {month_day(end)}"


def clock_label(minutes: int, *, compact: bool = False) -> str:
    """Minutes past midnight (may exceed 1440) -> '2:00 PM' ('2 PM' compact)."""
    minutes %= _DAY_MINUTES
    hour, minute = divmod(minutes, 60)
    suffix = "AM" if hour < 12 else "PM"
    hour12 = hour % 12 or 12
    if compact and minute == 0:
        return f"{hour12} {suffix}"
    return f"{hour12}:{minute:02d} {suffix}"


def time_label(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return clock_label(value.hour * 60 + value.minute)


def range_label(start: int, end: int, *, compact: bool = False) -> str:
    """'2:00 – 8:00 PM' when both ends share AM/PM, else '10:00 AM – 2:00 PM'.

    ``compact`` drops ':00' ('10 AM – 2 PM') so the hero's 40px headline
    fits on one line on a 375px phone.
    """
    start_txt = clock_label(start, compact=compact)
    end_txt = clock_label(end, compact=compact)
    if start_txt[-2:] == end_txt[-2:]:
        return f"{start_txt[:-3]} – {end_txt}"
    return f"{start_txt} – {end_txt}"


def shift_display(label: str) -> str:
    """Normalised time range for a hand-typed label; falls back to the label."""
    ranges = parse_shift_ranges(label or "")
    if not ranges:
        return (label or "").strip() or "Shift"
    return " / ".join(range_label(start, end) for start, end in ranges)


def hours_number(seconds: int) -> str:
    """Seconds -> '18.2' (no trailing '.0')."""
    value = round(max(0, seconds) / 3600.0, 1)
    text = f"{value:.1f}"
    return text[:-2] if text.endswith(".0") else text


def hours_number_from_hours(hours: float) -> str:
    return hours_number(int(round(hours * 3600)))


def _minutes_of(value: datetime) -> int:
    return value.hour * 60 + value.minute


def _until_label(minutes: int) -> str:
    minutes = max(0, minutes)
    hours, mins = divmod(minutes, 60)
    if hours and mins:
        return f"{hours}h {mins}m"
    if hours:
        return f"{hours}h"
    return f"{mins}m"


# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------

def _shift_windows(shift: dict[str, Any]) -> list[tuple[int, int]]:
    return parse_shift_ranges(str(shift.get("label") or ""))


def _place(shift: dict[str, Any]) -> str:
    parts = [str(shift.get("calendar_label") or "").strip()]
    note = str(shift.get("day_note") or "").strip()
    if note and note not in parts:
        parts.append(note)
    return " · ".join(p for p in parts if p)


def _next_shift_day_label(shift_date: date, today: date) -> str:
    delta = (shift_date - today).days
    if delta == 1:
        return "Tomorrow"
    if 1 < delta < 7:
        return f"{shift_date:%A}"
    return f"{shift_date:%a}, {month_day(shift_date)}"


def build_hero(
    *,
    now_local: datetime,
    today: date,
    today_shifts: list[dict[str, Any]],
    upcoming_shifts: list[dict[str, Any]],
    clock: Optional[dict[str, Any]] = None,
    schedule_href: str = "/team/schedule",
    timeoff_href: Optional[str] = "/team/timeoff",
    hours_href: Optional[str] = "/team/hours",
) -> dict[str, Any]:
    """Pick one of three hero states.

    * ``on``    -- Clockify shows a running (non-break or break) entry now.
    * ``later`` -- not clocked in, and a shift today hasn't ended yet (or
                   today's label has no parseable times, so we can't tell).
    * ``off``   -- nothing left today; show the next scheduled shift.

    ``clock`` is ``{"linked", "running", "on_break", "since", "today_seconds"}``
    derived from the Clockify week; ``None`` means hours aren't connected.
    """
    clock = clock or {}
    now_min = _minutes_of(now_local)
    meta: list[dict[str, str]] = []
    actions: list[dict[str, str]] = []

    if clock.get("running"):
        since: Optional[datetime] = clock.get("since")
        current = None
        end_min: Optional[int] = None
        for shift in today_shifts:
            for start, end in _shift_windows(shift):
                if end > now_min and (end_min is None or end < end_min):
                    current, end_min = shift, end
        status = "On break" if clock.get("on_break") else "On the clock"
        if current is not None and _place(current):
            status = f"{status} · {_place(current)}"
        if since is not None:
            meta.append({"label": "Since", "value": time_label(since)})
        if end_min is not None:
            meta.append({"label": "shift ends", "value": clock_label(end_min)})
        progress = None
        if since is not None and end_min is not None:
            since_min = _minutes_of(since)
            span = end_min - since_min
            if span > 0:
                progress = max(0, min(100, int(round((now_min - since_min) * 100 / span))))
        if hours_href:
            actions.append({"label": "My hours", "href": hours_href, "kind": ""})
        actions.append({"label": "My week", "href": schedule_href, "kind": "ghost"})
        return {
            "state": HERO_ON,
            "live": True,
            "status": status,
            "big": format_hours(int(clock.get("today_seconds") or 0)),
            "meta": meta,
            "progress": progress,
            "actions": actions,
        }

    # A shift later today (or in progress without a clock-in).
    for shift in today_shifts:
        windows = _shift_windows(shift)
        pending = [(s, e) for s, e in windows if e > now_min]
        if windows and not pending:
            continue  # this shift is already over
        place = _place(shift)
        if not windows:
            status = "Scheduled today"
            big = str(shift.get("label") or "Shift").strip() or "Shift"
        else:
            start, end = pending[0]
            big = range_label(start, end, compact=True)
            if start > now_min:
                status = f"Next shift · starts in {_until_label(start - now_min)}"
            elif clock.get("linked"):
                status = f"Shift started {clock_label(start)} · not clocked in yet"
            else:
                status = f"On shift now · until {clock_label(end)}"
        if place:
            meta.append({"label": "", "value": place})
        actions.append({"label": "See my week", "href": schedule_href, "kind": "primary"})
        if timeoff_href:
            actions.append({"label": "Can't make it?", "href": timeoff_href, "kind": ""})
        return {
            "state": HERO_LATER,
            "live": False,
            "status": status,
            "big": big,
            "meta": meta,
            "progress": None,
            "actions": actions,
        }

    next_shift = next(
        (row for row in upcoming_shifts if row.get("shift_date") and row["shift_date"] > today),
        None,
    )
    status = "Done for today" if today_shifts else "You're off today"
    actions.append({"label": "See my week", "href": schedule_href, "kind": ""})
    if next_shift is None:
        return {
            "state": HERO_OFF,
            "live": False,
            "status": status,
            "big": "No shifts posted",
            "meta": [{"label": "No upcoming shifts posted yet.", "value": ""}],
            "progress": None,
            "actions": actions,
        }
    meta.append({"label": "Next shift", "value": shift_display(str(next_shift.get("label") or ""))})
    place = _place(next_shift)
    if place:
        meta.append({"label": "", "value": place})
    return {
        "state": HERO_OFF,
        "live": False,
        "status": status,
        "big": _next_shift_day_label(next_shift["shift_date"], today),
        "meta": meta,
        "progress": None,
        "actions": actions,
    }


# ---------------------------------------------------------------------------
# Week strip + scheduled hours
# ---------------------------------------------------------------------------

def build_week_strip(
    *,
    week_start: date,
    today: date,
    shifts_by_day: dict[date, list[str]],
    timeoff_days: Iterable[date],
) -> list[dict[str, Any]]:
    timeoff = set(timeoff_days)
    days: list[dict[str, Any]] = []
    for offset in range(7):
        day = week_start + timedelta(days=offset)
        labels = [label for label in shifts_by_day.get(day, []) if label]
        has_shift = bool(shifts_by_day.get(day))
        has_timeoff = day in timeoff
        if has_shift:
            what = ", ".join(shift_display(label) for label in labels) or "scheduled"
        elif has_timeoff:
            what = "approved time off"
        else:
            what = "off"
        if has_shift and has_timeoff:
            what += " (approved time off)"
        days.append(
            {
                "date": day,
                "letter": _WEEKDAY_LETTERS[day.weekday()],
                "number": day.day,
                "is_today": day == today,
                "is_past": day < today,
                "has_shift": has_shift,
                "has_timeoff": has_timeoff,
                "aria": f"{weekday_month_day(day)}: {what}",
            }
        )
    return days


def scheduled_hours(labels: Iterable[str]) -> float:
    return round(sum(parse_shift_hours(label or "") for label in labels), 2)


# ---------------------------------------------------------------------------
# Needs you (real to-dos only -- announcements are deliberately excluded)
# ---------------------------------------------------------------------------

MAX_POLICY_ROWS = 3


def build_needs_you(
    *,
    profile_completion: dict[str, Any],
    needs_fix_days: Iterable[dict[str, Any]] = (),
    clockify_configured: bool = False,
) -> list[dict[str, Any]]:
    """Actionable items only. Each clears itself once the employee acts.

    Sources: unacknowledged required policies, missing phone / emergency
    contact, unlinked Clockify (when Clockify is set up for the shop), and
    timecard days a manager marked "Needs fix". Announcements never appear
    here -- they are reading, not a to-do.
    """
    items: list[dict[str, Any]] = []
    done = int(profile_completion.get("complete_count") or 0)
    total = int(profile_completion.get("total_count") or 0)
    progress_sub = f"Profile is {done} of {total} done" if total else ""

    for day in needs_fix_days:
        work_date = day.get("day")
        note = str(day.get("status_note") or "").strip()
        week_param = ""
        if isinstance(work_date, date):
            week_param = f"?week={(work_date - timedelta(days=work_date.weekday())).isoformat()}"
        items.append(
            {
                "key": "timecard",
                "icon": "clock",
                "tone": "err",
                "label": (
                    f"Fix your timecard for {weekday_month_day(work_date)}"
                    if isinstance(work_date, date)
                    else "Fix your timecard"
                ),
                "sub": note or "A manager marked this day Needs fix",
                "href": f"/team/hours{week_param}",
            }
        )

    missing = list(profile_completion.get("missing_policies") or [])
    for policy in missing[:MAX_POLICY_ROWS]:
        version = str(policy.get("version") or "").strip()
        items.append(
            {
                "key": "policy",
                "icon": "shield",
                "tone": "warn",
                "label": f"Acknowledge {policy.get('title') or 'policy'}",
                "sub": f"Policy {version}".strip() if version else "Policy",
                "href": "/team/policies",
            }
        )
    if len(missing) > MAX_POLICY_ROWS:
        extra = len(missing) - MAX_POLICY_ROWS
        items.append(
            {
                "key": "policy-more",
                "icon": "shield",
                "tone": "warn",
                "label": f"{extra} more polic{'ies' if extra != 1 else 'y'} to acknowledge",
                "sub": "Open Policies to review and sign",
                "href": "/team/policies",
            }
        )

    by_key = {item.get("key"): item for item in profile_completion.get("items") or []}
    if by_key.get("phone") is not None and not by_key["phone"].get("done"):
        items.append(
            {
                "key": "phone",
                "icon": "phone",
                "tone": "accent",
                "label": "Add your phone number",
                "sub": progress_sub or "So a manager can reach you",
                "href": "/team/profile",
            }
        )
    if by_key.get("emergency") is not None and not by_key["emergency"].get("done"):
        items.append(
            {
                "key": "emergency",
                "icon": "user",
                "tone": "accent",
                "label": "Add an emergency contact",
                "sub": progress_sub or "Name and phone number",
                "href": "/team/profile",
            }
        )
    if (
        clockify_configured
        and by_key.get("clockify") is not None
        and not by_key["clockify"].get("done")
    ):
        items.append(
            {
                "key": "clockify",
                "icon": "clock",
                "tone": "info",
                "label": "Hours aren't connected yet",
                "sub": "Ask a manager to link your Clockify account",
                "href": "/team/help?page=/team/hours",
            }
        )
    return items


# ---------------------------------------------------------------------------
# My requests
# ---------------------------------------------------------------------------

_STATUS_PILLS = {
    "submitted": ("warn", "Pending"),
    "pending": ("warn", "Pending"),
    "approved": ("ok", "Approved"),
    "ordered": ("info", "Ordered"),
    "denied": ("err", "Declined"),
    "cancelled": ("neutral", "Cancelled"),
}


def status_pill(status: str) -> tuple[str, str]:
    key = (status or "").strip().lower()
    return _STATUS_PILLS.get(key, ("neutral", key.capitalize() or "Unknown"))


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def build_request_rows(
    *,
    timeoff: Iterable[Any] = (),
    supply: Iterable[Any] = (),
    limit: int = 3,
    timeoff_href: str = "/team/timeoff",
    supply_href: str = "/team/supply",
) -> list[dict[str, Any]]:
    """Newest requests first, with anything still pending ahead of decided."""
    rows: list[dict[str, Any]] = []
    for row in timeoff:
        start, end = row.start_date, row.end_date
        tone, pill = status_pill(row.status)
        decided = _as_date(getattr(row, "status_changed_at", None))
        sent = _as_date(getattr(row, "created_at", None))
        rows.append(
            {
                "kind": "timeoff",
                "icon": "beach",
                "tone": "info",
                "title": f"Time off · {date_span(start, end)}",
                "sub": (
                    f"Decided {month_day(decided)}"
                    if decided and tone != "warn"
                    else (f"Sent {month_day(sent)}" if sent else "Sent")
                ),
                "pill_tone": tone,
                "pill": pill,
                "href": timeoff_href,
                "pending": tone == "warn",
                "sort_at": getattr(row, "created_at", None),
            }
        )
    for row in supply:
        tone, pill = status_pill(row.status)
        decided = _as_date(getattr(row, "status_changed_at", None))
        sent = _as_date(getattr(row, "created_at", None))
        rows.append(
            {
                "kind": "supply",
                "icon": "box",
                "tone": "purple",
                "title": str(row.title or "Supply request"),
                "sub": (
                    f"Decided {month_day(decided)}"
                    if decided and tone != "warn"
                    else (f"Sent {month_day(sent)}" if sent else "Sent")
                ),
                "pill_tone": tone,
                "pill": pill,
                "href": supply_href,
                "pending": tone == "warn",
                "sort_at": getattr(row, "created_at", None),
            }
        )

    def sort_key(item: dict[str, Any]) -> tuple[int, float]:
        stamp = item.get("sort_at")
        ts = stamp.timestamp() if isinstance(stamp, datetime) else 0.0
        return (0 if item["pending"] else 1, -ts)

    rows.sort(key=sort_key)
    return rows[:limit]
