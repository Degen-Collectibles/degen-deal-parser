"""Pure view-model builders for the employee Schedule and Hours screens.

The routes in ``app/routers/team.py`` load rows (the schedule grid's
``entry_map`` per calendar, Stream Manager hints, approved time off, the
Clockify week from ``employee_week_hours()``) and hand them to these
functions, which decide what the pages say. No DB or router imports here,
so the shaping rules are unit-testable without rendering a template.

Design source: docs/design/team-portal-redesign/employee-mockup.html
(Schedule + Hours screens), PRD Phase 2.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Optional, Sequence

from .home import hours_number, month_day, range_label
from .shift_labels import parse_shift_ranges, parse_time_to_minutes

_DAY_MINUTES = 24 * 60

# ShiftEntry kinds that are not a shift the person works. Same set Home uses
# for its week strip and "scheduled" tile.
NON_WORK_KINDS = frozenset({"request", "off", "blank", ""})
TIMEOFF_KIND = "request"

LOCATION_STOREFRONT = "storefront"
LOCATION_PACKING = "packing"
LOCATION_STREAM = "stream"
LOCATION_ORDER = (LOCATION_STOREFRONT, LOCATION_PACKING, LOCATION_STREAM)

VIEW_MINE = "mine"
VIEW_TEAM = "team"

FACE_TONES = 6

# Grid shorthand typed in capitals, shown in plain case.
_TOKEN_LABELS = {"SHOW": "Show", "IF NEEDED": "If needed", "STREAM": "Stream", "ALL": "All day"}


def normalize_view(value: Any) -> str:
    """'team' or 'mine' (the default). Anything else falls back to 'mine'."""
    if isinstance(value, str) and value.strip().lower() == VIEW_TEAM:
        return VIEW_TEAM
    return VIEW_MINE


def week_label(start: date) -> str:
    """'Sep 22 – 28' or 'Sep 29 – Oct 5'."""
    end = start + timedelta(days=6)
    if start.month == end.month:
        return f"{month_day(start)} – {end.day}"
    return f"{month_day(start)} – {month_day(end)}"


def initials(name: str) -> str:
    parts = [p for p in (name or "").replace("(", " ").split() if p[:1].isalnum()]
    return "".join(p[0] for p in parts[:2]).upper() or "?"


def face_tone(user_id: Optional[int]) -> int:
    """Stable 1..FACE_TONES colour slot for a person's initials bubble."""
    return (int(user_id or 0) % FACE_TONES) + 1


def _get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    return getattr(row, key, default)


def _hours_text(hours: float) -> str:
    return hours_number(int(round(max(0.0, hours) * 3600)))


# ---------------------------------------------------------------------------
# Normalising schedule rows
# ---------------------------------------------------------------------------

def shift_from_entry(entry: Any) -> dict[str, Any]:
    """A ShiftEntry (or dict with label/kind) -> normalised shift dict."""
    label = str(_get(entry, "label") or "").strip()
    kind = str(_get(entry, "kind") or "").strip().lower()
    ranges = parse_shift_ranges(label) if kind not in NON_WORK_KINDS else []
    if ranges:
        time_text = " / ".join(range_label(a, b) for a, b in ranges)
    elif kind == "all_day":
        time_text = "All day"
    elif label.upper() in _TOKEN_LABELS:
        time_text = _TOKEN_LABELS[label.upper()]
    else:
        time_text = label or "Shift"
    return {
        "label": label,
        "kind": kind,
        "ranges": ranges,
        "time": time_text,
        "time_compact": (
            " / ".join(range_label(a, b, compact=True) for a, b in ranges)
            if ranges
            else time_text
        ),
        "hours": round(sum(max(0, b - a) for a, b in ranges) / 60.0, 2),
        "note": "If needed" if kind == "if_needed" else "",
        "start": ranges[0][0] if ranges else None,
    }


def shift_from_stream_hint(hint: Mapping[str, Any]) -> dict[str, Any]:
    """A Stream Manager hint (``_stream_schedule_hint_map`` item) -> shift."""
    start = parse_time_to_minutes(str(hint.get("start_time") or ""))
    end = parse_time_to_minutes(str(hint.get("end_time") or ""))
    ranges: list[tuple[int, int]] = []
    if start is not None and end is not None:
        if end <= start or hint.get("is_overnight"):
            end += _DAY_MINUTES
        ranges = [(start, end)]
    time_text = (
        range_label(ranges[0][0], ranges[0][1])
        if ranges
        else str(hint.get("label") or "Stream")
    )
    note = str(hint.get("account_name") or "").strip()
    if note == "Other":
        note = ""
    return {
        "label": str(hint.get("label") or ""),
        "kind": "stream",
        "ranges": ranges,
        "time": time_text,
        "time_compact": (
            range_label(ranges[0][0], ranges[0][1], compact=True) if ranges else time_text
        ),
        "hours": round(sum(b - a for a, b in ranges) / 60.0, 2),
        "note": note,
        "start": ranges[0][0] if ranges else None,
    }


def _calendar_rows(calendar: Mapping[str, Any]) -> dict[tuple[int, str], list[dict]]:
    """{(user_id, iso): [shift, ...]} for one calendar, normalised."""
    out: dict[tuple[int, str], list[dict]] = {}
    is_stream = calendar.get("kind") == LOCATION_STREAM
    for key, rows in (calendar.get("entries") or {}).items():
        shifts = [
            shift_from_stream_hint(row) if is_stream else shift_from_entry(row)
            for row in (rows or [])
        ]
        if shifts:
            out[key] = shifts
    return out


def _is_work(shift: Mapping[str, Any]) -> bool:
    return shift.get("kind") not in NON_WORK_KINDS


def _sort_start(shift: Mapping[str, Any]) -> int:
    start = shift.get("start")
    return start if isinstance(start, int) else _DAY_MINUTES * 2


# ---------------------------------------------------------------------------
# My shifts
# ---------------------------------------------------------------------------

def build_my_week(
    *,
    week_days: Sequence[date],
    today: date,
    me_id: int,
    calendars: Iterable[Mapping[str, Any]],
    names: Optional[Mapping[int, str]] = None,
    timeoff_days: Iterable[date] = (),
    day_notes: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """One row per day (Mon–Sun) for the current user.

    ``calendars`` is a list of ``{"kind", "label", "entries"}`` where
    ``entries`` is the grid's ``entry_map`` ((user_id, iso) -> rows) for
    Storefront/Packing, or the Stream Manager hint map for Stream. Coworkers
    are other people with a working shift on the same calendar and day.
    Approved time off comes from both ``timeoff_days`` (TimeOffRequest rows)
    and REQUEST-kind entries (what approval writes onto the grid).
    """
    names = names or {}
    day_notes = day_notes or {}
    timeoff = set(timeoff_days)
    normalised = [
        (cal, _calendar_rows(cal)) for cal in calendars
    ]

    days: list[dict[str, Any]] = []
    total_hours = 0.0
    shift_count = 0
    for day in week_days:
        iso = day.isoformat()
        shifts: list[dict[str, Any]] = []
        is_timeoff = day in timeoff
        is_off_marked = False
        for cal, rows in normalised:
            kind = str(cal.get("kind") or "")
            mine = rows.get((me_id, iso), [])
            for shift in mine:
                if shift["kind"] == TIMEOFF_KIND:
                    is_timeoff = True
                    continue
                if shift["kind"] == "off":
                    is_off_marked = True
                    continue
                if not _is_work(shift):
                    continue
                coworkers = []
                for (uid, other_iso), other in rows.items():
                    if uid == me_id or other_iso != iso:
                        continue
                    if not any(_is_work(s) for s in other):
                        continue
                    name = names.get(uid) or "Teammate"
                    coworkers.append(
                        {
                            "user_id": uid,
                            "name": name,
                            "initials": initials(name),
                            "tone": face_tone(uid),
                        }
                    )
                coworkers.sort(key=lambda c: c["name"].lower())
                place_note = day_notes.get(iso, "") if kind == LOCATION_STOREFRONT else ""
                notes = [n for n in (shift["note"], place_note) if n]
                shifts.append(
                    {
                        **shift,
                        "location": kind,
                        "location_label": str(cal.get("label") or kind.title()),
                        "notes": notes,
                        "coworkers": coworkers,
                    }
                )
        shifts.sort(key=_sort_start)
        hours = round(sum(s["hours"] for s in shifts), 2)
        total_hours += hours
        shift_count += len(shifts)
        if shifts:
            state = "work"
        elif is_timeoff:
            state = "timeoff"
        else:
            state = "off"
        days.append(
            {
                "date": day,
                "iso": iso,
                "weekday": f"{day:%a}",
                "number": day.day,
                "is_today": day == today,
                "is_past": day < today,
                "state": state,
                "timeoff": is_timeoff,
                # "approved" only when a TimeOffRequest backs it; a REQUEST
                # cell typed straight onto the grid is just "Time off".
                "timeoff_label": (
                    "Time off · approved" if day in timeoff else "Time off"
                ),
                "marked_off": is_off_marked,
                "shifts": shifts,
                "scheduled_hours": hours,
            }
        )

    upcoming = [d for d in days if d["state"] == "work" and d["date"] >= today]
    return {
        "days": days,
        "scheduled_hours": round(total_hours, 2),
        "scheduled_label": _hours_text(total_hours),
        "shift_count": shift_count,
        "next_work_date": upcoming[0]["date"] if upcoming else None,
    }


def hero_shifts(
    days: Sequence[Mapping[str, Any]], *, today: date
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(today_shifts, upcoming_shifts)`` for ``home.build_hero``.

    ``days`` is ``build_my_week()["days"]`` over yesterday .. some days
    ahead, so Home reads the same combined source as Schedule and Hours
    (Storefront/Packing ShiftEntry rows plus Stream Manager shifts).
    Each shift keeps its parsed ``ranges`` in minutes past *today's*
    midnight: an overnight shift from yesterday that is still running
    ("6 PM – 2 AM") is carried into today with a negative start.
    """
    today_rows: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    for day in days:
        shift_date = day.get("date")
        if not isinstance(shift_date, date):
            continue
        offset = (shift_date - today).days
        for shift in day.get("shifts") or ():
            ranges = list(shift.get("ranges") or [])
            row = {
                "shift_date": shift_date,
                "label": str(shift.get("label") or ""),
                "time": str(shift.get("time") or ""),
                "kind": str(shift.get("kind") or ""),
                "calendar_kind": str(shift.get("location") or ""),
                "calendar_label": str(shift.get("location_label") or ""),
                "day_note": " · ".join(n for n in (shift.get("notes") or ()) if n) or None,
                "ranges": ranges,
                "start": shift.get("start"),
            }
            if offset == -1:
                carried = [
                    (a - _DAY_MINUTES, b - _DAY_MINUTES)
                    for a, b in ranges
                    if b > _DAY_MINUTES
                ]
                if carried:
                    today_rows.append({**row, "ranges": carried})
            elif offset == 0:
                today_rows.append(row)
                upcoming.append(row)
            elif offset > 0:
                upcoming.append(row)
    today_rows.sort(key=lambda r: r["ranges"][0][0] if r["ranges"] else _DAY_MINUTES * 2)
    upcoming.sort(key=lambda r: (r["shift_date"], _sort_start(r)))
    return today_rows, upcoming


def my_week_eyebrow(my_week: Mapping[str, Any]) -> str:
    """'30h scheduled · 4 shifts' / 'No shifts this week'."""
    count = int(my_week.get("shift_count") or 0)
    if not count:
        return "No shifts this week"
    noun = "shift" if count == 1 else "shifts"
    hours = float(my_week.get("scheduled_hours") or 0)
    if hours <= 0:
        return f"{count} {noun}"
    return f"{my_week.get('scheduled_label')}h scheduled · {count} {noun}"


# ---------------------------------------------------------------------------
# Whole team
# ---------------------------------------------------------------------------

def build_team_week(
    *,
    week_days: Sequence[date],
    today: date,
    me_id: int,
    calendars: Iterable[Mapping[str, Any]],
    names: Optional[Mapping[int, str]] = None,
    day_notes: Optional[Mapping[str, str]] = None,
) -> list[dict[str, Any]]:
    """Day-by-day list grouped by location, earliest start first.

    Only working shifts are listed: OFF / time-off cells are the old grid's
    blanks, and other people's time off isn't this view's business.
    """
    names = names or {}
    day_notes = day_notes or {}
    normalised = [(cal, _calendar_rows(cal)) for cal in calendars]
    out: list[dict[str, Any]] = []
    for day in week_days:
        iso = day.isoformat()
        groups = []
        for cal, rows in normalised:
            kind = str(cal.get("kind") or "")
            people = []
            for (uid, other_iso), shifts in rows.items():
                if other_iso != iso:
                    continue
                work = sorted((s for s in shifts if _is_work(s)), key=_sort_start)
                if not work:
                    continue
                name = names.get(uid) or "Teammate"
                notes = [s["note"] for s in work if s["note"]]
                people.append(
                    {
                        "user_id": uid,
                        "name": name,
                        "initials": initials(name),
                        "tone": face_tone(uid),
                        "is_me": uid == me_id,
                        # Compact ("11 AM – 7 PM") so the name keeps room on a phone.
                        "time": " · ".join(s["time_compact"] for s in work),
                        "note": " · ".join(dict.fromkeys(notes)),
                        "sort": _sort_start(work[0]),
                    }
                )
            if not people:
                continue
            people.sort(key=lambda p: (p["sort"], p["name"].lower()))
            groups.append(
                {
                    "location": kind,
                    "label": str(cal.get("label") or kind.title()),
                    "note": day_notes.get(iso, "") if kind == LOCATION_STOREFRONT else "",
                    "people": people,
                }
            )
        out.append(
            {
                "date": day,
                "iso": iso,
                "heading": (
                    f"Today · {day:%a} {day.day}" if day == today else f"{day:%a} {day.day}"
                ),
                "is_today": day == today,
                "is_past": day < today,
                "groups": groups,
                "people_count": sum(len(g["people"]) for g in groups),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Hours
# ---------------------------------------------------------------------------

# Bar heights snap to 5% steps so the chart is plain CSS classes
# (.pt-h-0 … .pt-h-100) rather than inline style= heights.
_BAR_STEP = 5
_BAR_MIN_SCALE_HOURS = 9.0


def bar_bucket(value: float, scale: float) -> int:
    if value <= 0 or scale <= 0:
        return 0
    pct = min(100.0, value / scale * 100.0)
    bucket = int(round(pct / _BAR_STEP)) * _BAR_STEP
    # A real but tiny value still shows a sliver.
    return max(_BAR_STEP, min(100, bucket))


def _time_or_blank(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    minutes = value.hour * 60 + value.minute
    hour, minute = divmod(minutes, 60)
    suffix = "AM" if hour < 12 else "PM"
    return f"{hour % 12 or 12}:{minute:02d} {suffix}"


def _duration_text(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    hours, rem = divmod(seconds, 3600)
    minutes = rem // 60
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def build_hours_view(
    *,
    week: Mapping[str, Any],
    my_days: Sequence[Mapping[str, Any]],
    entries: Sequence[Mapping[str, Any]],
    today: date,
) -> dict[str, Any]:
    """Shape ``employee_week_hours()`` output for the Hours page.

    ``my_days`` is ``build_my_week()["days"]`` for the same week (scheduled
    hours per day). ``entries`` are the week's Clockify entries as dicts with
    ``start_local``, ``end_local``, ``duration_seconds``, ``running``,
    ``description`` and ``is_break`` (the router applies the same
    break-keyword rule payroll uses).

    Flags only come from rules the app already applies elsewhere:
      * missed-break deduction (``auto_break_seconds`` from payroll's rule),
      * manager marked the day "Needs fix" (TimecardApproval),
      * an entry with no clock-out (admin timecards "No clock-out"),
      * a scheduled past day with nothing logged (admin timecards "No-show",
        worded for the employee and limited to days already over).
    """
    sched_by_day = {d["date"]: float(d.get("scheduled_hours") or 0) for d in my_days}
    entries_by_day: dict[date, list[Mapping[str, Any]]] = {}
    for entry in entries:
        start = entry.get("start_local")
        if start is None:
            continue
        entries_by_day.setdefault(start.date(), []).append(entry)

    days_src = list(week.get("days") or [])
    worked_by_day = {d["day"]: int(d.get("work_seconds") or 0) for d in days_src}
    max_hours = max(
        [_BAR_MIN_SCALE_HOURS]
        + [v / 3600.0 for v in worked_by_day.values()]
        + list(sched_by_day.values())
    )

    bars = []
    rows = []
    days_worked = 0
    for src in days_src:
        day = src["day"]
        work = int(src.get("work_seconds") or 0)
        brk = int(src.get("break_seconds") or 0)
        auto = int(src.get("auto_break_seconds") or 0)
        sched = sched_by_day.get(day, 0.0)
        worked_h = work / 3600.0
        if work > 0:
            days_worked += 1
        bars.append(
            {
                "letter": f"{day:%a}"[0],
                "weekday": f"{day:%a}",
                "is_today": day == today,
                "worked_bucket": bar_bucket(worked_h, max_hours),
                "sched_bucket": bar_bucket(sched, max_hours),
                "worked_label": hours_number(work),
                "sched_label": _hours_text(sched),
                "has_sched": sched > 0,
            }
        )

        day_entries = sorted(
            entries_by_day.get(day, []),
            key=lambda e: e.get("start_local") or datetime.min,
        )
        lines = []
        running = False
        no_clock_out = False
        for entry in day_entries:
            start_txt = _time_or_blank(entry.get("start_local"))
            if entry.get("running"):
                end_txt = "now"
                running = True
            elif entry.get("end_local") is None:
                end_txt = "no clock-out"
                no_clock_out = True
            else:
                end_txt = _time_or_blank(entry.get("end_local"))
            desc = str(entry.get("description") or "").strip()
            if entry.get("is_break"):
                text = f"Break {start_txt} – {end_txt}"
                if not entry.get("running"):
                    text += f" · {_duration_text(entry.get('duration_seconds') or 0)}"
            else:
                text = f"{start_txt} – {end_txt}"
                if desc:
                    text += f" · {desc}"
            lines.append({"text": text, "is_break": bool(entry.get("is_break"))})

        flags = []
        status = str(src.get("status") or "")
        if status == "rejected":
            note = str(src.get("status_note") or "").strip()
            flags.append(
                {
                    "tone": "err",
                    "text": "Needs fix" + (f": {note}" if note else "")
                    + ". It won't be paid until it's sorted out.",
                }
            )
        if auto > 0:
            flags.append(
                {
                    "tone": "warn",
                    "text": (
                        f"No break logged on a day over 5 hours, so "
                        f"{_duration_text(auto)} was deducted automatically. "
                        "Tell your manager if that's wrong."
                    ),
                }
            )
        if no_clock_out:
            flags.append(
                {"tone": "warn", "text": "No clock-out recorded. Tell your manager."}
            )
        if sched > 0 and work <= 0 and not running and day < today:
            flags.append(
                {
                    "tone": "warn",
                    "text": (
                        "Scheduled, but no hours logged. "
                        "Tell your manager if you worked."
                    ),
                }
            )

        if not (work or brk or day_entries or sched or flags or status):
            continue
        if day > today and not (work or day_entries):
            # Upcoming scheduled days are on the chart; the list is history.
            continue
        rows.append(
            {
                "date": day,
                "heading": (
                    f"Today · {day:%a} {day.day}"
                    if day == today
                    else f"{day:%a} {month_day(day)}"
                ),
                "is_today": day == today,
                "worked_label": hours_number(work),
                "worked_seconds": work,
                "sched_label": _hours_text(sched) if sched else "",
                "break_label": _duration_text(brk) if brk else "",
                "running": running,
                "lines": lines,
                "flags": flags,
                "status_label": str(src.get("status_label") or ""),
                "status_tone": _pill_tone(str(src.get("status_tone") or "")),
                "status": status,
            }
        )
    rows.sort(key=lambda r: r["date"], reverse=True)

    total_work = int(week.get("total_work_seconds") or 0)
    total_break = int(week.get("total_break_seconds") or 0)
    total_auto = int(week.get("total_auto_break_seconds") or 0)
    scheduled_total = round(sum(sched_by_day.values()), 2)
    return {
        "total_label": hours_number(total_work),
        "total_seconds": total_work,
        "scheduled_total": scheduled_total,
        "scheduled_label": _hours_text(scheduled_total) if scheduled_total else "",
        "bars": bars,
        "rows": rows,
        "days_worked": days_worked,
        "days_scheduled": sum(1 for v in sched_by_day.values() if v > 0),
        "break_label": hours_number(total_break),
        "break_seconds": total_break,
        "auto_break_seconds": total_auto,
        "auto_break_label": _duration_text(total_auto) if total_auto else "",
    }


_PILL_TONES = {"ok": "ok", "danger": "err", "info": "info", "muted": "neutral"}


def _pill_tone(tone: str) -> str:
    return _PILL_TONES.get(tone, "neutral")
