"""Pure view-model builders for the employee Requests screen (/team/requests).

The routes in ``app/routers/team_timeoff.py`` load the user's TimeOffRequest
and SupplyRequest rows (plus their scheduled shifts, via the same schedule
helpers Schedule and Hours use) and hand them to these functions, which
decide what the page says. No DB or router imports here, so the card and
overlap rules are unit-testable without rendering a template.

Design source: docs/design/team-portal-redesign/employee-mockup.html
(Requests screen + "Request time off" sheet), PRD Phase 3.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

from .home import date_span, month_day, status_pill, weekday_month_day

KIND_TIMEOFF = "timeoff"
KIND_SUPPLY = "supply"
KINDS = (KIND_TIMEOFF, KIND_SUPPLY)

TAB_ALL = "all"

# Only a request still waiting on a manager can be edited or cancelled by
# the employee (PRD resolved question 1).
EDITABLE_STATUS = "submitted"
CANCELLED_STATUS = "cancelled"

URGENCY_LABELS = {"low": "Low", "normal": "Normal", "high": "High"}


def normalize_kind(value: Any, allowed: Iterable[str] = KINDS) -> str:
    """'timeoff' / 'supply' if allowed for this user, else ''."""
    if not isinstance(value, str):
        return ""
    key = value.strip().lower().replace("-", "")
    if key in ("timeoff", "time_off"):
        key = KIND_TIMEOFF
    elif key in ("supply", "supplies"):
        key = KIND_SUPPLY
    return key if key in set(allowed) else ""


def normalize_tab(value: Any, allowed: Iterable[str] = KINDS) -> str:
    """A kind the user can see, or 'all'."""
    return normalize_kind(value, allowed) or TAB_ALL


def parse_iso_date(value: Any) -> Optional[date]:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def sent_label(created: Any, today: date) -> str:
    """'sent today' / 'sent yesterday' / 'sent Sep 19'."""
    day = _as_date(created)
    if day is None:
        return "sent"
    delta = (today - day).days
    if delta == 0:
        return "sent today"
    if delta == 1:
        return "sent yesterday"
    return f"sent {month_day(day)}"


def day_count(start: date, end: date) -> int:
    return max(0, (end - start).days) + 1


def _days_text(count: int) -> str:
    return "1 day" if count == 1 else f"{count} days"


def _shifts_text(count: int) -> str:
    return "1 shift" if count == 1 else f"{count} shifts"


def _decided_text(status: str, when: Optional[date], by_name: str) -> str:
    if status == CANCELLED_STATUS:
        return f"you cancelled {month_day(when)}" if when else "you cancelled"
    parts = []
    if by_name:
        parts.append(f"decided by {by_name}")
    else:
        parts.append("decided")
    text = " ".join(parts)
    if when:
        text += f", {month_day(when)}" if by_name else f" {month_day(when)}"
    return text


def build_timeoff_card(
    row: Any,
    *,
    today: date,
    overlap_count: Optional[int] = None,
    decided_by: str = "",
) -> dict[str, Any]:
    status = str(getattr(row, "status", "") or "").strip().lower()
    tone, pill = status_pill(status)
    start, end = row.start_date, row.end_date
    is_open = status == EDITABLE_STATUS
    parts = ["Time off", _days_text(day_count(start, end))]
    if is_open:
        parts.append(sent_label(getattr(row, "created_at", None), today))
        if overlap_count:
            parts.append(f"overlaps {_shifts_text(overlap_count)}")
    else:
        parts.append(
            _decided_text(
                status,
                _as_date(getattr(row, "status_changed_at", None))
                or _as_date(getattr(row, "updated_at", None)),
                decided_by,
            )
        )
    return {
        "kind": KIND_TIMEOFF,
        "id": row.id,
        "title": date_span(start, end),
        "sub": " · ".join(parts),
        "pill_tone": tone,
        "pill": pill,
        "status": status,
        "note": str(getattr(row, "decision_notes", "") or "").strip(),
        "detail": str(getattr(row, "reason", "") or "").strip(),
        "is_open": is_open,
        "can_edit": is_open,
        "start_iso": start.isoformat(),
        "end_iso": end.isoformat(),
        "reason": str(getattr(row, "reason", "") or ""),
        "overlap_count": overlap_count or 0,
        "sort_at": getattr(row, "created_at", None),
        "decided_at": getattr(row, "status_changed_at", None)
        or getattr(row, "updated_at", None),
    }


def build_supply_card(
    row: Any,
    *,
    today: date,
    decided_by: str = "",
) -> dict[str, Any]:
    status = str(getattr(row, "status", "") or "").strip().lower()
    tone, pill = status_pill(status)
    is_open = status == EDITABLE_STATUS
    urgency = str(getattr(row, "urgency", "") or "normal").strip().lower()
    parts = ["Supplies"]
    if urgency == "high":
        parts.append("ASAP")
    if is_open:
        parts.append(sent_label(getattr(row, "created_at", None), today))
    else:
        parts.append(
            _decided_text(
                status,
                _as_date(getattr(row, "status_changed_at", None))
                or _as_date(getattr(row, "updated_at", None)),
                decided_by,
            )
        )
    return {
        "kind": KIND_SUPPLY,
        "id": row.id,
        "title": str(getattr(row, "title", "") or "Supply request"),
        "sub": " · ".join(parts),
        "pill_tone": tone,
        "pill": pill,
        "status": status,
        # SupplyRequest.notes holds the manager's note (set on deny).
        "note": str(getattr(row, "notes", "") or "").strip(),
        "detail": str(getattr(row, "description", "") or "").strip(),
        "is_open": is_open,
        "can_edit": is_open,
        "description": str(getattr(row, "description", "") or ""),
        "urgency": urgency if urgency in URGENCY_LABELS else "normal",
        "sort_at": getattr(row, "created_at", None),
        "decided_at": getattr(row, "status_changed_at", None)
        or getattr(row, "updated_at", None),
    }


def _stamp(value: Any) -> float:
    return value.timestamp() if isinstance(value, datetime) else 0.0


def split_open_past(cards: Iterable[Mapping[str, Any]]) -> dict[str, list]:
    """Open (pending) newest first; Past newest decision first."""
    cards = list(cards)
    open_cards = sorted(
        (c for c in cards if c.get("is_open")),
        key=lambda c: -_stamp(c.get("sort_at")),
    )
    past_cards = sorted(
        (c for c in cards if not c.get("is_open")),
        key=lambda c: (-_stamp(c.get("decided_at")), -_stamp(c.get("sort_at"))),
    )
    return {"open": open_cards, "past": past_cards}


# ---------------------------------------------------------------------------
# Scheduled shifts a time-off request would miss
# ---------------------------------------------------------------------------

def timeoff_overlaps(my_days: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Working shifts in ``build_my_week()["days"]`` for the chosen dates.

    Each item: ``{"iso", "text"}`` with text like
    ``"Fri Oct 10 · 12 – 8 PM (Storefront)"``.
    """
    out: list[dict[str, Any]] = []
    for day in my_days:
        if day.get("state") != "work":
            continue
        when = day.get("date")
        label = weekday_month_day(when) if isinstance(when, date) else str(day.get("iso") or "")
        for shift in day.get("shifts") or []:
            time_text = str(shift.get("time_compact") or shift.get("time") or "Shift")
            place = str(shift.get("location_label") or "").strip()
            text = f"{label} · {time_text}"
            if place:
                text += f" ({place})"
            out.append({"iso": day.get("iso") or "", "text": text})
    return out


def overlap_summary(overlaps: Sequence[Mapping[str, Any]]) -> str:
    """Heading line for the warning; '' when nothing overlaps."""
    count = len(overlaps)
    if not count:
        return ""
    return f"You're scheduled for {_shifts_text(count)} on these dates."
