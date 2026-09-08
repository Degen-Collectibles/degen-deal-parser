"""Single source of truth for parsing free-text schedule shift labels.

Shift labels are hand-typed into the schedule grid ("10:30 AM - 6:30 PM",
"12-8", "10-2 / 3-7"). Four surfaces need the time math behind them:

  * ``team_admin_schedule``          - labor cost forecast on the grid
  * ``team_admin_employees_timecards`` - scheduled hours, variance, late/early pills
  * ``team_admin_employees``        - hourly cost in the pay-rates summary
  * ``team``                        - shift ordering on the employee dashboard

Each of those grew its own copy with a slightly different bare-number
heuristic, so the same label produced different hours and different windows
depending on which page you were looking at. Everything now routes through
here.

Bare-number convention (no AM/PM written on either side):

  * A start hour of 1-5 means afternoon ("3-7" is 3 PM, not 3 AM). 6-12 is
    taken as written, so a bare "12" is noon.
  * The end is resolved to whichever 12-hour reading lands soonest after the
    start, which keeps "9-5" at 8h and "12-8" at 8h rather than wrapping
    overnight into a 20-hour shift.
  * A genuine overnight shift needs explicit AM/PM ("10 PM - 6 AM"); a bare
    "10-6" is unavoidably ambiguous and reads as daytime.
"""

from __future__ import annotations

import re
from typing import Optional

NON_SHIFT_TOKENS = {"OFF", "SHOW", "REQUEST", "IF NEEDED", "STREAM"}

_TIME_RE = re.compile(
    r"^\s*(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ap>[ap](?:\.?m\.?)?)?\s*$",
    re.IGNORECASE,
)
_RANGE_SPLIT_RE = re.compile(r"\s*[/,&]\s*")
_RANGE_RE = re.compile(
    r"^\s*(?P<a>[0-9:.apm\s]+?)\s*[-–—]\s*(?P<b>[0-9:.apm\s]+?)\s*$",
    re.IGNORECASE,
)
_AMPM_SUFFIX_RE = re.compile(r"[ap]\.?m?\.?$", re.IGNORECASE)
_LEADING_TIME_RE = re.compile(
    r"^\s*(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ap>a|am|p|pm)?\b",
    re.IGNORECASE,
)

_DAY_MINUTES = 24 * 60
_HALF_DAY_MINUTES = 12 * 60


def parse_time_to_minutes(value: str) -> Optional[int]:
    """Minutes past midnight for one side of a range, honoring explicit AM/PM."""
    match = _TIME_RE.match(value or "")
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m") or 0)
    ampm = (match.group("ap") or "").lower().replace(".", "").replace("m", "")
    if minute > 59 or hour > 23:
        return None
    if ampm == "p" and hour < 12:
        hour += 12
    elif ampm == "a" and hour == 12:
        hour = 0
    return hour * 60 + minute


def _bump_bare_start(minutes: int) -> int:
    """Read a bare start hour of 1-5 as afternoon; leave 6-12 as written."""
    hour = minutes // 60
    if 1 <= hour <= 5:
        return minutes + _HALF_DAY_MINUTES
    return minutes


def _resolve_bare_end(start: int, end: int) -> int:
    """Pick the 12-hour reading of a bare end that lands soonest after start."""
    candidates = []
    for base in (end, end + _HALF_DAY_MINUTES):
        value = base
        while value <= start:
            value += _DAY_MINUTES
        candidates.append(value)
    return min(candidates)


def _resolve_range(
    start: int,
    end: int,
    *,
    start_has_ampm: bool,
    end_has_ampm: bool,
) -> tuple[int, int]:
    """Return (start, end) minutes; end may exceed 1440 for an overnight wrap."""
    if not start_has_ampm:
        bumped = _bump_bare_start(start)
        # "3-7am" reads as morning: keep the literal start when the afternoon
        # reading would force the explicit end to wrap into the next day.
        if end_has_ampm and bumped != start and start < end <= bumped:
            bumped = start
        start = bumped
    if not end_has_ampm:
        end = _resolve_bare_end(start, end)
    elif end <= start:
        end += _DAY_MINUTES
    return start, end


def parse_shift_ranges(label: str) -> list[tuple[int, int]]:
    """Return [(start_min, end_min), ...] for a shift label.

    End may exceed 1440 to indicate an overnight wrap. Returns [] for
    non-shift tokens or anything unparseable.
    """
    if not label:
        return []
    if label.strip().upper() in NON_SHIFT_TOKENS:
        return []
    out: list[tuple[int, int]] = []
    for part in _RANGE_SPLIT_RE.split(label):
        match = _RANGE_RE.match(part)
        if not match:
            continue
        start_raw = match.group("a").strip()
        end_raw = match.group("b").strip()
        start = parse_time_to_minutes(start_raw)
        end = parse_time_to_minutes(end_raw)
        if start is None or end is None:
            continue
        out.append(
            _resolve_range(
                start,
                end,
                start_has_ampm=bool(_AMPM_SUFFIX_RE.search(start_raw)),
                end_has_ampm=bool(_AMPM_SUFFIX_RE.search(end_raw)),
            )
        )
    return out


def shift_total_hours(ranges: list[tuple[int, int]]) -> float:
    return round(sum(max(0, end - start) for start, end in ranges) / 60.0, 2)


def parse_shift_hours(label: str) -> float:
    """Total hours in a label, or 0.0 if unparseable."""
    return shift_total_hours(parse_shift_ranges(label))


def parse_shift_start_minutes(label: str) -> Optional[int]:
    """Sort key for schedule labels, agreeing with parse_shift_ranges."""
    ranges = parse_shift_ranges(label)
    if ranges:
        return ranges[0][0] % _DAY_MINUTES
    match = _LEADING_TIME_RE.search((label or "").strip())
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m") or "0")
    if minute > 59 or hour > 23:
        return None
    ampm = (match.group("ap") or "").lower()
    if ampm.startswith("p"):
        if hour != 12:
            hour += 12
    elif ampm.startswith("a"):
        if hour == 12:
            hour = 0
    else:
        return _bump_bare_start(hour * 60 + minute)
    if hour > 23:
        return None
    return hour * 60 + minute
