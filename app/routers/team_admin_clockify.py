"""/team/admin/clockify - Clockify setup and employee mapping tools."""
from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..clockify import (
    ClockifyClient,
    ClockifyApiError,
    ClockifyConfigError,
    ClockifyWeekSummary,
    build_week_summary,
    clockify_week_bounds,
    clockify_client_from_settings,
    clockify_is_configured,
    format_hours,
)
from ..auth import is_draft_user
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, User, utcnow
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

router = APIRouter()


CLOCKIFY_NAME_OVERRIDES = {
    # Store nickname differences that are safe enough to auto-link.
    "alex": ("mod alex",),
    "dat david": ("david",),
}
_CLOCKIFY_WEEK_CACHE: dict[tuple[str, date], tuple[float, ClockifyWeekSummary]] = {}
_CLOCKIFY_WEEK_CACHE_TTL_SECONDS = 60.0
_BREAK_KEYWORDS = ("break", "lunch", "meal", "rest")


def _mask_id(value: str) -> str:
    value = (value or "").strip()
    if len(value) <= 8:
        return value or "-"
    return f"{value[:4]}...{value[-4:]}"


def _clockify_user_id(row: dict[str, Any]) -> str:
    return str(row.get("id") or "").strip()


def _clockify_user_name(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return name
    email = str(row.get("email") or "").strip()
    if email:
        return email
    return _mask_id(_clockify_user_id(row))


def _clockify_user_email(row: dict[str, Any]) -> str:
    return str(row.get("email") or "").strip()


def _mask_email(email: str) -> str:
    email = (email or "").strip()
    if "@" not in email:
        return f"{email[:3]}***" if email else ""
    local, domain = email.split("@", 1)
    return f"{local[:3]}***@{domain}" if domain else f"{local[:3]}***"


def _clockify_display_name(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return _mask_email(name) if "@" in name else name
    email = _clockify_user_email(row)
    if email:
        return _mask_email(email)
    return _mask_id(_clockify_user_id(row))


def _masked_clockify_user(row: dict[str, Any]) -> dict[str, Any]:
    masked = dict(row)
    masked["email"] = _mask_email(_clockify_user_email(row))
    name = str(masked.get("name") or "").strip()
    if "@" in name:
        masked["name"] = _mask_email(name)
    return masked


def _is_matchable_team_user(user: Optional[User]) -> bool:
    return bool(user and (user.is_active or is_draft_user(user)))


def _normalize_match_name(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _clockify_match_keys(row: dict[str, Any]) -> list[str]:
    raw_name = _clockify_user_name(row)
    full_key = _normalize_match_name(raw_name)
    keys: list[str] = []
    overrides = CLOCKIFY_NAME_OVERRIDES.get(full_key, ())
    for override in overrides:
        key = _normalize_match_name(override)
        if key and key not in keys:
            keys.append(key)
    if overrides:
        return keys
    for inner in re.findall(r"\(([^)]+)\)", raw_name):
        key = _normalize_match_name(inner)
        if key and key not in keys:
            keys.append(key)
    if full_key and full_key not in keys:
        keys.append(full_key)
    return keys


def _safe_decrypt_name(profile: Optional[EmployeeProfile]) -> str:
    if profile is None or not profile.legal_name_enc:
        return ""
    try:
        return (decrypt_pii(profile.legal_name_enc) or "").strip()
    except ValueError:
        return ""


def _employee_match_keys(user: User, profile: Optional[EmployeeProfile]) -> list[str]:
    values = [
        user.display_name,
        _safe_decrypt_name(profile),
    ]
    if not is_draft_user(user):
        values.append(user.username)
    keys: list[str] = []
    for value in values:
        key = _normalize_match_name(value or "")
        if key and key not in keys:
            keys.append(key)
    return keys


def _employee_clockify_counts(employee_rows: list[dict[str, Any]]) -> dict[str, int]:
    matchable_profiles = [row.get("profile") for row in employee_rows if row.get("profile")]
    mapped = sum(1 for profile in matchable_profiles if profile.clockify_user_id)
    with_email = sum(1 for profile in matchable_profiles if profile.email_ciphertext)
    return {
        "active_profiles": len(matchable_profiles),
        "mapped": mapped,
        "unmapped": max(0, len(matchable_profiles) - mapped),
        "with_email": with_email,
    }


def _employee_rows(session: Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    result = session.exec(
        select(User, EmployeeProfile)
        .join(EmployeeProfile, EmployeeProfile.user_id == User.id, isouter=True)
        .where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
        .order_by(User.display_name, User.username)
    ).all()
    for employee, profile in result:
        out.append(
            {
                "user": employee,
                "profile": profile,
                "clockify_user_id": (profile.clockify_user_id or "").strip()
                if profile
                else "",
                "is_draft": is_draft_user(employee),
            }
        )
    return out


def _cached_user_week_summary(
    client: ClockifyClient,
    clockify_user_id: str,
    *,
    today: date,
    settings=None,
) -> ClockifyWeekSummary:
    week_start_local, _week_end_local = clockify_week_bounds(today, settings=settings)
    key = (clockify_user_id, week_start_local.date())
    now = time.time()
    cached = _CLOCKIFY_WEEK_CACHE.get(key)
    if cached is not None:
        cached_at, summary = cached
        if now - cached_at < _CLOCKIFY_WEEK_CACHE_TTL_SECONDS:
            return summary
    summary = client.user_week_summary(
        clockify_user_id,
        today=today,
        settings=settings,
    )
    _CLOCKIFY_WEEK_CACHE[key] = (now, summary)
    return summary


def _employee_link_map(employee_rows: list[dict[str, Any]]) -> dict[str, User]:
    linked: dict[str, User] = {}
    for row in employee_rows:
        clockify_id = (row.get("clockify_user_id") or "").strip()
        user = row.get("user")
        if clockify_id and isinstance(user, User):
            linked[clockify_id] = user
    return linked


def _clockify_users_by_email(clockify_users: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_email: dict[str, dict[str, Any]] = {}
    for row in clockify_users:
        email = str(row.get("email") or "").strip().lower()
        user_id = str(row.get("id") or "").strip()
        if email and user_id:
            by_email.setdefault(email, row)
    return by_email


def _clockify_users_by_name(
    clockify_users: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    by_name: dict[str, dict[str, Any]] = {}
    ambiguous: set[str] = set()
    for row in clockify_users:
        if not _clockify_user_id(row):
            continue
        for key in _clockify_match_keys(row):
            if key in ambiguous:
                continue
            if key in by_name and _clockify_user_id(by_name[key]) != _clockify_user_id(row):
                by_name.pop(key, None)
                ambiguous.add(key)
                continue
            by_name[key] = row
    return by_name, ambiguous


def _find_clockify_name_match(
    user: User,
    profile: Optional[EmployeeProfile],
    by_name: dict[str, dict[str, Any]],
    ambiguous_names: set[str],
) -> tuple[Optional[dict[str, Any]], bool]:
    matches: dict[str, dict[str, Any]] = {}
    saw_ambiguous = False
    for key in _employee_match_keys(user, profile):
        if key in ambiguous_names:
            saw_ambiguous = True
            continue
        match = by_name.get(key)
        if match is not None:
            matches[_clockify_user_id(match)] = match
    if len(matches) == 1:
        return next(iter(matches.values())), False
    if len(matches) > 1:
        return None, True
    return None, saw_ambiguous


def build_clockify_roster_preview(
    clockify_users: list[dict[str, Any]],
    *,
    client: Optional[ClockifyClient] = None,
    settings=None,
    include_hours: bool = False,
    today: Optional[date] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Build admin-safe rows for Clockify people and optional hour previews."""
    rows: list[dict[str, Any]] = []
    today = today or date.today()
    for row in clockify_users[: max(0, limit)]:
        clockify_id = _clockify_user_id(row)
        preview = {
            "id": clockify_id,
            "id_masked": _mask_id(clockify_id),
            "name": _clockify_display_name(row),
            "email": _mask_email(_clockify_user_email(row)),
            "status": str(row.get("status") or "").strip() or "-",
            "raw": _masked_clockify_user(row),
            "has_data": False,
            "hours_label": "-",
            "entry_count": 0,
            "running_count": 0,
            "data_error": "",
        }
        if include_hours and client is not None and clockify_id:
            try:
                summary = _cached_user_week_summary(
                    client,
                    clockify_id,
                    today=today,
                    settings=settings,
                )
                preview["has_data"] = bool(summary.entries or summary.total_seconds)
                preview["hours_label"] = format_hours(summary.total_seconds)
                preview["entry_count"] = len(summary.entries)
                preview["running_count"] = summary.running_count
            except (ClockifyApiError, ClockifyConfigError) as exc:
                preview["data_error"] = str(exc)
        rows.append(preview)
    return rows


def _clockify_day_bounds(
    today: Optional[date] = None,
    *,
    settings=None,
) -> tuple[datetime, datetime]:
    day = today or date.today()
    week_start_local, _week_end_local = clockify_week_bounds(day, settings=settings)
    day_offset = (day - week_start_local.date()).days
    start_local = week_start_local + timedelta(days=day_offset)
    return start_local, start_local + timedelta(days=1)


def _format_clockify_time(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.strftime("%I:%M %p").lstrip("0")


def _clockify_entry_search_text(entry: Any) -> str:
    pieces: list[str] = []
    if hasattr(entry, "description"):
        pieces.append(str(getattr(entry, "description") or ""))
    if isinstance(entry, dict):
        for key in ("description", "projectName", "taskName"):
            pieces.append(str(entry.get(key) or ""))
        for key in ("project", "task"):
            nested = entry.get(key)
            if isinstance(nested, dict):
                pieces.append(str(nested.get("name") or ""))
    return " ".join(piece for piece in pieces if piece).lower()


def _clockify_entry_is_break(entry: Any) -> bool:
    text = _clockify_entry_search_text(entry)
    return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in _BREAK_KEYWORDS)


def build_clockify_live_status(
    session: Session,
    client: ClockifyClient,
    *,
    settings=None,
    today: Optional[date] = None,
    now: Optional[datetime] = None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Build current Clockify timer status for mapped portal employees."""
    day = today or date.today()
    start_local, end_local = _clockify_day_bounds(day, settings=settings)
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    rows: list[dict[str, Any]] = []
    employees = employee_rows if employee_rows is not None else _employee_rows(session)
    eligible_rows = [row for row in employees if row.get("profile")]
    mapped_rows = [
        row for row in eligible_rows if (row.get("clockify_user_id") or "").strip()
    ]

    for row in mapped_rows:
        employee = row.get("user")
        clockify_user_id = (row.get("clockify_user_id") or "").strip()
        display_name = (
            getattr(employee, "display_name", None)
            or getattr(employee, "username", None)
            or "Employee"
        )
        base = {
            "employee": employee,
            "employee_name": display_name,
            "clockify_user_id": clockify_user_id,
            "clockify_user_id_masked": _mask_id(clockify_user_id),
            "status": "Not clocked in",
            "status_key": "not_clocked_in",
            "status_color": "var(--lx-muted)",
            "current_start_label": "-",
            "running_duration_label": "-",
            "today_total_label": "0m",
            "break_label": "No time today",
            "break_color": "var(--lx-muted)",
            "entry_count": 0,
            "error": "",
            "rank": 4,
        }
        try:
            raw_entries = client.get_user_time_entries(
                clockify_user_id,
                start_utc=start_local.astimezone(timezone.utc),
                end_utc=end_local.astimezone(timezone.utc),
            )
            summary = build_week_summary(
                raw_entries,
                week_start_local=start_local,
                week_end_local=end_local,
                settings=settings,
                now=now_utc,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            base.update(
                {
                    "status": "Clockify error",
                    "status_key": "error",
                    "status_color": "#fca5a5",
                    "break_label": "-",
                    "error": str(exc),
                    "rank": 5,
                }
            )
            rows.append(base)
            continue

        running_entries = [entry for entry in summary.entries if entry.running]
        running_entry = running_entries[-1] if running_entries else None
        break_entries = [entry for entry in summary.entries if _clockify_entry_is_break(entry)]
        break_taken = any(entry.duration_seconds > 0 for entry in break_entries)
        running_is_break = bool(running_entry and _clockify_entry_is_break(running_entry))
        base["today_total_label"] = format_hours(summary.total_seconds)
        base["entry_count"] = len(summary.entries)

        if running_entry is not None:
            base["current_start_label"] = _format_clockify_time(running_entry.start_local)
            base["running_duration_label"] = format_hours(running_entry.duration_seconds)
            if running_is_break:
                base["status"] = "On break"
                base["status_key"] = "on_break"
                base["status_color"] = "#facc15"
                base["break_label"] = "On break now"
                base["break_color"] = "#facc15"
                base["rank"] = 0
            else:
                base["status"] = "Clocked in"
                base["status_key"] = "clocked_in"
                base["status_color"] = "#86efac"
                base["break_label"] = "Taken" if break_taken else "No break yet"
                base["break_color"] = "#86efac" if break_taken else "var(--lx-muted)"
                base["rank"] = 1
        elif summary.total_seconds > 0:
            base["status"] = "Clocked out"
            base["status_key"] = "clocked_out"
            base["status_color"] = "var(--lx-text)"
            base["break_label"] = "Taken" if break_taken else "No break"
            base["break_color"] = "#86efac" if break_taken else "var(--lx-muted)"
            base["rank"] = 3

        rows.append(base)

    rows.sort(key=lambda item: (item["rank"], str(item["employee_name"]).lower()))
    generated_at = now_utc.astimezone(start_local.tzinfo)
    timezone_name = str(getattr(start_local.tzinfo, "key", None) or start_local.tzinfo)
    return {
        "rows": rows,
        "mapped_count": len(mapped_rows),
        "unmapped_count": max(0, len(eligible_rows) - len(mapped_rows)),
        "timezone_name": timezone_name,
        "date_label": day.strftime("%b %d, %Y").replace(" 0", " "),
        "generated_at_label": _format_clockify_time(generated_at),
    }


def set_employee_clockify_user_id(
    session: Session,
    *,
    current_user: User,
    user_id: int,
    clockify_user_id: str,
    ip_address: Optional[str] = None,
) -> tuple[bool, str]:
    employee = session.get(User, user_id)
    if employee is None:
        raise ValueError("employee_not_found")

    clockify_user_id = (clockify_user_id or "").strip()
    if clockify_user_id:
        existing = session.exec(
            select(EmployeeProfile).where(
                EmployeeProfile.clockify_user_id == clockify_user_id,
                EmployeeProfile.user_id != user_id,
            )
        ).first()
        if existing is not None:
            other_user = session.get(User, existing.user_id)
            other_name = (
                other_user.display_name or other_user.username
                if other_user is not None
                else f"employee {existing.user_id}"
            )
            return False, f"That Clockify user is already linked to {other_name}."

    profile = session.get(EmployeeProfile, user_id)
    if profile is None:
        profile = EmployeeProfile(user_id=user_id)
        session.add(profile)
        session.flush()

    old_value = (profile.clockify_user_id or "").strip()
    if old_value == clockify_user_id:
        return True, "No change."

    profile.clockify_user_id = clockify_user_id or None
    profile.updated_at = utcnow()
    session.add(profile)
    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            target_user_id=user_id,
            action="admin.clockify.manual_link",
            resource_key="admin.employees.edit",
            details_json=json.dumps(
                {
                    "old_clockify_user_id": old_value,
                    "new_clockify_user_id": clockify_user_id,
                },
                sort_keys=True,
            ),
            ip_address=ip_address,
        )
    )
    session.commit()
    if clockify_user_id:
        return True, "Clockify user linked."
    return True, "Clockify user unlinked."


def sync_clockify_user_ids_by_email(
    session: Session,
    *,
    current_user: User,
    clockify_users: list[dict[str, Any]],
    ip_address: Optional[str] = None,
) -> dict[str, int]:
    """Link local employee profiles to Clockify ids by email or safe name match.

    Existing conflicting Clockify ids are left untouched. Draft employees are
    included because they are inactive until onboarding. Counts are audited;
    raw email addresses and employee names are never written to AuditLog.
    """
    by_email = _clockify_users_by_email(clockify_users)
    by_name, ambiguous_names = _clockify_users_by_name(clockify_users)
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    linked_clockify_ids = {
        (profile.clockify_user_id or "").strip(): profile.user_id
        for profile in profiles
        if (profile.clockify_user_id or "").strip()
    }
    now = utcnow()
    counts = {
        "checked": 0,
        "mapped": 0,
        "email_matched": 0,
        "name_matched": 0,
        "already_mapped": 0,
        "conflicts": 0,
        "missing_email": 0,
        "email_decrypt_failed": 0,
        "no_clockify_match": 0,
        "ambiguous_name_match": 0,
    }

    for profile in profiles:
        user = users.get(profile.user_id)
        if not _is_matchable_team_user(user):
            continue
        counts["checked"] += 1
        match: Optional[dict[str, Any]] = None
        match_method = ""
        if not profile.email_ciphertext:
            counts["missing_email"] += 1
        else:
            try:
                email = (decrypt_pii(profile.email_ciphertext) or "").strip().lower()
            except ValueError:
                counts["email_decrypt_failed"] += 1
                email = ""
            if not email:
                counts["missing_email"] += 1
            else:
                match = by_email.get(email)
                if match is not None:
                    match_method = "email"

        if match is None:
            match, ambiguous = _find_clockify_name_match(
                user,
                profile,
                by_name,
                ambiguous_names,
            )
            if ambiguous:
                counts["ambiguous_name_match"] += 1
            if match is not None:
                match_method = "name"

        if match is None:
            counts["no_clockify_match"] += 1
            continue
        match_id = str(match.get("id") or "").strip()
        if not match_id:
            counts["no_clockify_match"] += 1
            continue
        existing = (profile.clockify_user_id or "").strip()
        if existing == match_id:
            counts["already_mapped"] += 1
            continue
        if existing and existing != match_id:
            counts["conflicts"] += 1
            continue
        linked_user_id = linked_clockify_ids.get(match_id)
        if linked_user_id is not None and linked_user_id != profile.user_id:
            counts["conflicts"] += 1
            continue
        profile.clockify_user_id = match_id
        profile.updated_at = now
        session.add(profile)
        linked_clockify_ids[match_id] = profile.user_id
        counts["mapped"] += 1
        if match_method == "email":
            counts["email_matched"] += 1
        elif match_method == "name":
            counts["name_matched"] += 1

    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            action="admin.clockify.sync_users",
            resource_key="admin.employees.edit",
            details_json=json.dumps(counts, sort_keys=True),
            ip_address=ip_address,
        )
    )
    session.commit()
    return counts


@router.get("/team/admin/clockify", response_class=HTMLResponse)
def admin_clockify_page(
    request: Request,
    flash: Optional[str] = None,
    error: Optional[str] = None,
    include_hours: str = Query(default="0"),
    live_status: str = Query(default="0"),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    workspace = None
    status_error = None
    clockify_users: list[dict[str, Any]] = []
    clockify_users_display: list[dict[str, Any]] = []
    roster_preview: list[dict[str, Any]] = []
    clockify_user_map: dict[str, dict[str, Any]] = {}
    preview_capped = False
    client: Optional[ClockifyClient] = None
    include_hour_preview = include_hours not in ("0", "false", "no", "off")
    load_live_status = live_status not in ("0", "false", "no", "off")
    if configured:
        try:
            client = clockify_client_from_settings(settings)
            workspace = client.workspace_info()
            clockify_users = client.list_workspace_users(status="ALL")
            clockify_users_display = [_masked_clockify_user(row) for row in clockify_users]
            clockify_user_map = {
                _clockify_user_id(row): row
                for row in clockify_users_display
                if _clockify_user_id(row)
            }
            roster_preview = build_clockify_roster_preview(
                clockify_users,
                client=client,
                settings=settings,
                include_hours=include_hour_preview,
            )
            preview_capped = len(clockify_users) > len(roster_preview)
        except (ClockifyApiError, ClockifyConfigError) as exc:
            status_error = str(exc)
    employees = _employee_rows(session)
    linked_by_clockify = _employee_link_map(employees)
    counts = _employee_clockify_counts(employees)
    live = None
    if configured and client is not None and load_live_status:
        live = build_clockify_live_status(
            session,
            client,
            settings=settings,
            employee_rows=employees,
        )
    return templates.TemplateResponse(
        request,
        "team/admin/clockify.html",
        {
            "request": request,
            "title": "Clockify",
            "current_user": user,
            "configured": configured,
            "workspace": workspace,
            "workspace_id_masked": _mask_id(settings.clockify_workspace_id),
            "status_error": status_error,
            "clockify_users": clockify_users_display,
            "clockify_user_map": clockify_user_map,
            "roster_preview": roster_preview,
            "preview_capped": preview_capped,
            "include_hours": include_hour_preview,
            "live_status": load_live_status,
            "live": live,
            "employees": employees,
            "linked_by_clockify": linked_by_clockify,
            "counts": counts,
            "can_sync": user.role == "admin",
            "mask_id": _mask_id,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/clockify/sync-users",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_sync_users(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    settings = get_settings()
    if not clockify_is_configured(settings):
        return RedirectResponse(
            "/team/admin/clockify?error=CLOCKIFY_API_KEY+and+CLOCKIFY_WORKSPACE_ID+are+required.",
            status_code=303,
        )
    try:
        clockify_users = clockify_client_from_settings(settings).list_workspace_users(
            status="ALL"
        )
        counts = sync_clockify_user_ids_by_email(
            session,
            current_user=user,
            clockify_users=clockify_users,
            ip_address=(request.client.host if request.client else None),
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": str(exc)}),
            status_code=303,
        )
    flash = (
        f"Mapped {counts['mapped']} employee(s) "
        f"({counts['email_matched']} by email, {counts['name_matched']} by name). "
        f"{counts['already_mapped']} already linked, "
        f"{counts['conflicts']} conflict(s), "
        f"{counts['no_clockify_match']} without a Clockify match."
    )
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({"flash": flash}),
        status_code=303,
    )


@router.post(
    "/team/admin/clockify/manual-link",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_manual_link(
    request: Request,
    user_id: int = Form(...),
    clockify_user_id: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    try:
        ok, message = set_employee_clockify_user_id(
            session,
            current_user=user,
            user_id=user_id,
            clockify_user_id=clockify_user_id,
            ip_address=(request.client.host if request.client else None),
        )
    except ValueError:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": "Employee not found."}),
            status_code=303,
        )
    key = "flash" if ok else "error"
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({key: message}),
        status_code=303,
    )
