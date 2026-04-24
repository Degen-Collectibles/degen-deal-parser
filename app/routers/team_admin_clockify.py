"""/team/admin/clockify - Clockify setup and employee mapping tools."""
from __future__ import annotations

import json
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..clockify import (
    ClockifyApiError,
    ClockifyConfigError,
    clockify_client_from_settings,
    clockify_is_configured,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, User, utcnow
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

router = APIRouter()


def _mask_id(value: str) -> str:
    value = (value or "").strip()
    if len(value) <= 8:
        return value or "-"
    return f"{value[:4]}...{value[-4:]}"


def _employee_clockify_counts(session: Session) -> dict[str, int]:
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    active_profiles = [
        profile
        for profile in profiles
        if (users.get(profile.user_id) is not None and users[profile.user_id].is_active)
    ]
    mapped = sum(1 for profile in active_profiles if profile.clockify_user_id)
    with_email = sum(1 for profile in active_profiles if profile.email_ciphertext)
    return {
        "active_profiles": len(active_profiles),
        "mapped": mapped,
        "unmapped": max(0, len(active_profiles) - mapped),
        "with_email": with_email,
    }


def _clockify_users_by_email(clockify_users: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_email: dict[str, dict[str, Any]] = {}
    for row in clockify_users:
        email = str(row.get("email") or "").strip().lower()
        user_id = str(row.get("id") or "").strip()
        if email and user_id:
            by_email.setdefault(email, row)
    return by_email


def sync_clockify_user_ids_by_email(
    session: Session,
    *,
    current_user: User,
    clockify_users: list[dict[str, Any]],
    ip_address: Optional[str] = None,
) -> dict[str, int]:
    """Link local employee profiles to Clockify ids by exact email match.

    Existing conflicting Clockify ids are left untouched. Counts are audited;
    raw email addresses are never written to AuditLog or returned to the UI.
    """
    by_email = _clockify_users_by_email(clockify_users)
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    now = utcnow()
    counts = {
        "checked": 0,
        "mapped": 0,
        "already_mapped": 0,
        "conflicts": 0,
        "missing_email": 0,
        "email_decrypt_failed": 0,
        "no_clockify_match": 0,
    }

    for profile in profiles:
        user = users.get(profile.user_id)
        if user is None or not user.is_active:
            continue
        counts["checked"] += 1
        if not profile.email_ciphertext:
            counts["missing_email"] += 1
            continue
        try:
            email = (decrypt_pii(profile.email_ciphertext) or "").strip().lower()
        except ValueError:
            counts["email_decrypt_failed"] += 1
            continue
        if not email:
            counts["missing_email"] += 1
            continue
        match = by_email.get(email)
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
        profile.clockify_user_id = match_id
        profile.updated_at = now
        session.add(profile)
        counts["mapped"] += 1

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
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    workspace = None
    status_error = None
    if configured:
        try:
            workspace = clockify_client_from_settings(settings).workspace_info()
        except (ClockifyApiError, ClockifyConfigError) as exc:
            status_error = str(exc)
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
            "counts": _employee_clockify_counts(session),
            "can_sync": user.role == "admin",
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
        f"Mapped {counts['mapped']} employee(s). "
        f"{counts['already_mapped']} already linked, "
        f"{counts['conflicts']} conflict(s), "
        f"{counts['no_clockify_match']} without a Clockify email match."
    )
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({"flash": flash}),
        status_code=303,
    )
