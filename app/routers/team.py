"""
/team/* — employee-facing portal (Wave 3).

Scope:
  * Auth flows: login / logout / invite-accept / password reset (public).
  * Authenticated surface: dashboard (widget-driven), profile (self-edit
    non-critical PII), policies (placeholders + ack via AuditLog), hours
    (Clockify stub), schedule (placeholder), supply (submit + list own).

Admin employee-management pages live under /team/admin/* (Wave 2 + Wave 4).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlencode, urlparse

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from .. import permissions as perms
from ..auth import (
    BadCurrentPasswordError,
    LoginRateLimitedError,
    WeakPasswordError,
    authenticate_user,
    change_user_password,
    consume_invite_token,
    consume_password_reset_token,
    generate_password_reset_token,
    has_permission,
    _find_token_row,
    _token_hmac_key,
    validate_password_strength,
)
from ..team.clockify import (
    ClockifyApiError,
    ClockifyConfigError,
    build_week_summary,
    clockify_client_from_settings,
    clockify_is_configured,
    clockify_week_bounds,
    format_hours,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf, rotate_token
from ..db import get_session
from ..models import (
    AuditLog,
    EmployeeProfile,
    InviteToken,
    PasswordResetToken,
    SCHEDULE_CALENDAR_PACKING,
    SCHEDULE_CALENDAR_STOREFRONT,
    SHIFT_KIND_ALL,
    SHIFT_KIND_BLANK,
    SHIFT_KIND_OFF,
    SHIFT_KIND_REQUEST,
    SHIFT_KIND_WORK,
    ScheduleDayNote,
    ShiftEntry,
    SupplyRequest,
    TeamAnnouncement,
    TeamPolicy,
    TimecardApproval,
    TimeOffRequest,
    User,
    utcnow,
)
from ..team.pii import PIIDecryptError, decrypt_pii, encrypt_pii
from ..team.sms_consent import consent_context, record_consent
from ..team import home as home_view
from ..team import inbox as inbox_view
from ..team import inbox_store
from ..team.inbox import TEAM_DOCUMENTS  # noqa: F401 -- re-exported for callers/tests
from ..team import schedule_view
from ..team.shift_labels import parse_shift_start_minutes
from ..rate_limit import rate_limited_or_429
from ..shared import app_home_for_role, templates
from ..team.email import email_address_fingerprint, mask_email_address, send_email
from ..team.team_notifications import (
    EMPLOYEE_NOTIFICATION_ACTION,
    notify_manager_admins,
)
from ..team.request_alerts import (
    send_help_request_alert,
    send_password_reset_manager_request_alert,
    send_supply_request_alert,
)

router = APIRouter()


MANAGER_ADMIN_DASHBOARD_ROLES = {"admin", "manager"}


LEGACY_POLICIES: tuple[dict, ...] = (
    {
        "id": "code-of-conduct",
        "title": "Code of Conduct",
        "version": "v1",
        "kind": "policy",
        "requires_ack": True,
        "body_md": (
            "Treat teammates, customers, and contractors with respect. "
            "Report safety or conduct concerns to Jeffrey directly. "
            "No harassment, theft, or discrimination will be tolerated."
        ),
    },
    {
        "id": "safety-handling",
        "title": "Safety & Handling",
        "version": "v1",
        "kind": "policy",
        "requires_ack": True,
        "body_md": (
            "Wash hands before handling cards. Sleeve slabs before storage. "
            "Never leave inventory unattended in common areas. "
            "Power tools require PPE; stop and ask if unsure."
        ),
    },
)
LEGACY_POLICY_BY_ID = {p["id"]: p for p in LEGACY_POLICIES}


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def _portal_or_404() -> None:
    if not get_settings().employee_portal_enabled:
        raise HTTPException(status_code=404)


def _require_employee(
    request: Request,
    session: Session,
    *,
    resource_key: Optional[str] = None,
) -> Tuple[Optional[Response], Optional[User]]:
    """Portal on + session present + optional resource check.

    Access is governed entirely by `has_permission` against the matrix; any
    role (employee, manager, reviewer, admin) that holds the required
    resource flag may view the page. Anonymous users are redirected to login.
    """
    _portal_or_404()
    user: Optional[User] = getattr(request.state, "current_user", None)
    if user is None:
        return RedirectResponse("/team/login", status_code=303), None
    if resource_key is not None and not has_permission(session, user, resource_key):
        return HTMLResponse(
            "You do not have permission to view this page.", status_code=403
        ), None
    return None, user


# ---------------------------------------------------------------------------
# Public auth flows
# ---------------------------------------------------------------------------

def _safe_next(value: Optional[str]) -> str:
    """Only forward local paths to prevent open-redirects through `next`."""
    value = (value or "").strip()
    if not value:
        return ""
    decoded = unquote(value).strip()
    if decoded.startswith("\\"):
        return ""
    parsed = urlparse(decoded)
    if parsed.netloc or parsed.scheme:
        return ""
    if decoded.startswith("/.") or decoded.startswith("/%2e"):
        return ""
    if decoded.startswith("/") and not (
        decoded.startswith("//") or (len(decoded) > 1 and decoded[1] == "\\")
    ):
        return decoded
    return ""


def _password_changed_session_value(user: User) -> Optional[str]:
    changed_at = getattr(user, "password_changed_at", None)
    return changed_at.isoformat() if changed_at is not None else None


def _session_invalidated_session_value(user: User) -> Optional[str]:
    invalidated_at = getattr(user, "session_invalidated_at", None)
    return invalidated_at.isoformat() if invalidated_at is not None else None


def _configured_public_base_url() -> str:
    configured = (get_settings().public_base_url or "").strip().rstrip("/")
    return configured


def _public_base_url(request: Request) -> str:
    return _configured_public_base_url()


def _password_reset_url(request: Request, raw_token: str) -> str:
    return f"{_public_base_url(request)}/team/password/reset/{raw_token}"


def _password_reset_email_subject() -> str:
    return "Reset your Degen Team password"


def _password_reset_email_body(user: User, reset_url: str) -> str:
    display_name = (user.display_name or user.username or "there").strip()
    return (
        f"Hi {display_name},\n\n"
        "Use this link to reset your Degen Team password:\n"
        f"{reset_url}\n\n"
        "This link expires in 60 minutes. If you did not request a password reset, "
        "you can ignore this email.\n"
    )


def _email_provider_can_deliver() -> bool:
    provider = (getattr(get_settings(), "password_reset_email_provider", "dry_run") or "dry_run").strip().lower()
    return provider not in {
        "",
        "dryrun",
        "dry_run",
        "log",
        "console",
        "disabled",
        "off",
        "none",
    }


def _find_password_reset_user(session: Session, identifier: str) -> Optional[User]:
    probe = (identifier or "").strip()
    if not probe:
        return None
    normalized = probe.lower()
    user = session.exec(
        select(User).where(func.lower(User.username) == normalized)
    ).first()
    if user is not None and user.is_active:
        return user
    if "@" in normalized:
        from ..team.pii import email_lookup_hash

        digest = email_lookup_hash(normalized)
        profile = session.exec(
            select(EmployeeProfile).where(EmployeeProfile.email_lookup_hash == digest)
        ).first()
        if profile is not None:
            user = session.get(User, profile.user_id)
            if user is not None and user.is_active:
                return user
    return None


def _password_reset_identifier_hash(identifier: str) -> str:
    probe = (identifier or "").strip().lower()
    if not probe:
        return ""
    return hmac.new(
        _token_hmac_key(),
        probe.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _queue_password_reset_request(
    session: Session,
    *,
    request: Request,
    user: User,
    probe_hash: str,
    reason: str,
) -> AuditLog:
    row = AuditLog(
        target_user_id=user.id,
        action="password.reset_manager_request",
        resource_key="admin.employees.reset_password",
        details_json=json.dumps(
            {
                "source": "http_forgot",
                "identifier_hash": probe_hash,
                "reason": reason,
            },
            sort_keys=True,
        ),
        ip_address=(request.client.host if request.client else None),
    )
    session.add(row)
    return row


def _try_send_password_reset_email(
    session: Session,
    *,
    request: Request,
    user: User,
    probe_hash: str,
) -> bool:
    if not _email_provider_can_deliver():
        return False
    profile = session.get(EmployeeProfile, user.id)
    if profile is None or not profile.email_ciphertext:
        return False
    try:
        email_plain = decrypt_pii(profile.email_ciphertext) or ""
    except (PIIDecryptError, ValueError):
        return False
    to_email = email_plain.strip().lower()
    if "@" not in to_email or to_email.startswith("@") or to_email.endswith("@"):
        return False

    raw_token = generate_password_reset_token(
        session,
        user_id=user.id,
        issued_by_user_id=user.id,
    )
    reset_url = _password_reset_url(request, raw_token)
    result = send_email(
        to_email=to_email,
        subject=_password_reset_email_subject(),
        body=_password_reset_email_body(user, reset_url),
        settings=get_settings(),
    )
    token_revoked = False
    if not (result.success and not result.dry_run):
        token_row = _find_token_row(session, PasswordResetToken, raw_token)
        if token_row is not None and token_row.used_at is None:
            token_row.used_at = utcnow()
            session.add(token_row)
            token_revoked = True
    details = {
        "provider": result.provider,
        "status": result.status,
        "dry_run": result.dry_run,
        "success": result.success and not result.dry_run,
        "token_revoked": token_revoked,
        "email": mask_email_address(to_email),
        "email_fingerprint": email_address_fingerprint(to_email),
        "identifier_hash": probe_hash,
    }
    if result.message_id:
        details["message_id"] = result.message_id
    if result.error:
        details["error"] = result.error[:240]
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action=(
                "password.reset_email_sent"
                if result.success and not result.dry_run
                else "password.reset_email_failed"
            ),
            details_json=json.dumps(details, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    if result.success and not result.dry_run:
        return True
    return False


@router.get("/team/login", response_class=HTMLResponse)
def team_login_page(
    request: Request,
    next: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
):
    _portal_or_404()
    from ..shared import app_home_for_role
    next_url = _safe_next(next)
    user = getattr(request.state, "current_user", None)
    if user is not None:
        if next_url:
            return RedirectResponse(next_url, status_code=303)
        return RedirectResponse(app_home_for_role(user.role), status_code=303)
    return templates.TemplateResponse(
        request,
        "team/login.html",
        {
            "request": request,
            "title": "Team Sign In",
            "error": error,
            "flash": flash,
            "next_url": next_url,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/login")
async def team_login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(default=""),
    next: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    from ..shared import app_home_for_role
    from urllib.parse import urlencode as _urlencode
    ip = request.client.host if request.client else None
    next_url = _safe_next(next)
    next_qs = f"&next={_urlencode({'next': next_url})[5:]}" if next_url else ""

    if limited := rate_limited_or_429(
        request, key_prefix="team:login", max_requests=5, window_seconds=900.0
    ):
        session.add(
            AuditLog(
                action="login.rate_limited",
                details_json=json.dumps({"ip": ip}),
                ip_address=ip,
            )
        )
        session.commit()
        return limited
    # CSRF is enforced manually here so we can also render the login form
    # with a fresh token on a failure without breaking the flow.
    from ..csrf import verify_token

    if not verify_token(request, csrf_token):
        return RedirectResponse(
            f"/team/login?error=Session+expired.+Please+try+again.{next_qs}",
            status_code=303,
        )

    try:
        user = authenticate_user(
            session, username, password, request=request, ip_address=ip
        )
    except LoginRateLimitedError as exc:
        return exc.response
    if not user:
        return RedirectResponse(
            f"/team/login?error=Invalid+username+or+password{next_qs}",
            status_code=303,
        )

    request.session["user_id"] = user.id
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)  # m1 — bind a fresh CSRF to the authenticated session
    if next_url:
        return RedirectResponse(next_url, status_code=303)
    return RedirectResponse(app_home_for_role(user.role), status_code=303)


@router.post("/team/logout", dependencies=[Depends(require_csrf)])
def team_logout(request: Request):
    _portal_or_404()
    request.session.clear()
    return RedirectResponse(
        "/team/login?flash=You+have+been+signed+out.", status_code=303
    )


@router.get("/team/invite/accept/{token}", response_class=HTMLResponse)
def team_invite_accept_page(
    request: Request,
    token: str,
    error: Optional[str] = Query(default=None),
    problems: Optional[str] = Query(default=None),
    username: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    invite_role = "employee"
    token_row = _find_token_row(session, InviteToken, token)
    if token_row is not None and token_row.role:
        invite_role = token_row.role.strip().lower() or "employee"
    else:
        claimed_token_row = _find_token_row(
            session,
            InviteToken,
            token,
            include_used=True,
            include_expired=True,
        )
        if claimed_token_row is not None and claimed_token_row.used_at is not None:
            claimed_user_id = (
                claimed_token_row.used_by_user_id or claimed_token_row.target_user_id
            )
            claimed_user = session.get(User, claimed_user_id) if claimed_user_id else None
            if (
                claimed_user is not None
                and claimed_user.is_active
                and claimed_user.password_hash
            ):
                current_user: Optional[User] = getattr(
                    request.state, "current_user", None
                )
                if current_user is None:
                    return RedirectResponse("/team/login", status_code=303)
                if current_user.id == claimed_user.id:
                    return RedirectResponse("/team/", status_code=303)
                return RedirectResponse(
                    app_home_for_role(current_user.role), status_code=303
                )
    show_employee_tutorial = invite_role == "employee"
    show_manager_tutorial = invite_role == "manager"
    show_portal_tutorial = show_employee_tutorial or show_manager_tutorial
    setup_step_total = 7 if show_portal_tutorial else 6
    return templates.TemplateResponse(
        request,
        "team/invite_accept.html",
        {
            "request": request,
            "title": "Accept Invite",
            "token": token,
            "invite_role": invite_role,
            "show_employee_tutorial": show_employee_tutorial,
            "show_manager_tutorial": show_manager_tutorial,
            "show_portal_tutorial": show_portal_tutorial,
            "setup_step_total": setup_step_total,
            "progress_dot_count": setup_step_total + 1,
            "error": error,
            "problems": (problems or "").split("|") if problems else [],
            "username": username or "",
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/invite/accept/{token}", dependencies=[Depends(require_csrf)])
async def team_invite_accept_post(
    request: Request,
    token: str,
    new_username: str = Form(...),
    new_password: str = Form(...),
    preferred_name: str = Form(default=""),
    legal_name: str = Form(default=""),
    email: str = Form(default=""),
    phone: str = Form(default=""),
    address_street: str = Form(default=""),
    address_city: str = Form(default=""),
    address_state: str = Form(default=""),
    address_zip: str = Form(default=""),
    emergency_contact_name: str = Form(default=""),
    emergency_contact_phone: str = Form(default=""),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    current_user: Optional[User] = getattr(request.state, "current_user", None)
    current_session_user_id = None
    try:
        current_session_user_id = request.session.get("user_id")
    except (AssertionError, AttributeError):
        current_session_user_id = None
    if current_user is not None or current_session_user_id:
        return HTMLResponse(
            "Sign out before accepting an invite for another account.",
            status_code=409,
        )
    if limited := rate_limited_or_429(
        request, key_prefix="team:invite", max_requests=3, window_seconds=900.0
    ):
        return limited
    address_payload = {
        "street": (address_street or "").strip(),
        "city": (address_city or "").strip(),
        "state": (address_state or "").strip(),
        "zip": (address_zip or "").strip(),
    }
    try:
        user = consume_invite_token(
            session,
            token,
            new_username=new_username,
            new_password=new_password,
            preferred_name=preferred_name,
            legal_name=legal_name,
            email=email,
            phone=phone,
            address=address_payload if any(address_payload.values()) else None,
            emergency_contact_name=emergency_contact_name,
            emergency_contact_phone=emergency_contact_phone,
        )
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        qs += f"&username={new_username}"
        return RedirectResponse(
            f"/team/invite/accept/{token}?{qs}", status_code=303
        )
    except ValueError as exc:
        return RedirectResponse(
            f"/team/invite/accept/{token}?error={str(exc)}", status_code=303
        )
    request.session["user_id"] = user.id
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)
    redirect_url = "/team/?flash=Welcome+to+the+team!"
    if session.info.pop("invite_email_skipped_due_to_clash", False):
        redirect_url += "&banner=Email+not+saved.+That+address+is+already+on+file+for+another+employee."
    return RedirectResponse(redirect_url, status_code=303)


@router.get("/team/password/forgot", response_class=HTMLResponse)
def team_password_forgot_page(
    request: Request,
    flash: Optional[str] = Query(default=None),
):
    _portal_or_404()
    return templates.TemplateResponse(
        request,
        "team/password_forgot.html",
        {
            "request": request,
            "title": "Reset password",
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/password/forgot", dependencies=[Depends(require_csrf)])
async def team_password_forgot_post(
    request: Request,
    identifier: str = Form(default=""),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    if limited := rate_limited_or_429(
        request, key_prefix="team:forgot", max_requests=3, window_seconds=900.0
    ):
        return limited
    probe = (identifier or "").strip().lower()
    probe_hash = _password_reset_identifier_hash(probe)
    if probe_hash:
        if limited := rate_limited_or_429(
            request,
            key_prefix=f"team:forgot:{probe_hash[:16]}",
            max_requests=3,
            window_seconds=900.0,
        ):
            return limited
    matched_user = _find_password_reset_user(session, probe)
    delivered = False
    manager_reset_alert: Optional[dict[str, Any]] = None
    if matched_user is not None:
        queue_reason = "email_delivery_unavailable"
        if not _configured_public_base_url():
            queue_reason = "missing_public_base_url"
        else:
            delivered = _try_send_password_reset_email(
                session,
                request=request,
                user=matched_user,
                probe_hash=probe_hash,
            )
        if not delivered:
            reset_request = _queue_password_reset_request(
                session,
                request=request,
                user=matched_user,
                probe_hash=probe_hash,
                reason=queue_reason,
            )
            session.flush()
            manager_reset_alert = {
                "request_id": reset_request.id,
                "employee_name": matched_user.display_name or matched_user.username,
                "employee_username": matched_user.username,
                "reason": "email_delivery_unavailable",
            }
    session.add(
        AuditLog(
            action="password.reset_requested",
            target_user_id=None,
            details_json=json.dumps(
                {
                    "identifier_hash": probe_hash,
                    "source": "http_forgot",
                    "status": "accepted",
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    if manager_reset_alert is not None:
        send_password_reset_manager_request_alert(**manager_reset_alert)
    return RedirectResponse(
        "/team/password/forgot?flash=If+that+account+exists%2C+we%27ll+email+a+reset+link+or+put+it+in+the+admin+reset+queue.",
        status_code=303,
    )


@router.get("/team/password/reset/{token}", response_class=HTMLResponse)
def team_password_reset_page(
    request: Request,
    token: str,
    problems: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    _portal_or_404()
    return templates.TemplateResponse(
        request,
        "team/password_reset.html",
        {
            "request": request,
            "title": "Choose a new password",
            "token": token,
            "problems": (problems or "").split("|") if problems else [],
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/password/reset/{token}", dependencies=[Depends(require_csrf)])
async def team_password_reset_post(
    request: Request,
    token: str,
    new_password: str = Form(...),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    if limited := rate_limited_or_429(
        request, key_prefix="team:reset", max_requests=5, window_seconds=900.0
    ):
        return limited
    try:
        consume_password_reset_token(session, token, new_password=new_password)
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        return RedirectResponse(
            f"/team/password/reset/{token}?{qs}", status_code=303
        )
    except ValueError as exc:
        return RedirectResponse(
            f"/team/password/reset/{token}?error={str(exc)}", status_code=303
        )
    return RedirectResponse(
        "/team/login?flash=Password+updated.+Please+sign+in.",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Authenticated employee surface
# ---------------------------------------------------------------------------

def _nav_context(session: Session, user: User) -> dict:
    cache: dict = {}
    # Keep the regular portal Schedule link employee-facing for every role.
    # Managers/admins get a separate Team Admin schedule link so they can
    # choose between checking the published view and editing the team grid.
    can_edit_schedule = has_permission(
        session, user, "admin.schedule.edit", cache=cache
    )
    schedule_href = "/team/schedule"
    keys = (
        # Order mirrors the phone tabs (redesign 2026-09): Home, Schedule,
        # Hours, then Requests, then Inbox + Policies, then Profile. base.html
        # groups these by name; the flat list stays the permission source of
        # truth. "inbox" has no key of its own: it shows when the user may
        # read announcements/updates (page.announcements) or documents
        # (page.documents), and only those sections appear inside it.
        ("dashboard", "Home", "page.dashboard", "/team/"),
        ("schedule", "Schedule", "page.schedule", schedule_href),
        ("hours", "Hours", "page.hours", "/team/hours"),
        ("time-off", "Time off", "page.timeoff", "/team/requests?tab=timeoff"),
        ("supply", "Supply", "page.supply_requests", "/team/requests?tab=supply"),
        ("inbox", "Inbox", None, "/team/inbox"),
        ("policies", "Policies", "page.policies", "/team/policies"),
        ("profile", "Profile", "page.profile", "/team/profile"),
    )
    inbox_kinds = _inbox_kinds_for(session, user, cache=cache)
    nav = []
    for name, label, key, href in keys:
        if name == "inbox":
            if inbox_kinds:
                nav.append({"name": name, "label": label, "href": href})
        elif has_permission(session, user, key, cache=cache):
            nav.append({"name": name, "label": label, "href": href})

    # Admin-only section. Rendered as a separate group in the sidebar when
    # at least one entry is visible. Gated per-key against the perms matrix
    # so managers/reviewers only see the admin links they actually have.
    # Each entry also carries the gate its /team/admin route enforces
    # (route permission key + whether the route requires role == "admin"
    # via `_admin_gate`), so a link is never shown that would 403.
    admin_keys = (
        ("employees", "Employees", "page.admin.employees", "/team/admin/employees", "admin.employees.view", False),
        ("invites", "Invites", "page.admin.invites", "/team/admin/invites", "admin.invites.view", True),
        ("permissions", "Permissions", "page.admin.permissions", "/team/admin/permissions", "admin.permissions.view", True),
        ("team-schedule", "Team schedule", "admin.schedule.view", "/team/admin/schedule", "admin.schedule.view", False),
        ("supply-queue", "Supply queue", "page.admin.supply", "/team/admin/supply", "admin.supply.view", False),
        ("buylist-submissions", "Buylist queue", "admin.supply.view", "/team/admin/buylist/submissions", "admin.supply.view", False),
        ("buylist", "Buylist pricing", "admin.supply.view", "/team/admin/buylist", "admin.buylist.edit", False),
        ("time-off-queue", "Time off queue", "admin.timeoff.view", "/team/admin/timeoff", "admin.timeoff.view", False),
        (
            "announcements-admin",
            "Announcements admin",
            "admin.announcements.view",
            "/team/admin/announcements",
            "admin.announcements.view",
            False,
        ),
    )
    role = getattr(user, "role", None)
    admin_nav = []
    for name, label, key, href, route_key, admin_role_only in admin_keys:
        # Mirror the route gates: `_permission_gate` has an
        # admin/manager/reviewer role floor; `_admin_gate` requires admin.
        if admin_role_only and role != "admin":
            continue
        if role not in {"admin", "manager", "reviewer"}:
            continue
        if has_permission(session, user, key, cache=cache) and has_permission(
            session, user, route_key, cache=cache
        ):
            admin_nav.append({"name": name, "label": label, "href": href})

    # Ops shortcuts are employee-facing tools. Keep them permission-filtered
    # so the sidebar never advertises a page this user will hit a 403 on.
    ops_keys = (
        ("inventory", "Inventory", "ops.inventory.view", "/inventory"),
        ("add-stock", "Add Stock", "ops.inventory.receive", "/inventory/add-stock"),
        ("buylist", "Buylist", "ops.buylist.view", "/team/buylist"),
        ("degen-eye", "Degen Eye", "ops.degen_eye.view", "/degen_eye?team_shell=1"),
        ("live-stream", "Live Stream", "ops.live_stream.view", "/tiktok/streamer?team_shell=1"),
        ("live-hits", "Live Hits", "ops.live_hits.view", "/hits"),
    )
    ops_nav = []
    for name, label, key, href in ops_keys:
        if has_permission(session, user, key, cache=cache):
            ops_nav.append({"name": name, "label": label, "href": href})

    return {
        "nav_items": nav,
        "admin_nav_items": admin_nav,
        "tools_nav_items": ops_nav,
        "schedule_href": schedule_href,
        "can_edit_schedule": can_edit_schedule,
        "inbox_kinds": inbox_kinds,
        # Sidebar Inbox item, More row and More-tab badge all read this.
        "inbox_unread": (
            inbox_store.unread_count(session, user.id, kinds=inbox_kinds)
            if inbox_kinds and user.id is not None
            else 0
        ),
    }


def _inbox_kinds_for(session: Session, user: User, *, cache: Optional[dict] = None) -> tuple[str, ...]:
    """Inbox sections this user may see, from the existing page.* keys."""
    cache = cache if cache is not None else {}
    return inbox_view.allowed_kinds(
        can_announcements=has_permission(session, user, "page.announcements", cache=cache),
        can_documents=has_permission(session, user, "page.documents", cache=cache),
    )


def _portal_now(*, settings=None, now: Optional[datetime] = None) -> datetime:
    """Return the current time in the configured business/Clockify timezone."""
    settings = settings or get_settings()
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    week_start_local, _ = clockify_week_bounds(now_utc.date(), settings=settings)
    return now_utc.astimezone(week_start_local.tzinfo)


def _portal_today(*, settings=None, now: Optional[datetime] = None) -> date:
    """Return the business-local date used by Clockify/team scheduling.

    The app server runs in UTC, but Degen's staff schedule and Clockify day
    are Pacific time. Employee-facing "today" widgets must not roll over at
    5 PM PT just because UTC is already tomorrow.
    """
    return _portal_now(settings=settings, now=now).date()


@router.get("/team/dashboard")
def team_dashboard_alias():
    # Unauthenticated-safe: this only redirects to /team/, which is auth-gated.
    return RedirectResponse("/team/", status_code=303)


@router.get("/team/", response_class=HTMLResponse)
def team_dashboard(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    widgets = perms.allowed_widgets_for(session, user)
    settings = get_settings()
    clockify_ready = clockify_is_configured(settings)
    show_supply_queue_count = (
        user.role in MANAGER_ADMIN_DASHBOARD_ROLES
        and has_permission(session, user, "admin.supply.view")
    )
    show_timeoff_queue_count = (
        user.role in MANAGER_ADMIN_DASHBOARD_ROLES
        and has_permission(session, user, "admin.timeoff.view")
    )
    dashboard_context: dict[str, Any] = {
        "request": request,
        "title": "Home",
        "active": "dashboard",
        "current_user": user,
        "widgets": widgets,
        "clockify_ready": clockify_ready,
        "show_supply_queue_count": show_supply_queue_count,
        "show_timeoff_queue_count": show_timeoff_queue_count,
    }
    if show_supply_queue_count:
        dashboard_context["supply_queue_count"] = int(
            session.exec(
                select(func.count())
                .select_from(SupplyRequest)
                .where(SupplyRequest.status == "submitted")
            ).one()
        )
    if show_timeoff_queue_count:
        dashboard_context["timeoff_queue_count"] = int(
            session.exec(
                select(func.count())
                .select_from(TimeOffRequest)
                .where(TimeOffRequest.status == "submitted")
            ).one()
        )
    now_local = _portal_now(settings=settings)
    today = now_local.date()
    nav_ctx = _nav_context(session, user)
    dashboard_context.update(
        _employee_home_context(
            session,
            user,
            today=today,
            now_local=now_local,
            settings=settings,
            clockify_ready=clockify_ready,
            nav_ctx=nav_ctx,
        )
    )
    dashboard_context.update(
        {
            "today_staffing": _today_staffing_for(session, today=today),
            "csrf_token": issue_token(request),
            **nav_ctx,
        }
    )
    return templates.TemplateResponse(
        request,
        "team/dashboard.html",
        dashboard_context,
    )



def _clock_status_from_week(week: dict[str, Any], *, today: date) -> dict[str, Any]:
    """Today's Clockify state for the Home hero, from employee_week_hours()."""
    status: dict[str, Any] = {
        "linked": bool(week.get("linked")) and not week.get("error"),
        "running": False,
        "on_break": False,
        "since": None,
        "today_seconds": 0,
    }
    if not status["linked"] or not week.get("entries"):
        return status
    from .team_admin_clockify import _clockify_entry_is_break

    start_local = week["start_local"]
    day_start = datetime.combine(today, time.min, tzinfo=start_local.tzinfo)
    day_end = day_start + timedelta(days=1)
    todays = [
        row
        for row in week["entries"]
        if _entry_overlap_seconds(row, day_start, day_end) > 0
    ]
    work = [row for row in todays if not _clockify_entry_is_break(row)]
    running_work = [row for row in work if row.running]
    running_break = [
        row for row in todays if row.running and _clockify_entry_is_break(row)
    ]
    status["running"] = bool(running_work or running_break)
    status["on_break"] = bool(running_break) and not running_work
    starts = [row.start_local for row in work if row.start_local is not None]
    status["since"] = min(starts) if starts else None
    status["today_seconds"] = week["adjusted_by_day"].get(today, (0, 0, 0))[0]
    return status


def _approved_timeoff_days(
    session: Session, user_id: int, first_day: date, last_day: date
) -> set[date]:
    """Days in [first_day, last_day] covered by the user's approved time off."""
    days: set[date] = set()
    for row in session.exec(
        select(TimeOffRequest)
        .where(TimeOffRequest.submitted_by_user_id == user_id)
        .where(TimeOffRequest.status == "approved")
        .where(TimeOffRequest.start_date <= last_day)
        .where(TimeOffRequest.end_date >= first_day)
    ).all():
        cursor = max(row.start_date, first_day)
        while cursor <= min(row.end_date, last_day):
            days.add(cursor)
            cursor += timedelta(days=1)
    return days


def _week_range_label(start: date) -> str:
    end = start + timedelta(days=6)
    if start.month == end.month:
        return f"{home_view.month_day(start)} – {end.day}"
    return f"{home_view.month_day(start)} – {home_view.month_day(end)}"


def _employee_home_context(
    session: Session,
    user: User,
    *,
    today: date,
    now_local: datetime,
    settings=None,
    clockify_ready: bool = False,
    nav_ctx: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Everything the Home screen (team/dashboard.html) renders.

    Pay periods are not modelled anywhere in the app (the payroll export
    works on arbitrary Mon-Sun windows and defaults to last week), so the
    first tile shows last week's Clockify total and the second shows this
    week worked vs scheduled. No estimated pay on Home, by design.
    """
    settings = settings or get_settings()
    nav_ctx = nav_ctx if nav_ctx is not None else _nav_context(session, user)
    nav_names = {item["name"] for item in nav_ctx.get("nav_items", [])}
    schedule_href = nav_ctx.get("schedule_href") or "/team/schedule"
    can_timeoff = "time-off" in nav_names
    can_supply = "supply" in nav_names
    can_hours = "hours" in nav_names

    today_shifts = _today_shifts_for(session, user, today=today)
    upcoming_shifts = _upcoming_shifts_for(session, user, today=today, limit=5)

    # --- Clockify: this week + last week (same helper /team/hours uses) ---
    week_start = today - timedelta(days=today.weekday())
    last_start = week_start - timedelta(days=7)
    week = employee_week_hours(session, user, today=today, settings=settings)
    last_week = (
        employee_week_hours(
            session, user, today=today, week_of=last_start, settings=settings
        )
        if week.get("linked")
        else None
    )
    clock = _clock_status_from_week(week, today=today)

    # --- This week's own schedule (week strip + scheduled hours) ---
    # Same sources and rules as /team/hours (ShiftEntry per calendar plus
    # Stream Manager shifts, shaped by schedule_view.build_my_week), so the
    # "scheduled" number on Home always matches the Hours page.
    week_end = week_start + timedelta(days=6)
    week_days = [week_start + timedelta(days=i) for i in range(7)]
    timeoff_days = _approved_timeoff_days(session, user.id, week_start, week_end)
    my_week = schedule_view.build_my_week(
        week_days=week_days,
        today=today,
        me_id=user.id,
        calendars=_my_schedule_calendars(session, user, week_days),
        timeoff_days=timeoff_days,
    )
    shifts_by_day: dict[date, list[str]] = {
        day["date"]: [shift["label"] or shift["time"] for shift in day["shifts"]]
        for day in my_week["days"]
        if day["shifts"]
    }
    scheduled = float(my_week["scheduled_hours"] or 0)

    linked = bool(week.get("linked"))
    week_ok = linked and not week.get("error")
    last_ok = bool(last_week) and not last_week.get("error")
    tiles = {
        "linked": linked,
        "error": week.get("error") or "",
        "last_week_value": (
            home_view.hours_number(last_week["total_work_seconds"]) if last_ok else "–"
        ),
        "last_week_sub": _week_range_label(last_start),
        "week_value": (
            home_view.hours_number(week["total_work_seconds"]) if week_ok else "–"
        ),
        "week_scheduled": (
            home_view.hours_number_from_hours(scheduled) if scheduled else ""
        ),
        "href": "/team/hours" if can_hours else "",
    }

    profile_completion = _profile_completion_for(
        session, user, clockify_ready=clockify_ready
    )
    needs_you = home_view.build_needs_you(
        profile_completion=profile_completion,
        needs_fix_days=week.get("needs_fix_days") or [],
        clockify_configured=clockify_ready,
    )
    request_rows = home_view.build_request_rows(
        timeoff=(
            session.exec(
                select(TimeOffRequest)
                .where(TimeOffRequest.submitted_by_user_id == user.id)
                .order_by(TimeOffRequest.created_at.desc())
                .limit(5)
            ).all()
            if can_timeoff
            else []
        ),
        supply=(
            session.exec(
                select(SupplyRequest)
                .where(SupplyRequest.submitted_by_user_id == user.id)
                .order_by(SupplyRequest.created_at.desc())
                .limit(5)
            ).all()
            if can_supply
            else []
        ),
        limit=3,
    )
    can_announcements = inbox_view.KIND_ANNOUNCEMENT in (nav_ctx.get("inbox_kinds") or ())
    latest = _active_announcements_for(session, limit=1) if can_announcements else []
    latest_unread = bool(latest) and inbox_view.is_unread(
        inbox_view.KIND_ANNOUNCEMENT,
        str(latest[0].id),
        latest[0].published_at,
        inbox_store.read_keys(session, user.id, (inbox_view.KIND_ANNOUNCEMENT,)),
        utcnow(),
        pinned=bool(latest[0].pinned),
    )
    hero = home_view.build_hero(
        now_local=now_local,
        today=today,
        today_shifts=today_shifts,
        upcoming_shifts=upcoming_shifts,
        clock=clock,
        schedule_href=schedule_href,
        timeoff_href="/team/requests?new=timeoff" if can_timeoff else None,
        hours_href="/team/hours" if can_hours else None,
    )
    name = (user.display_name or user.username or "").strip()
    return {
        "today_date": today,
        "now_hour": now_local.hour,
        "today_shifts": today_shifts,
        "upcoming_shifts": upcoming_shifts,
        "profile_completion": profile_completion,
        "home": {
            "eyebrow": f"{today:%A}, {home_view.month_day(today)}",
            "first_name": name.split()[0] if name else "there",
            "initials": "".join(part[0] for part in name.split()[:2]).upper() or "?",
            "hero": hero,
            "tiles": tiles,
            "needs_you": needs_you,
            "week": home_view.build_week_strip(
                week_start=week_start,
                today=today,
                shifts_by_day=shifts_by_day,
                timeoff_days=timeoff_days,
            ),
            "requests": request_rows,
            "can_requests": can_timeoff or can_supply,
            "latest": latest[0] if latest else None,
            "latest_unread": latest_unread,
            "latest_posted": (
                home_view.month_day(
                    inbox_view.as_utc(latest[0].published_at).astimezone(now_local.tzinfo).date()
                )
                if latest and latest[0].published_at
                else ""
            ),
            "schedule_href": schedule_href,
        },
    }


def _format_money_label(cents: int) -> str:
    return f"${Decimal(cents) / Decimal(100):,.2f}"


def _cents_for_seconds(seconds: int, rate_cents: int) -> int:
    if seconds <= 0 or rate_cents <= 0:
        return 0
    amount = (Decimal(seconds) / Decimal(3600)) * Decimal(rate_cents)
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _format_time_label(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.strftime("%I:%M %p").lstrip("0")


def _entry_overlap_seconds(entry: Any, start_local: datetime, end_local: datetime) -> int:
    entry_start = getattr(entry, "start_local", None)
    entry_end = getattr(entry, "end_local", None)
    if entry_start is None:
        return 0
    if entry_end is None:
        duration = int(getattr(entry, "duration_seconds", 0) or 0)
        entry_end = entry_start + timedelta(seconds=duration)
    overlap_start = max(entry_start, start_local)
    overlap_end = min(entry_end, end_local)
    if overlap_end <= overlap_start:
        return 0
    return int((overlap_end - overlap_start).total_seconds())


# Employee-facing view of TimecardApproval.status. "rejected" is deliberately
# worded as "Needs fix" -- it describes an action for the employee, not a
# judgement about them, and matches the wording managers see.
_EMPLOYEE_TIMECARD_STATUS_LABELS = {
    "pending": "Not reviewed yet",
    "approved": "Approved",
    "rejected": "Needs fix",
    "locked": "Final",
}
_EMPLOYEE_TIMECARD_STATUS_TONES = {
    "pending": "info",
    "approved": "ok",
    "rejected": "danger",
    "locked": "muted",
}


def employee_week_hours(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
    week_of: Optional[date] = None,
    settings=None,
) -> dict[str, Any]:
    """One source of truth for an employee's own weekly hours.

    Both the dashboard widget and /team/hours read from here. They used to
    compute the week independently -- the dashboard excluded break entries and
    applied the missed-break deduction, /team/hours summed raw Clockify
    durations -- so the same week showed two different totals one click apart,
    and the larger of the two sat on the page titled "My Hours".

    The numbers returned here are the ones payroll pays: break entries
    excluded, missed-break deduction applied.
    """
    today = today or _portal_today()
    settings = settings or get_settings()
    profile = session.get(EmployeeProfile, user.id)
    clockify_user_id = (profile.clockify_user_id or "").strip() if profile else ""
    # `week_of` picks which week to report; `today` stays the real date so the
    # is_today marker and the dashboard's "today" figures remain correct when
    # an employee pages back through history.
    start_local, end_local = clockify_week_bounds(week_of or today, settings=settings)

    out: dict[str, Any] = {
        "linked": bool(clockify_user_id),
        "error": "",
        "source_label": "",
        "week_start": start_local.date(),
        "week_end_inclusive": (end_local - timedelta(seconds=1)).date(),
        "timezone_name": str(getattr(start_local.tzinfo, "key", start_local.tzinfo)),
        "start_local": start_local,
        "end_local": end_local,
        "today": today,
        "summary": None,
        "entries": [],
        "days": [],
        "adjusted_by_day": {},
        "total_work_seconds": 0,
        "total_break_seconds": 0,
        "total_auto_break_seconds": 0,
        "running_count": 0,
    }
    if not clockify_user_id:
        return out

    from .team_admin_clockify import (
        _apply_missed_break_deduction,
        _cached_clockify_entries_by_user,
        _clockify_entry_is_break,
    )

    source_label = "Clockify cache"
    try:
        cached = _cached_clockify_entries_by_user(
            session,
            [clockify_user_id],
            start_local=start_local,
            end_local=end_local,
        )
        raw_entries = cached.get(clockify_user_id, [])
        # Live fallback only for the current week, where the cache may lag a
        # webhook. For a past week the cache is authoritative, and falling
        # through would hit Clockify once per empty week as an employee pages
        # back through history -- rendering an API error as if the week itself
        # had failed to load.
        is_current_week = start_local.date() == (today - timedelta(days=today.weekday()))
        if not raw_entries and is_current_week and clockify_is_configured(settings):
            raw_entries = clockify_client_from_settings(settings).get_user_time_entries(
                clockify_user_id,
                start_utc=start_local.astimezone(timezone.utc),
                end_utc=end_local.astimezone(timezone.utc),
            )
            source_label = "Clockify live"
        summary = build_week_summary(
            raw_entries,
            week_start_local=start_local,
            week_end_local=end_local,
            settings=settings,
            now=datetime.now(timezone.utc),
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        out["error"] = str(exc)
        return out

    daily_work_seconds: dict[date, int] = {}
    daily_break_seconds: dict[date, int] = {}
    last_range_day = (end_local - timedelta(seconds=1)).date()
    range_day = start_local.date()
    while range_day <= last_range_day:
        day_start = datetime.combine(range_day, time.min, tzinfo=start_local.tzinfo)
        day_end = day_start + timedelta(days=1)
        for row in summary.entries:
            seconds = _entry_overlap_seconds(row, day_start, day_end)
            if seconds <= 0:
                continue
            if _clockify_entry_is_break(row):
                daily_break_seconds[range_day] = (
                    daily_break_seconds.get(range_day, 0) + seconds
                )
            else:
                daily_work_seconds[range_day] = (
                    daily_work_seconds.get(range_day, 0) + seconds
                )
        range_day += timedelta(days=1)

    adjusted_by_day: dict[date, tuple[int, int, int]] = {}
    for day_key in set(daily_work_seconds) | set(daily_break_seconds):
        adjusted_by_day[day_key] = _apply_missed_break_deduction(
            daily_work_seconds.get(day_key, 0),
            daily_break_seconds.get(day_key, 0),
        )

    # An employee could not previously see whether their own day was approved,
    # rejected or locked -- a manager marking a day "Needs fix" with a note was
    # invisible to the only person who could act on it.
    approvals = {
        row.work_date: row
        for row in session.exec(
            select(TimecardApproval).where(
                TimecardApproval.user_id == user.id,
                TimecardApproval.work_date >= start_local.date(),
                TimecardApproval.work_date <= last_range_day,
            )
        ).all()
    }

    days: list[dict[str, Any]] = []
    cursor = start_local.date()
    while cursor <= last_range_day:
        work, brk, auto = adjusted_by_day.get(cursor, (0, 0, 0))
        approval = approvals.get(cursor)
        status = (approval.status if approval else "") or ""
        days.append(
            {
                "day": cursor,
                "work_seconds": work,
                "break_seconds": brk,
                "auto_break_seconds": auto,
                "is_today": cursor == today,
                "status": status,
                "status_label": _EMPLOYEE_TIMECARD_STATUS_LABELS.get(status, ""),
                "status_tone": _EMPLOYEE_TIMECARD_STATUS_TONES.get(status, ""),
                "status_note": (approval.note or "").strip() if approval else "",
            }
        )
        cursor += timedelta(days=1)

    out["source_label"] = source_label
    out["summary"] = summary
    out["entries"] = list(summary.entries)
    out["days"] = days
    out["adjusted_by_day"] = adjusted_by_day
    out["total_work_seconds"] = sum(row[0] for row in adjusted_by_day.values())
    out["total_break_seconds"] = sum(row[1] for row in adjusted_by_day.values())
    out["total_auto_break_seconds"] = sum(row[2] for row in adjusted_by_day.values())
    out["running_count"] = sum(1 for row in summary.entries if row.running)
    out["days"] = days
    out["needs_fix_days"] = [row for row in days if row["status"] == "rejected"]
    return out


def _employee_dashboard_pay_summary(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
    week: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    today = today or _portal_today()
    profile = session.get(EmployeeProfile, user.id)
    clockify_user_id = (profile.clockify_user_id or "").strip() if profile else ""
    base: dict[str, Any] = {
        "hours_label": "0m",
        "hours_this_week_sub": "Since Monday 12:00 AM",
        "clocked_in_today_label": "Not clocked in",
        "hours_today_label": "0m",
        "break_today_label": "Not yet",
        "estimated_pay_label": "$0.00",
        "pay_basis": "Hours not connected yet",
        "clockify_user_id": clockify_user_id,
        "has_clockify": bool(clockify_user_id),
        "has_rate": False,
        "error": "",
    }
    if not clockify_user_id:
        return base

    settings = get_settings()
    if week is None:
        week = employee_week_hours(session, user, today=today, settings=settings)
    start_local = week["start_local"]
    work_seconds = week["total_work_seconds"]
    if week["error"]:
        base["error"] = week["error"]
    else:
        from .team_admin_clockify import _clockify_entry_is_break

        today_start_local = datetime.combine(today, time.min, tzinfo=start_local.tzinfo)
        today_end_local = today_start_local + timedelta(days=1)
        today_work_entries = [
            row
            for row in week["entries"]
            if not _clockify_entry_is_break(row)
            and _entry_overlap_seconds(row, today_start_local, today_end_local) > 0
        ]
        today_break_entries = [
            row
            for row in week["entries"]
            if _clockify_entry_is_break(row)
            and _entry_overlap_seconds(row, today_start_local, today_end_local) > 0
        ]
        today_work_seconds, today_break_seconds, today_missed_break_seconds = week[
            "adjusted_by_day"
        ].get(today, (0, 0, 0))
        running_break = any(row.running for row in today_break_entries)
        base["clocked_in_today_label"] = (
            _format_time_label(today_work_entries[0].start_local)
            if today_work_entries
            else "Not clocked in"
        )
        base["hours_today_label"] = format_hours(today_work_seconds)
        if today_missed_break_seconds > 0:
            base["break_today_label"] = (
                f"Auto-deducted {format_hours(today_missed_break_seconds)}"
            )
        elif today_break_seconds > 0:
            prefix = "On break" if running_break else "Taken"
            base["break_today_label"] = f"{prefix} ({format_hours(today_break_seconds)})"
        base["hours_label"] = format_hours(work_seconds)

    try:
        from .team_admin_employees import (
            COMPENSATION_TYPE_HOURLY,
            COMPENSATION_TYPE_LABELS,
            COMPENSATION_TYPE_MONTHLY,
            compensation_history_rows_for_users,
            compensation_snapshot_for_day,
            _salary_cost_for_period,
        )

        history_rows = compensation_history_rows_for_users(
            session,
            [user.id] if user.id is not None else [],
            end_day=today,
        )
        snapshot = compensation_snapshot_for_day(
            profile,
            today,
            history_rows=history_rows.get(user.id or 0, []),
        )
        compensation_type = snapshot["compensation_type"]
        base["pay_basis"] = COMPENSATION_TYPE_LABELS.get(compensation_type, "Pay")
        if compensation_type == COMPENSATION_TYPE_HOURLY:
            rate_cents = snapshot["hourly_rate_cents"]
            base["has_rate"] = rate_cents is not None
            if rate_cents is not None:
                base["estimated_pay_label"] = _format_money_label(
                    _cents_for_seconds(work_seconds, rate_cents)
                )
                base["pay_basis"] = f"This week at {_format_money_label(rate_cents)}/hr"
            else:
                base["pay_basis"] = "Hourly rate missing"
        elif compensation_type == COMPENSATION_TYPE_MONTHLY:
            salary_cents = snapshot["monthly_salary_cents"]
            base["has_rate"] = salary_cents is not None
            if salary_cents is not None and isinstance(profile, EmployeeProfile):
                base["estimated_pay_label"] = _format_money_label(
                    _salary_cost_for_period(
                        salary_cents=salary_cents,
                        user=user,
                        profile=profile,
                        start_day=start_local.date(),
                        end_day=today,
                    )
                )
                base["pay_basis"] = "This week's salary accrual"
            else:
                base["pay_basis"] = "Salary missing"
    except Exception as exc:
        base["error"] = str(exc)
        base["pay_basis"] = "Pay setup unavailable"

    return base


def _active_announcements_for(
    session: Session,
    *,
    limit: Optional[int] = None,
) -> list[TeamAnnouncement]:
    return inbox_store.active_announcements(session, limit=limit)


def _policy_from_row(row: TeamPolicy) -> dict[str, Any]:
    return {
        "id": row.public_id,
        "title": row.title,
        "version": row.version or "v1",
        "kind": row.kind or "policy",
        "requires_ack": bool(row.requires_acknowledgement),
        "body_md": row.body or "",
        "published_at": row.published_at,
        "is_dynamic": True,
    }


def _published_policies(session: Session) -> list[dict[str, Any]]:
    rows = session.exec(
        select(TeamPolicy)
        .where(TeamPolicy.is_active == True)  # noqa: E712
        .order_by(TeamPolicy.published_at.desc(), TeamPolicy.id.desc())
    ).all()
    policies = [_policy_from_row(row) for row in rows]
    policies.extend(dict(policy, is_dynamic=False) for policy in LEGACY_POLICIES)
    return policies


def _policy_by_id(session: Session, policy_id: str) -> Optional[dict[str, Any]]:
    policy_id = (policy_id or "").strip()
    if not policy_id:
        return None
    row = session.exec(
        select(TeamPolicy).where(
            TeamPolicy.public_id == policy_id,
            TeamPolicy.is_active == True,  # noqa: E712
        )
    ).first()
    if row is not None:
        return _policy_from_row(row)
    legacy = LEGACY_POLICY_BY_ID.get(policy_id)
    return dict(legacy, is_dynamic=False) if legacy is not None else None


def _policy_acknowledgements_for(session: Session, user: User) -> set[str]:
    rows = session.exec(
        select(AuditLog).where(
            AuditLog.actor_user_id == user.id,
            AuditLog.action == "policy.acknowledge",
        )
    ).all()
    acknowledged: set[str] = set()
    for row in rows:
        try:
            payload = json.loads(row.details_json or "{}")
        except json.JSONDecodeError:
            continue
        policy_id = payload.get("policy_id")
        if isinstance(policy_id, str):
            acknowledged.add(policy_id)
    return acknowledged


def _decrypt_optional(blob: Optional[bytes]) -> str:
    if not blob:
        return ""
    try:
        return (decrypt_pii(blob) or "").strip()
    except (PIIDecryptError, ValueError):
        return ""


def _profile_completion_for(
    session: Session,
    user: User,
    *,
    profile: Optional[EmployeeProfile] = None,
    clockify_ready: Optional[bool] = None,
) -> dict[str, Any]:
    profile = profile or session.get(EmployeeProfile, user.id)
    clockify_ready = clockify_is_configured() if clockify_ready is None else clockify_ready
    acknowledged = _policy_acknowledgements_for(session, user)
    missing_policies = [
        p
        for p in _published_policies(session)
        if p.get("requires_ack", True) and p["id"] not in acknowledged
    ]
    phone_done = bool(_decrypt_optional(profile.phone_enc if profile else None))
    emergency_done = bool(
        _decrypt_optional(profile.emergency_contact_name_enc if profile else None)
        and _decrypt_optional(profile.emergency_contact_phone_enc if profile else None)
    )
    clockify_done = bool(
        clockify_ready and profile and (profile.clockify_user_id or "").strip()
    )
    items = [
        {
            "key": "phone",
            "label": "Phone number",
            "done": phone_done,
            "href": "/team/profile",
            "hint": "Contact number; SMS is a separate optional choice.",
        },
        {
            "key": "emergency",
            "label": "Emergency contact",
            "done": emergency_done,
            "href": "/team/profile",
            "hint": "Name and phone number.",
        },
        {
            "key": "policies",
            "label": "Policies signed",
            "done": not missing_policies,
            "href": "/team/policies",
            "hint": (
                "All caught up."
                if not missing_policies
                else f"{len(missing_policies)} left to sign."
            ),
        },
        {
            "key": "clockify",
            "label": "Clockify connected",
            "done": clockify_done,
            "href": "/team/hours",
            "hint": "Ask a manager to connect this if hours look blank.",
        },
    ]
    complete_count = sum(1 for item in items if item["done"])
    return {
        "items": items,
        "complete_count": complete_count,
        "total_count": len(items),
        "percent": int((complete_count / len(items)) * 100) if items else 100,
        "missing_policies": missing_policies,
        "is_complete": complete_count == len(items),
        "phone_ready": phone_done,
        "clockify_ready": clockify_done,
    }


def _employee_notifications_for(
    session: Session,
    user: User,
    *,
    limit: int = 20,
    since_id: int = 0,
    newest_first: bool = True,
) -> list[dict[str, Any]]:
    stmt = (
        select(AuditLog)
        .where(AuditLog.target_user_id == user.id)
        .where(AuditLog.action == EMPLOYEE_NOTIFICATION_ACTION)
    )
    if since_id > 0:
        stmt = stmt.where(AuditLog.id > since_id)
    if newest_first:
        stmt = stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    else:
        stmt = stmt.order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
    rows = session.exec(stmt.limit(limit)).all()
    notifications: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row.details_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        notifications.append(
            {
                "id": row.id,
                "kind": str(payload.get("kind") or "general"),
                "title": str(payload.get("title") or "Team update"),
                "body": str(payload.get("body") or ""),
                "link_path": str(payload.get("link_path") or "/team/"),
                "created_at": row.created_at,
                "sms": payload.get("sms") if isinstance(payload.get("sms"), dict) else {},
            }
        )
    return notifications


@router.get("/team/notifications/poll")
def team_notifications_poll(
    request: Request,
    since_id: int = Query(default=0),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="page.announcements"
    )
    if denial:
        return denial
    notifications = _employee_notifications_for(
        session,
        user,
        limit=10,
        since_id=max(0, since_id),
        newest_first=False,
    )
    latest_seen = max(
        [since_id]
        + [int(note["id"]) for note in notifications if note.get("id") is not None]
    )
    return {
        "latest_id": latest_seen,
        "notifications": [
            {
                "id": note["id"],
                "kind": note["kind"],
                "title": note["title"],
                "body": note["body"],
                "link_path": note["link_path"],
                "created_at": note["created_at"].isoformat()
                if note.get("created_at")
                else None,
            }
            for note in notifications
        ],
    }


# The three old reading pages merged into /team/inbox (redesign Phase 4).
# GETs redirect to the matching filter so bookmarks, SMS links and stored
# notification `link_path`s keep working. The Inbox does the auth check.
@router.get("/team/announcements")
def team_announcements():
    return RedirectResponse("/team/inbox?filter=announcements", status_code=303)


@router.get("/team/notifications")
def team_notifications():
    return RedirectResponse("/team/inbox?filter=updates", status_code=303)


@router.get("/team/help", response_class=HTMLResponse)
def team_help(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/help.html",
        {
            "request": request,
            "title": "Ask for Help",
            "active": "",
            "current_user": user,
            "flash": flash,
            "error": error,
            "help_page_path": _safe_next(page) or "/team/help",
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/help", dependencies=[Depends(require_csrf)])
async def team_help_post(
    request: Request,
    message: str = Form(default=""),
    page_path: str = Form(default="/team/help"),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:help:{user.id}",
        max_requests=5,
        window_seconds=3600.0,
    ):
        return limited

    clean_message = (message or "").strip()[:2000]
    if not clean_message:
        return RedirectResponse(
            "/team/help?error=Tell+us+what+you+need+help+with.",
            status_code=303,
        )
    clean_page = _safe_next(page_path) or "/team/help"
    row = AuditLog(
        actor_user_id=user.id,
        target_user_id=user.id,
        action="help.requested",
        resource_key="page.dashboard",
        details_json=json.dumps(
            {
                "source": "team_help",
                "message": clean_message,
                "page_path": clean_page,
            },
            sort_keys=True,
        ),
        ip_address=(request.client.host if request.client else None),
    )
    session.add(row)
    session.flush()
    alert_context = {
        "request_id": row.id,
        "employee_name": user.display_name or user.username,
        "employee_username": user.username,
        "message": clean_message,
        "page_path": clean_page,
    }
    session.commit()
    send_help_request_alert(**alert_context)
    return RedirectResponse("/team/help?flash=Help+request+sent.", status_code=303)


@router.get("/team/more", response_class=HTMLResponse)
def team_more(
    request: Request,
    session: Session = Depends(get_session),
):
    """Phone "More" tab: profile, reading, ops tools by role, help, sign out.

    No resource gate of its own: every link on the page is filtered through
    the same `_nav_context` permission checks as the sidebar, so it can only
    ever list pages this user may open.
    """
    denial, user = _require_employee(request, session)
    if denial:
        return denial
    nav_ctx = _nav_context(session, user)
    nav_names = {item["name"] for item in nav_ctx["nav_items"]}
    completion = (
        _profile_completion_for(session, user)
        if "profile" in nav_names or "policies" in nav_names
        else None
    )
    return templates.TemplateResponse(
        request,
        "team/more.html",
        {
            "request": request,
            "title": "More",
            "active": "more",
            "current_user": user,
            "profile_completion": completion,
            "unsigned_policy_count": (
                len(completion["missing_policies"]) if completion else 0
            ),
            "csrf_token": issue_token(request),
            **nav_ctx,
        },
    )


@router.get("/team/help/tutorial", response_class=HTMLResponse)
def team_help_tutorial(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/help_tutorial.html",
        {
            "request": request,
            "title": "Portal Tour",
            "active": "",
            "current_user": user,
            "csrf_token": issue_token(request),
            "tutorial_links": True,
            "show_manager_tutorial": user.role == "manager",
            **_nav_context(session, user),
        },
    )


@router.get("/team/tools/inventory")
def team_tool_inventory(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/inventory/add-stock", status_code=303)


@router.get("/team/tools/degen-eye")
def team_tool_degen_eye(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/degen_eye?team_shell=1", status_code=303)


@router.get("/team/tools/live-stream")
def team_tool_live_stream(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/tiktok/streamer?team_shell=1", status_code=303)


# Shift-label time math lives in app/team/shift_labels.py so the schedule grid,
# timecards, pay-rates summary, and employee dashboard all agree.
_parse_shift_start_minutes = parse_shift_start_minutes


def _schedule_calendar_label(calendar_kind: str) -> str:
    if calendar_kind == SCHEDULE_CALENDAR_PACKING:
        return "Packing"
    if calendar_kind == SCHEDULE_CALENDAR_STOREFRONT:
        return "Storefront"
    return "Schedule"


def _today_shifts_for(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user.id)
            .where(ShiftEntry.shift_date == today)
            .where(
                ~ShiftEntry.kind.in_(
                    (SHIFT_KIND_REQUEST, SHIFT_KIND_OFF, SHIFT_KIND_BLANK)
                )
            )
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )
    if not shifts:
        return []

    day_note_row = session.exec(
        select(ScheduleDayNote).where(ScheduleDayNote.day_date == today)
    ).first()
    day_note = None
    if day_note_row is not None:
        day_note = (
            (day_note_row.location_label or "").strip()
            or (day_note_row.notes or "").strip()
            or None
        )
    return [
        {
            "shift_date": shift.shift_date,
            "label": shift.label,
            "kind": shift.kind,
            "calendar_kind": shift.calendar_kind,
            "calendar_label": _schedule_calendar_label(shift.calendar_kind),
            "day_note": (
                day_note
                if shift.calendar_kind == SCHEDULE_CALENDAR_STOREFRONT
                else None
            ),
        }
        for shift in shifts
    ]


def _upcoming_shifts_for(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user.id)
            .where(ShiftEntry.shift_date >= today)
            .where(
                ~ShiftEntry.kind.in_(
                    (SHIFT_KIND_REQUEST, SHIFT_KIND_OFF, SHIFT_KIND_BLANK)
                )
            )
            .order_by(ShiftEntry.shift_date, ShiftEntry.sort_order, ShiftEntry.id)
            .limit(limit)
        ).all()
    )
    if not shifts:
        return []

    dates = sorted({shift.shift_date for shift in shifts})
    notes = {
        note.day_date: note
        for note in session.exec(
            select(ScheduleDayNote).where(ScheduleDayNote.day_date.in_(dates))
        ).all()
    }
    out: list[dict[str, Any]] = []
    for shift in shifts:
        day_note_row = notes.get(shift.shift_date)
        day_note = None
        if day_note_row is not None:
            day_note = (
                (day_note_row.location_label or "").strip()
                or (day_note_row.notes or "").strip()
                or None
            )
        out.append(
            {
                "shift_date": shift.shift_date,
                "label": shift.label,
                "kind": shift.kind,
                "calendar_kind": shift.calendar_kind,
                "calendar_label": _schedule_calendar_label(shift.calendar_kind),
                "day_note": (
                    day_note
                    if shift.calendar_kind == SCHEDULE_CALENDAR_STOREFRONT
                    else None
                ),
            }
        )
    return out


def _today_staffing_for(
    session: Session,
    *,
    today: Optional[date] = None,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.shift_date == today)
            .where(ShiftEntry.kind.in_((SHIFT_KIND_WORK, SHIFT_KIND_ALL)))
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )
    if not shifts:
        return []

    user_ids = sorted({shift.user_id for shift in shifts})
    users = {
        user.id: user
        for user in session.exec(select(User).where(User.id.in_(user_ids))).all()
    }
    grouped: dict[int, dict[str, Any]] = {}
    first_start: dict[int, int] = {}
    for shift in shifts:
        scheduled_user = users.get(shift.user_id)
        display_name = (
            (scheduled_user.display_name or scheduled_user.username)
            if scheduled_user is not None
            else f"User {shift.user_id}"
        )
        row = grouped.setdefault(
            shift.user_id,
            {"display_name": display_name, "shifts": []},
        )
        row["shifts"].append((shift.label or "").strip() or "Shift")
        start = _parse_shift_start_minutes(shift.label)
        if start is not None:
            first_start[shift.user_id] = min(start, first_start.get(shift.user_id, start))

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[bool, int, str]:
        user_id, row = item
        start = first_start.get(user_id)
        return (
            start is None,
            start if start is not None else 0,
            str(row["display_name"]).casefold(),
        )

    return [row for _, row in sorted(grouped.items(), key=sort_key)]


def _profile_for(session: Session, user_id: int) -> EmployeeProfile:
    row = session.get(EmployeeProfile, user_id)
    if row is None:
        row = EmployeeProfile(user_id=user_id)
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def _decode_address(blob: Optional[bytes]) -> dict[str, str]:
    if not blob:
        return {}
    try:
        raw = decrypt_pii(blob) or ""
        if not raw:
            return {}
        return json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return {}


@router.get("/team/profile", response_class=HTMLResponse)
def team_profile(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    profile = _profile_for(session, user.id)
    # Self-view of own PII is not audited per spec.
    phone = _decrypt_optional(profile.phone_enc)
    email = _decrypt_optional(profile.email_ciphertext)
    legal_name = _decrypt_optional(profile.legal_name_enc)
    emergency_contact_name = _decrypt_optional(profile.emergency_contact_name_enc)
    emergency_contact_phone = _decrypt_optional(profile.emergency_contact_phone_enc)
    address = _decode_address(profile.address_enc)
    profile_completion = _profile_completion_for(session, user, profile=profile)
    return templates.TemplateResponse(
        request,
        "team/profile.html",
        {
            "request": request,
            "title": "My Profile",
            "active": "profile",
            "current_user": user,
            "profile": profile,
            "preferred_name": user.display_name or "",
            "legal_name": legal_name,
            "email": email,
            "phone": phone,
            "emergency_contact_name": emergency_contact_name,
            "emergency_contact_phone": emergency_contact_phone,
            "address": address,
            "profile_completion": profile_completion,
            "flash": flash,
            "csrf_token": issue_token(request),
            "sms_consent": consent_context(session, user.id),
            **_nav_context(session, user),
        },
    )


@router.post("/team/profile/sms", dependencies=[Depends(require_csrf)])
def team_sms_preference(
    request: Request,
    action: str = Form(default=""),
    sms_opt_in: str = Form(default=""),
    consent_version: str = Form(default=""),
    phone_binding: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request, key_prefix=f"team:sms-consent:{user.id}", max_requests=20, window_seconds=900,
    ):
        return limited
    if action not in {"subscribe", "unsubscribe"}:
        return HTMLResponse("Choose a text preference.", status_code=400)
    if action == "subscribe" and sms_opt_in != "yes":
        return RedirectResponse("/team/profile?flash=No+subscription+added.+SMS+is+optional.#sms-preferences", status_code=303)
    try:
        record_consent(
            session, user_id=user.id, opted_in=action == "subscribe",
            version=consent_version, phone_binding=phone_binding,
            ip_address=request.client.host if request.client else None,
        )
    except ValueError as exc:
        return HTMLResponse(str(exc), status_code=400)
    session.commit()
    return RedirectResponse("/team/profile?flash=Text+preference+saved.#sms-preferences", status_code=303)


@router.post("/team/profile", dependencies=[Depends(require_csrf)])
async def team_profile_post(
    request: Request,
    preferred_name: str = Form(default=""),
    legal_name: str = Form(default=""),
    email: str = Form(default=""),
    phone: str = Form(default=""),
    emergency_contact_name: str = Form(default=""),
    emergency_contact_phone: str = Form(default=""),
    address_street: str = Form(default=""),
    address_city: str = Form(default=""),
    address_state: str = Form(default=""),
    address_zip: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:profile:{user.id}",
        max_requests=20,
        window_seconds=900,
    ):
        return limited
    profile = _profile_for(session, user.id)
    now = utcnow()
    changed: list[str] = []

    # Re-fetch into the router session; the middleware-supplied `user` is
    # detached from any session and cannot be safely mutated here.
    db_user = session.get(User, user.id)
    new_display = (preferred_name or "").strip()
    if db_user is not None and new_display and new_display != (db_user.display_name or ""):
        db_user.display_name = new_display
        db_user.updated_at = now
        session.add(db_user)
        changed.append("preferred_name")

    def _maybe_set_enc(attr: str, raw: str, label: str) -> None:
        try:
            current = decrypt_pii(getattr(profile, attr)) or ""
        except (PIIDecryptError, ValueError):
            current = ""
        raw_s = (raw or "").strip()
        if raw_s != current:
            setattr(profile, attr, encrypt_pii(raw_s) if raw_s else None)
            changed.append(label)

    _maybe_set_enc("legal_name_enc", legal_name, "legal_name")
    _maybe_set_enc("phone_enc", phone, "phone")
    _maybe_set_enc(
        "emergency_contact_name_enc",
        emergency_contact_name,
        "emergency_contact_name",
    )
    _maybe_set_enc(
        "emergency_contact_phone_enc",
        emergency_contact_phone,
        "emergency_contact_phone",
    )

    # Email needs both the ciphertext AND the lookup hash kept in sync.
    from ..team.pii import email_lookup_hash as _email_hash
    new_email = (email or "").strip().lower()
    try:
        current_email = decrypt_pii(profile.email_ciphertext) or ""
    except (PIIDecryptError, ValueError):
        current_email = ""
    if new_email != current_email:
        if new_email:
            new_hash = _email_hash(new_email)
            clash = session.exec(
                select(EmployeeProfile).where(
                    EmployeeProfile.email_lookup_hash == new_hash,
                    EmployeeProfile.user_id != user.id,
                )
            ).first()
            if clash is not None:
                return RedirectResponse(
                    "/team/profile?flash=That+email+is+already+taken.", status_code=303
                )
            profile.email_ciphertext = encrypt_pii(new_email)
            profile.email_lookup_hash = new_hash
        else:
            profile.email_ciphertext = None
            profile.email_lookup_hash = None
        changed.append("email")

    address_payload = {
        "street": (address_street or "").strip(),
        "city": (address_city or "").strip(),
        "state": (address_state or "").strip(),
        "zip": (address_zip or "").strip(),
    }
    current_address = _decode_address(profile.address_enc)
    if address_payload != current_address:
        if any(address_payload.values()):
            profile.address_enc = encrypt_pii(json.dumps(address_payload))
        else:
            profile.address_enc = None
        changed.append("address")

    if changed:
        profile.updated_at = now
        session.add(profile)
        session.add(
            AuditLog(
                actor_user_id=user.id,
                target_user_id=user.id,
                action="profile.self_update",
                details_json=json.dumps({"fields": changed}),
                ip_address=(request.client.host if request.client else None),
            )
        )
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return RedirectResponse("/team/profile?flash=email_taken", status_code=303)
    return RedirectResponse("/team/profile?flash=Saved.", status_code=303)


# ---------------------------------------------------------------------------
# Self-serve password change (authenticated)
# ---------------------------------------------------------------------------
# Sibling of /team/password/reset/<token>. That one is for people who forgot
# their password (admin issues reset link). This one is for people who know
# their current password and just want to rotate it — no admin in the loop,
# but auditable. Lives here (not in the auth reset module) because it's
# authenticated and nav-integrated.

@router.get("/team/password/change", response_class=HTMLResponse)
def team_password_change_page(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    problems: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/password_change.html",
        {
            "request": request,
            "title": "Change password",
            "active": "profile",
            "current_user": user,
            "flash": flash,
            "error": error,
            "problems": (problems or "").split("|") if problems else [],
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/password/change", dependencies=[Depends(require_csrf)])
async def team_password_change_post(
    request: Request,
    current_password: str = Form(default=""),
    new_password: str = Form(default=""),
    confirm_password: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request, key_prefix=f"team:pwchange:{user.id}", max_requests=8, window_seconds=900.0
    ):
        return limited
    if new_password != confirm_password:
        return RedirectResponse(
            "/team/password/change?error=New+password+and+confirmation+don%27t+match.",
            status_code=303,
        )
    try:
        user = change_user_password(
            session,
            user,
            current_password=current_password,
            new_password=new_password,
            ip_address=(request.client.host if request.client else None),
        )
    except BadCurrentPasswordError as exc:
        code = str(exc)
        message = {
            "current_password_required": "Enter your current password.",
            "current_password_wrong": "That's not your current password.",
        }.get(code, "Could not verify your current password.")
        from urllib.parse import quote_plus
        return RedirectResponse(
            f"/team/password/change?error={quote_plus(message)}",
            status_code=303,
        )
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        return RedirectResponse(
            f"/team/password/change?{qs}",
            status_code=303,
        )
    except ValueError as exc:
        code = str(exc)
        message = {
            "new_password_required": "Choose a new password.",
            "new_password_same_as_current": "Your new password has to be different from the current one.",
        }.get(code, "Could not update password.")
        from urllib.parse import quote_plus
        return RedirectResponse(
            f"/team/password/change?error={quote_plus(message)}",
            status_code=303,
        )
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)
    return RedirectResponse(
        "/team/profile?flash=Password+updated.",
        status_code=303,
    )


@router.get("/team/documents")
def team_documents():
    return RedirectResponse("/team/inbox?filter=documents", status_code=303)


@router.get("/team/policies", response_class=HTMLResponse)
def team_policies(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.policies")
    if denial:
        return denial
    policies = _published_policies(session)
    acknowledged = _policy_acknowledgements_for(session, user)
    return templates.TemplateResponse(
        request,
        "team/policies.html",
        {
            "request": request,
            "title": "Policies",
            "active": "policies",
            "current_user": user,
            "policies": policies,
            "acknowledged": acknowledged,
            "flash": flash,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post(
    "/team/policies/acknowledge/{policy_id}",
    dependencies=[Depends(require_csrf)],
)
async def team_policies_acknowledge(
    request: Request,
    policy_id: str,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.policies")
    if denial:
        return denial
    policy = _policy_by_id(session, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="policy_not_found")
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="policy.acknowledge",
            resource_key=f"policy.{policy_id}",
            details_json=json.dumps(
                {
                    "policy_id": policy_id,
                    "policy_version": policy["version"],
                    "policy_title": policy["title"],
                    "policy_kind": policy.get("kind", "policy"),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse("/team/policies?flash=Acknowledged.", status_code=303)


def _parse_employee_week(value: Optional[str], this_week_start: date) -> date:
    """Monday of the requested week, clamped to a sane range.

    Anything unparseable falls back to the current week rather than erroring --
    this is a read-only view of the employee's own hours.
    """
    if not value:
        return this_week_start
    try:
        parsed = date.fromisoformat(value.strip())
    except (ValueError, AttributeError):
        return this_week_start
    week_start = parsed - timedelta(days=parsed.weekday())
    # No forward paging past the current week, and two years of history is
    # more than any payroll question needs.
    if week_start > this_week_start:
        return this_week_start
    earliest = this_week_start - timedelta(days=730)
    return max(week_start, earliest)


def _my_schedule_calendars(
    session: Session, user: User, week_days: list[date]
) -> list[dict[str, Any]]:
    """The current user's own schedule rows for one week, per calendar.

    Same sources the schedule grid reads (ShiftEntry per calendar, Stream
    Manager hints for Stream), limited to one person so /team/hours can put
    scheduled hours next to worked hours without building the whole grid.
    """
    from .team_admin_schedule import _stream_schedule_hint_map
    from ..models import SCHEDULE_CALENDAR_PACKING, SCHEDULE_CALENDAR_STOREFRONT

    by_kind: dict[str, dict[tuple[int, str], list[ShiftEntry]]] = {
        SCHEDULE_CALENDAR_STOREFRONT: {},
        SCHEDULE_CALENDAR_PACKING: {},
    }
    for row in session.exec(
        select(ShiftEntry)
        .where(ShiftEntry.user_id == user.id)
        .where(ShiftEntry.shift_date >= week_days[0])
        .where(ShiftEntry.shift_date <= week_days[-1])
        .order_by(ShiftEntry.shift_date, ShiftEntry.sort_order, ShiftEntry.id)
    ).all():
        bucket = by_kind.setdefault(row.calendar_kind or SCHEDULE_CALENDAR_STOREFRONT, {})
        bucket.setdefault((row.user_id, row.shift_date.isoformat()), []).append(row)
    stream_hints, _legend = _stream_schedule_hint_map(session, week_days, {user.id})
    return [
        {"kind": schedule_view.LOCATION_STOREFRONT, "label": "Storefront",
         "entries": by_kind.get(SCHEDULE_CALENDAR_STOREFRONT, {})},
        {"kind": schedule_view.LOCATION_PACKING, "label": "Packing",
         "entries": by_kind.get(SCHEDULE_CALENDAR_PACKING, {})},
        {"kind": schedule_view.LOCATION_STREAM, "label": "Stream",
         "entries": stream_hints},
    ]


@router.get("/team/hours", response_class=HTMLResponse)
def team_hours(
    request: Request,
    week: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.hours")
    if denial:
        return denial
    settings = get_settings()
    clockify_ready = clockify_is_configured(settings)
    # Employees could only ever see the current week, so they had no way to
    # check a past week against a paycheck. Reads are scoped to their own rows.
    today = _portal_today(settings=settings)
    this_week_start = today - timedelta(days=today.weekday())
    week_of = _parse_employee_week(week, this_week_start)
    # Same helper the Home tiles use, so both report the hours payroll pays
    # (breaks excluded, missed-break deduction applied).
    week_data = employee_week_hours(
        session, user, today=today, week_of=week_of, settings=settings
    )
    is_this_week = week_data["week_start"] == this_week_start
    profile = session.get(EmployeeProfile, user.id)
    clockify_user_id = (
        (profile.clockify_user_id or "").strip() if profile and week_data["linked"] else ""
    )

    # No estimated pay on this page, by design (PRD 2026-09 decision): it
    # was a guess that disagreed with real paychecks.
    hours_view: dict[str, Any] = {}
    if clockify_ready and clockify_user_id and not week_data["error"]:
        from .team_admin_clockify import _clockify_entry_is_break

        week_start = week_data["week_start"]
        week_days = [week_start + timedelta(days=i) for i in range(7)]
        my_week = schedule_view.build_my_week(
            week_days=week_days,
            today=today,
            me_id=user.id,
            calendars=_my_schedule_calendars(session, user, week_days),
            timeoff_days=_approved_timeoff_days(
                session, user.id, week_days[0], week_days[-1]
            ),
        )
        entries = [
            {
                "start_local": row.start_local,
                "end_local": row.end_local,
                "duration_seconds": row.duration_seconds,
                "running": row.running,
                "description": row.description,
                "is_break": _clockify_entry_is_break(row),
            }
            for row in week_data["entries"]
        ]
        hours_view = schedule_view.build_hours_view(
            week=week_data, my_days=my_week["days"], entries=entries, today=today
        )

    week_start = week_data["week_start"]
    return templates.TemplateResponse(
        request,
        "team/hours.html",
        {
            "request": request,
            "title": "Hours",
            "active": "hours",
            "current_user": user,
            "clockify_ready": clockify_ready,
            "clockify_user_id": clockify_user_id,
            "week": week_data,
            "hours": hours_view,
            "is_this_week": is_this_week,
            "week_label": schedule_view.week_label(week_start),
            "prev_week": (week_start - timedelta(days=7)).isoformat(),
            "next_week": (week_start + timedelta(days=7)).isoformat(),
            "this_week": this_week_start.isoformat(),
            "can_go_forward": week_start < this_week_start,
            "clockify_error": week_data["error"],
            "format_hours": format_hours,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/schedule", response_class=HTMLResponse)
def team_schedule(
    request: Request,
    week: Optional[str] = Query(default=None),
    view: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.schedule")
    if denial:
        return denial
    # Same data the admin grid reads (entry_map per calendar + Stream
    # Manager hints), reshaped into lists in app/team/schedule_view.py so a
    # phone never has to scroll a 1000px grid sideways.
    from .team_admin_schedule import _grid_context, _parse_week_start
    from ..models import (
        SCHEDULE_CALENDAR_PACKING,
        SCHEDULE_CALENDAR_STOREFRONT,
        STAFF_KIND_STREAM,
    )

    week_start = _parse_week_start(week if isinstance(week, str) else None)
    view_mode = schedule_view.normalize_view(view)
    today = _portal_today()
    # include_financials=False: pay rates were loaded for the manager labor
    # total and never shown here.
    storefront_ctx = _grid_context(
        session,
        week_start,
        staff_kind=SCHEDULE_CALENDAR_STOREFRONT,
        include_financials=False,
    )
    packing_ctx = _grid_context(
        session,
        week_start,
        staff_kind=SCHEDULE_CALENDAR_PACKING,
        include_financials=False,
    )
    stream_ctx = _grid_context(session, week_start, staff_kind=STAFF_KIND_STREAM)

    names: dict[int, str] = {}
    for ctx in (storefront_ctx, packing_ctx, stream_ctx):
        for person in ctx["users"]:
            if person.id is not None:
                names[person.id] = person.display_name or person.username
    names.setdefault(user.id, user.display_name or user.username)

    def _visible(entry_map: dict) -> dict:
        # The grid only drew rows for non-terminated users; keep that contract.
        return {key: rows for key, rows in entry_map.items() if key[0] in names}

    stream_entries = dict(stream_ctx["stream_hint_map"])
    if not any(person.id == user.id for person in stream_ctx["users"]):
        # The Stream grid auto-rosters Stream-role staff only. Someone on
        # another team who is linked to a Streamer still has stream shifts;
        # show them in their own list (and in /team/hours) too.
        from .team_admin_schedule import _stream_schedule_hint_map

        own_hints, _legend = _stream_schedule_hint_map(
            session, stream_ctx["week_days"], {user.id}
        )
        stream_entries.update(own_hints)

    calendars = [
        {"kind": schedule_view.LOCATION_STOREFRONT, "label": "Storefront",
         "entries": _visible(storefront_ctx["entry_map"])},
        {"kind": schedule_view.LOCATION_PACKING, "label": "Packing",
         "entries": _visible(packing_ctx["entry_map"])},
        {"kind": schedule_view.LOCATION_STREAM, "label": "Stream",
         "entries": _visible(stream_entries)},
    ]
    week_days = storefront_ctx["week_days"]
    day_notes = {
        iso: (note.location_label or "").strip()
        for iso, note in storefront_ctx["day_note_map"].items()
        if (note.location_label or "").strip()
    }
    my_week = schedule_view.build_my_week(
        week_days=week_days,
        today=today,
        me_id=user.id,
        calendars=calendars,
        names=names,
        timeoff_days=_approved_timeoff_days(
            session, user.id, week_days[0], week_days[-1]
        ),
        day_notes=day_notes,
    )
    team_days = (
        schedule_view.build_team_week(
            week_days=week_days,
            today=today,
            me_id=user.id,
            calendars=calendars,
            names=names,
            day_notes=day_notes,
        )
        if view_mode == schedule_view.VIEW_TEAM
        else []
    )

    nav_ctx = _nav_context(session, user)
    can_timeoff = any(item["name"] == "time-off" for item in nav_ctx["nav_items"])
    timeoff_href = ""
    if can_timeoff:
        timeoff_href = "/team/requests?new=timeoff"
        next_work = my_week["next_work_date"]
        if next_work is not None:
            timeoff_href += f"&date={next_work.isoformat()}"

    return templates.TemplateResponse(
        request,
        "team/schedule.html",
        {
            "request": request,
            "title": "Schedule",
            "active": "schedule",
            "current_user": user,
            "csrf_token": issue_token(request),
            "view": view_mode,
            "my_week": my_week,
            "my_eyebrow": schedule_view.my_week_eyebrow(my_week),
            "team_days": team_days,
            "week_start": week_start,
            "week_label": schedule_view.week_label(week_start),
            "prev_week": storefront_ctx["prev_week"],
            "next_week": storefront_ctx["next_week"],
            "this_week": storefront_ctx["this_week"],
            "is_current_week": storefront_ctx["is_current_week"],
            "timeoff_href": timeoff_href,
            "today": today,
            **nav_ctx,
        },
    )


@router.get("/team/supply")
def team_supply(
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    """Old page URL: opens the supply form on /team/requests (redesign Phase 3)."""
    params = {"new": "supply"}
    if flash:
        params["flash"] = flash
    if error:
        params["error"] = error
    return RedirectResponse(f"/team/requests?{urlencode(params)}", status_code=303)


@router.post("/team/supply", dependencies=[Depends(require_csrf)])
async def team_supply_post(
    request: Request,
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
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:supply:{user.id}",
        max_requests=10,
        window_seconds=3600.0,
    ):
        return limited
    clean_title = (title or "").strip()
    if not clean_title:
        return RedirectResponse(
            "/team/requests?new=supply&error=Title+is+required.", status_code=303
        )
    if urgency not in ("low", "normal", "high"):
        urgency = "normal"
    row = SupplyRequest(
        submitted_by_user_id=user.id,
        title=clean_title[:200],
        description=(description or "")[:4000],
        urgency=urgency,
        status="submitted",
    )
    session.add(row)
    session.flush()
    notify_manager_admins(
        session,
        actor_user_id=user.id,
        resource_key="admin.supply.view",
        kind="supply_submitted",
        title="New supply request",
        body=(
            f"{user.display_name or user.username} requested {row.title}"
            f" ({row.urgency} urgency)."
        ),
        link_path="/team/admin/supply",
        request=request,
        exclude_user_ids=[user.id] if user.id is not None else None,
        send_text=False,
    )
    session.commit()
    session.refresh(row)
    send_supply_request_alert(
        request_id=row.id,
        employee_name=user.display_name or user.username,
        employee_username=user.username,
        title=row.title,
        description=row.description,
        urgency=row.urgency,
    )
    return RedirectResponse(
        "/team/requests?flash=Supply+request+submitted.", status_code=303
    )
