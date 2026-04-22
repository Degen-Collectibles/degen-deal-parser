"""
/team/admin/invites/* — invite issuance + revocation (Wave 4).

Admin copies the link manually (no SMTP). Each invite is displayed exactly
ONCE upon issuance; there is no resend.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..auth import generate_invite_token
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, InviteToken, User, utcnow
from ..shared import templates
from .team_admin import _admin_gate

router = APIRouter()


ROLES = ("employee", "viewer", "manager", "reviewer", "admin")


def _base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"


def _as_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Force a datetime into tz-aware UTC.

    SQLite round-trips datetimes as tz-naive even when we store them
    aware, so any Python-side comparison between a stored `expires_at`
    and `utcnow()` raises `TypeError: can't compare offset-naive and
    offset-aware datetimes`. We normalize on read before the template
    does any comparing.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _invite_status(inv: InviteToken, now: datetime) -> str:
    """Render-time status, computed server-side so the template never
    has to compare datetimes (and therefore can't blow up on tz
    mismatches again)."""
    if inv.used_at is not None:
        return "used/revoked"
    expires = _as_utc_aware(inv.expires_at)
    if expires is not None and expires <= now:
        return "expired"
    return "outstanding"


@router.get("/team/admin/invites", response_class=HTMLResponse)
def admin_invites_list(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.invites.view")
    if denial:
        return denial
    now = utcnow()
    outstanding = list(
        session.exec(
            select(InviteToken)
            .where(InviteToken.used_at.is_(None), InviteToken.expires_at > now)
            .order_by(InviteToken.created_at.desc())
        ).all()
    )
    history_cutoff = now - timedelta(days=30)
    recent = list(
        session.exec(
            select(InviteToken)
            .where(InviteToken.created_at >= history_cutoff)
            .order_by(InviteToken.created_at.desc())
        ).all()
    )
    creator_ids = {row.created_by_user_id for row in recent + outstanding if row.created_by_user_id}
    creators: dict[int, User] = {}
    if creator_ids:
        creators = {
            u.id: u
            for u in session.exec(select(User).where(User.id.in_(creator_ids))).all()
        }

    # Pre-compute per-row status + tz-aware fields so the template stays
    # dumb and stops comparing datetimes.
    outstanding_rows = [
        {"inv": inv, "status": _invite_status(inv, now)} for inv in outstanding
    ]
    recent_rows = [
        {"inv": inv, "status": _invite_status(inv, now)} for inv in recent
    ]

    return templates.TemplateResponse(
        request,
        "team/admin/invites.html",
        {
            "request": request,
            "title": "Invites",
            "current_user": current,
            "outstanding": outstanding,
            "recent": recent,
            "outstanding_rows": outstanding_rows,
            "recent_rows": recent_rows,
            "creators": creators,
            "roles": ROLES,
            "now": now,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/invites/issue",
    dependencies=[Depends(require_csrf)],
)
async def admin_invites_issue(
    request: Request,
    role: str = Form(default="employee"),
    email_hint: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.invites.issue")
    if denial:
        return denial
    role_clean = (role or "").strip().lower()
    if role_clean not in ROLES:
        role_clean = "employee"
    hint = (email_hint or "").strip() or None
    raw = generate_invite_token(
        session,
        role=role_clean,
        created_by_user_id=current.id,
        email_hint=hint,
    )
    # Audit the issuance explicitly (generate_invite_token does not audit).
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="invite.issued",
            resource_key="admin.invites.issue",
            details_json=json.dumps(
                {"role": role_clean, "email_hint": hint}
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    invite_url = f"{_base_url(request)}/team/invite/accept/{raw}"
    return templates.TemplateResponse(
        request,
        "team/admin/invite_issued.html",
        {
            "request": request,
            "title": "Invite issued",
            "current_user": current,
            "invite_url": invite_url,
            "role": role_clean,
            "email_hint": hint or "",
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/invites/{invite_id}/revoke",
    dependencies=[Depends(require_csrf)],
)
async def admin_invites_revoke(
    request: Request,
    invite_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.invites.issue")
    if denial:
        return denial
    row = session.get(InviteToken, invite_id)
    if row is None:
        return HTMLResponse("Invite not found", status_code=404)
    now = utcnow()
    if row.used_at is None:
        row.used_at = now
        session.add(row)
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="invite.revoked",
            resource_key="admin.invites.issue",
            details_json=json.dumps({"invite_id": invite_id}),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse("/team/admin/invites", status_code=303)
