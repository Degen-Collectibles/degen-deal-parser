"""
/team/admin/supply/* — supply approval queue (Wave 4).

Managers + admins may view + approve. Deny + mark-ordered share the same
permission key.
"""
from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, SupplyRequest, User, utcnow
from ..shared import templates
from .team_admin import _permission_gate

router = APIRouter()


VALID_STATUSES = ("submitted", "approved", "denied", "ordered")


@router.get("/team/admin/supply", response_class=HTMLResponse)
def admin_supply_list(
    request: Request,
    status: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.supply.view")
    if denial:
        return denial
    filter_status = status if status in VALID_STATUSES else None
    stmt = select(SupplyRequest)
    if filter_status:
        stmt = stmt.where(SupplyRequest.status == filter_status)
    stmt = stmt.order_by(SupplyRequest.created_at.asc())
    rows = list(session.exec(stmt).all())

    submitter_ids = {r.submitted_by_user_id for r in rows}
    submitters: dict[int, User] = {}
    if submitter_ids:
        submitters = {
            u.id: u
            for u in session.exec(
                select(User).where(User.id.in_(submitter_ids))
            ).all()
        }

    counts = {s: 0 for s in VALID_STATUSES}
    for row in session.exec(select(SupplyRequest)).all():
        counts[row.status] = counts.get(row.status, 0) + 1

    return templates.TemplateResponse(
        request,
        "team/admin/supply.html",
        {
            "request": request,
            "title": "Supply queue",
            "current_user": current,
            "requests": rows,
            "submitters": submitters,
            "filter_status": filter_status,
            "statuses": VALID_STATUSES,
            "counts": counts,
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


def _transition(
    session: Session,
    *,
    request_id: int,
    actor: User,
    new_status: str,
    action: str,
    notes: str = "",
    request: Optional[Request] = None,
) -> Optional[HTMLResponse]:
    row = session.get(SupplyRequest, request_id)
    if row is None:
        return HTMLResponse("Supply request not found", status_code=404)
    now = utcnow()
    row.status = new_status
    row.status_changed_at = now
    row.updated_at = now
    if new_status in ("approved", "denied"):
        row.approved_by_user_id = actor.id
    if notes:
        row.notes = (notes[:2000] if notes else row.notes)
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=actor.id,
            action=action,
            resource_key="admin.supply.approve",
            details_json=json.dumps(
                {"supply_request_id": request_id, "status": new_status}
            ),
            ip_address=(request.client.host if request and request.client else None),
        )
    )
    session.commit()
    return None


@router.post(
    "/team/admin/supply/{request_id}/approve",
    dependencies=[Depends(require_csrf)],
)
async def admin_supply_approve(
    request: Request,
    request_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    err = _transition(
        session,
        request_id=request_id,
        actor=current,
        new_status="approved",
        action="supply.approved",
        request=request,
    )
    if err:
        return err
    return RedirectResponse(
        "/team/admin/supply?flash=Approved.", status_code=303
    )


@router.post(
    "/team/admin/supply/{request_id}/deny",
    dependencies=[Depends(require_csrf)],
)
async def admin_supply_deny(
    request: Request,
    request_id: int,
    notes: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    err = _transition(
        session,
        request_id=request_id,
        actor=current,
        new_status="denied",
        action="supply.denied",
        notes=notes,
        request=request,
    )
    if err:
        return err
    return RedirectResponse(
        "/team/admin/supply?flash=Denied.", status_code=303
    )


@router.post(
    "/team/admin/supply/{request_id}/mark-ordered",
    dependencies=[Depends(require_csrf)],
)
async def admin_supply_mark_ordered(
    request: Request,
    request_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    err = _transition(
        session,
        request_id=request_id,
        actor=current,
        new_status="ordered",
        action="supply.ordered",
        request=request,
    )
    if err:
        return err
    return RedirectResponse(
        "/team/admin/supply?flash=Marked+ordered.", status_code=303
    )
