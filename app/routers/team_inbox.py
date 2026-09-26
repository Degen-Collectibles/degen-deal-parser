"""
/team/inbox — one list for announcements, updates and documents (redesign
Phase 4), with per-user read/unread state in ``TeamInboxRead``.

Opening an item is a state change, so it goes through POST + CSRF:

* ``POST /team/inbox/open`` marks the item read and 303s to it (announcement
  detail page, the notification's own link, or the document). Every row in
  the list is a small form, so this works without JavaScript.
* ``POST /team/inbox/read`` is the JSON twin used by /static/portal-inbox.js
  for documents, which open in a new tab (a form POST into a new tab would
  lose the session inside the installed iOS app).
* ``POST /team/inbox/read-all`` marks everything in the current filter read.

The redirect target for a notification comes from the stored row, never
from the form, and is limited to same-site paths.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlmodel import Session, select

from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, TeamAnnouncement, User, utcnow
from ..shared import templates
from ..team import inbox as inbox_view
from ..team import inbox_store
from ..team.sms_consent import consent_context
from .team import _inbox_kinds_for, _nav_context, _portal_now, _require_employee

router = APIRouter()

_NO_ACCESS = "You do not have permission to view this page."


def _gate(request: Request, session: Session):
    """(denial, user, kinds). 403 when the user may see no Inbox section."""
    denial, user = _require_employee(request, session)
    if denial:
        return denial, None, ()
    kinds = _inbox_kinds_for(session, user)
    if not kinds:
        return HTMLResponse(_NO_ACCESS, status_code=403), None, ()
    return None, user, kinds


def _author_names(session: Session, rows) -> dict[int, str]:
    ids = {row.created_by_user_id for row in rows if row.created_by_user_id is not None}
    if not ids:
        return {}
    return {
        author.id: (author.display_name or author.username or "")
        for author in session.exec(select(User).where(User.id.in_(ids))).all()
        if author.id is not None
    }


def _inbox_view_model(session: Session, user: User, kinds, *, filter_value=None) -> dict:
    now = utcnow()
    tz = _portal_now(now=now).tzinfo
    announcements = (
        inbox_store.active_announcements(session, now=now)
        if inbox_view.KIND_ANNOUNCEMENT in kinds
        else []
    )
    return inbox_view.build_inbox(
        announcements=announcements,
        notifications=(
            inbox_store.inbox_notifications(session, user.id)
            if inbox_view.KIND_NOTIFICATION in kinds
            else []
        ),
        documents=inbox_view.TEAM_DOCUMENTS if inbox_view.KIND_DOCUMENT in kinds else [],
        kinds=kinds,
        read_keys=inbox_store.read_keys(session, user.id, kinds),
        now=now,
        tz=tz,
        filter_value=filter_value,
        authors=_author_names(session, announcements),
    )


def _filter_href(value: str) -> str:
    if value and value != inbox_view.FILTER_ALL:
        return f"/team/inbox?filter={value}"
    return "/team/inbox"


@router.get("/team/inbox", response_class=HTMLResponse)
def team_inbox(
    request: Request,
    filter: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user, kinds = _gate(request, session)
    if denial:
        return denial
    inbox = _inbox_view_model(session, user, kinds, filter_value=filter)
    nav_ctx = _nav_context(session, user)
    show_alerts = inbox_view.KIND_NOTIFICATION in kinds
    return templates.TemplateResponse(
        request,
        "team/inbox.html",
        {
            "request": request,
            "title": "Inbox",
            "active": "inbox",
            "current_user": user,
            "inbox": inbox,
            "show_alerts": show_alerts,
            "sms_enabled": (
                bool(consent_context(session, user.id)["opted_in"]) if show_alerts else False
            ),
            "flash": (flash or "")[:120] or None,
            "csrf_token": issue_token(request),
            **nav_ctx,
        },
    )


def _visible_announcement(session: Session, announcement_id: int) -> Optional[TeamAnnouncement]:
    now = utcnow()
    row = session.get(TeamAnnouncement, announcement_id)
    if row is None or not row.is_active:
        return None
    expires = inbox_view.as_utc(row.expires_at)
    if expires is not None and expires <= now:
        return None
    return row


@router.get("/team/inbox/announcements/{announcement_id}", response_class=HTMLResponse)
def team_inbox_announcement(
    request: Request,
    announcement_id: int,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.announcements")
    if denial:
        return denial
    row = _visible_announcement(session, announcement_id)
    if row is None:
        return HTMLResponse("Announcement not found", status_code=404)
    now = utcnow()
    tz = _portal_now(now=now).tzinfo
    author = _author_names(session, [row]).get(row.created_by_user_id, "")
    item = inbox_view.announcement_item(
        row,
        read_keys=inbox_store.read_keys(session, user.id, (inbox_view.KIND_ANNOUNCEMENT,)),
        now=now,
        tz=tz,
        author=author,
    )
    published = inbox_view.as_utc(row.published_at)
    return templates.TemplateResponse(
        request,
        "team/inbox_announcement.html",
        {
            "request": request,
            "title": row.title or "Announcement",
            "active": "inbox",
            "current_user": user,
            "announcement": row,
            "item": item,
            "posted_label": (
                inbox_view.month_day(published.astimezone(tz).date()) if published else ""
            ),
            "author": author,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


def _target_for(session: Session, user: User, kind: str, key: str) -> str:
    """Where opening (kind, key) goes. Read from our own data, not the form."""
    if kind == inbox_view.KIND_ANNOUNCEMENT:
        return f"/team/inbox/announcements/{key}"
    if kind == inbox_view.KIND_DOCUMENT:
        doc = inbox_view.document_by_key(key)
        return str(doc.get("href")) if doc else "/team/inbox"
    if kind == inbox_view.KIND_NOTIFICATION and key.isdigit():
        row = session.get(AuditLog, int(key))
        if row is not None and row.target_user_id == user.id:
            note = inbox_store.notification_dict(row)
            return inbox_view.safe_local_path(note["link_path"], default="/team/")
    return "/team/inbox"


def _mark(session: Session, user: User, kinds, kind: str, key: str) -> bool:
    kind = (kind or "").strip()
    key = (key or "").strip()
    if kind not in kinds or not inbox_store.item_exists_for(session, user.id, kind, key):
        return False
    inbox_store.mark_read(session, user.id, kind, key)
    session.commit()
    return True


@router.post("/team/inbox/open", dependencies=[Depends(require_csrf)])
def team_inbox_open(
    request: Request,
    kind: str = Form(default=""),
    key: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user, kinds = _gate(request, session)
    if denial:
        return denial
    if not _mark(session, user, kinds, kind, key):
        return RedirectResponse("/team/inbox", status_code=303)
    return RedirectResponse(_target_for(session, user, kind.strip(), key.strip()), status_code=303)


@router.post("/team/inbox/read", dependencies=[Depends(require_csrf)])
def team_inbox_read(
    request: Request,
    kind: str = Form(default=""),
    key: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user, kinds = _gate(request, session)
    if denial:
        return denial
    if not _mark(session, user, kinds, kind, key):
        return JSONResponse({"ok": False, "error": "unknown_item"}, status_code=404)
    return {"ok": True, "unread": inbox_store.unread_count(session, user.id, kinds=kinds)}


@router.post("/team/inbox/read-all", dependencies=[Depends(require_csrf)])
def team_inbox_read_all(
    request: Request,
    filter: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user, kinds = _gate(request, session)
    if denial:
        return denial
    inbox = _inbox_view_model(session, user, kinds, filter_value=filter)
    written = inbox_store.mark_all_read(
        session,
        user.id,
        [(item["kind"], item["key"]) for item in inbox["rows"] if item["unread"]],
    )
    session.commit()
    target = _filter_href(inbox["filter"])
    sep = "&" if "?" in target else "?"
    message = "Marked+all+as+read." if written else "Nothing+unread."
    return RedirectResponse(f"{target}{sep}flash={message}", status_code=303)
