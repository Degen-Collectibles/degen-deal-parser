"""Database side of the employee Inbox: load items, read markers, mark read.

Kept apart from ``app/team/inbox.py`` (pure rules) and the router so the
nav badge (computed on every portal page by ``_nav_context``) and the Inbox
page share one implementation without importing the router.

Query budget for the unread badge: at most three small indexed selects
(active announcements, the user's recent notifications, the user's read
markers), and the read-marker query is skipped when nothing could be unread.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..models import AuditLog, TeamAnnouncement, TeamInboxRead, utcnow
from . import inbox as inbox_view
from .team_notifications import EMPLOYEE_NOTIFICATION_ACTION

# Most recent notifications listed (and counted) in the Inbox. The old
# /team/notifications page showed 20.
NOTIFICATION_LIMIT = 50


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def active_announcements(
    session: Session,
    *,
    limit: Optional[int] = None,
    now: Optional[datetime] = None,
) -> list[TeamAnnouncement]:
    """Active, unexpired announcements, pinned first then newest."""
    now = now or utcnow()
    stmt = (
        select(TeamAnnouncement)
        .where(TeamAnnouncement.is_active == True)  # noqa: E712
        .where(
            or_(
                TeamAnnouncement.expires_at.is_(None),
                TeamAnnouncement.expires_at > now,
            )
        )
        .order_by(
            TeamAnnouncement.pinned.desc(),
            TeamAnnouncement.published_at.desc(),
            TeamAnnouncement.id.desc(),
        )
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.exec(stmt).all())


def _notification_filters(user_id: int) -> tuple:
    return (
        AuditLog.target_user_id == user_id,
        AuditLog.action == EMPLOYEE_NOTIFICATION_ACTION,
        or_(
            AuditLog.resource_key.is_(None),
            AuditLog.resource_key.not_in(inbox_view.FOLDED_NOTIFICATION_RESOURCE_KEYS),
        ),
    )


def notification_dict(row: AuditLog) -> dict[str, Any]:
    try:
        payload = json.loads(row.details_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "id": row.id,
        "kind": str(payload.get("kind") or "general"),
        "title": str(payload.get("title") or "Team update"),
        "body": str(payload.get("body") or ""),
        "link_path": str(payload.get("link_path") or "/team/"),
        "created_at": row.created_at,
        "sms": payload.get("sms") if isinstance(payload.get("sms"), dict) else {},
    }


def inbox_notifications(
    session: Session, user_id: int, *, limit: int = NOTIFICATION_LIMIT
) -> list[dict[str, Any]]:
    """The user's newest notifications, minus ones folded into announcements."""
    stmt = (
        select(AuditLog)
        .where(*_notification_filters(user_id))
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    )
    rows = session.exec(stmt.limit(limit)).all()
    notes = [notification_dict(row) for row in rows]
    return [n for n in notes if n["kind"] not in inbox_view.FOLDED_NOTIFICATION_KINDS]


def read_keys(
    session: Session, user_id: int, kinds: Iterable[str] = inbox_view.KINDS
) -> set[tuple[str, str]]:
    kinds = list(kinds)
    if not kinds:
        return set()
    rows = session.exec(
        select(TeamInboxRead.item_kind, TeamInboxRead.item_key)
        .where(TeamInboxRead.user_id == user_id)
        .where(TeamInboxRead.item_kind.in_(kinds))
    ).all()
    return {(str(kind), str(key)) for kind, key in rows}


# ---------------------------------------------------------------------------
# Unread count (nav badge, More row, Inbox header)
# ---------------------------------------------------------------------------

def unread_count(
    session: Session,
    user_id: int,
    *,
    kinds: Iterable[str],
    now: Optional[datetime] = None,
    documents: Iterable[dict] = inbox_view.TEAM_DOCUMENTS,
) -> int:
    """Unread items this user can see. Same rule as the Inbox list."""
    kinds = tuple(kinds)
    if not kinds:
        return 0
    now = now or utcnow()
    candidates: list[tuple[str, str, Optional[datetime], bool]] = []
    if inbox_view.KIND_ANNOUNCEMENT in kinds:
        rows = session.exec(
            select(
                TeamAnnouncement.id,
                TeamAnnouncement.published_at,
                TeamAnnouncement.pinned,
            )
            .where(TeamAnnouncement.is_active == True)  # noqa: E712
            .where(
                or_(
                    TeamAnnouncement.expires_at.is_(None),
                    TeamAnnouncement.expires_at > now,
                )
            )
        ).all()
        candidates += [
            (inbox_view.KIND_ANNOUNCEMENT, str(ann_id), published, bool(pinned))
            for ann_id, published, pinned in rows
        ]
    if inbox_view.KIND_NOTIFICATION in kinds:
        cutoff = now - inbox_view.STALE_AFTER
        rows = session.exec(
            select(AuditLog.id, AuditLog.created_at)
            .where(*_notification_filters(user_id))
            .where(AuditLog.created_at >= cutoff)
            .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
            .limit(NOTIFICATION_LIMIT)
        ).all()
        candidates += [
            (inbox_view.KIND_NOTIFICATION, str(note_id), created, False)
            for note_id, created in rows
        ]
    if inbox_view.KIND_DOCUMENT in kinds:
        candidates += [
            (
                inbox_view.KIND_DOCUMENT,
                inbox_view.document_key(doc),
                inbox_view.document_time(doc),
                False,
            )
            for doc in documents
        ]
    live = [c for c in candidates if not inbox_view.is_stale(c[2], now, pinned=c[3])]
    if not live:
        return 0
    return inbox_view.count_unread(live, read_keys(session, user_id, kinds), now)


# ---------------------------------------------------------------------------
# Marking read
# ---------------------------------------------------------------------------

def item_exists_for(session: Session, user_id: int, kind: str, key: str) -> bool:
    """Only let a user mark items they could actually see in the Inbox."""
    key = str(key or "").strip()
    if not key:
        return False
    if kind == inbox_view.KIND_DOCUMENT:
        return inbox_view.document_by_key(key) is not None
    if not key.isdigit():
        return False
    if kind == inbox_view.KIND_ANNOUNCEMENT:
        row = session.get(TeamAnnouncement, int(key))
        return row is not None and bool(row.is_active)
    if kind == inbox_view.KIND_NOTIFICATION:
        row = session.get(AuditLog, int(key))
        return (
            row is not None
            and row.target_user_id == user_id
            and row.action == EMPLOYEE_NOTIFICATION_ACTION
        )
    return False


def _already_read(session: Session, user_id: int, kind: str, key: str) -> bool:
    return (
        session.exec(
            select(TeamInboxRead.id)
            .where(TeamInboxRead.user_id == user_id)
            .where(TeamInboxRead.item_kind == kind)
            .where(TeamInboxRead.item_key == key)
        ).first()
        is not None
    )


def mark_read(
    session: Session,
    user_id: int,
    kind: str,
    key: str,
    *,
    now: Optional[datetime] = None,
) -> bool:
    """Record that `user_id` opened (kind, key). Idempotent on both engines.

    Returns True if a new marker was written. A duplicate (already read, or
    a concurrent request that won the race) hits the unique constraint
    inside a SAVEPOINT, which is rolled back without touching the outer
    transaction. Caller commits.
    """
    key = str(key)
    if _already_read(session, user_id, kind, key):
        return False
    try:
        with session.begin_nested():
            session.add(
                TeamInboxRead(
                    user_id=user_id,
                    item_kind=kind,
                    item_key=key,
                    read_at=now or utcnow(),
                )
            )
    except IntegrityError:
        return False
    return True


def mark_all_read(
    session: Session,
    user_id: int,
    items: Iterable[tuple[str, str]],
    *,
    now: Optional[datetime] = None,
) -> int:
    """Mark every (kind, key) read. Returns how many markers were written."""
    now = now or utcnow()
    wanted = {(str(kind), str(key)) for kind, key in items}
    if not wanted:
        return 0
    have = read_keys(session, user_id, {kind for kind, _ in wanted})
    written = 0
    for kind, key in sorted(wanted - have):
        if mark_read(session, user_id, kind, key, now=now):
            written += 1
    return written
