"""Pure view-model builders for the employee Inbox (/team/inbox).

The Inbox merges three sources into one list with read/unread state
(portal redesign PRD, Phase 4):

* announcements  -- active, unexpired ``TeamAnnouncement`` rows
* updates        -- employee notifications (``AuditLog`` rows with action
                    ``employee.notification`` targeted at the user)
* documents      -- the static ``TEAM_DOCUMENTS`` list below

Read state lives in ``TeamInboxRead`` rows keyed by (kind, key). Loading
rows is done by ``app/team/inbox_store.py``; everything here is plain data
in, plain data out, so the unread rules, filters, ordering and labels are
unit-testable without a database or a template.
"""
from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import unquote, urlparse

from .home import month_day

KIND_ANNOUNCEMENT = "announcement"
KIND_NOTIFICATION = "notification"
KIND_DOCUMENT = "document"
KINDS = (KIND_ANNOUNCEMENT, KIND_NOTIFICATION, KIND_DOCUMENT)

FILTER_ALL = "all"
FILTER_ANNOUNCEMENTS = "announcements"
FILTER_UPDATES = "updates"
FILTER_DOCUMENTS = "documents"
FILTER_KIND = {
    FILTER_ANNOUNCEMENTS: KIND_ANNOUNCEMENT,
    FILTER_UPDATES: KIND_NOTIFICATION,
    FILTER_DOCUMENTS: KIND_DOCUMENT,
}
KIND_FILTER = {kind: name for name, kind in FILTER_KIND.items()}
# (value, label, short label for narrow phones)
FILTER_LABELS = (
    (FILTER_ALL, "All", "All"),
    (FILTER_ANNOUNCEMENTS, "Announcements", "News"),
    (FILTER_UPDATES, "Updates", "Updates"),
    (FILTER_DOCUMENTS, "Documents", "Docs"),
)

# Items older than this count as read even without a TeamInboxRead row, so
# the first rollout (and a new hire's first login) doesn't open on a pile of
# months-old "unread" updates. Pinned announcements are exempt: pinning is
# how a manager says "everyone should see this".
STALE_AFTER = timedelta(days=30)

# The admin "publish announcement" flow also sends every employee a
# notification (kind "announcement") linking to the announcement. The Inbox
# already lists the announcement itself, so those notifications are folded
# into it rather than shown (and counted) twice. The poll endpoint and SMS
# still use them unchanged.
FOLDED_NOTIFICATION_KINDS = frozenset({"announcement"})
FOLDED_NOTIFICATION_RESOURCE_KEYS = tuple(
    f"employee.notification.{kind}" for kind in sorted(FOLDED_NOTIFICATION_KINDS)
)

# Notification SMS statuses that mean no text was attempted; anything else
# (queued / sent / delivered / failed ...) is worth showing on the row.
_SMS_NOT_ATTEMPTED = frozenset(
    {
        "",
        "not_requested",
        "no_phone",
        "phone_unreadable",
        "phone_invalid",
        "consent_required",
        "pilot_not_enabled",
    }
)

SNIPPET_CHARS = 140


# Documents have no table: this is the source of truth, keyed by a stable
# slug so read markers survive a file rename. Add new documents here with a
# fresh `key` and `updated` date and they show as unread for everyone.
TEAM_DOCUMENTS: tuple[dict[str, str], ...] = (
    {
        "key": "surprise-set-guide",
        "title": "TikTok Surprise Set Streamer Guide",
        "description": (
            "How to build an official TikTok Surprise Set, explain the pool, "
            "run dollar-start auctions, and keep the stream moving without "
            "making guarantees."
        ),
        "category": "TikTok Live",
        "updated": "2026-05-29",
        "href": "/static/team-documents/surprise-set-guide.pdf",
        "source_href": "/static/team-documents/surprise-set-guide.md",
    },
)


# ---------------------------------------------------------------------------
# Keys, filters, permissions
# ---------------------------------------------------------------------------

def document_key(doc: Mapping[str, Any]) -> str:
    """Stable key for a document: its slug, else its URL."""
    return str(doc.get("key") or doc.get("href") or doc.get("title") or "")


def document_by_key(key: str, documents: Sequence[Mapping[str, Any]] = TEAM_DOCUMENTS):
    for doc in documents:
        if document_key(doc) == key:
            return doc
    return None


def allowed_kinds(*, can_announcements: bool, can_documents: bool) -> tuple[str, ...]:
    """Inbox sections a user may see, from the existing page.* keys.

    Announcements and updates both ride on ``page.announcements`` (the old
    Notifications page used that key too); documents on ``page.documents``.
    """
    kinds: list[str] = []
    if can_announcements:
        kinds += [KIND_ANNOUNCEMENT, KIND_NOTIFICATION]
    if can_documents:
        kinds.append(KIND_DOCUMENT)
    return tuple(kinds)


def normalize_filter(value: Any, kinds: Iterable[str] = KINDS) -> str:
    """A filter the user can see, else 'all'."""
    if not isinstance(value, str):
        return FILTER_ALL
    name = value.strip().lower()
    if name in ("notifications", "update"):
        name = FILTER_UPDATES
    elif name in ("announcement", "news"):
        name = FILTER_ANNOUNCEMENTS
    elif name in ("document", "docs"):
        name = FILTER_DOCUMENTS
    kind = FILTER_KIND.get(name)
    return name if kind and kind in set(kinds) else FILTER_ALL


def filter_tabs(kinds: Iterable[str], current: str, unread: Mapping[str, int]) -> list[dict[str, Any]]:
    """Segmented-control tabs, only for sections the user can see.

    A single visible section gets no tabs (nothing to switch between).
    """
    kinds = set(kinds)
    visible = [
        (value, label, short)
        for value, label, short in FILTER_LABELS
        if value == FILTER_ALL or FILTER_KIND[value] in kinds
    ]
    if len(visible) <= 2:
        return []
    return [
        {
            "value": value,
            "label": label,
            "short": short,
            "href": "/team/inbox" if value == FILTER_ALL else f"/team/inbox?filter={value}",
            "current": value == current,
            "unread": int(unread.get(value, 0)),
        }
        for value, label, short in visible
    ]


def safe_local_path(value: Any, default: str = "/team/inbox") -> str:
    """Only same-site absolute paths; anything else falls back to `default`."""
    text = str(value or "").strip()
    if not text:
        return default
    decoded = unquote(text).strip()
    if not decoded.startswith("/") or decoded.startswith("//") or decoded.startswith("/\\"):
        return default
    parsed = urlparse(decoded)
    if parsed.scheme or parsed.netloc:
        return default
    return text


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """SQLite hands back naive datetimes; every stored time here is UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def document_time(doc: Mapping[str, Any], tz: tzinfo = timezone.utc) -> Optional[datetime]:
    """A document's `updated` date as midnight local time, in UTC."""
    raw = doc.get("updated")
    if isinstance(raw, datetime):
        return as_utc(raw)
    if isinstance(raw, date):
        day = raw
    else:
        try:
            day = date.fromisoformat(str(raw or "").strip())
        except ValueError:
            return None
    return datetime.combine(day, time(0, 0), tz).astimezone(timezone.utc)


def when_label(when: Optional[datetime], now: datetime, tz: tzinfo = timezone.utc) -> str:
    """'Just now' / '5m ago' / '3h ago' / 'Yesterday' / 'Mon' / 'Sep 20' / 'Sep 20, 2025'."""
    when = as_utc(when)
    now = as_utc(now)
    if when is None or now is None:
        return ""
    local_when = when.astimezone(tz)
    local_now = now.astimezone(tz)
    delta = now - when
    if delta < timedelta(minutes=1):
        return "Just now"
    if delta < timedelta(hours=1):
        return f"{int(delta.total_seconds() // 60)}m ago"
    days = (local_now.date() - local_when.date()).days
    if days <= 0:
        return f"{int(delta.total_seconds() // 3600)}h ago"
    if days == 1:
        return "Yesterday"
    if days < 7:
        return f"{local_when:%a}"
    if local_when.year == local_now.year:
        return month_day(local_when.date())
    return f"{month_day(local_when.date())}, {local_when.year}"


def is_stale(when: Optional[datetime], now: datetime, *, pinned: bool = False) -> bool:
    if pinned:
        return False
    when = as_utc(when)
    if when is None:
        return False
    return as_utc(now) - when > STALE_AFTER


def is_unread(
    kind: str,
    key: str,
    when: Optional[datetime],
    read_keys: Iterable[tuple[str, str]] | set,
    now: datetime,
    *,
    pinned: bool = False,
) -> bool:
    if (kind, str(key)) in read_keys:
        return False
    return not is_stale(when, now, pinned=pinned)


def count_unread(
    candidates: Iterable[tuple[str, str, Optional[datetime], bool]],
    read_keys: set,
    now: datetime,
) -> int:
    """Unread count from (kind, key, when, pinned) tuples -- same rule the list uses."""
    seen: set[tuple[str, str]] = set()
    total = 0
    for kind, key, when, pinned in candidates:
        ident = (kind, str(key))
        if ident in seen:
            continue
        seen.add(ident)
        if is_unread(kind, str(key), when, read_keys, now, pinned=pinned):
            total += 1
    return total


# ---------------------------------------------------------------------------
# Item builders
# ---------------------------------------------------------------------------

def snippet(text: Any, limit: int = SNIPPET_CHARS) -> str:
    flat = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(flat) <= limit:
        return flat
    return flat[: limit - 1].rstrip() + "…"


def notification_style(kind: str) -> tuple[str, str]:
    """(icon, tone) for a notification kind."""
    kind = (kind or "").lower()
    if kind.startswith("schedule"):
        return "calendar", "info"
    if kind.startswith("timeoff"):
        return "beach", "info"
    if kind.startswith("supply"):
        return "box", "purple"
    if kind.startswith("policy"):
        return "shield", "warn"
    if kind.startswith("announcement"):
        return "megaphone", "accent"
    return "bell", ""


def _sms_meta(sms: Any) -> str:
    if not isinstance(sms, Mapping):
        return ""
    status = str(sms.get("status") or "")
    if status in _SMS_NOT_ATTEMPTED:
        return ""
    shown = str(sms.get("delivery_status") or status).replace("_", " ")
    return f"Text {shown}"


def announcement_item(
    row: Any,
    *,
    read_keys: set,
    now: datetime,
    tz: tzinfo = timezone.utc,
    author: str = "",
) -> dict[str, Any]:
    key = str(row.id)
    when = as_utc(getattr(row, "published_at", None))
    pinned = bool(getattr(row, "pinned", False))
    meta = [part for part in ("Pinned" if pinned else "", f"From {author}" if author else "") if part]
    return {
        "kind": KIND_ANNOUNCEMENT,
        "key": key,
        "uid": f"{KIND_ANNOUNCEMENT}-{key}",
        "kind_label": "Announcement",
        "title": str(getattr(row, "title", "") or "Announcement"),
        "snippet": snippet(getattr(row, "body", "")),
        "when": when,
        "when_label": when_label(when, now, tz),
        "pinned": pinned,
        "unread": is_unread(KIND_ANNOUNCEMENT, key, when, read_keys, now, pinned=pinned),
        "icon": "megaphone",
        "tone": "accent",
        "meta": " · ".join(meta),
        "href": f"/team/inbox/announcements/{key}",
        "external": False,
    }


def notification_item(
    note: Mapping[str, Any],
    *,
    read_keys: set,
    now: datetime,
    tz: tzinfo = timezone.utc,
) -> dict[str, Any]:
    key = str(note.get("id"))
    when = as_utc(note.get("created_at"))
    icon, tone = notification_style(str(note.get("kind") or ""))
    return {
        "kind": KIND_NOTIFICATION,
        "key": key,
        "uid": f"{KIND_NOTIFICATION}-{key}",
        "kind_label": "Update",
        "title": str(note.get("title") or "Team update"),
        "snippet": snippet(note.get("body")),
        "when": when,
        "when_label": when_label(when, now, tz),
        "pinned": False,
        "unread": is_unread(KIND_NOTIFICATION, key, when, read_keys, now),
        "icon": icon,
        "tone": tone,
        "meta": _sms_meta(note.get("sms")),
        "href": safe_local_path(note.get("link_path"), default="/team/"),
        "external": False,
    }


def document_item(
    doc: Mapping[str, Any],
    *,
    read_keys: set,
    now: datetime,
    tz: tzinfo = timezone.utc,
) -> dict[str, Any]:
    key = document_key(doc)
    when = document_time(doc, tz)
    updated = when.astimezone(tz).date() if when else None
    meta = [str(doc.get("category") or "")]
    if updated:
        meta.append(f"Updated {month_day(updated)}")
    return {
        "kind": KIND_DOCUMENT,
        "key": key,
        "uid": f"{KIND_DOCUMENT}-{key}",
        "kind_label": "Document",
        "title": str(doc.get("title") or "Document"),
        "snippet": snippet(doc.get("description")),
        "when": when,
        "when_label": when_label(when, now, tz),
        "pinned": False,
        "unread": is_unread(KIND_DOCUMENT, key, when, read_keys, now),
        "icon": "doc",
        "tone": "purple",
        "meta": " · ".join(part for part in meta if part),
        "href": str(doc.get("href") or "/team/inbox"),
        "external": True,
    }


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def sort_items(items: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Pinned announcements first, then everything newest first."""
    return sorted(
        items,
        key=lambda item: (
            0 if item.get("pinned") else 1,
            -((as_utc(item.get("when")) or _EPOCH).timestamp()),
            item.get("kind", ""),
            str(item.get("key", "")),
        ),
    )


def build_inbox(
    *,
    announcements: Sequence[Any] = (),
    notifications: Sequence[Mapping[str, Any]] = (),
    documents: Sequence[Mapping[str, Any]] = (),
    kinds: Iterable[str] = KINDS,
    read_keys: set,
    now: datetime,
    tz: tzinfo = timezone.utc,
    filter_value: Any = None,
    authors: Optional[Mapping[int, str]] = None,
) -> dict[str, Any]:
    """Everything /team/inbox renders."""
    kinds = tuple(kinds)
    authors = authors or {}
    items: list[dict[str, Any]] = []
    if KIND_ANNOUNCEMENT in kinds:
        items += [
            announcement_item(
                row,
                read_keys=read_keys,
                now=now,
                tz=tz,
                author=authors.get(getattr(row, "created_by_user_id", None), ""),
            )
            for row in announcements
        ]
    if KIND_NOTIFICATION in kinds:
        items += [
            notification_item(note, read_keys=read_keys, now=now, tz=tz)
            for note in notifications
            if str(note.get("kind") or "") not in FOLDED_NOTIFICATION_KINDS
        ]
    if KIND_DOCUMENT in kinds:
        items += [document_item(doc, read_keys=read_keys, now=now, tz=tz) for doc in documents]
    items = sort_items(items)

    current = normalize_filter(filter_value, kinds)
    unread = {FILTER_ALL: sum(1 for item in items if item["unread"])}
    for name, kind in FILTER_KIND.items():
        unread[name] = sum(1 for item in items if item["kind"] == kind and item["unread"])
    shown = (
        items
        if current == FILTER_ALL
        else [item for item in items if item["kind"] == FILTER_KIND[current]]
    )
    return {
        "filter": current,
        "tabs": filter_tabs(kinds, current, unread),
        "rows": shown,
        "unread": unread,
        "unread_total": unread[FILTER_ALL],
        "empty_text": _empty_text(current, kinds),
    }


def _empty_text(current: str, kinds: Sequence[str]) -> str:
    if current == FILTER_ANNOUNCEMENTS:
        return "No announcements right now."
    if current == FILTER_UPDATES:
        return "No updates for you yet."
    if current == FILTER_DOCUMENTS:
        return "No documents are posted yet."
    return "Nothing here yet."
