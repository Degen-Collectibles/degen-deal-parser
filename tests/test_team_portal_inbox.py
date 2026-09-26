"""Employee Inbox (2026-09 portal redesign, Phase 4).

Pure rules in app/team/inbox.py are tested without a database. The store
(app/team/inbox_store.py) and routes (app/routers/team_inbox.py) run against
an in-memory SQLite DB with the same direct-call harness as the Schedule /
Requests tests; CSRF and redirects go through the real app + TestClient.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlmodel import select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-inbox")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-inbox")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-inbox")

from tests.test_team_schedule_view import _RouteHarness  # noqa: E402

LA = ZoneInfo("America/Los_Angeles")
NOW = datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc)  # Fri 12:00 PT
ME = 10
OTHER = 11
ADMIN = 20
FRESH_DOC = {
    "key": "new-handbook",
    "title": "Store handbook",
    "description": "Opening, closing and cash handling.",
    "category": "Store",
    "updated": "2026-09-20",
    "href": "/static/team-documents/handbook.pdf",
}


def _ann(id, title="Restock Friday", *, pinned=False, published=None, body="Details", author=ADMIN):
    return SimpleNamespace(
        id=id,
        title=title,
        body=body,
        pinned=pinned,
        published_at=published or NOW - timedelta(hours=2),
        created_by_user_id=author,
    )


def _note(id, *, kind="schedule_change", created=None, link="/team/schedule", sms=None):
    return {
        "id": id,
        "kind": kind,
        "title": f"Update {id}",
        "body": "Your shift moved.",
        "link_path": link,
        "created_at": created or NOW - timedelta(hours=1),
        "sms": sms or {},
    }


# ---------------------------------------------------------------------------
# Pure rules
# ---------------------------------------------------------------------------


class InboxRuleTests(unittest.TestCase):
    def test_allowed_kinds_follow_page_keys(self):
        from app.team.inbox import allowed_kinds

        self.assertEqual(
            allowed_kinds(can_announcements=True, can_documents=True),
            ("announcement", "notification", "document"),
        )
        self.assertEqual(allowed_kinds(can_announcements=False, can_documents=True), ("document",))
        self.assertEqual(
            allowed_kinds(can_announcements=True, can_documents=False),
            ("announcement", "notification"),
        )
        self.assertEqual(allowed_kinds(can_announcements=False, can_documents=False), ())

    def test_normalize_filter(self):
        from app.team.inbox import normalize_filter

        self.assertEqual(normalize_filter("updates"), "updates")
        self.assertEqual(normalize_filter("notifications"), "updates")
        self.assertEqual(normalize_filter(" Documents "), "documents")
        self.assertEqual(normalize_filter("bogus"), "all")
        self.assertEqual(normalize_filter(None), "all")
        # A filter for a section the user can't see falls back to All.
        self.assertEqual(normalize_filter("documents", ("announcement", "notification")), "all")

    def test_list_order_pinned_first_then_newest(self):
        from app.team.inbox import build_inbox

        inbox = build_inbox(
            announcements=[
                _ann(1, "Old pinned", pinned=True, published=NOW - timedelta(days=3)),
                _ann(2, "Newer", published=NOW - timedelta(hours=5)),
            ],
            notifications=[_note(7, created=NOW - timedelta(hours=1))],
            documents=[FRESH_DOC],
            read_keys=set(),
            now=NOW,
            tz=LA,
        )
        self.assertEqual(
            [(r["kind"], r["key"]) for r in inbox["rows"]],
            [
                ("announcement", "1"),
                ("notification", "7"),
                ("announcement", "2"),
                ("document", "new-handbook"),
            ],
        )
        self.assertEqual(inbox["unread_total"], 4)
        self.assertEqual(
            inbox["unread"], {"all": 4, "announcements": 2, "updates": 1, "documents": 1}
        )

    def test_filters_limit_rows_and_mark_current_tab(self):
        from app.team.inbox import build_inbox

        common = dict(
            announcements=[_ann(1)],
            notifications=[_note(7)],
            documents=[FRESH_DOC],
            read_keys=set(),
            now=NOW,
            tz=LA,
        )
        for name, kind in (("announcements", "announcement"), ("updates", "notification"), ("documents", "document")):
            inbox = build_inbox(filter_value=name, **common)
            self.assertEqual({r["kind"] for r in inbox["rows"]}, {kind}, name)
            current = [t["value"] for t in inbox["tabs"] if t["current"]]
            self.assertEqual(current, [name])
        tabs = build_inbox(**common)["tabs"]
        self.assertEqual([t["label"] for t in tabs], ["All", "Announcements", "Updates", "Documents"])
        self.assertEqual(tabs[0]["href"], "/team/inbox")
        self.assertEqual(tabs[2]["href"], "/team/inbox?filter=updates")

    def test_documents_only_user_gets_no_tabs(self):
        from app.team.inbox import build_inbox

        inbox = build_inbox(documents=[FRESH_DOC], kinds=("document",), read_keys=set(), now=NOW)
        self.assertEqual(inbox["tabs"], [])
        self.assertEqual([r["kind"] for r in inbox["rows"]], ["document"])

    def test_announcement_notifications_fold_into_the_announcement(self):
        from app.team.inbox import build_inbox

        inbox = build_inbox(
            announcements=[_ann(1)],
            notifications=[_note(7, kind="announcement", link="/team/announcements"), _note(8)],
            read_keys=set(),
            now=NOW,
        )
        self.assertEqual(
            sorted((r["kind"], r["key"]) for r in inbox["rows"]),
            [("announcement", "1"), ("notification", "8")],
        )

    def test_read_keys_rule_ignores_age(self):
        from app.team.inbox import build_inbox, count_unread

        inbox = build_inbox(
            announcements=[
                _ann(1),
                _ann(2, published=NOW - timedelta(days=45)),
                _ann(3, pinned=True, published=NOW - timedelta(days=90)),
            ],
            notifications=[_note(7), _note(8, created=NOW - timedelta(days=31))],
            documents=[FRESH_DOC, {**FRESH_DOC, "key": "old", "updated": "2026-05-29"}],
            read_keys={("announcement", "1")},
            now=NOW,
        )
        unread = {(r["kind"], r["key"]) for r in inbox["rows"] if r["unread"]}
        # Only 1 was opened. Age doesn't matter: items that are months old
        # stay unread until the user opens them.
        self.assertEqual(
            unread,
            {
                ("announcement", "2"),
                ("announcement", "3"),
                ("notification", "7"),
                ("notification", "8"),
                ("document", "new-handbook"),
                ("document", "old"),
            },
        )
        candidates = [(r["kind"], r["key"], r["when"], r["pinned"]) for r in inbox["rows"]]
        self.assertEqual(count_unread(candidates, {("announcement", "1")}, NOW), 6)

    def test_item_fields(self):
        from app.team.inbox import build_inbox

        rows = build_inbox(
            announcements=[_ann(1, pinned=True, body="Line one\n\n  line two " + "x" * 300)],
            notifications=[
                _note(7, kind="timeoff_approved", link="//evil.example/x", sms={"status": "sent", "delivery_status": "delivered"}),
                _note(8, sms={"status": "consent_required"}),
            ],
            documents=[FRESH_DOC],
            read_keys=set(),
            now=NOW,
            tz=LA,
            authors={ADMIN: "Jef"},
        )["rows"]
        by_key = {(r["kind"], r["key"]): r for r in rows}
        ann = by_key[("announcement", "1")]
        self.assertEqual(ann["href"], "/team/inbox/announcements/1")
        self.assertEqual(ann["meta"], "Pinned · From Jef")
        self.assertTrue(ann["snippet"].startswith("Line one line two"))
        self.assertTrue(ann["snippet"].endswith("…"))
        self.assertLessEqual(len(ann["snippet"]), 140)
        note = by_key[("notification", "7")]
        self.assertEqual(note["href"], "/team/")  # off-site link refused
        self.assertEqual(note["icon"], "beach")
        self.assertEqual(note["meta"], "Text delivered")
        self.assertEqual(by_key[("notification", "8")]["meta"], "")
        doc = by_key[("document", "new-handbook")]
        self.assertTrue(doc["external"])
        self.assertEqual(doc["href"], FRESH_DOC["href"])
        self.assertEqual(doc["meta"], "Store · Updated Sep 20")

    def test_when_label(self):
        from app.team.inbox import when_label

        self.assertEqual(when_label(NOW - timedelta(seconds=20), NOW, LA), "Just now")
        self.assertEqual(when_label(NOW - timedelta(minutes=5), NOW, LA), "5m ago")
        self.assertEqual(when_label(NOW - timedelta(hours=3), NOW, LA), "3h ago")
        self.assertEqual(when_label(NOW - timedelta(days=1), NOW, LA), "Yesterday")
        self.assertEqual(when_label(NOW - timedelta(days=3), NOW, LA), "Tue")
        self.assertEqual(when_label(NOW - timedelta(days=20), NOW, LA), "Sep 5")
        self.assertEqual(when_label(NOW - timedelta(days=400), NOW, LA), "Aug 21, 2025")
        # Naive datetimes (SQLite) are UTC.
        self.assertEqual(when_label((NOW - timedelta(hours=3)).replace(tzinfo=None), NOW, LA), "3h ago")

    def test_safe_local_path(self):
        from app.team.inbox import safe_local_path

        self.assertEqual(safe_local_path("/team/schedule?week=2026-09-21"), "/team/schedule?week=2026-09-21")
        for bad in ("//evil.com", "https://evil.com", "/\\evil.com", "javascript:alert(1)", "team/x", ""):
            self.assertEqual(safe_local_path(bad, default="/team/"), "/team/", bad)

    def test_team_documents_have_stable_unique_keys(self):
        from app.team.inbox import TEAM_DOCUMENTS, document_key

        keys = [document_key(doc) for doc in TEAM_DOCUMENTS]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertIn("surprise-set-guide", keys)


# ---------------------------------------------------------------------------
# Table + store
# ---------------------------------------------------------------------------


class _InboxHarness(_RouteHarness):
    def setUp(self):
        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        self.jordan = self._user(OTHER, "Jordan Lee")
        self.admin = self._user(ADMIN, "Jef Boss", role="admin")
        self.addCleanup(self.session.close)
        self.now = datetime.now(timezone.utc)

    def _announcement(self, title="Restock Friday", *, active=True, pinned=False, age=timedelta(hours=2), expires=None, body="Details"):
        from app.models import TeamAnnouncement

        row = TeamAnnouncement(
            title=title,
            body=body,
            created_by_user_id=ADMIN,
            is_active=active,
            pinned=pinned,
            published_at=self.now - age,
            expires_at=expires,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _notification(self, user_id=ME, *, kind="schedule_change", title="Shift moved", link="/team/schedule", age=timedelta(hours=1)):
        from app.team.team_notifications import notify_employee

        row = notify_employee(
            self.session,
            user_id=user_id,
            actor_user_id=ADMIN,
            kind=kind,
            title=title,
            body="Your Saturday shift is now 12-8.",
            link_path=link,
            send_text=False,
        )
        row.created_at = self.now - age
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _set_permission(self, role, key, allowed):
        from app.models import RolePermission

        row = self.session.exec(
            select(RolePermission).where(RolePermission.role == role, RolePermission.resource_key == key)
        ).first()
        self.assertIsNotNone(row, key)
        row.is_allowed = allowed
        self.session.add(row)
        self.session.commit()

    def _reads(self, user_id=ME):
        from app.models import TeamInboxRead

        return sorted(
            (r.item_kind, r.item_key)
            for r in self.session.exec(select(TeamInboxRead).where(TeamInboxRead.user_id == user_id)).all()
        )

    def _count(self, user_id=ME, kinds=("announcement", "notification", "document")):
        from app.team import inbox_store

        return inbox_store.unread_count(self.session, user_id, kinds=kinds)

    def _inbox(self, user=None, filter=None):
        from app.routers import team_inbox

        return self._capture(
            team_inbox.team_inbox,
            self._request(user or self.maya, "/team/inbox"),
            filter=filter,
            flash=None,
            session=self.session,
        )

    def _open(self, kind, key, user=None):
        from app.routers import team_inbox

        return team_inbox.team_inbox_open(
            self._request(user or self.maya, "/team/inbox/open"), kind=kind, key=str(key), session=self.session
        )


class InboxTableTests(_InboxHarness, unittest.TestCase):
    def test_unique_constraint_on_user_kind_key(self):
        from sqlalchemy.exc import IntegrityError

        from app.models import TeamInboxRead

        self.session.add(TeamInboxRead(user_id=ME, item_kind="document", item_key="surprise-set-guide"))
        self.session.commit()
        # Same key for another user or another kind is fine.
        self.session.add(TeamInboxRead(user_id=OTHER, item_kind="document", item_key="surprise-set-guide"))
        self.session.add(TeamInboxRead(user_id=ME, item_kind="announcement", item_key="surprise-set-guide"))
        self.session.commit()
        self.session.add(TeamInboxRead(user_id=ME, item_kind="document", item_key="surprise-set-guide"))
        with self.assertRaises(IntegrityError):
            self.session.commit()
        self.session.rollback()

    def test_postgres_ddl_has_unique_constraint_and_string_key(self):
        from sqlalchemy.dialects import postgresql
        from sqlalchemy.schema import CreateTable

        from app.models import TeamInboxRead

        ddl = str(CreateTable(TeamInboxRead.__table__).compile(dialect=postgresql.dialect()))
        self.assertIn("CREATE TABLE team_inbox_read", ddl)
        self.assertIn("item_key VARCHAR(120) NOT NULL", ddl)
        self.assertIn("CONSTRAINT uq_team_inbox_read_item UNIQUE (user_id, item_kind, item_key)", ddl)
        self.assertIn('FOREIGN KEY(user_id) REFERENCES "user" (id)', ddl)

    def test_create_all_adds_table_to_an_existing_database(self):
        # Simulates the production upgrade: a DB built from the previous
        # schema (everything but team_inbox_read) gets the new table from the
        # same create_all() call init_db() makes, and nothing else changes.
        from sqlalchemy import create_engine, inspect

        from app.models import SQLModel

        with tempfile.TemporaryDirectory() as tmp:
            engine = create_engine(f"sqlite:///{Path(tmp) / 'old.db'}")
            old_tables = [t for name, t in SQLModel.metadata.tables.items() if name != "team_inbox_read"]
            SQLModel.metadata.create_all(engine, tables=old_tables)
            self.assertNotIn("team_inbox_read", inspect(engine).get_table_names())
            SQLModel.metadata.create_all(engine)
            insp = inspect(engine)
            self.assertIn("team_inbox_read", insp.get_table_names())
            uniques = insp.get_unique_constraints("team_inbox_read")
            self.assertEqual(
                [u["column_names"] for u in uniques], [["user_id", "item_kind", "item_key"]]
            )
            engine.dispose()

    def test_init_db_postgres_ready_check_requires_the_new_table(self):
        # The "existing Postgres schema detected" fallback only skips a
        # failed create_all when every metadata table (now including
        # team_inbox_read) already exists.
        from app.models import SQLModel

        self.assertIn("team_inbox_read", SQLModel.metadata.tables)


class InboxStoreTests(_InboxHarness, unittest.TestCase):
    def test_mark_read_is_idempotent(self):
        from app.team import inbox_store

        ann = self._announcement()
        self.assertTrue(inbox_store.mark_read(self.session, ME, "announcement", str(ann.id)))
        self.session.commit()
        self.assertFalse(inbox_store.mark_read(self.session, ME, "announcement", str(ann.id)))
        self.session.commit()
        self.assertEqual(self._reads(), [("announcement", str(ann.id))])

    def test_mark_read_race_hits_unique_constraint_without_breaking_session(self):
        from app.team import inbox_store

        ann = self._announcement()
        inbox_store.mark_read(self.session, ME, "announcement", str(ann.id))
        self.session.commit()
        # Pretend another request inserted the row after our existence check.
        with patch.object(inbox_store, "_already_read", return_value=False):
            self.assertFalse(inbox_store.mark_read(self.session, ME, "announcement", str(ann.id)))
        # Outer transaction still usable.
        self.assertTrue(inbox_store.mark_read(self.session, ME, "document", "surprise-set-guide"))
        self.session.commit()
        self.assertEqual(
            self._reads(), [("announcement", str(ann.id)), ("document", "surprise-set-guide")]
        )

    def test_unread_count_rules(self):
        from app.team import inbox_store

        visible = self._announcement("Visible")
        self._announcement("Archived", active=False)
        self._announcement("Expired", expires=self.now - timedelta(hours=1))
        self._announcement("Old", age=timedelta(days=40))
        self._announcement("Old but pinned", pinned=True, age=timedelta(days=40))
        mine = self._notification(ME)
        self._notification(OTHER)  # someone else's
        self._notification(ME, kind="announcement", link="/team/announcements")  # folded
        self._notification(ME, age=timedelta(days=45))  # old, still unread
        # Visible + old + old pinned announcements, both of my notifications
        # and the shipped document. Age never marks anything read.
        self.assertEqual(self._count(), 6)
        # OTHER: the 3 announcements, their own notification, the document.
        self.assertEqual(self._count(OTHER), 5)
        inbox_store.mark_read(self.session, ME, "announcement", str(visible.id))
        inbox_store.mark_read(self.session, ME, "notification", str(mine.id))
        self.session.commit()
        self.assertEqual(self._count(), 4)
        # Only the kinds the user can see are counted: the shipped document alone.
        self.assertEqual(self._count(kinds=("document",)), 1)
        self.assertEqual(self._count(kinds=()), 0)

    def test_new_documents_count_as_unread(self):
        from app.team import inbox_store

        kinds = ("document",)
        self.assertEqual(
            inbox_store.unread_count(self.session, ME, kinds=kinds, documents=(FRESH_DOC,), now=NOW), 1
        )
        inbox_store.mark_read(self.session, ME, "document", "new-handbook")
        self.session.commit()
        self.assertEqual(
            inbox_store.unread_count(self.session, ME, kinds=kinds, documents=(FRESH_DOC,), now=NOW), 0
        )

    def test_unread_count_query_budget(self):
        from sqlalchemy import event

        from app.team import inbox_store

        self._announcement()
        self._notification(ME)
        statements = []
        listener = lambda *args: statements.append(args[2])  # noqa: E731
        event.listen(self.engine, "before_cursor_execute", listener)
        try:
            inbox_store.unread_count(self.session, ME, kinds=("announcement", "notification", "document"))
        finally:
            event.remove(self.engine, "before_cursor_execute", listener)
        self.assertLessEqual(len(statements), 3, statements)

    def test_item_exists_for_rejects_unknown_and_other_users_items(self):
        from app.team import inbox_store

        other_note = self._notification(OTHER)
        archived = self._announcement(active=False)
        self.assertFalse(inbox_store.item_exists_for(self.session, ME, "notification", str(other_note.id)))
        self.assertTrue(inbox_store.item_exists_for(self.session, OTHER, "notification", str(other_note.id)))
        self.assertFalse(inbox_store.item_exists_for(self.session, ME, "announcement", str(archived.id)))
        self.assertFalse(inbox_store.item_exists_for(self.session, ME, "document", "nope"))
        self.assertFalse(inbox_store.item_exists_for(self.session, ME, "announcement", "abc"))
        self.assertTrue(inbox_store.item_exists_for(self.session, ME, "document", "surprise-set-guide"))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


class InboxPageTests(_InboxHarness, unittest.TestCase):
    def test_page_lists_all_sources_with_unread_dots(self):
        self._announcement("Pinned rules", pinned=True, age=timedelta(days=2))
        self._announcement("Restock Friday")
        self._notification(ME, title="Shift moved")
        self._notification(OTHER, title="Not for Maya")
        _, cap = self._inbox()
        self.assertEqual(cap["template"], "team/inbox.html")
        html = self._render(cap)
        self.assertLess(html.index("Pinned rules"), html.index("Shift moved"))
        self.assertLess(html.index("Shift moved"), html.index("Restock Friday"))
        self.assertIn("TikTok Surprise Set Streamer Guide", html)
        self.assertNotIn("Not for Maya", html)
        # 2 announcements + 1 update + the shipped document (old, never opened).
        self.assertEqual(html.count('class="pt-dot-unread"'), 4)
        self.assertIn("4 unread", html)
        self.assertIn('action="/team/inbox/read-all"', html)
        self.assertIn('<nav class="pt-seg pt-seg-4" aria-label="Filter inbox">', html)
        self.assertIn('href="/team/inbox?filter=documents"', html)
        # Every non-document row opens through a CSRF'd POST form.
        self.assertEqual(html.count('action="/team/inbox/open"'), 3)
        self.assertEqual(html.count('name="csrf_token"'), html.count("<form"))
        # SMS / browser alert settings moved here from /team/notifications.
        self.assertIn('id="alerts"', html)
        self.assertIn("Optional SMS subscription", html)
        self.assertIn("data-push-enable", html)
        self.assertIn("/static/portal-inbox.js", html)
        self.assertEqual(cap["context"]["active"], "inbox")

    def _render(self, cap):
        from app.routers import team

        return team.templates.env.get_template(cap["template"]).render(cap["context"])

    def test_filters_work_without_js(self):
        self._announcement("Restock Friday")
        self._notification(ME, title="Shift moved")
        _, cap = self._inbox(filter="updates")
        html = self._render(cap)
        self.assertIn("Shift moved", html)
        self.assertNotIn("Restock Friday", html)
        self.assertNotIn("Surprise Set", html)
        self.assertIn('<a href="/team/inbox?filter=updates" aria-current="page">', html)
        _, cap = self._inbox(filter="documents")
        html = self._render(cap)
        self.assertIn("Surprise Set", html)
        self.assertNotIn("Shift moved", html)
        self.assertIn('target="_blank" rel="noopener"', html)
        self.assertIn('data-inbox-kind="document"', html)
        _, cap = self._inbox(filter="nonsense")
        self.assertEqual(cap["context"]["inbox"]["filter"], "all")

    def test_announcement_body_stays_escaped(self):
        ann = self._announcement("Heads up", body="<script>alert(1)</script>\nLine two")
        _, cap = self._inbox()
        html = self._render(cap)
        self.assertNotIn("<script>alert(1)</script>", html)
        from app.routers import team_inbox

        _, cap = self._capture(
            team_inbox.team_inbox_announcement,
            self._request(self.maya, f"/team/inbox/announcements/{ann.id}"),
            ann.id,
            session=self.session,
        )
        html = self._render(cap)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn('class="pt-announce-body"', html)
        # Not yet opened through the Inbox: offer "Mark as read" (POST form).
        self.assertIn("Mark as read", html)

    def test_announcement_detail_404_for_archived_or_expired(self):
        from app.routers import team_inbox

        for row in (
            self._announcement(active=False),
            self._announcement(expires=self.now - timedelta(minutes=1)),
        ):
            response = team_inbox.team_inbox_announcement(
                self._request(self.maya, "/x"), row.id, session=self.session
            )
            self.assertEqual(response.status_code, 404)

    def test_open_marks_read_and_redirects_to_the_item(self):
        ann = self._announcement()
        note = self._notification(ME, link="/team/schedule?week=2026-09-21")
        r = self._open("announcement", ann.id)
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"/team/inbox/announcements/{ann.id}"))
        r = self._open("notification", note.id)
        self.assertEqual(r.headers["location"], "/team/schedule?week=2026-09-21")
        r = self._open("document", "surprise-set-guide")
        self.assertEqual(r.headers["location"], "/static/team-documents/surprise-set-guide.pdf")
        self.assertEqual(
            self._reads(),
            sorted([("announcement", str(ann.id)), ("notification", str(note.id)), ("document", "surprise-set-guide")]),
        )
        # Opening again is harmless.
        r = self._open("announcement", ann.id)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(len(self._reads()), 3)

    def test_open_refuses_other_users_items_and_unknown_kinds(self):
        other = self._notification(OTHER)
        for kind, key in (("notification", other.id), ("policy", "x"), ("document", "nope"), ("announcement", "999")):
            r = self._open(kind, key)
            self.assertEqual(r.headers["location"], "/team/inbox", (kind, key))
        self.assertEqual(self._reads(), [])

    def test_open_never_redirects_off_site(self):
        note = self._notification(ME, link="https://evil.example/phish")
        r = self._open("notification", note.id)
        self.assertEqual(r.headers["location"], "/team/")

    def test_read_json_endpoint(self):
        from app.routers import team_inbox

        self._announcement()
        r = team_inbox.team_inbox_read(
            self._request(self.maya, "/team/inbox/read"), kind="document", key="surprise-set-guide", session=self.session
        )
        self.assertEqual(r, {"ok": True, "unread": 1})
        r = team_inbox.team_inbox_read(
            self._request(self.maya, "/team/inbox/read"), kind="document", key="nope", session=self.session
        )
        self.assertEqual(r.status_code, 404)

    def test_mark_all_read_respects_filter_and_is_idempotent(self):
        from app.routers import team_inbox

        self._announcement()
        self._notification(ME)
        self._notification(ME, title="Second")
        r = team_inbox.team_inbox_read_all(
            self._request(self.maya, "/team/inbox/read-all"), filter="updates", session=self.session
        )
        self.assertEqual(r.headers["location"], "/team/inbox?filter=updates&flash=Marked+all+as+read.")
        self.assertEqual([k for k, _ in self._reads()], ["notification", "notification"])
        # The announcement and the shipped document are still unread.
        self.assertEqual(self._count(), 2)
        r = team_inbox.team_inbox_read_all(
            self._request(self.maya, "/team/inbox/read-all"), filter="", session=self.session
        )
        self.assertEqual(r.headers["location"], "/team/inbox?flash=Marked+all+as+read.")
        self.assertEqual(self._count(), 0)
        r = team_inbox.team_inbox_read_all(
            self._request(self.maya, "/team/inbox/read-all"), filter="", session=self.session
        )
        self.assertEqual(r.headers["location"], "/team/inbox?flash=Nothing+unread.")
        # 2 notifications + the announcement + the shipped document.
        self.assertEqual(len(self._reads()), 4)
        _, cap = self._inbox()
        html = self._render(cap)
        self.assertIn("All caught up", html)
        self.assertNotIn('action="/team/inbox/read-all"', html)
        self.assertNotIn('class="pt-dot-unread"', html)

    def test_permission_combinations(self):
        from app.routers import team_inbox

        # Documents only: no tabs, no updates, no alerts section.
        self._set_permission("employee", "page.announcements", False)
        self._announcement("Hidden announcement")
        _, cap = self._inbox()
        html = self._render(cap)
        self.assertIn("Surprise Set", html)
        self.assertNotIn("Hidden announcement", html)
        self.assertNotIn("pt-seg", html)
        self.assertNotIn('id="alerts"', html)
        r = team_inbox.team_inbox_open(
            self._request(self.maya, "/x"), kind="announcement", key="1", session=self.session
        )
        self.assertEqual(r.headers["location"], "/team/inbox")
        r = team_inbox.team_inbox_announcement(self._request(self.maya, "/x"), 1, session=self.session)
        self.assertEqual(r.status_code, 403)
        # Announcements only.
        self._set_permission("employee", "page.announcements", True)
        self._set_permission("employee", "page.documents", False)
        _, cap = self._inbox(filter="documents")
        ctx = cap["context"]["inbox"]
        self.assertEqual(ctx["filter"], "all")
        self.assertEqual([t["label"] for t in ctx["tabs"]], ["All", "Announcements", "Updates"])
        self.assertNotIn("Surprise Set", self._render(cap))
        # Neither: 403 like the old pages, and no Inbox in the nav.
        self._set_permission("employee", "page.announcements", False)
        r = team_inbox.team_inbox(self._request(self.maya, "/team/inbox"), filter=None, flash=None, session=self.session)
        self.assertEqual(r.status_code, 403)
        for fn, kw in (
            (team_inbox.team_inbox_open, dict(kind="document", key="surprise-set-guide")),
            (team_inbox.team_inbox_read, dict(kind="document", key="surprise-set-guide")),
            (team_inbox.team_inbox_read_all, dict(filter="")),
        ):
            self.assertEqual(fn(self._request(self.maya, "/x"), session=self.session, **kw).status_code, 403)
        from app.routers.team import _nav_context

        ctx = _nav_context(self.session, self.maya)
        self.assertNotIn("inbox", [i["name"] for i in ctx["nav_items"]])
        self.assertEqual(ctx["inbox_unread"], 0)


class InboxNavTests(_InboxHarness, unittest.TestCase):
    def _page(self, fn, path, **kw):
        _, cap = self._capture(fn, self._request(self.maya, path), session=self.session, **kw)
        from app.routers import team

        return team.templates.env.get_template(cap["template"]).render(cap["context"])

    def _sidebar(self, html):
        start = html.index('<aside class="pt-side"')
        return html[start:html.index("</aside>", start)]

    def _bottom_nav(self, html):
        start = html.index('<nav class="pt-mobile-bottom-nav"')
        return html[start:html.index("</nav>", start)]

    def test_nav_context_has_one_inbox_item_and_unread_count(self):
        from app.routers.team import _nav_context

        self._announcement()
        self._notification(ME)
        ctx = _nav_context(self.session, self.maya)
        names = [i["name"] for i in ctx["nav_items"]]
        self.assertEqual(names.count("inbox"), 1)
        for old in ("announcements", "notifications", "documents"):
            self.assertNotIn(old, names)
        self.assertIn("policies", names)
        # Announcement + notification + the shipped document.
        self.assertEqual(ctx["inbox_unread"], 3)
        self.assertLess(names.index("supply"), names.index("inbox"))
        self.assertLess(names.index("inbox"), names.index("policies"))

    def test_more_page_single_inbox_row_with_count_and_tab_badge(self):
        from app.routers import team

        self._announcement()
        self._notification(ME)
        html = self._page(team.team_more, "/team/more")
        self.assertIn('<a class="pt-row" href="/team/inbox">', html)
        # Announcement + notification + the shipped document.
        self.assertIn('<span class="pt-count" aria-label="3 unread">3</span>', html)
        for old in ('href="/team/announcements"', 'href="/team/notifications"', 'href="/team/documents"'):
            self.assertNotIn(old, html)
        self.assertIn('href="/team/policies"', html)
        self.assertIn('href="/team/inbox#alerts"', html)
        nav = self._bottom_nav(html)
        self.assertIn('aria-label="More, 3 unread"', nav)
        self.assertIn('<span class="pt-mbn-badge" aria-hidden="true">3</span>', nav)
        self.assertEqual(nav.count('class="pt-mbn-item'), 5)

    def test_sidebar_single_inbox_link_with_count(self):
        from app.routers import team

        self._announcement()
        html = self._page(team.team_more, "/team/more")
        side = self._sidebar(html)
        self.assertEqual(side.count('href="/team/inbox"'), 1)
        # Announcement + the shipped document.
        self.assertIn('<span class="pt-count pt-link-count" aria-label="2 unread">2</span>', side)
        for old in ("/team/announcements", "/team/notifications", "/team/documents"):
            self.assertNotIn(old, side)
        self.assertIn('href="/team/policies"', side)

    def test_no_badge_when_nothing_unread(self):
        from app.routers import team
        from app.team import inbox_store

        # The shipped document starts unread for everyone; open it first.
        inbox_store.mark_read(self.session, ME, "document", "surprise-set-guide")
        self.session.commit()
        html = self._page(team.team_more, "/team/more")
        self.assertNotIn("pt-mbn-badge", html)
        self.assertNotIn("pt-link-count", html)
        self.assertIn('aria-label="More"', self._bottom_nav(html))

    def test_home_latest_row_has_unread_dot_until_opened(self):
        from app.routers import team

        ann = self._announcement("Restock Friday")
        with patch.object(team, "clockify_is_configured", return_value=False):
            html = self._page(team.team_dashboard, "/team/")
        start = html.index('id="pt-latest-h"')
        latest = html[start:html.index("</section>", start)]
        self.assertIn('<a href="/team/inbox">Inbox</a>', latest)
        self.assertIn("Restock Friday", latest)
        self.assertIn('class="pt-dot-unread"', latest)
        self.assertIn('action="/team/inbox/open"', latest)
        self.assertIn(f'name="key" value="{ann.id}"', latest)
        self._open("announcement", ann.id)
        with patch.object(team, "clockify_is_configured", return_value=False):
            html = self._page(team.team_dashboard, "/team/")
        start = html.index('id="pt-latest-h"')
        latest = html[start:html.index("</section>", start)]
        self.assertNotIn('class="pt-dot-unread"', latest)

    def test_templates_have_no_inline_styles_or_scripts(self):
        for name in ("inbox.html", "inbox_announcement.html", "more.html"):
            source = Path("app/templates/team", name).read_text(encoding="utf-8")
            self.assertNotIn("style=", source, name)
            self.assertNotIn("<style", source, name)
            self.assertNotIn("<script>", source, name)

    def test_phase4_css_uses_tokens_and_min_12px_type(self):
        css = Path("app/static/portal.css").read_text(encoding="utf-8")
        block = css.split("Inbox (employee portal redesign, Phase 4)", 1)[1]
        self.assertIsNone(re.search(r"#[0-9a-fA-F]{3,8}\b", block))
        for size in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", block):
            self.assertGreaterEqual(float(size), 12)

    def test_old_templates_removed(self):
        for name in ("announcements.html", "notifications.html", "documents.html"):
            self.assertFalse(Path("app/templates/team", name).exists(), name)


# ---------------------------------------------------------------------------
# Over the wire: redirects, CSRF, poll contract
# ---------------------------------------------------------------------------


class InboxEndToEndTests(unittest.TestCase):
    def setUp(self):
        from tests.test_employee_portal_wave3 import SupplyAndPoliciesTests

        self.h = SupplyAndPoliciesTests("test_supply_post_without_csrf_is_403")
        self.h._setup_portal()
        self.addCleanup(self.h._teardown_portal)
        self.uid = self.h._seed_employee(user_id=71, username="emp_inbox")

    def _note(self, **kw):
        from app.team.team_notifications import notify_employee

        row = notify_employee(
            self.h.session,
            user_id=self.uid,
            actor_user_id=None,
            kind=kw.get("kind", "schedule_change"),
            title=kw.get("title", "Shift moved"),
            body="Details",
            link_path=kw.get("link", "/team/schedule"),
            send_text=False,
        )
        self.h.session.commit()
        self.h.session.refresh(row)
        return row

    def _reads(self):
        from app.models import TeamInboxRead

        self.h.session.expire_all()
        return self.h.session.exec(select(TeamInboxRead)).all()

    def test_old_urls_redirect_to_inbox_filters(self):
        for old, new in (
            ("/team/announcements", "/team/inbox?filter=announcements"),
            ("/team/notifications", "/team/inbox?filter=updates"),
            ("/team/documents", "/team/inbox?filter=documents"),
        ):
            r = self.h.client.get(old, follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, new), old)
            page = self.h.client.get(old)
            self.assertEqual(page.status_code, 200)
            self.assertIn('<h1 class="pt-title">Inbox</h1>', page.text)

    def test_state_changes_require_csrf(self):
        note = self._note()
        for path, data in (
            ("/team/inbox/open", {"kind": "notification", "key": str(note.id)}),
            ("/team/inbox/read", {"kind": "notification", "key": str(note.id)}),
            ("/team/inbox/read-all", {"filter": ""}),
        ):
            r = self.h.client.post(path, data=data, follow_redirects=False)
            self.assertEqual(r.status_code, 403, path)
        self.assertEqual(self._reads(), [])
        csrf = self.h._csrf()
        r = self.h.client.post(
            "/team/inbox/open",
            data={"kind": "notification", "key": str(note.id), "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/team/schedule"))
        r = self.h.client.post(
            "/team/inbox/read",
            data={"kind": "document", "key": "surprise-set-guide"},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(r.json()["ok"], True)
        self.assertEqual(len(self._reads()), 2)

    def test_poll_endpoint_contract_unchanged(self):
        # base.html's browser-alert poller depends on this exact shape, and
        # still gets announcement notifications (the Inbox folds them, the
        # poller does not).
        first = self._note(title="Shift moved")
        second = self._note(kind="announcement", title="New announcement: Restock", link="/team/announcements")
        r = self.h.client.get("/team/notifications/poll?since_id=0")
        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertEqual(set(payload), {"latest_id", "notifications"})
        self.assertEqual(payload["latest_id"], second.id)
        self.assertEqual([n["id"] for n in payload["notifications"]], [first.id, second.id])
        for note in payload["notifications"]:
            self.assertEqual(set(note), {"id", "kind", "title", "body", "link_path", "created_at"})
        self.assertEqual(payload["notifications"][1]["link_path"], "/team/announcements")
        r = self.h.client.get(f"/team/notifications/poll?since_id={second.id}")
        self.assertEqual(r.json(), {"latest_id": second.id, "notifications": []})

    def test_poll_endpoint_keeps_page_announcements_permission(self):
        from app.models import RolePermission

        row = self.h.session.exec(
            select(RolePermission).where(
                RolePermission.role == "employee", RolePermission.resource_key == "page.announcements"
            )
        ).one()
        row.is_allowed = False
        self.h.session.add(row)
        self.h.session.commit()
        r = self.h.client.get("/team/notifications/poll?since_id=0")
        self.assertEqual(r.status_code, 403)


if __name__ == "__main__":
    unittest.main()
