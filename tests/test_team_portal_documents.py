"""Regression tests for employee-facing Documents (now an Inbox filter)."""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-documents")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-documents")
os.environ.setdefault("SESSION_SECRET", "unit-test-session-documents")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-documents")


ROOT = Path(__file__).resolve().parents[1]


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class _FakeRequest:
    def __init__(self, current_user, *, path: str = "/team/documents"):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path=path,
            scheme="http",
            netloc="testserver",
        )


class TeamDocumentsTests(unittest.TestCase):
    def setUp(self):
        from app.db import seed_employee_portal_defaults

        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _seed_user(self, user_id: int, *, role: str = "employee"):
        from app.models import User

        user = User(
            id=user_id,
            username=f"user{user_id}",
            password_hash="x",
            password_salt="x",
            display_name=f"User {user_id}",
            role=role,
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _documents_html(self, user) -> str:
        # Documents are listed in the Inbox's Documents filter since the
        # redesign (Phase 4); render it through the real route.
        from unittest.mock import patch

        from app.routers import team_inbox

        captured = {}

        def fake_template_response(request, template, context):
            captured["html"] = team_inbox.templates.env.get_template(template).render(context)
            return SimpleNamespace(status_code=200)

        with patch.object(
            team_inbox.templates, "TemplateResponse", side_effect=fake_template_response
        ):
            team_inbox.team_inbox(
                _FakeRequest(user, path="/team/inbox"),
                filter="documents",
                flash=None,
                session=self.session,
            )
        return captured["html"]

    def test_documents_permission_seeded_for_all_portal_roles(self):
        from app.models import RolePermission

        rows = self.session.exec(
            select(RolePermission).where(RolePermission.resource_key == "page.documents")
        ).all()
        allowed_by_role = {row.role: row.is_allowed for row in rows}

        self.assertEqual(
            allowed_by_role,
            {
                "employee": True,
                "viewer": True,
                "manager": True,
                "reviewer": True,
                "admin": True,
            },
        )

    def test_sidebar_shows_inbox_link_with_documents(self):
        # One Inbox link replaces the Documents link; documents are one of
        # its sections for anyone holding page.documents.
        from app.routers.team import _nav_context

        employee = self._seed_user(1)

        ctx = _nav_context(self.session, employee)

        self.assertIn(
            {"name": "inbox", "label": "Inbox", "href": "/team/inbox"},
            ctx["nav_items"],
        )
        self.assertNotIn("documents", [item["name"] for item in ctx["nav_items"]])
        self.assertIn("document", ctx["inbox_kinds"])

    def test_documents_page_links_surprise_set_pdf(self):
        employee = self._seed_user(2)

        html = self._documents_html(employee)

        self.assertIn("TikTok Surprise Set Streamer Guide", html)
        self.assertIn('href="/static/team-documents/surprise-set-guide.pdf"', html)
        # Opens in a new tab, like the old "Open PDF" button.
        self.assertIn('target="_blank" rel="noopener"', html)

    def test_surprise_set_guide_source_exists(self):
        self.assertTrue((ROOT / "docs" / "team" / "surprise-set-guide.md").exists())

    def _deny(self, key: str):
        from app.models import RolePermission

        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "employee",
                RolePermission.resource_key == key,
            )
        ).one()
        row.is_allowed = False
        self.session.add(row)
        self.session.commit()

    def test_documents_route_redirects_to_inbox_filter(self):
        from app.routers.team import team_documents

        response = team_documents()

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/team/inbox?filter=documents")

    def test_documents_respect_permission(self):
        # Without page.documents the Inbox hides documents (and the filter
        # falls back to All); with no Inbox section at all it's a 403, the
        # same answer the old Documents page gave.
        from app.routers.team_inbox import team_inbox

        employee = self._seed_user(3)
        self._deny("page.documents")

        html = self._documents_html(employee)
        self.assertNotIn("surprise-set-guide.pdf", html)

        self._deny("page.announcements")
        response = team_inbox(
            _FakeRequest(employee, path="/team/inbox"),
            filter="documents",
            flash=None,
            session=self.session,
        )
        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
