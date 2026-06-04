"""Regression tests for the employee-facing Documents page."""
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
        from app.routers.team import TEAM_DOCUMENTS, _nav_context
        from app.shared import templates

        request = _FakeRequest(user)
        context = {
            "request": request,
            "title": "Documents",
            "active": "documents",
            "current_user": user,
            "documents": TEAM_DOCUMENTS,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/documents.html").render(context)

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

    def test_sidebar_shows_documents_link(self):
        from app.routers.team import _nav_context

        employee = self._seed_user(1)

        nav = _nav_context(self.session, employee)["nav_items"]

        self.assertIn(
            {"name": "documents", "label": "Documents", "href": "/team/documents"},
            nav,
        )

    def test_documents_page_links_surprise_set_pdf(self):
        employee = self._seed_user(2)

        html = self._documents_html(employee)

        self.assertIn("TikTok Surprise Set Streamer Guide", html)
        self.assertIn("/static/team-documents/surprise-set-guide.pdf", html)
        self.assertIn("Open PDF", html)

    def test_surprise_set_guide_source_exists(self):
        self.assertTrue((ROOT / "docs" / "team" / "surprise-set-guide.md").exists())

    def test_documents_route_respects_permission(self):
        from app.models import RolePermission
        from app.routers.team import team_documents

        employee = self._seed_user(3)
        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "employee",
                RolePermission.resource_key == "page.documents",
            )
        ).one()
        row.is_allowed = False
        self.session.add(row)
        self.session.commit()

        response = team_documents(_FakeRequest(employee), session=self.session)

        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
