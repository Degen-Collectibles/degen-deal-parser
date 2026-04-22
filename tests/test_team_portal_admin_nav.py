"""Sidebar admin section — visibility by role.

The "Admin" group in the portal sidebar (`base.html`) is populated by
`_nav_context` in `app/routers/team.py`, which filters admin links through
`has_permission()`. This test locks in:

- Admins see all four admin links (Employees, Invites, Permissions, Supply).
- Employees see ZERO admin links and no "Admin" divider.
- Managers see only Supply Queue (page.admin.supply is manager+).
- Reviewers see only Supply Queue.
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-adminnav")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-adminnav")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class AdminSidebarVisibilityTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        from app import config as cfg
        cfg.get_settings.cache_clear()
        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

        from app.db import get_session as real_get_session

        def _session_override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _session_override

        from fastapi.testclient import TestClient
        self.client = TestClient(self.app_main.app)

    def tearDown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_patcher_shared", "_patcher_main"):
            p = getattr(self, attr, None)
            if p:
                p.stop()
                setattr(self, attr, None)

    def _login_as(self, role: str, user_id: int = 100, username: str = "u"):
        from app import shared
        import app.main as app_main
        from app.models import User

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        # Also persist so /team/ handler can load perms against this row.
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()

        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        return u

    def _dashboard_html(self) -> str:
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 200, f"dashboard failed: {r.status_code} {r.text[:200]}")
        return r.text

    def test_admin_sees_all_four_admin_links(self):
        self._login_as("admin", user_id=101, username="adm")
        html = self._dashboard_html()
        self.assertIn('href="/team/admin/employees"', html)
        self.assertIn('href="/team/admin/invites"', html)
        self.assertIn('href="/team/admin/permissions"', html)
        self.assertIn('href="/team/admin/supply"', html)
        self.assertIn(">Admin<", html, "admin group divider should render")

    def test_employee_sees_no_admin_links(self):
        self._login_as("employee", user_id=102, username="emp")
        html = self._dashboard_html()
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)
        self.assertNotIn('href="/team/admin/supply"', html)
        # The "Admin" group header must not render for plain employees.
        self.assertNotIn(
            '<div class="pt-side-group">Admin</div>',
            html,
            "admin divider leaked into an employee's sidebar",
        )

    def test_manager_sees_only_supply_queue(self):
        self._login_as("manager", user_id=103, username="mgr")
        html = self._dashboard_html()
        # page.admin.supply is manager=True in DEFAULT_ROLE_PERMISSIONS.
        self.assertIn('href="/team/admin/supply"', html)
        # The other three admin pages are admin-only.
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)

    def test_reviewer_sees_only_supply_queue(self):
        self._login_as("reviewer", user_id=104, username="rev")
        html = self._dashboard_html()
        self.assertIn('href="/team/admin/supply"', html)
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)


if __name__ == "__main__":
    unittest.main()
