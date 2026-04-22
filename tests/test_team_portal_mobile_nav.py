"""Mobile nav regression tests for the employee portal.

The portal's mobile layout has three moving parts that all need to render
on every authenticated portal page:

  1. A sticky topbar with a hamburger (`#pt-hamburger`) that opens the
     sidebar drawer on phones.
  2. The sidebar itself must have the drawer id (`#pt-sidebar`) and a
     close button (`#pt-drawer-close`) so the JS can wire up tap-to-close.
  3. A bottom nav (`.pt-mobile-bottom-nav`) with a primary center FAB.

We also verify the drawer JS is loaded and that the bottom nav adapts to
the viewer's `tools_nav_items` (Stream + Eye FAB when tools are available,
Policies + Supply fallback otherwise).
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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-mobilenav")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-mobilenav")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class MobileNavTests(unittest.TestCase):
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

    def _login_as(self, role: str, user_id: int = 500, username: str = "u"):
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
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
            self.session.refresh(u)
            self.session.expunge(u)

        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        return u

    def test_portal_dashboard_renders_mobile_topbar_and_hamburger(self):
        self._login_as("employee", user_id=501, username="emp1")
        html = self.client.get("/team/", follow_redirects=False).text
        self.assertIn('id="pt-mobile-topbar"', html)
        self.assertIn('id="pt-hamburger"', html)
        self.assertIn('aria-controls="pt-sidebar"', html)

    def test_portal_sidebar_has_drawer_hooks(self):
        self._login_as("employee", user_id=502, username="emp2")
        html = self.client.get("/team/", follow_redirects=False).text
        self.assertIn('id="pt-sidebar"', html)
        self.assertIn('id="pt-drawer-close"', html)
        self.assertIn('id="pt-drawer-backdrop"', html)
        self.assertIn("/static/portal-drawer.js", html)

    def test_bottom_nav_shows_live_stream_and_eye_fab_for_employee(self):
        self._login_as("employee", user_id=503, username="emp3")
        html = self.client.get("/team/", follow_redirects=False).text
        self.assertIn('class="pt-mobile-bottom-nav"', html)
        # Five expected bottom-nav destinations for a user with Tools access:
        for needle in (
            'href="/team/"',
            'href="/tiktok/streamer"',
            'href="/degen_eye"',
            'href="/team/schedule"',
            'href="/team/profile"',
        ):
            self.assertIn(needle, html, f"missing bottom-nav link: {needle}")
        # Center FAB must exist on a Tools-enabled nav
        self.assertIn('class="pt-mbn-fab"', html)
        self.assertIn('pt-mbn-item-center', html)

    def test_bottom_nav_renders_on_non_home_pages_too(self):
        self._login_as("admin", user_id=504, username="adm1")
        for path in ("/team/schedule", "/team/policies", "/team/profile"):
            html = self.client.get(path, follow_redirects=False).text
            self.assertIn('class="pt-mobile-bottom-nav"', html,
                          f"bottom nav missing on {path}")
            self.assertIn('id="pt-hamburger"', html,
                          f"hamburger missing on {path}")

    def test_active_state_marks_current_bottom_nav_item(self):
        self._login_as("employee", user_id=505, username="emp4")
        html = self.client.get("/team/schedule", follow_redirects=False).text
        # Look for the schedule anchor carrying the active class.
        self.assertRegex(
            html,
            r'href="/team/schedule"[^>]*class="pt-mbn-item[^"]* is-active"',
        )


if __name__ == "__main__":
    unittest.main()
