"""Mobile nav regression tests for the employee portal.

The portal's mobile layout has three moving parts that all need to render
on every authenticated portal page:

  1. A sticky topbar with a hamburger (`#pt-hamburger`) that opens the
     sidebar drawer on phones.
  2. The sidebar itself must have the drawer id (`#pt-sidebar`) and a
     close button (`#pt-drawer-close`) so the JS can wire up tap-to-close.
  3. A bottom nav (`.pt-mobile-bottom-nav`) with the same five tabs for
     every role: Home · Schedule · Hours · Requests · More (redesign
     2026-09; the old centre FAB and ops-tool slots are gone — ops tools
     now live under More and in the sidebar's Ops group).

We also verify the drawer JS is loaded, that a tab is hidden (not a 403)
when the user lacks its page.* permission, and that team-admin pages (which
don't run `_nav_context()`) still render all five tabs with the editable
admin schedule.
"""
from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-mobilenav")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-mobilenav")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-mobilenav")


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

    def tearDown(self):
        self.session.close()

    def _login_as(self, role: str, user_id: int = 500, username: str = "u"):
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
        return u

    def _dashboard_html(self, path: str = "/team/") -> str:
        from app import permissions as perms
        from app.routers.team import _nav_context
        from app.shared import templates

        user = self._current_user
        request = SimpleNamespace(url=SimpleNamespace(path=path))
        context = {
            "request": request,
            "title": "Dashboard",
            "active": "dashboard",
            "current_user": user,
            "widgets": perms.allowed_widgets_for(self.session, user),
            "clockify_ready": False,
            "supply_queue_count": 0,
            "now_hour": 12,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/dashboard.html").render(context)

    def test_portal_dashboard_renders_mobile_topbar_and_hamburger(self):
        self._current_user = self._login_as("employee", user_id=501, username="emp1")
        html = self._dashboard_html()
        self.assertIn('id="pt-mobile-topbar"', html)
        self.assertIn('id="pt-hamburger"', html)
        self.assertIn('aria-controls="pt-sidebar"', html)

    def test_portal_sidebar_has_drawer_hooks(self):
        self._current_user = self._login_as("employee", user_id=502, username="emp2")
        html = self._dashboard_html()
        self.assertIn('id="pt-sidebar"', html)
        self.assertIn('id="pt-drawer-close"', html)
        self.assertIn('id="pt-drawer-backdrop"', html)
        self.assertIn("/static/portal-drawer.js", html)

    FIVE_TABS = (
        ("Home", 'href="/team/"'),
        ("Schedule", 'href="/team/schedule"'),
        ("Hours", 'href="/team/hours"'),
        ("Requests", 'href="/team/requests"'),
        ("More", 'href="/team/more"'),
    )

    @staticmethod
    def _bottom_nav(html: str) -> str:
        start = html.index('<nav class="pt-mobile-bottom-nav"')
        return html[start:html.index("</nav>", start)]

    def _assert_five_tabs(self, nav: str, schedule_href: str = "/team/schedule"):
        self.assertEqual(nav.count('class="pt-mbn-item'), 5, nav)
        labels = [label for label, _ in self.FIVE_TABS]
        positions = [nav.index(f'<span class="pt-mbn-label">{label}</span>') for label in labels]
        self.assertEqual(positions, sorted(positions), "tabs out of order")
        for label, href in self.FIVE_TABS:
            if label == "Schedule":
                href = f'href="{schedule_href}"'
            self.assertIn(href, nav, f"missing bottom-nav tab: {label}")

    def test_bottom_nav_has_five_tabs_for_hourly_employee(self):
        self._current_user = self._login_as("employee", user_id=503, username="emp3")
        nav = self._bottom_nav(self._dashboard_html())
        self._assert_five_tabs(nav)
        # No centre FAB and no ops tools in the bar any more.
        self.assertNotIn("pt-mbn-fab", nav)
        self.assertNotIn("pt-mbn-item-center", nav)
        self.assertNotIn('href="/degen_eye?team_shell=1"', nav)
        self.assertNotIn('href="/tiktok/streamer?team_shell=1"', nav)
        self.assertNotIn('href="/team/profile"', nav)
        self.assertNotIn('href="/team/admin/schedule"', nav)

    def test_bottom_nav_same_five_tabs_for_ops_staff(self):
        # Managers hold the ops tools (Degen Eye, Live Stream, ...). They get
        # the same bar; the tools are reachable from the sidebar Ops group
        # and the More page instead.
        self._current_user = self._login_as("manager", user_id=506, username="mgr1")
        html = self._dashboard_html()
        nav = self._bottom_nav(html)
        self._assert_five_tabs(nav)
        self.assertNotIn("pt-mbn-fab", nav)
        self.assertNotIn('href="/degen_eye?team_shell=1"', nav)
        self.assertIn('href="/degen_eye?team_shell=1"', html)  # sidebar Ops
        self.assertIn('href="/tiktok/streamer?team_shell=1"', html)
        self.assertIn('href="/team/admin/schedule"', html)  # sidebar Admin

    def test_bottom_nav_hides_tab_without_permission(self):
        from sqlmodel import select
        from app.models import RolePermission

        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "employee",
                RolePermission.resource_key == "page.hours",
            )
        ).first()
        self.assertIsNotNone(row)
        row.is_allowed = False
        self.session.add(row)
        self.session.commit()
        self._current_user = self._login_as("employee", user_id=507, username="emp7")
        nav = self._bottom_nav(self._dashboard_html())
        self.assertEqual(nav.count('class="pt-mbn-item'), 4)
        self.assertNotIn('href="/team/hours"', nav)
        self.assertIn('href="/team/more"', nav)

    def test_admin_base_renders_five_tabs_with_admin_schedule(self):
        from app.shared import templates

        user = self._login_as("admin", user_id=508, username="adm8")
        html = templates.env.get_template("team/admin/base.html").render(
            {
                "request": SimpleNamespace(
                    url=SimpleNamespace(path="/team/admin"),
                    state=SimpleNamespace(),
                ),
                "title": "Team admin",
                "current_user": user,
                "csrf_token": "test-token",
            }
        )
        self._assert_five_tabs(
            self._bottom_nav(html), schedule_href="/team/admin/schedule"
        )

    def test_bottom_nav_renders_on_non_home_pages_too(self):
        self._current_user = self._login_as("employee", user_id=504, username="emp5")
        for path in ("/team/schedule", "/team/policies", "/team/profile"):
            html = self._dashboard_html(path=path)
            self.assertIn('class="pt-mobile-bottom-nav"', html,
                          f"bottom nav missing on {path}")
            self.assertIn('id="pt-hamburger"', html,
                          f"hamburger missing on {path}")

    def test_active_state_marks_current_bottom_nav_item(self):
        self._current_user = self._login_as("employee", user_id=505, username="emp4")
        html = self._dashboard_html(path="/team/schedule")
        # Look for the schedule anchor carrying the active class.
        self.assertRegex(
            html,
            r'href="/team/schedule"[^>]*class="pt-mbn-item[^"]* is-active"',
        )


if __name__ == "__main__":
    unittest.main()
