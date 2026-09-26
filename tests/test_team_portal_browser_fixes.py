"""Regressions for bugs found by end-to-end browser testing of the employee
portal redesign (2026-09-25).

Covers the nav / markup / CSS / JS fixes; the Home hero (stream shifts) and
request-date timezone fixes are tested next to their view-models in
test_team_home_view.py, test_employee_portal_homepage.py and
test_team_portal_requests.py.
"""
from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-browserfix")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-browserfix")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-browserfix")

from tests.test_team_schedule_view import ME, _RouteHarness  # noqa: E402

CSS = Path("app/static/portal.css")
_FIX_BLOCK_MARKER = "Shared utilities (employee portal redesign, 2026-09 browser-test fixes)"


def _css() -> str:
    return CSS.read_text(encoding="utf-8")


def _rule(css: str, selector: str) -> str:
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert match, f"no CSS rule for {selector}"
    return match.group(1)


def _bottom_nav(html: str) -> str:
    start = html.index('<nav class="pt-mobile-bottom-nav"')
    return html[start:html.index("</nav>", start)]


def _active_tabs(html: str) -> list[str]:
    return re.findall(r'href="([^"]+)"\s+class="pt-mbn-item is-active"', _bottom_nav(html))


class HiddenAttributeTests(unittest.TestCase):
    def test_hidden_beats_component_display(self):
        css = _css()
        block = css.split(_FIX_BLOCK_MARKER, 1)[1]
        self.assertIn("body.pt-body [hidden] { display: none !important; }", block)
        # .pt-callout sets display:flex, which used to beat the UA [hidden] rule.
        self.assertIn("display: flex", _rule(css, "\n.pt-callout"))

    def test_fix_block_uses_tokens_and_min_12px_type(self):
        block = _css().split(_FIX_BLOCK_MARKER, 1)[1]
        self.assertIsNone(re.search(r"#[0-9a-fA-F]{3,8}\b", block))
        for size in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", block):
            self.assertGreaterEqual(float(size), 12)


class OverlapCalloutTests(_RouteHarness, unittest.TestCase):
    def setUp(self):
        from unittest.mock import patch

        from app.routers import team_timeoff
        from tests.test_team_portal_requests import TODAY

        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        patcher = patch.object(team_timeoff, "clockify_today", return_value=TODAY)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.session.close)

    def test_overlap_warning_starts_hidden_when_nothing_overlaps(self):
        from app.routers import team_timeoff

        response, _ = self._capture(
            team_timeoff.team_requests,
            self._request(self.maya, "/team/requests"),
            session=self.session,
            tab=None, new=None, edit=None, id=None, date=None, flash=None, error=None,
        )
        boxes = re.findall(r'<div class="pt-callout is-warn pt-overlap"[^>]*>', response.body)
        self.assertTrue(boxes)
        for tag in boxes:
            self.assertIn(" hidden", tag)


class ProfileAddressLabelTests(_RouteHarness, unittest.TestCase):
    def setUp(self):
        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        self.addCleanup(self.session.close)

    def test_every_address_input_has_a_label(self):
        from app.routers import team

        response, _ = self._capture(
            team.team_profile,
            self._request(self.maya, "/team/profile"),
            flash=None,
            session=self.session,
        )
        html = response.body
        for name in ("address_street", "address_city", "address_state", "address_zip"):
            self.assertRegex(html, rf'<input[^>]*id="{name}"[^>]*name="{name}"')
            self.assertRegex(html, rf'<label[^>]*for="{name}"')
        # The group label points at the street field instead of nothing.
        self.assertIn('<label class="pt-field-label" for="address_street">Mailing address</label>', html)
        self.assertNotRegex(html, r'<label class="pt-field-label">')
        self.assertIn(".pt-sr-only", _css().split(_FIX_BLOCK_MARKER, 1)[1])


class NavActiveStateTests(_RouteHarness, unittest.TestCase):
    def setUp(self):
        self._setup_db()
        self.maya = self._user(ME, "Maya Rodriguez")
        self.addCleanup(self.session.close)

    def _html(self, fn, path, **kwargs):
        response, captured = self._capture(
            fn, self._request(self.maya, path), session=self.session, **kwargs
        )
        return response.body, captured["context"]

    def test_more_tab_is_active_on_pages_reached_from_more(self):
        from app.routers import team, team_inbox

        pages = [
            (team.team_more, "/team/more", {}),
            (team_inbox.team_inbox, "/team/inbox", {"filter": None, "flash": None}),
            (team.team_policies, "/team/policies", {"flash": None}),
            (team.team_profile, "/team/profile", {"flash": None}),
            (team.team_help, "/team/help", {"flash": None, "error": None, "page": None}),
            (team.team_password_change_page, "/team/password/change",
             {"flash": None, "error": None, "problems": None}),
        ]
        for fn, path, kwargs in pages:
            html, _ = self._html(fn, path, **kwargs)
            self.assertEqual(_active_tabs(html), ["/team/more"], path)

    def test_main_tabs_are_not_overridden_by_more(self):
        from app.routers import team

        html, _ = self._html(team.team_schedule, "/team/schedule", week=None, view=None)
        self.assertEqual(_active_tabs(html), ["/team/schedule"])

    def test_help_page_highlights_help_in_sidebar(self):
        from app.routers import team

        html, ctx = self._html(team.team_help, "/team/help", flash=None, error=None, page=None)
        self.assertEqual(ctx["active"], "help")
        self.assertIn('<a class="pt-link active" href="/team/help">', html)
        html, ctx = self._html(team.team_help_tutorial, "/team/help/tutorial")
        self.assertEqual(ctx["active"], "help")


class HamburgerTapTargetTests(unittest.TestCase):
    def test_hamburger_is_at_least_44px(self):
        body = _rule(_css(), ".pt-hamburger")
        self.assertIn("width: var(--pt-tap);", body)
        self.assertIn("height: var(--pt-tap);", body)
        self.assertIn("--pt-tap: 44px;", _css())


class SupplyDenyConfirmTests(unittest.TestCase):
    def test_supply_deny_asks_for_confirmation_like_timeoff(self):
        supply = Path("app/templates/team/admin/supply.html").read_text(encoding="utf-8")
        timeoff = Path("app/templates/team/admin/timeoff.html").read_text(encoding="utf-8")
        self.assertRegex(
            timeoff,
            r'action="/team/admin/timeoff/\{\{ r.id \}\}/deny"[^>]*\n\s*onsubmit="return confirm\(',
        )
        self.assertRegex(
            supply,
            r'action="/team/admin/supply/\{\{ r.id \}\}/deny"[^>]*\n\s*'
            r"onsubmit=\"return confirm\('Deny this supply request\?'\);\"",
        )


class InboxLiveCountTests(unittest.TestCase):
    def test_marking_read_updates_every_unread_count(self):
        js = Path("app/static/portal-inbox.js").read_text(encoding="utf-8")
        # Uses the new total /team/inbox/read returns...
        self.assertIn('typeof data.unread === "number"', js)
        # ...for the eyebrow, sidebar count, bottom-nav badge and its label,
        for hook in (
            "[data-inbox-eyebrow]",
            "#pt-sidebar a[href='/team/inbox'] .pt-count",
            "#pt-mobile-bottom-nav a[href='/team/more']",
            ".pt-mbn-badge",
            '"More, " + n + " unread"',
            "All caught up",
            ".pt-inbox-readall",
        ):
            self.assertIn(hook, js)
        # ...and the page loads the new file, not a cached copy.
        inbox = Path("app/templates/team/inbox.html").read_text(encoding="utf-8")
        self.assertNotIn("portal-inbox.js?v=2026092501", inbox)

    def test_read_endpoint_returns_the_new_unread_total(self):
        source = Path("app/routers/team_inbox.py").read_text(encoding="utf-8")
        self.assertIn('return {"ok": True, "unread": inbox_store.unread_count(', source)


class PortalCssCacheParamTests(unittest.TestCase):
    def test_every_portal_page_links_the_same_css_version(self):
        versions = set()
        for path in Path("app/templates").rglob("*.html"):
            versions.update(
                re.findall(r"/static/portal\.css\?v=(\w+)", path.read_text(encoding="utf-8"))
            )
        self.assertEqual(len(versions), 1, versions)
        self.assertNotIn("2026092503", versions)


if __name__ == "__main__":
    unittest.main()
