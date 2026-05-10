"""Rank-and-file employee access to selected ops pages.

Employees should be able to:
  - Use Degen Eye (`/degen_eye`) and the camera scanner (`/inventory/scan*`).
  - Search inventory (`/inventory`) through a limited shop-floor view that
    hides cost basis and manager-only edit/Shopify actions.
  - Use live hits (`/hits`) to log and review stream hits.
  - Open the TikTok live-stream dashboard (`/tiktok/streamer`) so they can
    chase GMV goals during a live. TikTok numbers are explicitly visible.

Employees must NOT be able to:
  - Hit the ops dashboard, reports, bookkeeping, or admin surfaces.

The portal sidebar should expose an "Ops" group with Live Stream + Degen Eye
for every authenticated user (rank employees included).

The TikTok streamer template's hamburger nav should hide ops / admin links
for anyone below role=viewer so employees aren't tempted into 403s.
"""
from __future__ import annotations

import importlib
import json
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-opsaccess")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-opsaccess")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeeOpsAccessTests(unittest.TestCase):
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

    def _login_as(self, role: str, user_id: int = 200, username: str = "u"):
        from app import shared
        import app.main as app_main
        from app.models import User

        # Persist a real User row so anything that hits the DB (e.g. perms
        # lookups on /team/) works. We then expunge it from the session so
        # attribute access (`.role`) never triggers a lazy refresh against a
        # session that might be in an inconsistent state — lazy refreshes
        # were the root cause of a flaky "role reads back as default
        # 'viewer'" bug when asserting against the streamer template.
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

    def _csrf_from_html(self, html: str) -> str:
        marker = "var token = "
        start = html.find(marker)
        if start == -1:
            raise AssertionError("no csrf token rendered")
        raw = html[start + len(marker):].split(";", 1)[0].strip()
        return json.loads(raw)

    # ---------- Sidebar "Tools" group ----------

    def test_employee_sees_tools_group_in_portal_sidebar(self):
        self._login_as("employee", user_id=201, username="emp1")
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        html = r.text
        self.assertIn('<div class="pt-side-group">Ops</div>', html)
        self.assertIn('href="/inventory"', html)
        self.assertIn('href="/tiktok/streamer?team_shell=1"', html)
        self.assertIn('href="/hits"', html)
        self.assertIn('href="/degen_eye?team_shell=1"', html)
        self.assertIn('href="/inventory/add-stock"', html)

    def test_admin_also_sees_tools_group(self):
        self._login_as("admin", user_id=202, username="adm1")
        html = self.client.get("/team/", follow_redirects=False).text
        self.assertIn('href="/inventory"', html)
        self.assertIn('<div class="pt-side-group">Ops</div>', html)
        self.assertIn('href="/tiktok/streamer?team_shell=1"', html)
        self.assertIn('href="/hits"', html)
        self.assertIn('href="/degen_eye?team_shell=1"', html)

    # ---------- Degen Eye + scanner access ----------

    def test_employee_can_open_degen_eye(self):
        self._login_as("employee", user_id=203, username="emp2")
        r = self.client.get("/degen_eye", follow_redirects=False)
        self.assertEqual(r.status_code, 200, f"degen_eye denied: {r.status_code}")
        self.assertIn("Degen Eye", r.text)

    def test_employee_can_open_scanner_singles(self):
        self._login_as("employee", user_id=204, username="emp3")
        r = self.client.get("/inventory/scan/singles", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_employee_can_open_scanner_slabs(self):
        self._login_as("employee", user_id=205, username="emp4")
        r = self.client.get("/inventory/scan/slabs", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_employee_can_open_scan_root(self):
        self._login_as("employee", user_id=206, username="emp5")
        r = self.client.get("/inventory/scan", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_employee_can_confirm_scanned_single_batch(self):
        self._login_as("employee", user_id=221, username="emp21")
        page = self.client.get("/inventory/add-stock", follow_redirects=False)
        token = page.text.split("var token = ", 1)[1].split(";", 1)[0].strip().strip('"')
        r = self.client.post(
            "/inventory/batch/confirm",
            headers={"X-CSRF-Token": token},
            json=[
                {
                    "card_name": "Pikachu",
                    "game": "Pokemon",
                    "set_name": "Base Set",
                    "card_number": "58/102",
                    "variant": "Normal",
                    "condition": "NM",
                    "auto_price": 12.34,
                }
            ],
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["created"], 1)

    def test_employee_scan_shell_hides_inventory_admin_actions(self):
        self._login_as("employee", user_id=216, username="emp16")
        r = self.client.get("/inventory/scan?team_shell=1", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("Back to Inventory", r.text)
        self.assertNotIn("+ Add New Item", r.text)
        self.assertIn("ask a manager", r.text)

    # ---------- Pages that should STAY gated above employee ----------

    def test_employee_can_open_add_stock(self):
        self._login_as("employee", user_id=207, username="emp6")
        r = self.client.get("/inventory/add-stock", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Add Stock", r.text)

    def test_employee_can_open_limited_inventory_list(self):
        self._login_as("employee", user_id=220, username="emp20")
        from app.models import InventoryItem

        self.session.add(
            InventoryItem(
                barcode="DGN-EMPINV1",
                item_type="sealed",
                game="Pokemon",
                card_name="Employee Visible ETB",
                set_name="Test Set",
                cost_basis=12.34,
                auto_price=49.99,
            )
        )
        self.session.commit()
        r = self.client.get("/inventory", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Employee Visible ETB", r.text)
        self.assertIn("$49.99", r.text)
        self.assertNotIn("<th>Cost</th>", r.text)
        self.assertNotIn("$12.34", r.text)

    def test_employee_inventory_detail_hides_manager_fields(self):
        self._login_as("employee", user_id=222, username="emp22")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-EMPINV2",
            item_type="sealed",
            game="Pokemon",
            card_name="Employee Detail ETB",
            set_name="Test Set",
            cost_basis=23.45,
            auto_price=59.99,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)

        r = self.client.get(f"/inventory/{item.id}", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Employee Detail ETB", r.text)
        self.assertIn("$59.99", r.text)
        self.assertNotIn("Cost Basis", r.text)
        self.assertNotIn("Save Changes", r.text)
        self.assertNotIn("Push to Shopify", r.text)
        self.assertNotIn("Archive Item", r.text)

    def test_admin_inventory_management_actions_are_visible(self):
        self._login_as("admin", user_id=223, username="adm23")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-ADMIN1",
            item_type="sealed",
            game="Pokemon",
            card_name="Admin Editable ETB",
            set_name="Test Set",
            auto_price=59.99,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)

        list_response = self.client.get("/inventory", follow_redirects=False)
        self.assertEqual(list_response.status_code, 200)
        self.assertIn(f'href="/inventory/{item.id}#edit-item"', list_response.text)

        detail_response = self.client.get(f"/inventory/{item.id}", follow_redirects=False)
        self.assertEqual(detail_response.status_code, 200)
        self.assertIn('id="edit-item"', detail_response.text)
        self.assertIn('id="adjust-stock"', detail_response.text)
        self.assertIn(f'action="/inventory/{item.id}/delete"', detail_response.text)
        self.assertIn("Archive Item", detail_response.text)

    def test_admin_can_archive_and_restore_inventory_item_with_history_intact(self):
        self._login_as("admin", user_id=224, username="adm24")
        from app.models import InventoryItem, InventoryStockMovement, PriceHistory

        item = InventoryItem(
            barcode="DGN-DEL1",
            item_type="sealed",
            game="Pokemon",
            card_name="Delete Me Booster Box",
            set_name="Test Set",
            auto_price=199.99,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)
        item_id = item.id
        self.session.add(
            PriceHistory(
                item_id=item_id,
                source="tcgplayer",
                market_price=199.99,
            )
        )
        self.session.add(
            InventoryStockMovement(
                item_id=item_id,
                reason="receive",
                quantity_delta=1,
                quantity_before=0,
                quantity_after=1,
            )
        )
        self.session.commit()

        detail_response = self.client.get(f"/inventory/{item_id}", follow_redirects=False)
        csrf = self._csrf_from_html(detail_response.text)
        response = self.client.post(
            f"/inventory/{item_id}/delete",
            headers={"X-CSRF-Token": csrf},
            data={"archive_reason": "duplicate"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/inventory?deleted=Delete+Me+Booster+Box", response.headers["location"])
        self.session.expire_all()
        archived = self.session.get(InventoryItem, item_id)
        self.assertIsNotNone(archived)
        self.assertIsNotNone(archived.archived_at)
        self.assertEqual(archived.archive_reason, "duplicate")
        self.assertEqual(
            len(self.session.exec(select(PriceHistory).where(PriceHistory.item_id == item_id)).all()),
            1,
        )
        self.assertEqual(
            len(
                self.session.exec(
                    select(InventoryStockMovement).where(InventoryStockMovement.item_id == item_id)
                ).all()
            ),
            1,
        )

        restore_page = self.client.get(f"/inventory/{item_id}", follow_redirects=False)
        restore_csrf = self._csrf_from_html(restore_page.text)
        restore = self.client.post(
            f"/inventory/{item_id}/restore",
            headers={"X-CSRF-Token": restore_csrf},
            follow_redirects=False,
        )
        self.assertEqual(restore.status_code, 303)
        self.session.expire_all()
        restored = self.session.get(InventoryItem, item_id)
        self.assertIsNone(restored.archived_at)
        self.assertIsNone(restored.archive_reason)

    def test_manager_can_adjust_stock_with_movement_log(self):
        self._login_as("manager", user_id=225, username="mgr25")
        from app.models import InventoryItem, InventoryStockMovement

        item = InventoryItem(
            barcode="DGN-MGR1",
            item_type="sealed",
            game="Pokemon",
            card_name="Manager Stock ETB",
            set_name="Test Set",
            quantity=5,
            auto_price=49.99,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)
        item_id = item.id

        detail_response = self.client.get(f"/inventory/{item_id}", follow_redirects=False)
        self.assertEqual(detail_response.status_code, 200)
        self.assertIn('id="adjust-stock"', detail_response.text)
        csrf = self._csrf_from_html(detail_response.text)

        response = self.client.post(
            f"/inventory/{item_id}/adjust-stock",
            headers={"X-CSRF-Token": csrf},
            data={
                "quantity_delta": "-2",
                "reason": "missing",
                "location": "Shelf B",
                "source": "Cycle Count",
                "notes": "Could not find two boxes",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        adjusted = self.session.get(InventoryItem, item_id)
        self.assertEqual(adjusted.quantity, 3)
        self.assertEqual(adjusted.location, "Shelf B")
        movement = self.session.exec(
            select(InventoryStockMovement).where(InventoryStockMovement.item_id == item_id)
        ).one()
        self.assertEqual(movement.reason, "missing")
        self.assertEqual(movement.quantity_delta, -2)
        self.assertEqual(movement.quantity_before, 5)
        self.assertEqual(movement.quantity_after, 3)
        self.assertEqual(movement.created_by, "mgr25")

    def test_manager_can_bulk_update_inventory_location(self):
        self._login_as("manager", user_id=226, username="mgr26")
        from app.models import InventoryItem, InventoryStockMovement

        items = [
            InventoryItem(
                barcode="DGN-BULK1",
                item_type="sealed",
                game="Pokemon",
                card_name="Bulk One",
                quantity=2,
            ),
            InventoryItem(
                barcode="DGN-BULK2",
                item_type="sealed",
                game="Pokemon",
                card_name="Bulk Two",
                quantity=4,
            ),
        ]
        self.session.add_all(items)
        self.session.commit()
        for item in items:
            self.session.refresh(item)

        page = self.client.get("/inventory", follow_redirects=False)
        self.assertIn('action="/inventory/bulk-action"', page.text)
        csrf = self._csrf_from_html(page.text)
        response = self.client.post(
            "/inventory/bulk-action",
            headers={"X-CSRF-Token": csrf},
            data={
                "bulk_action": "set_location",
                "bulk_location": "Case 3",
                "bulk_reason": "Moved to showcase",
                "item_id": [str(items[0].id), str(items[1].id)],
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/inventory?updated=2", response.headers["location"])
        self.session.expire_all()
        for item in items:
            refreshed = self.session.get(InventoryItem, item.id)
            self.assertEqual(refreshed.location, "Case 3")
        movements = self.session.exec(select(InventoryStockMovement)).all()
        self.assertEqual(len(movements), 2)
        self.assertEqual({row.reason for row in movements}, {"bulk_location"})

    def test_portal_viewer_blocked_from_legacy_reports(self):
        self._login_as("viewer", user_id=217, username="viewer1")
        r = self.client.get("/reports", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_portal_manager_blocked_from_legacy_reviewer_pages(self):
        self._login_as("manager", user_id=218, username="manager1")
        r = self.client.get("/bookkeeping", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_legacy_ops_permission_is_explicit(self):
        from app.auth import LEGACY_OPS_PERMISSION, has_legacy_role
        from app.models import RolePermission, User
        from sqlmodel import select

        viewer = User(
            id=219,
            username="viewer2",
            password_hash="x",
            password_salt="x",
            display_name="viewer2",
            role="viewer",
            is_active=True,
        )
        self.session.add(viewer)
        self.session.commit()

        self.assertFalse(has_legacy_role(self.session, viewer, "viewer"))
        permission = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "viewer",
                RolePermission.resource_key == LEGACY_OPS_PERMISSION,
            )
        ).first()
        if permission is None:
            permission = RolePermission(
                role="viewer",
                resource_key=LEGACY_OPS_PERMISSION,
            )
        permission.is_allowed = True
        self.session.add(permission)
        self.session.commit()
        self.assertTrue(has_legacy_role(self.session, viewer, "viewer"))

    # ---------- TikTok streamer access ----------

    def test_employee_can_open_tiktok_streamer_dashboard(self):
        self._login_as("employee", user_id=208, username="emp7")
        r = self.client.get("/tiktok/streamer", follow_redirects=False)
        self.assertEqual(r.status_code, 200, f"streamer denied: {r.status_code}")

    def test_employee_can_open_live_hits(self):
        self._login_as("employee", user_id=223, username="emp23")
        r = self.client.get("/hits", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Live Hits", r.text)
        self.assertIn('href="/team/"', r.text)
        self.assertNotIn('href="/dashboard"', r.text)
        self.assertNotIn('href="/reports"', r.text)
        self.assertNotIn('href="/bookkeeping"', r.text)

    def test_employee_ops_permissions_gate_direct_urls(self):
        self._login_as("employee", user_id=224, username="emp24")
        from app.models import RolePermission
        from sqlmodel import select

        disabled_keys = (
            "ops.inventory.view",
            "ops.inventory.receive",
            "ops.live_hits.view",
            "ops.live_stream.view",
            "ops.degen_eye.view",
        )
        for key in disabled_keys:
            permission = self.session.exec(
                select(RolePermission).where(
                    RolePermission.role == "employee",
                    RolePermission.resource_key == key,
                )
            ).first()
            self.assertIsNotNone(permission, f"missing seeded permission: {key}")
            permission.is_allowed = False
            self.session.add(permission)
        self.session.commit()

        portal = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(portal.status_code, 200)
        for hidden_link in (
            'href="/inventory"',
            'href="/inventory/add-stock"',
            'href="/hits"',
            'href="/tiktok/streamer?team_shell=1"',
            'href="/degen_eye?team_shell=1"',
        ):
            self.assertNotIn(hidden_link, portal.text)

        for url in (
            "/inventory",
            "/inventory/add-stock",
            "/hits",
            "/tiktok/streamer",
            "/degen_eye",
        ):
            r = self.client.get(url, follow_redirects=False)
            self.assertEqual(r.status_code, 403, f"{url} should honor ops permissions")

    def test_streamer_dashboard_hides_ops_links_for_employees(self):
        self._login_as("employee", user_id=209, username="emp8")
        html = self.client.get("/tiktok/streamer", follow_redirects=False).text
        # Employee-safe tiles: the Team Portal + Degen Eye must be there.
        self.assertIn('href="/team/">Team Portal</a>', html)
        self.assertIn('href="/degen_eye">Degen Eye</a>', html)
        # Ops-only subgroup labels only render inside {% if _is_ops %}. Their
        # absence is the clean signal that the whole ops block was skipped.
        self.assertNotIn(
            '<div class="nav-dropdown-label">Operators</div>',
            html,
            "ops subgroup leaked into employee streamer view",
        )
        self.assertNotIn(
            '<div class="nav-dropdown-label">TikTok</div>',
            html,
            "internal TikTok subgroup leaked into employee streamer view",
        )
        # Specific dashboard / admin / bookkeeping anchors must also be gone.
        self.assertNotIn('<a href="/dashboard">', html)
        self.assertNotIn('<a href="/admin">', html)
        self.assertNotIn('<a href="/bookkeeping">', html)

    def test_streamer_dashboard_shows_ops_links_for_admin(self):
        self._login_as("admin", user_id=210, username="adm2")
        html = self.client.get("/tiktok/streamer", follow_redirects=False).text
        self.assertIn('<a href="/dashboard">', html)
        self.assertIn('<a href="/admin">', html)
        self.assertIn('<a href="/bookkeeping">', html)
        self.assertIn(
            '<div class="nav-dropdown-label">Operators</div>', html,
        )

    # ---------- Unauthenticated requests still redirect ----------

    def test_anonymous_redirected_from_degen_eye(self):
        # No _login_as(); stub get_request_user to return None so middleware
        # doesn't try to hit the (real) configured DB.
        from app import shared
        import app.main as app_main
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=None)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=None)
        self._patcher_main.start()
        r = self.client.get("/degen_eye", follow_redirects=False)
        self.assertIn(r.status_code, (302, 303, 307))


if __name__ == "__main__":
    unittest.main()
