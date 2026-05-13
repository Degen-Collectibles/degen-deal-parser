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
from unittest.mock import AsyncMock, patch

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
                quantity=6,
                cost_basis=12.34,
                auto_price=49.99,
            )
        )
        self.session.commit()
        r = self.client.get("/inventory", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Employee Visible ETB", r.text)
        self.assertIn("<th>Qty</th>", r.text)
        self.assertIn('class="qty-badge">6</span>', r.text)
        self.assertIn("$49.99", r.text)
        self.assertNotIn("/adjust-stock", r.text)
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

    def test_manager_can_set_quantity_from_inventory_list(self):
        self._login_as("manager", user_id=227, username="mgr27")
        from app.models import InventoryItem, InventoryStockMovement

        item = InventoryItem(
            barcode="DGN-LISTQTY1",
            item_type="sealed",
            game="Pokemon",
            card_name="List Quantity ETB",
            set_name="Test Set",
            quantity=5,
            auto_price=49.99,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)
        item_id = item.id

        page = self.client.get("/inventory?q=List+Quantity", follow_redirects=False)
        self.assertEqual(page.status_code, 200)
        self.assertIn("<th>Qty</th>", page.text)
        self.assertIn(f'action="/inventory/{item_id}/adjust-stock"', page.text)
        self.assertIn('name="target_quantity"', page.text)
        csrf = self._csrf_from_html(page.text)

        response = self.client.post(
            f"/inventory/{item_id}/adjust-stock",
            headers={"X-CSRF-Token": csrf},
            data={
                "target_quantity": "8",
                "reason": "stock_count",
                "source": "Inventory List",
                "return_to": "/inventory?q=List+Quantity",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/inventory?q=List+Quantity&updated=1", response.headers["location"])
        self.session.expire_all()
        adjusted = self.session.get(InventoryItem, item_id)
        self.assertEqual(adjusted.quantity, 8)
        movement = self.session.exec(
            select(InventoryStockMovement).where(InventoryStockMovement.item_id == item_id)
        ).one()
        self.assertEqual(movement.reason, "stock_count")
        self.assertEqual(movement.quantity_delta, 3)
        self.assertEqual(movement.quantity_before, 5)
        self.assertEqual(movement.quantity_after, 8)

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

    def test_inventory_list_surfaces_tcgplayer_market_gap_first(self):
        self._login_as("manager", user_id=236, username="mgr36")
        from app.models import InventoryItem

        fine = InventoryItem(
            barcode="DGN-PRICEOK",
            item_type="sealed",
            game="Pokemon",
            card_name="Fine Box",
            quantity=1,
            list_price=101.0,
            auto_price=100.0,
        )
        review = InventoryItem(
            barcode="DGN-PRICEBAD",
            item_type="sealed",
            game="Pokemon",
            card_name="Review Box",
            quantity=1,
            list_price=70.0,
            auto_price=100.0,
        )
        self.session.add_all([fine, review])
        self.session.commit()

        page = self.client.get("/inventory", follow_redirects=False)

        self.assertEqual(page.status_code, 200)
        self.assertIn("TCGPlayer Market", page.text)
        self.assertIn("Review Prices", page.text)
        self.assertIn("price-review-row", page.text)
        self.assertLess(page.text.index("Review Box"), page.text.index("Fine Box"))
        self.assertIn('action="/inventory/', page.text)
        self.assertIn("priced below TCGPlayer market", page.text)

    def test_inventory_price_review_ignores_prices_above_market(self):
        self._login_as("manager", user_id=241, username="mgr41")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-PRICEHIGH",
            item_type="sealed",
            game="Pokemon",
            card_name="High Price Box",
            quantity=1,
            list_price=130.0,
            auto_price=100.0,
        )
        self.session.add(item)
        self.session.commit()

        page = self.client.get("/inventory", follow_redirects=False)

        self.assertEqual(page.status_code, 200)
        self.assertNotIn("priced below TCGPlayer market", page.text)
        self.assertNotIn("Review Prices", page.text)

    def test_manager_can_bulk_reprice_inventory(self):
        self._login_as("manager", user_id=237, username="mgr37")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-REPRICE1",
            item_type="sealed",
            game="Pokemon",
            card_name="Reprice Box",
            quantity=1,
            list_price=50.0,
            auto_price=82.0,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)

        page = self.client.get("/inventory", follow_redirects=False)
        csrf = self._csrf_from_html(page.text)
        response = self.client.post(
            "/inventory/bulk-action",
            headers={"X-CSRF-Token": csrf},
            data={"bulk_action": "reprice", "item_id": [str(item.id)]},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/inventory?repriced=1&price_errors=0", response.headers["location"])
        self.session.expire_all()
        refreshed = self.session.get(InventoryItem, item.id)
        self.assertEqual(refreshed.list_price, 82.0)
        self.assertEqual(refreshed.auto_price, 82.0)

    def test_manager_can_bulk_reprice_inventory_above_market(self):
        self._login_as("manager", user_id=238, username="mgr38")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-REPRICE5",
            item_type="sealed",
            game="Pokemon",
            card_name="Markup Box",
            quantity=1,
            list_price=50.0,
            auto_price=100.0,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)

        page = self.client.get("/inventory", follow_redirects=False)
        csrf = self._csrf_from_html(page.text)
        response = self.client.post(
            "/inventory/bulk-action",
            headers={"X-CSRF-Token": csrf},
            data={"bulk_action": "reprice_5", "item_id": [str(item.id)]},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(InventoryItem, item.id)
        self.assertEqual(refreshed.list_price, 105.0)

    def test_manager_can_bulk_edit_inventory_qty_cost_and_price(self):
        self._login_as("manager", user_id=239, username="mgr39")
        from app.models import InventoryItem, InventoryStockMovement

        item = InventoryItem(
            barcode="DGN-BULKEDIT",
            item_type="sealed",
            game="Pokemon",
            card_name="Bulk Edit Box",
            quantity=2,
            cost_basis=10.0,
            list_price=20.0,
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)

        page = self.client.get("/inventory?edit=1", follow_redirects=False)
        self.assertIn("Edit Mode", page.text)
        self.assertIn(f'name="bulk_qty_{item.id}"', page.text)
        self.assertIn(f'data-target-name="bulk_price_{item.id}"', page.text)
        self.assertIn('type="button" data-market-fill="1"', page.text)
        self.assertIn('data-markup-percent="5"', page.text)
        self.assertIn('data-markup-percent="10"', page.text)
        self.assertIn("Save Selected Edits", page.text)
        csrf = self._csrf_from_html(page.text)
        response = self.client.post(
            "/inventory/bulk-action",
            headers={"X-CSRF-Token": csrf},
            data={
                "bulk_action": "bulk_edit",
                "item_id": [str(item.id)],
                f"bulk_qty_{item.id}": "7",
                f"bulk_cost_{item.id}": "12.34",
                f"bulk_price_{item.id}": "29.99",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/inventory?updated=1", response.headers["location"])
        self.session.expire_all()
        refreshed = self.session.get(InventoryItem, item.id)
        self.assertEqual(refreshed.quantity, 7)
        self.assertEqual(refreshed.cost_basis, 12.34)
        self.assertEqual(refreshed.list_price, 29.99)
        movement = self.session.exec(
            select(InventoryStockMovement).where(InventoryStockMovement.item_id == item.id)
        ).one()
        self.assertEqual(movement.reason, "bulk_edit_qty")
        self.assertEqual(movement.quantity_delta, 5)

    def test_add_stock_existing_item_has_market_markup_buttons(self):
        self._login_as("manager", user_id=240, username="mgr40")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-ADDPRICE",
            item_type="sealed",
            game="Pokemon",
            card_name="Existing Market Box",
            quantity=3,
            list_price=90.0,
            auto_price=100.0,
        )
        self.session.add(item)
        self.session.commit()

        with patch("app.inventory._cached_add_stock_sealed_search", new=AsyncMock(return_value=([], ""))):
            page = self.client.get(
                "/inventory/add-stock?game=Pokemon&search_type=sealed&q=Existing+Market+Box",
                follow_redirects=False,
            )

        self.assertEqual(page.status_code, 200)
        self.assertIn('data-market-price="100.00"', page.text)
        self.assertIn('data-price-percent="5"', page.text)
        self.assertIn('data-price-percent="10"', page.text)

    def test_archived_item_hidden_from_default_inventory_list(self):
        self._login_as("admin", user_id=227, username="adm27")
        from app.models import InventoryItem
        from datetime import datetime, timezone

        active = InventoryItem(barcode="DGN-ACTIVE1", item_type="sealed", game="Pokemon", card_name="Active ETB")
        archived = InventoryItem(
            barcode="DGN-ARCH1",
            item_type="sealed",
            game="Pokemon",
            card_name="Archived ETB",
            archived_at=datetime.now(timezone.utc),
            archived_by="adm27",
            archive_reason="duplicate",
        )
        self.session.add_all([active, archived])
        self.session.commit()

        default_list = self.client.get("/inventory", follow_redirects=False)
        self.assertEqual(default_list.status_code, 200)
        self.assertIn("Active ETB", default_list.text)
        self.assertNotIn("Archived ETB", default_list.text)

        archived_list = self.client.get("/inventory?archived=1", follow_redirects=False)
        self.assertEqual(archived_list.status_code, 200)
        self.assertNotIn("Active ETB", archived_list.text)
        self.assertIn("Archived ETB", archived_list.text)

        all_list = self.client.get("/inventory?archived=all", follow_redirects=False)
        self.assertEqual(all_list.status_code, 200)
        self.assertIn("Active ETB", all_list.text)
        self.assertIn("Archived ETB", all_list.text)

    def test_receive_stock_unarchives_item(self):
        self._login_as("manager", user_id=228, username="mgr28")
        from app.models import InventoryItem
        from datetime import datetime, timezone

        item = InventoryItem(
            barcode="DGN-UNARCH1",
            item_type="sealed",
            game="Pokemon",
            card_name="Revived Booster Box",
            set_name="Test Set",
            quantity=0,
            archived_at=datetime.now(timezone.utc),
            archived_by="adm27",
            archive_reason="out of stock",
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)
        item_id = item.id

        page = self.client.get("/inventory/add-stock", follow_redirects=False)
        csrf = self._csrf_from_html(page.text)
        response = self.client.post(
            "/inventory/sealed/receive",
            headers={"X-CSRF-Token": csrf},
            data={
                "item_id": str(item_id),
                "game": "Pokemon",
                "product_name": "Revived Booster Box",
                "quantity": "2",
                "unit_cost": "80",
            },
            follow_redirects=False,
        )
        self.assertIn(response.status_code, (200, 303))
        self.session.expire_all()
        refreshed = self.session.get(InventoryItem, item_id)
        self.assertIsNone(refreshed.archived_at)
        self.assertIsNone(refreshed.archive_reason)

    def test_employee_without_manage_permission_blocked_from_inventory_new(self):
        self._login_as("employee", user_id=229, username="emp29")
        r = self.client.get("/inventory/new", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_resticker_apply_and_dismiss_routes(self):
        self._login_as("admin", user_id=230, username="adm30")
        from app.models import InventoryItem

        item = InventoryItem(
            barcode="DGN-RST1",
            item_type="slab",
            game="Pokemon",
            card_name="Charizard",
            grading_company="PSA",
            grade="10",
            list_price=500.0,
            resticker_alert_active=True,
            resticker_reference_price=500.0,
            resticker_alert_price=625.0,
            resticker_alert_reason="Card Ladder price up 25%",
        )
        self.session.add(item)
        self.session.commit()
        self.session.refresh(item)
        item_id = item.id

        # dismiss clears the alert without changing list_price
        detail = self.client.get(f"/inventory/{item_id}", follow_redirects=False)
        csrf = self._csrf_from_html(detail.text)
        dismiss = self.client.post(
            f"/inventory/{item_id}/resticker/dismiss",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        self.assertIn(dismiss.status_code, (200, 303))
        self.session.expire_all()
        dismissed = self.session.get(InventoryItem, item_id)
        self.assertFalse(dismissed.resticker_alert_active)
        self.assertEqual(dismissed.list_price, 500.0)

        # re-arm the alert and test apply (updates list_price to alert_price)
        dismissed.resticker_alert_active = True
        dismissed.resticker_alert_price = 625.0
        self.session.add(dismissed)
        self.session.commit()

        detail2 = self.client.get(f"/inventory/{item_id}", follow_redirects=False)
        csrf2 = self._csrf_from_html(detail2.text)
        apply = self.client.post(
            f"/inventory/{item_id}/resticker/apply",
            headers={"X-CSRF-Token": csrf2},
            follow_redirects=False,
        )
        self.assertIn(apply.status_code, (200, 303))
        self.session.expire_all()
        applied = self.session.get(InventoryItem, item_id)
        self.assertFalse(applied.resticker_alert_active)
        self.assertEqual(applied.list_price, 625.0)

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
