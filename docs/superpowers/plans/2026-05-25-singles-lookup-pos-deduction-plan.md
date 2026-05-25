# Singles Lookup + POS Deduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make tracked singles usable for employee lookup and Shopify POS checkout deduction without exposing pilot singles on customer-facing sales channels by default.

**Architecture:** Keep the existing `/inventory`, `/inventory/add-stock`, Degen Eye, and Shopify webhook surfaces. Route all single intake through the existing single receive helper so quantities and stock movements are consistent, then add lookup cleanup filters and Shopify guardrails that allow POS-linked deduction while blocking automatic public product creation for singles.

**Tech Stack:** Python 3.14, FastAPI, SQLModel, Jinja2 templates, existing Shopify REST/GraphQL helpers, pytest/unittest-style tests.

---

## References

- Spec: `docs/superpowers/specs/2026-05-25-singles-lookup-pilot-design.md`
- Shopify inventory docs: inventory items have inventory levels per location, and variants map to inventory items.
- Shopify product status docs: `ACTIVE` products are not automatically published to sales channels, while `DRAFT` products are unavailable to sales channels.
- Shopify product publishing docs: product/channel visibility is controlled through publications; `resourcePublicationsV2` and `unpublishedPublications` can audit publication state.

## File Map

- `app/inventory/routes.py`
  - Route scanner batch-confirm singles through `_receive_single_stock()`.
  - Enforce pilot-required location/condition for single receive and scanner batch confirm.
  - Add missing-location and missing-condition filters to `/inventory`.
  - Keep manager-only Shopify sync entry points manager-only.
- `app/templates/inventory_batch_review.html`
  - Add required batch location input and send that location with each scanned single.
- `app/templates/inventory.html`
  - Add missing-location and missing-condition cleanup links/filter state.
- `app/shopify_sync_worker.py`
  - Block automatic Shopify product creation for singles unless the item is already linked or an existing Shopify variant with matching DGN SKU is found.
- `app/inventory/shopify_ingest.py`
  - No expected behavior change; add a single-item POS deduction test around current webhook matching behavior.
- `tests/test_employee_ops_access.py`
  - Route-level tests for scanner batch confirm, receive movement history, merging, and required location.
- `tests/test_sealed_inventory.py`
  - Unit tests for single receive and Shopify POS deduction behavior.
- `tests/test_inventory_shopify_sync.py`
  - Unit tests for Shopify single-creation guardrails.
- `docs/superpowers/specs/2026-05-25-singles-lookup-pilot-design.md`
  - Update if implementation discovers a channel-safety constraint different from the draft.

---

### Task 0: Shopify Channel Safety Preflight

**Files:**
- No file changes.

- [ ] **Step 1: Confirm current Shopify behavior is not assumed**

Do a read-only check before implementing any code that could create or publish Shopify products. The goal is to establish whether the app has Shopify Admin credentials and a configured inventory location. This task does not create products, publish products, or query customer-facing channel availability; the first implementation guardrail is to block automatic single creation unless a safe POS-only flow is proven separately.

Run this only with existing production credentials already present in the environment:

```powershell
@'
from app.config import get_settings
from app.inventory.shopify import shopify_admin_configured, resolve_shopify_access_token

settings = get_settings()
print("store_domain=", settings.shopify_store_domain)
print("admin_configured=", shopify_admin_configured(settings))
print("has_access_token=", bool(resolve_shopify_access_token(settings)))
print("configured_location_id=", settings.shopify_location_id)
'@ | .\.venv\Scripts\python.exe -
```

Expected: prints the configured store domain, whether Shopify admin credentials are configured, and the current configured location id if one exists.

- [ ] **Step 2: If credentials are missing, stop Shopify creation work**

If the preflight prints `admin_configured=False`, do not implement automatic POS product creation. Continue with local lookup and scanner receive tasks, but leave Shopify POS creation/linking as manager-manual until credentials are verified.

- [ ] **Step 3: Record preflight result in the implementation notes**

Add a short note to the eventual PR or final implementation summary:

```text
Shopify preflight: admin_configured=true, configured_location_id=123456789, automatic single product creation=blocked.
```

Expected: no code changes and no Shopify mutations.

---

### Task 1: Scanner Batch Confirm Uses Single Receive Helper

**Files:**
- Modify: `tests/test_employee_ops_access.py`
- Modify: `app/inventory/routes.py`

- [ ] **Step 1: Write the failing route test**

Add this test below `test_employee_can_confirm_scanned_single_batch` in `tests/test_employee_ops_access.py`:

```python
    def test_employee_batch_confirm_merges_single_and_logs_stock_movements(self):
        from app.models import InventoryItem, InventoryStockMovement, ITEM_TYPE_SINGLE

        self._login_as("employee", user_id=231, username="emp31")
        page = self.client.get("/inventory/add-stock", follow_redirects=False)
        token = page.text.split("var token = ", 1)[1].split(";", 1)[0].strip().strip('"')
        payload = [
            {
                "card_name": "Pikachu",
                "game": "Pokemon",
                "set_name": "Base Set",
                "card_number": "58/102",
                "variant": "Normal",
                "condition": "NM",
                "auto_price": 12.34,
                "location": "Case A",
                "source": "Degen Eye",
            }
        ]

        first = self.client.post(
            "/inventory/batch/confirm",
            headers={"X-CSRF-Token": token},
            json=payload,
        )
        second = self.client.post(
            "/inventory/batch/confirm",
            headers={"X-CSRF-Token": token},
            json=payload,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.session.expire_all()
        items = self.session.exec(
            select(InventoryItem).where(
                InventoryItem.item_type == ITEM_TYPE_SINGLE,
                InventoryItem.card_name == "Pikachu",
            )
        ).all()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].quantity, 2)
        self.assertEqual(items[0].location, "Case A")
        movements = self.session.exec(
            select(InventoryStockMovement).where(InventoryStockMovement.item_id == items[0].id)
        ).all()
        self.assertEqual(len(movements), 2)
        self.assertEqual([m.quantity_delta for m in movements], [1, 1])
```

- [ ] **Step 2: Run the focused failing test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_batch_confirm_merges_single_and_logs_stock_movements -q
```

Expected before implementation: FAIL because scanner batch confirm creates duplicate direct `InventoryItem` rows and does not create stock movement rows for singles.

- [ ] **Step 3: Replace direct scanner single creation with `_receive_single_stock()`**

In `app/inventory/routes.py`, inside `inventory_batch_confirm()`, replace the `else:` branch that directly constructs `InventoryItem(...)` for singles with:

```python
        else:
            quantity_raw = raw.get("quantity") or 1
            try:
                quantity_value = max(1, int(quantity_raw))
            except (TypeError, ValueError):
                quantity_value = 1

            try:
                item, _movement, _created = _receive_single_stock(
                    session,
                    game=(raw.get("game") or "Other").strip(),
                    card_name=card_name,
                    set_name=(raw.get("set_name") or "").strip(),
                    set_code=(raw.get("set_code") or "").strip(),
                    card_number=(raw.get("card_number") or "").strip(),
                    variant=(raw.get("variant") or "").strip(),
                    condition=(raw.get("condition") or "NM").strip(),
                    image_url=(raw.get("image_url") or "").strip(),
                    quantity=quantity_value,
                    unit_cost=_parse_float(str(raw.get("cost_basis") or raw.get("unit_cost") or "")),
                    list_price=_parse_float(str(raw.get("list_price") or "")),
                    auto_price=_parse_float(str(raw.get("auto_price") or "")),
                    low_price=_parse_float(str(raw.get("low_price") or "")),
                    location=(raw.get("location") or "").strip(),
                    source=(raw.get("source") or "Degen Eye").strip(),
                    notes=(raw.get("notes") or "").strip(),
                    price_payload={
                        "scanner_payload": raw,
                        "source": raw.get("source") or "Degen Eye",
                    },
                    actor_label=_current_user_label(request),
                )
            except ValueError as exc:
                return JSONResponse(
                    {"error": str(exc), "card_name": card_name},
                    status_code=400,
                )
```

Leave the existing `created.append(...)`, capture-label update, and final `session.commit()` behavior in place.

- [ ] **Step 4: Run the focused passing test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_batch_confirm_merges_single_and_logs_stock_movements -q
```

Expected after implementation: PASS.

- [ ] **Step 5: Run nearby inventory tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_can_confirm_scanned_single_batch tests/test_sealed_inventory.py::SealedInventoryTests::test_receive_single_tracks_variant_condition_and_price_history -q
```

Expected: PASS.

- [ ] **Step 6: Commit task if committing this branch**

```powershell
git add app/inventory/routes.py tests/test_employee_ops_access.py
git commit -m "Route scanned singles through inventory receive"
```

Expected: commit succeeds only after the focused tests pass.

---

### Task 2: Require Location For Pilot Single Intake

**Files:**
- Modify: `tests/test_employee_ops_access.py`
- Modify: `tests/test_sealed_inventory.py`
- Modify: `app/inventory/routes.py`
- Modify: `app/templates/inventory_batch_review.html`

- [ ] **Step 1: Add missing-location tests**

Add this route-level test to `tests/test_employee_ops_access.py`:

```python
    def test_employee_batch_confirm_requires_location_for_singles(self):
        self._login_as("employee", user_id=232, username="emp32")
        page = self.client.get("/inventory/add-stock", follow_redirects=False)
        token = page.text.split("var token = ", 1)[1].split(";", 1)[0].strip().strip('"')

        response = self.client.post(
            "/inventory/batch/confirm",
            headers={"X-CSRF-Token": token},
            json=[
                {
                    "card_name": "No Location Pikachu",
                    "game": "Pokemon",
                    "condition": "NM",
                }
            ],
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Location is required", response.json()["error"])
```

Add this manual receive test to `tests/test_sealed_inventory.py`:

```python
    def test_single_receive_route_requires_location(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/inventory/singles/receive",
                "headers": [],
            }
        )
        with Session(self.engine) as session:
            with patch("app.inventory.routes._require_employee_permission", return_value=None):
                response = asyncio.run(
                    inventory_singles_receive(
                        request=request,
                        session=session,
                        game="Pokemon",
                        card_name="Missing Location Pikachu",
                        condition="NM",
                        quantity="1",
                        location="",
                    )
                )

            self.assertEqual(response.status_code, 303)
            self.assertIn("single_error=Location+is+required", response.headers["location"])
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_batch_confirm_requires_location_for_singles tests/test_sealed_inventory.py::SealedInventoryTests::test_single_receive_route_requires_location -q
```

Expected before implementation: FAIL because location is currently optional.

- [ ] **Step 3: Enforce location in manual single receive**

In `inventory_singles_receive()` in `app/inventory/routes.py`, after the negative price checks and before parsing `variants_json`, add:

```python
    if not location.strip():
        params = urlencode({
            "game": selected_game,
            "search_type": selected_search_type,
            "q": card_name or "",
            "single_error": "Location is required.",
        })
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)
```

- [ ] **Step 4: Enforce location in scanner batch confirm**

In `inventory_batch_confirm()` before calling `_receive_single_stock()` for singles, add:

```python
            location_value = (raw.get("location") or "").strip()
            if not location_value:
                return JSONResponse(
                    {"error": "Location is required for scanned singles.", "card_name": card_name},
                    status_code=400,
                )
```

Then pass `location=location_value` into `_receive_single_stock()`.

- [ ] **Step 5: Add batch location to the review UI**

In `app/templates/inventory_batch_review.html`, add a location control near the confirm bar:

```html
            <label class="field-label" for="batch-location">Location</label>
            <input id="batch-location" class="name-input" type="text" placeholder="Case A, Binder 1, Back Stock" autocomplete="off">
```

In `confirmBatch()`, replace the fetch body with a payload that requires the location:

```javascript
        var locationInput = document.getElementById('batch-location');
        var location = locationInput ? locationInput.value.trim() : '';
        if (!location) {
            alert('Location is required before adding scanned singles to inventory.');
            if (locationInput) locationInput.focus();
            return;
        }
        var payload = batch.map(function(item) {
            var copy = Object.assign({}, item);
            if (!copy.location) copy.location = location;
            if (!copy.source) copy.source = 'Degen Eye';
            return copy;
        });
```

Then change:

```javascript
                body: JSON.stringify(batch)
```

to:

```javascript
                body: JSON.stringify(payload)
```

- [ ] **Step 6: Update the existing scanner confirm smoke test**

In `tests/test_employee_ops_access.py`, update `test_employee_can_confirm_scanned_single_batch` so its JSON payload includes a location:

```python
                    "location": "Case A",
```

Expected: the existing smoke test continues to prove employees can confirm a scanned single, now using the required pilot location.

- [ ] **Step 7: Run focused tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_can_confirm_scanned_single_batch tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_batch_confirm_requires_location_for_singles tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_employee_batch_confirm_merges_single_and_logs_stock_movements tests/test_sealed_inventory.py::SealedInventoryTests::test_single_receive_route_requires_location -q
```

Expected: PASS.

- [ ] **Step 8: Commit task if committing this branch**

```powershell
git add app/inventory/routes.py app/templates/inventory_batch_review.html tests/test_employee_ops_access.py tests/test_sealed_inventory.py
git commit -m "Require locations for pilot single intake"
```

---

### Task 3: Add Missing Location / Condition Lookup Cleanup Filters

**Files:**
- Modify: `tests/test_employee_ops_access.py`
- Modify: `app/inventory/routes.py`
- Modify: `app/templates/inventory.html`

- [ ] **Step 1: Add filter test**

Add this test to `tests/test_employee_ops_access.py`:

```python
    def test_inventory_lookup_can_filter_missing_single_locations(self):
        from app.models import InventoryItem, ITEM_TYPE_SINGLE

        self._login_as("viewer", user_id=233, username="viewer33")
        self.session.add(
            InventoryItem(
                barcode="DGN-MISSLOC",
                item_type=ITEM_TYPE_SINGLE,
                game="Pokemon",
                card_name="Missing Location Card",
                condition="NM",
                quantity=1,
            )
        )
        self.session.add(
            InventoryItem(
                barcode="DGN-HASLOC",
                item_type=ITEM_TYPE_SINGLE,
                game="Pokemon",
                card_name="Located Card",
                condition="NM",
                quantity=1,
                location="Case A",
            )
        )
        self.session.commit()

        response = self.client.get("/inventory?item_type=single&missing_location=1", follow_redirects=False)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Missing Location Card", response.text)
        self.assertNotIn("Located Card", response.text)
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_inventory_lookup_can_filter_missing_single_locations -q
```

Expected before implementation: FAIL because `/inventory` ignores `missing_location`.

- [ ] **Step 3: Add route query params and filters**

In `inventory_list()` in `app/inventory/routes.py`, add query params:

```python
    missing_location: str = Query(default=""),
    missing_condition: str = Query(default=""),
```

After the existing `price_review` filter, add:

```python
    if missing_location == "1":
        query = query.where(
            InventoryItem.item_type == ITEM_TYPE_SINGLE,
            (InventoryItem.location == None) | (InventoryItem.location == ""),  # noqa: E711
        )
    if missing_condition == "1":
        query = query.where(
            InventoryItem.item_type == ITEM_TYPE_SINGLE,
            (InventoryItem.condition == None) | (InventoryItem.condition == ""),  # noqa: E711
        )
```

Add summary counts:

```python
        "missing_single_locations": session.exec(
            select(func.count()).where(
                active_items,
                InventoryItem.item_type == ITEM_TYPE_SINGLE,
                (InventoryItem.location == None) | (InventoryItem.location == ""),  # noqa: E711
            )
        ).one(),
        "missing_single_conditions": session.exec(
            select(func.count()).where(
                active_items,
                InventoryItem.item_type == ITEM_TYPE_SINGLE,
                (InventoryItem.condition == None) | (InventoryItem.condition == ""),  # noqa: E711
            )
        ).one(),
```

Pass template context:

```python
            "missing_location_filter": missing_location,
            "missing_condition_filter": missing_condition,
```

- [ ] **Step 4: Add cleanup links in `inventory.html`**

Near the existing inventory summary/filter area, add:

```html
        {% if inventory_summary.missing_single_locations %}
        <div class="warning-banner">
            <span>{{ inventory_summary.missing_single_locations }} single{% if inventory_summary.missing_single_locations != 1 %}s{% endif %} need a location.</span>
            <a href="/inventory?item_type=single&missing_location=1" class="btn-sm">Review Locations</a>
        </div>
        {% endif %}
        {% if inventory_summary.missing_single_conditions %}
        <div class="warning-banner">
            <span>{{ inventory_summary.missing_single_conditions }} single{% if inventory_summary.missing_single_conditions != 1 %}s{% endif %} need a condition.</span>
            <a href="/inventory?item_type=single&missing_condition=1" class="btn-sm">Review Conditions</a>
        </div>
        {% endif %}
```

Use existing warning/success banner classes already present in the template; do not introduce a new page or side tool.

- [ ] **Step 5: Run focused test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py::EmployeeOpsAccessTests::test_inventory_lookup_can_filter_missing_single_locations -q
```

Expected: PASS.

- [ ] **Step 6: Commit task if committing this branch**

```powershell
git add app/inventory/routes.py app/templates/inventory.html tests/test_employee_ops_access.py
git commit -m "Add inventory cleanup filters for pilot singles"
```

---

### Task 4: Verify POS Sale Deducts Local Singles

**Files:**
- Modify: `tests/test_sealed_inventory.py`

- [ ] **Step 1: Add single POS deduction test**

Add this test near the existing Shopify sale webhook tests in `tests/test_sealed_inventory.py`:

```python
    def test_shopify_pos_sale_decrements_single_quantity_and_logs_movement(self) -> None:
        from app.inventory.shopify_ingest import mark_inventory_sold_from_shopify_order

        with Session(self.engine) as session:
            item = InventoryItem(
                barcode="DGN-POS1",
                item_type=ITEM_TYPE_SINGLE,
                game="Pokemon",
                card_name="POS Pikachu",
                set_name="Base Set",
                card_number="58/102",
                condition="NM",
                location="Case A",
                quantity=2,
            )
            session.add(item)
            session.commit()
            session.refresh(item)

            marked = mark_inventory_sold_from_shopify_order(
                session,
                {
                    "id": "pos-order-1",
                    "name": "#POS1",
                    "line_items": [
                        {"sku": "DGN-POS1", "quantity": 1, "price": "12.34", "title": "POS Pikachu"},
                    ],
                },
                runtime_name="unit-test",
            )

            self.assertEqual(marked, 1)
            session.refresh(item)
            self.assertEqual(item.quantity, 1)
            self.assertNotEqual(item.status, INVENTORY_SOLD)
            movement = session.exec(
                select(InventoryStockMovement).where(InventoryStockMovement.item_id == item.id)
            ).one()
            self.assertEqual(movement.reason, "sale")
            self.assertEqual(movement.source, "Shopify")
            self.assertEqual(movement.quantity_delta, -1)
            self.assertEqual(movement.quantity_before, 2)
            self.assertEqual(movement.quantity_after, 1)
```

- [ ] **Step 2: Run the test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sealed_inventory.py::SealedInventoryTests::test_shopify_pos_sale_decrements_single_quantity_and_logs_movement -q
```

Expected: PASS with current code. If it fails, fix only `mark_inventory_sold_from_shopify_order()` enough to make SKU-matched singles behave like sealed inventory.

- [ ] **Step 3: Run existing webhook idempotency and unknown SKU tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sealed_inventory.py::SealedInventoryTests::test_shopify_order_retry_does_not_double_decrement_inventory tests/test_sealed_inventory.py::SealedInventoryTests::test_shopify_unknown_sku_creates_visible_sync_issue -q
```

Expected: PASS.

- [ ] **Step 4: Commit task if committing this branch**

```powershell
git add tests/test_sealed_inventory.py
git commit -m "Cover Shopify POS deduction for singles"
```

---

### Task 5: Block Automatic Shopify Product Creation For Singles

**Files:**
- Modify: `tests/test_inventory_shopify_sync.py`
- Modify: `app/shopify_sync_worker.py`

- [ ] **Step 1: Add single-creation guardrail test**

In `tests/test_inventory_shopify_sync.py`, add `ITEM_TYPE_SINGLE` to the model import list:

```python
    ITEM_TYPE_SINGLE,
```

Add this test to `ShopifySyncWorkerTests`:

```python
    async def test_single_without_existing_shopify_variant_is_not_created_publicly(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-SINGLE1",
                    item_type=ITEM_TYPE_SINGLE,
                    game="Pokemon",
                    card_name="Unsafe Public Single",
                    quantity=1,
                    list_price=19.99,
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.find_shopify_variant_by_sku",
                    new=AsyncMock(return_value=None),
                ) as mocked_find, patch(
                    "app.shopify_sync_worker.push_item_to_shopify",
                    new=AsyncMock(return_value={"shopify_variant_id": "SHOULD_NOT_CREATE"}),
                ) as mocked_push:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = "shpat_test"
                    mocked_settings.shopify_api_key = ""
                    mocked_settings.shopify_location_id = "444"

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertFalse(ok)
                self.assertIn("Singles must be linked to an existing POS-only Shopify variant", error)
                mocked_find.assert_awaited_once_with(
                    "DGN-SINGLE1",
                    store_domain="degen-test.myshopify.com",
                    access_token="shpat_test",
                )
                mocked_push.assert_not_awaited()
        finally:
            engine.dispose()
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_inventory_shopify_sync.py::ShopifySyncWorkerTests::test_single_without_existing_shopify_variant_is_not_created_publicly -q
```

Expected before implementation: FAIL because the worker currently calls `push_item_to_shopify()` when no variant is found.

- [ ] **Step 3: Block single creation after SKU lookup**

In `sync_inventory_item_to_shopify()` in `app/shopify_sync_worker.py`, after the `find_shopify_variant_by_sku()` block and before `push_item_to_shopify()`, add:

```python
        if not item.shopify_variant_id and item.item_type == "single":
            return (
                False,
                "Singles must be linked to an existing POS-only Shopify variant before sync. "
                "Automatic Shopify product creation for singles is blocked to avoid publishing pilot inventory.",
            )
```

Do not change sealed-product behavior in this task.

- [ ] **Step 4: Run focused and regression tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_inventory_shopify_sync.py::ShopifySyncWorkerTests::test_single_without_existing_shopify_variant_is_not_created_publicly tests/test_inventory_shopify_sync.py::ShopifySyncWorkerTests::test_sync_inventory_item_updates_price_quantity_and_status -q
```

Expected: both PASS.

- [ ] **Step 5: Commit task if committing this branch**

```powershell
git add app/shopify_sync_worker.py tests/test_inventory_shopify_sync.py
git commit -m "Block automatic Shopify creation for singles"
```

---

### Task 6: Add POS-Linked Existing Variant Coverage

**Files:**
- Modify: `tests/test_inventory_shopify_sync.py`

- [ ] **Step 1: Add test for linking an existing Shopify SKU**

Add this test to `ShopifySyncWorkerTests`:

```python
    async def test_single_with_existing_shopify_variant_can_sync_quantity(self):
        from app.inventory.shopify import ShopifyVariantRef

        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-SINGLE2",
                    item_type=ITEM_TYPE_SINGLE,
                    game="Pokemon",
                    card_name="POS Linked Single",
                    quantity=1,
                    list_price=24.99,
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                variant = ShopifyVariantRef(
                    sku="DGN-SINGLE2",
                    product_id="111",
                    variant_id="222",
                    inventory_item_id="333",
                    location_gid="gid://shopify/Location/444",
                    product_status="ACTIVE",
                )

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.find_shopify_variant_by_sku",
                    new=AsyncMock(return_value=variant),
                ) as mocked_find, patch(
                    "app.shopify_sync_worker.update_shopify_variant_price",
                    new=AsyncMock(return_value=True),
                ) as mocked_price, patch(
                    "app.shopify_sync_worker.sync_shopify_inventory_quantity",
                    new=AsyncMock(return_value=(True, None)),
                ) as mocked_qty:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = "shpat_test"
                    mocked_settings.shopify_api_key = ""
                    mocked_settings.shopify_location_id = "444"

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(error, "")
                mocked_find.assert_awaited_once()
                mocked_price.assert_awaited_once()
                mocked_qty.assert_awaited_once()
                session.refresh(item)
                self.assertEqual(item.shopify_variant_id, "222")
                self.assertEqual(item.shopify_inventory_item_id, "333")
                self.assertEqual(item.shopify_sku, "DGN-SINGLE2")
                self.assertEqual(item.shopify_sync_status, "synced")
        finally:
            engine.dispose()
```

- [ ] **Step 2: Run the test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_inventory_shopify_sync.py::ShopifySyncWorkerTests::test_single_with_existing_shopify_variant_can_sync_quantity -q
```

Expected: PASS after Task 5. If it fails, adjust only the single guardrail location so existing SKU links are allowed and only product creation is blocked.

- [ ] **Step 3: Commit task if committing this branch**

```powershell
git add tests/test_inventory_shopify_sync.py
git commit -m "Cover POS-linked Shopify singles"
```

---

### Task 7: Update Pilot Operating Notes

**Files:**
- Modify: `docs/superpowers/specs/2026-05-25-singles-lookup-pilot-design.md`
- Create: `docs/plans/singles-lookup-pos-pilot-runbook.md`

- [ ] **Step 1: Create the shop-floor runbook**

Create `docs/plans/singles-lookup-pos-pilot-runbook.md`:

```markdown
# Singles Lookup + POS Pilot Runbook

## Goal

Use Degen inventory as the employee lookup source for tracked singles, and use Shopify POS only for checkout of linked DGN-SKU products.

## What To Inventory During Pilot

- Binder cards
- Case cards
- High-demand singles
- Higher-value singles
- Cards customers ask about repeatedly

Do not inventory bulk commons during the pilot.

## Required Fields

- Card identity
- Condition
- Quantity
- Location/bin
- Degen barcode
- Optional sell price

## POS Checkout Rule

If a tracked single is sold in person, use the Shopify POS product/variant whose SKU is the Degen barcode. Do not ring tracked singles as custom products if inventory deduction matters.

## Channel Rule

Pilot singles are POS-only by default. Do not publish them to Online Store, TikTok, Shop, Google, marketplaces, or other customer-facing channels unless a manager explicitly approves that product.

## Cleanup Queue

Managers review:

- missing location
- missing condition
- duplicate-looking singles
- Shopify unknown SKU issues
- anything that appears customer-facing unexpectedly
```

- [ ] **Step 2: Update PRD status**

In `docs/superpowers/specs/2026-05-25-singles-lookup-pilot-design.md`, change:

```markdown
Status: Draft v2 for Jeffrey review
```

to:

```markdown
Status: Approved for implementation planning
```

Only do this if Jeffrey has approved the implementation direction in chat.

- [ ] **Step 3: Commit task if committing this branch**

```powershell
git add docs/superpowers/specs/2026-05-25-singles-lookup-pilot-design.md docs/plans/singles-lookup-pos-pilot-runbook.md
git commit -m "Document singles POS pilot runbook"
```

---

### Task 8: Final Verification

**Files:**
- No file changes expected.

- [ ] **Step 1: Compile app code**

Run:

```powershell
.\.venv\Scripts\python.exe -m compileall app
```

Expected: compile succeeds.

- [ ] **Step 2: Run focused test suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_employee_ops_access.py tests/test_sealed_inventory.py tests/test_inventory_shopify_sync.py -q
```

Expected: PASS.

- [ ] **Step 3: Run full test suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest --tb=short -q
```

Expected baseline from `AGENTS.md`: all tests pass except the known acceptable `test_schedule_mobile.py` sandbox-only failures if they still exist in this environment. Any other failure must be fixed before commit/push.

- [ ] **Step 4: Manual local smoke**

Start local web:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1
```

Smoke these pages:

```text
http://127.0.0.1:8000/inventory
http://127.0.0.1:8000/inventory/add-stock
http://127.0.0.1:8000/inventory/scan/batch-review
```

Expected:

- `/inventory` loads.
- Missing-location cleanup link appears when seeded data has missing locations.
- `/inventory/scan/batch-review` requires location before confirming.
- No employee-facing flow publishes a single to customer-facing channels.

- [ ] **Step 5: Final git scope check**

Run:

```powershell
git status --short
git diff --stat
```

Expected: only files from this plan are changed.

---

## Execution Notes

- Do not SSH to Machine B for this plan unless Jeffrey explicitly asks or local verification shows a production-only issue.
- Do not register new Shopify webhooks. Existing order webhooks are the deduction path.
- Do not push any Shopify product creation behavior for singles until POS-only channel safety is verified.
- If automatic POS-only creation is not provably safe, ship the pilot with manager-manual POS-only Shopify product linking and local lookup improvements first.
