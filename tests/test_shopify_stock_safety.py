from __future__ import annotations

import asyncio
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest
from jinja2 import DictLoader, Environment
from sqlalchemy import event
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select
from starlette.requests import Request

from app.inventory.shopify_ingest import (
    _begin_stock_mutation_transaction,
    mark_inventory_sold_from_shopify_order,
    normalize_shopify_order_identity,
)
from app.models import (
    INVENTORY_IN_STOCK,
    INVENTORY_SOLD,
    InventoryItem,
    InventoryStockMovement,
    ShopifySyncIssue,
    ShopifySyncJob,
)


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _item(*, barcode: str, quantity: int = 5) -> InventoryItem:
    return InventoryItem(
        barcode=barcode,
        item_type="sealed",
        game="Pokemon",
        card_name=f"{barcode} Booster Box",
        quantity=quantity,
        status=INVENTORY_IN_STOCK,
    )


def _paid_payload(
    *,
    order_id: str | None = "order-1001",
    sku: str = "DGN-SAFE1",
    quantity: int = 1,
    current_quantity: int | None = None,
    name: str | None = "#1001",
) -> dict:
    line_item = {
        "sku": sku,
        "quantity": quantity,
        "price": "19.99",
        "title": f"{sku} Product",
        "variant_id": f"variant-{sku}",
    }
    if current_quantity is not None:
        line_item["current_quantity"] = current_quantity
    payload = {
        "financial_status": "paid",
        "line_items": [line_item],
    }
    if order_id is not None:
        payload["id"] = order_id
    if name is not None:
        payload["name"] = name
    return payload


def _rows(session: Session, model):
    return session.exec(select(model)).all()


_MISSING_QUANTITY = object()


@pytest.mark.parametrize("financial_status", ["pending", None])
def test_non_paid_or_missing_status_defers_without_local_side_effects(
    financial_status, capsys
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload()
            if financial_status is None:
                payload.pop("financial_status")
            else:
                payload["financial_status"] = financial_status

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
            assert _rows(session, ShopifySyncIssue) == []

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        event = next(
            row
            for row in events
            if row["action"] == "inventory.shopify_order.stock_mutation_deferred"
        )
        assert event["financial_status"] == (financial_status or "")
    finally:
        engine.dispose()


def test_pending_then_paid_same_order_deducts_exactly_once():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)
            payload = _paid_payload(quantity=2)

            pending = dict(payload, financial_status="pending")
            assert mark_inventory_sold_from_shopify_order(session, pending) == 0
            assert mark_inventory_sold_from_shopify_order(session, payload) == 1
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            assert item.quantity == 3
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert len(_rows(session, ShopifySyncJob)) == 1
    finally:
        engine.dispose()


def test_exact_paid_replay_uses_one_stable_unique_movement_and_one_job():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)
            payload = _paid_payload(order_id="order-replay", quantity=2)

            assert mark_inventory_sold_from_shopify_order(session, payload) == 1
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            movements = _rows(session, InventoryStockMovement)
            assert item.quantity == 3
            assert len(movements) == 1
            assert movements[0].dedupe_key == (
                f"shopify-order:order-replay:inventory-item:{item.id}"
            )
            assert len(_rows(session, ShopifySyncJob)) == 1
    finally:
        engine.dispose()


def test_paid_delivery_claims_zero_stock_so_old_replay_cannot_consume_restock():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=0)
            item.status = INVENTORY_SOLD
            session.add(item)
            session.commit()
            session.refresh(item)
            item_id = item.id

            payload = _paid_payload(order_id="order-already-sold", quantity=2)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            movements = _rows(session, InventoryStockMovement)
            assert item.quantity == 0
            assert len(movements) == 1
            assert movements[0].dedupe_key == (
                f"shopify-order:order-already-sold:inventory-item:{item.id}"
            )
            assert movements[0].requested_quantity == 2
            assert movements[0].quantity_delta == 0
            assert movements[0].quantity_before == 0
            assert movements[0].quantity_after == 0
            assert _rows(session, ShopifySyncJob) == []

        with Session(engine) as session:
            item = session.get(InventoryItem, item_id)
            assert item is not None
            assert len(_rows(session, InventoryStockMovement)) == 1
            item.quantity = 5
            item.status = INVENTORY_IN_STOCK
            session.add(item)
            session.commit()

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            session.refresh(item)
            assert item.quantity == 5
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert _rows(session, ShopifySyncJob) == []
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "invalid_quantity",
    [
        _MISSING_QUANTITY,
        None,
        "",
        "   ",
        True,
        False,
        -1,
        1.5,
        Decimal("1.5"),
        float("nan"),
        float("inf"),
        float("-inf"),
        "1.5",
    ],
    ids=[
        "missing",
        "none",
        "empty-string",
        "blank-string",
        "true",
        "false",
        "negative",
        "fractional-float",
        "fractional-decimal",
        "nan",
        "positive-infinity",
        "negative-infinity",
        "fractional-string",
    ],
)
def test_invalid_paid_quantity_records_deduped_nonimportable_issue(
    invalid_quantity, capsys
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=5)
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id="order-invalid-quantity")
            if invalid_quantity is _MISSING_QUANTITY:
                payload["line_items"][0].pop("quantity")
            else:
                payload["line_items"][0]["quantity"] = invalid_quantity

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "invalid_order_quantity"
            assert issue.issue_key == (
                "invalid_order_quantity:order-invalid-quantity:variant-DGN-SAFE1"
            )
            assert issue.inventory_item_id == item.id
            assert issue.quantity == 0
            assert issue.occurrence_count == 2

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        invalid_events = [
            event
            for event in events
            if event["action"] == "inventory.shopify_order.invalid_quantity"
        ]
        assert len(invalid_events) == 2
        assert all(event["success"] is False for event in invalid_events)

        from app import shopify_sync

        assert (
            "invalid_order_quantity"
            not in shopify_sync.SHOPIFY_SYNC_IMPORTABLE_ISSUE_TYPES
        )
    finally:
        engine.dispose()


def test_invalid_quantity_issue_key_stays_stable_when_inventory_link_appears():
    engine = _engine()
    try:
        payload = _paid_payload(
            order_id="order-invalid-link",
            sku="DGN-LINK-LATER",
        )
        payload["line_items"][0]["quantity"] = "not-a-quantity"

        with Session(engine) as session:
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_key == (
                "invalid_order_quantity:order-invalid-link:variant-DGN-LINK-LATER"
            )
            assert issue.inventory_item_id is None

            item = _item(barcode="DGN-LINK-LATER")
            session.add(item)
            session.commit()
            session.refresh(item)

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            issues = _rows(session, ShopifySyncIssue)
            assert len(issues) == 1
            assert issues[0].issue_key == (
                "invalid_order_quantity:order-invalid-link:variant-DGN-LINK-LATER"
            )
            assert issues[0].inventory_item_id == item.id
            assert issues[0].occurrence_count == 2
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("quantity", "expected_remaining"),
    [
        (0, 5),
        (0.0, 5),
        (Decimal("0"), 5),
        ("0", 5),
        (2, 3),
        (2.0, 3),
        (Decimal("2"), 3),
        ("2", 3),
    ],
    ids=[
        "zero-int",
        "zero-float",
        "zero-decimal",
        "zero-string",
        "positive-int",
        "positive-float",
        "positive-decimal",
        "positive-string",
    ],
)
def test_explicit_nonnegative_integral_quantity_is_valid(
    quantity, expected_remaining
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=5)
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id="order-valid-quantity")
            payload["line_items"][0]["quantity"] = quantity

            marked = mark_inventory_sold_from_shopify_order(session, payload)
            session.refresh(item)
            assert item.quantity == expected_remaining
            assert _rows(session, ShopifySyncIssue) == []
            if expected_remaining == 5:
                assert marked == 0
                assert _rows(session, InventoryStockMovement) == []
                assert _rows(session, ShopifySyncJob) == []
            else:
                assert marked == 1
                movement = session.exec(select(InventoryStockMovement)).one()
                assert movement.requested_quantity == 2
                assert movement.quantity_delta == -2
                assert len(_rows(session, ShopifySyncJob)) == 1
    finally:
        engine.dispose()


def test_requested_quantity_distinguishes_replay_from_later_quantity_edit():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=1)
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id="order-requested", quantity=3)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 1
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            assert _rows(session, ShopifySyncIssue) == []

            movement = session.exec(select(InventoryStockMovement)).one()
            assert movement.quantity_delta == -1
            assert movement.requested_quantity == 3

            edited = _paid_payload(
                order_id="order-requested",
                quantity=3,
                current_quantity=1,
            )
            assert mark_inventory_sold_from_shopify_order(session, edited) == 0

            session.refresh(item)
            assert item.quantity == 0
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert len(_rows(session, ShopifySyncJob)) == 1
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "order_quantity_changed"
            payload_json = json.loads(issue.raw_payload_json)
            assert payload_json["requested_quantity"] == 3
            assert payload_json["applied_quantity"] == 1
            assert payload_json["current_quantity"] == 1
    finally:
        engine.dispose()


def test_name_only_paid_delivery_records_invalid_identity_then_real_id_deducts_once():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id=None, name="#name-only-1001")
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []

            issues = _rows(session, ShopifySyncIssue)
            assert len(issues) == 1
            issue = issues[0]
            expected_hash = hashlib.sha256(
                json.dumps(
                    payload,
                    default=str,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            assert issue.issue_type == "invalid_order_identity"
            assert issue.issue_key == f"invalid_order_identity:{expected_hash}"
            assert issue.occurrence_count == 2
            assert issue.status == "open"

            identified = dict(payload, id="immutable-order-1001")
            assert mark_inventory_sold_from_shopify_order(session, identified) == 1
            assert mark_inventory_sold_from_shopify_order(session, identified) == 0
            session.refresh(item)
            assert item.quantity == 4
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert len(_rows(session, ShopifySyncJob)) == 1
            assert len(_rows(session, ShopifySyncIssue)) == 1
    finally:
        engine.dispose()


def test_paid_order_without_id_logs_invalid_identity_and_creates_visible_issue(capsys):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id=None, name=None)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            session.refresh(item)
            assert item.quantity == 5
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "invalid_order_identity"
            assert issue.status == "open"
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []

        assert "inventory.shopify_order.invalid_identity" in capsys.readouterr().out
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "invalid_identity",
    [
        False,
        0,
        -1,
        1.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        [],
        ["nested"],
        {},
        {"nested": "value"},
        ("nested",),
    ],
    ids=[
        "bool",
        "zero",
        "negative",
        "fractional",
        "nan",
        "positive-infinity",
        "negative-infinity",
        "empty-list",
        "list",
        "empty-dict",
        "dict",
        "tuple",
    ],
)
def test_malformed_order_identity_records_stable_issue_without_stock_side_effects(
    invalid_identity,
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id=None, name="#bad-identity")
            payload["id"] = invalid_identity
            expected_hash = hashlib.sha256(
                json.dumps(
                    payload,
                    default=str,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()

            assert normalize_shopify_order_identity(payload) == ""
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "invalid_order_identity"
            assert issue.issue_key == f"invalid_order_identity:{expected_hash}"
            assert issue.occurrence_count == 2
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("identity", "expected"),
    [
        (1, "1"),
        (987654321012345678, "987654321012345678"),
        ("order-test-1001", "order-test-1001"),
        ("  order-test-1001  ", "order-test-1001"),
    ],
)
def test_valid_positive_integer_and_nonblank_string_order_identities(
    identity, expected
):
    assert normalize_shopify_order_identity({"id": identity}) == expected


@pytest.mark.parametrize(
    "financial_status",
    ["paid", "pending", None],
    ids=["paid", "pending", "absent"],
)
def test_missing_order_identity_records_one_deduped_issue_before_status_handling(
    financial_status,
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(order_id=None, name="#missing-id")
            if financial_status is None:
                payload.pop("financial_status")
            else:
                payload["financial_status"] = financial_status

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            issues = _rows(session, ShopifySyncIssue)
            assert item.quantity == 5
            assert len(issues) == 1
            assert issues[0].issue_type == "invalid_order_identity"
            assert issues[0].occurrence_count == 2
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
    finally:
        engine.dispose()


def test_current_quantity_overrides_historical_quantity():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(quantity=5, current_quantity=2)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 1

            session.refresh(item)
            movement = session.exec(select(InventoryStockMovement)).one()
            assert item.quantity == 3
            assert movement.quantity_delta == -2
    finally:
        engine.dispose()


def test_zero_current_quantity_does_not_deduct_or_queue_sync():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1")
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = _paid_payload(quantity=5, current_quantity=0)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
    finally:
        engine.dispose()


def test_legacy_shopify_movement_is_still_treated_as_replay():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=3)
            session.add(item)
            session.commit()
            session.refresh(item)
            session.add(
                InventoryStockMovement(
                    item_id=item.id,
                    reason="sale",
                    quantity_delta=-2,
                    quantity_before=5,
                    quantity_after=3,
                    source="Shopify",
                    notes="Shopify order legacy-order",
                )
            )
            session.commit()

            payload = _paid_payload(order_id="legacy-order", quantity=2)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

            session.refresh(item)
            assert item.quantity == 3
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert _rows(session, ShopifySyncJob) == []
            assert _rows(session, ShopifySyncIssue) == []
    finally:
        engine.dispose()


def test_paid_quantity_edit_records_durable_issue_without_second_mutation():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=10)
            session.add(item)
            session.commit()
            session.refresh(item)

            initial = _paid_payload(order_id="order-edited", quantity=2)
            edited = _paid_payload(
                order_id="order-edited", quantity=2, current_quantity=3
            )
            assert mark_inventory_sold_from_shopify_order(session, initial) == 1
            assert mark_inventory_sold_from_shopify_order(session, edited) == 0

            session.refresh(item)
            assert item.quantity == 8
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert len(_rows(session, ShopifySyncJob)) == 1
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "order_quantity_changed"
            assert issue.issue_key == f"order_quantity_changed:order-edited:{item.id}"
            assert issue.inventory_item_id == item.id
            assert issue.shopify_order_id == "order-edited"
            assert issue.shopify_sku == "DGN-SAFE1"
            assert "requested quantity 2" in issue.message
            assert "applied quantity 2" in issue.message
            assert "current paid quantity 3" in issue.message
            issue_payload = json.loads(issue.raw_payload_json)
            assert issue_payload["requested_quantity"] == 2
            assert issue_payload["applied_quantity"] == 2
            assert issue_payload["current_quantity"] == 3
    finally:
        engine.dispose()


def test_quantity_edit_to_zero_preserves_zero_on_non_importable_issue():
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-SAFE1", quantity=5)
            session.add(item)
            session.commit()
            session.refresh(item)

            initial = _paid_payload(order_id="order-zero-edit", quantity=2)
            removed = _paid_payload(
                order_id="order-zero-edit",
                quantity=2,
                current_quantity=0,
            )
            assert mark_inventory_sold_from_shopify_order(session, initial) == 1
            assert mark_inventory_sold_from_shopify_order(session, removed) == 0

            session.refresh(item)
            assert item.quantity == 3
            issue = session.exec(select(ShopifySyncIssue)).one()
            assert issue.issue_type == "order_quantity_changed"
            assert issue.quantity == 0
            assert "requested quantity 2" in issue.message
            assert "current paid quantity 0" in issue.message
            payload_json = json.loads(issue.raw_payload_json)
            assert payload_json["requested_quantity"] == 2
            assert payload_json["current_quantity"] == 0
    finally:
        engine.dispose()


class _FirstResult:
    def __init__(self, value):
        self.value = value

    def first(self):
        return self.value


class _ForcedMovementConflictSession(Session):
    def __init__(
        self,
        *args,
        conflict_key: str,
        hide_existing_once: bool = False,
        original_error: BaseException | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conflict_key = conflict_key
        self.hide_existing_once = hide_existing_once
        self.original_error = original_error or sqlite3.IntegrityError(
            "UNIQUE constraint failed: inventory_stock_movements.dedupe_key"
        )
        self.conflict_raised = False

    def exec(self, statement, *args, **kwargs):
        sql = str(statement)
        if (
            self.hide_existing_once
            and "inventory_stock_movements.dedupe_key" in sql
            and "WHERE" in sql
        ):
            self.hide_existing_once = False
            return _FirstResult(None)
        return super().exec(statement, *args, **kwargs)

    def flush(self, objects=None):
        has_target = any(
            isinstance(obj, InventoryStockMovement)
            and obj.dedupe_key == self.conflict_key
            for obj in self.new
        )
        if has_target and not self.conflict_raised:
            self.conflict_raised = True
            raise IntegrityError("INSERT inventory_stock_movements", {}, self.original_error)
        return super().flush(objects)


def test_exact_key_integrity_conflict_is_classified_as_replay():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item = _item(barcode="DGN-SAFE1")
            setup.add(item)
            setup.commit()
            setup.refresh(item)
            item_id = item.id
            key = f"shopify-order:order-race:inventory-item:{item_id}"
            setup.add(
                InventoryStockMovement(
                    item_id=item_id,
                    reason="forced_conflict_winner",
                    quantity_delta=-1,
                    quantity_before=5,
                    quantity_after=4,
                    dedupe_key=key,
                    requested_quantity=1,
                )
            )
            setup.commit()

        with _ForcedMovementConflictSession(
            engine,
            conflict_key=key,
            hide_existing_once=True,
        ) as session:
            payload = _paid_payload(order_id="order-race", quantity=1)
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            item = session.get(InventoryItem, item_id)
            assert item.quantity == 5
            assert len(_rows(session, InventoryStockMovement)) == 1
            assert _rows(session, ShopifySyncJob) == []
    finally:
        engine.dispose()


def test_unrelated_integrity_error_without_exact_key_propagates():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item = _item(barcode="DGN-SAFE1")
            setup.add(item)
            setup.commit()
            setup.refresh(item)
            item_id = item.id
        key = f"shopify-order:order-error:inventory-item:{item_id}"

        with _ForcedMovementConflictSession(
            engine,
            conflict_key=key,
            original_error=sqlite3.IntegrityError(
                "NOT NULL constraint failed: inventory_stock_movements.quantity_delta"
            ),
        ) as session:
            payload = _paid_payload(order_id="order-error", quantity=1)
            with pytest.raises(IntegrityError):
                mark_inventory_sold_from_shopify_order(session, payload)
    finally:
        engine.dispose()


def test_unrelated_integrity_error_propagates_even_when_exact_key_exists():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item = _item(barcode="DGN-SAFE1")
            setup.add(item)
            setup.commit()
            setup.refresh(item)
            item_id = item.id
            key = f"shopify-order:order-unrelated:inventory-item:{item_id}"
            setup.add(
                InventoryStockMovement(
                    item_id=item_id,
                    reason="forced_conflict_winner",
                    quantity_delta=-1,
                    quantity_before=5,
                    quantity_after=4,
                    dedupe_key=key,
                    requested_quantity=1,
                )
            )
            setup.commit()

        with _ForcedMovementConflictSession(
            engine,
            conflict_key=key,
            hide_existing_once=True,
            original_error=sqlite3.IntegrityError(
                "CHECK constraint failed: quantity_after_nonnegative"
            ),
        ) as session:
            with pytest.raises(IntegrityError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-unrelated", quantity=1),
                )
    finally:
        engine.dispose()


def test_sqlite_dedupe_near_match_propagates_even_when_exact_key_exists():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item = _item(barcode="DGN-SAFE1")
            setup.add(item)
            setup.commit()
            setup.refresh(item)
            item_id = item.id
            key = f"shopify-order:order-near-match:inventory-item:{item_id}"
            setup.add(
                InventoryStockMovement(
                    item_id=item_id,
                    reason="forced_conflict_winner",
                    quantity_delta=-1,
                    quantity_before=5,
                    quantity_after=4,
                    dedupe_key=key,
                    requested_quantity=1,
                )
            )
            setup.commit()

        with _ForcedMovementConflictSession(
            engine,
            conflict_key=key,
            hide_existing_once=True,
            original_error=sqlite3.IntegrityError(
                "UNIQUE constraint failed: inventory_stock_movements.dedupe_key; "
                "secondary constraint detail"
            ),
        ) as session:
            with pytest.raises(IntegrityError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-near-match", quantity=1),
                )
    finally:
        engine.dispose()


def test_postgres_named_dedupe_constraint_is_recognized():
    from app.inventory import shopify_ingest

    classifier = getattr(shopify_ingest, "_is_stock_movement_dedupe_conflict", None)
    assert callable(classifier)

    class Diag:
        constraint_name = "idx_inventory_stock_movements_dedupe_key"

    class PgUniqueViolation(Exception):
        diag = Diag()

    unique_error = IntegrityError("INSERT", {}, PgUniqueViolation("duplicate key"))
    assert classifier(unique_error)

    Diag.constraint_name = "some_other_unique_constraint"
    unrelated_error = IntegrityError("INSERT", {}, PgUniqueViolation("duplicate key"))
    assert not classifier(unrelated_error)


class _SchemaUnavailableSession(Session):
    missing_column = "dedupe_key"

    def exec(self, statement, *args, **kwargs):
        if "inventory_stock_movements.dedupe_key" in str(statement):
            raise OperationalError(
                f"SELECT inventory_stock_movements.{self.missing_column}",
                {},
                sqlite3.OperationalError(
                    f"no such column: inventory_stock_movements.{self.missing_column}"
                ),
            )
        return super().exec(statement, *args, **kwargs)


class _RequestedQuantitySchemaUnavailableSession(_SchemaUnavailableSession):
    missing_column = "requested_quantity"


class _PgDiag:
    def __init__(self, sqlstate: str):
        self.sqlstate = sqlstate


class _PgDatabaseError(Exception):
    def __init__(self, message: str, *, sqlstate: str):
        super().__init__(message)
        self.pgcode = sqlstate
        self.sqlstate = sqlstate
        self.diag = _PgDiag(sqlstate)


class _LookupDatabaseErrorSession(Session):
    def __init__(self, *args, lookup_error: BaseException, **kwargs):
        super().__init__(*args, **kwargs)
        self.lookup_error = lookup_error

    def exec(self, statement, *args, **kwargs):
        if "inventory_stock_movements.dedupe_key" in str(statement):
            raise self.lookup_error
        return super().exec(statement, *args, **kwargs)


class _InsertDatabaseErrorSession(Session):
    def __init__(self, *args, insert_error: BaseException, **kwargs):
        super().__init__(*args, **kwargs)
        self.insert_error = insert_error
        self.error_raised = False

    def flush(self, objects=None):
        has_movement = any(
            isinstance(obj, InventoryStockMovement) for obj in self.new
        )
        if has_movement and not self.error_raised:
            self.error_raised = True
            raise self.insert_error
        return super().flush(objects)


def test_missing_dedupe_schema_logs_failure_and_raises(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            item = _item(barcode="DGN-SAFE1")
            setup.add(item)
            setup.commit()

        with _SchemaUnavailableSession(engine) as session:
            with pytest.raises(OperationalError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-missing-schema", quantity=1),
                )
            session.rollback()

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        event = next(
            row
            for row in events
            if row["action"] == "inventory.shopify_order.schema_unavailable"
        )
        assert event["success"] is False
        assert event["required_column"] == "inventory_stock_movements.dedupe_key"
        with Session(engine) as verify:
            assert verify.exec(select(InventoryItem)).one().quantity == 5
            assert _rows(verify, InventoryStockMovement) == []
            assert _rows(verify, ShopifySyncJob) == []
    finally:
        engine.dispose()


def test_lookup_missing_requested_quantity_logs_exact_column_and_raises(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1"))
            setup.commit()

        with _RequestedQuantitySchemaUnavailableSession(engine) as session:
            with pytest.raises(OperationalError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-missing-requested", quantity=1),
                )
            session.rollback()

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        event = next(
            row
            for row in events
            if row["action"] == "inventory.shopify_order.schema_unavailable"
        )
        assert event["success"] is False
        assert event["required_column"] == (
            "inventory_stock_movements.requested_quantity"
        )
        with Session(engine) as verify:
            assert verify.exec(select(InventoryItem)).one().quantity == 5
            assert _rows(verify, InventoryStockMovement) == []
            assert _rows(verify, ShopifySyncJob) == []
    finally:
        engine.dispose()


def test_postgres_undefined_requested_quantity_lookup_logs_and_raises(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1"))
            setup.commit()

        original = _PgDatabaseError(
            'column inventory_stock_movements.requested_quantity does not exist',
            sqlstate="42703",
        )
        wrapped = ProgrammingError("SELECT inventory_stock_movements", {}, original)
        with _LookupDatabaseErrorSession(engine, lookup_error=wrapped) as session:
            with pytest.raises(ProgrammingError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-pg-lookup-schema", quantity=1),
                )
            session.rollback()

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        event = next(
            row
            for row in events
            if row["action"] == "inventory.shopify_order.schema_unavailable"
        )
        assert event["required_column"] == (
            "inventory_stock_movements.requested_quantity"
        )
    finally:
        engine.dispose()


def test_postgres_undefined_dedupe_key_insert_logs_and_raises(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1"))
            setup.commit()

        original = _PgDatabaseError(
            'column "dedupe_key" of relation "inventory_stock_movements" does not exist',
            sqlstate="42703",
        )
        wrapped = ProgrammingError("INSERT inventory_stock_movements", {}, original)
        with _InsertDatabaseErrorSession(engine, insert_error=wrapped) as session:
            with pytest.raises(ProgrammingError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-pg-insert-schema", quantity=1),
                )
            session.rollback()

        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip().startswith("{")
        ]
        event = next(
            row
            for row in events
            if row["action"] == "inventory.shopify_order.schema_unavailable"
        )
        assert event["required_column"] == "inventory_stock_movements.dedupe_key"
    finally:
        engine.dispose()


def test_deadlock_operational_error_propagates_without_schema_log(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1"))
            setup.commit()

        wrapped = OperationalError(
            "SELECT inventory_stock_movements",
            {},
            sqlite3.OperationalError("database is locked"),
        )
        with _LookupDatabaseErrorSession(engine, lookup_error=wrapped) as session:
            with pytest.raises(OperationalError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-deadlock", quantity=1),
                )
            session.rollback()

        assert "inventory.shopify_order.schema_unavailable" not in capsys.readouterr().out
    finally:
        engine.dispose()


def test_non_schema_programming_error_propagates_without_schema_log(capsys):
    engine = _engine()
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1"))
            setup.commit()

        original = _PgDatabaseError(
            'syntax error at or near "inventory_stock_movements"',
            sqlstate="42601",
        )
        wrapped = ProgrammingError("SELECT inventory_stock_movements", {}, original)
        with _LookupDatabaseErrorSession(engine, lookup_error=wrapped) as session:
            with pytest.raises(ProgrammingError):
                mark_inventory_sold_from_shopify_order(
                    session,
                    _paid_payload(order_id="order-pg-syntax", quantity=1),
                )
            session.rollback()

        assert "inventory.shopify_order.schema_unavailable" not in capsys.readouterr().out
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("unsafe_line", "expected_issue_type"),
    [
        (
            {"sku": "DGN-Z-BAD", "quantity": "invalid", "price": "2.00"},
            "invalid_order_quantity",
        ),
        (
            {"sku": "DGN-Z-MISSING", "quantity": 1, "price": "2.00"},
            "unknown_sku",
        ),
    ],
    ids=["invalid-quantity", "unknown-sku"],
)
def test_multi_sku_safety_issue_prevents_every_stock_mutation(
    unsafe_line, expected_issue_type
):
    engine = _engine()
    try:
        with Session(engine) as session:
            item = _item(barcode="DGN-A", quantity=5)
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = {
                "id": f"order-all-or-nothing-{expected_issue_type}",
                "financial_status": "paid",
                "line_items": [
                    {"sku": "DGN-A", "quantity": 1, "price": "1.00"},
                    unsafe_line,
                ],
            }

            assert mark_inventory_sold_from_shopify_order(session, payload) == 0
            session.refresh(item)
            assert item.quantity == 5
            assert _rows(session, InventoryStockMovement) == []
            assert _rows(session, ShopifySyncJob) == []
            issues = _rows(session, ShopifySyncIssue)
            assert len(issues) == 1
            assert issues[0].issue_type == expected_issue_type
    finally:
        engine.dispose()


def test_multi_sku_quantity_change_issue_prevents_every_stock_mutation():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item_a = _item(barcode="DGN-A", quantity=5)
            item_b = _item(barcode="DGN-B", quantity=4)
            item_c = _item(barcode="DGN-C", quantity=5)
            setup.add_all([item_a, item_b, item_c])
            setup.commit()
            for item in (item_a, item_b, item_c):
                setup.refresh(item)
            setup.add(
                InventoryStockMovement(
                    item_id=item_b.id,
                    reason="sale",
                    quantity_delta=-1,
                    quantity_before=5,
                    quantity_after=4,
                    source="Shopify",
                    notes="Shopify order order-atomic",
                    dedupe_key=(
                        f"shopify-order:order-atomic:inventory-item:{item_b.id}"
                    ),
                    requested_quantity=1,
                )
            )
            setup.commit()
            item_a_id, item_b_id, item_c_id = item_a.id, item_b.id, item_c.id

        payload = {
            "id": "order-atomic",
            "financial_status": "paid",
            "line_items": [
                {"sku": "DGN-A", "quantity": 1},
                {"sku": "DGN-B", "quantity": 2},
                {"sku": "DGN-C", "quantity": 1},
            ],
        }
        with Session(engine) as session:
            assert mark_inventory_sold_from_shopify_order(session, payload) == 0

        with Session(engine) as verify:
            assert verify.get(InventoryItem, item_a_id).quantity == 5
            assert verify.get(InventoryItem, item_b_id).quantity == 4
            assert verify.get(InventoryItem, item_c_id).quantity == 5
            movements = _rows(verify, InventoryStockMovement)
            assert len(movements) == 1
            assert movements[0].item_id == item_b_id
            assert _rows(verify, ShopifySyncJob) == []
            issues = _rows(verify, ShopifySyncIssue)
            assert len(issues) == 1
            assert issues[0].issue_type == "order_quantity_changed"
            assert issues[0].inventory_item_id == item_b_id
    finally:
        engine.dispose()


def test_multi_sku_failure_rolls_back_early_mutation_job_and_quantity_issue():
    engine = _engine()
    try:
        with Session(engine) as setup:
            item_a = _item(barcode="DGN-A", quantity=5)
            item_b = _item(barcode="DGN-B", quantity=4)
            item_c = _item(barcode="DGN-C", quantity=5)
            setup.add_all([item_a, item_b, item_c])
            setup.commit()
            for item in (item_a, item_b, item_c):
                setup.refresh(item)
            setup.add(
                InventoryStockMovement(
                    item_id=item_b.id,
                    reason="sale",
                    quantity_delta=-1,
                    quantity_before=5,
                    quantity_after=4,
                    source="Shopify",
                    notes="Shopify order order-atomic-failure",
                    dedupe_key=(
                        f"shopify-order:order-atomic-failure:inventory-item:{item_b.id}"
                    ),
                    requested_quantity=1,
                )
            )
            setup.commit()
            item_a_id, item_b_id, item_c_id = item_a.id, item_b.id, item_c.id

        conflict_key = (
            f"shopify-order:order-atomic-failure:inventory-item:{item_c_id}"
        )
        payload = {
            "id": "order-atomic-failure",
            "financial_status": "paid",
            "line_items": [
                {"sku": "DGN-A", "quantity": 1},
                {"sku": "DGN-B", "quantity": 1},
                {"sku": "DGN-C", "quantity": 1},
            ],
        }
        with _ForcedMovementConflictSession(
            engine,
            conflict_key=conflict_key,
            original_error=sqlite3.IntegrityError(
                "CHECK constraint failed: quantity_after_nonnegative"
            ),
        ) as session:
            with pytest.raises(IntegrityError):
                mark_inventory_sold_from_shopify_order(session, payload)
            session.rollback()

        with Session(engine) as verify:
            assert verify.get(InventoryItem, item_a_id).quantity == 5
            assert verify.get(InventoryItem, item_b_id).quantity == 4
            assert verify.get(InventoryItem, item_c_id).quantity == 5
            movements = _rows(verify, InventoryStockMovement)
            assert len(movements) == 1
            assert movements[0].item_id == item_b_id
            assert _rows(verify, ShopifySyncJob) == []
            assert _rows(verify, ShopifySyncIssue) == []
    finally:
        engine.dispose()


class _CaptureInventorySelectSession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inventory_statements = []

    def exec(self, statement, *args, **kwargs):
        descriptions = getattr(statement, "column_descriptions", [])
        if any(row.get("entity") is InventoryItem for row in descriptions):
            self.inventory_statements.append(statement)
        return super().exec(statement, *args, **kwargs)


def test_sqlite_immediate_transaction_begins_before_inventory_read():
    engine = _engine()
    statements: list[str] = []

    def capture_statement(
        _connection, _cursor, statement, _parameters, _context, _many
    ):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_statement)
    try:
        with Session(engine) as session:
            session.add(_item(barcode="DGN-SAFE1"))
            session.commit()
            statements.clear()

            assert mark_inventory_sold_from_shopify_order(
                session,
                _paid_payload(order_id="order-immediate-lock"),
            ) == 1

        normalized = [" ".join(statement.upper().split()) for statement in statements]
        begin_index = normalized.index("BEGIN IMMEDIATE")
        inventory_select_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("SELECT") and "FROM INVENTORY_ITEMS" in statement
        )
        assert begin_index < inventory_select_index
    finally:
        event.remove(engine, "before_cursor_execute", capture_statement)
        engine.dispose()


def test_sqlite_replay_releases_function_owned_immediate_transaction(tmp_path):
    database_path = tmp_path / "shopify-replay-lock.db"
    engine = create_engine(
        f"sqlite:///{database_path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 0.1},
    )
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1", quantity=5))
            setup.commit()

        with Session(engine) as replay_session:
            payload = _paid_payload(order_id="order-replay-lock", quantity=1)
            assert mark_inventory_sold_from_shopify_order(replay_session, payload) == 1
            assert mark_inventory_sold_from_shopify_order(replay_session, payload) == 0
            assert replay_session.in_transaction() is False

            driver_connection = (
                replay_session.connection().connection.dbapi_connection
            )
            assert driver_connection.in_transaction is False

            with Session(engine) as second_writer:
                assert mark_inventory_sold_from_shopify_order(
                    second_writer,
                    _paid_payload(order_id="order-after-replay", quantity=1),
                ) == 1
    finally:
        engine.dispose()


def test_sqlite_replay_preserves_caller_owned_transaction(tmp_path):
    database_path = tmp_path / "shopify-caller-transaction.db"
    engine = create_engine(
        f"sqlite:///{database_path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 0.1},
    )
    SQLModel.metadata.create_all(engine)
    try:
        payload = _paid_payload(order_id="order-caller-owned", quantity=1)
        with Session(engine) as setup:
            setup.add(_item(barcode="DGN-SAFE1", quantity=5))
            assert mark_inventory_sold_from_shopify_order(setup, payload) == 1

        with Session(engine) as caller:
            pending = _item(barcode="DGN-CALLER-PENDING", quantity=1)
            caller.add(pending)
            assert caller.in_transaction() is True

            assert mark_inventory_sold_from_shopify_order(caller, payload) == 0
            assert caller.in_transaction() is True
            caller.commit()

        with Session(engine) as verify:
            persisted = verify.exec(
                select(InventoryItem).where(
                    InventoryItem.barcode == "DGN-CALLER-PENDING"
                )
            ).one()
            assert persisted.quantity == 1
    finally:
        engine.dispose()


class _TransactionOwnershipDialect:
    name = "postgresql"


class _TransactionOwnershipConnection:
    dialect = _TransactionOwnershipDialect()


class _ExternalTransactionBind:
    def in_transaction(self):
        return True


class _TransactionOwnershipSession:
    def __init__(self, *, session_preexisting: bool, external_preexisting: bool):
        self.session_preexisting = session_preexisting
        self.bind = _ExternalTransactionBind() if external_preexisting else object()

    def in_transaction(self):
        return self.session_preexisting

    def get_bind(self):
        return self.bind

    def connection(self):
        return _TransactionOwnershipConnection()


@pytest.mark.parametrize(
    ("session_preexisting", "external_preexisting", "expected_owned"),
    [
        (False, False, True),
        (True, False, False),
        (False, True, False),
        (True, True, False),
    ],
)
def test_stock_mutation_transaction_ownership_is_dialect_independent(
    session_preexisting, external_preexisting, expected_owned
):
    session = _TransactionOwnershipSession(
        session_preexisting=session_preexisting,
        external_preexisting=external_preexisting,
    )

    assert _begin_stock_mutation_transaction(session) is expected_owned


def test_inventory_rows_are_locked_and_skus_process_in_sorted_order():
    engine = _engine()
    try:
        with _CaptureInventorySelectSession(engine) as session:
            # Insert B first so movement insertion order cannot accidentally match item id order.
            item_b = _item(barcode="DGN-B")
            item_a = _item(barcode="DGN-A")
            session.add_all([item_b, item_a])
            session.commit()
            session.refresh(item_a)
            session.refresh(item_b)

            payload = {
                "id": "order-sorted",
                "financial_status": "paid",
                "line_items": [
                    {"sku": "DGN-B", "quantity": 1, "price": "2.00"},
                    {"sku": "DGN-A", "quantity": 1, "price": "1.00"},
                ],
            }
            assert mark_inventory_sold_from_shopify_order(session, payload) == 2

            movements = session.exec(
                select(InventoryStockMovement).order_by(InventoryStockMovement.id)
            ).all()
            item_by_id = {item_a.id: item_a.barcode, item_b.id: item_b.barcode}
            assert [item_by_id[row.item_id] for row in movements] == ["DGN-A", "DGN-B"]
            assert len(session.inventory_statements) == 2
            for statement in session.inventory_statements:
                compiled = str(statement.compile(dialect=postgresql.dialect()))
                assert "FOR UPDATE" in compiled
    finally:
        engine.dispose()


def test_quantity_change_issue_type_is_not_importable():
    from app import shopify_sync

    issue_type = getattr(shopify_sync, "SHOPIFY_SYNC_ISSUE_ORDER_QUANTITY_CHANGED", None)
    invalid_identity_type = getattr(
        shopify_sync, "SHOPIFY_SYNC_ISSUE_INVALID_ORDER_IDENTITY", None
    )
    importable = getattr(shopify_sync, "SHOPIFY_SYNC_IMPORTABLE_ISSUE_TYPES", frozenset())

    assert issue_type == "order_quantity_changed"
    assert invalid_identity_type == "invalid_order_identity"
    assert shopify_sync.SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU in importable
    assert shopify_sync.SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT in importable
    assert issue_type not in importable
    assert invalid_identity_type not in importable


def _render_sync_issue(issue: ShopifySyncIssue, importable_issue_types: set[str]) -> str:
    template_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "templates"
        / "inventory_shopify_sync.html"
    )
    template_source = template_path.read_text(encoding="utf-8")
    environment = Environment(
        loader=DictLoader(
            {
                "inventory_shopify_sync.html": template_source,
                "_linear_sidebar.html": "",
                "_csrf_bootstrap.html": "",
            }
        ),
        autoescape=True,
    )
    environment.filters["money"] = lambda value: f"${float(value or 0):.2f}"
    return environment.get_template("inventory_shopify_sync.html").render(
        current_user=None,
        linked_items=[],
        unlinked_items=[],
        recent_jobs=[],
        issues=[issue],
        summary={"linked": 0, "unlinked": 0, "issues": 1, "errors": 0},
        effective_price=lambda _item: 0,
        importable_issue_types=importable_issue_types,
    )


def test_sync_issue_template_render_is_independent_of_working_directory(
    tmp_path, monkeypatch
):
    issue = ShopifySyncIssue(
        id=1,
        issue_key="unknown_sku:order-1:DGN-MISSING",
        issue_type="unknown_sku",
        message="Unknown SKU.",
    )
    monkeypatch.chdir(tmp_path)

    html = _render_sync_issue(issue, {"unknown_sku"})

    assert 'action="/inventory/shopify-sync/import"' in html


def test_quantity_change_issue_hides_import_action_in_rendered_ui():
    changed = ShopifySyncIssue(
        id=1,
        issue_key="order_quantity_changed:order-1:1",
        issue_type="order_quantity_changed",
        message="Quantity changed.",
    )
    importable = {"unknown_sku", "unlinked_product"}

    changed_html = _render_sync_issue(changed, importable)
    assert 'action="/inventory/shopify-sync/import"' not in changed_html

    unknown = ShopifySyncIssue(
        id=2,
        issue_key="unknown_sku:order-1:DGN-MISSING",
        issue_type="unknown_sku",
        message="Unknown SKU.",
    )
    unknown_html = _render_sync_issue(unknown, importable)
    assert 'action="/inventory/shopify-sync/import"' in unknown_html


def test_server_rejects_import_for_quantity_change_issue():
    async def exercise_route():
        from app.inventory.routes import inventory_shopify_sync_import

        engine = _engine()
        try:
            with Session(engine) as session:
                issue = ShopifySyncIssue(
                    issue_key="order_quantity_changed:order-1:1",
                    issue_type="order_quantity_changed",
                    message="Quantity changed.",
                )
                session.add(issue)
                session.commit()
                session.refresh(issue)
                request = Request(
                    {
                        "type": "http",
                        "method": "POST",
                        "path": "/inventory/shopify-sync/import",
                        "headers": [],
                    }
                )

                with patch("app.inventory.routes._require_inventory_manage", return_value=None):
                    response = await inventory_shopify_sync_import(
                        request,
                        session=session,
                        issue_id=issue.id,
                    )

                assert response.status_code == 409
                assert "cannot be imported" in response.body.decode("utf-8").lower()
                assert _rows(session, InventoryItem) == []
        finally:
            engine.dispose()

    asyncio.run(exercise_route())
