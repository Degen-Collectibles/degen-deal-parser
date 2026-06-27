import unittest
from decimal import Decimal
from datetime import datetime, timezone
import json
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine, select
from sqlalchemy import func, distinct

from app.inventory.shopify_ingest import (
    backfill_shopify_orders,
    extract_order_tax_fields,
    normalize_shopify_line_items,
    order_record_from_payload,
    upsert_shopify_order,
)
from app.models import ShopifyOrder


def _base_payload(**overrides):
    payload = {
        "id": "1001",
        "name": "#1001",
        "created_at": "2024-03-15T10:00:00-05:00",
        "updated_at": "2024-03-15T10:00:00-05:00",
        "total_price": "100.00",
        "subtotal_price": "90.00",
        "total_tax": "10.00",
        "financial_status": "paid",
        "fulfillment_status": "fulfilled",
        "line_items": [],
        "customer": {"first_name": "John", "last_name": "Doe", "email": "john@example.com"},
    }
    payload.update(overrides)
    return payload


class ExtractOrderTaxFieldsTests(unittest.TestCase):
    def test_total_tax_field_used_when_present(self):
        payload = {"total_price": "100.00", "total_tax": "10.00"}
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        self.assertEqual(total_tax, 10.0)
        self.assertEqual(subtotal_ex_tax, 90.0)
        self.assertFalse(missing_tax)

    def test_tax_summed_from_tax_lines_when_total_tax_absent(self):
        payload = {
            "total_price": "100.00",
            "total_tax": None,
            "tax_lines": [
                {"price": "6.00"},
                {"price": "4.00"},
            ],
        }
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        self.assertEqual(total_tax, 10.0)
        self.assertEqual(subtotal_ex_tax, 90.0)
        self.assertFalse(missing_tax)

    def test_missing_tax_flag_set_when_no_tax_data(self):
        payload = {"total_price": "100.00", "total_tax": None, "tax_lines": []}
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        self.assertIsNone(total_tax)
        self.assertIsNone(subtotal_ex_tax)
        self.assertTrue(missing_tax)

    def test_empty_string_total_tax_falls_back_to_tax_lines(self):
        payload = {
            "total_price": "50.00",
            "total_tax": "",
            "tax_lines": [{"price": "5.00"}],
        }
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        self.assertEqual(total_tax, 5.0)
        self.assertFalse(missing_tax)


class OrderRecordFromPayloadTests(unittest.TestCase):
    def test_basic_fields_extracted(self):
        record = order_record_from_payload(_base_payload(), source="webhook")
        self.assertEqual(record["shopify_order_id"], "1001")
        self.assertEqual(record["order_number"], "#1001")
        self.assertEqual(record["financial_status"], "paid")
        self.assertEqual(record["total_price"], 100.0)
        self.assertEqual(record["total_tax"], 10.0)

    def test_customer_name_from_customer_field(self):
        record = order_record_from_payload(_base_payload(), source="webhook")
        self.assertEqual(record["customer_name"], "John Doe")

    def test_customer_name_falls_back_to_billing_address(self):
        payload = _base_payload()
        del payload["customer"]
        payload["billing_address"] = {"first_name": "Jane", "last_name": "Smith"}
        record = order_record_from_payload(payload, source="webhook")
        self.assertEqual(record["customer_name"], "Jane Smith")

    def test_missing_id_raises(self):
        payload = _base_payload()
        del payload["id"]
        with self.assertRaises(ValueError):
            order_record_from_payload(payload, source="webhook")

    def test_malformed_ids_are_rejected_instead_of_stringified(self):
        invalid_ids = [
            True,
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
        ]
        for invalid_id in invalid_ids:
            with self.subTest(invalid_id=repr(invalid_id)):
                with self.assertRaises(ValueError):
                    order_record_from_payload(
                        _base_payload(id=invalid_id), source="webhook"
                    )

    def test_line_item_summary_uses_zero_for_invalid_quantity_and_retains_raw(self):
        payload = _base_payload(
            line_items=[
                {
                    "title": "Invalid quantity item",
                    "sku": "DGN-BADQTY",
                    "quantity": "not-a-quantity",
                }
            ]
        )

        record = order_record_from_payload(payload, source="webhook")

        raw_line_items = json.loads(record["line_items_json"])
        summary = json.loads(record["line_items_summary_json"])
        self.assertEqual(raw_line_items[0]["quantity"], "not-a-quantity")
        self.assertEqual(summary[0]["quantity"], 0)


class NormalizeShopifyLineItemsTests(unittest.TestCase):
    def test_invalid_quantities_are_zero_not_invented_as_one(self):
        invalid_quantities = [
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
            "not-a-number",
        ]
        cases = [("missing", {})] + [
            (repr(quantity), {"quantity": quantity})
            for quantity in invalid_quantities
        ]
        for label, quantity_field in cases:
            with self.subTest(quantity=label):
                normalized = normalize_shopify_line_items(
                    [{"title": "Test item", **quantity_field}]
                )
                self.assertEqual(normalized[0]["quantity"], 0)

    def test_integral_quantities_remain_exact_including_explicit_zero(self):
        valid_quantities = [
            (0, 0),
            (0.0, 0),
            (Decimal("0"), 0),
            ("0", 0),
            (2, 2),
            (2.0, 2),
            (Decimal("2"), 2),
            ("2", 2),
        ]
        for raw_quantity, expected in valid_quantities:
            with self.subTest(quantity=repr(raw_quantity)):
                normalized = normalize_shopify_line_items(
                    [{"title": "Test item", "quantity": raw_quantity}]
                )
                self.assertEqual(normalized[0]["quantity"], expected)


class UpsertShopifyOrderTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_inserts_new_order(self):
        with Session(self.engine) as session:
            action = upsert_shopify_order(session, _base_payload(), source="webhook")
            session.commit()

        self.assertEqual(action, "inserted")
        with Session(self.engine) as session:
            rows = session.exec(select(ShopifyOrder)).all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].shopify_order_id, "1001")

    def test_updates_existing_order(self):
        with Session(self.engine) as session:
            upsert_shopify_order(session, _base_payload(), source="webhook")
            session.commit()

        updated_payload = _base_payload(financial_status="refunded")
        with Session(self.engine) as session:
            action = upsert_shopify_order(session, updated_payload, source="webhook")
            session.commit()

        self.assertEqual(action, "updated")
        with Session(self.engine) as session:
            row = session.exec(select(ShopifyOrder)).first()
        self.assertEqual(row.financial_status, "refunded")

    def test_dry_run_does_not_persist(self):
        with Session(self.engine) as session:
            upsert_shopify_order(session, _base_payload(), source="webhook", dry_run=True)
            session.commit()

        with Session(self.engine) as session:
            rows = session.exec(select(ShopifyOrder)).all()
        self.assertEqual(len(rows), 0)

    def test_malformed_identity_upsert_rejects_without_persisting(self):
        invalid_ids = [True, 0, -1, 1.5, float("nan"), [], {}]
        with Session(self.engine) as session:
            for invalid_id in invalid_ids:
                with self.subTest(invalid_id=repr(invalid_id)):
                    with self.assertRaises(ValueError):
                        upsert_shopify_order(
                            session,
                            _base_payload(id=invalid_id),
                            source="webhook",
                        )
                    session.rollback()

        with Session(self.engine) as session:
            rows = session.exec(select(ShopifyOrder)).all()
        self.assertEqual(rows, [])

    def test_backfill_counts_malformed_identity_as_failed_without_persisting(self):
        malformed = _base_payload(id={"not": "an-order-id"})
        with Session(self.engine) as session:
            with patch(
                "app.inventory.shopify_ingest.fetch_shopify_orders_page",
                return_value=([malformed], None),
            ):
                summary = backfill_shopify_orders(
                    session,
                    store_domain="example.myshopify.com",
                    api_key="test-token",
                )

        self.assertEqual(summary.fetched, 1)
        self.assertEqual(summary.failed, 1)
        self.assertEqual(summary.inserted, 0)
        self.assertEqual(summary.updated, 0)
        with Session(self.engine) as session:
            rows = session.exec(select(ShopifyOrder)).all()
        self.assertEqual(rows, [])

    def test_distinct_financial_statuses(self):
        payloads = [
            _base_payload(id="2001", name="#2001", financial_status="paid"),
            _base_payload(id="2002", name="#2002", financial_status="paid"),
            _base_payload(id="2003", name="#2003", financial_status="refunded"),
            _base_payload(id="2004", name="#2004", financial_status="pending"),
            _base_payload(id="2005", name="#2005", financial_status="pending"),
        ]
        with Session(self.engine) as session:
            for p in payloads:
                upsert_shopify_order(session, p, source="webhook")
            session.commit()

        with Session(self.engine) as session:
            statuses = session.exec(select(distinct(ShopifyOrder.financial_status))).all()
        self.assertEqual(len(statuses), 3)
        self.assertIn("paid", statuses)
        self.assertIn("refunded", statuses)
        self.assertIn("pending", statuses)


if __name__ == "__main__":
    unittest.main()
