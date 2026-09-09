import asyncio
from copy import deepcopy
from contextlib import contextmanager, redirect_stdout
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import importlib
from io import StringIO
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from sqlalchemy.exc import OperationalError
from sqlmodel import SQLModel, Session, create_engine, select

from app.config import Settings
from app.models import AppSetting, ShopifyOrder, ShopifySyncIssue, utcnow
from app import main as main_module
from app import shopify_tax_sentinel as tax_sentinel
from app import shopify_sync
from app.inventory import shopify as shopify_inventory
from app.shopify_tax_sentinel import (
    MAX_ORDER_LOOKBACK_DAYS,
    TaxFinding,
    OFFICIAL_CHECK_AT_KEY,
    OFFICIAL_EFFECTIVE_KEY,
    OFFICIAL_RATE_KEY,
    _decimal,
    _taxable_lines,
    evaluate_shopify_order,
    parse_order_payload,
    run_shopify_tax_sentinel_once,
)


CDTFA_HTML = """
<!doctype html>
<html>
  <body>
    <h1>California City &amp; County Sales &amp; Use Tax Rates (effective April 1, 2026)</h1>
    <table>
      <thead>
        <tr>
          <th>Location</th>
          <th>Rate</th>
          <th>County</th>
          <th>Type</th>
          <th>Notes</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>San Jose</td>
          <td><strong>10.000%</strong></td>
          <td>Santa Clara</td>
          <td>City</td>
          <td></td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def paid_pos_order(**overrides):
    tax_line = {"title": "San Jose tax", "rate": 0.10, "price": "10.00"}
    payload = {
        "id": 12345,
        "name": "#1001",
        "financial_status": "paid",
        "source_name": "pos",
        "location_id": 555,
        "total_tax": "10.00",
        "tax_lines": [deepcopy(tax_line)],
        "line_items": [
            {
                "id": 67890,
                "title": "Taxable card",
                "taxable": True,
                "tax_lines": [deepcopy(tax_line)],
            }
        ],
    }
    payload.update(overrides)
    return payload


class ShopifyPosOrderEvaluationTests(unittest.TestCase):
    def evaluate(self, payload, **overrides):
        options = {
            "expected_location_id": "555",
            "expected_rate": Decimal("0.10"),
            "pos_only": True,
        }
        options.update(overrides)
        return evaluate_shopify_order(payload, **options)

    def test_expected_paid_pos_order_has_no_findings(self):
        self.assertEqual(self.evaluate(paid_pos_order()), [])

    def test_paid_web_order_is_one_critical_non_pos_finding(self):
        tax_lines = [
            {
                "title": "San Jose tax",
                "rate": 0.10,
                "price": "10.00",
                "customer_email": "customer@example.com",
                "metadata": {"card_number": "sensitive"},
                "channel_liable": {"email": "nested@example.com"},
            }
        ]
        payload = paid_pos_order(
            source_name="web",
            location_id=999,
            tax_lines=tax_lines,
            customer={"email": "customer@example.com"},
        )

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding.issue_type, "non_pos_order_detected")
        self.assertEqual(finding.severity, "critical")
        self.assertEqual(finding.order_id, "12345")
        self.assertEqual(finding.order_number, "#1001")
        self.assertEqual(finding.location_id, "999")
        self.assertEqual(
            finding.payload,
            {
                "source_name": "web",
                "location_id": "999",
                "total_tax": "10.00",
                "tax_lines": [
                    {
                        "title": "San Jose tax",
                        "rate": 0.10,
                        "price": "10.00",
                    }
                ],
            },
        )

    def test_pos_location_mismatch_is_critical(self):
        findings = self.evaluate(paid_pos_order(location_id="777"))

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].issue_type, "pos_location_mismatch")
        self.assertEqual(findings[0].severity, "critical")
        self.assertEqual(findings[0].location_id, "777")

    def test_zero_tax_without_tax_lines_is_override_warning(self):
        payload = paid_pos_order(
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 67890,
                    "taxable": True,
                    "tax_lines": [],
                }
            ],
        )

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].issue_type, "pos_tax_override_observed")
        self.assertEqual(findings[0].severity, "warning")

    def test_observed_825_percent_rate_is_critical_mismatch_with_evidence(self):
        tax_lines = [{"title": "San Jose tax", "rate": "0.0825", "price": "8.25"}]
        payload = paid_pos_order(total_tax="8.25", tax_lines=tax_lines)

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding.issue_type, "pos_tax_rate_mismatch")
        self.assertEqual(finding.severity, "critical")
        self.assertEqual(
            finding.payload,
            {
                "source_name": "pos",
                "location_id": "555",
                "total_tax": "8.25",
                "tax_lines": tax_lines,
                "observed_rate": "0.0825",
                "expected_rate": "0.10",
            },
        )

    def test_positive_tax_without_order_tax_lines_is_missing_lines_warning(self):
        findings = self.evaluate(paid_pos_order(total_tax="10.00", tax_lines=[]))

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].issue_type, "pos_tax_lines_missing")
        self.assertEqual(findings[0].severity, "warning")

    def test_non_paid_pos_orders_are_skipped(self):
        for status in ["refunded", "pending", "voided", None]:
            with self.subTest(status=status):
                payload = paid_pos_order(
                    financial_status=status,
                    location_id="wrong",
                    total_tax="malformed-but-unread",
                )
                self.assertEqual(self.evaluate(payload), [])

    def test_non_paid_pos_orders_skip_every_other_field_validation(self):
        payloads = [
            {"financial_status": "refunded", "source_name": "pos"},
            {
                "financial_status": "pending",
                "source_name": "pos",
                "id": {"customer_email": "private@example.com"},
                "location_id": {"customer_email": "private@example.com"},
                "tax_lines": {},
                "line_items": [None],
                "total_tax": "not-a-number",
            },
            {
                "financial_status": {"customer_email": "private@example.com"},
                "source_name": "pos",
                "id": None,
            },
        ]

        for payload in payloads:
            with self.subTest(payload=payload):
                self.assertEqual(self.evaluate(payload), [])

    def test_non_paid_non_pos_orders_are_always_one_critical_boundary_finding(self):
        for status in ["pending", "refunded", "voided"]:
            with self.subTest(status=status):
                payload = paid_pos_order(
                    financial_status=status,
                    source_name="web",
                    location_id=999,
                    total_tax={"customer_email": "private@example.com"},
                    tax_lines={"customer_email": "private@example.com"},
                    line_items=[{"customer_email": "private@example.com"}],
                )

                findings = self.evaluate(payload)

                self.assertEqual(len(findings), 1)
                self.assertEqual(findings[0].issue_type, "non_pos_order_detected")
                self.assertEqual(findings[0].severity, "critical")
                self.assertIn("#1001", findings[0].message)
                self.assertNotIn("private@example.com", repr(findings[0]))

    def test_paid_non_pos_boundary_precedes_pos_container_validation(self):
        payload = paid_pos_order(
            source_name="web",
            tax_lines={},
            location_id={"customer_email": "private@example.com"},
        )

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding.issue_type, "non_pos_order_detected")
        self.assertEqual(finding.location_id, "")
        self.assertEqual(finding.payload["location_id"], "")
        self.assertNotIn("private@example.com", repr(finding))

    def test_non_pos_order_is_skipped_when_pos_only_is_disabled(self):
        payload = paid_pos_order(
            source_name="web",
            location_id="wrong-location",
            total_tax="malformed-but-unread",
            tax_lines=[{"rate": "malformed-but-unread"}],
        )

        self.assertEqual(self.evaluate(payload, pos_only=False), [])

    def test_paid_non_pos_skip_precedes_line_item_validation(self):
        payload = paid_pos_order(
            source_name="web",
            line_items=[None],
            location_id={"customer_email": "private@example.com"},
        )

        self.assertEqual(self.evaluate(payload, pos_only=False), [])

    def test_string_and_integer_location_ids_match(self):
        cases = [(555, "555"), ("555", 555)]

        for payload_location, expected_location in cases:
            with self.subTest(
                payload_location=payload_location,
                expected_location=expected_location,
            ):
                self.assertEqual(
                    self.evaluate(
                        paid_pos_order(location_id=payload_location),
                        expected_location_id=expected_location,
                    ),
                    [],
                )

    def test_multiple_order_tax_line_rates_can_sum_to_expected_rate(self):
        tax_lines = [
            {"title": "State tax", "rate": "0.0600", "price": "6.00"},
            {"title": "Local tax", "rate": Decimal("0.0400"), "price": "4.00"},
        ]

        self.assertEqual(
            self.evaluate(paid_pos_order(tax_lines=tax_lines)),
            [],
        )

    def test_parse_order_payload_rejects_malformed_and_non_object_json(self):
        for raw_payload in [
            "{",
            "[]",
            "null",
            '"order"',
            "123",
            '{"rate": NaN}',
            '{"rate": Infinity}',
            '{"rate": -Infinity}',
        ]:
            with self.subTest(raw_payload=raw_payload):
                with self.assertRaises(ValueError) as raised:
                    parse_order_payload(raw_payload)

                self.assertEqual(
                    str(raised.exception),
                    "Shopify order raw payload is not a JSON object",
                )

    def test_parse_order_payload_returns_json_object(self):
        self.assertEqual(
            parse_order_payload('{"id": 12345, "financial_status": "paid"}'),
            {"id": 12345, "financial_status": "paid"},
        )

    def test_decimal_coercion_preserves_numeric_values(self):
        cases = [
            ("0.10", Decimal("0.10")),
            (Decimal("0.0825"), Decimal("0.0825")),
            (10, Decimal("10")),
            (0.1, Decimal("0.1")),
        ]

        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(_decimal(value), expected)

    def test_decimal_coercion_rejects_malformed_and_nonfinite_values(self):
        for value in [
            None,
            "",
            "   ",
            "not-a-rate",
            "NaN",
            "Infinity",
            float("-inf"),
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as raised:
                    _decimal(value)

                self.assertEqual(str(raised.exception), "Invalid decimal evidence")

    def test_decimal_coercion_rejects_unsupported_types_without_stringifying(self):
        class ValueThatMustNotBeStringified:
            def __str__(self):
                raise AssertionError("unsupported values must not be stringified")

        for value in [
            True,
            {},
            [],
            (),
            set(),
            object(),
            ValueThatMustNotBeStringified(),
        ]:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(ValueError) as raised:
                    _decimal(value)

                self.assertEqual(str(raised.exception), "Invalid decimal evidence")

    def test_nested_total_tax_pii_is_absent_from_decimal_error(self):
        customer_email = "decimal-total-tax-customer@example.com"
        total_tax = {"customer": {"contacts": [customer_email]}}

        with self.assertRaises(ValueError) as raised:
            self.evaluate(paid_pos_order(total_tax=total_tax))

        self.assertEqual(str(raised.exception), "Invalid decimal evidence")
        self.assertNotIn(customer_email, str(raised.exception))
        self.assertNotIn(repr(total_tax), str(raised.exception))

    def test_nested_tax_line_rate_pii_is_absent_from_decimal_error(self):
        customer_email = "decimal-tax-rate-customer@example.com"
        rate = ["unexpected", {"customer_email": customer_email}]
        payload = paid_pos_order(
            tax_lines=[{"title": "Invalid tax", "rate": rate, "price": "10.00"}]
        )

        with self.assertRaises(ValueError) as raised:
            self.evaluate(payload)

        self.assertEqual(str(raised.exception), "Invalid decimal evidence")
        self.assertNotIn(customer_email, str(raised.exception))
        self.assertNotIn(repr(rate), str(raised.exception))

    def test_malformed_tax_line_rates_fail_visible(self):
        for rate in [None, "not-a-rate", "NaN", "Infinity"]:
            with self.subTest(rate=rate):
                payload = paid_pos_order(
                    tax_lines=[{"title": "Invalid tax", "rate": rate, "price": "10.00"}]
                )
                with self.assertRaises(ValueError):
                    self.evaluate(payload)

    def test_missing_null_and_empty_line_items_cannot_pass_clean(self):
        for line_items_marker in ["missing", None, []]:
            with self.subTest(line_items=line_items_marker):
                payload = paid_pos_order()
                if line_items_marker == "missing":
                    payload.pop("line_items")
                else:
                    payload["line_items"] = line_items_marker

                with self.assertRaises(ValueError):
                    self.evaluate(payload)

    def test_taxable_line_missing_null_or_malformed_tax_lines_fails_visible(self):
        malformed_values = ["missing", None, {}, False, [None]]
        for tax_lines_marker in malformed_values:
            with self.subTest(tax_lines=tax_lines_marker):
                line_item = {"id": 1, "taxable": True}
                if tax_lines_marker != "missing":
                    line_item["tax_lines"] = tax_lines_marker
                payload = paid_pos_order(line_items=[line_item])

                with self.assertRaises(ValueError):
                    self.evaluate(payload)

    def test_empty_tax_lines_on_one_of_multiple_taxable_lines_is_one_override(self):
        expected_tax_line = {
            "title": "San Jose tax",
            "rate": "0.10",
            "price": "10.00",
        }
        payload = paid_pos_order(
            line_items=[
                {"id": 1, "taxable": True, "tax_lines": []},
                {
                    "id": 2,
                    "taxable": True,
                    "tax_lines": [expected_tax_line],
                },
            ]
        )

        findings = self.evaluate(payload)

        override_findings = [
            finding
            for finding in findings
            if finding.issue_type == "pos_tax_override_observed"
        ]
        self.assertEqual(len(override_findings), 1)

    def test_explicit_empty_tax_lines_on_all_taxable_lines_is_one_override(self):
        payload = paid_pos_order(
            line_items=[
                {"id": 1, "taxable": True, "tax_lines": []},
                {"id": 2, "taxable": True, "tax_lines": []},
            ]
        )

        findings = self.evaluate(payload)

        self.assertEqual(
            [
                finding.issue_type
                for finding in findings
                if finding.issue_type == "pos_tax_override_observed"
            ],
            ["pos_tax_override_observed"],
        )

    def test_non_taxable_physical_line_is_one_override(self):
        payload = paid_pos_order(
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": False,
                }
            ],
        )

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].issue_type, "pos_tax_override_observed")
        self.assertEqual(findings[0].payload["line_override_indexes"], [0])

    def test_multiple_non_taxable_physical_lines_consolidate_one_override(self):
        payload = paid_pos_order(
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": False,
                },
                {
                    "id": 2,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": False,
                },
            ],
        )

        findings = self.evaluate(payload)

        override_findings = [
            finding
            for finding in findings
            if finding.issue_type == "pos_tax_override_observed"
        ]
        self.assertEqual(len(override_findings), 1)
        self.assertEqual(
            override_findings[0].payload["line_override_indexes"],
            [0, 1],
        )

    def test_non_taxable_gift_card_is_legitimate_exclusion(self):
        payload = paid_pos_order(
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": True,
                }
            ],
        )

        self.assertEqual(self.evaluate(payload), [])

    def test_non_taxable_nonphysical_line_is_legitimate_exclusion(self):
        payload = paid_pos_order(
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": False,
                    "gift_card": False,
                }
            ],
        )

        self.assertEqual(self.evaluate(payload), [])

    def test_non_taxable_lines_require_literal_classification_booleans(self):
        private_value = {"customer_email": "private@example.com"}
        invalid_cases = [
            ("requires_shipping", "missing"),
            ("requires_shipping", None),
            ("requires_shipping", "true"),
            ("requires_shipping", 1),
            ("requires_shipping", private_value),
            ("gift_card", "missing"),
            ("gift_card", None),
            ("gift_card", "false"),
            ("gift_card", 0),
            ("gift_card", private_value),
        ]
        for field, invalid_value in invalid_cases:
            with self.subTest(field=field, invalid_value=invalid_value):
                line_item = {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": False,
                }
                if invalid_value == "missing":
                    line_item.pop(field)
                else:
                    line_item[field] = invalid_value
                payload = paid_pos_order(
                    total_tax="0",
                    tax_lines=[],
                    line_items=[line_item],
                )

                with self.assertRaises(ValueError) as raised:
                    self.evaluate(payload)

                self.assertNotIn("private@example.com", str(raised.exception))
                self.assertNotIn(repr(private_value), str(raised.exception))

    def test_mixed_line_override_evidence_uses_original_line_indexes(self):
        expected_tax_line = {
            "title": "San Jose tax",
            "rate": "0.10",
            "price": "10.00",
        }
        payload = paid_pos_order(
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "requires_shipping": False,
                    "gift_card": False,
                },
                {
                    "id": 2,
                    "taxable": True,
                    "tax_lines": [expected_tax_line],
                },
                {
                    "id": 3,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": False,
                    "title": "must not be persisted",
                    "customer": {"email": "private@example.com"},
                },
            ]
        )

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding.issue_type, "pos_tax_override_observed")
        self.assertEqual(finding.payload["line_override_indexes"], [2])
        self.assertNotIn("must not be persisted", repr(finding))
        self.assertNotIn("private@example.com", repr(finding))

    def test_line_level_rate_mismatch_is_detected_when_order_rate_is_expected(self):
        payload = paid_pos_order(
            tax_lines=[
                {"title": "San Jose tax", "rate": "0.10", "price": "10.00"}
            ],
            line_items=[
                {
                    "id": 1,
                    "taxable": True,
                    "tax_lines": [
                        {
                            "title": "Wrong line rate",
                            "rate": "0.0825",
                            "price": "8.25",
                        }
                    ],
                }
            ],
        )

        findings = self.evaluate(payload)

        rate_findings = [
            finding
            for finding in findings
            if finding.issue_type == "pos_tax_rate_mismatch"
        ]
        self.assertEqual(len(rate_findings), 1)
        self.assertIn("0.0825", rate_findings[0].message)
        self.assertEqual(rate_findings[0].payload["line_observed_rates"], ["0.0825"])

    def test_line_tax_rates_reject_malformed_nonfinite_and_out_of_bounds(self):
        invalid_rates = [None, "not-a-rate", "NaN", "Infinity", "-0.01", "1.01"]
        for invalid_rate in invalid_rates:
            with self.subTest(invalid_rate=invalid_rate):
                payload = paid_pos_order(
                    line_items=[
                        {
                            "id": 1,
                            "taxable": True,
                            "tax_lines": [
                                {
                                    "title": "Invalid line rate",
                                    "rate": invalid_rate,
                                    "price": "10.00",
                                }
                            ],
                        }
                    ]
                )

                with self.assertRaises(ValueError):
                    self.evaluate(payload)

    def test_order_and_line_rate_mismatches_are_one_consolidated_finding(self):
        wrong_tax_line = {
            "title": "Wrong tax",
            "rate": "0.0825",
            "price": "8.25",
        }
        payload = paid_pos_order(
            total_tax="8.25",
            tax_lines=[wrong_tax_line],
            line_items=[
                {
                    "id": 1,
                    "taxable": True,
                    "tax_lines": [wrong_tax_line],
                }
            ],
        )

        findings = self.evaluate(payload)

        self.assertEqual(
            [
                finding.issue_type
                for finding in findings
                if finding.issue_type == "pos_tax_rate_mismatch"
            ],
            ["pos_tax_rate_mismatch"],
        )

    def test_wrong_location_returns_only_boundary_before_tax_evidence_checks(self):
        payloads = [
            paid_pos_order(
                location_id="777",
                total_tax="10.75",
                tax_lines=[
                    {"title": "Other city tax", "rate": "0.1075", "price": "10.75"}
                ],
                line_items=[
                    {
                        "id": 1,
                        "taxable": True,
                        "tax_lines": [
                            {
                                "title": "Other city tax",
                                "rate": "0.1075",
                                "price": "10.75",
                            }
                        ],
                    }
                ],
            ),
            paid_pos_order(
                location_id="777",
                total_tax={"customer_email": "private@example.com"},
                tax_lines={"customer_email": "private@example.com"},
                line_items=[None],
            ),
            paid_pos_order(
                location_id="777",
                total_tax="scalar-private@example.com",
                tax_lines=[],
                line_items=[],
            ),
        ]

        for payload in payloads:
            with self.subTest(payload=payload):
                findings = self.evaluate(payload)

                self.assertEqual(len(findings), 1)
                self.assertEqual(findings[0].issue_type, "pos_location_mismatch")
                self.assertNotIn("private@example.com", repr(findings[0]))

    def test_line_collections_reject_falsey_non_list_values(self):
        for field in ["tax_lines", "line_items"]:
            for invalid_value in [{}, False, 0, ""]:
                with self.subTest(field=field, invalid_value=invalid_value):
                    payload = paid_pos_order()
                    payload[field] = invalid_value
                    with self.assertRaises(ValueError):
                        self.evaluate(payload)

    def test_line_collections_reject_non_object_entries(self):
        cases = [
            ("tax_lines", [None]),
            ("tax_lines", ["tax line"]),
            ("line_items", [None]),
            ("line_items", [123]),
        ]

        for field, invalid_value in cases:
            with self.subTest(field=field, invalid_value=invalid_value):
                payload = paid_pos_order()
                payload[field] = invalid_value
                with self.assertRaises(ValueError):
                    self.evaluate(payload)

    def test_taxable_lines_require_boolean_taxable_field(self):
        invalid_lines = [
            {"id": 1},
            {"id": 2, "taxable": "true"},
            {"id": 3, "taxable": 1},
            {"id": 4, "taxable": None},
        ]

        for invalid_line in invalid_lines:
            with self.subTest(invalid_line=invalid_line):
                with self.assertRaises(ValueError):
                    _taxable_lines({"line_items": [invalid_line]})

    def test_taxable_lines_return_only_literal_true_rows(self):
        literal_true = {"id": 1, "taxable": True}
        payload = {
            "line_items": [
                literal_true,
                {"id": 2, "taxable": False},
            ]
        }

        self.assertEqual(_taxable_lines(payload), [literal_true])

    def test_order_tax_line_requires_rate_for_rate_evaluation(self):
        payload = paid_pos_order(
            tax_lines=[{"title": "Missing rate", "price": "10.00"}]
        )

        with self.assertRaises(ValueError):
            self.evaluate(payload)

    def test_total_tax_rejects_negative_values(self):
        with self.assertRaises(ValueError):
            self.evaluate(paid_pos_order(total_tax="-0.01"))

    def test_component_rate_rejects_negative_offset_that_sums_to_expected(self):
        tax_lines = [
            {"title": "Invalid negative", "rate": "-0.10", "price": "-10.00"},
            {"title": "Invalid positive", "rate": "0.20", "price": "20.00"},
        ]

        with self.assertRaises(ValueError):
            self.evaluate(paid_pos_order(tax_lines=tax_lines))

    def test_component_rate_rejects_values_above_one(self):
        tax_lines = [{"title": "Invalid rate", "rate": "1.01", "price": "101.00"}]

        with self.assertRaises(ValueError):
            self.evaluate(paid_pos_order(tax_lines=tax_lines))

    def test_rate_mismatch_evidence_projects_allowlisted_scalar_fields(self):
        tax_lines = [
            {
                "title": "San Jose tax",
                "rate": "0.0825",
                "price": "8.25",
                "channel_liable": True,
                "customer_email": "customer@example.com",
                "nested_sensitive": {"address": "private"},
            },
            {
                "title": {"customer_name": "private"},
                "rate": "0",
                "price": ["private"],
            },
        ]

        findings = self.evaluate(paid_pos_order(total_tax="8.25", tax_lines=tax_lines))

        self.assertEqual(len(findings), 1)
        self.assertEqual(
            findings[0].payload["tax_lines"],
            [
                {
                    "title": "San Jose tax",
                    "rate": "0.0825",
                    "price": "8.25",
                    "channel_liable": True,
                },
                {"rate": "0"},
            ],
        )

    def test_order_id_is_required_and_must_be_non_empty_immutable_scalar(self):
        for invalid_id in [None, "", "   ", [], {}, True, 1.5]:
            with self.subTest(invalid_id=invalid_id):
                payload = paid_pos_order(id=invalid_id)
                with self.assertRaises(ValueError):
                    self.evaluate(payload)

        payload = paid_pos_order()
        payload.pop("id")
        with self.assertRaises(ValueError):
            self.evaluate(payload)

    def test_order_number_falls_back_without_rendering_none(self):
        cases = [
            (None, 9876, "9876"),
            ("   ", None, "12345"),
            (None, None, "12345"),
        ]

        for name, order_number, expected in cases:
            with self.subTest(name=name, order_number=order_number):
                payload = paid_pos_order(
                    name=name,
                    order_number=order_number,
                    source_name="web",
                )
                finding = self.evaluate(payload)[0]
                self.assertEqual(finding.order_number, expected)

    def test_order_number_ignores_nested_or_boolean_identity_values(self):
        payload = paid_pos_order(
            name={"customer_email": "name@example.com"},
            order_number={"customer_email": "number@example.com"},
            source_name="web",
        )

        finding = self.evaluate(payload)[0]

        self.assertEqual(finding.order_number, "12345")
        self.assertNotIn("name@example.com", repr(finding))
        self.assertNotIn("number@example.com", repr(finding))

        payload = paid_pos_order(name=True, order_number=9876, source_name="web")
        self.assertEqual(self.evaluate(payload)[0].order_number, "9876")

    def test_invalid_or_blank_pos_locations_are_redacted_boundary_findings(self):
        invalid_locations = [
            {"customer_email": "private@example.com"},
            ["private@example.com"],
            True,
            1.5,
            "",
            "   ",
        ]

        for location_id in invalid_locations:
            with self.subTest(location_id=location_id):
                findings = self.evaluate(
                    paid_pos_order(
                        location_id=location_id,
                        tax_lines={"customer_email": "private@example.com"},
                    )
                )
                self.assertEqual(len(findings), 1)
                self.assertEqual(findings[0].issue_type, "pos_location_mismatch")
                self.assertEqual(findings[0].location_id, "")
                self.assertNotIn("private@example.com", repr(findings[0]))

    def test_missing_pos_location_remains_a_redacted_mismatch(self):
        payload = paid_pos_order()
        payload.pop("location_id")

        findings = self.evaluate(payload)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].issue_type, "pos_location_mismatch")
        self.assertEqual(findings[0].location_id, "")
        self.assertEqual(findings[0].payload["location_id"], "")

    def test_tax_finding_is_frozen(self):
        finding = TaxFinding(
            issue_type="pos_location_mismatch",
            severity="critical",
            message="Wrong location.",
            order_id="12345",
            order_number="#1001",
            location_id="777",
            payload={},
        )

        with self.assertRaises(AttributeError):
            finding.severity = "warning"


def _tax_sources():
    return importlib.import_module("app.shopify_tax_sources")


def _variant_node(
    variant_id,
    *,
    taxable=False,
    requires_shipping=True,
    status="ACTIVE",
    is_gift_card=False,
):
    return {
        "id": f"gid://shopify/ProductVariant/{variant_id}",
        "title": f"Variant {variant_id}",
        "sku": f"SKU-{variant_id}",
        "taxable": taxable,
        "inventoryItem": {"requiresShipping": requires_shipping},
        "product": {
            "id": f"gid://shopify/Product/{variant_id}",
            "title": f"Product {variant_id}",
            "status": status,
            "isGiftCard": is_gift_card,
        },
    }


def _variant_page(nodes, *, has_next_page=False, end_cursor=None):
    return {
        "data": {
            "productVariants": {
                "pageInfo": {
                    "hasNextPage": has_next_page,
                    "endCursor": end_cursor,
                },
                "nodes": nodes,
            }
        }
    }


class ShopifyTaxIssueLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite:///:memory:", connect_args={"check_same_thread": False}
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_tax_sentinel_issue_types_are_complete(self):
        from app.shopify_sync import (
            SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
            SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
            SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
            SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
            SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
            SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
            SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
            SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
        )

        issue_types = frozenset(
            {
                SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
                SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
                SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
                SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
                SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
                SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
                SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
            }
        )

        self.assertEqual(
            issue_types,
            frozenset(
                {
                    "taxable_variant_disabled",
                    "pos_tax_rate_mismatch",
                    "pos_tax_lines_missing",
                    "pos_location_mismatch",
                    "pos_tax_override_observed",
                    "non_pos_order_detected",
                    "official_tax_rate_changed",
                    "official_tax_source_unavailable",
                }
            ),
        )
        self.assertEqual(shopify_sync.SHOPIFY_TAX_SENTINEL_ISSUE_TYPES, issue_types)

    def test_successful_check_resolves_only_unobserved_open_issues_of_one_type(self):
        issue_type = "taxable_variant_disabled"
        with Session(self.engine) as session:
            observed = shopify_sync.record_shopify_sync_issue(
                session,
                issue_type=issue_type,
                message="Still disabled.",
                shopify_product_id="product-observed",
                shopify_variant_id="variant-observed",
            )
            stale = shopify_sync.record_shopify_sync_issue(
                session,
                issue_type=issue_type,
                message="Previously disabled.",
                shopify_product_id="product-stale",
                shopify_variant_id="variant-stale",
            )
            other_type = shopify_sync.record_shopify_sync_issue(
                session,
                issue_type="pos_tax_rate_mismatch",
                message="Different check.",
                shopify_product_id="product-other",
                shopify_variant_id="variant-other",
            )
            already_resolved = shopify_sync.record_shopify_sync_issue(
                session,
                issue_type=issue_type,
                message="Already handled.",
                shopify_product_id="product-resolved",
                shopify_variant_id="variant-resolved",
            )
            already_resolved.status = shopify_sync.SHOPIFY_SYNC_ISSUE_RESOLVED
            already_resolved.resolution_note = "Existing resolution"
            already_resolved.resolved_by = "operator"
            already_resolved.resolved_at = utcnow()
            session.add(already_resolved)
            session.commit()
            session.refresh(stale)
            stale_last_seen_at = stale.last_seen_at

            resolved_count = shopify_sync.resolve_unobserved_shopify_sync_issues(
                session,
                issue_type=issue_type,
                observed_issue_keys={observed.issue_key},
                resolution_note="No longer observed by the complete check.",
            )
            session.commit()

            issues = {
                issue.issue_key: issue
                for issue in session.exec(select(ShopifySyncIssue)).all()
            }
            resolved_stale = issues[stale.issue_key]

            self.assertEqual(resolved_count, 1)
            self.assertEqual(
                issues[observed.issue_key].status,
                shopify_sync.SHOPIFY_SYNC_ISSUE_OPEN,
            )
            self.assertEqual(
                resolved_stale.status,
                shopify_sync.SHOPIFY_SYNC_ISSUE_RESOLVED,
            )
            self.assertEqual(
                resolved_stale.resolution_note,
                "No longer observed by the complete check.",
            )
            self.assertEqual(
                resolved_stale.resolved_by,
                "shopify_pos_tax_sentinel",
            )
            self.assertIsNotNone(resolved_stale.resolved_at)
            self.assertGreaterEqual(resolved_stale.resolved_at, stale_last_seen_at)
            self.assertEqual(resolved_stale.last_seen_at, resolved_stale.resolved_at)
            self.assertEqual(
                issues[other_type.issue_key].status,
                shopify_sync.SHOPIFY_SYNC_ISSUE_OPEN,
            )
            self.assertEqual(
                issues[already_resolved.issue_key].resolution_note,
                "Existing resolution",
            )


class OfficialTaxRateSourceTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_cdtfa_city_rate_uses_structured_table_cells(self):
        tax_sources = _tax_sources()

        result = tax_sources.parse_cdtfa_city_rate_html(
            CDTFA_HTML,
            city="San Jose",
            county="Santa Clara",
        )

        self.assertEqual(result.city, "San Jose")
        self.assertEqual(result.county, "Santa Clara")
        self.assertEqual(result.rate, Decimal("0.10000"))
        self.assertEqual(result.effective_label, "April 1, 2026")

    def test_parse_cdtfa_city_rate_rejects_missing_or_mismatched_rows(self):
        tax_sources = _tax_sources()
        cases = [
            (CDTFA_HTML, "Alameda"),
            (CDTFA_HTML.replace("San Jose", "Milpitas"), "Santa Clara"),
        ]

        for html, county in cases:
            with self.subTest(county=county):
                with self.assertRaisesRegex(ValueError, "San Jose"):
                    tax_sources.parse_cdtfa_city_rate_html(
                        html,
                        city="San Jose",
                        county=county,
                    )

    def test_parse_cdtfa_city_rate_rejects_nonfinite_or_out_of_range_rates(self):
        tax_sources = _tax_sources()

        for invalid_rate in ["NaN%", "Infinity%", "-0.001%", "100.001%"]:
            with self.subTest(rate=invalid_rate):
                html = CDTFA_HTML.replace("10.000%", invalid_rate)
                with self.assertRaisesRegex(ValueError, "San Jose"):
                    tax_sources.parse_cdtfa_city_rate_html(
                        html,
                        city="San Jose",
                        county="Santa Clara",
                    )

    def test_parse_cdtfa_city_rate_rejects_impossible_effective_date(self):
        tax_sources = _tax_sources()
        html = CDTFA_HTML.replace("April 1, 2026", "February 30, 2026")

        with self.assertRaisesRegex(ValueError, "San Jose"):
            tax_sources.parse_cdtfa_city_rate_html(
                html,
                city="San Jose",
                county="Santa Clara",
            )

    async def test_fetch_cdtfa_city_rate_uses_official_source(self):
        tax_sources = _tax_sources()
        client = _FakeAsyncClient([_FakeResponse(text=CDTFA_HTML)])

        result = await tax_sources.fetch_cdtfa_city_rate(
            city="San Jose",
            county="Santa Clara",
            client=client,
        )

        self.assertEqual(result.rate, Decimal("0.10000"))
        self.assertEqual(client.gets[0]["url"], tax_sources.CDTFA_CITY_RATES_URL)


class ShopifyTaxSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_shopify_graphql_request_is_public_and_reuses_admin_transport(self):
        client = _FakeAsyncClient([_FakeResponse(payload={"data": {"shop": {"id": "1"}}})])

        payload = await shopify_inventory.shopify_graphql_request(
            client,
            store_domain="degen-test.myshopify.com",
            access_token="shpat_test",
            query="query { shop { id } }",
            variables={},
        )

        self.assertEqual(payload, {"data": {"shop": {"id": "1"}}})
        self.assertEqual(
            client.posts[0]["url"],
            "https://degen-test.myshopify.com/admin/api/2026-04/graphql.json",
        )
        self.assertEqual(
            client.posts[0]["headers"]["X-Shopify-Access-Token"],
            "shpat_test",
        )

    async def test_get_shopify_locations_returns_complete_dict_rows(self):
        locations = [
            {
                "id": 101,
                "name": "San Jose",
                "active": True,
                "address1": "123 Main St",
                "city": "San Jose",
                "province_code": "CA",
                "zip": "95113",
                "country_code": "US",
            },
            {
                "id": 202,
                "name": "Storage",
                "active": False,
                "city": "Santa Clara",
            },
        ]
        client = _FakeAsyncClient(
            [_FakeResponse(payload={"locations": [locations[0], "bad-row", locations[1]]})]
        )

        result = await shopify_inventory.get_shopify_locations(
            store_domain="degen-test.myshopify.com",
            access_token="shpat_test",
            client=client,
        )

        self.assertEqual(result, locations)
        self.assertEqual(
            client.gets[0]["url"],
            "https://degen-test.myshopify.com/admin/api/2026-04/locations.json",
        )

    async def test_get_shopify_locations_returns_empty_without_configuration(self):
        client = _FakeAsyncClient([])

        result = await shopify_inventory.get_shopify_locations(
            store_domain="",
            access_token="",
            client=client,
        )

        self.assertEqual(result, [])
        self.assertEqual(client.gets, [])

    async def test_primary_location_ignores_empty_location_rows(self):
        client = _FakeAsyncClient(
            [
                _FakeResponse(
                    payload={
                        "locations": [
                            {},
                            {"id": 101, "name": "San Jose", "active": True},
                        ]
                    }
                )
            ]
        )

        result = await shopify_inventory.get_shopify_primary_location_id(
            store_domain="degen-test.myshopify.com",
            access_token="shpat_test",
            client=client,
        )

        self.assertEqual(result, "101")

    async def test_non_taxable_physical_variants_paginate_and_filter(self):
        tax_sources = _tax_sources()
        pages = [
            _variant_page(
                [
                    _variant_node("keep-1"),
                    _variant_node("taxable", taxable=True),
                    _variant_node("digital", requires_shipping=False),
                ],
                has_next_page=True,
                end_cursor="cursor-1",
            ),
            _variant_page(
                [
                    _variant_node("keep-2"),
                    _variant_node("draft", status="DRAFT"),
                    _variant_node("gift", is_gift_card=True),
                ]
            ),
        ]
        request = AsyncMock(side_effect=pages)

        with patch.object(tax_sources, "shopify_graphql_request", new=request):
            result = await tax_sources.fetch_non_taxable_physical_variants(
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=object(),
            )

        self.assertEqual([row.variant_id for row in result], [
            "gid://shopify/ProductVariant/keep-1",
            "gid://shopify/ProductVariant/keep-2",
        ])
        self.assertEqual(result[0].product_title, "Product keep-1")
        self.assertEqual(result[0].variant_title, "Variant keep-1")
        self.assertEqual(result[0].sku, "SKU-keep-1")
        self.assertEqual(request.await_count, 2)
        self.assertIsNone(request.await_args_list[0].kwargs["variables"]["cursor"])
        self.assertEqual(
            request.await_args_list[1].kwargs["variables"]["cursor"],
            "cursor-1",
        )
        self.assertIn('query: "taxable:false"', request.await_args_list[0].kwargs["query"])

    async def test_non_taxable_physical_variants_raise_on_graphql_errors(self):
        tax_sources = _tax_sources()
        request = AsyncMock(side_effect=[{"errors": [{"message": "Access denied"}]}])

        with patch.object(tax_sources, "shopify_graphql_request", new=request):
            with self.assertRaisesRegex(ValueError, "Access denied"):
                await tax_sources.fetch_non_taxable_physical_variants(
                    store_domain="degen-test.myshopify.com",
                    access_token="shpat_test",
                    client=object(),
                )

    async def test_non_taxable_physical_variants_normalize_nullable_sku(self):
        tax_sources = _tax_sources()
        node = _variant_node("nullable-sku")
        node["sku"] = None
        request = AsyncMock(side_effect=[_variant_page([node])])

        with patch.object(tax_sources, "shopify_graphql_request", new=request):
            result = await tax_sources.fetch_non_taxable_physical_variants(
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=object(),
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].sku, "")

    async def test_non_taxable_physical_variants_reject_malformed_node_evidence(self):
        tax_sources = _tax_sources()
        valid_node = _variant_node("keep")
        malformed_cases = []

        malformed_cases.append(("node", None))
        for field, value in [
            ("id", ""),
            ("title", None),
            ("taxable", "false"),
        ]:
            node = deepcopy(_variant_node("bad"))
            node[field] = value
            malformed_cases.append((field, node))

        node = deepcopy(_variant_node("bad"))
        del node["sku"]
        malformed_cases.append(("sku missing", node))

        node = deepcopy(_variant_node("bad"))
        node["sku"] = 123
        malformed_cases.append(("sku type", node))

        node = deepcopy(_variant_node("bad"))
        node["inventoryItem"] = None
        malformed_cases.append(("inventoryItem", node))

        node = deepcopy(_variant_node("bad"))
        node["inventoryItem"]["requiresShipping"] = "true"
        malformed_cases.append(("requiresShipping", node))

        node = deepcopy(_variant_node("bad"))
        node["product"] = None
        malformed_cases.append(("product", node))

        for field, value in [
            ("id", ""),
            ("title", ""),
            ("status", None),
            ("isGiftCard", None),
        ]:
            node = deepcopy(_variant_node("bad"))
            node["product"][field] = value
            malformed_cases.append((f"product.{field}", node))

        for field, malformed_node in malformed_cases:
            with self.subTest(field=field):
                request = AsyncMock(
                    side_effect=[_variant_page([valid_node, malformed_node])]
                )
                with patch.object(
                    tax_sources, "shopify_graphql_request", new=request
                ):
                    with self.assertRaises(ValueError):
                        await tax_sources.fetch_non_taxable_physical_variants(
                            store_domain="degen-test.myshopify.com",
                            access_token="shpat_test",
                            client=object(),
                        )

    async def test_non_taxable_physical_variants_raise_on_missing_page_shape(self):
        tax_sources = _tax_sources()
        malformed_pages = [
            {"data": {}},
            {"data": {"productVariants": {"nodes": []}}},
        ]

        for page in malformed_pages:
            with self.subTest(page=page):
                request = AsyncMock(side_effect=[page])
                with patch.object(tax_sources, "shopify_graphql_request", new=request):
                    with self.assertRaises(ValueError):
                        await tax_sources.fetch_non_taxable_physical_variants(
                            store_domain="degen-test.myshopify.com",
                            access_token="shpat_test",
                            client=object(),
                        )

    async def test_non_taxable_physical_variants_raise_on_blank_next_cursor(self):
        tax_sources = _tax_sources()
        request = AsyncMock(
            side_effect=[_variant_page([], has_next_page=True, end_cursor=" ")]
        )

        with patch.object(tax_sources, "shopify_graphql_request", new=request):
            with self.assertRaisesRegex(ValueError, "cursor"):
                await tax_sources.fetch_non_taxable_physical_variants(
                    store_domain="degen-test.myshopify.com",
                    access_token="shpat_test",
                    client=object(),
                )

    async def test_non_taxable_physical_variants_raise_on_repeated_next_cursor(self):
        tax_sources = _tax_sources()
        request = AsyncMock(
            side_effect=[
                _variant_page([], has_next_page=True, end_cursor="cursor-1"),
                _variant_page([], has_next_page=True, end_cursor="cursor-1"),
            ]
        )

        with patch.object(tax_sources, "shopify_graphql_request", new=request):
            with self.assertRaisesRegex(ValueError, "cursor"):
                await tax_sources.fetch_non_taxable_physical_variants(
                    store_domain="degen-test.myshopify.com",
                    access_token="shpat_test",
                    client=object(),
                )


class ShopifyTaxSentinelSchedulingConfigurationTests(unittest.TestCase):
    @staticmethod
    def default_settings():
        with patch.dict(os.environ, {}, clear=True):
            return Settings(_env_file=None)

    def test_sentinel_is_disabled_by_default(self):
        settings_obj = self.default_settings()
        self.assertTrue(hasattr(settings_obj, "shopify_pos_tax_sentinel_enabled"))
        self.assertFalse(settings_obj.shopify_pos_tax_sentinel_enabled)

    def test_sentinel_config_defaults_and_aliases_are_exact(self):
        expected = {
            "shopify_pos_tax_sentinel_enabled": (
                "SHOPIFY_POS_TAX_SENTINEL_ENABLED",
                False,
            ),
            "shopify_pos_location_id": ("SHOPIFY_POS_LOCATION_ID", ""),
            "shopify_pos_tax_city": ("SHOPIFY_POS_TAX_CITY", "San Jose"),
            "shopify_pos_tax_county": (
                "SHOPIFY_POS_TAX_COUNTY",
                "Santa Clara",
            ),
            "shopify_pos_expected_tax_rate": (
                "SHOPIFY_POS_EXPECTED_TAX_RATE",
                0.10,
            ),
            "shopify_pos_only": ("SHOPIFY_POS_ONLY", True),
            "shopify_pos_tax_order_lookback_days": (
                "SHOPIFY_POS_TAX_ORDER_LOOKBACK_DAYS",
                7,
            ),
        }
        settings_obj = self.default_settings()

        for field_name, (alias, default) in expected.items():
            with self.subTest(field_name=field_name):
                self.assertIn(field_name, Settings.model_fields)
                self.assertEqual(Settings.model_fields[field_name].alias, alias)
                self.assertEqual(getattr(settings_obj, field_name), default)

    def test_configured_requires_enable_pos_location_and_shopify_admin(self):
        self.assertTrue(
            hasattr(tax_sentinel, "shopify_pos_tax_sentinel_configured")
        )
        predicate = tax_sentinel.shopify_pos_tax_sentinel_configured
        base = {
            "shopify_pos_tax_sentinel_enabled": True,
            "shopify_pos_location_id": "555",
            "shopify_location_id": "inventory-location-must-not-be-used",
            "shopify_store_domain": "degen-test.myshopify.com",
            "shopify_access_token": "admin-token",
            "shopify_api_key": "",
        }
        incomplete = (
            {**base, "shopify_pos_tax_sentinel_enabled": False},
            {
                **base,
                "shopify_pos_location_id": "",
                "shopify_location_id": "inventory-location-must-not-be-used",
            },
            {**base, "shopify_pos_location_id": "   "},
            {**base, "shopify_store_domain": ""},
            {**base, "shopify_access_token": "", "shopify_api_key": ""},
        )

        for settings_values in incomplete:
            with self.subTest(settings_values=settings_values):
                self.assertFalse(predicate(SimpleNamespace(**settings_values)))

        self.assertTrue(predicate(SimpleNamespace(**base)))
        self.assertTrue(
            predicate(
                SimpleNamespace(
                    **{
                        **base,
                        "shopify_access_token": "",
                        "shopify_api_key": "legacy-admin-token",
                    }
                )
            )
        )

    def test_runtime_name_uses_configured_name_and_safe_fallback(self):
        self.assertTrue(hasattr(tax_sentinel, "shopify_pos_tax_runtime_name"))
        runtime_name = tax_sentinel.shopify_pos_tax_runtime_name
        self.assertEqual(
            runtime_name(SimpleNamespace(runtime_name="test-runtime")),
            "test-runtime_shopify_tax",
        )
        self.assertEqual(
            runtime_name(SimpleNamespace(runtime_name="   ")),
            "app_shopify_tax",
        )

    def test_check_error_identities_are_bounded_deduplicated_and_sanitized(self):
        raw_errors = [
            "order check failed (ValueError)",
            "order check failed (ValueError)",
            "catalog check failed (RuntimeError)",
            "location check failed (OperationalError)",
            "official check failed (TimeoutError)",
            "configuration check failed (ValueError)",
            "private@example.com shpat_super_secret_token",
        ] * 4

        check_errors = tax_sentinel._sanitized_check_errors(raw_errors)

        self.assertLessEqual(
            len(check_errors),
            tax_sentinel.MAX_LOG_CHECK_ERRORS,
        )
        self.assertEqual(len(check_errors), len(set(check_errors)))
        self.assertEqual(check_errors[0], "order check failed (ValueError)")
        visible = repr(check_errors)
        self.assertNotIn("private@example.com", visible)
        self.assertNotIn("shpat_super_secret_token", visible)

    def test_prior_official_effective_date_canonicalizes_padded_and_unpadded_days(self):
        for value in ("April 1, 2026", "April 01, 2026"):
            with self.subTest(value=value):
                self.assertEqual(
                    tax_sentinel._prior_official_effective_label(value),
                    "April 1, 2026",
                )

    def test_prior_official_effective_date_rejects_impossible_and_arbitrary_text(self):
        invalid_values = (
            "February 30, 2026",
            "1 Secret Lane",
            "Private Buyer 123",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                self.assertIsNone(
                    tax_sentinel._prior_official_effective_label(value)
                )


class ShopifyTaxSentinelMainWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_startup_sets_none_without_creating_task(self):
        self.assertTrue(
            hasattr(main_module, "_start_shopify_pos_tax_sentinel_task")
        )
        app = SimpleNamespace(state=SimpleNamespace())
        stop_event = asyncio.Event()
        background_tasks = []
        settings_obj = SimpleNamespace(runtime_name="test-runtime")
        configured = Mock(return_value=False)
        tracker = Mock()

        with (
            patch.object(main_module, "settings", settings_obj),
            patch.object(
                main_module,
                "shopify_pos_tax_sentinel_configured",
                new=configured,
            ),
            patch.object(main_module, "track_background_task", new=tracker),
        ):
            result = main_module._start_shopify_pos_tax_sentinel_task(
                app,
                stop_event,
                background_tasks,
            )

        self.assertIsNone(result)
        self.assertIsNone(app.state.shopify_pos_tax_sentinel_task)
        self.assertEqual(background_tasks, [])
        configured.assert_called_once_with(settings_obj)
        tracker.assert_not_called()

    async def test_enabled_startup_tracks_named_task_with_normalized_runtime(self):
        self.assertTrue(
            hasattr(main_module, "_start_shopify_pos_tax_sentinel_task")
        )
        app = SimpleNamespace(state=SimpleNamespace())
        stop_event = asyncio.Event()
        background_tasks = []
        settings_obj = SimpleNamespace(runtime_name="test-runtime")
        configured = Mock(return_value=True)
        tracker = Mock(side_effect=lambda task, **_kwargs: task)

        async def wait_until_stopped(event):
            await event.wait()

        task = None
        try:
            with (
                patch.object(main_module, "settings", settings_obj),
                patch.object(
                    main_module,
                    "shopify_pos_tax_sentinel_configured",
                    new=configured,
                ),
                patch.object(
                    main_module,
                    "periodic_shopify_pos_tax_sentinel_loop",
                    new=wait_until_stopped,
                ),
                patch.object(main_module, "track_background_task", new=tracker),
            ):
                task = main_module._start_shopify_pos_tax_sentinel_task(
                    app,
                    stop_event,
                    background_tasks,
                )

            self.assertIsInstance(task, asyncio.Task)
            self.assertEqual(task.get_name(), "shopify-pos-tax-sentinel")
            self.assertIs(app.state.shopify_pos_tax_sentinel_task, task)
            self.assertEqual(background_tasks, [task])
            configured.assert_called_once_with(settings_obj)
            tracker.assert_called_once_with(
                task,
                runtime_name="test-runtime_shopify_tax",
                task_name="shopify-pos-tax-sentinel",
                stop_event=stop_event,
            )
        finally:
            stop_event.set()
            if task is not None:
                if not task.done():
                    await asyncio.wait_for(task, timeout=0.2)
                else:
                    await task


class ShopifyTaxSentinelLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_loop_runs_once_and_does_not_duplicate_coordinator_summary(self):
        self.assertTrue(
            hasattr(tax_sentinel, "periodic_shopify_pos_tax_sentinel_loop")
        )
        stop_event = asyncio.Event()
        calls = []

        async def run_once(*, settings_obj):
            calls.append(settings_obj)
            print('{"action": "audit_once", "owner": "coordinator"}')
            stop_event.set()

        output = StringIO()
        with (
            patch.object(tax_sentinel, "run_shopify_tax_sentinel_once", new=run_once),
            redirect_stdout(output),
        ):
            await tax_sentinel.periodic_shopify_pos_tax_sentinel_loop(stop_event)

        self.assertEqual(calls, [tax_sentinel.settings])
        self.assertEqual(
            output.getvalue().splitlines(),
            ['{"action": "audit_once", "owner": "coordinator"}'],
        )

    async def test_stop_event_interrupts_daily_wait_without_second_cycle(self):
        stop_event = asyncio.Event()
        cycle_complete = asyncio.Event()
        calls = 0

        async def run_once(*, settings_obj):
            nonlocal calls
            calls += 1
            cycle_complete.set()

        with patch.object(
            tax_sentinel,
            "run_shopify_tax_sentinel_once",
            new=run_once,
        ):
            task = asyncio.create_task(
                tax_sentinel.periodic_shopify_pos_tax_sentinel_loop(stop_event)
            )
            try:
                await asyncio.wait_for(cycle_complete.wait(), timeout=0.2)
                await asyncio.sleep(0)
                stop_event.set()
                await asyncio.wait_for(task, timeout=0.2)
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(calls, 1)

    async def test_loop_sanitizes_exception_and_continues_until_stopped(self):
        self.assertTrue(
            hasattr(tax_sentinel, "periodic_shopify_pos_tax_sentinel_loop")
        )
        stop_event = asyncio.Event()
        settings_obj = SimpleNamespace(runtime_name="test-runtime")
        calls = 0
        sensitive_values = (
            "shpat_super_secret_token",
            "Private Customer",
            "private@example.com",
            "1 Secret Lane",
            "nested-secret-value",
        )

        async def run_once(*, settings_obj):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError(
                    {
                        "credential": sensitive_values[0],
                        "customer": {
                            "name": sensitive_values[1],
                            "email": sensitive_values[2],
                            "address": sensitive_values[3],
                            "nested": sensitive_values[4],
                        },
                    }
                )
            stop_event.set()

        output = StringIO()
        with (
            patch.object(tax_sentinel, "settings", settings_obj),
            patch.object(tax_sentinel, "run_shopify_tax_sentinel_once", new=run_once),
            patch.object(
                tax_sentinel,
                "SHOPIFY_POS_TAX_SENTINEL_INTERVAL_SECONDS",
                0,
            ),
            redirect_stdout(output),
        ):
            await tax_sentinel.periodic_shopify_pos_tax_sentinel_loop(stop_event)

        self.assertEqual(calls, 2)
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        logged = json.loads(lines[0])
        self.assertEqual(logged["runtime"], "test-runtime_shopify_tax")
        self.assertEqual(logged["action"], "shopify.pos_tax_sentinel.failed")
        self.assertFalse(logged["success"])
        self.assertIsNone(logged["error"])
        self.assertEqual(logged["error_type"], "RuntimeError")
        self.assertEqual(
            logged["check_errors"],
            ["runtime check failed (RuntimeError)"],
        )
        started_at = datetime.fromisoformat(logged["started_at"])
        completed_at = datetime.fromisoformat(logged["completed_at"])
        self.assertIsNotNone(started_at.utcoffset())
        self.assertIsNotNone(completed_at.utcoffset())
        self.assertGreaterEqual(completed_at, started_at)
        for sensitive_value in sensitive_values:
            with self.subTest(sensitive_value=sensitive_value):
                self.assertNotIn(sensitive_value, output.getvalue())


class ShopifyTaxSentinelCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite:///:memory:", connect_args={"check_same_thread": False}
        )
        SQLModel.metadata.create_all(self.engine)
        engine = self.engine

        @contextmanager
        def session_factory():
            with Session(engine) as session:
                yield session

        self.session_factory = session_factory
        self.now = datetime(2026, 6, 29, 19, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.engine.dispose()

    def settings(self, **overrides):
        values = {
            "runtime_name": "test-runtime",
            "shopify_store_domain": "degen-test.myshopify.com",
            "shopify_access_token": "shpat_super_secret_token",
            "shopify_api_key": "",
            "shopify_pos_expected_tax_rate": "0.10",
            "shopify_pos_location_id": "555",
            "shopify_pos_tax_city": "San Jose",
            "shopify_pos_tax_county": "Santa Clara",
            "shopify_pos_tax_order_lookback_days": 7,
            "shopify_pos_only": True,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def official_rate(self, rate="0.10000"):
        tax_sources = _tax_sources()
        return tax_sources.OfficialTaxRate(
            city="San Jose",
            county="Santa Clara",
            rate=Decimal(rate),
            effective_label="April 1, 2026",
        )

    def variant(self, suffix="disabled"):
        tax_sources = _tax_sources()
        return tax_sources.ShopifyVariantTaxState(
            product_id=f"gid://shopify/Product/{suffix}",
            product_title=f"Product {suffix}",
            variant_id=f"gid://shopify/ProductVariant/{suffix}",
            variant_title=f"Variant {suffix}",
            sku=f"SKU-{suffix}",
        )

    def valid_locations(self):
        return [
            {
                "id": 555,
                "name": "San Jose POS",
                "active": True,
                "city": "San Jose",
                "province_code": "CA",
                "address1": "must not be persisted",
            }
        ]

    def add_order(
        self,
        payload,
        *,
        order_id=None,
        order_number=None,
        raw_payload=None,
        created_at=None,
    ):
        stored_order_id = str(order_id or payload.get("id") or "stored-order")
        stored_order_number = str(
            order_number or payload.get("name") or stored_order_id
        )
        row = ShopifyOrder(
            shopify_order_id=stored_order_id,
            order_number=stored_order_number,
            created_at=created_at or self.now - timedelta(days=1),
            updated_at=created_at or self.now - timedelta(days=1),
            financial_status=str(payload.get("financial_status") or ""),
            raw_payload=(
                raw_payload if raw_payload is not None else json.dumps(payload)
            ),
        )
        with Session(self.engine) as session:
            session.add(row)
            session.commit()

    def add_issue(self, *, issue_type, **kwargs):
        with Session(self.engine) as session:
            issue = shopify_sync.record_shopify_sync_issue(
                session,
                issue_type=issue_type,
                message="Previously observed sentinel issue.",
                **kwargs,
            )
            session.commit()
            session.refresh(issue)
            return issue.issue_key

    def set_app_setting(self, key, value):
        with Session(self.engine) as session:
            row = session.get(AppSetting, key)
            if row is None:
                row = AppSetting(key=key, value=value)
            else:
                row.value = value
            session.add(row)
            session.commit()

    def issues(self):
        with Session(self.engine) as session:
            return session.exec(
                select(ShopifySyncIssue).order_by(ShopifySyncIssue.issue_key)
            ).all()

    async def run_sentinel(
        self,
        *,
        settings=None,
        official=None,
        variants=None,
        locations=None,
        force_official_check=True,
        capture_output=False,
        session_factory=None,
    ):
        official_result = self.official_rate() if official is None else official
        variant_result = [] if variants is None else variants
        location_result = self.valid_locations() if locations is None else locations
        official_mock = AsyncMock()
        variant_mock = AsyncMock()
        location_mock = AsyncMock()
        if isinstance(official_result, BaseException):
            official_mock.side_effect = official_result
        else:
            official_mock.return_value = official_result
        if isinstance(variant_result, BaseException):
            variant_mock.side_effect = variant_result
        else:
            variant_mock.return_value = variant_result
        if isinstance(location_result, BaseException):
            location_mock.side_effect = location_result
        else:
            location_mock.return_value = location_result

        output = StringIO()
        output_context = (
            redirect_stdout(output)
            if capture_output
            else redirect_stdout(StringIO())
        )
        with (
            patch(
                "app.shopify_tax_sentinel.fetch_cdtfa_city_rate",
                new=official_mock,
            ),
            patch(
                "app.shopify_tax_sentinel.fetch_non_taxable_physical_variants",
                new=variant_mock,
            ),
            patch(
                "app.shopify_tax_sentinel.get_shopify_locations",
                new=location_mock,
            ),
            output_context,
        ):
            summary = await run_shopify_tax_sentinel_once(
                settings_obj=settings or self.settings(),
                session_factory=session_factory or self.session_factory,
                now=self.now,
                force_official_check=force_official_check,
            )
        return summary, official_mock, variant_mock, location_mock, output.getvalue()

    async def test_normal_complete_run_has_no_open_issues(self):
        self.add_order(paid_pos_order())

        summary, official, variants, locations, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        self.assertEqual(summary.variants_checked, 0)
        self.assertEqual(summary.orders_checked, 1)
        self.assertEqual(summary.findings_recorded, 0)
        self.assertEqual(summary.errors, ())
        self.assertEqual(
            [issue for issue in self.issues() if issue.status == "open"], []
        )
        official.assert_awaited_once()
        variants.assert_awaited_once()
        locations.assert_awaited_once()

    async def test_non_taxable_variant_records_sanitized_catalog_finding(self):
        bad_variant = self.variant()

        summary, _, _, _, _ = await self.run_sentinel(variants=[bad_variant])

        self.assertTrue(summary.success)
        self.assertEqual(summary.variants_checked, 1)
        issue = self.issues()[0]
        self.assertEqual(issue.issue_type, "taxable_variant_disabled")
        self.assertEqual(issue.severity, "warning")
        self.assertEqual(issue.shopify_product_id, bad_variant.product_id)
        self.assertEqual(issue.shopify_variant_id, bad_variant.variant_id)
        self.assertEqual(issue.shopify_sku, bad_variant.sku)
        self.assertIn(bad_variant.product_title, issue.shopify_title)
        evidence = json.loads(issue.raw_payload_json)
        self.assertEqual(evidence["product_title"], bad_variant.product_title)
        self.assertEqual(evidence["variant_title"], bad_variant.variant_title)
        self.assertNotIn("token", issue.raw_payload_json.casefold())

    async def test_official_rate_change_records_critical_finding(self):
        summary, _, _, _, _ = await self.run_sentinel(
            official=self.official_rate("0.1025")
        )

        self.assertTrue(summary.success)
        issue = self.issues()[0]
        self.assertEqual(issue.issue_type, "official_tax_rate_changed")
        self.assertEqual(issue.severity, "critical")
        evidence = json.loads(issue.raw_payload_json)
        self.assertEqual(evidence["official_rate"], "0.1025")
        self.assertEqual(evidence["expected_rate"], "0.10")
        self.assertIn("San Jose", issue.message)
        self.assertIn("Santa Clara", issue.message)
        self.assertIn("0.1025", issue.message)
        self.assertIn("0.10", issue.message)
        self.assertIn("April 1, 2026", issue.message)

    async def test_official_persistence_failure_rolls_back_and_reraises(self):
        stale_key = self.add_issue(issue_type="official_tax_rate_changed")
        rollback = Mock()
        engine = self.engine

        @contextmanager
        def tracking_session_factory():
            with Session(engine) as session:
                original_rollback = session.rollback

                def tracked_rollback():
                    rollback()
                    original_rollback()

                session.rollback = tracked_rollback
                yield session

        original_record = shopify_sync.record_shopify_sync_issue
        database_error = OperationalError(
            "INSERT shopify_sync_issues",
            {},
            RuntimeError("database unavailable"),
        )

        def fail_official_finding(session, *, issue_type, **kwargs):
            if issue_type == "official_tax_rate_changed":
                raise database_error
            return original_record(
                session,
                issue_type=issue_type,
                **kwargs,
            )

        with patch(
            "app.shopify_tax_sentinel.record_shopify_sync_issue",
            side_effect=fail_official_finding,
        ):
            with self.assertRaises(OperationalError):
                await self.run_sentinel(
                    official=self.official_rate("0.1025"),
                    session_factory=tracking_session_factory,
                )

        rollback.assert_called_once_with()
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")
        with Session(self.engine) as session:
            self.assertIsNone(session.get(AppSetting, OFFICIAL_CHECK_AT_KEY))
            self.assertIsNone(session.get(AppSetting, OFFICIAL_RATE_KEY))
            self.assertIsNone(session.get(AppSetting, OFFICIAL_EFFECTIVE_KEY))
            self.assertIsNone(
                session.get(AppSetting, tax_sentinel.LOCATION_CHECK_AT_KEY)
            )
            self.assertIsNone(
                session.get(AppSetting, tax_sentinel.CATALOG_CHECK_AT_KEY)
            )
            self.assertIsNone(
                session.get(AppSetting, tax_sentinel.ORDER_CHECK_AT_KEY)
            )
        self.assertFalse(any(issue.issue_type == "sync_error" for issue in self.issues()))

    async def test_official_failure_records_source_issue_without_resolving_rate_issue(self):
        old_key = self.add_issue(
            issue_type="official_tax_rate_changed",
            shopify_product_id="cdtfa",
            shopify_variant_id="san-jose-santa-clara",
        )
        secret_error = RuntimeError(
            "request failed with shpat_super_secret_token for private@example.com"
        )

        summary, _, _, _, _ = await self.run_sentinel(official=secret_error)

        self.assertFalse(summary.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[old_key].status, "open")
        source_issue = next(
            issue
            for issue in issues.values()
            if issue.issue_type == "official_tax_source_unavailable"
        )
        self.assertEqual(source_issue.severity, "critical")
        self.assertIn("San Jose", source_issue.message)
        self.assertIn("Santa Clara", source_issue.message)
        self.assertIn("RuntimeError", source_issue.message)
        visible = (
            repr(summary.errors)
            + source_issue.raw_payload_json
            + source_issue.message
        )
        self.assertNotIn("shpat_super_secret_token", visible)
        self.assertNotIn("private@example.com", visible)

    async def test_complete_matching_official_check_resolves_prior_official_issues(self):
        keys = {
            self.add_issue(issue_type="official_tax_rate_changed"),
            self.add_issue(issue_type="official_tax_source_unavailable"),
        }

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        self.assertEqual(summary.findings_resolved, 2)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertTrue(all(issues[key].status == "resolved" for key in keys))

    async def test_complete_catalog_check_resolves_variant_no_longer_returned(self):
        stale_key = self.add_issue(
            issue_type="taxable_variant_disabled",
            shopify_product_id="product-stale",
            shopify_variant_id="variant-stale",
        )

        summary, _, _, _, _ = await self.run_sentinel(variants=[])

        self.assertTrue(summary.success)
        self.assertEqual(summary.findings_resolved, 1)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "resolved")

    async def test_failed_catalog_check_records_sync_error_and_resolves_nothing(self):
        stale_key = self.add_issue(
            issue_type="taxable_variant_disabled",
            shopify_product_id="product-stale",
            shopify_variant_id="variant-stale",
        )
        secret_error = RuntimeError(
            "GraphQL rejected shpat_super_secret_token for Buyer Name"
        )

        summary, _, _, _, _ = await self.run_sentinel(variants=secret_error)

        self.assertFalse(summary.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")
        sync_issue = next(
            issue
            for issue in issues.values()
            if issue.issue_type == shopify_sync.SHOPIFY_SYNC_ISSUE_SYNC_ERROR
            and json.loads(issue.raw_payload_json).get("check") == "catalog"
        )
        visible = repr(summary.errors) + sync_issue.raw_payload_json + sync_issue.message
        self.assertNotIn("shpat_super_secret_token", visible)
        self.assertNotIn("Buyer Name", visible)

    async def test_catalog_success_resolves_only_sentinel_catalog_sync_error(self):
        failed, _, _, _, _ = await self.run_sentinel(
            variants=RuntimeError("catalog unavailable")
        )
        self.assertFalse(failed.success)
        catalog_error = next(
            issue
            for issue in self.issues()
            if issue.issue_type == "sync_error"
            and json.loads(issue.raw_payload_json).get("check") == "catalog"
        )
        unrelated_key = self.add_issue(
            issue_type="sync_error",
            shopify_product_id="inventory-sync",
            shopify_title="inventory:push",
        )

        recovered, _, _, _, _ = await self.run_sentinel(variants=[])

        self.assertTrue(recovered.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[catalog_error.issue_key].status, "resolved")
        self.assertEqual(issues[unrelated_key].status, "open")
        self.assertEqual(catalog_error.shopify_title, "tax-sentinel:catalog")

    async def test_non_pos_order_records_critical_order_finding(self):
        self.add_order(paid_pos_order(id=8765, name="#2002", source_name="web"))

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        issue = self.issues()[0]
        self.assertEqual(issue.issue_type, "non_pos_order_detected")
        self.assertEqual(issue.severity, "critical")
        self.assertEqual(issue.shopify_order_id, "8765")
        self.assertEqual(issue.shopify_order_number, "#2002")
        self.assertIn("#2002", issue.message)

    async def test_line_item_evidence_errors_persist_as_missing_findings(self):
        missing_items = paid_pos_order(id="missing-items", name="#missing-items")
        missing_items.pop("line_items")
        null_line_tax = paid_pos_order(id="null-line-tax", name="#null-line-tax")
        null_line_tax["line_items"][0]["tax_lines"] = None
        malformed_line_tax = paid_pos_order(
            id="malformed-line-tax",
            name="#malformed-line-tax",
        )
        malformed_line_tax["line_items"][0]["tax_lines"] = [None]
        for payload in (missing_items, null_line_tax, malformed_line_tax):
            self.add_order(payload)

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertFalse(summary.success)
        missing_issues = [
            issue
            for issue in self.issues()
            if issue.issue_type == "pos_tax_lines_missing"
        ]
        self.assertEqual(
            {issue.shopify_order_id for issue in missing_issues},
            {"missing-items", "null-line-tax", "malformed-line-tax"},
        )
        for issue in missing_issues:
            self.assertIn(issue.shopify_order_number, issue.message)

    async def test_non_taxable_line_classification_errors_persist_safely(self):
        missing_classification = paid_pos_order(
            id="missing-classification",
            name="#missing-classification",
            total_tax="0",
            tax_lines=[],
            line_items=[
                {
                    "id": 1,
                    "taxable": False,
                    "gift_card": False,
                }
            ],
        )
        malformed_classification = paid_pos_order(
            id="malformed-classification",
            name="#malformed-classification",
            total_tax="0",
            tax_lines=[],
            customer={"email": "private@example.com"},
            line_items=[
                {
                    "id": 2,
                    "taxable": False,
                    "requires_shipping": True,
                    "gift_card": {"customer_email": "nested-private@example.com"},
                    "title": "Private Customer Item",
                }
            ],
        )
        for payload in (missing_classification, malformed_classification):
            self.add_order(payload)

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertFalse(summary.success)
        missing_issues = [
            issue
            for issue in self.issues()
            if issue.issue_type == "pos_tax_lines_missing"
        ]
        self.assertEqual(
            {issue.shopify_order_id for issue in missing_issues},
            {"missing-classification", "malformed-classification"},
        )
        visible = repr(summary.errors) + "".join(
            issue.message + issue.raw_payload_json for issue in missing_issues
        )
        self.assertNotIn("private@example.com", visible)
        self.assertNotIn("nested-private@example.com", visible)
        self.assertNotIn("Private Customer Item", visible)

    async def test_malformed_orders_are_visible_and_block_order_resolutions(self):
        stale_key = self.add_issue(
            issue_type="pos_tax_rate_mismatch",
            shopify_order_id="stale-order",
        )
        self.add_order(
            {},
            order_id="malformed-json",
            order_number="#bad-json",
            raw_payload='{"customer_email":"private@example.com","broken":',
        )
        invalid_evidence = paid_pos_order(
            id="invalid-evidence",
            name="#bad-evidence",
            customer={"name": "Private Buyer", "address": "1 Secret Lane"},
            total_tax="not-a-number",
        )
        self.add_order(invalid_evidence)

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertFalse(summary.success)
        self.assertEqual(summary.orders_checked, 2)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")
        missing = [
            issue
            for issue in issues.values()
            if issue.issue_type == "pos_tax_lines_missing"
        ]
        self.assertEqual(
            {issue.shopify_order_id for issue in missing},
            {"malformed-json", "invalid-evidence"},
        )
        visible = repr(summary.errors) + "".join(
            issue.raw_payload_json + issue.message for issue in missing
        )
        self.assertNotIn("private@example.com", visible)
        self.assertNotIn("Private Buyer", visible)
        self.assertNotIn("1 Secret Lane", visible)

    async def test_in_window_clean_order_resolves_its_stale_finding(self):
        stale_key = self.add_issue(
            issue_type="non_pos_order_detected",
            shopify_order_id="12345",
            shopify_order_number="#1001",
        )
        self.add_order(paid_pos_order())

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        self.assertEqual(summary.findings_resolved, 1)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "resolved")

    async def test_order_issue_outside_lookback_remains_open(self):
        stale_key = self.add_issue(
            issue_type="non_pos_order_detected",
            shopify_order_id="old-order",
            shopify_order_number="#old",
        )
        self.add_order(
            paid_pos_order(id="old-order", name="#old"),
            created_at=self.now - timedelta(days=8),
        )

        summary, _, _, _, _ = await self.run_sentinel(
            settings=self.settings(shopify_pos_tax_order_lookback_days=7)
        )

        self.assertTrue(summary.success)
        self.assertEqual(summary.orders_checked, 0)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")

    async def test_order_issue_without_corresponding_order_remains_open(self):
        stale_key = self.add_issue(
            issue_type="pos_tax_rate_mismatch",
            shopify_order_id="deleted-order",
        )

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        self.assertEqual(summary.orders_checked, 0)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")

    async def test_location_resolution_combines_configuration_and_order_keys(self):
        wrong_order = paid_pos_order(id=9753, name="#3003", location_id="777")
        self.add_order(wrong_order)
        wrong_config_locations = [
            {
                "id": "555",
                "active": False,
                "city": "Milpitas",
                "province_code": "NV",
            }
        ]

        first, _, _, _, _ = await self.run_sentinel(
            locations=wrong_config_locations
        )

        self.assertTrue(first.success)
        initial = [
            issue
            for issue in self.issues()
            if issue.issue_type == "pos_location_mismatch"
        ]
        self.assertEqual(len(initial), 2)
        config_issue = next(issue for issue in initial if not issue.shopify_order_id)
        order_issue = next(issue for issue in initial if issue.shopify_order_id)
        unrelated_key = self.add_issue(
            issue_type="pos_location_mismatch",
            shopify_product_id="other-location-audit",
            shopify_title="other:location-config",
        )

        second, _, _, _, _ = await self.run_sentinel(locations=self.valid_locations())

        self.assertTrue(second.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[config_issue.issue_key].status, "resolved")
        self.assertEqual(issues[order_issue.issue_key].status, "open")
        self.assertEqual(issues[unrelated_key].status, "open")
        self.assertEqual(
            config_issue.shopify_title,
            "tax-sentinel:location-config",
        )
        self.assertIn("555", config_issue.message)
        self.assertIn("San Jose", config_issue.message)
        self.assertIn("CA", config_issue.message)
        self.assertIn("Milpitas", config_issue.message)
        self.assertIn("NV", config_issue.message)
        self.assertIn("false", config_issue.message.casefold())
        self.assertNotIn("must not be persisted", config_issue.message)

    async def test_location_requires_literal_true_active_field(self):
        invalid_active_values = [
            (False, None),
            (True, None),
            (True, False),
            (True, 0),
            (True, "true"),
        ]

        for active_present, active_value in invalid_active_values:
            with self.subTest(
                active_present=active_present,
                active_value=active_value,
            ):
                location = {
                    "id": "555",
                    "city": "San Jose",
                    "province_code": "CA",
                }
                if active_present:
                    location["active"] = active_value

                summary, _, _, _, _ = await self.run_sentinel(
                    locations=[location]
                )

                self.assertTrue(summary.success)
                config_issues = [
                    issue
                    for issue in self.issues()
                    if issue.issue_type == "pos_location_mismatch"
                    and not issue.shopify_order_id
                ]
                self.assertEqual(len(config_issues), 1)
                self.assertEqual(config_issues[0].status, "open")
                self.assertEqual(config_issues[0].severity, "critical")

    async def test_future_dated_order_is_not_audited(self):
        future_order = paid_pos_order(
            id="future-order",
            name="#future",
            source_name="web",
        )
        self.add_order(
            future_order,
            created_at=self.now + timedelta(seconds=1),
        )

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        self.assertEqual(summary.orders_checked, 0)
        self.assertFalse(
            any(
                issue.issue_type == "non_pos_order_detected"
                for issue in self.issues()
            )
        )

    async def test_location_source_failure_preserves_all_location_findings(self):
        stale_key = self.add_issue(
            issue_type="pos_location_mismatch",
            shopify_product_id="tax-sentinel",
            shopify_variant_id="location-configuration",
        )

        summary, _, _, _, _ = await self.run_sentinel(
            locations=RuntimeError("shpat_super_secret_token location failure")
        )

        self.assertFalse(summary.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[stale_key].status, "open")
        location_error = next(
            issue
            for issue in issues.values()
            if issue.issue_type == "sync_error"
            and json.loads(issue.raw_payload_json).get("check") == "location"
        )
        self.assertNotIn("shpat_super_secret_token", location_error.raw_payload_json)

    async def test_location_success_resolves_sentinel_location_sync_error(self):
        failed, _, _, _, _ = await self.run_sentinel(
            locations=RuntimeError("location unavailable")
        )
        self.assertFalse(failed.success)
        location_error = next(
            issue
            for issue in self.issues()
            if issue.issue_type == "sync_error"
            and json.loads(issue.raw_payload_json).get("check") == "location"
        )

        recovered, _, _, _, _ = await self.run_sentinel(
            locations=self.valid_locations()
        )

        self.assertTrue(recovered.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[location_error.issue_key].status, "resolved")
        self.assertEqual(location_error.shopify_title, "tax-sentinel:location")

    async def test_successful_run_records_each_check_last_success(self):
        self.add_order(paid_pos_order())

        summary, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(summary.success)
        with Session(self.engine) as session:
            for key in (
                tax_sentinel.LOCATION_CHECK_AT_KEY,
                tax_sentinel.CATALOG_CHECK_AT_KEY,
                tax_sentinel.ORDER_CHECK_AT_KEY,
                OFFICIAL_CHECK_AT_KEY,
            ):
                with self.subTest(key=key):
                    self.assertEqual(session.get(AppSetting, key).value, self.now.isoformat())

    async def test_location_failure_preserves_its_prior_success_independently(self):
        prior_location = (self.now - timedelta(days=2)).isoformat()
        prior_other = (self.now - timedelta(days=3)).isoformat()
        self.set_app_setting(tax_sentinel.LOCATION_CHECK_AT_KEY, prior_location)
        self.set_app_setting(tax_sentinel.CATALOG_CHECK_AT_KEY, prior_other)
        self.set_app_setting(tax_sentinel.ORDER_CHECK_AT_KEY, prior_other)

        summary, _, _, _, _ = await self.run_sentinel(
            locations=RuntimeError("private@example.com shpat_super_secret_token")
        )

        self.assertFalse(summary.success)
        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.LOCATION_CHECK_AT_KEY).value,
                prior_location,
            )
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.CATALOG_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.ORDER_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )

    async def test_catalog_failure_preserves_its_prior_success_independently(self):
        prior_catalog = (self.now - timedelta(days=2)).isoformat()
        prior_other = (self.now - timedelta(days=3)).isoformat()
        self.set_app_setting(tax_sentinel.CATALOG_CHECK_AT_KEY, prior_catalog)
        self.set_app_setting(tax_sentinel.LOCATION_CHECK_AT_KEY, prior_other)
        self.set_app_setting(tax_sentinel.ORDER_CHECK_AT_KEY, prior_other)

        summary, _, _, _, _ = await self.run_sentinel(
            variants=RuntimeError("private@example.com shpat_super_secret_token")
        )

        self.assertFalse(summary.success)
        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.CATALOG_CHECK_AT_KEY).value,
                prior_catalog,
            )
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.LOCATION_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.ORDER_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )

    async def test_incomplete_order_batch_preserves_prior_then_complete_batch_advances(self):
        prior_order = (self.now - timedelta(days=2)).isoformat()
        self.set_app_setting(tax_sentinel.ORDER_CHECK_AT_KEY, prior_order)
        self.add_order(
            {},
            order_id="repairable-order",
            order_number="#repairable",
            raw_payload='{"broken":',
        )

        failed, _, _, _, _ = await self.run_sentinel()

        self.assertFalse(failed.success)
        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.ORDER_CHECK_AT_KEY).value,
                prior_order,
            )
            order = session.exec(
                select(ShopifyOrder).where(
                    ShopifyOrder.shopify_order_id == "repairable-order"
                )
            ).one()
            order.raw_payload = json.dumps(
                paid_pos_order(id="repairable-order", name="#repairable")
            )
            session.add(order)
            session.commit()

        self.now += timedelta(hours=1)
        recovered, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(recovered.success)
        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, tax_sentinel.ORDER_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )

    async def test_completed_checks_with_findings_record_last_success(self):
        self.add_order(paid_pos_order(source_name="web"))
        wrong_location = [
            {
                "id": "555",
                "active": False,
                "city": "Milpitas",
                "province_code": "NV",
            }
        ]

        summary, _, _, _, _ = await self.run_sentinel(
            locations=wrong_location,
            variants=[self.variant("finding")],
        )

        self.assertTrue(summary.success)
        with Session(self.engine) as session:
            for key in (
                tax_sentinel.LOCATION_CHECK_AT_KEY,
                tax_sentinel.CATALOG_CHECK_AT_KEY,
                tax_sentinel.ORDER_CHECK_AT_KEY,
            ):
                with self.subTest(key=key):
                    self.assertEqual(session.get(AppSetting, key).value, self.now.isoformat())

    async def test_official_failure_exposes_valid_stale_observation_safely(self):
        prior_checked_at = (self.now - timedelta(days=2)).isoformat()
        self.set_app_setting(OFFICIAL_CHECK_AT_KEY, prior_checked_at)
        self.set_app_setting(OFFICIAL_RATE_KEY, "0.10000")
        self.set_app_setting(OFFICIAL_EFFECTIVE_KEY, "April 1, 2026")

        summary, _, _, _, _ = await self.run_sentinel(
            official=RuntimeError("private@example.com shpat_super_secret_token")
        )

        self.assertFalse(summary.success)
        issue = next(
            issue
            for issue in self.issues()
            if issue.issue_type == "official_tax_source_unavailable"
        )
        evidence = json.loads(issue.raw_payload_json)
        self.assertIs(evidence["stale"], True)
        self.assertEqual(evidence["prior_checked_at"], prior_checked_at)
        self.assertEqual(evidence["prior_rate"], "0.10000")
        self.assertEqual(evidence["prior_effective_label"], "April 1, 2026")
        for expected in (prior_checked_at, "0.10000", "April 1, 2026"):
            self.assertIn(expected, issue.message)
        self.assertIn("stale", issue.message.casefold())
        visible = issue.message + issue.raw_payload_json + repr(summary.errors)
        self.assertNotIn("private@example.com", visible)
        self.assertNotIn("shpat_super_secret_token", visible)
        with Session(self.engine) as session:
            self.assertEqual(session.get(AppSetting, OFFICIAL_CHECK_AT_KEY).value, prior_checked_at)
            self.assertEqual(session.get(AppSetting, OFFICIAL_RATE_KEY).value, "0.10000")
            self.assertEqual(
                session.get(AppSetting, OFFICIAL_EFFECTIVE_KEY).value,
                "April 1, 2026",
            )

    async def test_official_failure_omits_tampered_prior_observation(self):
        tampered_values = (
            "private@example.com timestamp",
            "NaN shpat_super_secret_token",
            "private@example.com shpat_super_secret_token effective",
        )
        self.set_app_setting(OFFICIAL_CHECK_AT_KEY, tampered_values[0])
        self.set_app_setting(OFFICIAL_RATE_KEY, tampered_values[1])
        self.set_app_setting(OFFICIAL_EFFECTIVE_KEY, tampered_values[2])

        summary, _, _, _, _ = await self.run_sentinel(
            official=RuntimeError("raw exception private@example.com")
        )

        self.assertFalse(summary.success)
        issue = next(
            issue
            for issue in self.issues()
            if issue.issue_type == "official_tax_source_unavailable"
        )
        evidence = json.loads(issue.raw_payload_json)
        self.assertIs(evidence["stale"], True)
        self.assertIsNone(evidence["prior_checked_at"])
        self.assertIsNone(evidence["prior_rate"])
        self.assertIsNone(evidence["prior_effective_label"])
        self.assertIn(
            "no prior successful official observation",
            issue.message.casefold(),
        )
        visible = issue.message + issue.raw_payload_json + repr(summary.errors)
        for tampered_value in tampered_values:
            self.assertNotIn(tampered_value, visible)
        self.assertNotIn("raw exception", visible)

    async def test_invalid_prior_effective_date_clears_atomic_triplet_without_echo(self):
        prior_checked_at = (self.now - timedelta(days=2)).isoformat()
        invalid_labels = (
            "February 30, 2026",
            "1 Secret Lane",
            "Private Buyer 123",
        )
        for invalid_label in invalid_labels:
            with self.subTest(invalid_label=invalid_label):
                self.set_app_setting(OFFICIAL_CHECK_AT_KEY, prior_checked_at)
                self.set_app_setting(OFFICIAL_RATE_KEY, "0.10000")
                self.set_app_setting(OFFICIAL_EFFECTIVE_KEY, invalid_label)

                summary, _, _, _, _ = await self.run_sentinel(
                    official=RuntimeError(
                        "raw exception private@example.com shpat_super_secret_token"
                    )
                )

                self.assertFalse(summary.success)
                issue = next(
                    issue
                    for issue in self.issues()
                    if issue.issue_type == "official_tax_source_unavailable"
                )
                evidence = json.loads(issue.raw_payload_json)
                self.assertIs(evidence["stale"], True)
                self.assertIsNone(evidence["prior_checked_at"])
                self.assertIsNone(evidence["prior_rate"])
                self.assertIsNone(evidence["prior_effective_label"])
                self.assertIn(
                    "no prior successful official observation",
                    issue.message.casefold(),
                )
                visible = issue.message + issue.raw_payload_json + repr(summary.errors)
                self.assertNotIn(invalid_label, visible)
                self.assertNotIn("private@example.com", visible)
                self.assertNotIn("shpat_super_secret_token", visible)
                self.assertNotIn("raw exception", visible)

    async def test_any_invalid_prior_component_clears_atomic_triplet(self):
        valid_values = {
            OFFICIAL_CHECK_AT_KEY: (self.now - timedelta(days=2)).isoformat(),
            OFFICIAL_RATE_KEY: "0.10000",
            OFFICIAL_EFFECTIVE_KEY: "April 1, 2026",
        }
        invalid_cases = (
            (OFFICIAL_CHECK_AT_KEY, "not-a-time private@example.com"),
            (OFFICIAL_RATE_KEY, "NaN shpat_super_secret_token"),
        )
        for invalid_key, invalid_value in invalid_cases:
            with self.subTest(invalid_key=invalid_key):
                for key, value in valid_values.items():
                    self.set_app_setting(key, value)
                self.set_app_setting(invalid_key, invalid_value)

                summary, _, _, _, _ = await self.run_sentinel(
                    official=RuntimeError("raw exception private@example.com")
                )

                self.assertFalse(summary.success)
                issue = next(
                    issue
                    for issue in self.issues()
                    if issue.issue_type == "official_tax_source_unavailable"
                )
                evidence = json.loads(issue.raw_payload_json)
                self.assertIsNone(evidence["prior_checked_at"])
                self.assertIsNone(evidence["prior_rate"])
                self.assertIsNone(evidence["prior_effective_label"])
                self.assertIn(
                    "no prior successful official observation",
                    issue.message.casefold(),
                )
                visible = issue.message + issue.raw_payload_json + repr(summary.errors)
                self.assertNotIn(invalid_value, visible)
                self.assertNotIn("raw exception", visible)
                self.assertNotIn("private@example.com", visible)
                self.assertNotIn("shpat_super_secret_token", visible)

    async def test_official_settings_update_only_after_successful_fetch(self):
        await self.run_sentinel()

        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, OFFICIAL_CHECK_AT_KEY).value,
                self.now.isoformat(),
            )
            self.assertEqual(
                session.get(AppSetting, OFFICIAL_RATE_KEY).value,
                "0.10000",
            )
            self.assertEqual(
                session.get(AppSetting, OFFICIAL_EFFECTIVE_KEY).value,
                "April 1, 2026",
            )

        later = self.now + timedelta(days=1)
        self.now = later
        await self.run_sentinel(official=RuntimeError("source down"))

        with Session(self.engine) as session:
            self.assertEqual(
                session.get(AppSetting, OFFICIAL_CHECK_AT_KEY).value,
                (later - timedelta(days=1)).isoformat(),
            )
            self.assertEqual(session.get(AppSetting, OFFICIAL_RATE_KEY).value, "0.10000")

    async def test_official_due_logic_checks_only_for_force_missing_malformed_or_old(self):
        _, missing, _, _, _ = await self.run_sentinel(force_official_check=False)
        missing.assert_awaited_once()

        _, forced, _, _, _ = await self.run_sentinel(force_official_check=True)
        forced.assert_awaited_once()

        self.set_app_setting(OFFICIAL_CHECK_AT_KEY, "not-a-timestamp")
        _, malformed, _, _, _ = await self.run_sentinel(force_official_check=False)
        malformed.assert_awaited_once()

        self.set_app_setting(
            OFFICIAL_CHECK_AT_KEY,
            (self.now - timedelta(days=8)).isoformat(),
        )
        _, old, _, _, _ = await self.run_sentinel(force_official_check=False)
        old.assert_awaited_once()

        skipped_key = self.add_issue(issue_type="official_tax_rate_changed")
        self.set_app_setting(
            OFFICIAL_CHECK_AT_KEY,
            (self.now - timedelta(days=1)).isoformat(),
        )
        summary, recent, _, _, _ = await self.run_sentinel(
            force_official_check=False
        )

        self.assertTrue(summary.success)
        recent.assert_not_awaited()
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[skipped_key].status, "open")

    async def test_future_official_timestamp_is_due(self):
        self.set_app_setting(
            OFFICIAL_CHECK_AT_KEY,
            (self.now + timedelta(seconds=1)).isoformat(),
        )

        summary, official, _, _, _ = await self.run_sentinel(
            force_official_check=False
        )

        self.assertTrue(summary.success)
        official.assert_awaited_once()

    async def test_recent_non_utc_official_timestamp_skips_check(self):
        recent_offset_time = (self.now - timedelta(hours=1)).astimezone(
            timezone(timedelta(hours=-7))
        )
        self.set_app_setting(
            OFFICIAL_CHECK_AT_KEY,
            recent_offset_time.isoformat(),
        )

        summary, official, _, _, _ = await self.run_sentinel(
            force_official_check=False
        )

        self.assertTrue(summary.success)
        official.assert_not_awaited()

    async def test_summary_and_single_log_are_counted_and_redacted(self):
        private_payload = paid_pos_order(
            customer={
                "name": "Private Customer",
                "email": "private@example.com",
                "address": "1 Secret Lane",
            }
        )
        self.add_order(private_payload)
        catalog_error = RuntimeError(
            "shpat_super_secret_token private@example.com 1 Secret Lane"
        )

        summary, _, _, _, output = await self.run_sentinel(
            variants=catalog_error,
            capture_output=True,
        )

        lines = output.strip().splitlines()
        self.assertEqual(len(lines), 1)
        logged = json.loads(lines[0])
        self.assertEqual(logged["runtime"], "test-runtime_shopify_tax")
        self.assertEqual(logged["action"], "audit_once")
        self.assertFalse(logged["success"])
        self.assertEqual(logged["orders_checked"], 1)
        self.assertEqual(logged["variants_checked"], 0)
        self.assertEqual(logged["errors_count"], 1)
        self.assertEqual(
            logged["check_errors"],
            ["catalog check failed (RuntimeError)"],
        )
        started_at = datetime.fromisoformat(logged["started_at"])
        completed_at = datetime.fromisoformat(logged["completed_at"])
        self.assertIsNotNone(started_at.utcoffset())
        self.assertIsNotNone(completed_at.utcoffset())
        self.assertGreaterEqual(completed_at, started_at)
        visible = output + repr(summary) + repr(summary.errors)
        self.assertNotIn("shpat_super_secret_token", visible)
        self.assertNotIn("Private Customer", visible)
        self.assertNotIn("private@example.com", visible)
        self.assertNotIn("1 Secret Lane", visible)
        self.assertNotIn(json.dumps(private_payload), visible)

    async def test_success_summary_log_has_timing_and_empty_check_errors(self):
        summary, _, _, _, output = await self.run_sentinel(capture_output=True)

        self.assertTrue(summary.success)
        lines = output.strip().splitlines()
        self.assertEqual(len(lines), 1)
        logged = json.loads(lines[0])
        self.assertEqual(logged["check_errors"], [])
        self.assertEqual(logged["errors_count"], 0)
        started_at = datetime.fromisoformat(logged["started_at"])
        completed_at = datetime.fromisoformat(logged["completed_at"])
        self.assertIsNotNone(started_at.utcoffset())
        self.assertIsNotNone(completed_at.utcoffset())
        self.assertGreaterEqual(completed_at, started_at)

    async def test_invalid_configuration_is_visible_without_exposing_secret(self):
        summary, official, variants, locations, output = await self.run_sentinel(
            settings=self.settings(
                shopify_pos_expected_tax_rate="NaN",
                shopify_pos_location_id="",
            ),
            capture_output=True,
        )

        self.assertFalse(summary.success)
        self.assertEqual(summary.orders_checked, 0)
        official.assert_not_awaited()
        variants.assert_not_awaited()
        locations.assert_not_awaited()
        issue = self.issues()[0]
        self.assertEqual(issue.issue_type, "sync_error")
        self.assertEqual(json.loads(issue.raw_payload_json)["check"], "configuration")
        self.assertNotIn("shpat_super_secret_token", repr(summary) + issue.raw_payload_json)
        logged = json.loads(output.strip())
        self.assertEqual(
            logged["check_errors"],
            ["configuration check failed (ValueError)"],
        )
        started_at = datetime.fromisoformat(logged["started_at"])
        completed_at = datetime.fromisoformat(logged["completed_at"])
        self.assertIsNotNone(started_at.utcoffset())
        self.assertIsNotNone(completed_at.utcoffset())
        self.assertGreaterEqual(completed_at, started_at)

    async def test_configuration_recovery_resolves_only_sentinel_config_sync_error(self):
        failed, _, _, _, _ = await self.run_sentinel(
            settings=self.settings(shopify_pos_expected_tax_rate="NaN")
        )
        self.assertFalse(failed.success)
        config_error = next(
            issue
            for issue in self.issues()
            if issue.issue_type == "sync_error"
            and json.loads(issue.raw_payload_json).get("check") == "configuration"
        )
        unrelated_key = self.add_issue(
            issue_type="sync_error",
            shopify_product_id="inventory-sync",
            shopify_title="inventory:pull",
        )

        recovered, _, _, _, _ = await self.run_sentinel()

        self.assertTrue(recovered.success)
        issues = {issue.issue_key: issue for issue in self.issues()}
        self.assertEqual(issues[config_error.issue_key].status, "resolved")
        self.assertEqual(issues[unrelated_key].status, "open")
        self.assertEqual(config_error.shopify_title, "tax-sentinel:configuration")

    async def test_config_recovery_persistence_failure_rolls_back_before_sources(self):
        await self.run_sentinel(
            settings=self.settings(shopify_pos_expected_tax_rate="NaN")
        )
        rollback = Mock()
        engine = self.engine

        @contextmanager
        def failing_session_factory():
            with Session(engine) as session:
                original_rollback = session.rollback

                def tracked_rollback():
                    rollback()
                    original_rollback()

                session.rollback = tracked_rollback
                session.exec = Mock(
                    side_effect=OperationalError(
                        "SELECT shopify_sync_issues",
                        {},
                        RuntimeError("database unavailable"),
                    )
                )
                yield session

        official = AsyncMock(return_value=self.official_rate())
        variants = AsyncMock(return_value=[])
        locations = AsyncMock(return_value=self.valid_locations())
        with (
            patch(
                "app.shopify_tax_sentinel.fetch_cdtfa_city_rate",
                new=official,
            ),
            patch(
                "app.shopify_tax_sentinel.fetch_non_taxable_physical_variants",
                new=variants,
            ),
            patch(
                "app.shopify_tax_sentinel.get_shopify_locations",
                new=locations,
            ),
            redirect_stdout(StringIO()),
        ):
            with self.assertRaises(OperationalError):
                await run_shopify_tax_sentinel_once(
                    settings_obj=self.settings(),
                    session_factory=failing_session_factory,
                    now=self.now,
                    force_official_check=True,
                )

        rollback.assert_called_once_with()
        official.assert_not_awaited()
        variants.assert_not_awaited()
        locations.assert_not_awaited()

    async def test_order_lookback_accepts_documented_bounds(self):
        self.assertEqual(MAX_ORDER_LOOKBACK_DAYS, 365)

        for lookback_days in (1, MAX_ORDER_LOOKBACK_DAYS):
            with self.subTest(lookback_days=lookback_days):
                summary, official, variants, locations, _ = await self.run_sentinel(
                    settings=self.settings(
                        shopify_pos_tax_order_lookback_days=lookback_days
                    )
                )

                self.assertTrue(summary.success)
                official.assert_awaited_once()
                variants.assert_awaited_once()
                locations.assert_awaited_once()

    async def test_order_lookback_rejects_out_of_range_before_external_reads(self):
        for lookback_days in (0, MAX_ORDER_LOOKBACK_DAYS + 1, 10**12):
            with self.subTest(lookback_days=lookback_days):
                summary, official, variants, locations, _ = await self.run_sentinel(
                    settings=self.settings(
                        shopify_pos_tax_order_lookback_days=lookback_days
                    )
                )

                self.assertFalse(summary.success)
                official.assert_not_awaited()
                variants.assert_not_awaited()
                locations.assert_not_awaited()


class _FakeResponse:
    def __init__(self, payload=None, *, text="", status_code=200):
        self._payload = payload
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.gets = []
        self.posts = []

    async def get(self, url, **kwargs):
        self.gets.append({"url": url, **kwargs})
        return self.responses.pop(0)

    async def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        return self.responses.pop(0)


if __name__ == "__main__":
    unittest.main()
