from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.shared import (
    MAX_FINANCE_RANGE_DAYS,
    build_finance_daily_rows,
    build_finance_channel_rows,
    build_finance_kpi_drilldown_rows,
    build_finance_kpi_rows,
    build_finance_overall_expense_rows,
    build_finance_quality_rows,
    build_finance_spend_mix_rows,
    build_finance_statement_rows,
    compose_finance_statement,
    resolve_finance_range,
)
from app.models import Transaction
from app.models import ShopifyOrder
from app.models import TikTokOrder


def test_finance_range_accepts_the_maximum_supported_inclusive_span():
    result = resolve_finance_range(
        start="2025-01-01",
        end="2026-01-01",
        window="custom",
    )

    assert result["day_count"] == MAX_FINANCE_RANGE_DAYS == 366


def test_finance_range_reversed_dates_preserve_full_inclusive_boundaries():
    forward = resolve_finance_range(
        start="2025-01-01",
        end="2026-01-01",
        window="custom",
    )
    reversed_range = resolve_finance_range(
        start="2026-01-01",
        end="2025-01-01",
        window="custom",
    )

    assert reversed_range["day_count"] == MAX_FINANCE_RANGE_DAYS
    assert reversed_range["start_dt"] == forward["start_dt"]
    assert reversed_range["end_dt"] == forward["end_dt"]
    assert reversed_range["selected_start"] == "2025-01-01"
    assert reversed_range["selected_end"] == "2026-01-01"


@pytest.mark.parametrize("date_value", ["2026-02-15", "2026-03-08"])
def test_finance_range_same_day_includes_entire_local_day_across_dst(date_value):
    result = resolve_finance_range(
        start=date_value,
        end=date_value,
        window="custom",
    )

    assert result["day_count"] == 1
    assert result["selected_start"] == date_value
    assert result["selected_end"] == date_value
    pacific_start = result["start_dt"].astimezone(ZoneInfo("America/Los_Angeles"))
    pacific_end = result["end_dt"].astimezone(ZoneInfo("America/Los_Angeles"))
    assert (pacific_start.hour, pacific_start.minute, pacific_start.second, pacific_start.microsecond) == (0, 0, 0, 0)
    assert (pacific_end.hour, pacific_end.minute, pacific_end.second, pacific_end.microsecond) == (23, 59, 59, 999999)


@pytest.mark.parametrize("missing_end", [None, "not-a-date"])
def test_finance_range_future_start_with_missing_end_preserves_full_days(missing_end):
    pacific = ZoneInfo("America/Los_Angeles")
    today = datetime.now(pacific).date()
    future = today + timedelta(days=10)

    result = resolve_finance_range(
        start=future.isoformat(),
        end=missing_end,
        window="custom",
    )

    start_local = result["start_dt"].astimezone(pacific)
    end_local = result["end_dt"].astimezone(pacific)
    assert result["selected_start"] == today.isoformat()
    assert result["selected_end"] == future.isoformat()
    assert result["day_count"] == 11
    assert (start_local.hour, start_local.minute, start_local.second, start_local.microsecond) == (0, 0, 0, 0)
    assert (end_local.hour, end_local.minute, end_local.second, end_local.microsecond) == (23, 59, 59, 999999)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("1900-01-01", "2026-01-01"),
        ("2026-01-02", "2025-01-01"),
    ],
)
def test_finance_range_rejects_oversized_forward_and_reversed_spans(start, end):
    with pytest.raises(Exception) as exc_info:
        resolve_finance_range(start=start, end=end, window="custom")

    assert getattr(exc_info.value, "status_code", None) == 400
    assert "366 days" in str(getattr(exc_info.value, "detail", exc_info.value))


def test_finance_statement_uses_assumed_gross_margin_and_keeps_inventory_as_cash_deployed():
    statement = compose_finance_statement(
        discord_summary={
            "rows": 4,
            "totals": {
                "money_in": 1000.0,
                "money_out": 350.0,
                "sales": 1000.0,
                "buys": 250.0,
                "inventory_spend": 300.0,
                "operating_expenses": 50.0,
            },
            "counts": {},
        },
        shopify_summary={"net_revenue": 200.0, "total_tax": 18.0, "paid_orders": 2},
        tiktok_summary={"net_revenue": 100.0, "total_tax": 9.0, "paid_orders": 1},
        bank_expense_data={
            "bank_only_total": 280.0,
            "inventory_total": 40.0,
            "non_operating_total": 70.0,
            "discord_logged_total": 999.0,
        },
        day_count=1,
    )

    assert statement["revenue"] == 1300.0
    assert statement["inventory_spend"] == 340.0
    assert statement["estimated_cogs"] == 1040.0
    assert statement["operating_expenses"] == 220.0
    assert statement["gross_profit"] == 260.0
    assert statement["operating_profit"] == 40.0
    assert statement["gross_margin_pct"] == 20.0
    assert statement["bank_operating_expenses"] == 170.0
    assert statement["bank_inventory_spend"] == 40.0


def test_finance_kpi_rows_link_to_drilldown_sections():
    current = {
        "revenue": 1300.0,
        "gross_profit": 260.0,
        "operating_profit": 40.0,
        "operating_margin_pct": 3.1,
        "inventory_spend": 340.0,
        "external_tax": 27.0,
    }
    prior = {key: 0.0 for key in current}

    rows = build_finance_kpi_rows(current, prior)
    by_key = {row["key"]: row for row in rows}

    assert by_key["revenue"]["drilldown_href"] == "#finance-drilldown-revenue"
    assert by_key["gross_profit"]["drilldown_href"] == "#finance-drilldown-gross-profit"
    assert by_key["operating_profit"]["drilldown_href"] == "#finance-drilldown-operating-profit"
    assert by_key["inventory_spend"]["drilldown_href"] == "#finance-drilldown-inventory-spend"


def test_finance_kpi_drilldowns_explain_margin_model_and_actions():
    statement = {
        "discord_revenue": 1000.0,
        "shopify_net_revenue": 200.0,
        "tiktok_net_revenue": 100.0,
        "revenue": 1300.0,
        "estimated_cogs": 1040.0,
        "gross_profit": 260.0,
        "discord_inventory_spend": 300.0,
        "bank_inventory_spend": 40.0,
        "inventory_spend": 340.0,
        "discord_operating_expenses": 50.0,
        "bank_operating_expenses": 170.0,
        "operating_expenses": 220.0,
        "operating_profit": 40.0,
        "operating_margin_pct": 3.1,
        "shopify_tax": 18.0,
        "tiktok_tax": 9.0,
        "external_tax": 27.0,
    }
    range_data = {"selected_start": "2026-05-01", "selected_end": "2026-05-23"}

    rows = build_finance_kpi_drilldown_rows(statement, range_data=range_data)
    by_key = {row["key"]: row for row in rows}

    gross_items = [item["label"] for item in by_key["gross_profit"]["items"]]
    operating_items = [item["label"] for item in by_key["operating_profit"]["items"]]
    inventory = by_key["inventory_spend"]

    assert by_key["gross_profit"]["title"] == "Estimated Gross Profit"
    assert "20% gross product margin" in by_key["gross_profit"]["body"]
    assert "Estimated product COGS (80%)" in gross_items
    assert "Estimated gross product profit (20%)" in gross_items
    assert "Operating expenses" in operating_items
    assert inventory["action_url"] == "/bookkeeping/bank?expense_category=inventory"
    assert by_key["revenue"]["action_url"] == "/reports?start=2026-05-01&end=2026-05-23"


def test_finance_kpi_drilldowns_include_real_supporting_rows():
    occurred_at = datetime(2026, 5, 2, 18, tzinfo=timezone.utc)
    statement = {
        "discord_revenue": 1000.0,
        "shopify_net_revenue": 200.0,
        "tiktok_net_revenue": 100.0,
        "revenue": 1300.0,
        "estimated_cogs": 1040.0,
        "gross_profit": 260.0,
        "discord_inventory_spend": 300.0,
        "bank_inventory_spend": 40.0,
        "inventory_spend": 340.0,
        "discord_operating_expenses": 50.0,
        "bank_operating_expenses": 170.0,
        "operating_expenses": 220.0,
        "operating_profit": 40.0,
        "operating_margin_pct": 3.1,
        "shopify_tax": 18.0,
        "tiktok_tax": 9.0,
        "external_tax": 27.0,
    }
    rows = build_finance_kpi_drilldown_rows(
        statement,
        range_data={"selected_start": "2026-05-01", "selected_end": "2026-05-23"},
        transactions=[
            Transaction(
                id=1,
                source_message_id=101,
                occurred_at=occurred_at,
                channel_name="Discord Financials",
                author_name="Jeffrey",
                entry_kind="sale",
                money_in=1000.0,
                money_out=0.0,
                source_content="Sold sealed case",
            ),
            Transaction(
                id=2,
                source_message_id=102,
                occurred_at=occurred_at,
                channel_name="Loans",
                entry_kind="loan_draw",
                money_in=5000.0,
                money_out=0.0,
                expense_category="loan_owner_payments",
                source_content="Owner loan",
            ),
            Transaction(
                id=3,
                source_message_id=103,
                occurred_at=occurred_at,
                channel_name="Discord Financials",
                entry_kind="buy",
                money_in=0.0,
                money_out=300.0,
                source_content="Bought inventory",
            ),
            Transaction(
                id=4,
                source_message_id=104,
                occurred_at=occurred_at,
                channel_name="Discord Financials",
                entry_kind="expense",
                money_in=0.0,
                money_out=50.0,
                expense_category="supplies",
                source_content="Shipping supplies",
            ),
        ],
        shopify_rows=[
            ShopifyOrder(
                id=10,
                shopify_order_id="gid://shopify/Order/10",
                order_number="#1001",
                created_at=occurred_at,
                updated_at=occurred_at,
                customer_name="Shopify Buyer",
                total_price=218.0,
                subtotal_price=200.0,
                total_tax=18.0,
                financial_status="paid",
            )
        ],
        tiktok_rows=[
            TikTokOrder(
                id=20,
                tiktok_order_id="tt-20",
                order_number="TT1001",
                created_at=occurred_at,
                updated_at=occurred_at,
                customer_name="TikTok Buyer",
                total_price=109.0,
                subtotal_price=100.0,
                total_tax=9.0,
                financial_status="completed",
            )
        ],
        bank_expense_data={
            "detail_rows": [
                {
                    "id": 201,
                    "date": "2026-05-02",
                    "description": "PSA grading",
                    "account_label": "Chase Checking",
                    "amount": 40.0,
                    "category_label": "Grading fees",
                    "group": "operating",
                    "is_discord_logged": False,
                    "is_non_operating": False,
                },
                {
                    "id": 202,
                    "date": "2026-05-02",
                    "description": "Software subscription",
                    "account_label": "Credit Card",
                    "amount": 170.0,
                    "category_label": "Software",
                    "group": "operating",
                    "is_discord_logged": False,
                    "is_non_operating": False,
                },
                {
                    "id": 203,
                    "date": "2026-05-02",
                    "description": "Already logged buy",
                    "account_label": "Chase Checking",
                    "amount": 999.0,
                    "category_label": "Inventory",
                    "group": "inventory",
                    "is_discord_logged": True,
                    "is_non_operating": False,
                },
            ]
        },
        row_limit=10,
    )
    by_key = {row["key"]: row for row in rows}

    revenue_sources = {row["source"] for row in by_key["revenue"]["supporting_rows"]}
    inventory_labels = [row["description"] for row in by_key["inventory_spend"]["supporting_rows"]]
    operating_labels = [row["description"] for row in by_key["operating_profit"]["supporting_rows"]]
    tax_sources = {row["source"] for row in by_key["external_tax"]["supporting_rows"]}

    assert revenue_sources == {"Discord", "Shopify", "TikTok"}
    assert all("Owner loan" not in row["description"] for row in by_key["revenue"]["supporting_rows"])
    assert "Bought inventory" in inventory_labels
    assert "PSA grading" not in inventory_labels
    assert "Already logged buy" not in inventory_labels
    assert "PSA grading" in operating_labels
    assert "Shipping supplies" in operating_labels
    assert "Software subscription" in operating_labels
    assert tax_sources == {"Shopify", "TikTok"}
    assert by_key["inventory_spend"]["supporting_row_count"] == 1


def test_finance_quality_rows_have_direct_action_links():
    rows = build_finance_quality_rows(
        current_statement={
            "discord_rows": 571,
            "review_required": 39,
            "shopify_paid_orders": 1657,
            "tiktok_paid_orders": 6787,
            "tax_unknown_orders": 0,
        },
        range_data={
            "day_count": 23,
            "label": "May 01 - May 23, 2026",
            "selected_start": "2026-05-01",
            "selected_end": "2026-05-23",
        },
    )
    by_label = {row["label"]: row for row in rows}

    assert by_label["Range length"]["action_url"] == "#finance-range-controls"
    assert by_label["Discord rows"]["action_label"] == "Review rows"
    assert "/review-table" in by_label["Discord rows"]["action_url"]
    assert "after=2026-05-01" in by_label["Discord rows"]["action_url"]
    assert by_label["Paid platform orders"]["action_url"] == "/reports?start=2026-05-01&end=2026-05-23"
    assert by_label["Tax completeness"]["action_label"] == "View tax detail"


def test_finance_statement_excludes_discord_non_operating_money_in_from_revenue():
    statement = compose_finance_statement(
        discord_summary={
            "rows": 2,
            "totals": {
                "money_in": 6000.0,
                "non_operating_money_in": 5000.0,
                "money_out": 0.0,
                "sales": 1000.0,
            },
            "counts": {},
        },
        shopify_summary={"net_revenue": 200.0, "total_tax": 18.0, "paid_orders": 2},
        tiktok_summary={"net_revenue": 100.0, "total_tax": 9.0, "paid_orders": 1},
        bank_expense_data={},
        day_count=1,
    )

    assert statement["discord_gross_money_in"] == 6000.0
    assert statement["discord_non_operating_money_in"] == 5000.0
    assert statement["discord_revenue"] == 1000.0
    assert statement["revenue"] == 1300.0


def test_finance_breakdowns_label_discord_and_bank_only_outflows_separately():
    current = {
        "discord_revenue": 1000.0,
        "shopify_net_revenue": 200.0,
        "tiktok_net_revenue": 100.0,
        "revenue": 1300.0,
        "estimated_cogs": 1040.0,
        "discord_non_operating_money_in": 5000.0,
        "discord_inventory_spend": 300.0,
        "bank_inventory_spend": 40.0,
        "inventory_spend": 340.0,
        "gross_profit": 260.0,
        "discord_operating_expenses": 50.0,
        "bank_operating_expenses": 170.0,
        "operating_expenses": 220.0,
        "operating_profit": 40.0,
        "operating_margin_pct": 3.1,
    }
    prior = {key: 0.0 for key in current}

    statement_labels = [row["label"] for row in build_finance_statement_rows(current, prior)]
    spend_labels = [row["label"] for row in build_finance_spend_mix_rows(current)]

    assert "Discord operating cash-in sales" in statement_labels
    assert "Discord non-operating cash in excluded" in statement_labels
    assert "Estimated product COGS (80%)" in statement_labels
    assert "Estimated gross product profit (20%)" in statement_labels
    assert "Bank-only inventory outflow" in statement_labels
    assert "Bank-only operating outflow" in statement_labels
    assert "Bank-only inventory" in spend_labels
    assert "Bank-only operating outflow" in spend_labels


def test_finance_overall_expense_rows_combine_discord_and_bank_only_categories():
    occurred_at = datetime(2026, 5, 2, 18, tzinfo=timezone.utc)

    rows = build_finance_overall_expense_rows(
        transactions=[
            Transaction(
                id=1,
                source_message_id=101,
                occurred_at=occurred_at,
                entry_kind="buy",
                money_out=300.0,
                expense_category="inventory",
            ),
            Transaction(
                id=2,
                source_message_id=102,
                occurred_at=occurred_at,
                entry_kind="expense",
                money_out=50.0,
                expense_category="supplies",
            ),
            Transaction(
                id=3,
                source_message_id=103,
                occurred_at=occurred_at,
                entry_kind="transfer",
                money_out=70.0,
                expense_category="transfers",
            ),
        ],
        bank_expense_data={
            "detail_rows": [
                {
                    "amount": 40.0,
                    "category": "inventory",
                    "category_label": "Inventory",
                    "group": "inventory",
                    "is_discord_logged": False,
                    "is_non_operating": False,
                },
                {
                    "amount": 170.0,
                    "category": "supplies",
                    "category_label": "Supplies",
                    "group": "operating",
                    "is_discord_logged": False,
                    "is_non_operating": False,
                },
                {
                    "amount": 999.0,
                    "category": "inventory",
                    "category_label": "Inventory",
                    "group": "inventory",
                    "is_discord_logged": True,
                    "is_non_operating": False,
                },
                {
                    "amount": 20.0,
                    "category": "partner_paybacks",
                    "category_label": "Partner Paybacks",
                    "group": "partner_paybacks",
                    "is_discord_logged": False,
                    "is_non_operating": True,
                },
                {
                    "amount": 1000.0,
                    "category": "transfers",
                    "category_label": "Bank/credit-card transfers",
                    "group": "non_operating",
                    "is_discord_logged": False,
                    "is_non_operating": True,
                    "excluded_from_finance": True,
                },
                {
                    "amount": 500.0,
                    "category": "cash_inventory_purchases",
                    "category_label": "Cash inventory purchases",
                    "group": "cash_movement",
                    "is_discord_logged": False,
                    "is_non_operating": False,
                    "excluded_from_finance": True,
                },
            ]
        },
        range_data={"selected_start": "2026-05-01", "selected_end": "2026-05-23"},
    )
    by_key = {row["key"]: row for row in rows}

    assert by_key["inventory"]["total"] == 340.0
    assert by_key["inventory"]["discord_total"] == 300.0
    assert by_key["inventory"]["bank_only_total"] == 40.0
    assert by_key["inventory"]["row_count"] == 2
    assert by_key["inventory"]["group"] == "inventory"
    assert by_key["supplies"]["total"] == 220.0
    assert by_key["supplies"]["discord_total"] == 50.0
    assert by_key["supplies"]["bank_only_total"] == 170.0
    assert by_key["partner_paybacks"]["total"] == 20.0
    assert "transfers" not in by_key
    assert "cash_inventory_purchases" not in by_key
    assert all(row["key"] != "already_logged_buy" for row in rows)


def test_finance_daily_rows_keep_inventory_cash_out_outside_profit():
    rows = build_finance_daily_rows(
        transactions=[
            Transaction(
                occurred_at=datetime(2026, 5, 1, 18, tzinfo=timezone.utc),
                channel_id="sales",
                channel_name="Sales",
                entry_kind="sale",
                amount=1000.0,
                money_in=1000.0,
                money_out=0.0,
            ),
        ],
        shopify_rows=[],
        tiktok_rows=[],
        bank_daily_rows=[
            {
                "date": "2026-05-01",
                "inventory": 40.0,
                "operating": 90.0,
                "uncategorized": 80.0,
                "non_operating": 70.0,
                "partner_paybacks": 20.0,
                "already_logged": 500.0,
            }
        ],
        start=datetime(2026, 5, 1, 7, tzinfo=timezone.utc),
        end=datetime(2026, 5, 2, 6, 59, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert rows[0]["inventory_spend"] == 40.0
    assert rows[0]["operating_expenses"] == 170.0
    assert rows[0]["estimated_cogs"] == 800.0
    assert rows[0]["gross_profit"] == 200.0
    assert rows[0]["operating_profit"] == 30.0


def test_finance_daily_rows_exclude_discord_non_operating_money_in_from_revenue():
    rows = build_finance_daily_rows(
        transactions=[
            Transaction(
                occurred_at=datetime(2026, 5, 1, 18, tzinfo=timezone.utc),
                channel_id="sales",
                channel_name="Sales",
                entry_kind="sale",
                amount=1000.0,
                money_in=1000.0,
                money_out=0.0,
            ),
            Transaction(
                occurred_at=datetime(2026, 5, 1, 19, tzinfo=timezone.utc),
                channel_id="loans",
                channel_name="Loans",
                entry_kind="loan_draw",
                amount=5000.0,
                money_in=5000.0,
                money_out=0.0,
                expense_category="loan_owner_payments",
            ),
        ],
        shopify_rows=[],
        tiktok_rows=[],
        start=datetime(2026, 5, 1, 7, tzinfo=timezone.utc),
        end=datetime(2026, 5, 2, 6, 59, tzinfo=timezone.utc),
    )

    assert rows[0]["discord_revenue"] == 1000.0
    assert rows[0]["revenue"] == 1000.0
    assert rows[0]["activity_count"] == 1


def test_finance_channel_rows_exclude_non_operating_transactions():
    rows = build_finance_channel_rows(
        [
            Transaction(
                occurred_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
                channel_id="sales",
                channel_name="Sales",
                entry_kind="sale",
                amount=1000.0,
                money_in=1000.0,
                money_out=0.0,
            ),
            Transaction(
                occurred_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
                channel_id="loans",
                channel_name="Loans",
                entry_kind="loan_draw",
                amount=5000.0,
                money_in=5000.0,
                money_out=0.0,
                expense_category="loan_owner_payments",
            ),
        ]
    )

    assert [row["label"] for row in rows] == ["Sales"]
    assert rows[0]["money_in_display"] == "$1,000"
