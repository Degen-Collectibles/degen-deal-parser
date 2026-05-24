from datetime import datetime, timezone

from app.shared import (
    build_finance_daily_rows,
    build_finance_channel_rows,
    build_finance_kpi_drilldown_rows,
    build_finance_kpi_rows,
    build_finance_quality_rows,
    build_finance_spend_mix_rows,
    build_finance_statement_rows,
    compose_finance_statement,
)
from app.models import Transaction


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
    assert "Bank-only inventory/grading outflow" in statement_labels
    assert "Bank-only operating outflow" in statement_labels
    assert "Bank-only inventory/grading" in spend_labels
    assert "Bank-only operating outflow" in spend_labels


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
