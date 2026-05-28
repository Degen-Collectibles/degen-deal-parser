import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

from app.discord.financials import compute_financials
from app.discord.parser import TimedOutRowError, parse_message
from app.discord.transactions import build_transaction_summary, sync_transaction_from_message
from sqlmodel import select

from app.models import DiscordMessage, PARSE_REVIEW_REQUIRED, Transaction


def _parse_financial_message(text: str, *, channel_name: str, attachments: list[str] | None = None) -> dict:
    with patch("app.discord.parser.get_exact_correction_match", return_value=None), patch(
        "app.discord.parser.get_learned_rule_match",
        return_value=(None, None),
    ), patch(
        "app.discord.parser.parse_deal_with_ai_async",
        side_effect=AssertionError("financial channel parser should not call deal AI"),
    ):
        return asyncio.run(
            parse_message(
                text,
                attachments or [],
                author_name="tester",
                channel_name=channel_name,
            )
        )


def test_loans_channel_logs_principal_draw_as_non_operating_cash_in() -> None:
    parsed = _parse_financial_message(
        "Take out $5,000 loan for Florida Guy Lot",
        channel_name="loans",
    )

    assert parsed["parsed_type"] == "loan_draw"
    assert parsed["parsed_amount"] == 5000.0
    assert parsed["parsed_category"] == "loan_owner_payments"
    assert parsed["needs_review"] is False

    financials = compute_financials(
        parsed_type=parsed["parsed_type"],
        parsed_category=parsed["parsed_category"],
        amount=parsed["parsed_amount"],
        cash_direction=parsed["parsed_cash_direction"],
        message_text="Take out $5,000 loan for Florida Guy Lot",
    )

    assert financials.entry_kind == "loan_draw"
    assert financials.money_in == 5000.0
    assert financials.money_out == 0.0
    assert financials.expense_category == "loan_owner_payments"


def test_loans_channel_logs_principal_payback_without_expense_category_pollution() -> None:
    parsed = _parse_financial_message("Paid back 2500 loan", channel_name="loans")

    assert parsed["parsed_type"] == "loan_repayment"
    assert parsed["parsed_amount"] == 2500.0
    assert parsed["parsed_category"] == "loan_owner_payments"

    financials = compute_financials(
        parsed_type=parsed["parsed_type"],
        parsed_category=parsed["parsed_category"],
        amount=parsed["parsed_amount"],
        cash_direction=parsed["parsed_cash_direction"],
        message_text="Paid back 2500 loan",
    )

    assert financials.entry_kind == "loan_repayment"
    assert financials.money_in == 0.0
    assert financials.money_out == 2500.0
    assert financials.expense_category == "loan_owner_payments"


def test_loans_channel_logs_interest_as_real_expense() -> None:
    parsed = _parse_financial_message(
        "500 pay interest. Will be taken out 5/1",
        channel_name="loans",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 500.0
    assert parsed["parsed_category"] == "loan_interest"

    financials = compute_financials(
        parsed_type=parsed["parsed_type"],
        parsed_category=parsed["parsed_category"],
        amount=parsed["parsed_amount"],
        cash_direction=parsed["parsed_cash_direction"],
        message_text="500 pay interest. Will be taken out 5/1",
    )

    assert financials.entry_kind == "expense"
    assert financials.money_out == 500.0
    assert financials.expense_category == "loan_interest"


def test_financials_channel_logs_payroll_and_show_costs_as_expenses() -> None:
    payroll = _parse_financial_message(
        "Pay Sam payroll of may 300$",
        channel_name="financials",
    )
    show_cost = _parse_financial_message(
        "Johnny paid 1100 for 2 front row sd tables",
        channel_name="financials",
    )

    assert payroll["parsed_type"] == "expense"
    assert payroll["parsed_amount"] == 300.0
    assert payroll["parsed_category"] == "payroll"
    assert payroll["needs_review"] is False

    assert show_cost["parsed_type"] == "expense"
    assert show_cost["parsed_amount"] == 1100.0
    assert show_cost["parsed_category"] == "show_fees"
    assert show_cost["needs_review"] is False


def test_financials_channel_sums_multi_payee_payroll_rows_without_total() -> None:
    parsed = _parse_financial_message(
        "Pay cow bow 200 Marsh 300 Dat 450 For show",
        channel_name="financials",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 950.0
    assert parsed["parsed_category"] == "payroll"
    assert parsed["needs_review"] is False


def test_financials_channel_treats_show_payee_amount_lists_as_payroll() -> None:
    parsed = _parse_financial_message(
        "San Jose card show 18-19 Dat 600 Peter 400 Ranz 300 Mars 150",
        channel_name="financials",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 1450.0
    assert parsed["parsed_category"] == "payroll"
    assert parsed["needs_review"] is False


def test_financials_channel_sums_payee_rows_when_show_words_sit_between_name_and_amount() -> None:
    parsed = _parse_financial_message(
        "Pay ranz show 150 Dat 400 East bay show Jan 3-4",
        channel_name="financials",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 550.0
    assert parsed["parsed_category"] == "payroll"
    assert parsed["needs_review"] is False


def test_financials_channel_sums_payee_rows_with_for_show_between_name_and_amount() -> None:
    parsed = _parse_financial_message(
        "Pay dat for show 680 500$ Pay rocky 200$",
        channel_name="financials",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 1380.0
    assert parsed["parsed_category"] == "payroll"
    assert parsed["needs_review"] is False


def test_financials_channel_multiplies_each_amount_by_named_payees() -> None:
    parsed = _parse_financial_message(
        "Pay marsh and Brandon for Dan jose show Sunday 250 each!",
        channel_name="financials",
    )

    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 500.0
    assert parsed["parsed_category"] == "payroll"
    assert parsed["needs_review"] is False


def test_loans_channel_does_not_sum_product_quantity_numbers_into_loan_amount() -> None:
    parsed = _parse_financial_message(
        "take out $8486.90 loan for 100 mega dream and 1 case op15",
        channel_name="loans",
    )

    assert parsed["parsed_type"] == "loan_draw"
    assert parsed["parsed_amount"] == 8486.9
    assert parsed["parsed_category"] == "loan_owner_payments"


def test_financials_channel_marks_statement_links_as_ignored_evidence_not_transactions() -> None:
    parsed = _parse_financial_message(
        "Red Tab is the Total Financials up to 9/30(May-September 2025)\nhttps://1drv.ms/example",
        channel_name="financials",
    )

    assert parsed["ignore_message"] is True
    assert "statement" in parsed["parsed_notes"]


def test_financials_channel_ignores_informational_policy_notes_without_ai() -> None:
    parsed = _parse_financial_message(
        "For all sales, assume COGS is 85% less than revenue.",
        channel_name="financials",
    )

    assert parsed["ignore_message"] is True
    assert "informational note" in parsed["parsed_notes"]


def test_financials_channel_marks_ambiguous_attachment_rows_for_review() -> None:
    with patch(
        "app.discord.parser.parse_financial_image_with_ai_async",
        return_value={"parsed_amount": None},
    ):
        parsed = _parse_financial_message(
            "",
            channel_name="financials",
            attachments=["https://cdn.discordapp.com/attachments/example.png"],
        )

    assert parsed["parsed_type"] == "unknown"
    assert parsed["parsed_amount"] is None
    assert parsed["needs_review"] is True
    assert "needs review" in parsed["parsed_notes"]


def test_financials_channel_uses_image_ai_when_amount_is_only_in_attachment() -> None:
    image_parse = {
        "parsed_type": "expense",
        "parsed_amount": 1200.0,
        "parsed_payment_method": "check",
        "parsed_cash_direction": None,
        "parsed_category": "taxes_licenses",
        "parsed_items": [],
        "parsed_items_in": [],
        "parsed_items_out": [],
        "parsed_trade_summary": "",
        "parsed_notes": "image shows April check for 1200",
        "image_summary": "check image with visible $1,200.00 amount",
        "confidence": 0.84,
        "needs_review": True,
    }
    with patch("app.discord.parser.get_exact_correction_match", return_value=None), patch(
        "app.discord.parser.get_learned_rule_match",
        return_value=(None, None),
    ), patch(
        "app.discord.parser.parse_deal_with_ai_async",
        side_effect=AssertionError("financial channel image parser should not call deal AI"),
    ), patch(
        "app.discord.parser.parse_financial_image_with_ai_async",
        return_value=image_parse,
        create=True,
    ) as image_ai:
        parsed = asyncio.run(
            parse_message(
                "April check!",
                ["https://cdn.discordapp.com/attachments/april-check.png"],
                author_name="tester",
                channel_name="financials",
            )
        )

    assert image_ai.called
    assert parsed["parsed_type"] == "expense"
    assert parsed["parsed_amount"] == 1200.0
    assert parsed["parsed_payment_method"] == "check"
    assert parsed["parsed_category"] == "taxes_licenses"
    assert parsed["needs_review"] is True
    assert "image amount" in parsed["parsed_notes"].lower()


def test_financials_channel_image_ai_timeout_keeps_review_fallback() -> None:
    with patch("app.discord.parser.get_exact_correction_match", return_value=None), patch(
        "app.discord.parser.get_learned_rule_match",
        return_value=(None, None),
    ), patch(
        "app.discord.parser.parse_deal_with_ai_async",
        side_effect=AssertionError("financial channel image parser should not call deal AI"),
    ), patch(
        "app.discord.parser.parse_financial_image_with_ai_async",
        side_effect=TimedOutRowError("vision timeout"),
    ):
        parsed = asyncio.run(
            parse_message(
                "April check!",
                ["https://cdn.discordapp.com/attachments/april-check.png"],
                author_name="tester",
                channel_name="financials",
            )
        )

    assert parsed["parsed_type"] == "unknown"
    assert parsed["parsed_amount"] is None
    assert parsed["needs_review"] is True
    assert "needs review" in parsed["parsed_notes"]


def test_financials_channel_preserves_review_row_category_when_amount_missing() -> None:
    parsed = _parse_financial_message("Pay rent (may)", channel_name="financials")

    assert parsed["parsed_type"] == "unknown"
    assert parsed["parsed_category"] == "rent_facilities"
    assert parsed["needs_review"] is True

    financials = compute_financials(
        parsed_type=parsed["parsed_type"],
        parsed_category=parsed["parsed_category"],
        amount=parsed["parsed_amount"],
        cash_direction=parsed["parsed_cash_direction"],
        message_text="Pay rent (may)",
    )

    assert financials.entry_kind == "unknown"
    assert financials.expense_category == "rent_facilities"


def test_sync_flags_parsed_sale_without_amount_for_review() -> None:
    from sqlmodel import Session, SQLModel, create_engine

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        row = DiscordMessage(
            discord_message_id="missing-amount-sale",
            channel_id="sales",
            channel_name="store-sales-and-trades",
            author_name="tester",
            content="sold slab zelle amount unknown",
            created_at=datetime(2026, 5, 20, 12, tzinfo=timezone.utc),
            parse_status="parsed",
            deal_type="sell",
            amount=None,
            payment_method="zelle",
            needs_review=False,
        )
        session.add(row)
        session.commit()
        session.refresh(row)

        sync_transaction_from_message(session, row)
        session.commit()
        session.refresh(row)
        tx = session.exec(select(Transaction).where(Transaction.source_message_id == row.id)).one()

    assert row.parse_status == PARSE_REVIEW_REQUIRED
    assert row.needs_review is True
    assert tx.needs_review is True
    assert tx.money_in == 0.0


def test_sync_flags_trade_amount_without_cash_direction_for_review() -> None:
    from sqlmodel import Session, SQLModel, create_engine

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        row = DiscordMessage(
            discord_message_id="trade-missing-direction",
            channel_id="sales",
            channel_name="store-sales-and-trades",
            author_name="tester",
            content="trade plus 200 zelle direction unclear",
            created_at=datetime(2026, 5, 20, 12, tzinfo=timezone.utc),
            parse_status="parsed",
            deal_type="trade",
            amount=200.0,
            payment_method="zelle",
            cash_direction=None,
            needs_review=False,
        )
        session.add(row)
        session.commit()
        session.refresh(row)

        sync_transaction_from_message(session, row)
        session.commit()
        session.refresh(row)
        tx = session.exec(select(Transaction).where(Transaction.source_message_id == row.id)).one()

    assert row.parse_status == PARSE_REVIEW_REQUIRED
    assert row.needs_review is True
    assert tx.needs_review is True
    assert tx.money_in == 0.0
    assert tx.money_out == 0.0


def test_card_deal_financials_default_to_inventory_category() -> None:
    for parsed_type in ("sell", "buy", "trade"):
        financials = compute_financials(
            parsed_type=parsed_type,
            parsed_category=None,
            amount=25.0,
            cash_direction="to_store",
            message_text="card deal 25 zelle",
        )

        assert financials.expense_category == "inventory"


def test_loan_principal_is_not_counted_as_sales_or_expense_net() -> None:
    occurred_at = datetime(2026, 5, 20, tzinfo=timezone.utc)
    rows = [
        Transaction(
            occurred_at=occurred_at,
            entry_kind="loan_draw",
            amount=5000.0,
            money_in=5000.0,
            money_out=0.0,
            expense_category="loan_owner_payments",
        ),
        Transaction(
            occurred_at=occurred_at,
            entry_kind="loan_repayment",
            amount=2500.0,
            money_in=0.0,
            money_out=2500.0,
            expense_category="loan_owner_payments",
        ),
        Transaction(
            occurred_at=occurred_at,
            entry_kind="sale",
            amount=10000.0,
            money_in=10000.0,
            money_out=0.0,
            expense_category="loan_proceeds",
        ),
        Transaction(
            occurred_at=occurred_at,
            entry_kind="expense",
            amount=300.0,
            money_in=0.0,
            money_out=300.0,
            expense_category="loan_interest",
        ),
    ]

    summary = build_transaction_summary(rows)

    assert summary["totals"]["sales"] == 0.0
    assert summary["totals"]["expenses"] == 300.0
    assert summary["totals"]["net"] == -300.0
    assert summary["totals"]["non_operating_money_in"] == 15000.0
    assert summary["totals"]["non_operating_money_out"] == 2500.0
