import json
from datetime import datetime, timezone

from sqlmodel import Session, SQLModel, create_engine

from app.bank_reconciliation import (
    build_finance_bank_expense_data,
    categorize_bank_payload,
    match_bank_rows_to_transactions,
    summarize_bank_transactions,
)
from app.models import BankStatementImport, BankTransaction, Transaction


class FakeBankRow:
    def __init__(self, amount, expense_category):
        self.amount = amount
        self.expense_category = expense_category
        self.classification = "expense_or_purchase_needs_review"
        self.review_status = "open"


class FakeMatchedTransaction:
    entry_kind = "buy"
    expense_category = "inventory"
    category = "inventory"


def test_categorize_psa_as_grading_fee():
    result = categorize_bank_payload(
        {
            "amount": -934.99,
            "description": "WWW.PSACARD.COM",
            "raw_row_json": json.dumps({"Category": "Merchandise & Inventory"}),
        }
    )

    assert result["expense_category"] == "grading_fees"
    assert result["category_confidence"] == "high"


def test_categorize_zelle_outflow_as_inventory_purchase():
    result = categorize_bank_payload(
        {
            "amount": -5000.00,
            "description": "Zelle payment to Example Seller JPM99abc",
        }
    )

    assert result["expense_category"] == "inventory_purchases"
    assert result["category_confidence"] == "medium"


def test_partner_paybacks_are_not_inventory_purchases():
    for payee in ("Chia Hua Wang", "Chia Wang", "Jeffrey Lee"):
        result = categorize_bank_payload(
            {
                "amount": -5000.00,
                "description": f"Zelle payment to {payee} JPM99abc",
            }
        )

        assert result["expense_category"] == "partner_paybacks"
        assert result["category_confidence"] == "high"


def test_partner_payback_rule_overrides_matched_inventory_transaction():
    result = categorize_bank_payload(
        {
            "amount": -5000.00,
            "description": "Zelle payment to Jeffrey Lee JPM99abc",
        },
        FakeMatchedTransaction(),
    )

    assert result["expense_category"] == "partner_paybacks"


def test_check_outflows_are_payroll():
    result = categorize_bank_payload(
        {
            "amount": -1250.00,
            "description": "CHECK PAID",
            "check_or_slip": "1042",
        }
    )

    assert result["expense_category"] == "payroll"
    assert result["category_confidence"] == "high"


def test_summary_excludes_transfers_from_expense_total():
    summary = summarize_bank_transactions(
        [
            FakeBankRow(-100.0, "shipping_postage"),
            FakeBankRow(-250.0, "transfers"),
            FakeBankRow(-400.0, "loan_owner_payments"),
            FakeBankRow(-50.0, "partner_paybacks"),
        ]
    )

    assert summary["debits"] == -800.0
    assert summary["expense_total"] == 100.0
    assert summary["non_operating_debits"] == 700.0


def test_finance_bank_data_excludes_discord_matches_from_bank_only_totals():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    posted_at = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        session.add(
            BankStatementImport(
                id=1,
                label="Test import",
                account_label="Checking",
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=2,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Logged shipping",
                amount=-100.0,
                classification="logged_in_discord_strong",
                expense_category="shipping_postage",
                matched_transaction_id=123,
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=3,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Bank-only shipping",
                amount=-80.0,
                classification="expense_or_purchase_needs_review",
                expense_category="shipping_postage",
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=4,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Zelle payment to Jeffrey Lee",
                amount=-50.0,
                classification="direct_payment_out_needs_log_check",
                expense_category="partner_paybacks",
            )
        )
        session.commit()

        data = build_finance_bank_expense_data(
            session,
            start=datetime(2026, 5, 1, tzinfo=timezone.utc),
            end=datetime(2026, 5, 2, tzinfo=timezone.utc),
        )

    shipping = next(row for row in data["category_rows"] if row["category"] == "shipping_postage")
    assert data["gross_outflow_total"] == 230.0
    assert data["discord_logged_total"] == 100.0
    assert data["bank_only_total"] == 130.0
    assert data["operating_total"] == 80.0
    assert data["non_operating_total"] == 50.0
    assert shipping["total"] == 180.0
    assert shipping["discord_logged_total"] == 100.0
    assert shipping["bank_only_total"] == 80.0


def test_apple_cash_bank_row_does_not_match_cash_discord_buy():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 15, 12, tzinfo=timezone.utc),
            "description": "PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA 8293",
            "amount": -280.0,
        }
    ]
    cash_buy = Transaction(
        id=1282,
        source_message_id=2600,
        occurred_at=datetime(2026, 5, 10, 5, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="cash",
        expense_category="inventory",
        amount=280.0,
        money_in=0.0,
        money_out=280.0,
        source_content="Bought $280",
    )

    match_bank_rows_to_transactions(bank_rows, [cash_buy])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_source_message_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "direct_payment_out_needs_log_check"


def test_bank_credit_does_not_match_discord_buy_outflow():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
            "description": "Zelle payment from LONG NGUYEN BACovtim2q6e",
            "amount": 185.0,
        }
    ]
    buy = Transaction(
        id=1390,
        source_message_id=2900,
        occurred_at=datetime(2026, 5, 11, 1, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="zelle",
        expense_category="inventory",
        amount=185.0,
        money_in=0.0,
        money_out=185.0,
        source_content="Bought singles for 185 zelle",
    )

    match_bank_rows_to_transactions(bank_rows, [buy])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_source_message_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "direct_customer_payment_needs_log_check"
