import json
from datetime import datetime, timezone

from sqlmodel import Session, SQLModel, create_engine

from app.bank_reconciliation import (
    build_finance_bank_expense_data,
    categorize_bank_payload,
    summarize_bank_transactions,
)
from app.models import BankStatementImport, BankTransaction


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
