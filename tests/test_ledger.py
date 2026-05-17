import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.bank_reconciliation import rerun_bank_reconciliation
from app.ledger import (
    LedgerFilters,
    apply_ledger_rule,
    build_ledger_page_data,
    draft_ledger_rule_from_instruction,
    preview_ledger_rule,
)
from app.models import BankStatementImport, BankTransaction, LedgerRule, Transaction
from app.routers.ledger import ledger_page, ledger_row_status_form


def make_request(path: str, role: str = "admin", *, method: str = "GET", headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    request = Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": headers or [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        username="tester",
        display_name="Test Operator",
        role=role,
    )
    return request


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def add_import(session: Session) -> BankStatementImport:
    row = BankStatementImport(
        label="Chase feed",
        account_label="Chase Checking",
        account_type="checking",
        source_kind="plaid",
        provider="plaid",
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def test_ledger_builder_counts_bank_rows_and_separates_unbanked_cash():
    engine = make_engine()
    posted_at = datetime(2026, 5, 15, 12, tzinfo=timezone.utc)
    cash_at = datetime(2026, 5, 14, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        bank_import = add_import(session)
        matched_tx = Transaction(
            id=500,
            source_message_id=1500,
            occurred_at=posted_at,
            parse_status="parsed",
            entry_kind="buy",
            payment_method="zelle",
            expense_category="inventory",
            amount=250.0,
            money_in=0.0,
            money_out=250.0,
            source_content="buy inventory 250 zelle",
        )
        cash_tx = Transaction(
            id=501,
            source_message_id=1501,
            occurred_at=cash_at,
            parse_status="parsed",
            entry_kind="buy",
            payment_method="cash",
            expense_category="inventory",
            amount=90.0,
            money_in=0.0,
            money_out=90.0,
            source_content="buy inventory 90 cash",
        )
        session.add(matched_tx)
        session.add(cash_tx)
        session.add(
            BankTransaction(
                id=10,
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Zelle payment to inventory seller",
                amount=-250.0,
                classification="logged_in_discord_strong",
                confidence="high",
                expense_category="inventory_purchases",
                matched_transaction_id=500,
                matched_source_message_id=1500,
                matched_platform="discord",
            )
        )
        session.add(
            BankTransaction(
                id=11,
                import_id=bank_import.id,
                row_index=2,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="SHOPIFY PAYOUT 123",
                amount=600.0,
                classification="shopify_payout",
                confidence="high",
                expense_category="platform_payouts",
                matched_platform="shopify",
            )
        )
        session.add(
            BankTransaction(
                id=12,
                import_id=bank_import.id,
                row_index=3,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA",
                amount=-280.0,
                classification="direct_payment_out_needs_log_check",
                confidence="medium",
                expense_category="inventory_purchases",
            )
        )
        session.commit()

        data = build_ledger_page_data(session, LedgerFilters(status="all"))

    rows_by_id = {row["id"]: row for row in data["rows"]}
    assert data["summary"]["bank_row_count"] == 3
    assert data["summary"]["bank_net_total"] == 70.0
    assert rows_by_id[10]["source"] == "discord"
    assert rows_by_id[10]["ledger_status"] == "reconciled"
    assert rows_by_id[11]["source"] == "shopify"
    assert rows_by_id[11]["ledger_status"] == "reconciled"
    assert rows_by_id[12]["ledger_status"] == "needs_action"
    assert data["unbanked_cash_rows"][0]["transaction_id"] == 501
    assert data["summary"]["unbanked_cash_total"] == 90.0


def test_rule_draft_preview_and_apply_updates_only_matching_rows():
    engine = make_engine()
    posted_at = datetime(2026, 5, 15, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        bank_import = add_import(session)
        session.add(
            BankTransaction(
                id=20,
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA",
                amount=-280.0,
                classification="direct_payment_out_needs_log_check",
                expense_category="uncategorized",
            )
        )
        session.add(
            BankTransaction(
                id=21,
                import_id=bank_import.id,
                row_index=2,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="WWW.PSACARD.COM",
                amount=-140.0,
                classification="expense_or_purchase_needs_review",
                expense_category="uncategorized",
            )
        )
        session.commit()

        draft = draft_ledger_rule_from_instruction(
            "Always categorize Apple Cash sent as inventory purchases and mark reviewed"
        )
        preview = preview_ledger_rule(
            session,
            conditions=draft["conditions"],
            actions=draft["actions"],
            filters=LedgerFilters(status="all"),
        )
        rule = LedgerRule(
            name=draft["name"],
            conditions_json=json.dumps(draft["conditions"]),
            actions_json=json.dumps(draft["actions"]),
        )
        session.add(rule)
        session.commit()
        session.refresh(rule)
        applied = apply_ledger_rule(session, rule, filters=LedgerFilters(status="all"), applied_by="tester")
        apple = session.get(BankTransaction, 20)
        psa = session.get(BankTransaction, 21)

    assert preview["affected_count"] == 1
    assert preview["sample_rows"][0]["id"] == 20
    assert applied["updated_count"] == 1
    assert apple.expense_category == "inventory_purchases"
    assert apple.category_confidence == "rule"
    assert apple.review_status == "reviewed"
    assert psa.expense_category == "uncategorized"
    assert psa.review_status == "open"


def test_rule_draft_can_force_unmatch_rows_from_discord():
    draft = draft_ledger_rule_from_instruction("Unmatch Apple Cash rows from Discord and keep them reviewed")

    assert draft["conditions"]["description_contains"] == "apple cash"
    assert draft["actions"]["match_override_status"] == "force_unmatched"
    assert draft["actions"]["review_status"] == "reviewed"


def test_force_unmatched_survives_bank_reconciliation_rerun():
    engine = make_engine()
    posted_at = datetime(2026, 5, 15, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        bank_import = add_import(session)
        session.add(
            Transaction(
                id=700,
                source_message_id=1700,
                occurred_at=posted_at,
                parse_status="parsed",
                entry_kind="buy",
                payment_method="apple_cash",
                expense_category="inventory",
                amount=280.0,
                money_in=0.0,
                money_out=280.0,
                source_content="buy inventory 280 apple cash",
            )
        )
        session.add(
            BankTransaction(
                id=30,
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA",
                amount=-280.0,
                classification="logged_in_discord_possible",
                expense_category="inventory_purchases",
                category_confidence="manual",
                matched_transaction_id=700,
                matched_source_message_id=1700,
                matched_platform="discord",
                match_override_status="force_unmatched",
                match_override_note="Cash deal was already handled outside the bank feed.",
                review_status="reviewed",
            )
        )
        session.commit()

        rerun_bank_reconciliation(session, bank_import.id)
        row = session.get(BankTransaction, 30)

    assert row.match_override_status == "force_unmatched"
    assert row.matched_transaction_id is None
    assert row.matched_source_message_id is None
    assert row.matched_platform is None
    assert row.review_status == "reviewed"
    assert row.category_confidence == "manual"
    assert "forced unmatched" in row.match_reason.lower()


def test_ledger_route_renders_default_needs_action_grid():
    engine = make_engine()
    posted_at = datetime(2026, 5, 15, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        bank_import = add_import(session)
        session.add(
            BankTransaction(
                id=40,
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA",
                amount=-280.0,
                classification="direct_payment_out_needs_log_check",
                expense_category="inventory_purchases",
            )
        )
        session.commit()

        with patch("app.routers.ledger.require_role_response", return_value=None):
            response = ledger_page(
                make_request("/ledger"),
                account="",
                start="",
                end="",
                status="needs_action",
                category="",
                source="",
                search="",
                sort="posted_at",
                direction="desc",
                session=session,
            )

    body = response.body.decode("utf-8")
    assert response.status_code == 200
    assert "Unified Ledger" in body
    assert "Ledger Assistant" in body
    assert "PYMT SENT APPLE CASH" in body


def test_ledger_template_uses_dense_full_width_review_surface():
    source = open("app/templates/ledger.html", encoding="utf-8").read()

    assert "min-width: 1160px" not in source
    assert "minmax(340px, 420px)" not in source
    assert 'class="ledger-shell"' in source
    assert 'class="quick-chip' in source
    assert 'id="ledger-tools-drawer"' in source
    assert "data-ledger-row-id" in source
    assert "data-row-edit-form" in source
    assert "document.addEventListener(\"keydown\"" in source
    assert "focusSearch" in source


def test_ledger_row_status_form_can_return_json_for_in_place_updates():
    engine = make_engine()
    posted_at = datetime(2026, 5, 15, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        bank_import = add_import(session)
        session.add(
            BankTransaction(
                id=50,
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                description="PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA",
                amount=-280.0,
                classification="direct_payment_out_needs_log_check",
                expense_category="uncategorized",
            )
        )
        session.commit()

        with patch("app.routers.ledger.require_role_response", return_value=None):
            response = ledger_row_status_form(
                make_request(
                    "/ledger/rows/50/status-form",
                    method="POST",
                    headers=[(b"x-requested-with", b"fetch")],
                ),
                row_id=50,
                review_status="reviewed",
                classification="",
                expense_category="inventory_purchases",
                note="handled in-grid",
                selected_account="",
                selected_start="",
                selected_end="",
                selected_status="needs_action",
                selected_category="",
                selected_source="",
                selected_search="",
                selected_sort="posted_at",
                selected_direction="desc",
                session=session,
            )
        row = session.get(BankTransaction, 50)

    assert response.status_code == 200
    assert json.loads(response.body)["ok"] is True
    assert json.loads(response.body)["row"]["review_status"] == "reviewed"
    assert row.review_status == "reviewed"
    assert row.expense_category == "inventory_purchases"
