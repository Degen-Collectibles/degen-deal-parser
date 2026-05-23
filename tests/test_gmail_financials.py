import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import event, text
from sqlmodel import SQLModel, Session, create_engine, select

from app.config import get_settings
from app.discord.gmail_financials import (
    access_token_for_gmail_connection,
    build_gmail_financial_search_query,
    encrypt_gmail_token,
    link_gmail_evidence_to_bank_row,
    parse_sortswift_buylist_email,
    upsert_gmail_receipt_from_message,
)
from app.models import BankTransaction, GmailConnection, GmailReceipt, GmailReceiptLineItem, Transaction, TransactionItem


SORTSWIFT_HTML = """
<html><body>
<h1>Buylist Confirmation - Degen Collectibles</h1>
<p>Thanks for your buylist submission. Your order details are below.</p>
<table>
<tr><th>Name</th><th>Set</th><th>Number</th><th>Cond</th><th>Lang</th><th>Print</th><th>Qty</th><th>Cash</th><th>Credit</th><th>Notes</th></tr>
<tr><td>Tornadus 210</td><td>SV: Scarlet & Violet Promo Cards</td><td>210</td><td>NM</td><td>EN</td><td>Holofoil</td><td>1</td><td>USD $0.200</td><td></td><td></td></tr>
<tr><td>Charcadet</td><td>ME02: Phantasmal Flames</td><td>022</td><td>NM</td><td>EN</td><td>Holofoil</td><td>2</td><td>USD $3.948</td><td></td><td></td></tr>
<tr><td>Piplup 098 094</td><td>ME02: Phantasmal Flames</td><td>098/094</td><td>NM</td><td>EN</td><td>Holofoil</td><td>1</td><td>USD $12.564</td><td></td><td></td></tr>
<tr><td>Pikachu 131 091</td><td>SV: Paldean Fates</td><td>131/091</td><td>NM</td><td>EN</td><td>Holofoil</td><td>1</td><td>USD $52.242</td><td></td><td></td></tr>
<tr><td>Bulbasaur 133 132</td><td>ME01: Mega Evolution</td><td>133/132</td><td>NM</td><td>EN</td><td>Holofoil</td><td>1</td><td>USD $13.914</td><td></td><td></td></tr>
<tr><td colspan="6">Totals:</td><td>6</td><td>USD $82.868</td><td>USD $0.000</td><td></td></tr>
</table>
</body></html>
"""


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def test_gmail_financial_query_targets_receipts_and_sortswift():
    query = build_gmail_financial_search_query(days=180)

    assert "newer_than:180d" in query
    assert "invoice" in query
    assert "receipt" in query
    assert "no-reply@mail.sortswift.com" in query
    assert "-category:promotions" in query


def test_parse_sortswift_buylist_email_extracts_total_and_line_items():
    parsed = parse_sortswift_buylist_email(SORTSWIFT_HTML)

    assert parsed.is_sortswift is True
    assert parsed.vendor == "SortSwift"
    assert parsed.receipt_type == "sortswift_buylist"
    assert parsed.total_cash == 82.87
    assert parsed.total_credit == 0.0
    assert parsed.actual_tender_amount == 82.87
    assert parsed.actual_tender == "cash"
    assert parsed.quantity_total == 6
    assert len(parsed.line_items) == 5
    assert parsed.line_items[1]["name"] == "Charcadet"
    assert parsed.line_items[1]["quantity"] == 2


def test_upsert_sortswift_email_creates_deduped_gmail_transaction():
    engine = make_engine()
    received_at = datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc)

    with Session(engine) as session:
        first = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-msg-1",
            thread_id="thread-1",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            snippet="Buylist Confirmation - Degen Collectibles",
        )
        second = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-msg-1",
            thread_id="thread-1",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            snippet="Buylist Confirmation - Degen Collectibles",
        )
        session.commit()

        receipts = session.exec(select(GmailReceipt)).all()
        transactions = session.exec(select(Transaction)).all()
        line_items = session.exec(select(GmailReceiptLineItem)).all()
        transaction_items = session.exec(select(TransactionItem)).all()

    assert first.id == second.id
    assert len(receipts) == 1
    assert len(transactions) == 1
    assert len(line_items) == 5
    assert len(transaction_items) == 5
    tx = transactions[0]
    assert tx.source_kind == "gmail_sortswift"
    assert tx.source_external_id == "gmail-msg-1"
    assert tx.entry_kind == "buy"
    assert tx.expense_category == "inventory"
    assert tx.money_out == 82.87
    assert tx.amount == 82.87
    assert tx.needs_review is True


def test_upsert_gmail_receipt_handles_concurrent_duplicate_insert(tmp_path):
    db_path = tmp_path / "gmail-race.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    injected = {"done": False}

    def inject_duplicate(_conn, _cursor, statement, _parameters, _context, _executemany):
        if injected["done"] or "INSERT INTO gmail_receipts" not in str(statement):
            return
        injected["done"] = True
        now = datetime(2026, 5, 23, 22, 3, 27, tzinfo=timezone.utc).isoformat()
        with engine.begin() as other:
            other.execute(
                text(
                    """
                    INSERT INTO gmail_receipts
                    (gmail_message_id, thread_id, sender, subject, detected_vendor, detected_type,
                     status, confidence, snippet, parsed_json, raw_text, dedupe_hash, created_at, updated_at)
                    VALUES
                    (:gmail_message_id, :thread_id, :sender, :subject, :detected_vendor, :detected_type,
                     :status, :confidence, :snippet, :parsed_json, :raw_text, :dedupe_hash, :created_at, :updated_at)
                    """
                ),
                {
                    "gmail_message_id": "race-message-1",
                    "thread_id": "race-thread",
                    "sender": "Race Sender <race@example.com>",
                    "subject": "Existing raced receipt",
                    "detected_vendor": "Race Sender",
                    "detected_type": "invoice_or_receipt",
                    "status": "unmatched",
                    "confidence": "low",
                    "snippet": "existing",
                    "parsed_json": "{}",
                    "raw_text": "existing",
                    "dedupe_hash": "existing",
                    "created_at": now,
                    "updated_at": now,
                },
            )

    event.listen(engine, "before_cursor_execute", inject_duplicate)
    try:
        with Session(engine) as session:
            receipt = upsert_gmail_receipt_from_message(
                session,
                gmail_message_id="race-message-1",
                thread_id="race-thread",
                sender="PG&E Customer Service <PGECustomerService@notifications.pge.com>",
                subject="Protect Yourself Against Utility Scams",
                received_at=datetime(2026, 5, 23, 16, 24, 16, tzinfo=timezone.utc),
                html_body="<html><body>Recognize and Avoid Utility Scams</body></html>",
                snippet="Recognize and Avoid Utility Scams",
            )
            session.commit()
            receipts = session.exec(select(GmailReceipt)).all()
    finally:
        event.remove(engine, "before_cursor_execute", inject_duplicate)

    assert injected["done"] is True
    assert len(receipts) == 1
    assert receipt.gmail_message_id == "race-message-1"
    assert receipt.sender == "PG&E Customer Service <PGECustomerService@notifications.pge.com>"
    assert receipt.subject == "Protect Yourself Against Utility Scams"


def test_generic_gmail_receipt_links_to_bank_row_as_evidence():
    engine = make_engine()

    with Session(engine) as session:
        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-generic-1",
            thread_id="thread-generic-1",
            sender="FedEx <tracking@fedex.com>",
            subject="Your receipt from FedEx",
            received_at=datetime(2026, 5, 20, 15, 0, tzinfo=timezone.utc),
            html_body="<html><body>Receipt total USD $18.44</body></html>",
            snippet="Receipt total USD $18.44",
        )
        bank_row = BankTransaction(
            import_id=1,
            row_index=1,
            account_label="Chase",
            amount=-18.44,
            description="FEDEX 12345",
            classification="expense_or_purchase_needs_review",
            expense_category="shipping_postage",
        )
        session.add(bank_row)
        session.commit()
        session.refresh(receipt)
        session.refresh(bank_row)

        link = link_gmail_evidence_to_bank_row(session, receipt.id or 0, bank_row.id or 0)
        session.commit()
        session.refresh(bank_row)
        session.refresh(receipt)
        link_receipt_id = link.gmail_receipt_id
        link_bank_row_id = link.bank_transaction_id

    assert link_receipt_id == receipt.id
    assert link_bank_row_id == bank_row.id
    assert receipt.status == "matched"
    assert "Gmail evidence" in (bank_row.review_note or "")
    assert "FedEx" in json.loads(receipt.parsed_json)["vendor"]


def test_expired_gmail_access_token_refreshes_before_sync(monkeypatch):
    engine = make_engine()

    class FakeResponse:
        status_code = 200
        content = b"{}"

        def json(self):
            return {"access_token": "fresh-access", "expires_in": 3600}

    monkeypatch.setenv("SESSION_SECRET", "gmail-test-session-secret-xxxxxxxx")
    monkeypatch.setenv("GOOGLE_GMAIL_CLIENT_ID", "client-id")
    monkeypatch.setenv("GOOGLE_GMAIL_CLIENT_SECRET", "client-secret")
    get_settings.cache_clear()
    monkeypatch.setattr("app.discord.gmail_financials.httpx.post", lambda *args, **kwargs: FakeResponse())

    with Session(engine) as session:
        connection = GmailConnection(
            email_address="degencollectiblesllc@gmail.com",
            access_token_enc=encrypt_gmail_token("expired-access"),
            refresh_token_enc=encrypt_gmail_token("refresh-token"),
            access_token_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        session.add(connection)
        session.commit()
        session.refresh(connection)

        token = access_token_for_gmail_connection(session, connection)
        session.commit()
        session.refresh(connection)

    assert token == "fresh-access"
    refreshed_at = connection.access_token_expires_at
    if refreshed_at.tzinfo is None:
        refreshed_at = refreshed_at.replace(tzinfo=timezone.utc)
    assert refreshed_at > datetime.now(timezone.utc)
