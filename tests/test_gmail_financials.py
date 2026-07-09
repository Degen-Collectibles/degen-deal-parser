import base64
import inspect
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import event, text
from sqlmodel import SQLModel, Session, create_engine, select

from app.config import get_settings
from app.discord.gmail_authentication import persisted_source_trust_decision
from app.discord.gmail_financials import (
    GMAIL_RAW_SOURCE_STORAGE_LIMIT,
    access_token_for_gmail_connection,
    build_gmail_financial_search_query,
    encrypt_gmail_token,
    link_gmail_evidence_to_bank_row,
    parse_sortswift_buylist_email,
    upsert_gmail_receipt_from_message,
)
from app.discord.transactions import get_transactions
from app.models import (
    BankStatementImport,
    BankTransaction,
    BookkeepingEntry,
    BookkeepingImport,
    DiscordMessage,
    GmailConnection,
    GmailEvidenceLink,
    GmailReceipt,
    GmailReceiptLineItem,
    Transaction,
    TransactionItem,
)
from app.routers.bookkeeping import _gmail_receipt_views


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


def _install_single_message_gmail_api(
    monkeypatch,
    *,
    authentication_results,
    sender=None,
    additional_from_values=(),
):
    import app.discord.gmail_financials as gmail_module

    message_id = "gmail-sync-sortswift-1"
    encoded_body = base64.urlsafe_b64encode(SORTSWIFT_HTML.encode("utf-8")).decode("ascii").rstrip("=")
    headers = [
        {"name": "From", "value": sender or "SortSwift Buylist <no-reply@mail.sortswift.com>"},
        {"name": "To", "value": "Degen Collectibles <degencollectiblesllc@gmail.com>"},
        {"name": "Subject", "value": "Buylist Confirmation - Degen Collectibles"},
        {"name": "Date", "value": "Tue, 19 May 2026 19:33:00 +0000"},
        {"name": "Message-ID", "value": "<sortswift-1@mail.sortswift.com>"},
    ]
    headers.extend({"name": "From", "value": value} for value in additional_from_values)
    headers.extend(
        {"name": "Authentication-Results", "value": value}
        for value in authentication_results
    )
    full_message = {
        "id": message_id,
        "threadId": "thread-gmail-sync-sortswift-1",
        "labelIds": ["INBOX"],
        "snippet": "Buylist Confirmation - Degen Collectibles",
        "historyId": "123456789",
        "internalDate": str(int(datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc).timestamp() * 1000)),
        "sizeEstimate": len(SORTSWIFT_HTML),
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": headers,
            "body": {"attachmentId": None, "size": len(SORTSWIFT_HTML), "data": encoded_body},
        },
    }

    def fake_gmail_get(_access_token, path, *, params=None):
        if path == "/users/me/messages":
            return {"messages": [{"id": message_id, "threadId": full_message["threadId"]}], "resultSizeEstimate": 1}
        assert path == f"/users/me/messages/{message_id}"
        assert params == {"format": "full"}
        return full_message

    monkeypatch.setattr(gmail_module, "access_token_for_gmail_connection", lambda _session, _connection: "access-token")
    monkeypatch.setattr(gmail_module, "_gmail_get", fake_gmail_get)
    return gmail_module


def _sized_authentication_result(authserv_id: str, size: int) -> str:
    prefix = f"{authserv_id}; dmarc=pass ("
    suffix = ") header.from=sortswift.com"
    fill_length = size - len(prefix) - len(suffix)
    assert fill_length >= 0
    value = prefix + ("x" * fill_length) + suffix
    assert len(value.encode("utf-8")) == size
    return value


def test_gmail_financial_query_targets_receipts_and_sortswift():
    query = build_gmail_financial_search_query(days=180)

    assert "newer_than:180d" in query
    assert "invoice" in query
    assert "receipt" in query
    assert "no-reply@mail.sortswift.com" in query
    assert "-category:promotions" in query


def test_gmail_financial_query_accepts_explicit_start_date():
    query = build_gmail_financial_search_query(start_date=date(2026, 1, 1))

    assert "after:2026/01/01" in query
    assert "newer_than:" not in query
    assert "invoice" in query
    assert "no-reply@mail.sortswift.com" in query


def test_sync_gmail_connection_can_backfill_from_start_date_and_paginate(monkeypatch):
    import app.discord.gmail_financials as gmail_module

    engine = make_engine()
    list_calls = []

    def fake_access_token(_session, _connection):
        return "access-token"

    def fake_gmail_get(_access_token, path, *, params=None):
        if path == "/users/me/messages":
            list_calls.append(dict(params or {}))
            if "pageToken" not in (params or {}):
                return {"messages": [{"id": "msg-1"}, {"id": "msg-2"}], "nextPageToken": "page-2"}
            return {"messages": [{"id": "msg-3"}]}
        message_id = path.rsplit("/", 1)[-1]
        return {
            "id": message_id,
            "threadId": f"thread-{message_id}",
            "internalDate": str(int(datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)),
            "snippet": "Receipt total USD $10.00",
            "payload": {
                "mimeType": "text/html",
                "headers": [
                    {"name": "From", "value": "Vendor <vendor@example.com>"},
                    {"name": "Subject", "value": f"Receipt {message_id}"},
                ],
                "body": {"data": "UmVjZWlwdCB0b3RhbCBVU0QgJDEwLjAw"},
            },
        }

    monkeypatch.setattr(gmail_module, "access_token_for_gmail_connection", fake_access_token)
    monkeypatch.setattr(gmail_module, "_gmail_get", fake_gmail_get)

    with Session(engine) as session:
        connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = gmail_module.sync_gmail_connection(
            session,
            connection.id or 0,
            start_date=date(2026, 1, 1),
            limit=3,
        )
        receipts = session.exec(select(GmailReceipt)).all()

    assert result["scanned"] == 3
    assert result["imported"] == 3
    assert len(receipts) == 3
    assert len(list_calls) == 2
    assert "after:2026/01/01" in list_calls[0]["q"]
    assert list_calls[0]["maxResults"] == 3
    assert list_calls[1]["pageToken"] == "page-2"
    assert list_calls[1]["maxResults"] == 1


@pytest.mark.parametrize(
    ("authentication_results", "expected_reason"),
    [
        ([], "auth_missing"),
        (
            [
                "mx.google.com; dmarc=fail header.from=sortswift.com; "
                "dkim=fail header.i=@mail.sortswift.com; "
                "spf=fail smtp.mailfrom=no-reply@mail.sortswift.com"
            ],
            "auth_no_aligned_pass",
        ),
        (
            [
                "mail.attacker.example; dmarc=pass header.from=sortswift.com; "
                "dkim=pass header.i=@mail.sortswift.com"
            ],
            "auth_no_google_receiver",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=attacker.example; "
                "dkim=pass header.i=@attacker.example; spf=pass smtp.mailfrom=attacker@example.net"
            ],
            "auth_no_aligned_pass",
        ),
        (["mx.google.com; dmarc=pass; dkim=fail header.i=@mail.sortswift.com"], "auth_malformed"),
        (["mx.google.com; dmarc=pass reason=\"header.from=sortswift.com\""], "auth_malformed"),
        (["mx.google.com; dmarc=pass header.from=sortswift.com attacker-garbage"], "auth_malformed"),
        (["mx.google.com; dmarc=pass dmarc=fail header.from=sortswift.com"], "auth_malformed"),
        (["mx.google.com;; dmarc=pass header.from=sortswift.com"], "auth_malformed"),
        (["mx.google.com; dmarc=pass header.from=sortswift.com;"], "auth_malformed"),
        (["; mx.google.com; dmarc=pass header.from=sortswift.com"], "auth_malformed"),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com; "
                "mx.google.com; dmarc=fail header.from=sortswift.com"
            ],
            "auth_malformed",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com",
                "mail.attacker.example; dmarc=pass header.from=sortswift.com",
            ],
            "auth_mixed_receivers",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com; "
                "dmarc=fail header.from=sortswift.com"
            ],
            "auth_ambiguous_results",
        ),
        (
            [
                "mx.google.com; "
                "spf=pass smtp.mailfrom=no-reply@mail.sortswift.com; "
                "spf=fail smtp.mailfrom=no-reply@mail.sortswift.com"
            ],
            "auth_ambiguous_results",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com "
                "header.from=attacker.example"
            ],
            "auth_ambiguous_results",
        ),
        (
            [
                "mx.google.com; dkim=pass header.i=@mail.sortswift.com "
                "header.d=attacker.example"
            ],
            "auth_ambiguous_results",
        ),
        (
            [
                "mx.google.com; spf=pass "
                "smtp.mailfrom=no-reply@mail.sortswift.com smtp.helo=mail.sortswift.com"
            ],
            "auth_ambiguous_results",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com; "
                "spf=fail smtp.mailfrom=no-reply@mail.sortswift.com "
                "smtp.helo=mail.sortswift.com"
            ],
            "auth_ambiguous_results",
        ),
        (
            ["mx.google.com; spf=pass smtp.helo=mail.sortswift.com"],
            "auth_no_aligned_pass",
        ),
        (["mx.google.com; spf=fail"], "auth_malformed"),
        (["mx.google.com; arc=pass (i=1)"], "auth_no_aligned_pass"),
        (
            [
                "mx.google.com; "
                "dkim=pass header.i=@mail.sortswift.com header.d=sortswift.com; "
                "dkim=fail header.i=@bad..example.net header.d=example.net"
            ],
            "auth_malformed",
        ),
        (
            [
                "mx.google.com; dmarc=pass header.from=sortswift.com",
                "mx.google.com; dmarc=fail header.from=sortswift.com; "
                "dkim=fail header.i=@mail.sortswift.com; "
                "spf=fail smtp.mailfrom=no-reply@mail.sortswift.com",
            ],
            "auth_ambiguous_google_receiver",
        ),
    ],
)
def test_sync_quarantines_sortswift_without_unambiguous_google_aligned_authentication(
    monkeypatch,
    authentication_results,
    expected_reason,
):
    engine = make_engine()
    gmail_module = _install_single_message_gmail_api(
        monkeypatch,
        authentication_results=authentication_results,
    )

    with Session(engine) as session:
        connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
        receipt = session.exec(select(GmailReceipt)).one()
        transactions = session.exec(select(Transaction)).all()
        synthetic_messages = session.exec(select(DiscordMessage)).all()
        parsed = json.loads(receipt.parsed_json)

    assert result == {"scanned": 1, "imported": 1, "transactions": 0}
    assert receipt.status == "quarantined"
    assert receipt.transaction_id is None
    assert parsed["source_trusted"] is False
    assert parsed["source_trust_reason"] == expected_reason
    assert transactions == []
    assert synthetic_messages == []
    assert "Authentication-Results" not in receipt.raw_text
    assert "mx.google.com" not in receipt.parsed_json


@pytest.mark.parametrize(
    "authentication_result",
    [
        "mx.goo(x)gle.com; dmarc=pass header.from=sortswift.com",
        'mx.google.com; "dmarc"=pass header.from=sortswift.com',
        'mx.google.com; dmarc=pass header."from"=sortswift.com',
        'mx.google.com; dmarc=pass "header.from=sortswift.com"',
        r"mx.google.com; dmarc=pass header.fr\om=sortswift.com",
        r"mx.google.com; dmarc=pass header.from=sortswift\.com",
        "mx.google.com; dmarc=pass header.from=sortswift.com header.from=sortswift.com",
        "mx.google.com; dmarc=pass reason=ok reason=ok header.from=sortswift.com",
        "mx.google.com; dmarc=pass header.from=sortswift.com evil.foo=<<<",
        "mx.google.com; dmarc=pass header.from=<sortswift.com>",
        "mx.google.com; dmarc=pass header.from=<sortswift.com",
        "mx.google.com; dmarc=pass header.from=sortswift.com>",
        'mx.google.com; dmarc=pass header.from=" sortswift.com "',
        "mx.google.com; spf=pass smtp.mailfrom=bad..local@mail.sortswift.com",
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com; "
            "dmarc=fail header.from=sortswift.com header.from=attacker.example"
        ),
        "mx.google.com; dmarc=pass\nheader.from=sortswift.com",
        "mx.google.com; dmarc=pass\vheader.from=sortswift.com",
        "mx.google.com; dmarc=pass\u00a0header.from=sortswift.com",
        "mx.google.com; dmarc=pass header.from=sortswift.com reason=late",
        "mx.google.com; dkim=pass header.i=@a.sortswift.com header.d=b.sortswift.com",
        "mx.google.com; dkim=pass header.i=@sortswift.com header.d=mail.sortswift.com",
        (
            "mx.google.com; dmarc=pass header.from=sortswift.com "
            "smtp.mailfrom=no-reply@sortswift.com"
        ),
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com policy.dmarc=none",
        (
            "mx.google.com; spf=pass smtp.mailfrom=no-reply@mail.sortswift.com "
            "header.from=sortswift.com"
        ),
        (
            "mx.google.com; dmarc=pass header.from=sortswift.com; "
            "arc=pass header.i=@mail.sortswift.com"
        ),
        "mx.google.com 999; dmarc=pass header.from=sortswift.com",
        "mx.google.com; dmarc=pass reason=foo=bar header.from=sortswift.com",
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com; "
            "dmarc=bogus header.from=sortswift.com"
        ),
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com; "
            "spf=temp_error smtp.mailfrom=attacker@example.net"
        ),
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com; "
            "dmarc=softfail header.from=attacker.example"
        ),
        (
            "mx.google.com; dmarc=pass header.from=sortswift.com "
            "policy.published-domain=sortswift.com"
        ),
        "mx.google.com; dmarc=pass header.from=sortswift.com policy.dmarc=none",
        "mx.google.com; dmarc=pass header.from=sortswift.com policy.spf=none",
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com "
            "header.s=bad..selector"
        ),
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=@@@",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=AbC-",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=AbC_",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=---",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=AbC+/==",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=A=",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=Ab=C",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=AbC===",
        "mx.google.com; dkim=pass header.i=@mail.sortswift.com header.b=" + ("A" * 1025),
        (
            "mx.google.com; dmarc=pass header.from=sortswift.com; "
            "dkim=fail header.i=@a.sortswift.com header.d=b.sortswift.com"
        ),
    ],
    ids=[
        "comment_spliced_authserv",
        "quoted_method",
        "quoted_property_key_fragment",
        "quoted_whole_property",
        "backslash_spliced_property_key",
        "backslash_spliced_domain",
        "duplicate_identity_property",
        "duplicate_reason",
        "unknown_property",
        "angle_wrapped_domain",
        "leading_angle_domain",
        "trailing_angle_domain",
        "quoted_surrounding_whitespace",
        "malformed_mailfrom_local_part",
        "conflicting_identities_on_failed_method",
        "line_feed_whitespace",
        "vertical_tab_whitespace",
        "nbsp_whitespace",
        "late_reason",
        "dkim_sibling_domains",
        "dkim_reversed_binding",
        "dmarc_cross_method_property",
        "dkim_cross_method_property",
        "spf_cross_method_property",
        "unknown_method",
        "authserv_version_999",
        "bare_reason_tspecial",
        "unknown_result",
        "underscored_result",
        "inapplicable_result",
        "dmarc_published_domain_policy_property",
        "dmarc_policy_dmarc_property",
        "dmarc_policy_spf_property",
        "dkim_malformed_selector",
        "dkim_malformed_signature_token",
        "dkim_urlsafe_hyphen_signature_token",
        "dkim_urlsafe_underscore_signature_token",
        "dkim_hyphen_only_signature_token",
        "dkim_invalid_double_padding_length",
        "dkim_invalid_single_padding_length",
        "dkim_internal_padding",
        "dkim_excess_padding",
        "dkim_signature_token_over_length_bound",
        "failed_dkim_sibling_domains",
    ],
)
def test_sync_quarantines_fail_open_authentication_result_grammar(monkeypatch, authentication_result):
    engine = make_engine()
    gmail_module = _install_single_message_gmail_api(
        monkeypatch,
        authentication_results=[authentication_result],
    )

    with Session(engine) as session:
        connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
        receipt = session.exec(select(GmailReceipt)).one()
        parsed = json.loads(receipt.parsed_json)
        transactions = session.exec(select(Transaction)).all()

    assert result["transactions"] == 0
    assert receipt.status == "quarantined"
    assert parsed["source_trusted"] is False
    assert parsed["source_trust_reason"].startswith("auth_")
    assert transactions == []


@pytest.mark.parametrize(
    ("authentication_result", "expected_reason"),
    [
        (
            "mx.google.com; dmarc=pass (p=NONE sp=NONE dis=NONE) header.from=sortswift.com; "
            "dkim=pass header.i=@mail.sortswift.com; "
            "spf=pass smtp.mailfrom=no-reply@mail.sortswift.com",
            "trusted_dmarc_aligned",
        ),
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com; "
            "spf=fail smtp.mailfrom=attacker@example.net",
            "trusted_dkim_aligned",
        ),
        (
            "mx.google.com; spf=pass smtp.mailfrom=no-reply@mail.sortswift.com",
            "trusted_spf_aligned",
        ),
        (
            "mx.google.com; dkim=pass header.i=@sub.mail.sortswift.com "
            "header.d=mail.sortswift.com",
            "trusted_dkim_aligned",
        ),
        (
            "mx.google.com 1; dmarc=pass header.from=sortswift.com",
            "trusted_dmarc_aligned",
        ),
        (
            'mx.google.com; dmarc=pass reason="foo=bar; visible" '
            "header.from=sortswift.com",
            "trusted_dmarc_aligned",
        ),
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com "
            "header.s=selector.sub header.b=AbC+/w==",
            "trusted_dkim_aligned",
        ),
        (
            "mx.google.com; dkim=pass header.i=@mail.sortswift.com "
            "header.b=AbC123xy",
            "trusted_dkim_aligned",
        ),
        (
            "mx.google.com; dmarc=pass header.from=sortswift.com; "
            "dkim=fail header.i=@sub.mail.sortswift.com header.d=mail.sortswift.com",
            "trusted_dmarc_aligned",
        ),
        (
            "mx.google.com; arc=pass "
            "(i=1 spf=pass spfdomain=mail.sortswift.com "
            "dkim=pass dkdomain=mail.sortswift.com "
            "dmarc=pass fromdomain=sortswift.com); "
            "dmarc=pass header.from=sortswift.com",
            "trusted_dmarc_aligned",
        ),
        (
            "mx.google.com; "
            "dkim=pass header.i=@mail.sortswift.com header.d=sortswift.com "
            "header.s=selector1 header.b=AbC123xy; "
            "dkim=pass header.i=@bulk.sortswift.com header.d=sortswift.com "
            "header.s=selector2 header.b=Def456xy",
            "trusted_dkim_aligned",
        ),
        (
            "mx.google.com; "
            "dkim=pass header.i=@mail.sortswift.com header.d=sortswift.com "
            "header.s=selector1 header.b=AbC123xy; "
            "dkim=fail header.i=@mailer.example.net header.d=example.net "
            "header.s=selector2 header.b=Def456xy",
            "trusted_dkim_aligned",
        ),
    ],
)
def test_sync_creates_one_transaction_for_google_aligned_sortswift_authentication(
    monkeypatch,
    authentication_result,
    expected_reason,
):
    engine = make_engine()
    gmail_module = _install_single_message_gmail_api(
        monkeypatch,
        authentication_results=[authentication_result],
    )

    with Session(engine) as session:
        connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
        session.add(connection)
        session.commit()
        session.refresh(connection)

        first_result = gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
        second_result = gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
        receipts = session.exec(select(GmailReceipt)).all()
        transactions = session.exec(select(Transaction)).all()
        synthetic_messages = session.exec(select(DiscordMessage)).all()
        parsed = json.loads(receipts[0].parsed_json)

    assert first_result["transactions"] == 1
    assert second_result["transactions"] == 1
    assert len(receipts) == 1
    assert receipts[0].status == "transaction_created"
    assert parsed["source_trusted"] is True
    assert parsed["source_trust_reason"] == expected_reason
    assert len(transactions) == 1
    assert transactions[0].is_deleted is False
    assert len(synthetic_messages) == 1
    assert synthetic_messages[0].is_deleted is False


def test_source_authentication_from_header_limit_accepts_exact_and_rejects_over(monkeypatch):
    suffix = " <no-reply@mail.sortswift.com>"
    auth = ["mx.google.com; dmarc=pass header.from=sortswift.com"]
    outcomes = []

    for length in (1024, 1025):
        sender = ("A" * (length - len(suffix))) + suffix
        engine = make_engine()
        gmail_module = _install_single_message_gmail_api(
            monkeypatch,
            authentication_results=auth,
            sender=sender,
        )
        with Session(engine) as session:
            connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
            session.add(connection)
            session.commit()
            gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
            receipt = session.exec(select(GmailReceipt)).one()
            outcomes.append((receipt.status, json.loads(receipt.parsed_json)["source_trust_reason"]))

    assert outcomes == [
        ("transaction_created", "trusted_dmarc_aligned"),
        ("quarantined", "from_malformed"),
    ]


def test_source_authentication_per_value_limit_accepts_exact_and_rejects_over(monkeypatch):
    outcomes = []

    for size in (16 * 1024, (16 * 1024) + 1):
        engine = make_engine()
        gmail_module = _install_single_message_gmail_api(
            monkeypatch,
            authentication_results=[_sized_authentication_result("mx.google.com", size)],
        )
        with Session(engine) as session:
            connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
            session.add(connection)
            session.commit()
            gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
            receipt = session.exec(select(GmailReceipt)).one()
            outcomes.append((receipt.status, json.loads(receipt.parsed_json)["source_trust_reason"]))

    assert outcomes == [
        ("transaction_created", "trusted_dmarc_aligned"),
        ("quarantined", "auth_malformed"),
    ]


def test_source_authentication_header_count_limit_accepts_exact_and_rejects_over(monkeypatch):
    outcomes = []

    for count in (8, 9):
        values = ["mx.google.com; dmarc=pass header.from=sortswift.com"]
        values.extend(
            f"mx{index}.example; dmarc=pass header.from=sortswift.com"
            for index in range(1, count)
        )
        engine = make_engine()
        gmail_module = _install_single_message_gmail_api(
            monkeypatch,
            authentication_results=values,
        )
        with Session(engine) as session:
            connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
            session.add(connection)
            session.commit()
            gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
            receipt = session.exec(select(GmailReceipt)).one()
            outcomes.append(json.loads(receipt.parsed_json)["source_trust_reason"])

    assert outcomes == ["auth_mixed_receivers", "auth_malformed"]


def test_source_authentication_aggregate_limit_accepts_exact_and_rejects_over(monkeypatch):
    outcomes = []
    exact_sizes = [13108, 13108, 13107, 13107, 13106]

    for extra_byte in (0, 1):
        sizes = [*exact_sizes]
        sizes[-1] += extra_byte
        values = [
            _sized_authentication_result(
                "mx.google.com" if index == 0 else f"mx{index}.example",
                size,
            )
            for index, size in enumerate(sizes)
        ]
        assert sum(len(value.encode("utf-8")) for value in values) == (64 * 1024) + extra_byte
        engine = make_engine()
        gmail_module = _install_single_message_gmail_api(
            monkeypatch,
            authentication_results=values,
        )
        with Session(engine) as session:
            connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
            session.add(connection)
            session.commit()
            gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
            receipt = session.exec(select(GmailReceipt)).one()
            outcomes.append(json.loads(receipt.parsed_json)["source_trust_reason"])

    assert outcomes == ["auth_mixed_receivers", "auth_malformed"]


def test_authentication_result_segment_limit_accepts_exact_and_rejects_over():
    import app.discord.gmail_authentication as gmail_module

    def value(segment_count):
        segments = ["dmarc=pass header.from=sortswift.com"] * segment_count
        return "mx.google.com; " + "; ".join(segments)

    assert len(gmail_module._authentication_result_parts(value(32))[1]) == 32
    assert gmail_module._authentication_result_parts(value(33)) is None


def test_authentication_result_token_limit_accepts_exact_and_rejects_over():
    import app.discord.gmail_authentication as gmail_module

    def segment(token_count):
        return " ".join(f"property{index}=value" for index in range(token_count))

    assert len(gmail_module._lex_authentication_key_values(segment(32))) == 32
    assert gmail_module._lex_authentication_key_values(segment(33)) is None


def test_authentication_result_comment_depth_accepts_exact_and_rejects_over():
    import app.discord.gmail_authentication as gmail_module

    def value(depth):
        return ("(" * depth) + "comment" + (")" * depth)

    assert gmail_module._replace_authentication_result_comments(value(8)) == " "
    assert gmail_module._replace_authentication_result_comments(value(9)) is None


def test_sync_quarantines_multiple_from_headers_even_when_first_header_and_authentication_pass(monkeypatch):
    engine = make_engine()
    gmail_module = _install_single_message_gmail_api(
        monkeypatch,
        authentication_results=["mx.google.com; dmarc=pass header.from=sortswift.com"],
        additional_from_values=["Attacker <attacker@example.net>"],
    )

    with Session(engine) as session:
        connection = GmailConnection(email_address="degencollectiblesllc@gmail.com", status="active")
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = gmail_module.sync_gmail_connection(session, connection.id or 0, limit=1)
        receipt = session.exec(select(GmailReceipt)).one()
        parsed = json.loads(receipt.parsed_json)

    assert result["transactions"] == 0
    assert receipt.status == "quarantined"
    assert parsed["source_trust_reason"] == "from_malformed"


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


def test_upsert_requires_explicit_source_trust_decision():
    signature = inspect.signature(upsert_gmail_receipt_from_message)

    assert signature.parameters["source_trusted"].default is inspect.Parameter.empty
    assert signature.parameters["source_trust_reason"].default is inspect.Parameter.empty


@pytest.mark.parametrize(
    ("parsed_json", "expected"),
    [
        (
            json.dumps(
                {
                    "source_trusted": True,
                    "source_trust_reason": "trusted_dmarc_aligned",
                }
            ),
            (True, True, "trusted_dmarc_aligned"),
        ),
        (
            json.dumps(
                {
                    "source_trusted": False,
                    "source_trust_reason": "auth_no_aligned_pass",
                }
            ),
            (True, False, "auth_no_aligned_pass"),
        ),
        ("{}", (False, False, "source_not_evaluated")),
        (json.dumps({"source_trusted": False}), (False, False, "source_not_evaluated")),
        (
            json.dumps(
                {
                    "source_trusted": False,
                    "source_trust_reason": "source_not_evaluated",
                }
            ),
            (False, False, "source_not_evaluated"),
        ),
        ("not-json", (False, False, "source_not_evaluated")),
        (
            json.dumps(
                {
                    "source_trusted": False,
                    "source_trust_reason": "untrusted_explicit",
                }
            ),
            (False, False, "source_not_evaluated"),
        ),
    ],
)
def test_persisted_source_trust_decision_is_tri_state(parsed_json, expected):
    decision = persisted_source_trust_decision(
        "SortSwift Buylist <no-reply@mail.sortswift.com>",
        parsed_json,
    )

    assert (decision.verified, decision.trusted, decision.reason) == expected


def test_upsert_persists_source_body_truncation_flag():
    engine = make_engine()
    long_body = SORTSWIFT_HTML + ("x" * (GMAIL_RAW_SOURCE_STORAGE_LIMIT + 1))

    with Session(engine) as session:
        complete = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-source-body-complete",
            thread_id="thread-source-body-complete",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc),
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_dmarc_aligned",
        )
        truncated = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-source-body-truncated",
            thread_id="thread-source-body-truncated",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=datetime(2026, 5, 19, 19, 34, tzinfo=timezone.utc),
            html_body=long_body,
            source_trusted=True,
            source_trust_reason="trusted_dmarc_aligned",
        )
        session.commit()
        complete_parsed = json.loads(complete.parsed_json)
        truncated_parsed = json.loads(truncated.parsed_json)
        complete_raw_length = len(complete.raw_text)
        truncated_raw_length = len(truncated.raw_text)

    assert complete_parsed["source_body_truncated"] is False
    assert truncated_parsed["source_body_truncated"] is True
    assert complete_raw_length < GMAIL_RAW_SOURCE_STORAGE_LIMIT
    assert truncated_raw_length == GMAIL_RAW_SOURCE_STORAGE_LIMIT


def test_upsert_sortswift_body_from_attacker_is_quarantined_without_financial_rows():
    engine = make_engine()

    with Session(engine) as session:
        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-attacker-1",
            thread_id="thread-attacker-1",
            sender="Attacker <attacker@example.net>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc),
            html_body=SORTSWIFT_HTML,
            snippet="Buylist Confirmation - Degen Collectibles",
            source_trusted=False,
            source_trust_reason="source_not_evaluated",
        )
        session.commit()

        transactions = session.exec(select(Transaction)).all()
        synthetic_messages = session.exec(select(DiscordMessage)).all()
        receipt_status = receipt.status
        receipt_transaction_id = receipt.transaction_id
        receipt_parsed_json = receipt.parsed_json

    assert receipt_status == "quarantined"
    assert receipt_transaction_id is None
    assert json.loads(receipt_parsed_json)["source_trust_reason"] == "source_not_evaluated"
    assert transactions == []
    assert synthetic_messages == []


@pytest.mark.parametrize(
    "sender",
    [
        "Attacker <attacker@example.net>",
        "SortSwift <no-reply@mail.sortswift.com>, Attacker <attacker@example.net>",
        "SortSwift <no-reply@mail.sortswift.com.attacker.example>",
        "SortSwift <no-reply@mail.sortswift.com>,",
        "SortSwift <no-reply@mail.sortswift.com>;",
        "SortSwift <no-reply@mail.sortswift.com>:",
    ],
)
def test_explicit_trust_does_not_bypass_exact_single_sortswift_mailbox(sender):
    engine = make_engine()

    with Session(engine) as session:
        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id=f"gmail-strict-from-{abs(hash(sender))}",
            thread_id="thread-strict-from",
            sender=sender,
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc),
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_explicit",
        )
        session.commit()
        receipt_status = receipt.status
        transaction_count = len(session.exec(select(Transaction)).all())

    assert receipt_status == "quarantined"
    assert transaction_count == 0


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
            source_trusted=True,
            source_trust_reason="trusted_explicit",
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
            source_trusted=True,
            source_trust_reason="trusted_explicit",
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


@pytest.mark.parametrize("clear_receipt_link", [False, True])
def test_reprocessing_existing_sortswift_transaction_with_untrusted_evidence_removes_it_from_reports(
    clear_receipt_link,
):
    engine = make_engine()
    received_at = datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc)

    with Session(engine) as session:
        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-reprocess-untrusted-1",
            thread_id="thread-reprocess-untrusted-1",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_explicit",
        )
        session.commit()
        assert len(get_transactions(session)) == 1
        if clear_receipt_link:
            receipt.transaction_id = None
            session.add(receipt)
            session.commit()

        for _ in range(2):
            receipt = upsert_gmail_receipt_from_message(
                session,
                gmail_message_id="gmail-reprocess-untrusted-1",
                thread_id="thread-reprocess-untrusted-1",
                sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
                subject="Buylist Confirmation - Degen Collectibles",
                received_at=received_at,
                html_body=SORTSWIFT_HTML,
                source_trusted=False,
                source_trust_reason="auth_no_aligned_pass",
            )
            session.commit()

        transaction = session.exec(select(Transaction)).one()
        synthetic_message = session.exec(select(DiscordMessage)).one()
        report_transactions = get_transactions(session)
        transaction_count = len(session.exec(select(Transaction)).all())
        synthetic_message_count = len(session.exec(select(DiscordMessage)).all())
        receipt_status = receipt.status
        receipt_transaction_id = receipt.transaction_id

    assert receipt_status == "quarantined"
    assert receipt_transaction_id is None
    assert transaction.is_deleted is True
    assert transaction.needs_review is True
    assert synthetic_message.is_deleted is True
    assert synthetic_message.parse_status == "ignored"
    assert report_transactions == []
    assert transaction_count == 1
    assert synthetic_message_count == 1


def test_untrusted_reprocess_ignores_wrong_receipt_transaction_link():
    engine = make_engine()
    received_at = datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc)

    with Session(engine) as session:
        first = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-safe-invalidate-first",
            thread_id="thread-safe-invalidate-first",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_dmarc_aligned",
        )
        second = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-safe-invalidate-second",
            thread_id="thread-safe-invalidate-second",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_dmarc_aligned",
        )
        session.commit()
        first_transaction_id = first.transaction_id
        second_transaction_id = second.transaction_id
        first.transaction_id = second_transaction_id
        session.add(first)
        session.commit()

        reparsed = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id=first.gmail_message_id,
            thread_id=first.thread_id,
            sender=first.sender,
            subject=first.subject,
            received_at=first.received_at,
            html_body=first.raw_text,
            snippet=first.snippet,
            source_trusted=False,
            source_trust_reason="auth_no_aligned_pass",
        )
        session.commit()
        first_transaction = session.get(Transaction, first_transaction_id)
        second_transaction = session.get(Transaction, second_transaction_id)
        reparsed_transaction_id = reparsed.transaction_id

    assert reparsed_transaction_id is None
    assert first_transaction is not None and first_transaction.is_deleted is True
    assert second_transaction is not None and second_transaction.is_deleted is False


def test_untrusted_reprocess_clears_transaction_children_and_reconciliation_matches():
    engine = make_engine()
    received_at = datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc)

    with Session(engine) as session:
        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-cleanup-dependents",
            thread_id="thread-cleanup-dependents",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            source_trusted=True,
            source_trust_reason="trusted_dmarc_aligned",
        )
        session.flush()
        transaction_id = receipt.transaction_id or 0
        bookkeeping_import = BookkeepingImport(show_label="Gmail cleanup")
        bank_import = BankStatementImport(
            label="Gmail cleanup",
            account_label="Chase",
            account_type="checking",
        )
        session.add(bookkeeping_import)
        session.add(bank_import)
        session.flush()
        bookkeeping_entry = BookkeepingEntry(
            import_id=bookkeeping_import.id or 0,
            row_index=1,
            matched_transaction_id=transaction_id,
            match_status="matched_strong",
        )
        bank_transaction = BankTransaction(
            import_id=bank_import.id or 0,
            row_index=1,
            account_label="Chase",
            account_type="checking",
            description="SortSwift payout",
            amount=-82.87,
            classification="logged_in_discord_strong",
            confidence="high",
            matched_transaction_id=transaction_id,
            matched_source_message_id=1,
            matched_platform="gmail",
            match_reason="Original match",
        )
        session.add(bookkeeping_entry)
        session.add(bank_transaction)
        session.flush()
        evidence_link = GmailEvidenceLink(
            gmail_receipt_id=receipt.id or 0,
            bank_transaction_id=bank_transaction.id,
            transaction_id=transaction_id,
        )
        session.add(evidence_link)
        session.commit()
        bookkeeping_entry_id = bookkeeping_entry.id
        bank_transaction_id = bank_transaction.id
        evidence_link_id = evidence_link.id

        reparsed = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id=receipt.gmail_message_id,
            thread_id=receipt.thread_id,
            sender=receipt.sender,
            subject=receipt.subject,
            received_at=receipt.received_at,
            html_body=receipt.raw_text,
            snippet=receipt.snippet,
            source_trusted=False,
            source_trust_reason="auth_no_aligned_pass",
        )
        session.commit()
        transaction = session.get(Transaction, transaction_id)
        remaining_items = session.exec(
            select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
        ).all()
        bookkeeping_entry = session.get(BookkeepingEntry, bookkeeping_entry_id)
        bank_transaction = session.get(BankTransaction, bank_transaction_id)
        evidence_link = session.get(GmailEvidenceLink, evidence_link_id)
        reparsed_transaction_id = reparsed.transaction_id

    assert reparsed_transaction_id is None
    assert transaction is not None and transaction.is_deleted is True
    assert remaining_items == []
    assert bookkeeping_entry is not None
    assert bookkeeping_entry.matched_transaction_id is None
    assert bookkeeping_entry.match_status == "unmatched"
    assert bank_transaction is not None
    assert bank_transaction.matched_transaction_id is None
    assert bank_transaction.matched_source_message_id is None
    assert bank_transaction.matched_platform is None
    assert bank_transaction.classification == "needs_review"
    assert bank_transaction.confidence == "low"
    assert "Gmail SortSwift" in bank_transaction.match_reason
    assert evidence_link is not None
    assert evidence_link.transaction_id is None
    assert evidence_link.bank_transaction_id == bank_transaction_id


def test_gmail_receipt_views_can_filter_sortswift_transactions_needing_review():
    engine = make_engine()
    received_at = datetime(2026, 5, 19, 19, 33, tzinfo=timezone.utc)

    with Session(engine) as session:
        sortswift = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id="gmail-sortswift-review",
            thread_id="thread-sortswift",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=received_at,
            html_body=SORTSWIFT_HTML,
            snippet="Buylist Confirmation - Degen Collectibles",
            source_trusted=True,
            source_trust_reason="trusted_explicit",
        )
        invoice = GmailReceipt(
            gmail_message_id="gmail-invoice",
            thread_id="thread-invoice",
            sender="Vendor <vendor@example.com>",
            subject="Invoice",
            received_at=received_at,
            detected_vendor="Vendor",
            detected_type="invoice_or_receipt",
            status="unmatched",
            confidence="low",
            snippet="Invoice",
            parsed_json="{}",
            raw_text="Invoice",
            dedupe_hash="invoice",
        )
        session.add(invoice)
        session.commit()
        sortswift_id = sortswift.id

        views = _gmail_receipt_views(session, status="needs_review", limit=100)

    assert [view["receipt"].id for view in views] == [sortswift_id]
    assert views[0]["transaction"].needs_review is True


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
                source_trusted=False,
                source_trust_reason="source_not_evaluated",
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
            source_trusted=False,
            source_trust_reason="source_not_evaluated",
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
