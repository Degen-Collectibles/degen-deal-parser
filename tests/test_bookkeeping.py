import asyncio
import base64
import csv
import hashlib
import json
import os
import unittest
from datetime import datetime, timezone
from io import StringIO
from urllib.parse import unquote, unquote_plus
from unittest.mock import patch

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, utils
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select


def _set_test_env_default(key: str, value: str) -> None:
    if not os.environ.get(key):
        os.environ[key] = value


# These tests import app.main to exercise the Plaid webhook route. Provide a
# valid PII key so an externally-enabled employee portal env cannot fail closed
# during app import, but do not flip EMPLOYEE_PORTAL_ENABLED globally: unittest
# discovery runs later employee-portal tests in this same Python process.
_set_test_env_default("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
_set_test_env_default("SESSION_SECRET", "bookkeeping-test-session-xxxxxxxxxxxxxxxx")
_set_test_env_default("ADMIN_PASSWORD", "bookkeeping-test-admin-password")
_set_test_env_default("EMPLOYEE_TOKEN_HMAC_KEY", "bookkeeping-test-token-key")

from app.discord.bookkeeping import (
    fetch_google_sheet_export,
    infer_show_label_from_message,
    normalize_bookkeeping_rows,
    read_tabular_rows,
    reconcile_bookkeeping_import,
)
from app.discord.gmail_financials import GMAIL_RAW_SOURCE_STORAGE_LIMIT
import app.cache as cache_module
from app.cache import cache_get, cache_set
from app.models import (
    BankStatementImport,
    BankTransaction,
    BookkeepingEntry,
    BookkeepingImport,
    DiscordMessage,
    GmailReceipt,
    Transaction,
    TransactionItem,
    PARSE_PARSED,
)


def _utcnow():
    return datetime.now(timezone.utc)


def _dt(year, month, day):
    return datetime(year, month, day, 12, 0, 0, tzinfo=timezone.utc)


def _fresh_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _plaid_jwk(private_key: ec.EllipticCurvePrivateKey, kid: str) -> dict[str, object]:
    numbers = private_key.public_key().public_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "alg": "ES256",
        "use": "sig",
        "kid": kid,
        "x": _b64url(numbers.x.to_bytes(32, "big")),
        "y": _b64url(numbers.y.to_bytes(32, "big")),
    }


def _plaid_verification_jwt(
    private_key: ec.EllipticCurvePrivateKey,
    *,
    kid: str = "kid-1",
    raw_body: bytes = b'{"item_id":"item-1","webhook_code":"DEFAULT_UPDATE"}',
    iat: int = 1_700_000_000,
    alg: str = "ES256",
) -> str:
    header = {"alg": alg, "kid": kid, "typ": "JWT"}
    payload = {
        "iat": iat,
        "request_body_sha256": hashlib.sha256(raw_body).hexdigest(),
    }
    signing_input = (
        f"{_b64url(json.dumps(header, separators=(',', ':')).encode('utf-8'))}."
        f"{_b64url(json.dumps(payload, separators=(',', ':')).encode('utf-8'))}"
    ).encode("ascii")
    der_signature = private_key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = utils.decode_dss_signature(der_signature)
    raw_signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return f"{signing_input.decode('ascii')}.{_b64url(raw_signature)}"


SORTSWIFT_REPARSE_HTML = """
<html><body>
<h1>SortSwift Buylist Confirmation</h1>
<table>
<tr><th>Name</th><th>Set</th><th>Qty</th><th>Cash</th><th>Credit</th></tr>
<tr><td>Pikachu</td><td>Base</td><td>1</td><td>USD $10.00</td><td></td></tr>
<tr><td>Totals:</td><td></td><td>1</td><td>USD $10.00</td><td>USD $0.00</td></tr>
</table>
</body></html>
"""


def _long_sortswift_reparse_html():
    table = """
    <table>
    <tr><th>Name</th><th>Set</th><th>Qty</th><th>Cash</th><th>Credit</th></tr>
    <tr><td>Charizard</td><td>Base</td><td>2</td><td>USD $25.00</td><td></td></tr>
    <tr><td>Totals:</td><td></td><td>2</td><td>USD $25.00</td><td>USD $0.00</td></tr>
    </table>
    """
    return (
        "<html><body><h1>SortSwift Buylist Confirmation</h1><p>"
        + ("x" * GMAIL_RAW_SOURCE_STORAGE_LIMIT)
        + "</p>"
        + table
        + "</body></html>"
    )


class ReadTabularRowsTests(unittest.TestCase):
    def test_csv_parses_headers_and_values(self):
        csv_bytes = b"date,kind,amount\n2024-01-01,sale,50\n2024-01-02,buy,30\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["kind"], "sale")
        self.assertEqual(rows[0]["amount"], "50")
        self.assertEqual(rows[1]["kind"], "buy")

    def test_csv_strips_utf8_bom(self):
        csv_bytes = b"\xef\xbb\xbfdate,kind,amount\n2024-01-01,sale,50\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(len(rows), 1)
        # BOM should be stripped; header should be "date" not "\ufeffdate"
        self.assertIn("date", rows[0])
        self.assertNotIn("\ufeffdate", rows[0])

    def test_csv_adds_sheet_name_key(self):
        csv_bytes = b"date,amount\n2024-01-01,50\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(rows[0]["__sheet_name"], "import")

    def test_csv_empty_file_returns_empty_list(self):
        rows = read_tabular_rows("export.csv", b"")
        self.assertEqual(rows, [])

    def test_csv_headers_only_returns_empty_list(self):
        rows = read_tabular_rows("export.csv", b"date,kind,amount\n")
        self.assertEqual(rows, [])

    def test_unsupported_extension_raises(self):
        with self.assertRaises(ValueError):
            read_tabular_rows("export.txt", b"some data")

    def test_normalize_bookkeeping_rows_skips_card_reference_tabs(self):
        rows = [
            {
                "__sheet_name": "CardList",
                "set": "1st Edition Base Set",
                "shortcut": "1B",
                "number": "4/102",
                "card_name": "Charizard",
                "rarity_variant": "Holo Rare",
                "combined": "1st Edition Base Set - 4/102 - Charizard",
            },
            {
                "__sheet_name": "INCOME",
                "date": "2026-05-17",
                "amount": "100",
                "payment": "cash",
                "notes": "show sale",
            },
        ]

        normalized = normalize_bookkeeping_rows(rows)

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["sheet_name"], "INCOME")
        self.assertEqual(normalized[0]["entry_kind"], "sale")
        self.assertEqual(normalized[0]["amount"], 100.0)

    def test_infer_show_label_strips_sheet_url_before_colon_split(self):
        label = infer_show_label_from_message(
            "May 16\nhttps://docs.google.com/spreadsheets/d/example/edit?gid=0",
            "Discord bookkeeping import",
        )

        self.assertEqual(label, "May 16")


class GoogleSheetExportFetchTests(unittest.TestCase):
    def test_follows_safe_google_redirect_before_streaming_export(self):
        class FakeResponse:
            def __init__(self, status_code=200, headers=None, chunks=None):
                self.status_code = status_code
                self.headers = headers or {}
                self._chunks = chunks or [b"date,amount\n", b"2026-05-15,50\n"]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def raise_for_status(self):
                return None

            async def aiter_bytes(self):
                for chunk in self._chunks:
                    yield chunk

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                self.streamed_urls = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, method, url):
                self.streamed_urls.append(url)
                if len(self.streamed_urls) == 1:
                    return FakeResponse(
                        status_code=302,
                        headers={
                            "location": (
                                "https://doc-10-18-sheets.googleusercontent.com/exported-spreadsheet.xlsx"
                            )
                        },
                    )
                return FakeResponse()

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        client = FakeAsyncClient()
        with patch("app.discord.bookkeeping.httpx.AsyncClient", return_value=client):
            content = asyncio.run(fetch_google_sheet_export(export_url))

        self.assertEqual(content, b"date,amount\n2026-05-15,50\n")
        self.assertEqual(len(client.streamed_urls), 2)

    def test_rejects_google_sheet_export_redirect_to_untrusted_host(self):
        class FakeResponse:
            status_code = 302
            headers = {"location": "https://evil.example/export.xlsx"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

        class FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, method, url):
                return FakeResponse()

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        with patch("app.discord.bookkeeping.httpx.AsyncClient", return_value=FakeAsyncClient()):
            with self.assertRaises(ValueError):
                asyncio.run(fetch_google_sheet_export(export_url))

    def test_rejects_google_sheet_export_redirect_to_non_sheet_googleusercontent_host(self):
        class FakeResponse:
            status_code = 302
            headers = {"location": "https://docs-usercontent.googleusercontent.com/export.xlsx"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

        class FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, method, url):
                return FakeResponse()

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        with patch("app.discord.bookkeeping.httpx.AsyncClient", return_value=FakeAsyncClient()):
            with self.assertRaises(ValueError):
                asyncio.run(fetch_google_sheet_export(export_url))


class PlaidWebhookVerificationTests(unittest.TestCase):
    def test_plaid_webhook_verifier_accepts_valid_es256_jwt_and_body_hash(self):
        from app.discord import plaid_bank_feed

        raw_body = b'{"item_id":"item-1","webhook_code":"DEFAULT_UPDATE"}'
        private_key = ec.generate_private_key(ec.SECP256R1())
        token = _plaid_verification_jwt(private_key, raw_body=raw_body, iat=1_700_000_000)
        jwk = _plaid_jwk(private_key, "kid-1")

        with patch.object(plaid_bank_feed, "_plaid_post", return_value={"key": jwk}) as plaid_post:
            verified = plaid_bank_feed.verify_plaid_webhook_signature(
                raw_body,
                token,
                now=1_700_000_120,
            )

        self.assertTrue(verified)
        plaid_post.assert_called_once_with("/webhook_verification_key/get", {"key_id": "kid-1"})

    def test_plaid_webhook_verifier_rejects_missing_stale_or_mismatched_tokens(self):
        from app.discord import plaid_bank_feed

        raw_body = b'{"item_id":"item-1","webhook_code":"DEFAULT_UPDATE"}'
        private_key = ec.generate_private_key(ec.SECP256R1())
        jwk = _plaid_jwk(private_key, "kid-1")
        stale = _plaid_verification_jwt(private_key, raw_body=raw_body, iat=1_700_000_000)
        mismatched = _plaid_verification_jwt(
            private_key,
            raw_body=b'{"item_id":"different"}',
            iat=1_700_000_100,
        )

        with patch.object(plaid_bank_feed, "_plaid_post", return_value={"key": jwk}):
            with self.assertRaises(plaid_bank_feed.PlaidWebhookVerificationError):
                plaid_bank_feed.verify_plaid_webhook_signature(raw_body, "", now=1_700_000_120)
            with self.assertRaises(plaid_bank_feed.PlaidWebhookVerificationError):
                plaid_bank_feed.verify_plaid_webhook_signature(raw_body, stale, now=1_700_000_400)
            with self.assertRaises(plaid_bank_feed.PlaidWebhookVerificationError):
                plaid_bank_feed.verify_plaid_webhook_signature(raw_body, mismatched, now=1_700_000_120)

    def test_plaid_webhook_route_rejects_missing_verification_before_handler(self):
        from app.db import get_session
        import app.main as app_main

        engine = _fresh_engine()

        def override_get_session():
            with Session(engine) as session:
                yield session

        app_main.app.dependency_overrides[get_session] = override_get_session
        client = TestClient(app_main.app)
        raw_body = b'{"item_id":"item-1","webhook_code":"DEFAULT_UPDATE"}'

        try:
            with patch("app.routers.bookkeeping.handle_plaid_webhook") as handler:
                response = client.post(
                    "/webhooks/plaid",
                    content=raw_body,
                    headers={"content-type": "application/json"},
                )
        finally:
            app_main.app.dependency_overrides.clear()
            engine.dispose()

        self.assertEqual(response.status_code, 401)
        handler.assert_not_called()

    def test_plaid_webhook_route_rejects_invalid_json_after_valid_verification(self):
        from app.db import get_session
        import app.main as app_main

        engine = _fresh_engine()

        def override_get_session():
            with Session(engine) as session:
                yield session

        app_main.app.dependency_overrides[get_session] = override_get_session
        client = TestClient(app_main.app)
        raw_body = b"{not-json"

        try:
            with (
                patch("app.routers.bookkeeping.verify_plaid_webhook_signature", return_value=True) as verifier,
                patch("app.routers.bookkeeping.handle_plaid_webhook") as handler,
            ):
                response = client.post(
                    "/webhooks/plaid",
                    content=raw_body,
                    headers={
                        "content-type": "application/json",
                        "Plaid-Verification": "signed.jwt",
                    },
                )
        finally:
            app_main.app.dependency_overrides.clear()
            engine.dispose()

        self.assertEqual(response.status_code, 400)
        verifier.assert_called_once_with(raw_body, "signed.jwt")
        handler.assert_not_called()

    def test_plaid_webhook_route_verifies_raw_body_then_calls_handler(self):
        from app.db import get_session
        import app.main as app_main

        engine = _fresh_engine()

        def override_get_session():
            with Session(engine) as session:
                yield session

        app_main.app.dependency_overrides[get_session] = override_get_session
        client = TestClient(app_main.app)
        raw_body = b'{"item_id":"item-1","webhook_code":"DEFAULT_UPDATE"}'
        cache_module._cache.clear()
        cache_set("finance:v4:test", {"stale": True})
        cache_set("reports:test", {"stale": True})
        cache_set("other:test", {"keep": True})

        try:
            with (
                patch("app.routers.bookkeeping.verify_plaid_webhook_signature", return_value=True) as verifier,
                patch("app.routers.bookkeeping.handle_plaid_webhook", return_value={"ok": True}) as handler,
            ):
                response = client.post(
                    "/webhooks/plaid",
                    content=raw_body,
                    headers={
                        "content-type": "application/json",
                        "Plaid-Verification": "signed.jwt",
                    },
                )
        finally:
            app_main.app.dependency_overrides.clear()
            engine.dispose()

        self.assertEqual(response.status_code, 200)
        verifier.assert_called_once_with(raw_body, "signed.jwt")
        handler.assert_called_once()
        args, _kwargs = handler.call_args
        self.assertEqual(args[1]["item_id"], "item-1")
        self.assertIsNone(cache_get("finance:v4:test"))
        self.assertIsNone(cache_get("reports:test"))
        self.assertEqual(cache_get("other:test"), {"keep": True})
        cache_module._cache.clear()

    def test_bank_row_status_form_invalidates_finance_caches(self):
        from app.routers.bookkeeping import bank_reconciliation_row_status_form

        engine = _fresh_engine()
        cache_module._cache.clear()

        try:
            with Session(engine) as session:
                session.add(
                    BankStatementImport(
                        id=1,
                        label="Chase import",
                        account_label="Chase Checking",
                    )
                )
                session.add(
                    BankTransaction(
                        id=101,
                        import_id=1,
                        row_index=1,
                        account_label="Chase Checking",
                        account_type="checking",
                        posted_at=_dt(2026, 5, 25),
                        description="Shipping supplies",
                        description_stem="SHIPPING SUPPLIES",
                        amount=-12.34,
                        classification="expense_or_purchase_needs_review",
                        expense_category="uncategorized",
                    )
                )
                session.commit()

                cache_set("finance:v4:test", {"stale": True})
                cache_set("reports:test", {"stale": True})
                cache_set("other:test", {"keep": True})

                with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                    response = bank_reconciliation_row_status_form(
                        request=object(),
                        row_id=101,
                        import_id=1,
                        review_status="reviewed",
                        classification="",
                        expense_category="shipping_postage",
                        note="",
                        selected_classification="",
                        selected_expense_category="",
                        selected_review_status="",
                        selected_attention="",
                        selected_expenses_only="true",
                        selected_search="",
                        selected_limit="250",
                        session=session,
                    )

                self.assertEqual(response.status_code, 303)
                self.assertIsNone(cache_get("finance:v4:test"))
                self.assertIsNone(cache_get("reports:test"))
                self.assertEqual(cache_get("other:test"), {"keep": True})
        finally:
            cache_module._cache.clear()
            engine.dispose()

    def test_bank_reconciliation_export_escapes_formula_like_text_cells(self):
        from app.routers.bookkeeping import bank_reconciliation_export_csv

        engine = _fresh_engine()
        try:
            with Session(engine) as session:
                session.add(
                    BankStatementImport(
                        id=1,
                        label="Chase import",
                        account_label="Chase Checking",
                    )
                )
                session.add(
                    BankTransaction(
                        id=101,
                        import_id=1,
                        row_index=1,
                        account_label="Chase Checking",
                        account_type="checking",
                        posted_at=_dt(2026, 5, 25),
                        description='=HYPERLINK("http://evil.test","x")',
                        description_stem="HYPERLINK",
                        amount=-12.34,
                        classification="expense_or_purchase_needs_review",
                        expense_category="uncategorized",
                        category_reason="+SUM(1,2)",
                        match_reason="-cmd",
                        review_note="@risk",
                    )
                )
                session.commit()

                with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                    response = bank_reconciliation_export_csv(
                        request=object(),
                        import_id=1,
                        classification="",
                        expense_category="",
                        review_status="",
                        attention=False,
                        expenses_only=True,
                        search="",
                        session=session,
                    )

            exported_row = next(csv.DictReader(StringIO(response.body.decode("utf-8"))))
            self.assertTrue(exported_row["description"].startswith("'="))
            self.assertTrue(exported_row["category_reason"].startswith("'+"))
            self.assertTrue(exported_row["match_reason"].startswith("'-"))
            self.assertTrue(exported_row["review_note"].startswith("'@"))
        finally:
            engine.dispose()


class GmailReceiptReparseRouteTests(unittest.TestCase):
    def setUp(self):
        self.engine = _fresh_engine()

    def tearDown(self):
        self.engine.dispose()

    def _seed_receipt(self, session, *, message_id, trusted, reason, html_body=SORTSWIFT_REPARSE_HTML):
        from app.discord.gmail_financials import upsert_gmail_receipt_from_message

        receipt = upsert_gmail_receipt_from_message(
            session,
            gmail_message_id=message_id,
            thread_id=f"thread-{message_id}",
            sender="SortSwift Buylist <no-reply@mail.sortswift.com>",
            subject="Buylist Confirmation - Degen Collectibles",
            received_at=_dt(2026, 5, 19),
            html_body=html_body,
            snippet="Buylist Confirmation - Degen Collectibles",
            source_trusted=trusted,
            source_trust_reason=reason,
        )
        session.commit()
        session.refresh(receipt)
        return receipt

    def test_reparse_preserves_persisted_verified_trust_and_live_transaction(self):
        from app.routers.bookkeeping import gmail_receipt_reparse_form

        with Session(self.engine) as session:
            receipt = self._seed_receipt(
                session,
                message_id="gmail-route-reparse-trusted",
                trusted=True,
                reason="trusted_dmarc_aligned",
            )
            transaction_id = receipt.transaction_id

            with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                response = gmail_receipt_reparse_form(
                    request=object(),
                    receipt_id=receipt.id or 0,
                    session=session,
                )

            session.expire_all()
            reparsed = session.get(GmailReceipt, receipt.id)
            transaction = session.get(Transaction, transaction_id)

        self.assertEqual(response.status_code, 303)
        self.assertIn("success=Reparsed+Gmail+receipt", response.headers["location"])
        self.assertIsNotNone(reparsed)
        self.assertEqual(reparsed.status, "transaction_created")
        self.assertEqual(reparsed.transaction_id, transaction_id)
        self.assertIsNotNone(transaction)
        self.assertFalse(transaction.is_deleted)

    def test_reparse_keeps_persisted_untrusted_receipt_quarantined(self):
        from app.routers.bookkeeping import gmail_receipt_reparse_form

        with Session(self.engine) as session:
            receipt = self._seed_receipt(
                session,
                message_id="gmail-route-reparse-quarantined",
                trusted=False,
                reason="auth_no_aligned_pass",
            )

            with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                response = gmail_receipt_reparse_form(
                    request=object(),
                    receipt_id=receipt.id or 0,
                    session=session,
                )

            session.expire_all()
            reparsed = session.get(GmailReceipt, receipt.id)
            transactions = list(session.exec(select(Transaction)).all())

        self.assertEqual(response.status_code, 303)
        self.assertIsNotNone(reparsed)
        self.assertEqual(reparsed.status, "quarantined")
        self.assertIsNone(reparsed.transaction_id)
        self.assertEqual(transactions, [])

    def test_reparse_refuses_truncated_trusted_source_without_mutating_transaction(self):
        from app.routers.bookkeeping import gmail_receipt_reparse_form

        with Session(self.engine) as session:
            receipt = self._seed_receipt(
                session,
                message_id="gmail-route-reparse-truncated",
                trusted=True,
                reason="trusted_dmarc_aligned",
                html_body=_long_sortswift_reparse_html(),
            )
            transaction_id = receipt.transaction_id
            transaction = session.get(Transaction, transaction_id)
            before_amount = transaction.amount
            before_status = receipt.status
            before_link = receipt.transaction_id
            before_transaction_updated_at = transaction.updated_at
            before_receipt_updated_at = receipt.updated_at
            before_items = [
                item.item_name
                for item in session.exec(
                    select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
                ).all()
            ]

            with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                response = gmail_receipt_reparse_form(
                    request=object(),
                    receipt_id=receipt.id or 0,
                    session=session,
                )

            session.expire_all()
            reparsed = session.get(GmailReceipt, receipt.id)
            transaction = session.get(Transaction, transaction_id)
            after_items = [
                item.item_name
                for item in session.exec(
                    select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
                ).all()
            ]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(before_amount, 25.0)
        self.assertEqual(before_items, ["Charizard (Base)"])
        self.assertEqual(transaction.amount, before_amount)
        self.assertEqual(after_items, before_items)
        self.assertEqual(reparsed.status, before_status)
        self.assertEqual(reparsed.transaction_id, before_link)
        self.assertEqual(transaction.updated_at, before_transaction_updated_at)
        self.assertEqual(reparsed.updated_at, before_receipt_updated_at)
        self.assertFalse(transaction.is_deleted)
        self.assertIn("truncated", unquote(response.headers["location"]).lower())

    def test_reparse_refuses_legacy_source_at_storage_limit_without_mutation(self):
        from app.routers.bookkeeping import gmail_receipt_reparse_form

        with Session(self.engine) as session:
            receipt = self._seed_receipt(
                session,
                message_id="gmail-route-reparse-legacy-limit",
                trusted=True,
                reason="trusted_dmarc_aligned",
            )
            parsed = json.loads(receipt.parsed_json)
            parsed.pop("source_body_truncated", None)
            receipt.parsed_json = json.dumps(parsed, sort_keys=True)
            receipt.raw_text = (
                SORTSWIFT_REPARSE_HTML + (" " * GMAIL_RAW_SOURCE_STORAGE_LIMIT)
            )[:GMAIL_RAW_SOURCE_STORAGE_LIMIT]
            session.add(receipt)
            session.commit()
            transaction_id = receipt.transaction_id
            transaction = session.get(Transaction, transaction_id)
            before_amount = transaction.amount
            before_link = receipt.transaction_id
            before_transaction_updated_at = transaction.updated_at
            before_receipt_updated_at = receipt.updated_at

            with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                response = gmail_receipt_reparse_form(
                    request=object(),
                    receipt_id=receipt.id or 0,
                    session=session,
                )

            session.expire_all()
            reparsed = session.get(GmailReceipt, receipt.id)
            transaction = session.get(Transaction, transaction_id)

        self.assertEqual(response.status_code, 303)
        self.assertEqual(transaction.amount, before_amount)
        self.assertEqual(reparsed.transaction_id, before_link)
        self.assertEqual(transaction.updated_at, before_transaction_updated_at)
        self.assertEqual(reparsed.updated_at, before_receipt_updated_at)
        self.assertIn("truncated", unquote(response.headers["location"]).lower())

    def test_reparse_refuses_unknown_legacy_trust_without_mutating_financial_state(self):
        from app.routers.bookkeeping import gmail_receipt_reparse_form

        with Session(self.engine) as session:
            receipt = self._seed_receipt(
                session,
                message_id="gmail-route-reparse-unknown-trust",
                trusted=True,
                reason="trusted_dmarc_aligned",
            )
            transaction_id = receipt.transaction_id or 0
            transaction = session.get(Transaction, transaction_id)
            source = session.get(DiscordMessage, transaction.source_message_id)

            bookkeeping_import = BookkeepingImport(show_label="Legacy Gmail trust")
            bank_import = BankStatementImport(
                label="Legacy Gmail trust",
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
                description="Legacy Gmail match",
                amount=-10.0,
                classification="logged_in_discord_strong",
                confidence="high",
                matched_transaction_id=transaction_id,
                matched_source_message_id=source.id,
                matched_platform="gmail",
                match_reason="Original Gmail match",
            )
            session.add(bookkeeping_entry)
            session.add(bank_transaction)
            parsed = json.loads(receipt.parsed_json)
            parsed.pop("source_trusted", None)
            parsed.pop("source_trust_reason", None)
            receipt.parsed_json = json.dumps(parsed, sort_keys=True)
            session.add(receipt)
            session.commit()

            receipt_id = receipt.id or 0
            source_id = source.id or 0
            bookkeeping_entry_id = bookkeeping_entry.id or 0
            bank_transaction_id = bank_transaction.id or 0
            before_receipt = (
                receipt.status,
                receipt.transaction_id,
                receipt.updated_at,
                receipt.parsed_json,
            )
            before_transaction = (
                transaction.amount,
                transaction.is_deleted,
                transaction.parse_status,
                transaction.updated_at,
            )
            before_items = [
                (item.id, item.direction, item.item_name)
                for item in session.exec(
                    select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
                ).all()
            ]
            before_source = (
                source.is_deleted,
                source.deleted_at,
                source.parse_status,
                source.needs_review,
                source.content,
            )
            before_bookkeeping = (
                bookkeeping_entry.matched_transaction_id,
                bookkeeping_entry.match_status,
            )
            before_bank = (
                bank_transaction.matched_transaction_id,
                bank_transaction.matched_source_message_id,
                bank_transaction.matched_platform,
                bank_transaction.classification,
                bank_transaction.confidence,
                bank_transaction.match_reason,
                bank_transaction.updated_at,
            )

            with patch("app.routers.bookkeeping.require_role_response", return_value=None):
                response = gmail_receipt_reparse_form(
                    request=object(),
                    receipt_id=receipt_id,
                    session=session,
                )

            session.expire_all()
            receipt = session.get(GmailReceipt, receipt_id)
            transaction = session.get(Transaction, transaction_id)
            source = session.get(DiscordMessage, source_id)
            bookkeeping_entry = session.get(BookkeepingEntry, bookkeeping_entry_id)
            bank_transaction = session.get(BankTransaction, bank_transaction_id)
            after_items = [
                (item.id, item.direction, item.item_name)
                for item in session.exec(
                    select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
                ).all()
            ]

        self.assertEqual(
            (
                receipt.status,
                receipt.transaction_id,
                receipt.updated_at,
                receipt.parsed_json,
            ),
            before_receipt,
        )
        self.assertEqual(
            (
                transaction.amount,
                transaction.is_deleted,
                transaction.parse_status,
                transaction.updated_at,
            ),
            before_transaction,
        )
        self.assertEqual(after_items, before_items)
        self.assertEqual(
            (
                source.is_deleted,
                source.deleted_at,
                source.parse_status,
                source.needs_review,
                source.content,
            ),
            before_source,
        )
        self.assertEqual(
            (bookkeeping_entry.matched_transaction_id, bookkeeping_entry.match_status),
            before_bookkeeping,
        )
        self.assertEqual(
            (
                bank_transaction.matched_transaction_id,
                bank_transaction.matched_source_message_id,
                bank_transaction.matched_platform,
                bank_transaction.classification,
                bank_transaction.confidence,
                bank_transaction.match_reason,
                bank_transaction.updated_at,
            ),
            before_bank,
        )
        location = unquote_plus(response.headers["location"]).lower()
        self.assertIn("sync gmail", location)
        self.assertIn("trust evidence unavailable", location)


class ReconcileBookkeepingTests(unittest.TestCase):
    _tx_counter = 0

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_import(self, session, entries):
        bk_import = BookkeepingImport(
            show_label="Test Import",
            row_count=len(entries),
        )
        session.add(bk_import)
        session.commit()
        session.refresh(bk_import)

        for i, entry_data in enumerate(entries):
            entry = BookkeepingEntry(
                import_id=bk_import.id,
                row_index=i,
                **entry_data,
            )
            session.add(entry)
        session.commit()
        return bk_import.id

    def _seed_transaction(self, session, money_in=None, money_out=None, occurred_at=None, entry_kind="sale"):
        ReconcileBookkeepingTests._tx_counter += 1
        uid = ReconcileBookkeepingTests._tx_counter
        dm = DiscordMessage(
            discord_message_id=f"disc-bk-{uid}",
            channel_id="999",
            channel_name="deals",
            author_id="777",
            author_name="Trader#0001",
            content="$50 sale",
            attachment_urls_json="[]",
            parse_status=PARSE_PARSED,
            created_at=_utcnow(),
        )
        session.add(dm)
        session.commit()
        session.refresh(dm)

        tx = Transaction(
            source_message_id=dm.id,
            discord_message_id=f"disc-bk-{uid}",
            occurred_at=occurred_at or _dt(2024, 1, 15),
            parse_status=PARSE_PARSED,
            entry_kind=entry_kind,
            money_in=money_in,
            money_out=money_out,
            amount=money_in or money_out,
            is_deleted=False,
        )
        session.add(tx)
        session.commit()
        session.refresh(tx)
        return tx.id

    def test_matches_entry_to_transaction_by_amount(self):
        with Session(self.engine) as session:
            self._seed_transaction(session, money_in=50.0, occurred_at=_dt(2024, 1, 15))
            import_id = self._seed_import(session, [
                {"amount": 50.0, "occurred_at": _dt(2024, 1, 15), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        self.assertGreater(summary["matched_exact"] + summary["matched_amount_only"], 0)

    def test_unmatched_when_no_transactions(self):
        with Session(self.engine) as session:
            import_id = self._seed_import(session, [
                {"amount": 75.0, "occurred_at": _dt(2024, 3, 10), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        self.assertEqual(summary["matched_exact"], 0)
        self.assertEqual(summary["matched_amount_only"], 0)
        self.assertEqual(summary["unmatched_rows"], 1)

    def test_no_double_match(self):
        with Session(self.engine) as session:
            # One transaction, two entries with the same amount
            self._seed_transaction(session, money_in=100.0, occurred_at=_dt(2024, 2, 1))
            import_id = self._seed_import(session, [
                {"amount": 100.0, "occurred_at": _dt(2024, 2, 1), "entry_kind": "sale"},
                {"amount": 100.0, "occurred_at": _dt(2024, 2, 1), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        # Only one can match; the other must be unmatched
        self.assertEqual(summary["import_rows"], 2)
        self.assertLessEqual(summary["matched_rows"], 1)

    def test_raises_for_missing_import(self):
        with Session(self.engine) as session:
            with self.assertRaises(ValueError):
                reconcile_bookkeeping_import(session, 99999)


if __name__ == "__main__":
    unittest.main()
