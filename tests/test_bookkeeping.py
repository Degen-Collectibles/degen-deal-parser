import asyncio
import base64
import hashlib
import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, utils
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine


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

from app.discord.bookkeeping import fetch_google_sheet_export, read_tabular_rows, reconcile_bookkeeping_import
from app.models import (
    BookkeepingEntry,
    BookkeepingImport,
    DiscordMessage,
    Transaction,
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
                        headers={"location": "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx&gid=0"},
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
