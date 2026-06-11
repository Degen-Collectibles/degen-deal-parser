import asyncio
import hashlib
import hmac
import json
import shutil
import time
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import httpx
from fastapi import HTTPException
from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine, select

import app.db as db_module
import app.main as main_module
import app.routers.reports as reports_module
import app.routers.dashboard as dashboard_module
import app.routers.tiktok_orders as tiktok_orders_module
import app.routers.shopify as shopify_module
import app.routers.tiktok_streamer as streamer_module
import app.shared as shared_module
from app.models import AppSetting, TikTokAuth, TikTokCreatorAuth, TikTokOrder, TikTokWebhookEnrichmentJob, utcnow
from app.reporting import (
    build_tiktok_reporting_summary,
    classify_tiktok_reporting_status,
    get_tiktok_reporting_rows,
)
from scripts.tiktok_backfill import (
    affiliate_order_attribution_from_payload,
    backfill_tiktok_creator_affiliate_attributions,
    build_tiktok_request,
    fetch_tiktok_creator_affiliate_orders_page,
    request_json,
    upsert_tiktok_order,
    upsert_tiktok_order_affiliate_attribution,
    upsert_tiktok_order_creator_affiliate_attribution,
)
from app.tiktok.tiktok_ingest import (
    TIKTOK_DEFAULT_API_BASE_URL,
    TIKTOK_SHOP_AUTH_BASE_URL,
    TIKTOK_SHOP_TOKEN_GET_PATH,
    TikTokIngestError,
    build_tiktok_auth_record,
    build_tiktok_reconciliation_snapshot,
    exchange_tiktok_authorization_code,
    normalize_tiktok_order_payload,
    parse_tiktok_webhook_payload,
    refresh_tiktok_shop_token,
    upsert_tiktok_order_from_payload,
    verify_tiktok_webhook_signature,
)
from app.tiktok_enrichment_queue import (
    ENRICH_PROCESSING,
    enqueue_tiktok_webhook_enrichment,
    get_tiktok_webhook_enrichment_queue_counts,
    process_due_tiktok_webhook_enrichment_jobs,
    requeue_interrupted_tiktok_webhook_enrichment_jobs,
)


class FakeTikTokRequest:
    def __init__(
        self,
        path: str,
        *,
        query_params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> None:
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.session: dict[str, object] = {}
        self.url = SimpleNamespace(path=path)
        self._body = body

    async def body(self) -> bytes:
        return self._body


class FakeTikTokHTTPResponse:
    def __init__(self, payload: dict[str, object] | None = None) -> None:
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class FakeTikTokHTTPClient:
    def __init__(self, response_payload: dict[str, object] | None = None) -> None:
        self.response_payload = response_payload or {
            "code": 0,
            "message": "success",
            "data": {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "access_token_expire_in": 3600,
                "refresh_token_expire_in": 7200,
                "open_id": "open-1",
            },
        }
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> FakeTikTokHTTPResponse:
        self.calls.append((url, params or {}))
        return FakeTikTokHTTPResponse(self.response_payload)

    def post(
        self,
        url: str,
        data: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        json: dict[str, object] | None = None,
    ) -> FakeTikTokHTTPResponse:
        payload = data if data is not None else json if json is not None else {}
        self.calls.append((url, payload))
        return FakeTikTokHTTPResponse(self.response_payload)

    def close(self) -> None:
        self.closed = True


class TikTokRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        import app.cache as cache_module
        cache_module._cache.clear()
        # Other portal tests reload app.main; router modules keep their own
        # imported `settings` name. Re-bind those names so patches on
        # main_module.settings affect the modules under test.
        for module in (
            shared_module,
            reports_module,
            dashboard_module,
            shopify_module,
            tiktok_orders_module,
        ):
            module.settings = main_module.settings
        self.temp_dir = Path.cwd() / "tests" / ".tmp_tiktok_reporting" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "tiktok_reporting.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _immediate_to_thread(self, func, /, *args, **kwargs):
        return func(*args, **kwargs)

    def _reset_tiktok_state(self) -> None:
        main_module.update_tiktok_integration_state(
            last_callback=None,
            last_error=None,
            last_authorization_at=None,
            last_webhook_at=None,
            last_webhook=None,
            is_pull_running=False,
            last_pull_started_at=None,
            last_pull_finished_at=None,
            last_pull_at=None,
            last_pull={},
        )

    def test_tiktok_callback_missing_auth_config_redirects_and_records_session(self) -> None:
        oauth_state = "test-oauth-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "app_key": "expected-key",
                "code": "TTP_7uiSewAAAADOpEzqRGelGGdjsXyE7_hWWHsDgwDodg32Dzg_s9WqptBSVEn6mA7PoOxIUKykLtFMPQ2l8O8iSeSbgE4gyciq6gAnNKBzxC-nKFQorJowSPwPMiwHCMxaA5HeesYu_rNKKTt-tQiTAuUGsgupbg8o",
                "locale": "en",
                "shop_region": "US",
                "state": oauth_state,
            },
        )
        request.session["oauth_state"] = oauth_state

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", ""
        ), patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/status?error=TikTok+auth+config+missing%3A+app+secret",
        )
        self.assertIn("tiktok_callback", request.session)
        self.assertTrue(request.session["tiktok_callback"]["query"]["code"].startswith("TTP_7uiS"))
        update_tiktok_integration_state.assert_called_once()
        self.assertEqual(
            update_tiktok_integration_state.call_args.kwargs["last_error"],
            "TikTok auth config missing: app secret",
        )

    def test_tiktok_authorization_exchange_uses_shop_auth_endpoint(self) -> None:
        http_client = FakeTikTokHTTPClient()
        result = exchange_tiktok_authorization_code(
            auth_code="auth-code",
            app_key="app-key",
            app_secret="app-secret",
            client=http_client,  # type: ignore[arg-type]
        )

        self.assertFalse(http_client.closed)
        self.assertEqual(len(http_client.calls), 1)
        url, params = http_client.calls[0]
        self.assertEqual(url, f"{TIKTOK_SHOP_AUTH_BASE_URL}{TIKTOK_SHOP_TOKEN_GET_PATH}")
        self.assertIn("/api/v2/token/get", url)
        self.assertEqual(params["grant_type"], "authorized_code")
        self.assertEqual(params["app_key"], "app-key")
        self.assertEqual(params["app_secret"], "app-secret")
        self.assertEqual(params["auth_code"], "auth-code")
        self.assertEqual(result.access_token, "access-token")
        self.assertEqual(result.refresh_token, "refresh-token")
        self.assertEqual(result.open_id, "open-1")

    def test_tiktok_authorization_exchange_raises_on_error_shaped_payload_without_code_field(self) -> None:
        http_client = FakeTikTokHTTPClient(
            {
                "error": "invalid_grant",
                "error_description": "Authorization code is expired.",
                "log_id": "test-log-id",
            }
        )

        with self.assertRaises(TikTokIngestError) as ctx:
            exchange_tiktok_authorization_code(
                auth_code="auth-code",
                app_key="app-key",
                app_secret="app-secret",
                client=http_client,  # type: ignore[arg-type]
            )

        self.assertIn("Authorization code is expired", str(ctx.exception))

    def test_tiktok_shop_token_refresh_uses_shop_auth_endpoint(self) -> None:
        http_client = FakeTikTokHTTPClient()
        result = refresh_tiktok_shop_token(
            app_key="app-key",
            app_secret="app-secret",
            refresh_token="refresh-token",
            client=http_client,  # type: ignore[arg-type]
        )

        self.assertFalse(http_client.closed)
        self.assertEqual(len(http_client.calls), 1)
        url, params = http_client.calls[0]
        self.assertIn("auth.tiktok-shops.com", url)
        self.assertIn("/api/v2/token/refresh", url)
        self.assertEqual(params["grant_type"], "refresh_token")
        self.assertEqual(params["app_key"], "app-key")
        self.assertEqual(params["app_secret"], "app-secret")
        self.assertEqual(params["refresh_token"], "refresh-token")
        self.assertEqual(result.access_token, "access-token")
        self.assertEqual(result.refresh_token, "refresh-token")

    def test_tiktok_shop_token_refresh_raises_on_error_payload(self) -> None:
        http_client = FakeTikTokHTTPClient(
            {
                "code": 1,
                "message": "Invalid refresh token",
            }
        )

        with self.assertRaises(TikTokIngestError) as ctx:
            refresh_tiktok_shop_token(
                app_key="app-key",
                app_secret="app-secret",
                refresh_token="bad-token",
                client=http_client,  # type: ignore[arg-type]
            )

        self.assertIn("Invalid refresh token", str(ctx.exception))

    def test_tiktok_shop_oauth_start_wraps_service_authorization_link_with_session_state(self) -> None:
        request = FakeTikTokRequest("/integrations/tiktok/oauth/start")

        with patch.object(shopify_module, "require_role_response", return_value=None), patch.object(
            main_module.settings, "tiktok_app_key", "expected-key"
        ), patch.object(
            main_module.settings, "tiktok_redirect_uri", "https://ops.degencollectibles.com/integrations/tiktok/callback"
        ), patch.object(
            main_module.settings, "tiktok_service_id", ""
        ):
            response = shopify_module.tiktok_oauth_shop_start(
                request,  # type: ignore[arg-type]
                service_id="7623804575159174925",
            )

        self.assertEqual(response.status_code, 302)
        state = request.session.get("oauth_state")
        self.assertIsInstance(state, str)
        parsed = urlparse(response.headers["location"])
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "services.tiktokshops.us")
        self.assertEqual(parsed.path, "/open/authorize")
        params = parse_qs(parsed.query)
        self.assertEqual(params["service_id"], ["7623804575159174925"])
        self.assertEqual(params["state"], [state])

    def test_tiktok_shop_oauth_start_rejects_invalid_service_id(self) -> None:
        request = FakeTikTokRequest("/integrations/tiktok/oauth/start")

        with patch.object(shopify_module, "require_role_response", return_value=None), patch.object(
            main_module.settings, "tiktok_app_key", "expected-key"
        ), patch.object(
            main_module.settings, "tiktok_redirect_uri", "https://ops.degencollectibles.com/integrations/tiktok/callback"
        ):
            with self.assertRaises(HTTPException) as ctx:
                shopify_module.tiktok_oauth_shop_start(
                    request,  # type: ignore[arg-type]
                    service_id="https://services.tiktokshops.us/open/authorize?service_id=7623804575159174925",
                )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "TikTok service_id must be numeric")

    def test_tiktok_shop_creator_oauth_start_records_creator_for_service_callback(self) -> None:
        request = FakeTikTokRequest("/integrations/tiktok/oauth/creator-shop-start")

        with patch.object(shopify_module, "require_role_response", return_value=None), patch.object(
            main_module.settings, "tiktok_app_key", "expected-key"
        ), patch.object(
            main_module.settings, "tiktok_service_id", "7623804575159174925"
        ):
            response = shopify_module.tiktok_oauth_shop_creator_start(
                request,  # type: ignore[arg-type]
                creator="degenboss0",
                service_id=None,
            )

        self.assertEqual(response.status_code, 302)
        state = request.session.get("oauth_state")
        self.assertIsInstance(state, str)
        self.assertEqual(request.session["tiktok_oauth_kind"], "shop_creator")
        self.assertEqual(request.session["tiktok_oauth_creator_username"], "degenboss0")
        parsed = urlparse(response.headers["location"])
        self.assertEqual(parsed.netloc, "services.tiktokshops.us")
        params = parse_qs(parsed.query)
        self.assertEqual(params["service_id"], ["7623804575159174925"])
        self.assertEqual(params["state"], [state])

    def test_tiktok_callback_persists_shop_creator_token_separately(self) -> None:
        oauth_state = "creator-shop-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "code": "creator-auth-code",
                "state": oauth_state,
            },
        )
        request.session["oauth_state"] = oauth_state
        request.session["tiktok_oauth_kind"] = "shop_creator"
        request.session["tiktok_oauth_creator_username"] = "degenboss0"

        fake_token_result = SimpleNamespace(
            access_token="creator-access-token",
            refresh_token="creator-refresh-token",
            access_token_expires_at=None,
            refresh_token_expires_at=None,
            seller_id=None,
            shop_id=None,
            shop_cipher=None,
            open_id="creator-open-id",
            raw_payload={
                "user_type": 1,
                "open_id": "creator-open-id",
                "seller_name": "Degen Boss",
                "granted_scopes": ["creator.affiliate_collaboration.read"],
            },
        )

        def write_with_test_session(fn):
            with Session(self.engine) as session:
                result = fn(session)
                session.commit()
                return result

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "secret"
        ), patch.object(
            main_module.settings,
            "tiktok_redirect_uri",
            "https://ops.degencollectibles.com/integrations/tiktok/callback",
        ), patch.object(
            shopify_module,
            "exchange_tiktok_authorization_code",
            return_value=fake_token_result,
        ), patch.object(
            shopify_module,
            "run_write_with_retry",
            side_effect=write_with_test_session,
        ):
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/status?success=TikTok+creator+authorization+captured")
        with Session(self.engine) as session:
            seller_auth_count = len(session.exec(select(TikTokAuth)).all())
            creator_auth = session.exec(
                select(TikTokCreatorAuth).where(TikTokCreatorAuth.creator_username == "degenboss0")
            ).one()

        self.assertEqual(seller_auth_count, 0)
        self.assertEqual(creator_auth.open_id, "creator-open-id")
        self.assertEqual(creator_auth.access_token, "creator-access-token")
        self.assertIn("creator.affiliate_collaboration.read", creator_auth.scopes_json)

    def test_tiktok_shop_creator_callback_rejects_seller_token(self) -> None:
        oauth_state = "creator-shop-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "code": "seller-auth-code",
                "state": oauth_state,
            },
        )
        request.session["oauth_state"] = oauth_state
        request.session["tiktok_oauth_kind"] = "shop_creator"
        request.session["tiktok_oauth_creator_username"] = "degenboss0"

        fake_token_result = SimpleNamespace(
            access_token="seller-access-token",
            refresh_token="seller-refresh-token",
            access_token_expires_at=None,
            refresh_token_expires_at=None,
            seller_id=None,
            shop_id="dc-llc-shop",
            shop_cipher="dc-llc-cipher",
            open_id="seller-open-id",
            raw_payload={
                "user_type": 0,
                "open_id": "seller-open-id",
                "seller_name": "D.C. LLC",
                "granted_scopes": ["seller.affiliate_collaboration.read"],
            },
        )

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "secret"
        ), patch.object(
            shopify_module,
            "exchange_tiktok_authorization_code",
            return_value=fake_token_result,
        ), patch.object(
            shopify_module,
            "run_write_with_retry",
        ) as run_write_with_retry_mock, patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state_mock:
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/status?error=TikTok+creator+authorization+returned+a+seller+token",
        )
        run_write_with_retry_mock.assert_not_called()
        update_tiktok_integration_state_mock.assert_called_once()
        with Session(self.engine) as session:
            self.assertEqual(len(session.exec(select(TikTokCreatorAuth)).all()), 0)

    def test_tiktok_get_detail_requests_sign_empty_body(self) -> None:
        url, body_json, headers = build_tiktok_request(
            base_url="https://open-api.tiktokglobalshop.com",
            path="/order/202309/orders",
            app_key="app-key",
            app_secret="app-secret",
            shop_id="shop-1",
            shop_cipher="cipher-1",
            access_token="access-token",
            body=None,
            extra_query={"ids": "123"},
        )

        self.assertEqual(body_json, "")
        self.assertIn("sign=", url)
        self.assertEqual(headers["x-tts-access-token"], "access-token")

    def test_tiktok_post_search_requests_keep_json_body_for_signing(self) -> None:
        _, body_json, _ = build_tiktok_request(
            base_url="https://open-api.tiktokglobalshop.com",
            path="/order/202309/orders/search",
            app_key="app-key",
            app_secret="app-secret",
            shop_id="shop-1",
            shop_cipher="cipher-1",
            access_token="access-token",
            body={"create_time_ge": 123},
            extra_query={"page_size": "50"},
        )

        self.assertEqual(body_json, '{"create_time_ge":123}')

    def test_tiktok_request_json_redacts_access_token_from_http_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                401,
                json={"code": 105001, "message": "access token is invalid"},
                request=request,
            )

        url = "https://open-api.tiktokglobalshop.com/order/202309/orders/search?access_token=TTP_secret_token&app_key=app-key"
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(httpx.HTTPStatusError) as ctx:
                request_json(client, method="POST", url=url, raw_body="{}")

        error_text = str(ctx.exception)
        self.assertNotIn("TTP_secret_token", error_text)
        self.assertIn("access_token=REDACTED", error_text)

    def test_tiktok_callback_success_exchanges_code_with_shop_auth_endpoint(self) -> None:
        oauth_state = "test-oauth-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "app_key": "expected-key",
                "code": "auth-code",
                "locale": "en",
                "shop_region": "US",
                "state": oauth_state,
            },
        )
        request.session["oauth_state"] = oauth_state

        fake_token_result = SimpleNamespace(
            access_token="access-token",
            refresh_token="refresh-token",
            access_token_expires_at=None,
            refresh_token_expires_at=None,
            seller_id="seller-1",
            shop_id="shop-1",
            shop_cipher="cipher-1",
            open_id=None,
            raw_payload={"shop_region": "US"},
        )

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "secret"
        ), patch.object(
            main_module.settings,
            "tiktok_redirect_uri",
            "https://ops.degencollectibles.com/integrations/tiktok/callback",
        ), patch.object(
            shopify_module,
            "exchange_tiktok_authorization_code",
            return_value=fake_token_result,
        ) as exchange_tiktok_authorization_code_mock, patch.object(
            shopify_module,
            "run_write_with_retry",
            return_value=(
                "inserted",
                {"tiktok_shop_id": "shop-1", "shop_region": "US"},
                SimpleNamespace(shop_name=None),
            ),
        ), patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/status?success=TikTok+authorization+captured")
        exchange_tiktok_authorization_code_mock.assert_called_once()
        self.assertEqual(exchange_tiktok_authorization_code_mock.call_args.kwargs["auth_code"], "auth-code")
        self.assertEqual(exchange_tiktok_authorization_code_mock.call_args.kwargs["app_key"], "expected-key")
        self.assertNotIn("redirect_uri", exchange_tiktok_authorization_code_mock.call_args.kwargs)
        self.assertNotIn("api_base_url", exchange_tiktok_authorization_code_mock.call_args.kwargs)
        update_tiktok_integration_state.assert_called_once()
        self.assertEqual(update_tiktok_integration_state.call_args.kwargs["last_error"], None)
        self.assertEqual(request.session["tiktok_callback"]["auth_status"], "inserted")
        self.assertEqual(request.session["tiktok_callback"]["shop_id"], "shop-1")

    def test_tiktok_callback_success_with_pending_shop_identifier(self) -> None:
        oauth_state = "test-oauth-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "app_key": "expected-key",
                "code": "auth-code",
                "locale": "en",
                "shop_region": "US",
                "state": oauth_state,
            },
        )
        request.session["oauth_state"] = oauth_state

        fake_token_result = SimpleNamespace(
            access_token="access-token",
            refresh_token="refresh-token",
            access_token_expires_at=None,
            refresh_token_expires_at=None,
            seller_id=None,
            shop_id=None,
            shop_cipher=None,
            open_id=None,
            raw_payload={"shop_region": "US"},
        )

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "secret"
        ), patch.object(
            main_module.settings,
            "tiktok_redirect_uri",
            "https://ops.degencollectibles.com/integrations/tiktok/callback",
        ), patch.object(
            shopify_module,
            "exchange_tiktok_authorization_code",
            return_value=fake_token_result,
        ), patch.object(
            shopify_module,
            "run_write_with_retry",
            return_value=(
                "inserted",
                {"tiktok_shop_id": "pending:abc123", "shop_region": "US"},
                SimpleNamespace(shop_name=None),
            ),
        ), patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/status?success=TikTok+authorization+captured%3B+waiting+for+shop+identifier",
        )
        update_tiktok_integration_state.assert_called_once()
        self.assertEqual(update_tiktok_integration_state.call_args.kwargs["last_error"], None)
        self.assertEqual(request.session["tiktok_callback"]["auth_status"], "inserted")
        self.assertEqual(request.session["tiktok_callback"]["shop_key_status"], "pending")
        self.assertEqual(request.session["tiktok_callback"]["pending_shop_key"], "pending:abc123")
        self.assertNotIn("shop_id", request.session["tiktok_callback"])

    def test_tiktok_callback_missing_code_redirects_with_error(self) -> None:
        oauth_state = "test-oauth-state"
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={"app_key": "expected-key", "state": oauth_state},
        )
        request.session["oauth_state"] = oauth_state

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"), patch.object(
            main_module.settings, "tiktok_app_secret", ""
        ), patch.object(
            main_module.settings, "tiktok_redirect_uri", ""
        ), patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/status?error=TikTok+callback+missing+authorization+code")
        update_tiktok_integration_state.assert_called_once()
        self.assertEqual(update_tiktok_integration_state.call_args.kwargs["last_error"], "Missing authorization code")

    def test_tiktok_callback_invalid_oauth_state_returns_403(self) -> None:
        request = FakeTikTokRequest(
            "/integrations/tiktok/callback",
            query_params={
                "app_key": "expected-key",
                "code": "auth-code",
                "state": "from-callback",
            },
        )
        request.session["oauth_state"] = "session-state-mismatch"

        with patch.object(main_module.settings, "tiktok_app_key", "expected-key"):
            with self.assertRaises(HTTPException) as ctx:
                shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(ctx.exception.detail, "Invalid OAuth state")

    def test_tiktok_webhook_missing_secret_rejects_payload(self) -> None:
        body = json.dumps({"order_id": "tt-1", "status": "PAID"}).encode("utf-8")
        request = FakeTikTokRequest(
            "/webhooks/tiktok/orders",
            headers={"X-TikTok-Topic": "order.status.change"},
            body=body,
        )

        with patch.object(main_module.settings, "tiktok_app_secret", ""), patch.object(
            main_module.settings, "tiktok_shop_id", ""
        ), patch.object(
            tiktok_orders_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(tiktok_orders_module.tiktok_orders_webhook(request))

        self.assertEqual(ctx.exception.status_code, 400)
        update_tiktok_integration_state.assert_called_once()

    def test_tiktok_webhook_uses_app_secret_when_webhook_secret_is_blank(self) -> None:
        secret = "app-secret"
        timestamp = str(int(time.time()))
        body = json.dumps({"order_id": "tt-3", "status": "PAID"}).encode("utf-8")
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        request = FakeTikTokRequest(
            "/webhooks/tiktok/orders",
            headers={
                "X-TikTok-Topic": "order.status.change",
                "x-tiktok-signature": signature,
                "x-tiktok-timestamp": timestamp,
            },
            body=body,
        )

        with patch.object(main_module.settings, "tiktok_app_secret", secret), patch.object(
            tiktok_orders_module,
            "run_write_with_retry",
            return_value=("inserted", {"tiktok_order_id": "tt-3", "shop_id": "shop-1"}),
        ), patch.object(
            tiktok_orders_module.asyncio,
            "to_thread",
            side_effect=self._immediate_to_thread,
        ), patch.object(
            tiktok_orders_module,
            "_start_tiktok_webhook_enrichment",
        ) as start_tiktok_webhook_enrichment, patch.object(
            tiktok_orders_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = asyncio.run(tiktok_orders_module.tiktok_orders_webhook(request))

        self.assertEqual(response.status_code, 200)
        start_tiktok_webhook_enrichment.assert_called_once_with("tt-3")
        update_tiktok_integration_state.assert_called_once()

    def test_tiktok_webhook_accepts_signed_json_and_stubs_background_enrichment(self) -> None:
        secret = "app-secret"
        timestamp = str(int(time.time()))
        body = json.dumps({"order_id": "tt-2", "status": "PAID"}).encode("utf-8")
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        request = FakeTikTokRequest(
            "/webhooks/tiktok/orders",
            headers={
                "X-TikTok-Topic": "order.status.change",
                "x-tiktok-signature": signature,
                "x-tiktok-timestamp": timestamp,
            },
            body=body,
        )

        with patch.object(main_module.settings, "tiktok_app_secret", secret), patch.object(
            tiktok_orders_module,
            "run_write_with_retry",
            return_value=("updated", {"tiktok_order_id": "tt-2", "shop_id": "shop-1"}),
        ), patch.object(
            tiktok_orders_module.asyncio,
            "to_thread",
            side_effect=self._immediate_to_thread,
        ), patch.object(
            tiktok_orders_module,
            "_start_tiktok_webhook_enrichment",
        ) as start_tiktok_webhook_enrichment, patch.object(
            tiktok_orders_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            response = asyncio.run(tiktok_orders_module.tiktok_orders_webhook(request))

        self.assertEqual(response.status_code, 200)
        start_tiktok_webhook_enrichment.assert_called_once_with("tt-2")
        update_tiktok_integration_state.assert_called_once()
        self.assertEqual(update_tiktok_integration_state.call_args.kwargs["last_webhook"]["topic"], "order.status.change")
        self.assertEqual(update_tiktok_integration_state.call_args.kwargs["last_webhook"]["tiktok_order_id"], "tt-2")

    def test_tiktok_webhook_invalid_json_returns_400_and_records_error(self) -> None:
        request = FakeTikTokRequest(
            "/webhooks/tiktok/orders",
            headers={"X-TikTok-Topic": "order.status.change"},
            body=b"not-json",
        )

        with patch.object(main_module.settings, "tiktok_app_secret", "configured-secret"), patch.object(
            tiktok_orders_module, "update_tiktok_integration_state"
        ) as update_tiktok_integration_state:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(tiktok_orders_module.tiktok_orders_webhook(request))

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "Invalid TikTok webhook payload")
        update_tiktok_integration_state.assert_called_once()
        self.assertIn("not valid JSON", update_tiktok_integration_state.call_args.kwargs["last_error"])

    def test_tiktok_webhook_signature_parsing_accepts_timestamped_payload(self) -> None:
        secret = "super-secret"
        timestamp = str(int(time.time()))
        raw_body = json.dumps({"order_id": "tt-1", "status": "PAID", "timestamp": timestamp}, separators=(",", ":")).encode("utf-8")
        expected_digest = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).digest()
        headers = {
            "x-tiktok-signature": expected_digest.hex(),
            "x-tiktok-timestamp": timestamp,
            "x-tiktok-topic": "order.status.change",
        }

        self.assertTrue(
            verify_tiktok_webhook_signature(
                raw_body=raw_body,
                app_secret=secret,
                received_signature=expected_digest.hex(),
                received_timestamp=timestamp,
            )
        )
        parsed = parse_tiktok_webhook_payload(raw_body, app_secret=secret, headers=headers)
        self.assertEqual(parsed["order_id"], "tt-1")
        self.assertEqual(parsed["status"], "PAID")

    def test_tiktok_webhook_signature_parsing_accepts_combined_header_format(self) -> None:
        secret = "super-secret"
        timestamp = str(int(time.time()))
        raw_body = b'{"order_id":"tt-9","status":"PAID"}'
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "TikTok-Signature": f"t={timestamp},s={signature}",
            "X-TikTok-Topic": "order.status.change",
        }

        parsed = parse_tiktok_webhook_payload(raw_body, app_secret=secret, headers=headers)
        self.assertEqual(parsed["order_id"], "tt-9")
        self.assertEqual(parsed["status"], "PAID")

    def test_tiktok_webhook_prefers_tiktok_signature_pair_over_mismatched_split_headers(self) -> None:
        """TikTok-Signature t+s must stay paired; a bogus x-tiktok-timestamp must not break verification."""
        secret = "app-secret"
        raw_body = json.dumps({"order_id": "tt-pair", "status": "PAID"}, separators=(",", ":")).encode("utf-8")
        good_ts = str(int(time.time()))
        bad_ts = "9999999999"
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{good_ts}.{raw_body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "x-tiktok-signature": signature,
            "x-tiktok-timestamp": bad_ts,
            "TikTok-Signature": f"t={good_ts},s={signature}",
            "X-TikTok-Topic": "order.status.change",
        }
        parsed = parse_tiktok_webhook_payload(raw_body, app_secret=secret, headers=headers)
        self.assertEqual(parsed["order_id"], "tt-pair")

    def test_tiktok_webhook_accepts_uppercase_hex_signature(self) -> None:
        secret = "app-secret"
        timestamp = str(int(time.time()))
        raw_body = b'{"order_id":"tt-upper","status":"PAID"}'
        digest = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "x-tiktok-signature": digest.upper(),
            "x-tiktok-timestamp": timestamp,
            "X-TikTok-Topic": "order.status.change",
        }
        parsed = parse_tiktok_webhook_payload(raw_body, app_secret=secret, headers=headers)
        self.assertEqual(parsed["order_id"], "tt-upper")

    def test_tiktok_order_payload_normalization_and_reconciliation_snapshot(self) -> None:
        normalized = normalize_tiktok_order_payload(
            {
                "order_id": "tt-1",
                "order_sn": "#1001",
                "create_time": "2026-04-01T08:00:00Z",
                "update_time": "2026-04-01T08:15:00Z",
                "buyer_name": "Casey",
                "buyer_email": "casey@example.com",
                "pay_amount": "12.5",
                "tax_amount": "1.5",
                "financial_status": "paid",
                "shipping_status": "shipped",
                "skus": [
                    {
                        "creator_username": "degenboss0",
                        "content_type": "LIVE",
                        "content_id": "live-123",
                    }
                ],
                "sku_list": [
                    {"product_name": "Charizard", "quantity": 2, "price": "5.00"},
                    {"sku_name": "Pikachu", "qty": 1, "sale_price": "2.50"},
                ],
            },
            source="webhook",
        )

        snapshot = build_tiktok_reconciliation_snapshot(normalized)

        self.assertEqual(normalized["tiktok_order_id"], "tt-1")
        self.assertEqual(normalized["order_number"], "#1001")
        self.assertEqual(normalized["total_price"], 12.5)
        self.assertEqual(json.loads(normalized["line_items_summary_json"])[0]["title"], "Charizard")
        self.assertEqual(normalized["affiliate_creator_username"], "degenboss0")
        self.assertEqual(normalized["affiliate_content_type"], "LIVE")
        self.assertEqual(normalized["affiliate_content_id"], "live-123")
        self.assertEqual(snapshot["line_item_count"], 2)
        self.assertEqual(snapshot["customer_name"], "Casey")

    def test_tiktok_order_envelope_merges_parent_shop_id_into_data(self) -> None:
        """Shop webhooks often send shop_id on the envelope and order fields under data."""
        normalized = normalize_tiktok_order_payload(
            {
                "type": 1,
                "tts_notification_id": "7625789752946231054",
                "shop_id": "7495987383262087496",
                "timestamp": 1775530257,
                "data": {
                    "is_on_hold_order": False,
                    "order_id": "577299788258775181",
                    "order_status": "COMPLETED",
                    "update_time": 1775517537,
                },
            },
            source="webhook",
        )
        self.assertEqual(normalized["tiktok_order_id"], "577299788258775181")
        self.assertEqual(normalized["shop_id"], "7495987383262087496")
        self.assertEqual(normalized["financial_status"], "")
        self.assertEqual(normalized["order_status"], "COMPLETED")

    def test_tiktok_order_upsert_persists_and_updates_existing_row(self) -> None:
        with Session(self.engine) as session:
            status, record = upsert_tiktok_order_from_payload(
                session,
                TikTokOrder,
                {
                    "order_id": "tt-1",
                    "order_sn": "#1001",
                    "create_time": "2026-04-01T08:00:00Z",
                    "update_time": "2026-04-01T08:15:00Z",
                    "financial_status": "paid",
                    "pay_amount": "12.5",
                    "sku_list": [{"product_name": "Charizard", "quantity": 1, "price": "12.50"}],
                },
            )
            self.assertEqual(status, "inserted")
            session.commit()

            status, record = upsert_tiktok_order_from_payload(
                session,
                TikTokOrder,
                {
                    "order_id": "tt-1",
                    "order_sn": "#1001",
                    "create_time": "2026-04-01T08:00:00Z",
                    "update_time": "2026-04-01T09:00:00Z",
                    "financial_status": "paid",
                    "pay_amount": "15.0",
                    "sku_list": [{"product_name": "Charizard", "quantity": 1, "price": "15.00"}],
                },
            )
            self.assertEqual(status, "updated")
            session.commit()

            stored = session.exec(select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "tt-1")).first()

        self.assertIsNotNone(stored)
        self.assertEqual(stored.total_price, 15.0)
        self.assertEqual(json.loads(stored.line_items_summary_json)[0]["unit_price"], 15.0)
        self.assertEqual(record["tiktok_order_id"], "tt-1")

    def test_tiktok_affiliate_order_payload_updates_creator_attribution(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="577419230985949431",
                    order_number="577419230985949431",
                    created_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    financial_status="paid",
                    subtotal_price=4.0,
                    total_price=4.19,
                )
            )
            session.commit()

            payload = {
                "id": "577419230985949431",
                "status": "PROCESSING",
                "create_time": 1780604364,
                "skus": [
                    {
                        "creator_username": "degenboss0",
                        "content_type": "LIVE",
                        "content_id": "7493990579714164574",
                        "product_name": "NIHIL ZERO PACK",
                        "product_id": "1729435310697057093",
                        "quantity": 1,
                    }
                ],
            }

            attribution = affiliate_order_attribution_from_payload(payload)
            result = upsert_tiktok_order_affiliate_attribution(session, payload)
            session.commit()
            stored = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "577419230985949431")
            ).one()

        self.assertEqual(attribution["affiliate_creator_username"], "degenboss0")
        self.assertEqual(result, "updated")
        self.assertEqual(stored.affiliate_creator_username, "degenboss0")
        self.assertEqual(stored.affiliate_content_type, "LIVE")
        self.assertEqual(stored.affiliate_content_id, "7493990579714164574")
        self.assertIn("NIHIL ZERO PACK", stored.affiliate_attribution_json)

    def test_tiktok_creator_affiliate_order_search_uses_creator_token_without_shop_cipher(self) -> None:
        captured_requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "success",
                    "data": {
                        "orders": [
                            {
                                "order_id": "577419230985949431",
                                "product_id": "1729435310697057093",
                            }
                        ],
                        "next_page_token": "next-page",
                    },
                },
            )

        since = datetime(2026, 6, 4, 20, 0, tzinfo=timezone.utc)
        until = datetime(2026, 6, 4, 21, 0, tzinfo=timezone.utc)
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            payload, orders = fetch_tiktok_creator_affiliate_orders_page(
                client,
                base_url="https://open-api.tiktokglobalshop.com",
                app_key="app-key",
                app_secret="secret",
                access_token="creator-access-token",
                since=since,
                until=until,
                page_size=100,
            )

        self.assertEqual(payload["code"], 0)
        self.assertEqual(orders[0]["order_id"], "577419230985949431")
        self.assertEqual(len(captured_requests), 1)
        parsed = urlparse(str(captured_requests[0].url))
        self.assertEqual(parsed.path, "/affiliate_creator/202410/orders/search")
        params = parse_qs(parsed.query)
        self.assertNotIn("shop_cipher", params)
        self.assertEqual(params["app_key"], ["app-key"])
        self.assertEqual(captured_requests[0].headers["x-tts-access-token"], "creator-access-token")
        self.assertEqual(
            captured_requests[0].content,
            b'{"create_time_ge":1780603200,"create_time_lt":1780606800}',
        )

    def test_tiktok_creator_affiliate_trace_marks_shared_shop_order_with_creator(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="577419230985949431",
                    shop_id="dc-llc-shop",
                    shop_cipher="shared-shop-cipher",
                    order_number="577419230985949431",
                    created_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    financial_status="paid",
                    subtotal_price=4.0,
                    total_price=4.19,
                    line_items_json=json.dumps([
                        {
                            "product_id": "1729435310697057093",
                            "product_name": "NIHIL ZERO PACK",
                            "quantity": 1,
                            "sale_price": 4.0,
                        }
                    ]),
                )
            )
            session.commit()

            result = upsert_tiktok_order_creator_affiliate_attribution(
                session,
                {
                    "order_id": "577419230985949431",
                    "product_id": "1729435310697057093",
                    "content_type": "LIVE",
                    "content_id": "boss-live-123",
                },
                creator_username="degenboss0",
            )
            session.commit()
            stored = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "577419230985949431")
            ).one()

        self.assertEqual(result, "updated")
        self.assertEqual(stored.affiliate_creator_username, "degenboss0")
        self.assertEqual(stored.affiliate_content_type, "LIVE")
        self.assertEqual(stored.affiliate_content_id, "boss-live-123")
        self.assertIn("1729435310697057093", stored.affiliate_attribution_json)

    def test_tiktok_creator_affiliate_backfill_reports_per_creator_telemetry(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="creator-trace-order",
                    shop_id="dc-llc-shop",
                    shop_cipher="shared-shop-cipher",
                    order_number="creator-trace-order",
                    created_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 6, 4, 20, 19, tzinfo=timezone.utc),
                    financial_status="paid",
                    subtotal_price=8.0,
                    total_price=8.0,
                    line_items_json=json.dumps([
                        {"product_id": "trace-product", "product_name": "Trace Product", "quantity": 1}
                    ]),
                )
            )
            session.add(
                TikTokCreatorAuth(
                    creator_username="degencollectibles",
                    app_key="app-key",
                    access_token="creator-access-token",
                    refresh_token="creator-refresh-token",
                    access_token_expires_at=datetime(2026, 6, 5, tzinfo=timezone.utc),
                    refresh_token_expires_at=datetime(2026, 12, 1, tzinfo=timezone.utc),
                    scopes_json=json.dumps(["creator.affiliate_collaboration.read"]),
                    updated_at=datetime(2026, 6, 4, 20, 0, tzinfo=timezone.utc),
                )
            )
            session.add(
                TikTokCreatorAuth(
                    creator_username="degenboss0",
                    app_key="app-key",
                    access_token="boss-access-token",
                    refresh_token="boss-refresh-token",
                    access_token_expires_at=datetime(2026, 6, 5, tzinfo=timezone.utc),
                    refresh_token_expires_at=datetime(2026, 12, 1, tzinfo=timezone.utc),
                    scopes_json=json.dumps(["data.analytics.public.read"]),
                    updated_at=datetime(2026, 6, 4, 19, 0, tzinfo=timezone.utc),
                )
            )
            session.commit()

            def fake_fetch_page(*args, **kwargs):
                return (
                    {"code": 0, "data": {"orders": []}},
                    [
                        {
                            "order_id": "creator-trace-order",
                            "product_id": "trace-product",
                            "content_type": "LIVE",
                            "content_id": "main-live",
                        }
                    ],
                )

            telemetry: dict[str, dict[str, object]] = {}

            def capture_telemetry(creator_handle: str, summary) -> None:
                telemetry[creator_handle] = {
                    "attributed": summary.affiliate_attributed,
                    "missing": summary.affiliate_missing,
                    "failed": summary.affiliate_failed,
                    "scope_missing": summary.affiliate_scope_missing,
                    "last_error": summary.last_error,
                }

            with patch("scripts.tiktok_backfill.fetch_tiktok_creator_affiliate_orders_page", side_effect=fake_fetch_page):
                summary = backfill_tiktok_creator_affiliate_attributions(
                    session,
                    base_url="https://open-api.tiktokglobalshop.com",
                    app_key="app-key",
                    app_secret="secret",
                    since=datetime(2026, 6, 4, tzinfo=timezone.utc),
                    until=datetime(2026, 6, 5, tzinfo=timezone.utc),
                    telemetry_callback=capture_telemetry,
                )

        self.assertEqual(summary.affiliate_attributed, 1)
        self.assertTrue(summary.affiliate_scope_missing)
        self.assertEqual(telemetry["degencollectibles"]["attributed"], 1)
        self.assertFalse(telemetry["degencollectibles"]["scope_missing"])
        self.assertEqual(telemetry["degenboss0"]["attributed"], 0)
        self.assertTrue(telemetry["degenboss0"]["scope_missing"])

    def test_status_snapshot_includes_viewer_safe_creator_attribution(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokCreatorAuth(
                    creator_username="degenboss0",
                    display_name="Boss",
                    app_key="app-key",
                    access_token="creator-access-token-secret",
                    refresh_token="creator-refresh-token-secret",
                    access_token_expires_at=utcnow() + timedelta(hours=6),
                    refresh_token_expires_at=utcnow() + timedelta(days=30),
                    scopes_json=json.dumps(["creator.affiliate_collaboration.read"]),
                    raw_payload=json.dumps({"access_token": "raw-secret-token"}),
                    updated_at=utcnow(),
                )
            )
            session.add(
                AppSetting(
                    key="tiktok_creator_trace_status",
                    value=json.dumps(
                        {
                            "degenboss0": {
                                "last_trace_pull_at": "2026-06-10T20:00:00+00:00",
                                "affiliate_attributed": 8,
                                "affiliate_missing": 1,
                                "affiliate_failed": 0,
                                "affiliate_scope_missing": False,
                                "last_error": "scope failed with creator-access-token-secret",
                            }
                        }
                    ),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="attributed-order",
                    order_number="attributed-order",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    subtotal_price=10.0,
                    total_price=10.0,
                    affiliate_creator_username="degenboss0",
                )
            )
            session.commit()

            snapshot = shared_module.build_status_snapshot(session)
            admin_status = shared_module.build_tiktok_creator_attribution_status(
                session,
                include_admin_details=True,
            )

        creator_status = snapshot["tiktok_creator_attribution"]
        self.assertTrue(creator_status["affiliate_order_scope_authorized"])
        self.assertEqual(creator_status["creators"][0]["handle"], "degenboss0")
        self.assertTrue(creator_status["creators"][0]["scope_ok"])
        self.assertEqual(creator_status["creators"][0]["last_trace_attributed_count"], 8)
        self.assertEqual(
            set(creator_status["creators"][0].keys()),
            {
                "handle",
                "scope_ok",
                "access_expired",
                "refresh_expired",
                "last_trace_pull_at",
                "last_trace_attributed_count",
            },
        )
        serialized = json.dumps(creator_status)
        self.assertNotIn("creator-access-token-secret", serialized)
        self.assertNotIn("creator-refresh-token-secret", serialized)
        self.assertNotIn("raw-secret-token", serialized)
        self.assertNotIn("scope failed", serialized)
        self.assertNotIn("scopes_json", serialized)
        admin_serialized = json.dumps(admin_status)
        self.assertNotIn("creator-access-token-secret", admin_serialized)
        self.assertIn("[REDACTED]", admin_serialized)

    def test_streamer_config_renders_creator_attribution_empty_state(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="admin")))
        with Session(self.engine) as session:
            with patch.object(streamer_module, "require_role_response", return_value=None):
                response = streamer_module.tiktok_streamer_config(request, session)

        html = response.body.decode("utf-8")
        self.assertIn("Creator Attribution", html)
        self.assertIn("No creator authorizations", html)
        self.assertIn("/docs/ops/tiktok-creator-attribution-runbook.md", html)
        self.assertIn("/integrations/tiktok/oauth/creator-shop-start?creator=degenboss0", html)

    def test_sqlite_schema_migration_creates_tiktok_webhook_enrichment_queue_table(self) -> None:
        with self.engine.begin() as connection:
            connection.execute(text("DROP TABLE tiktok_webhook_enrichment_jobs"))

        original_engine = db_module.engine
        original_database_url = db_module.database_url
        try:
            db_module.engine = self.engine
            db_module.database_url = str(self.engine.url)
            db_module.ensure_sqlite_schema()
        finally:
            db_module.engine = original_engine
            db_module.database_url = original_database_url

        with self.engine.begin() as connection:
            table_row = connection.execute(
                text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'tiktok_webhook_enrichment_jobs'"
                )
            ).first()
            index_rows = connection.execute(
                text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'index' AND tbl_name = 'tiktok_webhook_enrichment_jobs'"
                )
            ).all()

        self.assertIsNotNone(table_row)
        self.assertIn(
            "ix_tiktok_webhook_enrichment_jobs_tiktok_order_id",
            {row[0] for row in index_rows},
        )

    def test_tiktok_webhook_enrichment_queue_persists_and_deduplicates_jobs(self) -> None:
        due_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            first = enqueue_tiktok_webhook_enrichment(session, "tt-durable-1", now=due_at)
            second = enqueue_tiktok_webhook_enrichment(session, "tt-durable-1", now=due_at)
            session.commit()

            self.assertEqual(first.tiktok_order_id, "tt-durable-1")
            self.assertEqual(second.id, first.id)
            rows = session.exec(select(TikTokWebhookEnrichmentJob)).all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].status, "pending")

        with Session(self.engine) as session:
            persisted = session.exec(
                select(TikTokWebhookEnrichmentJob).where(
                    TikTokWebhookEnrichmentJob.tiktok_order_id == "tt-durable-1"
                )
            ).one()
            self.assertEqual(persisted.status, "pending")

    def test_tiktok_webhook_enrichment_queue_retries_transient_failure_then_succeeds(self) -> None:
        started_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        calls: list[str] = []

        with Session(self.engine) as session:
            enqueue_tiktok_webhook_enrichment(session, "tt-retry-1", now=started_at)
            session.commit()

            def fail_once(order_id: str) -> None:
                calls.append(order_id)
                raise RuntimeError("temporary TikTok API outage")

            processed = process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=started_at,
                enrich_fn=fail_once,
            )
            session.commit()

            self.assertEqual(processed, 1)
            self.assertEqual(calls, ["tt-retry-1"])
            job = session.exec(select(TikTokWebhookEnrichmentJob)).one()
            self.assertEqual(job.status, "pending")
            self.assertEqual(job.attempts, 1)
            self.assertIsNotNone(job.next_attempt_at)
            assert job.next_attempt_at is not None
            self.assertGreater(
                job.next_attempt_at.replace(tzinfo=timezone.utc),
                started_at,
            )
            self.assertIn("temporary TikTok API outage", job.last_error or "")

            calls.clear()
            skipped = process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=started_at + timedelta(seconds=1),
                enrich_fn=lambda order_id: calls.append(order_id),
            )
            self.assertEqual(skipped, 0)
            self.assertEqual(calls, [])

            retried = process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=started_at + timedelta(minutes=10),
                enrich_fn=lambda order_id: calls.append(order_id),
            )
            session.commit()

            self.assertEqual(retried, 1)
            self.assertEqual(calls, ["tt-retry-1"])
            session.refresh(job)
            self.assertEqual(job.status, "succeeded")
            self.assertEqual(job.last_error, "")

    def test_tiktok_webhook_enrichment_queue_duplicate_preserves_pending_backoff(self) -> None:
        started_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        duplicate_at = started_at + timedelta(seconds=10)
        with Session(self.engine) as session:
            enqueue_tiktok_webhook_enrichment(session, "tt-backoff-1", now=started_at)
            process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=started_at,
                enrich_fn=lambda order_id: (_ for _ in ()).throw(RuntimeError("slow down")),
            )
            job = session.exec(select(TikTokWebhookEnrichmentJob)).one()
            first_retry_at = job.next_attempt_at
            assert first_retry_at is not None

            duplicate = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-backoff-1",
                now=duplicate_at,
            )
            session.commit()

            self.assertEqual(duplicate.status, "pending")
            self.assertEqual(duplicate.attempts, 1)
            self.assertEqual(duplicate.next_attempt_at, first_retry_at)

            calls: list[str] = []
            processed = process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=duplicate_at,
                enrich_fn=lambda order_id: calls.append(order_id),
            )
            self.assertEqual(processed, 0)
            self.assertEqual(calls, [])

    def test_tiktok_webhook_enrichment_queue_duplicate_preserves_processing_claim(self) -> None:
        now = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            job = enqueue_tiktok_webhook_enrichment(session, "tt-processing-1", now=now)
            job.status = ENRICH_PROCESSING
            job.last_attempt_at = now
            job.next_attempt_at = now
            session.add(job)
            session.commit()

            duplicate = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-processing-1",
                now=now + timedelta(seconds=5),
            )
            session.commit()

            self.assertEqual(duplicate.status, ENRICH_PROCESSING)
            self.assertIsNotNone(duplicate.last_attempt_at)
            self.assertIsNotNone(duplicate.next_attempt_at)
            assert duplicate.last_attempt_at is not None
            assert duplicate.next_attempt_at is not None
            self.assertEqual(
                duplicate.last_attempt_at.replace(tzinfo=timezone.utc),
                now,
            )
            self.assertEqual(
                duplicate.next_attempt_at.replace(tzinfo=timezone.utc),
                now,
            )

    def test_tiktok_webhook_enrichment_queue_requeues_interrupted_processing_jobs(self) -> None:
        interrupted_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        restart_at = interrupted_at + timedelta(minutes=5)
        with Session(self.engine) as session:
            job = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-interrupted-1",
                now=interrupted_at,
            )
            job.status = ENRICH_PROCESSING
            job.last_attempt_at = interrupted_at
            job.next_attempt_at = None
            session.add(job)
            session.commit()

            fresh_count = requeue_interrupted_tiktok_webhook_enrichment_jobs(
                session,
                now=restart_at,
            )
            session.commit()

            self.assertEqual(fresh_count, 0)
            session.refresh(job)
            self.assertEqual(job.status, ENRICH_PROCESSING)

            stale_count = requeue_interrupted_tiktok_webhook_enrichment_jobs(
                session,
                now=interrupted_at + timedelta(minutes=11),
            )
            session.commit()

            self.assertEqual(stale_count, 1)
            session.refresh(job)
            self.assertEqual(job.status, "pending")
            self.assertIsNotNone(job.next_attempt_at)
            assert job.next_attempt_at is not None
            self.assertEqual(
                job.next_attempt_at.replace(tzinfo=timezone.utc),
                interrupted_at + timedelta(minutes=11),
            )

    def test_tiktok_webhook_enrichment_queue_processor_recovers_stale_processing_jobs(self) -> None:
        interrupted_at = utcnow() - timedelta(minutes=20)
        calls: list[tuple[str, bool]] = []

        with Session(self.engine) as session:
            job = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-loop-recovery-1",
                now=interrupted_at,
            )
            job.status = ENRICH_PROCESSING
            job.last_attempt_at = interrupted_at
            job.next_attempt_at = None
            session.add(job)
            session.commit()

        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        def fake_enrich(order_id: str, *, raise_errors: bool = False) -> None:
            calls.append((order_id, raise_errors))

        with patch.object(shared_module, "managed_session", fake_managed_session), patch.object(
            shared_module, "_fetch_tiktok_order_details", object()
        ), patch.object(
            shared_module, "_order_record_from_payload", object()
        ), patch.object(
            shared_module, "_enrich_tiktok_order_from_api", side_effect=fake_enrich
        ):
            attempted = shared_module._process_tiktok_webhook_enrichment_queue_once(limit=5)

        self.assertEqual(attempted, 1)
        self.assertEqual(calls, [("tt-loop-recovery-1", True)])
        with Session(self.engine) as session:
            job = session.exec(
                select(TikTokWebhookEnrichmentJob).where(
                    TikTokWebhookEnrichmentJob.tiktok_order_id == "tt-loop-recovery-1"
                )
            ).one()
            self.assertEqual(job.status, "succeeded")

    def test_tiktok_webhook_enrichment_uses_existing_order_shop_credentials_first(self) -> None:
        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="secondary-shop",
                    shop_cipher="secondary-cipher",
                    access_token="secondary-token",
                    refresh_token="secondary-refresh",
                    seller_name="D.C. LLC",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-webhook-order",
                    shop_id="main-shop",
                    shop_cipher="main-cipher",
                    order_number="main-webhook-order",
                    created_at=datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc),
                    source="webhook",
                    line_items_json="[]",
                    line_items_summary_json="[]",
                )
            )
            session.commit()

        fetch_calls: list[tuple[str, str, str]] = []

        def fake_fetch_details(*args, **kwargs):
            fetch_calls.append((kwargs["shop_id"], kwargs["shop_cipher"], kwargs["access_token"]))
            return [{"order_id": "main-webhook-order", "shop_id": "main-shop"}]

        def fake_order_record(payload, *, shop_id, shop_cipher, source):
            return {
                "tiktok_order_id": payload["order_id"],
                "shop_id": shop_id,
                "shop_cipher": shop_cipher,
                "source": source,
                "line_items_json": "[]",
                "line_items_summary_json": "[]",
            }

        with patch.object(shared_module, "managed_session", fake_managed_session), patch.object(
            shared_module,
            "_refresh_tiktok_auth_if_needed",
            return_value=None,
        ), patch.object(
            shared_module,
            "_fetch_tiktok_order_details",
            side_effect=fake_fetch_details,
        ), patch.object(
            shared_module,
            "_order_record_from_payload",
            side_effect=fake_order_record,
        ), patch.object(
            shared_module,
            "resolve_tiktok_shop_pull_base_url",
            return_value="https://open-api.tiktokglobalshop.com",
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", "main-shop"
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", "main-cipher"
        ), patch.object(
            main_module.settings, "tiktok_access_token", "main-token"
        ):
            shared_module._enrich_tiktok_order_from_api("main-webhook-order", raise_errors=True)

        self.assertEqual(fetch_calls, [("main-shop", "main-cipher", "main-token")])

    def test_tiktok_webhook_enrichment_falls_through_stale_env_token_to_oauth_row(self) -> None:
        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    access_token="fresh-oauth-token",
                    refresh_token="fresh-refresh-token",
                    seller_name="D.C. LLC",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="shared-shop-webhook-order",
                    shop_id="7495987383262087496",
                    order_number="shared-shop-webhook-order",
                    created_at=datetime(2026, 6, 5, 3, 45, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 6, 5, 3, 45, tzinfo=timezone.utc),
                    source="webhook",
                    line_items_json="[]",
                    line_items_summary_json="[]",
                )
            )
            session.commit()

        fetch_calls: list[tuple[str, str, str]] = []

        def fake_fetch_details(*args, **kwargs):
            fetch_calls.append((kwargs["shop_id"], kwargs["shop_cipher"], kwargs["access_token"]))
            if kwargs["access_token"] == "stale-env-token":
                request = httpx.Request("GET", "https://open-api.tiktokglobalshop.com/order/202309/orders")
                response = httpx.Response(401, request=request)
                raise httpx.HTTPStatusError("expired", request=request, response=response)
            return [{"order_id": "shared-shop-webhook-order"}]

        def fake_order_record(payload, *, shop_id, shop_cipher, source):
            return {
                "tiktok_order_id": payload["order_id"],
                "shop_id": shop_id,
                "shop_cipher": shop_cipher,
                "order_number": payload["order_id"],
                "source": source,
                "subtotal_price": 11.0,
                "total_price": 12.0,
                "line_items_json": json.dumps([
                    {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 11.0}
                ]),
                "line_items_summary_json": "[]",
            }

        with patch.object(shared_module, "managed_session", fake_managed_session), patch.object(
            shared_module,
            "_refresh_tiktok_auth_if_needed",
            return_value=None,
        ), patch.object(
            shared_module,
            "_fetch_tiktok_order_details",
            side_effect=fake_fetch_details,
        ), patch.object(
            shared_module,
            "_order_record_from_payload",
            side_effect=fake_order_record,
        ), patch.object(
            shared_module,
            "resolve_tiktok_shop_pull_base_url",
            return_value="https://open-api.tiktokglobalshop.com",
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", "7495987383262087496"
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", "shared-shop-cipher"
        ), patch.object(
            main_module.settings, "tiktok_access_token", "stale-env-token"
        ):
            shared_module._enrich_tiktok_order_from_api("shared-shop-webhook-order", raise_errors=True)

        self.assertEqual(
            fetch_calls,
            [
                ("7495987383262087496", "shared-shop-cipher", "stale-env-token"),
                ("oauth-open-id", "shared-shop-cipher", "fresh-oauth-token"),
            ],
        )
        with Session(self.engine) as session:
            order = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "shared-shop-webhook-order")
            ).one()
            self.assertEqual(order.source, "webhook_enriched")
            self.assertEqual(order.shop_id, "7495987383262087496")
            self.assertEqual(order.shop_cipher, "shared-shop-cipher")
            self.assertEqual(order.subtotal_price, 11.0)

    def test_tiktok_webhook_enrichment_queue_reenqueue_resets_terminal_retry_budget(self) -> None:
        first_seen_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        second_seen_at = first_seen_at + timedelta(hours=1)
        with Session(self.engine) as session:
            job = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-requeue-1",
                now=first_seen_at,
                max_attempts=2,
            )
            job.status = "failed"
            job.attempts = 2
            job.last_error = "previous outage"
            session.add(job)
            session.commit()

            refreshed = enqueue_tiktok_webhook_enrichment(
                session,
                "tt-requeue-1",
                now=second_seen_at,
                max_attempts=2,
            )
            session.commit()

            self.assertEqual(refreshed.status, "pending")
            self.assertEqual(refreshed.attempts, 0)
            self.assertEqual(refreshed.last_error, "")
            self.assertEqual(
                refreshed.next_attempt_at.replace(tzinfo=timezone.utc),
                second_seen_at,
            )

            processed = process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=second_seen_at,
                enrich_fn=lambda order_id: (_ for _ in ()).throw(RuntimeError("new temporary outage")),
            )

            self.assertEqual(processed, 1)
            session.refresh(refreshed)
            self.assertEqual(refreshed.status, "pending")
            self.assertEqual(refreshed.attempts, 1)
            self.assertIn("new temporary outage", refreshed.last_error or "")

    def test_tiktok_webhook_enrichment_queue_redacts_sensitive_retry_errors(self) -> None:
        now = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            enqueue_tiktok_webhook_enrichment(session, "tt-redact-1", now=now)
            session.commit()

            process_due_tiktok_webhook_enrichment_jobs(
                session,
                now=now,
                enrich_fn=lambda order_id: (_ for _ in ()).throw(
                    RuntimeError("access_token=tok_live_123 app_secret: super-secret")
                ),
            )

            job = session.exec(select(TikTokWebhookEnrichmentJob)).one()
            self.assertIn("access_token=[REDACTED]", job.last_error)
            self.assertIn("app_secret: [REDACTED]", job.last_error)
            self.assertNotIn("tok_live_123", job.last_error)
            self.assertNotIn("super-secret", job.last_error)

    def test_tiktok_webhook_enrichment_queue_counts_pending_and_failed_jobs(self) -> None:
        now = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            enqueue_tiktok_webhook_enrichment(session, "tt-pending-1", now=now)
            failed = enqueue_tiktok_webhook_enrichment(session, "tt-failed-1", now=now)
            failed.status = "failed"
            failed.last_error = "permanent failure"
            session.add(failed)
            session.commit()

            counts = get_tiktok_webhook_enrichment_queue_counts(session)

        self.assertEqual(counts["pending"], 1)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["active"], 1)

    def test_tiktok_webhook_enrichment_queue_counts_are_visible_in_status_surfaces(self) -> None:
        now = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            enqueue_tiktok_webhook_enrichment(session, "tt-visible-pending", now=now)
            failed = enqueue_tiktok_webhook_enrichment(session, "tt-visible-failed", now=now)
            failed.status = "failed"
            failed.last_error = "detail fetch failed"
            session.add(failed)
            session.commit()

            status_snapshot = shared_module.build_tiktok_status_snapshot(session)
            orders_page_data = tiktok_orders_module._collect_tiktok_orders_page_data(session)

        self.assertEqual(status_snapshot["webhook_enrichment_queue"]["pending"], 1)
        self.assertEqual(status_snapshot["webhook_enrichment_queue"]["failed"], 1)
        self.assertEqual(orders_page_data["sync_snapshot"]["webhook_enrichment_queue"]["pending"], 1)
        self.assertEqual(orders_page_data["sync_snapshot"]["webhook_enrichment_queue"]["failed"], 1)

        status_template = (Path(__file__).parents[1] / "app" / "templates" / "status.html").read_text()
        orders_template = (Path(__file__).parents[1] / "app" / "templates" / "tiktok_orders.html").read_text()
        self.assertIn("webhook_enrichment_queue", status_template)
        self.assertIn("Enrichment queue", status_template)
        self.assertIn("webhook_enrichment_queue", orders_template)
        self.assertIn("Enrichment queue", orders_template)

    def test_tiktok_backfill_thin_payload_preserves_existing_paid_status(self) -> None:
        created_at = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        created_ts = int(created_at.timestamp())
        update_ts = int((created_at + timedelta(hours=1)).timestamp())
        with Session(self.engine) as session:
            upsert_tiktok_order(
                session,
                {
                    "order_id": "thin-paid-1",
                    "create_time": created_ts,
                    "update_time": created_ts,
                    "payment_status": "paid",
                    "total_amount": "10.00",
                    "line_items": [{"product_name": "Pack", "quantity": 1, "price": "10.00"}],
                },
                shop_id="shop-1",
                shop_cipher="cipher-1",
                source="backfill",
            )
            session.commit()

            upsert_tiktok_order(
                session,
                {"order_id": "thin-paid-1", "update_time": update_ts, "order_status": "AWAITING_SHIPMENT"},
                shop_id="shop-1",
                shop_cipher="cipher-1",
                source="webhook",
            )
            session.commit()

            stored = session.exec(select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "thin-paid-1")).first()

        assert stored is not None
        self.assertEqual(stored.financial_status, "paid")
        self.assertEqual(stored.order_status, "AWAITING_SHIPMENT")
        self.assertIsNotNone(stored.created_at)
        stored_created_at = stored.created_at if stored.created_at.tzinfo else stored.created_at.replace(tzinfo=timezone.utc)
        stored_updated_at = stored.updated_at if stored.updated_at.tzinfo else stored.updated_at.replace(tzinfo=timezone.utc)
        self.assertEqual(int(stored_created_at.timestamp()), created_ts)
        self.assertEqual(int(stored_updated_at.timestamp()), update_ts)
        self.assertEqual(stored.total_price, 10.0)
        self.assertEqual(classify_tiktok_reporting_status(stored), "paid")

    def test_tiktok_refund_aliases_dominate_paid_status(self) -> None:
        for alias in (
            "cancel",
            "in_cancel",
            "reverse",
            "refund",
            "partially_refunded",
            "refund_request",
        ):
            row = TikTokOrder(
                tiktok_order_id=f"refund-alias-{alias}",
                created_at=utcnow(),
                updated_at=utcnow(),
                financial_status="paid",
                order_status=alias,
            )
            self.assertEqual(classify_tiktok_reporting_status(row), "refunded", alias)

    def test_tiktok_reporting_summary_counts_orders_and_line_items(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-1",
                    shop_id="shop-1",
                    order_number="#1001",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    total_price=12.50,
                    total_tax=1.50,
                    subtotal_ex_tax=11.00,
                    line_items_summary_json='[{"title":"Charizard","quantity":1}]',
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-2",
                    shop_id="shop-1",
                    order_number="#1002",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="",
                    order_status="pending",
                    total_price=8.00,
                    line_items_summary_json='[{"title":"Pikachu","quantity":2}]',
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-3",
                    shop_id="shop-1",
                    order_number="#1003",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="refunded",
                    total_price=7.00,
                    line_items_summary_json='[{"title":"Mew","quantity":1}]',
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-cancel-requested",
                    shop_id="shop-1",
                    order_number="#1004",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    order_status="cancel_requested",
                    total_price=99.00,
                    line_items_summary_json='[{"title":"Refunded Pack","quantity":1}]',
                )
            )
            session.commit()

            rows = get_tiktok_reporting_rows(session)
            summary = build_tiktok_reporting_summary(rows)
            cancel_requested = next(row for row in rows if row.tiktok_order_id == "tt-cancel-requested")

        self.assertEqual(len(rows), 4)
        self.assertEqual(summary["orders"], 4)
        self.assertEqual(summary["status_counts"]["paid"], 1)
        self.assertEqual(summary["status_counts"]["pending"], 1)
        self.assertEqual(summary["status_counts"]["refunded"], 2)
        self.assertEqual(classify_tiktok_reporting_status(cancel_requested), "refunded")
        self.assertEqual(summary["paid_orders"], 1)
        self.assertEqual(summary["paid_orders_with_known_tax"], 1)
        self.assertEqual(summary["gross_revenue"], 12.5)
        self.assertEqual(summary["total_tax"], 1.5)
        self.assertEqual(summary["net_revenue"], 11.0)
        self.assertFalse(summary["has_missing_tax_data"])
        self.assertEqual(summary["line_item_summary"]["orders_with_items"], 4)
        self.assertEqual(summary["line_item_summary"]["line_items_total"], 5)

    def test_tiktok_reporting_rows_filter_and_order_by_created_at(self) -> None:
        earlier = utcnow()
        later = utcnow()
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-1",
                    shop_id="shop-1",
                    order_number="#1001",
                    created_at=later,
                    updated_at=later,
                    financial_status="paid",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-2",
                    shop_id="shop-1",
                    order_number="#1002",
                    created_at=earlier,
                    updated_at=earlier,
                    financial_status="paid",
                )
            )
            session.commit()

            rows = get_tiktok_reporting_rows(session, start=earlier, end=later)

        self.assertEqual([row.tiktok_order_id for row in rows], ["tt-2", "tt-1"])

    def test_tiktok_reports_page_exposes_tiktok_only_summary_context(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-standalone",
                    shop_id="shop-1",
                    order_number="#2001",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    total_price=42.00,
                    total_tax=3.50,
                    subtotal_ex_tax=38.50,
                    line_items_summary_json='[{"title":"Mewtwo","quantity":1,"unit_price":42.0}]',
                )
            )
            session.commit()

            with patch.object(reports_module, "require_role_response", return_value=None), patch.object(
                reports_module, "get_transactions", return_value=[]
            ), patch.object(
                reports_module,
                "build_transaction_summary",
                return_value={"totals": {"net": 0.0}, "expense_categories": [], "channel_net": []},
            ), patch.object(
                reports_module, "get_shopify_reporting_rows", return_value=[]
            ), patch.object(
                reports_module,
                "build_shopify_reporting_summary",
                return_value={
                    "gross_revenue": 0.0,
                    "total_tax": 0.0,
                    "net_revenue": 0.0,
                    "tax_unknown_orders": 0,
                },
            ), patch.object(
                reports_module, "build_report_period_comparison_rows", return_value=[]
            ), patch.object(
                reports_module, "get_channel_filter_choices", return_value=([], False)
            ), patch.object(
                reports_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ):
                response = reports_module.reports_page(  # type: ignore[arg-type]
                    request,
                    start=None,
                    end=None,
                    channel_id=None,
                    entry_kind=None,
                    source=main_module.REPORT_SOURCE_TIKTOK,
                    session=session,
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["template_name"], "reports.html")
        context = captured["context"]
        self.assertIsInstance(context, dict)
        self.assertTrue(context["show_tiktok_reports"])
        self.assertFalse(context["show_discord_reports"])
        self.assertFalse(context["show_shopify_reports"])
        self.assertEqual(context["tiktok_summary"]["orders"], 1)
        self.assertEqual(context["tiktok_summary"]["gross_revenue"], 42.0)
        self.assertEqual(context["tiktok_summary"]["net_revenue"], 38.5)
        self.assertEqual(context["report_totals"]["tiktok_net"], 38.5)
        self.assertEqual(len(context["tiktok_daily_totals"]), 1)
        self.assertEqual(context["tiktok_daily_totals"][0]["orders"], 1)
        self.assertEqual(context["tiktok_daily_totals"][0]["gross"], 42.0)
        self.assertEqual(context["tiktok_daily_totals"][0]["tax"], 3.5)
        self.assertEqual(context["tiktok_daily_totals"][0]["net"], 38.5)

    def test_tiktok_reporting_summary_daily_totals_count_only_paid_revenue(self) -> None:
        with Session(self.engine) as session:
            session.add_all(
                [
                    TikTokOrder(
                        tiktok_order_id="tt-paid-1",
                        shop_id="shop-1",
                        order_number="#2001",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="paid",
                        total_price=42.0,
                        total_tax=3.5,
                        subtotal_ex_tax=38.5,
                    ),
                    TikTokOrder(
                        tiktok_order_id="tt-paid-2",
                        shop_id="shop-1",
                        order_number="#2002",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="paid",
                        total_price=10.0,
                        total_tax=None,
                        subtotal_ex_tax=None,
                    ),
                    TikTokOrder(
                        tiktok_order_id="tt-pending",
                        shop_id="shop-1",
                        order_number="#2003",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="pending",
                        total_price=99.0,
                        total_tax=9.9,
                        subtotal_ex_tax=89.1,
                    ),
                    TikTokOrder(
                        tiktok_order_id="tt-refunded",
                        shop_id="shop-1",
                        order_number="#2004",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="refunded",
                        total_price=5.0,
                        total_tax=0.5,
                        subtotal_ex_tax=4.5,
                    ),
                ]
            )
            session.commit()
            rows = session.exec(select(TikTokOrder).order_by(TikTokOrder.created_at.asc(), TikTokOrder.id.asc())).all()

        summary = build_tiktok_reporting_summary(rows)

        self.assertEqual(summary["orders"], 4)
        self.assertEqual(summary["paid_orders"], 2)
        self.assertEqual(summary["status_counts"]["paid"], 2)
        self.assertEqual(summary["status_counts"]["pending"], 1)
        self.assertEqual(summary["status_counts"]["refunded"], 1)
        self.assertEqual(summary["tax_unknown_orders"], 1)
        self.assertEqual(summary["gross_revenue"], 52.0)
        self.assertEqual(summary["total_tax"], 3.5)
        self.assertEqual(summary["net_revenue"], 48.5)
        self.assertEqual(len(summary["daily_totals"]), 1)
        self.assertEqual(summary["daily_totals"][0]["orders"], 4)
        self.assertEqual(summary["daily_totals"][0]["paid_orders"], 2)
        self.assertEqual(summary["daily_totals"][0]["pending_orders"], 1)
        self.assertEqual(summary["daily_totals"][0]["refunded_orders"], 1)
        self.assertEqual(summary["daily_totals"][0]["gross"], 52.0)
        self.assertEqual(summary["daily_totals"][0]["tax"], 3.5)
        self.assertEqual(summary["daily_totals"][0]["net"], 48.5)
        self.assertEqual(summary["daily_totals"][0]["tax_unknown_orders"], 1)

    def test_tiktok_reporting_summary_treats_completed_and_shipment_states_as_paid_like(self) -> None:
        with Session(self.engine) as session:
            session.add_all(
                [
                    TikTokOrder(
                        tiktok_order_id="tt-completed",
                        shop_id="shop-1",
                        order_number="#2101",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="",
                        order_status="completed",
                        total_price=20.0,
                        total_tax=2.0,
                        subtotal_ex_tax=18.0,
                    ),
                    TikTokOrder(
                        tiktok_order_id="tt-awaiting-shipment",
                        shop_id="shop-1",
                        order_number="#2102",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="",
                        order_status="awaiting_shipment",
                        total_price=15.0,
                        total_tax=1.5,
                        subtotal_ex_tax=13.5,
                    ),
                    TikTokOrder(
                        tiktok_order_id="tt-cancelled",
                        shop_id="shop-1",
                        order_number="#2103",
                        created_at=utcnow(),
                        updated_at=utcnow(),
                        financial_status="",
                        order_status="cancelled",
                        total_price=40.0,
                        total_tax=4.0,
                        subtotal_ex_tax=36.0,
                    ),
                ]
            )
            session.commit()
            rows = session.exec(select(TikTokOrder).order_by(TikTokOrder.created_at.asc(), TikTokOrder.id.asc())).all()

        summary = build_tiktok_reporting_summary(rows)

        self.assertEqual(summary["orders"], 3)
        self.assertEqual(summary["paid_orders"], 2)
        self.assertEqual(summary["status_counts"]["paid"], 2)
        self.assertEqual(summary["status_counts"]["refunded"], 1)
        self.assertEqual(summary["gross_revenue"], 35.0)
        self.assertEqual(summary["total_tax"], 3.5)
        self.assertEqual(summary["net_revenue"], 31.5)
        self.assertEqual(summary["daily_totals"][0]["orders"], 3)
        self.assertEqual(summary["daily_totals"][0]["paid_orders"], 2)
        self.assertEqual(summary["daily_totals"][0]["refunded_orders"], 1)
        self.assertEqual(summary["daily_totals"][0]["gross"], 35.0)
        self.assertEqual(summary["daily_totals"][0]["net"], 31.5)

    def test_reports_page_combined_revenue_excludes_discord_non_operating_cash_in(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-combined",
                    shop_id="shop-1",
                    order_number="#3001",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    total_price=5.0,
                    total_tax=1.0,
                    subtotal_ex_tax=4.0,
                )
            )
            session.commit()

            with patch.object(reports_module, "require_role_response", return_value=None), patch.object(
                reports_module, "get_transactions", return_value=[]
            ), patch.object(
                reports_module,
                "build_transaction_summary",
                return_value={
                    "totals": {
                        "net": 100.0,
                        "money_in": 160.0,
                        "non_operating_money_in": 100.0,
                        "money_out": 60.0,
                    },
                    "expense_categories": [],
                    "channel_net": [],
                },
            ), patch.object(
                reports_module, "get_shopify_reporting_rows", return_value=[]
            ), patch.object(
                reports_module,
                "build_shopify_reporting_summary",
                return_value={
                    "orders": 1,
                    "gross_revenue": 20.0,
                    "total_tax": 2.0,
                    "net_revenue": 20.0,
                    "tax_unknown_orders": 0,
                },
            ), patch.object(
                reports_module, "build_report_period_comparison_rows", return_value=[]
            ), patch.object(
                reports_module, "get_channel_filter_choices", return_value=([], False)
            ), patch.object(
                reports_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ):
                response = reports_module.reports_page(  # type: ignore[arg-type]
                    request,
                    start=None,
                    end=None,
                    channel_id=None,
                    entry_kind=None,
                    source=main_module.REPORT_SOURCE_ALL,
                    session=session,
                )

        self.assertEqual(response.status_code, 200)
        context = captured["context"]
        self.assertIsInstance(context, dict)
        self.assertEqual(context["report_totals"]["discord_gross"], 60.0)
        self.assertEqual(context["report_totals"]["discord_outflow"], 60.0)
        self.assertEqual(context["report_totals"]["shopify_net"], 20.0)
        self.assertEqual(context["report_totals"]["tiktok_net"], 4.0)
        self.assertEqual(context["report_totals"]["combined_revenue"], 84.0)
        self.assertTrue(context["show_tiktok_reports"])

    def test_report_period_rows_exclude_discord_non_operating_cash_in(self) -> None:
        period_start = datetime(2026, 5, 1, tzinfo=timezone.utc)
        period_end = datetime(2026, 5, 31, tzinfo=timezone.utc)

        with Session(self.engine) as session, patch.object(
            shared_module, "get_transactions", return_value=[]
        ), patch.object(
            shared_module,
            "build_transaction_summary",
            return_value={
                "totals": {
                    "net": 100.0,
                    "money_in": 160.0,
                    "non_operating_money_in": 100.0,
                    "money_out": 60.0,
                },
                "expense_categories": [],
                "channel_net": [],
            },
        ), patch.object(
            shared_module, "get_shopify_reporting_rows", return_value=[]
        ), patch.object(
            shared_module,
            "build_shopify_reporting_summary",
            return_value={
                "gross_revenue": 20.0,
                "total_tax": 2.0,
                "net_revenue": 20.0,
                "tax_unknown_orders": 0,
            },
        ), patch.object(
            shared_module, "get_tiktok_reporting_rows", return_value=[]
        ), patch.object(
            shared_module,
            "build_tiktok_reporting_summary",
            return_value={
                "gross_revenue": 5.0,
                "total_tax": 1.0,
                "net_revenue": 4.0,
                "tax_unknown_orders": 0,
            },
        ):
            rows = shared_module.build_report_period_comparison_rows(
                session,
                periods=[
                    {
                        "key": "mtd",
                        "label": "May 2026",
                        "start": period_start,
                        "end": period_end,
                    }
                ],
            )

        self.assertEqual(rows[0]["discord_gross"], 60.0)
        self.assertEqual(rows[0]["combined_revenue"], 84.0)

    def test_reports_page_uses_shared_tiktok_daily_totals_status_logic(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-report-completed",
                    shop_id="shop-1",
                    order_number="#3101",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="",
                    order_status="completed",
                    total_price=25.0,
                    total_tax=2.5,
                    subtotal_ex_tax=22.5,
                )
            )
            session.commit()

            with patch.object(reports_module, "require_role_response", return_value=None), patch.object(
                reports_module, "get_transactions", return_value=[]
            ), patch.object(
                reports_module,
                "build_transaction_summary",
                return_value={"totals": {"net": 0.0}, "expense_categories": [], "channel_net": []},
            ), patch.object(
                reports_module, "get_shopify_reporting_rows", return_value=[]
            ), patch.object(
                reports_module,
                "build_shopify_reporting_summary",
                return_value={
                    "orders": 0,
                    "gross_revenue": 0.0,
                    "total_tax": 0.0,
                    "net_revenue": 0.0,
                    "tax_unknown_orders": 0,
                },
            ), patch.object(
                reports_module, "build_report_period_comparison_rows", return_value=[]
            ), patch.object(
                reports_module, "get_channel_filter_choices", return_value=([], False)
            ), patch.object(
                reports_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ):
                response = reports_module.reports_page(  # type: ignore[arg-type]
                    request,
                    start=None,
                    end=None,
                    channel_id=None,
                    entry_kind=None,
                    source=main_module.REPORT_SOURCE_TIKTOK,
                    session=session,
                )

        self.assertEqual(response.status_code, 200)
        context = captured["context"]
        self.assertIsInstance(context, dict)
        self.assertEqual(context["tiktok_summary"]["paid_orders"], 1)
        self.assertEqual(len(context["tiktok_daily_totals"]), 1)
        self.assertEqual(context["tiktok_daily_totals"][0]["paid_orders"], 1)
        self.assertEqual(context["tiktok_daily_totals"][0]["gross"], 25.0)
        self.assertEqual(context["tiktok_daily_totals"][0]["net"], 22.5)

    def test_dashboard_page_includes_tiktok_summary_context(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        with Session(self.engine) as session:
            dashboard_today = utcnow().astimezone(main_module.PACIFIC_TZ).replace(
                hour=12, minute=0, second=0, microsecond=0
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-dashboard",
                    shop_id="shop-1",
                    order_number="#4001",
                    created_at=dashboard_today,
                    updated_at=dashboard_today,
                    financial_status="",
                    order_status="completed",
                    total_price=7.0,
                    total_tax=0.7,
                    subtotal_ex_tax=6.3,
                )
            )
            session.commit()

            with patch.object(dashboard_module, "require_role_response", return_value=None), patch.object(
                dashboard_module,
                "get_summary",
                side_effect=[
                    {"rows": 0, "totals": {"net": 0.0}},
                    {"rows": 1, "totals": {"net": 1.0}},
                ],
            ), patch.object(
                dashboard_module,
                "build_dashboard_snapshot",
                return_value={"today": {}},
            ), patch.object(
                dashboard_module, "get_parser_progress", return_value={"is_running": False}
            ), patch.object(
                dashboard_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ):
                response = dashboard_module.dashboard_page(request, session=session)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["template_name"], "dashboard.html")
        context = captured["context"]
        self.assertIsInstance(context, dict)
        self.assertIn("tiktok_summary", context)
        self.assertEqual(context["tiktok_summary"]["orders"], 1)
        self.assertEqual(context["tiktok_summary"]["gross_revenue"], 7.0)
        self.assertEqual(context["dashboard_snapshot"]["today"]["tiktok"]["order_count"], 1)
        self.assertEqual(context["dashboard_snapshot"]["today"]["tiktok"]["paid_order_count"], 1)
        self.assertEqual(context["dashboard_snapshot"]["today"]["tiktok"]["gross"], 7.0)
        self.assertEqual(context["dashboard_snapshot"]["today"]["revenue"]["tiktok_total"], 6.3)
        self.assertEqual(context["dashboard_snapshot"]["today"]["revenue"]["total"], 6.3)
        self.assertEqual(context["tiktok_recent_order_count"], 1)

    def test_collect_tiktok_orders_page_data_uses_shared_reporting_helper(self) -> None:
        self._reset_tiktok_state()
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="7495987383262087496",
                    shop_cipher="cipher-1",
                    access_token="token-1",
                    refresh_token="refresh-1",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-collect-1",
                    shop_id="7495987383262087496",
                    order_number="#TT-2001",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    customer_name="Jordan",
                    financial_status="",
                    order_status="completed",
                    fulfillment_status="fulfilled",
                    currency="USD",
                    total_price=21.0,
                    total_tax=1.5,
                    subtotal_ex_tax=19.5,
                    source="automatic_pull",
                    line_items_summary_json='[{"title":"Pikachu","quantity":2,"unit_price":10.5}]',
                )
            )
            session.commit()

            page_data = main_module._collect_tiktok_orders_page_data(
                session,
                start=None,
                end=None,
                financial_status=None,
                fulfillment_status=None,
                order_status=None,
                source=None,
                currency=None,
                search=None,
                sort_by="date",
                sort_dir="desc",
                page=1,
                limit=50,
            )

        self.assertEqual(page_data["summary"]["orders"], 1)
        self.assertEqual(page_data["summary"]["paid_orders"], 1)
        self.assertEqual(len(page_data["orders"]), 1)
        self.assertEqual(page_data["orders"][0]["order"].order_number, "#TT-2001")
        self.assertIn("Pikachu", page_data["orders"][0]["items_summary"])
        self.assertEqual(page_data["daily_totals"][0]["paid_orders"], 1)
        self.assertEqual(page_data["line_item_summary"]["line_items_total"], 2)
        self.assertEqual(page_data["sync_snapshot"]["status_label"], "Connected")

    def test_tiktok_standalone_orders_page_exposes_summary_orders_and_sync_context(self) -> None:
        self._reset_tiktok_state()
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        rendered_order_id = None
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="7495987383262087496",
                    shop_cipher="cipher-1",
                    access_token="token-1",
                    refresh_token="refresh-1",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-page-1",
                    shop_id="7495987383262087496",
                    order_number="#TT-1001",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    customer_name="Casey",
                    financial_status="paid",
                    fulfillment_status="fulfilled",
                    order_status="completed",
                    currency="USD",
                    total_price=42.0,
                    total_tax=3.5,
                    subtotal_ex_tax=38.5,
                    source="automatic_pull",
                    line_items_summary_json='[{"title":"Mewtwo","quantity":1,"unit_price":42.0}]',
                )
            )
            session.commit()

            auth_row = session.exec(
                select(TikTokAuth).where(TikTokAuth.tiktok_shop_id == "7495987383262087496")
            ).first()
            main_module.update_tiktok_integration_state(
                last_callback={
                    "received_at": utcnow().isoformat(),
                    "query": {"app_key": "app-key", "shop_region": "US"},
                },
                last_pull_at=utcnow(),
                last_pull_started_at=utcnow(),
                last_pull_finished_at=utcnow(),
                last_pull={
                    "status": "success",
                    "trigger": "automatic",
                    "fetched": 1,
                    "inserted": 1,
                    "updated": 0,
                    "failed": 0,
                    "detail_calls": 1,
                },
            )

            def fake_collect_tiktok_orders_page_data(*args, **kwargs):
                return {
                    "summary": {"orders": 1, "gross_revenue": 42.0, "net_revenue": 38.5},
                    "total_count": 1,
                    "orders": [
                        {
                            "order": SimpleNamespace(tiktok_order_id="tt-page-1"),
                            "items_summary": "Mewtwo",
                            "customer_label": "TestBuyer",
                        }
                    ],
                    "auth_row": auth_row,
                    "sync_snapshot": {"status_label": "Connected", "sync_label": "Sync healthy"},
                    "integration_state": {"is_pull_running": False},
                }

            with patch.object(tiktok_orders_module, "require_role_response", return_value=None), patch.object(
                tiktok_orders_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ), patch.object(
                tiktok_orders_module,
                "_collect_tiktok_orders_page_data",
                side_effect=fake_collect_tiktok_orders_page_data,
                create=True,
            ), patch.object(main_module.settings, "tiktok_sync_enabled", True), patch.object(
                main_module.settings, "tiktok_sync_interval_minutes", 15
            ), patch.object(
                main_module.settings, "tiktok_sync_limit", 100
            ):
                response = tiktok_orders_module.tiktok_orders_page(  # type: ignore[arg-type]
                    request,
                    start=None,
                    end=None,
                    financial_status=None,
                    fulfillment_status=None,
                    order_status=None,
                    source=None,
                    currency=None,
                    search=None,
                    sort_by="date",
                    sort_dir="desc",
                    page=1,
                    limit=50,
                    success=None,
                    error=None,
                    session=session,
                )
                rendered_order_id = captured["context"]["recent_orders"][0]["order"].tiktok_order_id

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["template_name"], "tiktok_orders.html")
        context = captured["context"]
        self.assertIsInstance(context, dict)
        self.assertEqual(context["summary"]["orders"], 1)
        self.assertEqual(context["summary"]["gross_revenue"], 42.0)
        self.assertEqual(context["summary"]["net_revenue"], 38.5)
        self.assertEqual(len(context["recent_orders"]), 1)
        self.assertEqual(rendered_order_id, "tt-page-1")
        self.assertIn("Mewtwo", context["recent_orders"][0]["items_summary"])
        self.assertEqual(context["auth_row"].tiktok_shop_id, "7495987383262087496")
        self.assertEqual(context["sync_snapshot"]["status_label"], "Connected")
        self.assertEqual(context["sync_snapshot"]["sync_label"], "Sync healthy")
        self.assertFalse(context["integration_state"]["is_pull_running"])
        self.assertTrue(callable(context["page_url"]))

    def test_tiktok_page_bootstraps_saved_auth_record_from_configured_tokens(self) -> None:
        self._reset_tiktok_state()
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="viewer")))
        captured: dict[str, object] = {}

        def fake_template_response(*args, **kwargs):
            captured["template_name"] = args[1] if len(args) > 1 else kwargs.get("name")
            captured["context"] = args[2] if len(args) > 2 else kwargs.get("context")
            return SimpleNamespace(status_code=200, body=b"ok")

        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-page-2",
                    shop_id="7495987383262087496",
                    order_number="#TT-1002",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    total_price=15.0,
                    total_tax=1.0,
                    subtotal_ex_tax=14.0,
                    source="automatic_pull",
                    line_items_summary_json='[{"title":"Charizard","quantity":1,"unit_price":15.0}]',
                )
            )
            session.commit()

            def fake_collect_tiktok_orders_page_data(*args, **kwargs):
                auth_row = main_module.ensure_tiktok_auth_row(session)
                return {
                    "summary": {"orders": 1, "gross_revenue": 15.0, "net_revenue": 14.0},
                    "total_count": 1,
                    "orders": [
                        {
                            "order": SimpleNamespace(tiktok_order_id="tt-page-2"),
                            "items_summary": "Charizard",
                            "customer_label": "TestBuyer",
                        }
                    ],
                    "auth_row": auth_row,
                    "sync_snapshot": {"status_label": "Connected", "sync_label": "Sync healthy"},
                    "integration_state": {"is_pull_running": False},
                }

            with patch.object(tiktok_orders_module, "require_role_response", return_value=None), patch.object(
                tiktok_orders_module, "templates", SimpleNamespace(TemplateResponse=fake_template_response)
            ), patch.object(
                tiktok_orders_module,
                "_collect_tiktok_orders_page_data",
                side_effect=fake_collect_tiktok_orders_page_data,
                create=True,
            ), patch.object(main_module.settings, "tiktok_sync_enabled", True), patch.object(
                main_module.settings, "tiktok_sync_interval_minutes", 15
            ), patch.object(
                main_module.settings, "tiktok_sync_limit", 100
            ), patch.object(
                main_module.settings, "tiktok_app_key", "app-key"
            ), patch.object(
                main_module.settings, "tiktok_redirect_uri", "https://ops.degencollectibles.com/integrations/tiktok/callback"
            ), patch.object(
                main_module.settings, "tiktok_shop_id", "7495987383262087496"
            ), patch.object(
                main_module.settings, "tiktok_shop_cipher", "cipher-1"
            ), patch.object(
                main_module.settings, "tiktok_access_token", "access-token-1"
            ), patch.object(
                main_module.settings, "tiktok_refresh_token", "refresh-token-1"
            ):
                response = tiktok_orders_module.tiktok_orders_page(  # type: ignore[arg-type]
                    request,
                    start=None,
                    end=None,
                    financial_status=None,
                    fulfillment_status=None,
                    order_status=None,
                    source=None,
                    currency=None,
                    search=None,
                    sort_by="date",
                    sort_dir="desc",
                    page=1,
                    limit=50,
                    success=None,
                    error=None,
                    session=session,
                )
                auth_row = session.exec(select(TikTokAuth).where(TikTokAuth.tiktok_shop_id == "7495987383262087496")).first()

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(auth_row)
        self.assertEqual(auth_row.source, "configured_env")
        self.assertEqual(auth_row.shop_cipher, "cipher-1")
        self.assertEqual(auth_row.access_token, "access-token-1")
        context = captured["context"]
        self.assertEqual(context["sync_snapshot"]["status_label"], "Connected")

    def test_tiktok_sync_form_starts_background_sync_thread(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="admin")))
        thread_capture: dict[str, object] = {}

        class FakeThread:
            def __init__(self, *, target=None, kwargs=None, daemon=None, name=None):
                thread_capture["target"] = target
                thread_capture["kwargs"] = kwargs
                thread_capture["daemon"] = daemon
                thread_capture["name"] = name
                thread_capture["started"] = False

            def start(self):
                thread_capture["started"] = True

        with patch.object(tiktok_orders_module, "require_role_response", return_value=None), patch.object(
            tiktok_orders_module, "read_tiktok_integration_state", return_value={"is_pull_running": False}
        ), patch.object(main_module.settings, "tiktok_sync_limit", 100), patch.object(
            tiktok_orders_module, "threading", SimpleNamespace(Thread=FakeThread)
        ):
            response = tiktok_orders_module.tiktok_orders_sync_form(  # type: ignore[arg-type]
                request,
                since="2026-04-01",
                limit="15",
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/tiktok/orders?success=Started+TikTok+sync+orders+will+appear+shortly",
        )
        self.assertTrue(thread_capture["started"])
        self.assertIs(thread_capture["target"], tiktok_orders_module.run_tiktok_pull_in_background)
        self.assertEqual(
            thread_capture["kwargs"],
            {"since": "2026-04-01", "limit": 15, "trigger": "manual"},
        )
        self.assertTrue(thread_capture["daemon"])
        self.assertEqual(thread_capture["name"], "tiktok-pull-manual")

    def test_ensure_tiktok_auth_row_preserves_persisted_auth_over_configured_env(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="7495987383262087496",
                    shop_cipher="db-cipher",
                    app_key="app-key",
                    redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
                    access_token="db-access-token",
                    refresh_token="db-refresh-token",
                    source="oauth_callback",
                )
            )
            session.commit()

            with patch.object(main_module.settings, "tiktok_shop_id", "7495987383262087496"), patch.object(
                main_module.settings, "tiktok_shop_cipher", "env-cipher"
            ), patch.object(main_module.settings, "tiktok_access_token", "env-access-token"), patch.object(
                main_module.settings, "tiktok_refresh_token", "env-refresh-token"
            ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
                main_module.settings, "tiktok_redirect_uri", "https://ops.degencollectibles.com/integrations/tiktok/callback"
            ):
                auth_row = main_module.ensure_tiktok_auth_row(session)

            self.assertIsNotNone(auth_row)
            session.refresh(auth_row)
            self.assertEqual(auth_row.source, "oauth_callback")
            self.assertEqual(auth_row.shop_cipher, "db-cipher")
            self.assertEqual(auth_row.access_token, "db-access-token")
            self.assertEqual(auth_row.refresh_token, "db-refresh-token")

    def test_run_tiktok_pull_cycle_updates_state_after_successful_pull(self) -> None:
        self._reset_tiktok_state()

        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="7495987383262087496",
                    shop_cipher="cipher-1",
                    access_token="token-1",
                    refresh_token="refresh-1",
                )
            )
            session.commit()

        fake_summary = SimpleNamespace(
            fetched=3,
            inserted=2,
            updated=1,
            failed=0,
            detail_calls=3,
        )

        import app.shared as _shared_module
        with patch.object(_shared_module, "managed_session", side_effect=fake_managed_session), patch.object(
            _shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            _shared_module, "pull_tiktok_orders", return_value=fake_summary
        ) as pull_tiktok_orders_mock, patch.object(
            _shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://open-api.tiktokglobalshop.com"
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", ""
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", ""
        ), patch.object(
            main_module.settings, "tiktok_access_token", ""
        ), patch.object(
            main_module.settings, "tiktok_sync_enabled", True
        ), patch.object(
            main_module.settings, "tiktok_sync_limit", 25
        ), patch.object(
            main_module.settings, "tiktok_sync_lookback_hours", 24.0
        ):
            result = _shared_module.run_tiktok_pull_cycle(
                runtime_name="test_tiktok_runtime",
                limit=10,
                trigger="manual",
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["fetched"], 3)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["updated"], 1)
        pull_tiktok_orders_mock.assert_called_once()
        pull_kwargs = pull_tiktok_orders_mock.call_args.kwargs
        self.assertEqual(pull_kwargs["shop_id"], "7495987383262087496")
        self.assertEqual(pull_kwargs["shop_cipher"], "cipher-1")
        self.assertEqual(pull_kwargs["access_token"], "token-1")
        self.assertEqual(pull_kwargs["limit"], 10)
        state = main_module.read_tiktok_integration_state()
        self.assertFalse(state["is_pull_running"])
        self.assertEqual(state["last_pull"]["status"], "success")
        self.assertEqual(state["last_pull"]["trigger"], "manual")
        self.assertEqual(state["last_pull"]["fetched"], 3)

    def test_run_tiktok_pull_cycle_pulls_configured_env_and_persisted_auth_shops(self) -> None:
        self._reset_tiktok_state()

        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="secondary-shop",
                    shop_cipher="secondary-cipher",
                    access_token="secondary-token",
                    refresh_token="secondary-refresh",
                    seller_name="D.C. LLC",
                )
            )
            session.commit()

        def fake_pull_tiktok_orders(*args, **kwargs):
            return SimpleNamespace(
                fetched=1,
                inserted=1 if kwargs["shop_id"] == "main-shop" else 0,
                updated=0 if kwargs["shop_id"] == "main-shop" else 1,
                failed=0,
                detail_calls=1,
            )

        import app.shared as _shared_module
        with patch.object(_shared_module, "managed_session", side_effect=fake_managed_session), patch.object(
            _shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            _shared_module, "pull_tiktok_orders", side_effect=fake_pull_tiktok_orders
        ) as pull_tiktok_orders_mock, patch.object(
            _shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://open-api.tiktokglobalshop.com"
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", "main-shop"
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", "main-cipher"
        ), patch.object(
            main_module.settings, "tiktok_access_token", "main-token"
        ), patch.object(
            main_module.settings, "tiktok_sync_enabled", True
        ), patch.object(
            main_module.settings, "tiktok_sync_limit", 25
        ), patch.object(
            main_module.settings, "tiktok_sync_lookback_hours", 24.0
        ):
            result = _shared_module.run_tiktok_pull_cycle(
                runtime_name="test_tiktok_runtime",
                limit=10,
                trigger="manual",
            )

        pull_identities = [
            (call.kwargs["shop_id"], call.kwargs["shop_cipher"], call.kwargs["access_token"])
            for call in pull_tiktok_orders_mock.call_args_list
        ]
        self.assertEqual(
            pull_identities,
            [
                ("main-shop", "main-cipher", "main-token"),
                ("secondary-shop", "secondary-cipher", "secondary-token"),
            ],
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["shops_pulled"], 2)
        self.assertEqual(result["fetched"], 2)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["updated"], 1)

    def test_run_tiktok_pull_cycle_falls_through_stale_env_token_to_oauth_row(self) -> None:
        self._reset_tiktok_state()

        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    access_token="fresh-oauth-token",
                    refresh_token="fresh-refresh-token",
                    seller_name="D.C. LLC",
                )
            )
            session.commit()

        fake_summary = SimpleNamespace(
            fetched=6,
            inserted=1,
            updated=5,
            failed=0,
            detail_calls=6,
        )
        pull_tokens: list[str] = []

        def fake_pull_tiktok_orders(*args, **kwargs):
            pull_tokens.append(kwargs["access_token"])
            if kwargs["access_token"] == "stale-env-token":
                request = httpx.Request(
                    "POST",
                    "https://open-api.tiktokglobalshop.com/order/202309/orders/search",
                )
                response = httpx.Response(401, request=request)
                raise httpx.HTTPStatusError("expired", request=request, response=response)
            return fake_summary

        import app.shared as _shared_module
        with patch.object(_shared_module, "managed_session", side_effect=fake_managed_session), patch.object(
            _shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            _shared_module, "pull_tiktok_orders", side_effect=fake_pull_tiktok_orders
        ), patch.object(
            _shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://open-api.tiktokglobalshop.com"
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", "7495987383262087496"
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", "shared-shop-cipher"
        ), patch.object(
            main_module.settings, "tiktok_access_token", "stale-env-token"
        ), patch.object(
            main_module.settings, "tiktok_sync_enabled", True
        ), patch.object(
            main_module.settings, "tiktok_sync_limit", 25
        ), patch.object(
            main_module.settings, "tiktok_sync_lookback_hours", 24.0
        ):
            result = _shared_module.run_tiktok_pull_cycle(
                runtime_name="test_tiktok_runtime",
                limit=10,
                trigger="manual",
            )

        self.assertEqual(pull_tokens, ["stale-env-token", "fresh-oauth-token"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["shops_pulled"], 1)
        self.assertEqual(result["fetched"], 6)
        self.assertEqual(result["updated"], 5)

    def test_run_tiktok_pull_cycle_retries_after_401_with_refreshed_token(self) -> None:
        self._reset_tiktok_state()

        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="7495987383262087496",
                    shop_cipher="cipher-1",
                    app_key="app-key",
                    redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
                    access_token="stale-token",
                    refresh_token="refresh-1",
                    source="oauth_callback",
                )
            )
            session.commit()

        fake_summary = SimpleNamespace(
            fetched=4,
            inserted=2,
            updated=2,
            failed=0,
            detail_calls=4,
        )
        pull_tokens: list[str] = []

        def fake_pull_tiktok_orders(*args, **kwargs):
            pull_tokens.append(kwargs["access_token"])
            if kwargs["access_token"] == "stale-token":
                request = httpx.Request("GET", "https://open-api.tiktokglobalshop.com/order/202309/orders/search")
                response = httpx.Response(401, request=request)
                raise httpx.HTTPStatusError("expired", request=request, response=response)
            return fake_summary

        def fake_refresh(
            session: Session,
            *,
            runtime_name: str,
            force: bool = False,
            shop_id: str = "",
            shop_cipher: str = "",
        ):
            if not force:
                return None
            self.assertEqual(shop_id, "7495987383262087496")
            self.assertEqual(shop_cipher, "cipher-1")
            auth_row = session.exec(select(TikTokAuth).where(TikTokAuth.tiktok_shop_id == "7495987383262087496")).first()
            self.assertIsNotNone(auth_row)
            auth_row.access_token = "fresh-token"
            auth_row.source = "oauth_refresh"
            session.add(auth_row)
            session.commit()
            return {"status": "updated", "auth_record": {"tiktok_shop_id": auth_row.tiktok_shop_id}}

        import app.shared as _shared_module
        with patch.object(_shared_module, "managed_session", side_effect=fake_managed_session), patch.object(
            _shared_module, "_refresh_tiktok_auth_if_needed", side_effect=fake_refresh
        ), patch.object(
            _shared_module, "pull_tiktok_orders", side_effect=fake_pull_tiktok_orders
        ), patch.object(
            _shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://open-api.tiktokglobalshop.com"
        ), patch.object(main_module.settings, "tiktok_app_key", "app-key"), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_shop_id", "7495987383262087496"
        ), patch.object(
            main_module.settings, "tiktok_shop_cipher", "env-cipher"
        ), patch.object(
            main_module.settings, "tiktok_access_token", "env-stale-token"
        ), patch.object(
            main_module.settings, "tiktok_refresh_token", "env-refresh-token"
        ), patch.object(
            main_module.settings, "tiktok_sync_enabled", True
        ), patch.object(
            main_module.settings, "tiktok_sync_limit", 25
        ), patch.object(
            main_module.settings, "tiktok_sync_lookback_hours", 24.0
        ):
            result = _shared_module.run_tiktok_pull_cycle(
                runtime_name="test_tiktok_runtime",
                limit=10,
                trigger="manual",
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(pull_tokens, ["stale-token", "fresh-token"])

    def test_tiktok_orders_poll_reports_latest_updated_at(self) -> None:
        created_at = utcnow()
        updated_at = utcnow()
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-1",
                    shop_id="shop-1",
                    order_number="#1001",
                    created_at=created_at,
                    updated_at=updated_at,
                    financial_status="paid",
                    total_price=12.0,
                )
            )
            session.commit()

            from starlette.requests import Request as _Request
            _req = _Request({"type": "http", "method": "GET", "path": "/tiktok/orders/poll", "headers": [], "scheme": "http", "server": ("testserver", 80)})
            from app.routers.tiktok_orders import tiktok_orders_poll as _tiktok_orders_poll
            with patch("app.routers.tiktok_orders.require_role_response", return_value=None):
                payload = _tiktok_orders_poll(request=_req, session=session)

        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["latest_updated_at"], updated_at.isoformat())

    def test_tiktok_streamer_poll_scopes_orders_to_selected_creator(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 7200
        main_end = now_ts - 5400
        boss_start = now_ts - 1800
        boss_end = now_ts + 1800
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-order",
                    order_number="#1001",
                    created_at=datetime.fromtimestamp(main_start + 60, tz=timezone.utc),
                    updated_at=datetime.fromtimestamp(main_start + 120, tz=timezone.utc),
                    financial_status="paid",
                    subtotal_price=10.0,
                    total_price=10.0,
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="boss-order",
                    order_number="#2001",
                    created_at=datetime.fromtimestamp(boss_start + 60, tz=timezone.utc),
                    updated_at=datetime.fromtimestamp(boss_start + 120, tz=timezone.utc),
                    financial_status="paid",
                    subtotal_price=20.0,
                    total_price=20.0,
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": main_end,
                    "gmv": 10.0,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": boss_end,
                    "gmv": 123.45,
                    "sku_orders": 7,
                    "items_sold": 14,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )
                legacy_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator=None,
                    stream="boss-live",
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in payload["orders"]], ["boss-order"])
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["stream_gmv"], 123.45)
        self.assertEqual(payload["stream_orders"], 7)
        self.assertEqual(payload["stream_items"], 14)
        self.assertEqual(payload["stream_metric_source"], "tiktok_live_session")
        self.assertEqual(payload["stream_metric_label"], "TikTok live attribution")
        self.assertEqual(payload["selected_creator"], "degenboss0")
        self.assertIn("@degenboss0", payload["selected_creator_label"])
        self.assertEqual(payload["selected_stream_id"], "boss-live")
        self.assertEqual(payload["creator_order_attribution"], "time_window")
        self.assertEqual(legacy_payload["selected_creator"], "degenboss0")

    def test_tiktok_streamer_poll_cursor_does_not_move_backward_after_refund_update(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now = datetime.now(timezone.utc)
        older_paid_updated = now - timedelta(minutes=10)
        refund_updated = now - timedelta(minutes=2)
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="paid-old-cursor-order",
                    order_number="#3001",
                    created_at=now - timedelta(minutes=30),
                    updated_at=older_paid_updated,
                    financial_status="paid",
                    subtotal_price=12.0,
                    total_price=12.0,
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="refunded-cursor-order",
                    order_number="#3002",
                    created_at=now - timedelta(minutes=20),
                    updated_at=refund_updated,
                    financial_status="refunded",
                    subtotal_price=20.0,
                    total_price=20.0,
                    line_items_json=json.dumps([
                        {"product_id": "refund-product", "title": "Refunded item", "quantity": 1}
                    ]),
                )
            )
            session.commit()

            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": int((now - timedelta(hours=1)).timestamp()),
                    "end_time": 0,
                    "gmv": 12.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                },
            ]
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                first_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=(refund_updated - timedelta(seconds=1)).isoformat(),
                    session=session,
                )
                second_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=refund_updated.isoformat(),
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in first_payload["orders"]], ["refunded-cursor-order"])
        self.assertEqual(first_payload["latest_updated_at"], refund_updated.isoformat())
        self.assertEqual(second_payload["orders"], [])
        self.assertEqual(second_payload["latest_updated_at"], refund_updated.isoformat())

    def test_tiktok_streamer_poll_scopes_overlapping_shared_shop_by_live_product_identity(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_utc = datetime.now(timezone.utc)
        now_ts = int(now_utc.timestamp())
        main_start = now_ts - 3600
        boss_start = now_ts - 1800
        today_start_utc = now_utc.astimezone(main_module.PACIFIC_TZ).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ).astimezone(timezone.utc)
        # Keep the order inside the dashboard's Pacific-day "today" window even
        # when this test runs in the first few minutes after midnight.
        order_time = max(now_utc - timedelta(minutes=5), today_start_utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    scopes_json=json.dumps(["seller.affiliate_collaboration.read"]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-account-order",
                    shop_id="7495987383262087496",
                    shop_cipher="shared-shop-cipher",
                    order_number="#1001",
                    created_at=order_time,
                    updated_at=order_time,
                    customer_name="Main Buyer",
                    financial_status="paid",
                    subtotal_price=80.0,
                    total_price=88.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 80.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="boss-account-order",
                    shop_id="7495987383262087496",
                    shop_cipher="shared-shop-cipher",
                    order_number="#2001",
                    created_at=order_time + timedelta(seconds=1),
                    updated_at=order_time + timedelta(seconds=1),
                    customer_name="Boss Buyer",
                    financial_status="paid",
                    subtotal_price=35.0,
                    total_price=38.5,
                    line_items_json=json.dumps([
                        {"product_id": "p-boss", "product_name": "Boss Pack", "quantity": 1, "sale_price": 35.0}
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "shop_id": "oauth-open-id",
                    "shop_cipher": "shared-shop-cipher",
                    "start_time": main_start,
                    "end_time": 0,
                    "gmv": 0.0,
                    "sku_orders": 0,
                    "items_sold": 0,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "shop_id": "oauth-open-id",
                    "shop_cipher": "shared-shop-cipher",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 0.0,
                    "sku_orders": 0,
                    "items_sold": 0,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()

            def fake_live_product_scope(stream_context):
                if stream_context.get("selected_stream_id") == "main-live":
                    return {
                        "available": True,
                        "products": [{"id": "p-main", "name": "Main Pack", "direct_gmv": 80.0, "items_sold": 1}],
                        "product_ids": {"p-main"},
                        "selected_product_ids": {"p-main"},
                        "selected_products": [{"id": "p-main", "name": "Main Pack", "direct_gmv": 80.0, "items_sold": 1}],
                        "exclude_product_ids": set(),
                        "source": "test_live_products",
                    }
                return {
                    "available": True,
                    "products": [{"id": "p-boss", "name": "Boss Pack", "direct_gmv": 35.0, "items_sold": 1}],
                    "product_ids": {"p-boss"},
                    "selected_product_ids": {"p-boss"},
                    "selected_products": [{"id": "p-boss", "name": "Boss Pack", "direct_gmv": 35.0, "items_sold": 1}],
                    "exclude_product_ids": set(),
                    "source": "test_live_products",
                }

            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ), patch.object(
                streamer_module,
                "_fetch_live_product_scope",
                side_effect=fake_live_product_scope,
            ):
                main_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )
                boss_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in main_payload["orders"]], ["main-account-order"])
        self.assertEqual(main_payload["total_count"], 1)
        self.assertEqual(main_payload["stream_gmv"], 80.0)
        self.assertEqual(main_payload["stream_top_buyers"][0]["name"], "Main Buyer")
        self.assertEqual(main_payload["session_gmv"], 80.0)
        self.assertEqual({row["name"] for row in main_payload["top_buyers"]}, {"Main Buyer"})
        self.assertEqual(sum(point["count"] for point in main_payload["order_velocity"]), 1)

        self.assertEqual([row["tiktok_order_id"] for row in boss_payload["orders"]], ["boss-account-order"])
        self.assertEqual(boss_payload["total_count"], 1)
        self.assertEqual(boss_payload["stream_gmv"], 35.0)
        self.assertEqual(boss_payload["stream_top_buyers"][0]["name"], "Boss Buyer")
        self.assertEqual(boss_payload["session_gmv"], 35.0)
        self.assertEqual({row["name"] for row in boss_payload["top_buyers"]}, {"Boss Buyer"})
        self.assertEqual(sum(point["count"] for point in boss_payload["order_velocity"]), 1)

    def test_tiktok_streamer_poll_prefers_affiliate_creator_for_shared_shop_same_sku(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_utc = datetime.now(timezone.utc)
        now_ts = int(now_utc.timestamp())
        main_start = now_ts - 1800
        boss_start = now_ts - 1700
        order_time = now_utc - timedelta(minutes=4)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    scopes_json=json.dumps(["seller.order.info", "seller.affiliate_collaboration.read"]),
                )
            )
            for order_id, creator, subtotal in (
                ("main-affiliate-order", "degencollectibles", 14.99),
                ("boss-affiliate-order", "degenboss0", 4.0),
            ):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=order_id,
                        shop_id="oauth-open-id",
                        shop_cipher="shared-shop-cipher",
                        order_number=order_id,
                        created_at=order_time,
                        updated_at=order_time,
                        customer_name="Buyer",
                        financial_status="paid",
                        subtotal_price=subtotal,
                        total_price=subtotal,
                        affiliate_creator_username=creator,
                        affiliate_content_type="LIVE",
                        affiliate_content_id=f"{creator}-live",
                        line_items_json=json.dumps([
                            {
                                "product_id": "shared-surprise-set",
                                "product_name": "NIHIL ZERO PACK",
                                "quantity": 1,
                                "sale_price": subtotal,
                                "sku_type": "UNKNOWN",
                            }
                        ]),
                    )
                )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "shop_id": "oauth-open-id",
                    "shop_cipher": "shared-shop-cipher",
                    "start_time": main_start,
                    "end_time": 0,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "shop_id": "oauth-open-id",
                    "shop_cipher": "shared-shop-cipher",
                    "start_time": boss_start,
                    "end_time": 0,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                main_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )
                boss_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in main_payload["orders"]], ["main-affiliate-order"])
        self.assertEqual([row["tiktok_order_id"] for row in boss_payload["orders"]], ["boss-affiliate-order"])
        self.assertEqual(main_payload["creator_order_attribution"], "affiliate_orders")
        self.assertEqual(boss_payload["creator_order_attribution"], "affiliate_orders")
        self.assertEqual(main_payload["stream_gmv"], 14.99)
        self.assertEqual(boss_payload["stream_gmv"], 4.0)
        self.assertNotEqual(main_payload["surprise_sets_total_gmv"], boss_payload["surprise_sets_total_gmv"])

    def test_tiktok_streamer_poll_keeps_refund_updates_visible(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        created_at = datetime.now(timezone.utc) - timedelta(minutes=20)
        paid_updated_at = created_at + timedelta(minutes=1)
        refund_updated_at = created_at + timedelta(minutes=10)
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="paid-still-visible",
                    order_number="#3001",
                    created_at=created_at,
                    updated_at=paid_updated_at,
                    customer_name="Paid Buyer",
                    financial_status="paid",
                    order_status="awaiting_shipment",
                    subtotal_price=25.0,
                    total_price=27.5,
                    line_items_json=json.dumps([
                        {"product_id": "p-paid", "product_name": "Paid Pack", "quantity": 1, "sale_price": 25.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="refund-update-visible",
                    order_number="#3002",
                    created_at=created_at + timedelta(minutes=1),
                    updated_at=refund_updated_at,
                    customer_name="Refund Buyer",
                    financial_status="paid",
                    order_status="cancel_requested",
                    subtotal_price=40.0,
                    total_price=44.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-refund", "product_name": "Refund Pack", "quantity": 1, "sale_price": 40.0}
                    ]),
                )
            )
            session.commit()

            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": int((created_at - timedelta(minutes=5)).timestamp()),
                    "end_time": 0,
                    "gmv": 25.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                },
            ]
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=(refund_updated_at - timedelta(seconds=1)).isoformat(),
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in payload["orders"]], ["refund-update-visible"])
        self.assertEqual(payload["orders"][0]["order_status"], "cancel_requested")
        self.assertEqual(payload["latest_updated_at"], refund_updated_at.isoformat())
        self.assertNotIn("#3002", payload["current_order_ids"])
        self.assertEqual(payload["stream_gmv"], 25.0)
        self.assertEqual(payload["stream_orders"], 1)
        self.assertEqual(sum(point["count"] for point in payload["order_velocity"]), 1)

    def test_tiktok_streamer_template_refund_statuses_match_backend_classifier(self) -> None:
        template = (Path(__file__).parents[1] / "app" / "templates" / "tiktok_streamer.html").read_text()
        for status in (
            "canceled",
            "cancel_requested",
            "cancel_request",
            "return_requested",
            "return_or_refund_request_pending",
            "refund_complete",
        ):
            self.assertIn(f"'{status}'", template)

    def test_tiktok_streamer_poll_uses_local_secondary_metrics_when_tiktok_session_zero(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        boss_start = now_ts - 1800
        order_time = datetime.fromtimestamp(boss_start + 300, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="boss-local-order",
                    order_number="#2002",
                    created_at=order_time,
                    updated_at=order_time,
                    financial_status="paid",
                    subtotal_price=42.5,
                    total_price=45.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-boss", "product_name": "Boss Pack", "quantity": 2, "sale_price": 21.25}
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 0.0,
                    "sku_orders": 0,
                    "items_sold": 0,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in payload["orders"]], ["boss-local-order"])
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["stream_gmv"], 42.5)
        self.assertEqual(payload["stream_orders"], 1)
        self.assertEqual(payload["stream_items"], 2)
        self.assertEqual(payload["tiktok_gmv"], 0.0)
        self.assertEqual(payload["stream_metric_source"], "local_order_estimate")
        self.assertEqual(payload["stream_metric_label"], "local order estimate")
        self.assertEqual(payload["stream_metric_note"], "TikTok attribution delayed")

    def test_tiktok_streamer_poll_keeps_secondary_zero_when_tiktok_and_local_are_zero(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        boss_start = now_ts - 1800
        with Session(self.engine) as session:
            stream_sessions = [
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 0.0,
                    "sku_orders": 0,
                    "items_sold": 0,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual(payload["total_count"], 0)
        self.assertEqual(payload["stream_gmv"], 0.0)
        self.assertEqual(payload["stream_orders"], 0)
        self.assertEqual(payload["stream_items"], 0)
        self.assertEqual(payload["tiktok_gmv"], 0.0)
        self.assertEqual(payload["stream_metric_source"], "tiktok_live_session")
        self.assertEqual(payload["stream_metric_label"], "TikTok live attribution")
        self.assertIsNone(payload["stream_metric_note"])

    def test_tiktok_streamer_poll_falls_back_to_fresh_orders_when_live_session_missing(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        stale_session_end = now_ts - 1800
        fresh_order_time = datetime.fromtimestamp(now_ts - 300, tz=timezone.utc)
        old_order_time = datetime.fromtimestamp(now_ts - 2700, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="old-session-order",
                    order_number="#0901",
                    created_at=old_order_time,
                    updated_at=old_order_time,
                    financial_status="paid",
                    subtotal_price=10.0,
                    total_price=10.0,
                    line_items_json=json.dumps([
                        {"product_id": "old-pack", "product_name": "Old Pack", "quantity": 1, "sale_price": 10.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="fresh-fallback-order",
                    order_number="#1001",
                    created_at=fresh_order_time,
                    updated_at=fresh_order_time,
                    financial_status="paid",
                    subtotal_price=27.0,
                    total_price=28.56,
                    line_items_json=json.dumps([
                        {
                            "product_id": "surprise-set",
                            "product_name": "Surprise Set",
                            "quantity": 1,
                            "sale_price": 27.0,
                        }
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "stale-main-live",
                    "title": "DEGEN FUN WITH SUNA",
                    "username": "degencollectibles",
                    "start_time": now_ts - 7200,
                    "end_time": stale_session_end,
                    "gmv": 999.0,
                    "sku_orders": 99,
                    "items_sold": 99,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in payload["orders"]], ["fresh-fallback-order"])
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["stream_gmv"], 27.0)
        self.assertEqual(payload["stream_orders"], 1)
        self.assertEqual(payload["stream_items"], 1)
        self.assertTrue(payload["is_live"])
        self.assertEqual(payload["stream_range_source"], "order_activity_fallback")
        self.assertEqual(payload["creator_order_attribution"], "recent_orders")
        self.assertIn("fresh TikTok orders", payload["creator_order_attribution_message"])
        self.assertIn("fresh order fallback", payload["stream_metric_label"])
        self.assertIn("TikTok live session API", payload["stream_metric_note"])

    def test_public_tiktok_live_status_uses_fresh_orders_as_activity_fallback(self) -> None:
        import app.routers.tiktok_streamer as streamer_module

        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="main-shop",
                    shop_cipher="main-cipher",
                    seller_id="main-seller",
                    shop_name="Degen Collectibles",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="fresh-public-status-order",
                    shop_id="main-shop",
                    shop_cipher="main-cipher",
                    seller_id="main-seller",
                    order_number="#1002",
                    created_at=now - timedelta(minutes=4),
                    updated_at=now - timedelta(minutes=4),
                    financial_status="paid",
                    subtotal_price=21.0,
                    total_price=22.0,
                    line_items_json=json.dumps([
                        {"product_id": "surprise-set", "product_name": "Surprise Set", "quantity": 1, "sale_price": 21.0}
                    ]),
                )
            )
            session.commit()

            with patch.object(streamer_module, "_get_live_sessions_list", return_value=[]), patch.object(
                streamer_module,
                "_get_live_sessions_list_checked_at",
                return_value=now,
            ), patch.object(streamer_module, "_get_live_session_snapshot", return_value={}), patch.object(
                streamer_module.settings,
                "tiktok_shop_id",
                "main-shop",
            ), patch.object(streamer_module.settings, "tiktok_shop_cipher", "main-cipher"):
                payload = streamer_module._public_tiktok_live_status_payload(session)

        main = payload["channels"][0]
        self.assertTrue(main["isLive"])
        self.assertEqual(main["statusSource"], "fresh_orders")
        self.assertEqual(main["title"], "Fresh TikTok order activity")
        self.assertFalse(payload["channels"][1]["isLive"])
        serialized = json.dumps(payload)
        for internal_field in ("gmv", "sku_orders", "orders", "live_room_id", "vip_buyer_threshold"):
            self.assertNotIn(f'"{internal_field}":', serialized)

    def test_tiktok_streamer_poll_hides_order_feed_when_creator_sessions_overlap(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 3600
        main_end = 0
        boss_start = now_ts - 1800
        boss_end = 0
        overlap_order_time = datetime.fromtimestamp(now_ts - 300, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    scopes_json=json.dumps(["seller.affiliate_collaboration.read"]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-overlap-order",
                    order_number="#1001",
                    created_at=overlap_order_time,
                    updated_at=overlap_order_time,
                    financial_status="paid",
                    subtotal_price=12.0,
                    total_price=12.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 12.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="boss-overlap-order",
                    order_number="#7812",
                    created_at=overlap_order_time,
                    updated_at=overlap_order_time,
                    financial_status="paid",
                    subtotal_price=1.0,
                    total_price=1.1,
                    line_items_json=json.dumps([
                        {"product_id": "p-boss", "product_name": "BANG EX!", "quantity": 1, "sale_price": 1.0}
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": main_end,
                    "gmv": 43100.0,
                    "sku_orders": 656,
                    "items_sold": 1359,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": boss_end,
                    "gmv": 843.05,
                    "sku_orders": 279,
                    "items_sold": 279,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ), patch.object(
                streamer_module,
                "_fetch_live_product_scope",
                return_value={
                    "available": True,
                    "products": [{"id": "p-main", "name": "Main Pack", "direct_gmv": 12.0, "items_sold": 1}],
                    "product_ids": {"p-main"},
                },
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual([row["tiktok_order_id"] for row in payload["orders"]], ["main-overlap-order"])
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["stream_top_sellers"][0]["title"], "Main Pack")
        self.assertEqual(payload["stream_top_buyers"][0]["name"], "Guest")
        self.assertEqual(sum(point["count"] for point in payload["order_velocity"]), 1)
        self.assertEqual(payload["stream_gmv"], 12.0)
        self.assertEqual(payload["stream_orders"], 1)
        self.assertEqual(payload["stream_items"], 1)
        self.assertIsNone(payload["stream_metric_source"])
        self.assertEqual(payload["tiktok_gmv"], 43100.0)
        self.assertEqual(payload["creator_order_attribution"], "live_products")
        self.assertTrue(payload["creator_order_rows_precise"])
        self.assertIn("Estimated from the live products", payload["creator_order_attribution_message"])

    def test_tiktok_streamer_poll_pauses_overlap_when_attribution_is_unavailable(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 3600
        boss_start = now_ts - 1800
        main_order_time = datetime.fromtimestamp(now_ts - 180, tz=timezone.utc)
        boss_order_time = datetime.fromtimestamp(now_ts - 120, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    scopes_json=json.dumps(["seller.affiliate_collaboration.read"]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-product-order",
                    order_number="#1001",
                    created_at=main_order_time,
                    updated_at=main_order_time,
                    financial_status="paid",
                    subtotal_price=120.0,
                    total_price=120.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 120.0}
                    ]),
                )
            )
            for idx in range(3):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=f"boss-product-order-{idx}",
                        order_number=f"#200{idx}",
                        created_at=boss_order_time,
                        updated_at=boss_order_time,
                        financial_status="paid",
                        subtotal_price=1.0,
                        total_price=1.0,
                        line_items_json=json.dumps([
                            {"product_id": "p-boss", "product_name": "BANG EX!", "quantity": 1, "sale_price": 1.0}
                        ]),
                    )
                )
            for idx, product_id in enumerate(("p-boss-2", "p-boss-3"), start=1):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=f"boss-extra-product-order-{idx}",
                        order_number=f"#210{idx}",
                        created_at=boss_order_time,
                        updated_at=boss_order_time,
                        financial_status="paid",
                        subtotal_price=float(idx + 1),
                        total_price=float(idx + 1),
                        line_items_json=json.dumps([
                            {
                                "product_id": product_id,
                                "product_name": f"Boss Extra {idx}",
                                "quantity": 1,
                                "sale_price": float(idx + 1),
                            }
                        ]),
                    )
                )
            for idx in range(4):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=f"main-overlap-order-{idx}",
                        order_number=f"#300{idx}",
                        created_at=boss_order_time,
                        updated_at=boss_order_time,
                        financial_status="paid",
                        subtotal_price=25.0,
                        total_price=25.0,
                        line_items_json=json.dumps([
                            {
                                "product_id": "p-main-overlap",
                                "product_name": "Main Overlap Pack",
                                "quantity": 1,
                                "sale_price": 25.0,
                            }
                        ]),
                    )
                )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": 0,
                    "gmv": 120.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                    "avg_price": 120.0,
                    "products_added": 10,
                    "different_products_sold": 10,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 3.0,
                    "sku_orders": 3,
                    "items_sold": 3,
                    "avg_price": 1.0,
                    "products_added": 1,
                    "different_products_sold": 3,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ), patch.object(
                streamer_module,
                "_fetch_live_product_scope",
                return_value={
                    "available": False,
                    "products": [],
                    "product_ids": set(),
                    "exclude_product_ids": set(),
                    "source": "",
                },
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual(payload["orders"], [])
        self.assertEqual(payload["total_count"], 0)
        self.assertEqual(payload["stream_gmv"], 0.0)
        self.assertEqual(payload["stream_orders"], 0)
        self.assertEqual(payload["stream_items"], 0)

    def test_tiktok_streamer_poll_pauses_both_overlap_feeds_when_affiliate_scope_missing(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 3600
        boss_start = now_ts - 1800
        order_time = datetime.fromtimestamp(now_ts - 120, tz=timezone.utc)
        with Session(self.engine) as session:
            for order_id, product_name, subtotal in (
                ("shared-main-order", "Main Pack", 12.0),
                ("shared-boss-order", "Boss Pack", 1.0),
            ):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=order_id,
                        order_number=f"#{order_id}",
                        created_at=order_time,
                        updated_at=order_time,
                        financial_status="paid",
                        subtotal_price=subtotal,
                        total_price=subtotal,
                        line_items_json=json.dumps([
                            {
                                "product_id": order_id,
                                "product_name": product_name,
                                "quantity": 1,
                                "sale_price": subtotal,
                            }
                        ]),
                    )
                )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": 0,
                    "gmv": 12.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 1.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ):
                main_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )
                streamer_module._gmv_cache.clear()
                boss_payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual(main_payload["creator_order_attribution"], "affiliate_scope_missing")
        self.assertEqual(boss_payload["creator_order_attribution"], "affiliate_scope_missing")
        self.assertFalse(main_payload["creator_order_rows_precise"])
        self.assertFalse(boss_payload["creator_order_rows_precise"])
        self.assertEqual(main_payload["orders"], [])
        self.assertEqual(boss_payload["orders"], [])
        self.assertEqual(main_payload["total_count"], 0)
        self.assertEqual(boss_payload["total_count"], 0)
        self.assertEqual(main_payload["session_gmv"], 0.0)
        self.assertEqual(boss_payload["session_gmv"], 0.0)

    def test_tiktok_streamer_poll_does_not_share_today_gmv_when_secondary_has_no_session(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 3600
        order_time = datetime.fromtimestamp(now_ts - 120, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="dc-llc-shop",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    seller_name="D.C. LLC",
                    scopes_json=json.dumps(["seller.order.info", "seller.affiliate_collaboration.read"]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-visible-order",
                    order_number="#1001",
                    shop_id="dc-llc-shop",
                    shop_cipher="shared-shop-cipher",
                    created_at=order_time,
                    updated_at=order_time,
                    financial_status="paid",
                    subtotal_price=120.0,
                    total_price=120.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 120.0}
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": 0,
                    "gmv": 120.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ), patch.object(streamer_module.settings, "tiktok_shop_id", "dc-llc-shop"), patch.object(
                streamer_module.settings,
                "tiktok_shop_cipher",
                "shared-shop-cipher",
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degenboss0",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual(payload["orders"], [])
        self.assertEqual(payload["total_count"], 0)
        self.assertEqual(payload["session_gmv"], 0.0)
        self.assertEqual(payload["session_orders"], 0)
        self.assertEqual(payload["session_total_orders"], 0)
        self.assertIsNone(payload["stream_gmv"])
        self.assertEqual(payload["creator_order_attribution"], "no_session")
        self.assertFalse(payload["creator_order_rows_precise"])

    def test_tiktok_streamer_poll_uses_live_product_ids_for_main_creator(self) -> None:
        import app.routers.tiktok_streamer as streamer_module
        from starlette.requests import Request as _Request

        now_ts = int(time.time())
        main_start = now_ts - 3600
        boss_start = now_ts - 1800
        order_time = datetime.fromtimestamp(now_ts - 120, tz=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="oauth-open-id",
                    shop_cipher="shared-shop-cipher",
                    access_token="token",
                    scopes_json=json.dumps(["seller.affiliate_collaboration.read"]),
                )
            )
            for idx in range(2):
                session.add(
                    TikTokOrder(
                        tiktok_order_id=f"main-live-order-{idx}",
                        order_number=f"#10{idx}",
                        created_at=order_time,
                        updated_at=order_time,
                        financial_status="paid",
                        subtotal_price=50.0,
                        total_price=55.0,
                        line_items_json=json.dumps([
                            {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 50.0}
                        ]),
                    )
                )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-cancel-requested-order",
                    order_number="#1099",
                    created_at=order_time + timedelta(seconds=1),
                    updated_at=order_time + timedelta(seconds=1),
                    financial_status="paid",
                    order_status="cancel_requested",
                    subtotal_price=50.0,
                    total_price=55.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 50.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="main-pending-order",
                    order_number="#1098",
                    created_at=order_time + timedelta(seconds=2),
                    updated_at=order_time + timedelta(seconds=2),
                    financial_status="pending",
                    subtotal_price=60.0,
                    total_price=66.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-main", "product_name": "Main Pack", "quantity": 1, "sale_price": 60.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="shop-order",
                    order_number="#2001",
                    created_at=order_time,
                    updated_at=order_time,
                    financial_status="paid",
                    subtotal_price=25.0,
                    total_price=27.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-shop", "product_name": "Shop Pack", "quantity": 1, "sale_price": 25.0}
                    ]),
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="boss-live-order",
                    order_number="#3001",
                    created_at=order_time,
                    updated_at=order_time,
                    financial_status="paid",
                    subtotal_price=1.0,
                    total_price=1.0,
                    line_items_json=json.dumps([
                        {"product_id": "p-boss", "product_name": "Boss Pack", "quantity": 1, "sale_price": 1.0}
                    ]),
                )
            )
            session.commit()

            stream_sessions = [
                {
                    "id": "main-live",
                    "title": "Main live",
                    "username": "degencollectibles",
                    "start_time": main_start,
                    "end_time": 0,
                    "gmv": 999.0,
                    "sku_orders": 99,
                    "items_sold": 99,
                    "avg_price": 50.0,
                    "products_added": 1,
                    "different_products_sold": 1,
                },
                {
                    "id": "boss-live",
                    "title": "Boss live",
                    "username": "degenboss0",
                    "start_time": boss_start,
                    "end_time": 0,
                    "gmv": 1.0,
                    "sku_orders": 1,
                    "items_sold": 1,
                    "avg_price": 1.0,
                    "products_added": 1,
                    "different_products_sold": 1,
                },
            ]
            req = _Request({
                "type": "http",
                "method": "GET",
                "path": "/tiktok/streamer/poll",
                "headers": [],
                "scheme": "http",
                "server": ("testserver", 80),
            })
            streamer_module._gmv_cache.clear()
            with patch("app.routers.tiktok_streamer._require_live_stream", return_value=None), patch.object(
                streamer_module,
                "_get_live_sessions_list",
                return_value=stream_sessions,
            ), patch.object(
                streamer_module,
                "_fetch_live_product_scope",
                return_value={
                    "available": True,
                    "products": [{"id": "p-main", "name": "Main Pack", "direct_gmv": 100.0, "items_sold": 2}],
                    "product_ids": {"p-main"},
                    "selected_product_ids": {"p-main"},
                    "selected_products": [{"id": "p-main", "name": "Main Pack", "direct_gmv": 100.0, "items_sold": 2}],
                    "exclude_product_ids": set(),
                    "source": "test_live_products",
                },
            ):
                payload = streamer_module.tiktok_streamer_poll(
                    request=req,
                    creator="degencollectibles",
                    stream=None,
                    since=None,
                    session=session,
                )

        self.assertEqual(
            {row["tiktok_order_id"] for row in payload["orders"]},
            {"main-live-order-0", "main-live-order-1"},
        )
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["stream_gmv"], 100.0)
        self.assertEqual(payload["stream_orders"], 2)
        self.assertEqual(payload["stream_items"], 2)
        self.assertEqual(payload["session_gmv"], 100.0)
        self.assertEqual(payload["session_orders"], 2)
        self.assertEqual(payload["session_total_orders"], 4)
        self.assertEqual(payload["tiktok_gmv"], 999.0)
        self.assertIsNone(payload["stream_metric_source"])

    def test_periodic_tiktok_pull_loop_uses_automatic_trigger(self) -> None:
        stop_event = asyncio.Event()
        calls: list[dict[str, object]] = []

        def fake_run_tiktok_pull_cycle(**kwargs):
            calls.append(kwargs)
            stop_event.set()
            return {
                "status": "success",
                "shop_id": "7495987383262087496",
                "fetched": 1,
                "inserted": 1,
                "updated": 0,
                "failed": 0,
                "detail_calls": 1,
            }

        async def immediate_to_thread(func, /, *args, **kwargs):
            return func(*args, **kwargs)

        import app.shared as _shared_module
        with patch.object(_shared_module, "run_tiktok_pull_cycle", side_effect=fake_run_tiktok_pull_cycle), patch.object(
            _shared_module, "pull_tiktok_orders", object()
        ), patch.object(
            _shared_module.asyncio, "to_thread", side_effect=immediate_to_thread
        ), patch.object(main_module.settings, "tiktok_sync_enabled", True), patch.object(
            main_module.settings, "tiktok_app_key", "app-key"
        ), patch.object(
            main_module.settings, "tiktok_app_secret", "app-secret"
        ), patch.object(
            main_module.settings, "tiktok_sync_limit", 50
        ), patch.object(
            main_module.settings, "tiktok_sync_lookback_hours", 12.0
        ), patch.object(
            main_module.settings, "tiktok_sync_interval_minutes", 1
        ):
            asyncio.run(main_module.periodic_tiktok_pull_loop(stop_event))

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["trigger"], "automatic")
        self.assertEqual(calls[0]["limit"], 50)
        self.assertEqual(calls[0]["lookback_hours"], 12.0)

    def test_tiktok_auth_record_builds_pending_lookup_key_without_shop_identifier(self) -> None:
        record = build_tiktok_auth_record(
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
            },
            app_key="app-key",
            redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
        )

        self.assertTrue(record["tiktok_shop_id"].startswith("pending:"))
        self.assertEqual(record["source"], "oauth_callback_pending")

    def test_tiktok_auth_record_uses_configured_fallback_shop_id(self) -> None:
        record = build_tiktok_auth_record(
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
            },
            app_key="app-key",
            redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
            fallback_shop_id="7495987383262087496",
        )

        self.assertEqual(record["tiktok_shop_id"], "7495987383262087496")
        self.assertEqual(record["source"], "oauth_callback")

    def test_tiktok_auth_record_uses_auth_code_seed_for_pending_lookup_key(self) -> None:
        record = build_tiktok_auth_record(
            {},
            app_key="app-key",
            redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
            pending_key_seed="TTP_auth_code_seed",
        )

        self.assertTrue(record["tiktok_shop_id"].startswith("pending:"))
        self.assertEqual(record["source"], "oauth_callback_pending")

    def test_tiktok_auth_record_accepts_open_id_fallback(self) -> None:
        record = build_tiktok_auth_record(
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "open_id": "open-1",
                "scope": "seller.order.info,seller.return_refund.basic",
            },
            app_key="app-key",
            redirect_uri="https://ops.degencollectibles.com/integrations/tiktok/callback",
        )

        self.assertEqual(record["tiktok_shop_id"], "open-1")
        self.assertEqual(record["open_id"], "open-1")
        self.assertEqual(
            json.loads(record["scopes_json"]),
            "seller.order.info,seller.return_refund.basic",
        )

    def test_tiktok_model_tables_include_reconciliation_columns(self) -> None:
        with self.engine.connect() as connection:
            auth_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(tiktok_auth)")
            }
            order_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(tiktok_orders)")
            }

        self.assertTrue({"tiktok_shop_id", "access_token", "refresh_token", "scopes_json"}.issubset(auth_columns))
        self.assertTrue(
            {
                "tiktok_order_id",
                "shop_id",
                "line_items_summary_json",
                "order_status",
                "affiliate_creator_username",
                "affiliate_content_type",
                "affiliate_content_id",
                "affiliate_attribution_json",
            }.issubset(order_columns)
        )


if __name__ == "__main__":
    unittest.main()
