import asyncio
import io
import json
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import httpx
from sqlmodel import SQLModel, Session, create_engine

from app.tiktok.tiktok_auth_refresh import refresh_tiktok_auth_if_needed
from app.models import TikTokAuth, TikTokCreatorAuth


def _utcnow():
    return datetime.now(timezone.utc)


def _future(minutes=60):
    return _utcnow() + timedelta(minutes=minutes)


def _past(minutes=60):
    return _utcnow() - timedelta(minutes=minutes)


class RefreshTiktokAuthTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_auth(self, session, access_token="tok", refresh_token="rtok",
                   access_token_expires_at=None, app_key="key"):
        auth = TikTokAuth(
            app_key=app_key,
            tiktok_shop_id="shop-1",
            access_token=access_token,
            refresh_token=refresh_token,
            access_token_expires_at=access_token_expires_at or _future(60),
            created_at=_utcnow(),
            updated_at=_utcnow(),
            source="oauth",
        )
        session.add(auth)
        session.commit()
        session.refresh(auth)
        return auth

    def test_no_auth_row_returns_none(self):
        with Session(self.engine) as session:
            result = refresh_tiktok_auth_if_needed(
                session,
                runtime_name="test",
                resolve_base_url=lambda: "https://example.com",
            )
        self.assertIsNone(result)

    def test_valid_token_not_expiring_skips_refresh(self):
        """Token not close to expiry — no refresh needed."""
        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_future(60))
            with patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )
        self.assertIsNone(result)

    def test_missing_app_secret_returns_none(self):
        with Session(self.engine) as session:
            self._seed_auth(session)
            with patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = ""  # missing
                mock_settings.tiktok_refresh_token = "rtok"
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )
        self.assertIsNone(result)

    def test_expired_token_triggers_refresh(self):
        fake_result = {
            "data": {
                "access_token": "new-tok",
                "refresh_token": "new-rtok",
                "access_token_expire_in": 86400,
            }
        }

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            return fake_result

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings, \
                 patch("app.tiktok.tiktok_auth_refresh.upsert_tiktok_auth_from_callback",
                       return_value=("inserted", {"tiktok_shop_id": "shop-1"})) as mock_upsert:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "inserted")

    def test_forced_refresh_targets_matching_shop_not_latest_row(self):
        fake_result = {
            "data": {
                "access_token": "main-new-token",
                "refresh_token": "main-new-refresh",
                "access_token_expire_in": 86400,
            }
        }
        refresh_tokens = []

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            refresh_tokens.append(refresh_token)
            return fake_result

        with Session(self.engine) as session:
            self._seed_auth(
                session,
                access_token="main-stale-token",
                refresh_token="main-refresh",
                access_token_expires_at=_past(5),
            )
            secondary = TikTokAuth(
                app_key="key",
                tiktok_shop_id="secondary-shop",
                shop_cipher="secondary-cipher",
                access_token="secondary-token",
                refresh_token="secondary-refresh",
                access_token_expires_at=_future(60),
                created_at=_utcnow(),
                updated_at=_utcnow() + timedelta(minutes=1),
                source="oauth",
            )
            session.add(secondary)
            session.commit()
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings, \
                 patch("app.tiktok.tiktok_auth_refresh.upsert_tiktok_auth_from_callback",
                       return_value=("updated", {"tiktok_shop_id": "shop-1"})):
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    force=True,
                    shop_id="shop-1",
                    resolve_base_url=lambda: "https://example.com",
                )

        self.assertIsNotNone(result)
        self.assertEqual(refresh_tokens, ["main-refresh"])

    def test_update_state_called_when_provided(self):
        fake_result = {"data": {"access_token": "t", "refresh_token": "r", "access_token_expire_in": 3600}}

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            return fake_result

        update_state_calls = []

        def fake_update_state(**kwargs):
            update_state_calls.append(kwargs)

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings, \
                 patch("app.tiktok.tiktok_auth_refresh.upsert_tiktok_auth_from_callback",
                       return_value=("inserted", {"tiktok_shop_id": "shop-1"})):
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                    update_state=fake_update_state,
                )

        self.assertEqual(len(update_state_calls), 1)

    def test_exception_during_refresh_propagates(self):
        def exploding_refresh(client, *, base_url, app_key, app_secret, refresh_token):
            raise RuntimeError("network error")

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", exploding_refresh), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                with self.assertRaises(RuntimeError):
                    refresh_tiktok_auth_if_needed(
                        session,
                        runtime_name="test",
                        resolve_base_url=lambda: "https://example.com",
                    )

    def test_expiring_creator_auth_row_refreshes_after_seller_check(self):
        fake_result = {
            "data": {
                "access_token": "creator-new-token",
                "refresh_token": "creator-new-refresh",
                "access_token_expire_in": 86400,
                "open_id": "creator-open-id",
            }
        }
        refresh_tokens = []

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            refresh_tokens.append(refresh_token)
            return fake_result

        with Session(self.engine) as session:
            creator_auth = TikTokCreatorAuth(
                creator_username="degenboss0",
                open_id="creator-open-id",
                app_key="key",
                access_token="creator-old-token",
                refresh_token="creator-refresh",
                access_token_expires_at=_past(5),
                scopes_json='["creator.affiliate_collaboration.read"]',
                created_at=_utcnow(),
                updated_at=_utcnow(),
                source="creator_oauth_callback",
            )
            session.add(creator_auth)
            session.commit()
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )
            refreshed = session.get(TikTokCreatorAuth, creator_auth.id)

        self.assertIsNotNone(result)
        self.assertEqual(result["creator_auth_refreshed"], 1)
        self.assertEqual(refresh_tokens, ["creator-refresh"])
        self.assertEqual(refreshed.access_token, "creator-new-token")
        self.assertEqual(refreshed.refresh_token, "creator-new-refresh")


class PeriodicLoopTests(unittest.TestCase):
    def test_loop_exits_when_stop_event_set(self):
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        async def run():
            stop = asyncio.Event()
            stop.set()  # pre-set so the loop exits without sleeping
            with patch("app.discord.worker_service.settings") as mock_settings:
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001  # very short
                # Loop should exit cleanly after stop_event is set
                task = asyncio.create_task(periodic_tiktok_token_refresh_loop(stop))
                await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(run())

    def test_loop_catches_exceptions_without_crashing(self):
        """The per-iteration try/except swallows errors so the loop can keep running."""
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        sleep_calls = [0]
        secret = "worker-app-secret-SENTINEL-129a"
        refresh_token = "worker-refresh-token-SENTINEL-741b"

        async def fake_to_thread(fn, *args, **kwargs):
            if getattr(fn, "__name__", "") == "refresh_tiktok_creator_token_if_needed":
                return None
            request = httpx.Request(
                "GET",
                "https://auth.tiktok-shops.com/api/v2/token/refresh",
                params={"app_secret": secret, "refresh_token": refresh_token},
            )
            response = httpx.Response(
                400,
                request=request,
                headers={"x-debug-secret": secret},
                content=refresh_token,
            )
            exc = httpx.HTTPStatusError(
                f"failed app_secret={secret} refresh_token={refresh_token}",
                request=request,
                response=response,
            )
            exc.tiktok_error_code = "105001"
            raise exc

        async def fake_sleep(seconds):
            sleep_calls[0] += 1
            # On second sleep, the loop body has already run once and caught the error.
            # Set stop so the loop exits cleanly on its next iteration check.
            if sleep_calls[0] >= 2:
                stop.set()

        async def run():
            nonlocal stop
            stop = asyncio.Event()
            with patch("app.discord.worker_service.settings") as mock_settings, \
                 patch("asyncio.to_thread", fake_to_thread), \
                 patch("app.discord.worker_service.managed_session"), \
                 patch("asyncio.sleep", fake_sleep):
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001
                mock_settings.runtime_name = "test"
                task = asyncio.create_task(periodic_tiktok_token_refresh_loop(stop))
                await asyncio.wait_for(task, timeout=2.0)

        stop = None
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            asyncio.run(run())

        output = stdout.getvalue() + stderr.getvalue()
        self.assertNotIn(secret, output)
        self.assertNotIn(refresh_token, output)
        lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["action"], "tiktok.auth.refresh_failed")
        self.assertEqual(payload["error"], "TikTok token refresh failed")
        self.assertEqual(payload["error_type"], "HTTPStatusError")
        self.assertEqual(payload["error_code"], "105001")
        self.assertEqual(payload["status_code"], 400)
        self.assertEqual(payload["method"], "GET")
        self.assertEqual(payload["endpoint_host"], "auth.tiktok-shops.com")
        self.assertEqual(payload["endpoint_path"], "/api/v2/token/refresh")

    def test_loop_attempts_shop_then_open_creator_refresh_with_separate_sessions(self):
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        session_markers = []
        calls = []
        sleep_calls = 0

        @contextmanager
        def fake_managed_session():
            marker = object()
            session_markers.append(marker)
            yield marker

        def fake_shop_refresh(session, **kwargs):
            calls.append(("shop", session, kwargs))

        def fake_creator_refresh(session, **kwargs):
            calls.append(("creator", session, kwargs))

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        async def fake_sleep(seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                stop.set()

        async def run():
            nonlocal stop
            stop = asyncio.Event()
            with patch("app.discord.worker_service.settings") as mock_settings, \
                 patch("app.discord.worker_service.refresh_tiktok_auth_if_needed", fake_shop_refresh), \
                 patch(
                     "app.discord.worker_service.refresh_tiktok_creator_token_if_needed",
                     fake_creator_refresh,
                     create=True,
                 ), \
                 patch("app.discord.worker_service.managed_session", fake_managed_session), \
                 patch("asyncio.to_thread", fake_to_thread), \
                 patch("asyncio.sleep", fake_sleep):
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001
                mock_settings.runtime_name = "test-runtime"
                await asyncio.wait_for(periodic_tiktok_token_refresh_loop(stop), timeout=2.0)

        stop = None
        asyncio.run(run())

        self.assertEqual([call[0] for call in calls], ["shop", "creator"])
        self.assertEqual(len(session_markers), 2)
        self.assertIs(calls[0][1], session_markers[0])
        self.assertIs(calls[1][1], session_markers[1])
        self.assertIsNot(session_markers[0], session_markers[1])
        self.assertEqual(calls[0][2]["runtime_name"], "test-runtime")
        self.assertEqual(calls[1][2], {"runtime_name": "test-runtime"})

    def test_shop_refresh_failure_does_not_block_creator_attempt(self):
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        creator_calls = []
        sleep_calls = 0
        secret = "seller-worker-secret-SENTINEL-18c4"
        refresh_token = "seller-worker-refresh-SENTINEL-6b21"

        @contextmanager
        def fake_managed_session():
            yield object()

        def fail_shop_refresh(session, **kwargs):
            request = httpx.Request(
                "GET",
                "https://auth.tiktok-shops.com/api/v2/token/refresh",
                params={"app_secret": secret, "refresh_token": refresh_token},
            )
            response = httpx.Response(
                401,
                headers={"x-secret": secret},
                content=refresh_token.encode(),
                request=request,
            )
            raise httpx.HTTPStatusError(
                f"seller failed {secret} {refresh_token}",
                request=request,
                response=response,
            )

        def fake_creator_refresh(session, **kwargs):
            creator_calls.append((session, kwargs))

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        async def fake_sleep(seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                stop.set()

        async def run():
            nonlocal stop
            stop = asyncio.Event()
            with patch("app.discord.worker_service.settings") as mock_settings, \
                 patch("app.discord.worker_service.refresh_tiktok_auth_if_needed", fail_shop_refresh), \
                 patch(
                     "app.discord.worker_service.refresh_tiktok_creator_token_if_needed",
                     fake_creator_refresh,
                     create=True,
                 ), \
                 patch("app.discord.worker_service.managed_session", fake_managed_session), \
                 patch("asyncio.to_thread", fake_to_thread), \
                 patch("asyncio.sleep", fake_sleep):
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001
                mock_settings.runtime_name = "test-runtime"
                await asyncio.wait_for(periodic_tiktok_token_refresh_loop(stop), timeout=2.0)

        stop = None
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            asyncio.run(run())

        output = stdout.getvalue() + stderr.getvalue()
        self.assertNotIn(secret, output)
        self.assertNotIn(refresh_token, output)
        self.assertEqual(len(creator_calls), 1)
        payloads = [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["method"], "GET")
        self.assertEqual(payloads[0]["endpoint_host"], "auth.tiktok-shops.com")
        self.assertEqual(payloads[0]["endpoint_path"], "/api/v2/token/refresh")

    def test_creator_refresh_failure_does_not_undo_shop_attempt_and_logs_safe_metadata(self):
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        shop_calls = []
        sleep_calls = 0
        secret = "creator-worker-secret-SENTINEL-49f2"
        refresh_token = "creator-worker-refresh-SENTINEL-73ae"

        @contextmanager
        def fake_managed_session():
            yield object()

        def fake_shop_refresh(session, **kwargs):
            shop_calls.append((session, kwargs))

        def fail_creator_refresh(session, **kwargs):
            request = httpx.Request(
                "POST",
                "https://open.tiktokapis.com/v2/oauth/token/",
                data={"client_secret": secret, "refresh_token": refresh_token},
            )
            response = httpx.Response(
                400,
                headers={"x-secret": secret},
                content=refresh_token.encode(),
                request=request,
            )
            error = httpx.HTTPStatusError(
                f"creator failed {secret} {refresh_token}",
                request=request,
                response=response,
            )
            error.tiktok_error_code = "105001"
            raise error

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        async def fake_sleep(seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                stop.set()

        async def run():
            nonlocal stop
            stop = asyncio.Event()
            with patch("app.discord.worker_service.settings") as mock_settings, \
                 patch("app.discord.worker_service.refresh_tiktok_auth_if_needed", fake_shop_refresh), \
                 patch(
                     "app.discord.worker_service.refresh_tiktok_creator_token_if_needed",
                     fail_creator_refresh,
                     create=True,
                 ), \
                 patch("app.discord.worker_service.managed_session", fake_managed_session), \
                 patch("asyncio.to_thread", fake_to_thread), \
                 patch("asyncio.sleep", fake_sleep):
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001
                mock_settings.runtime_name = "test-runtime"
                await asyncio.wait_for(periodic_tiktok_token_refresh_loop(stop), timeout=2.0)

        stop = None
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            asyncio.run(run())

        output = stdout.getvalue() + stderr.getvalue()
        self.assertNotIn(secret, output)
        self.assertNotIn(refresh_token, output)
        self.assertEqual(len(shop_calls), 1)
        payloads = [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["method"], "POST")
        self.assertEqual(payloads[0]["endpoint_host"], "open.tiktokapis.com")
        self.assertEqual(payloads[0]["endpoint_path"], "/v2/oauth/token/")
        self.assertEqual(payloads[0]["error_code"], "105001")


if __name__ == "__main__":
    unittest.main()
