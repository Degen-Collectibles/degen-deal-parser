from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, quote, quote_plus
from unittest.mock import patch

import httpx
import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

import app.tiktok.tiktok_auth_refresh as refresh_module
from app.models import TikTokAuth, TikTokCreatorAuth


HTTPX_CLIENT_TYPE = httpx.Client
APP_SECRET = "creator-app-secret-SENTINEL-2e64"
CREATOR_REFRESH = "creator-refresh-token-SENTINEL-4b72/a+b"
NEW_ACCESS = "creator-new-access-token-SENTINEL-1f52"
NEW_REFRESH = "creator-new-refresh-token-SENTINEL-8ad3"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _seed_auth(
    session: Session,
    *,
    app_key: str | None = "creator-client-key",
    creator_access_token: str | None = "creator-old-access",
    creator_refresh_token: str | None = CREATOR_REFRESH,
    creator_expires_at: datetime | None = None,
) -> TikTokAuth:
    row = TikTokAuth(
        tiktok_shop_id="shop-creator-refresh",
        app_key=app_key,
        access_token="seller-access",
        refresh_token="seller-refresh",
        access_token_expires_at=_utcnow() + timedelta(hours=1),
        creator_access_token=creator_access_token,
        creator_refresh_token=creator_refresh_token,
        creator_token_expires_at=creator_expires_at or (_utcnow() - timedelta(minutes=1)),
        created_at=_utcnow(),
        updated_at=_utcnow(),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _settings(
    *,
    app_key: str = "creator-client-key",
    app_secret: str = APP_SECRET,
    base_url: str = "https://open.tiktokapis.com",
):
    return SimpleNamespace(
        tiktok_app_key=app_key,
        tiktok_app_secret=app_secret,
        tiktok_api_base_url=base_url,
    )


def _target_frame_locals(error: BaseException) -> list[dict[str, object]]:
    frames: list[dict[str, object]] = []
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            filename = frame.f_code.co_filename.replace("\\", "/")
            if filename.endswith("/app/tiktok/tiktok_auth_refresh.py"):
                frames.append(dict(frame.f_locals))
            traceback = traceback.tb_next
        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
    return frames


def _serialized_exception_graph(error: BaseException) -> str:
    pieces: list[str] = []
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pieces.extend((str(current), repr(current), repr(current.args), repr(getattr(current, "__dict__", {}))))
        request = getattr(current, "request", None)
        response = getattr(current, "response", None)
        response_request = getattr(response, "request", None) if response is not None else None
        for candidate in (request, response_request):
            if isinstance(candidate, httpx.Request):
                pieces.extend(
                    (
                        str(candidate.url),
                        repr(dict(candidate.headers)),
                        repr(candidate.content),
                    )
                )
        if isinstance(response, httpx.Response):
            pieces.extend((repr(dict(response.headers)), repr(response.content)))

        for frame_locals in _target_frame_locals(current):
            pieces.append(repr(frame_locals))
            for value in frame_locals.values():
                if isinstance(value, TikTokAuth):
                    pieces.extend(
                        (
                            repr(value.access_token),
                            repr(value.refresh_token),
                            repr(value.creator_access_token),
                            repr(value.creator_refresh_token),
                        )
                    )
                elif isinstance(value, Session):
                    for instance in value.identity_map.values():
                        if isinstance(instance, TikTokAuth):
                            pieces.extend(
                                (
                                    repr(instance.access_token),
                                    repr(instance.refresh_token),
                                    repr(instance.creator_access_token),
                                    repr(instance.creator_refresh_token),
                                )
                            )
                elif isinstance(value, httpx.Request):
                    pieces.extend((str(value.url), repr(dict(value.headers)), repr(value.content)))
                elif isinstance(value, httpx.Response):
                    pieces.extend((repr(dict(value.headers)), repr(value.content)))
                elif isinstance(value, BaseException):
                    pending.append(value)

        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
    return "\n".join(pieces)


def _assert_detached_and_redacted(error: BaseException) -> None:
    serialized = _serialized_exception_graph(error)
    forbidden_values = {
        APP_SECRET,
        APP_SECRET[:20],
        CREATOR_REFRESH,
        CREATOR_REFRESH[:24],
        quote(CREATOR_REFRESH, safe=""),
        quote_plus(CREATOR_REFRESH),
        NEW_ACCESS,
        NEW_ACCESS[:24],
        NEW_REFRESH,
        NEW_REFRESH[:24],
    }
    for value in forbidden_values:
        assert value not in serialized

    forbidden_names = {
        "session",
        "auth_row",
        "app_secret",
        "creator_refresh",
        "form",
        "client",
        "response",
        "body",
        "access_token",
        "new_refresh",
        "args",
        "kwargs",
        "failure",
        "exc",
    }
    frames = _target_frame_locals(error)
    assert frames
    for frame_locals in frames:
        assert forbidden_names.isdisjoint(frame_locals)
        assert not any(
            isinstance(value, (Session, TikTokAuth, HTTPX_CLIENT_TYPE, httpx.Request, httpx.Response))
            for value in frame_locals.values()
        )
    assert error.__cause__ is None
    assert error.__context__ is None


def _capture_creator_failure(session: Session) -> BaseException:
    try:
        refresh_module.refresh_tiktok_creator_token_if_needed(
            session,
            runtime_name="creator-security-test",
        )
    except Exception as error:
        return error
    raise AssertionError("expected Creator token refresh failure")


def test_creator_refresh_uses_post_form_normalizes_nested_success_and_persists_tokens() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "access_token": NEW_ACCESS,
                    "refresh_token": NEW_REFRESH,
                    "expires_in": 3600,
                },
            },
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        row = _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=client,
        ):
            result = refresh_module.refresh_tiktok_creator_token_if_needed(
                session,
                runtime_name="creator-success-test",
            )
        session.expire_all()
        refreshed = session.get(TikTokAuth, row.id)

    assert result is not None
    assert result["status"] == "creator_refreshed"
    assert result["runtime"] == "creator-success-test"
    assert len(outgoing) == 1
    request = outgoing[0]
    assert request.method == "POST"
    assert request.url.host == "open.tiktokapis.com"
    assert request.url.path == "/v2/oauth/token/"
    assert not request.url.query
    assert request.headers["content-type"].startswith("application/x-www-form-urlencoded")
    assert parse_qs(request.content.decode("utf-8")) == {
        "client_key": ["creator-client-key"],
        "client_secret": [APP_SECRET],
        "grant_type": ["refresh_token"],
        "refresh_token": [CREATOR_REFRESH],
    }
    assert refreshed is not None
    assert refreshed.creator_access_token == NEW_ACCESS
    assert refreshed.creator_refresh_token == NEW_REFRESH
    engine.dispose()


def test_creator_refresh_ignores_hostile_base_url_override_for_credential_destination() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            200,
            json={"access_token": NEW_ACCESS, "expires_in": 3600},
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(
            refresh_module,
            "settings",
            _settings(base_url="https://credential-capture.invalid/custom-base"),
        ), patch.object(refresh_module.httpx, "Client", return_value=client):
            refresh_module.refresh_tiktok_creator_token_if_needed(session, runtime_name="test")

    assert len(outgoing) == 1
    request = outgoing[0]
    assert request.url.scheme == "https"
    assert request.url.host == "open.tiktokapis.com"
    assert request.url.path == "/v2/oauth/token/"
    assert not request.url.query
    engine.dispose()


def test_creator_refresh_rejects_cross_host_redirect_without_replaying_credentials() -> None:
    outgoing: list[httpx.Request] = []
    client_options: list[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        if request.url.host == "open.tiktokapis.com":
            return httpx.Response(
                307,
                headers={"location": "https://credential-capture.invalid/token"},
                request=request,
            )
        return httpx.Response(
            200,
            json={"access_token": NEW_ACCESS, "expires_in": 3600},
            request=request,
        )

    def client_factory(*, timeout: float, follow_redirects: bool):
        client_options.append(follow_redirects)
        return HTTPX_CLIENT_TYPE(
            timeout=timeout,
            follow_redirects=follow_redirects,
            transport=httpx.MockTransport(handler),
        )

    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            side_effect=client_factory,
        ):
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    assert client_options == [False]
    assert len(outgoing) == 1
    assert outgoing[0].url.host == "open.tiktokapis.com"
    assert outgoing[0].url.path == "/v2/oauth/token/"
    assert isinstance(raised, httpx.HTTPStatusError)
    assert raised.response.status_code == 307
    engine.dispose()


def test_creator_refresh_preserves_old_refresh_token_when_rotation_is_omitted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"access_token": NEW_ACCESS, "expires_in": 3600},
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        row = _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=client,
        ):
            refresh_module.refresh_tiktok_creator_token_if_needed(session, runtime_name="test")
        session.expire_all()
        refreshed = session.get(TikTokAuth, row.id)

    assert refreshed is not None
    assert refreshed.creator_access_token == NEW_ACCESS
    assert refreshed.creator_refresh_token == CREATOR_REFRESH
    engine.dispose()


def test_future_creator_token_skips_network_and_commit() -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session, creator_expires_at=_utcnow() + timedelta(hours=1))
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
        ) as client_factory, patch.object(session, "commit") as commit:
            result = refresh_module.refresh_tiktok_creator_token_if_needed(session, runtime_name="test")

    assert result is None
    client_factory.assert_not_called()
    commit.assert_not_called()
    engine.dispose()


@pytest.mark.parametrize("missing", ["client_key", "app_secret", "refresh_token"])
def test_missing_creator_credentials_skip_without_network(missing: str) -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(
            session,
            app_key=None if missing == "client_key" else "creator-client-key",
            creator_refresh_token=None if missing == "refresh_token" else CREATOR_REFRESH,
        )
        configured = _settings(
            app_key="" if missing == "client_key" else "creator-client-key",
            app_secret="" if missing == "app_secret" else APP_SECRET,
        )
        with patch.object(refresh_module, "settings", configured), patch.object(
            refresh_module.httpx,
            "Client",
        ) as client_factory:
            result = refresh_module.refresh_tiktok_creator_token_if_needed(session, runtime_name="test")

    assert result is None
    client_factory.assert_not_called()
    engine.dispose()


def test_creator_http_failure_is_detached_and_credential_free() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            headers={"x-secret": APP_SECRET},
            content=f"refresh_token={CREATOR_REFRESH}".encode(),
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=client,
        ):
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    assert isinstance(raised, httpx.HTTPStatusError)
    assert raised.response.status_code == 401
    assert raised.request.url.host == "open.tiktokapis.com"
    assert raised.request.url.path == "/v2/oauth/token/"
    assert not raised.request.url.query
    assert raised.request.content == b""
    engine.dispose()


def test_creator_transport_failure_is_detached_and_credential_free() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"connect failed client_secret={APP_SECRET} refresh_token={CREATOR_REFRESH}",
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=client,
        ):
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    assert isinstance(raised, httpx.TransportError)
    engine.dispose()


def test_creator_api_error_preserves_only_safe_numeric_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": "105001",
                "message": f"client_secret={APP_SECRET} refresh_token={CREATOR_REFRESH}",
            },
            request=request,
        )

    engine = _engine()
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=client,
        ):
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    assert getattr(raised, "tiktok_error_code", None) == "105001"
    assert "code 105001" in str(raised)
    engine.dispose()


def test_redaction_boundary_survives_sensitive_value_collection_failure() -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            side_effect=RuntimeError(f"client failed refresh_token={CREATOR_REFRESH}"),
        ), patch.object(
            refresh_module,
            "_refresh_sensitive_values",
            side_effect=RuntimeError(f"collector failed client_secret={APP_SECRET}"),
        ):
            raised = _capture_creator_failure(session)

    _assert_detached_and_redacted(raised)
    engine.dispose()


def test_sensitive_value_collection_failure_drops_numeric_credential_derived_error_code() -> None:
    numeric_credential = "105001"
    original_error = RuntimeError("refresh failed")
    original_error.tiktok_error_code = numeric_credential
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session, creator_refresh_token=numeric_credential)
        with patch.object(
            refresh_module,
            "settings",
            _settings(app_secret=numeric_credential),
        ), patch.object(
            refresh_module.httpx,
            "Client",
            side_effect=original_error,
        ), patch.object(
            refresh_module,
            "_refresh_sensitive_values",
            side_effect=RuntimeError("collector unavailable"),
        ):
            raised = _capture_creator_failure(session)

    _assert_detached_and_redacted(raised)
    assert numeric_credential not in _serialized_exception_graph(raised)
    assert getattr(raised, "tiktok_error_code", None) is None
    assert isinstance(raised, RuntimeError)
    engine.dispose()


@pytest.mark.parametrize("failure_stage", ["query", "client", "add", "commit"])
def test_creator_setup_and_database_failures_are_detached_and_credential_free(failure_stage: str) -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session)
        patches = [patch.object(refresh_module, "settings", _settings())]
        if failure_stage == "query":
            patches.append(
                patch.object(
                    session,
                    "exec",
                    side_effect=RuntimeError(f"query failed client_secret={APP_SECRET}"),
                )
            )
        elif failure_stage == "client":
            patches.append(
                patch.object(
                    refresh_module.httpx,
                    "Client",
                    side_effect=RuntimeError(f"client failed refresh_token={CREATOR_REFRESH}"),
                )
            )
        else:
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "access_token": NEW_ACCESS,
                        "refresh_token": NEW_REFRESH,
                        "expires_in": 3600,
                    },
                    request=request,
                )

            client = httpx.Client(transport=httpx.MockTransport(handler))
            patches.append(patch.object(refresh_module.httpx, "Client", return_value=client))
            patches.append(
                patch.object(
                    session,
                    failure_stage,
                    side_effect=RuntimeError(
                        f"{failure_stage} failed access_token={NEW_ACCESS} refresh_token={NEW_REFRESH}"
                    ),
                )
            )

        with ExitStack() as stack:
            for active_patch in patches:
                stack.enter_context(active_patch)
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    engine.dispose()


@pytest.mark.parametrize("failure_mode", ["json", "api_error", "malformed_success", "close"])
def test_creator_response_and_close_failures_are_detached_and_credential_free(failure_mode: str) -> None:
    if failure_mode == "json":
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=f"not-json {NEW_ACCESS} {NEW_REFRESH}".encode(),
                request=request,
            )

        owned_client: object = httpx.Client(transport=httpx.MockTransport(handler))
    elif failure_mode == "api_error":
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "error": "invalid_grant",
                    "error_description": f"client_secret={APP_SECRET} refresh_token={CREATOR_REFRESH}",
                },
                request=request,
            )

        owned_client = httpx.Client(transport=httpx.MockTransport(handler))
    elif failure_mode == "malformed_success":
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=(
                    '{"access_token":"'
                    + NEW_ACCESS
                    + '","refresh_token":"'
                    + NEW_REFRESH
                    + '","expires_in":1e400}'
                ).encode(),
                request=request,
            )

        owned_client = httpx.Client(transport=httpx.MockTransport(handler))
    else:
        class CloseFailingClient:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                raise RuntimeError(
                    f"close failed client_secret={APP_SECRET} new_access={NEW_ACCESS} new_refresh={NEW_REFRESH}"
                )

            def post(self, url, *, data, headers):
                request = httpx.Request("POST", url, data=data, headers=headers)
                return httpx.Response(
                    200,
                    json={
                        "access_token": NEW_ACCESS,
                        "refresh_token": NEW_REFRESH,
                        "expires_in": 3600,
                    },
                    request=request,
                )

        owned_client = CloseFailingClient()

    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            return_value=owned_client,
        ):
            raised = _capture_creator_failure(session)
            _assert_detached_and_redacted(raised)

    engine.dispose()


def test_seller_refresh_failure_is_detached_from_outer_refresh_frame() -> None:
    engine = _engine()

    def fail_refresh(*args, **kwargs):
        raise RuntimeError(f"seller failed app_secret={APP_SECRET} refresh_token={CREATOR_REFRESH}")

    seller_settings = SimpleNamespace(
        tiktok_app_key="creator-client-key",
        tiktok_app_secret=APP_SECRET,
        tiktok_refresh_token="",
        tiktok_redirect_uri="",
        tiktok_shop_id="",
    )
    with Session(engine) as session:
        _seed_auth(session)
        row = session.get(TikTokAuth, 1)
        assert row is not None
        row.access_token_expires_at = _utcnow() - timedelta(minutes=1)
        session.add(row)
        session.commit()
        with patch.object(refresh_module, "settings", seller_settings), patch.object(
            refresh_module,
            "_refresh_fn",
            fail_refresh,
        ):
            try:
                refresh_module.refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="seller-security-test",
                    resolve_base_url=lambda: "https://open-api.tiktokglobalshop.com",
                )
            except Exception as error:
                raised = error
            else:
                raise AssertionError("expected seller refresh failure")
            _assert_detached_and_redacted(raised)

    engine.dispose()


def test_shop_refresh_clients_disable_redirects_for_seller_and_standalone_creator() -> None:
    client_options: list[bool] = []

    class DummyClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    def client_factory(*, timeout: float, follow_redirects: bool):
        client_options.append(follow_redirects)
        return DummyClient()

    def fake_refresh(client, *, base_url, app_key, app_secret, refresh_token):
        return {
            "data": {
                "access_token": "new-shop-access",
                "refresh_token": "new-shop-refresh",
                "access_token_expire_in": 3600,
                "open_id": "creator-open-id",
            }
        }

    seller_settings = SimpleNamespace(
        tiktok_app_key="creator-client-key",
        tiktok_app_secret=APP_SECRET,
        tiktok_refresh_token="",
        tiktok_redirect_uri="",
        tiktok_shop_id="",
    )
    engine = _engine()
    with Session(engine) as session:
        seller = _seed_auth(session)
        seller.access_token_expires_at = _utcnow() - timedelta(minutes=1)
        session.add(seller)
        session.add(
            TikTokCreatorAuth(
                creator_username="redirect-test-creator",
                open_id="creator-open-id",
                app_key="creator-client-key",
                access_token="old-creator-access",
                refresh_token="old-creator-refresh",
                access_token_expires_at=_utcnow() - timedelta(minutes=1),
                created_at=_utcnow(),
                updated_at=_utcnow(),
            )
        )
        session.commit()
        with patch.object(refresh_module, "settings", seller_settings), patch.object(
            refresh_module,
            "_refresh_fn",
            fake_refresh,
        ), patch.object(
            refresh_module.httpx,
            "Client",
            side_effect=client_factory,
        ), patch.object(
            refresh_module,
            "upsert_tiktok_auth_from_callback",
            return_value=("updated", {"tiktok_shop_id": "shop-creator-refresh"}),
        ), patch.object(
            refresh_module,
            "upsert_tiktok_creator_auth_from_callback",
            return_value=("updated", {"creator_username": "redirect-test-creator"}),
        ):
            refresh_module.refresh_tiktok_auth_if_needed(
                session,
                runtime_name="test",
                resolve_base_url=lambda: "https://open-api.tiktokglobalshop.com",
            )

    assert client_options == [False, False]
    engine.dispose()


@pytest.mark.parametrize("base_error", [SystemExit("stop"), KeyboardInterrupt("stop")])
def test_creator_refresh_does_not_catch_base_exceptions(base_error: BaseException) -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_auth(session)
        with patch.object(refresh_module, "settings", _settings()), patch.object(
            refresh_module.httpx,
            "Client",
            side_effect=base_error,
        ):
            with pytest.raises(type(base_error)):
                refresh_module.refresh_tiktok_creator_token_if_needed(session, runtime_name="test")

    assert refresh_module._tiktok_auth_refresh_lock.acquire(blocking=False)
    refresh_module._tiktok_auth_refresh_lock.release()
    engine.dispose()


@pytest.mark.parametrize("base_error", [SystemExit("stop"), KeyboardInterrupt("stop")])
def test_shop_refresh_does_not_catch_base_exceptions(base_error: BaseException) -> None:
    engine = _engine()
    seller_settings = SimpleNamespace(
        tiktok_app_key="creator-client-key",
        tiktok_app_secret=APP_SECRET,
        tiktok_refresh_token="",
        tiktok_redirect_uri="",
        tiktok_shop_id="",
    )
    with Session(engine) as session:
        row = _seed_auth(session)
        row.access_token_expires_at = _utcnow() - timedelta(minutes=1)
        session.add(row)
        session.commit()
        with patch.object(refresh_module, "settings", seller_settings), patch.object(
            refresh_module,
            "_refresh_fn",
            side_effect=base_error,
        ):
            with pytest.raises(type(base_error)):
                refresh_module.refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://open-api.tiktokglobalshop.com",
                )

    assert refresh_module._tiktok_auth_refresh_lock.acquire(blocking=False)
    refresh_module._tiktok_auth_refresh_lock.release()
    engine.dispose()
