from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, quote
from unittest.mock import patch

import httpx

import app.routers.shopify as shopify_module
import app.tiktok.tiktok_ingest as tiktok_ingest


AUTH_CODE = "auth-code-SENTINEL-7c91"
APP_SECRET = "app-secret-SENTINEL-2e64"
ACCESS_TOKEN = "access-token-SENTINEL-1f52"
REFRESH_TOKEN = "refresh-token-SENTINEL-8ad3"
REFRESH_INPUT_TOKEN = "refresh-input-token-SENTINEL-4b72"


def _serialized_exception_graph(exc: BaseException) -> str:
    pieces: list[str] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        request = getattr(current, "request", None)
        response = getattr(current, "response", None)
        response_request = getattr(response, "request", None) if response is not None else None
        pieces.extend((str(current), repr(current)))
        for candidate in (request, response_request):
            if candidate is None:
                continue
            pieces.extend(
                (
                    str(candidate.url),
                    repr(candidate.url),
                    repr(dict(candidate.headers)),
                    repr(candidate.content),
                )
            )
        if response is not None:
            pieces.extend((repr(dict(response.headers)), repr(response.content)))
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            normalized_filename = frame.f_code.co_filename.replace("\\", "/")
            if normalized_filename.endswith("/app/tiktok/tiktok_ingest.py"):
                frame_locals = dict(frame.f_locals)
                pieces.append(repr(frame_locals))
                for local_value in frame_locals.values():
                    if isinstance(local_value, httpx.Request):
                        pieces.extend(
                            (
                                str(local_value.url),
                                repr(dict(local_value.headers)),
                                repr(local_value.content),
                            )
                        )
                    elif isinstance(local_value, httpx.Response):
                        pieces.extend(
                            (
                                repr(dict(local_value.headers)),
                                repr(local_value.content),
                                str(local_value.request.url),
                                repr(dict(local_value.request.headers)),
                                repr(local_value.request.content),
                            )
                        )
                    elif isinstance(local_value, BaseException):
                        pending.append(local_value)
            traceback = traceback.tb_next
        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
    return "\n".join(pieces)


def _tiktok_ingest_traceback_locals(exc: BaseException) -> list[dict[str, object]]:
    frames: list[dict[str, object]] = []
    traceback = exc.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        normalized_filename = frame.f_code.co_filename.replace("\\", "/")
        if normalized_filename.endswith("/app/tiktok/tiktok_ingest.py"):
            frames.append(dict(frame.f_locals))
        traceback = traceback.tb_next
    return frames


def _assert_oauth_secrets_absent(text: str) -> None:
    assert AUTH_CODE not in text
    assert AUTH_CODE[:8] not in text
    assert APP_SECRET not in text


def _assert_token_canaries_absent(text: str) -> None:
    assert ACCESS_TOKEN not in text
    assert REFRESH_TOKEN not in text


def _assert_refresh_secrets_absent(text: str) -> None:
    assert APP_SECRET not in text
    assert REFRESH_INPUT_TOKEN not in text


def _assert_sensitive_exchange_locals_absent(
    exc: BaseException,
    *,
    forbidden_names: set[str],
) -> None:
    frames = _tiktok_ingest_traceback_locals(exc)
    assert frames
    for frame_locals in frames:
        assert forbidden_names.isdisjoint(frame_locals)
        assert not any(
            isinstance(value, (httpx.Request, httpx.Response, httpx.Client))
            for value in frame_locals.values()
        )


def test_http_exchange_failure_does_not_retain_oauth_secrets() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            401,
            headers={"x-debug-secret": APP_SECRET},
            json={"error": f"rejected auth_code={AUTH_CODE} app_secret={APP_SECRET}"},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.exchange_tiktok_authorization_code(
                auth_code=AUTH_CODE,
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the handler always returns HTTP 401
            raise AssertionError("expected TikTokIngestError")

    assert len(outgoing) == 1
    assert outgoing[0].url.params["auth_code"] == AUTH_CODE
    assert outgoing[0].url.params["app_secret"] == APP_SECRET
    serialized = _serialized_exception_graph(raised)
    _assert_oauth_secrets_absent(serialized)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "auth_code",
            "app_secret",
            "query_params",
            "client",
            "http_client",
            "response",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "HTTP 401" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/get" in str(raised)


def test_transport_exchange_failure_does_not_retain_oauth_secrets() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        raise httpx.ConnectError(
            f"connect failed auth_code={AUTH_CODE} app_secret={APP_SECRET}",
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.exchange_tiktok_authorization_code(
                auth_code=AUTH_CODE,
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the handler always raises
            raise AssertionError("expected TikTokIngestError")

    assert len(outgoing) == 1
    serialized = _serialized_exception_graph(raised)
    _assert_oauth_secrets_absent(serialized)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "auth_code",
            "app_secret",
            "query_params",
            "client",
            "http_client",
            "response",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "ConnectError" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/get" in str(raised)


def test_shop_malformed_success_payload_does_not_retain_token_canaries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=(
                '{"code":0,"data":{"access_token":"'
                + ACCESS_TOKEN
                + '","refresh_token":"'
                + REFRESH_TOKEN
                + '","access_token_expire_in":1e400}}'
            ),
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.exchange_tiktok_authorization_code(
                auth_code=AUTH_CODE,
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                client=client,
            )
        except Exception as exc:
            raised = exc
        else:  # pragma: no cover - malformed expiry always fails
            raise AssertionError("expected token parsing failure")

    serialized = _serialized_exception_graph(raised)
    _assert_token_canaries_absent(serialized)
    _assert_oauth_secrets_absent(serialized)
    assert isinstance(raised, tiktok_ingest.TikTokIngestError)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "api_data",
            "auth_code",
            "app_secret",
            "query_params",
            "client",
            "http_client",
            "response",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "OverflowError" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/get" in str(raised)


def test_shop_exchange_validation_failure_does_not_retain_supplied_credentials() -> None:
    try:
        tiktok_ingest.exchange_tiktok_authorization_code(
            auth_code=AUTH_CODE,
            app_key="",
            app_secret=APP_SECRET,
        )
    except tiktok_ingest.TikTokIngestError as exc:
        raised = exc
    else:  # pragma: no cover - the missing app key always fails validation
        raise AssertionError("expected TikTokIngestError")

    _assert_oauth_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={"auth_code", "app_key", "app_secret", "client"},
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "app key is required" in str(raised)


def test_creator_exchange_validation_failure_does_not_retain_supplied_credentials() -> None:
    try:
        tiktok_ingest.exchange_tiktok_creator_authorization_code(
            auth_code=AUTH_CODE,
            client_key="",
            client_secret=APP_SECRET,
            redirect_uri="https://example.test/integrations/tiktok/creator-callback",
        )
    except tiktok_ingest.TikTokIngestError as exc:
        raised = exc
    else:  # pragma: no cover - the missing client key always fails validation
        raise AssertionError("expected TikTokIngestError")

    _assert_oauth_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={"auth_code", "client_key", "client_secret", "client"},
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "client key is required" in str(raised)


def test_shop_exchange_owned_client_close_failure_clears_token_success() -> None:
    class CloseFailingShopClient:
        def get(self, url: str, params: dict[str, str]) -> httpx.Response:
            request = httpx.Request("GET", url, params=params)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "access_token": ACCESS_TOKEN,
                        "refresh_token": REFRESH_TOKEN,
                        "access_token_expire_in": 3600,
                    },
                },
                request=request,
            )

        def close(self) -> None:
            raise RuntimeError(f"close failed auth_code={AUTH_CODE} app_secret={APP_SECRET}")

    with patch.object(tiktok_ingest.httpx, "Client", return_value=CloseFailingShopClient()):
        try:
            tiktok_ingest.exchange_tiktok_authorization_code(
                auth_code=AUTH_CODE,
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - owned client close always fails
            raise AssertionError("expected TikTokIngestError")

    serialized = _serialized_exception_graph(raised)
    _assert_oauth_secrets_absent(serialized)
    _assert_token_canaries_absent(serialized)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "auth_code",
            "app_secret",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "token_result",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "RuntimeError" in str(raised)


def test_creator_exchange_close_failure_does_not_replace_sanitized_http_error() -> None:
    class FailingCreatorClient:
        def post(
            self,
            url: str,
            data: dict[str, str],
            headers: dict[str, str],
        ) -> httpx.Response:
            request = httpx.Request("POST", url, data=data, headers=headers)
            return httpx.Response(
                401,
                json={"error": f"code={AUTH_CODE} client_secret={APP_SECRET}"},
                request=request,
            )

        def close(self) -> None:
            raise RuntimeError(f"close failed code={AUTH_CODE} client_secret={APP_SECRET}")

    with patch.object(tiktok_ingest.httpx, "Client", return_value=FailingCreatorClient()):
        try:
            tiktok_ingest.exchange_tiktok_creator_authorization_code(
                auth_code=AUTH_CODE,
                client_key="synthetic-client-key",
                client_secret=APP_SECRET,
                redirect_uri="https://example.test/integrations/tiktok/creator-callback",
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the response always fails
            raise AssertionError("expected TikTokIngestError")

    _assert_oauth_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "auth_code",
            "client_secret",
            "form_data",
            "client",
            "http_client",
            "response",
            "api_data",
            "token_result",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "HTTP 401" in str(raised)


def test_shop_refresh_http_failure_does_not_retain_credentials() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            401,
            headers={"x-debug-secret": REFRESH_INPUT_TOKEN},
            json={"error": f"app_secret={APP_SECRET} refresh_token={REFRESH_INPUT_TOKEN}"},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN,
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the handler always returns HTTP 401
            raise AssertionError("expected TikTokIngestError")

    assert len(outgoing) == 1
    assert outgoing[0].url.params["app_secret"] == APP_SECRET
    assert outgoing[0].url.params["refresh_token"] == REFRESH_INPUT_TOKEN
    _assert_refresh_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "HTTP 401" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/refresh" in str(raised)


def test_shop_refresh_transport_failure_does_not_retain_credentials() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        raise httpx.ConnectError(
            f"connect failed app_secret={APP_SECRET} refresh_token={REFRESH_INPUT_TOKEN}",
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN,
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the handler always raises
            raise AssertionError("expected TikTokIngestError")

    assert len(outgoing) == 1
    _assert_refresh_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "ConnectError" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/refresh" in str(raised)


def test_shop_refresh_malformed_success_does_not_retain_credentials() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=(
                '{"code":0,"data":{"access_token":"'
                + ACCESS_TOKEN
                + '","refresh_token":"'
                + REFRESH_INPUT_TOKEN
                + '","access_token_expire_in":1e400}}'
            ),
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN,
                client=client,
            )
        except Exception as exc:
            raised = exc
        else:  # pragma: no cover - malformed expiry always fails
            raise AssertionError("expected token parsing failure")

    serialized = _serialized_exception_graph(raised)
    _assert_refresh_secrets_absent(serialized)
    assert ACCESS_TOKEN not in serialized
    assert isinstance(raised, tiktok_ingest.TikTokIngestError)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "OverflowError" in str(raised)
    assert "auth.tiktok-shops.com/api/v2/token/refresh" in str(raised)


def test_shop_refresh_validation_failure_does_not_retain_supplied_credentials() -> None:
    try:
        tiktok_ingest.refresh_tiktok_shop_token(
            app_key="",
            app_secret=APP_SECRET,
            refresh_token=REFRESH_INPUT_TOKEN,
        )
    except tiktok_ingest.TikTokIngestError as exc:
        raised = exc
    else:  # pragma: no cover - the missing app key always fails validation
        raise AssertionError("expected TikTokIngestError")

    _assert_refresh_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={"app_key", "app_secret", "refresh_token", "client"},
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "app key is required" in str(raised)


def test_shop_refresh_client_setup_failure_does_not_retain_credentials() -> None:
    setup_error = httpx.ConnectError(
        f"setup failed app_secret={APP_SECRET} refresh_token={REFRESH_INPUT_TOKEN}"
    )
    with patch.object(tiktok_ingest.httpx, "Client", side_effect=setup_error):
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - client construction always raises
            raise AssertionError("expected TikTokIngestError")

    _assert_refresh_secrets_absent(_serialized_exception_graph(raised))
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "ConnectError" in str(raised)


def test_shop_refresh_owned_client_close_failure_does_not_retain_credentials() -> None:
    class CloseFailingClient:
        def get(self, url: str, params: dict[str, str]) -> httpx.Response:
            request = httpx.Request("GET", url, params=params)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "access_token": ACCESS_TOKEN,
                        "refresh_token": REFRESH_INPUT_TOKEN,
                        "access_token_expire_in": 3600,
                    },
                },
                request=request,
            )

        def close(self) -> None:
            raise RuntimeError(
                f"close failed app_secret={APP_SECRET} refresh_token={REFRESH_INPUT_TOKEN}"
            )

    with patch.object(tiktok_ingest.httpx, "Client", return_value=CloseFailingClient()):
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - owned client close always raises
            raise AssertionError("expected TikTokIngestError")

    serialized = _serialized_exception_graph(raised)
    _assert_refresh_secrets_absent(serialized)
    assert ACCESS_TOKEN not in serialized
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "RuntimeError" in str(raised)


def test_shop_refresh_api_error_does_not_retain_percent_encoded_credentials() -> None:
    encoded_refresh_token = quote(REFRESH_INPUT_TOKEN + "/a+b", safe="")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": 105001,
                "message": f"refresh_token={encoded_refresh_token}",
            },
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.refresh_tiktok_shop_token(
                app_key="synthetic-app-key",
                app_secret=APP_SECRET,
                refresh_token=REFRESH_INPUT_TOKEN + "/a+b",
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the API error payload always fails
            raise AssertionError("expected TikTokIngestError")

    serialized = _serialized_exception_graph(raised)
    assert REFRESH_INPUT_TOKEN not in serialized
    assert encoded_refresh_token not in serialized
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "app_secret",
            "refresh_token",
            "query_params",
            "client",
            "http_client",
            "response",
            "api_data",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None


def test_creator_authorization_exchange_uses_post_form_without_query_credentials() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            200,
            json={
                "access_token": "synthetic-creator-access-token",
                "refresh_token": "synthetic-creator-refresh-token",
                "expires_in": 3600,
                "open_id": "synthetic-creator-open-id",
            },
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = tiktok_ingest.exchange_tiktok_creator_authorization_code(
            auth_code=AUTH_CODE,
            client_key="synthetic-client-key",
            client_secret=APP_SECRET,
            redirect_uri="https://example.test/integrations/tiktok/creator-callback",
            client=client,
        )

    assert len(outgoing) == 1
    request = outgoing[0]
    assert request.method == "POST"
    assert request.url.host == "open.tiktokapis.com"
    assert request.url.path == "/v2/oauth/token/"
    assert not request.url.query
    assert request.headers["content-type"].startswith("application/x-www-form-urlencoded")
    assert parse_qs(request.content.decode("utf-8")) == {
        "client_key": ["synthetic-client-key"],
        "client_secret": [APP_SECRET],
        "code": [AUTH_CODE],
        "grant_type": ["authorization_code"],
        "redirect_uri": ["https://example.test/integrations/tiktok/creator-callback"],
    }
    assert result.access_token == "synthetic-creator-access-token"
    assert result.refresh_token == "synthetic-creator-refresh-token"
    assert result.open_id == "synthetic-creator-open-id"


def test_creator_http_exchange_failure_does_not_retain_oauth_secrets() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            401,
            headers={"x-debug-secret": APP_SECRET},
            json={"error": f"rejected code={AUTH_CODE} client_secret={APP_SECRET}"},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.exchange_tiktok_creator_authorization_code(
                auth_code=AUTH_CODE,
                client_key="synthetic-client-key",
                client_secret=APP_SECRET,
                redirect_uri="https://example.test/integrations/tiktok/creator-callback",
                client=client,
            )
        except tiktok_ingest.TikTokIngestError as exc:
            raised = exc
        else:  # pragma: no cover - the handler always returns HTTP 401
            raise AssertionError("expected TikTokIngestError")

    assert len(outgoing) == 1
    serialized = _serialized_exception_graph(raised)
    _assert_oauth_secrets_absent(serialized)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "auth_code",
            "client_secret",
            "form_data",
            "client",
            "http_client",
            "response",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "HTTP 401" in str(raised)
    assert "open.tiktokapis.com/v2/oauth/token/" in str(raised)


def test_creator_malformed_success_payload_does_not_retain_token_canaries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=(
                '{"access_token":"'
                + ACCESS_TOKEN
                + '","refresh_token":"'
                + REFRESH_TOKEN
                + '","expires_in":1e400}'
            ),
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        try:
            tiktok_ingest.exchange_tiktok_creator_authorization_code(
                auth_code=AUTH_CODE,
                client_key="synthetic-client-key",
                client_secret=APP_SECRET,
                redirect_uri="https://example.test/integrations/tiktok/creator-callback",
                client=client,
            )
        except Exception as exc:
            raised = exc
        else:  # pragma: no cover - malformed expiry always fails
            raise AssertionError("expected token parsing failure")

    serialized = _serialized_exception_graph(raised)
    _assert_token_canaries_absent(serialized)
    _assert_oauth_secrets_absent(serialized)
    assert isinstance(raised, tiktok_ingest.TikTokIngestError)
    _assert_sensitive_exchange_locals_absent(
        raised,
        forbidden_names={
            "api_data",
            "auth_code",
            "client_secret",
            "form_data",
            "client",
            "http_client",
            "response",
            "exc",
        },
    )
    assert raised.__cause__ is None
    assert raised.__context__ is None
    assert "OverflowError" in str(raised)
    assert "open.tiktokapis.com/v2/oauth/token/" in str(raised)


class _FakeRequest:
    def __init__(self, path: str, *, oauth_kind: str = "") -> None:
        self.query_params = {
            "app_key": "synthetic-app-key",
            "code": AUTH_CODE,
            "state": "synthetic-oauth-state",
        }
        self.session: dict[str, object] = {"oauth_state": "synthetic-oauth-state"}
        if oauth_kind:
            self.session["tiktok_oauth_kind"] = oauth_kind
            self.session["tiktok_oauth_creator_username"] = "synthetic-creator"
        self.url = SimpleNamespace(path=path)


class _FailingTokenClient:
    def get(self, url: str, params: dict[str, str] | None = None) -> httpx.Response:
        request = httpx.Request("GET", url, params=params)
        raise httpx.ConnectError(
            f"connect failed auth_code={AUTH_CODE} app_secret={APP_SECRET}",
            request=request,
        )

    def post(
        self,
        url: str,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        request = httpx.Request("POST", url, data=data, headers=headers)
        raise httpx.ConnectError(
            f"connect failed code={AUTH_CODE} client_secret={APP_SECRET}",
            request=request,
        )

    def close(self) -> None:
        return None


def _capture_log_fields(captured: list[dict[str, object]]):
    def capture(**fields: object) -> str:
        captured.append(fields)
        return "{}"

    return capture


def test_seller_and_shop_creator_callback_failures_do_not_expose_oauth_secrets() -> None:
    for oauth_kind in ("", "shop_creator"):
        request = _FakeRequest("/integrations/tiktok/callback", oauth_kind=oauth_kind)
        captured_logs: list[dict[str, object]] = []
        with patch.object(
            shopify_module.settings, "tiktok_app_key", "synthetic-app-key"
        ), patch.object(
            shopify_module.settings, "tiktok_app_secret", APP_SECRET
        ), patch.object(
            tiktok_ingest.httpx, "Client", return_value=_FailingTokenClient()
        ), patch.object(
            shopify_module, "update_tiktok_integration_state"
        ) as update_state, patch.object(
            shopify_module,
            "structured_log_line",
            side_effect=_capture_log_fields(captured_logs),
        ):
            response = shopify_module.tiktok_oauth_callback(request)  # type: ignore[arg-type]

        assert response.status_code == 303
        assert response.headers["location"] == "/status?error=TikTok+authorization+exchange+failed"
        _assert_oauth_secrets_absent(repr(request.session))
        _assert_oauth_secrets_absent(repr(update_state.call_args))
        _assert_oauth_secrets_absent(repr(captured_logs))
        assert request.session["tiktok_callback"]["query"]["code"] == "present"


def test_creator_callback_failure_does_not_expose_oauth_secrets_to_logs() -> None:
    request = _FakeRequest("/integrations/tiktok/creator-callback")
    captured_logs: list[dict[str, object]] = []
    with patch.object(
        shopify_module.settings, "tiktok_app_key", "synthetic-app-key"
    ), patch.object(
        shopify_module.settings, "tiktok_app_secret", APP_SECRET
    ), patch.object(
        tiktok_ingest.httpx, "Client", return_value=_FailingTokenClient()
    ), patch.object(
        shopify_module,
        "structured_log_line",
        side_effect=_capture_log_fields(captured_logs),
    ):
        response = shopify_module.tiktok_creator_oauth_callback(request)  # type: ignore[arg-type]

    assert response.status_code == 303
    assert response.headers["location"] == "/status?error=Creator+token+exchange+failed"
    _assert_oauth_secrets_absent(repr(request.session))
    _assert_oauth_secrets_absent(repr(captured_logs))


def test_creator_callback_uses_creator_post_exchange() -> None:
    request = _FakeRequest("/integrations/tiktok/creator-callback")
    token_result = SimpleNamespace(
        access_token="synthetic-creator-access-token",
        refresh_token="synthetic-creator-refresh-token",
        access_token_expires_at=None,
    )
    with patch.object(
        shopify_module.settings, "tiktok_app_key", "synthetic-client-key"
    ), patch.object(
        shopify_module.settings, "tiktok_app_secret", APP_SECRET
    ), patch.object(
        shopify_module.settings,
        "tiktok_redirect_uri",
        "https://example.test/integrations/tiktok/callback",
    ), patch.object(
        shopify_module,
        "exchange_tiktok_creator_authorization_code",
        return_value=token_result,
    ) as creator_exchange, patch.object(
        shopify_module,
        "exchange_tiktok_authorization_code",
        side_effect=AssertionError("Creator callback must not use the Shop exchange"),
    ), patch.object(
        shopify_module, "run_write_with_retry", return_value=True
    ):
        response = shopify_module.tiktok_creator_oauth_callback(request)  # type: ignore[arg-type]

    assert response.status_code == 303
    assert response.headers["location"].startswith("/status?success=")
    creator_exchange.assert_called_once_with(
        auth_code=AUTH_CODE,
        client_key="synthetic-client-key",
        client_secret=APP_SECRET,
        redirect_uri="https://example.test/integrations/tiktok/creator-callback",
        runtime_name=f"{shopify_module.settings.runtime_name}_tiktok_creator",
    )
