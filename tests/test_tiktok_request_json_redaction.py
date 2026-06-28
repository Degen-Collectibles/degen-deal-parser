from __future__ import annotations

import math
from collections.abc import Mapping
from unittest.mock import patch

import httpx
import pytest

from scripts import tiktok_backfill


TOKEN = "access-token-SENTINEL-5c84"
ENCODED_TOKEN = "encoded-token-SENTINEL/7a+2b"
INVALID_URL_TOKEN = "query-SENTINEL-77"
RAW_BODY_TOKEN = "raw-SENTINEL-88"
UNEXPECTED_QUERY_TOKEN = "unexpected-query-SENTINEL-31"
UNEXPECTED_HEADER_TOKEN = "unexpected-header-SENTINEL-42"
UNEXPECTED_BODY_TOKEN = "unexpected-body-SENTINEL-53"


def _serialized_exception_graph(exc: BaseException) -> str:
    pieces: list[str] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pieces.extend((str(current), repr(current)))
        request = getattr(current, "request", None)
        response = getattr(current, "response", None)
        response_request = getattr(response, "request", None) if response is not None else None
        for candidate in (request, response_request):
            if candidate is not None:
                pieces.extend(
                    (
                        str(candidate.url),
                        repr(dict(candidate.headers)),
                        repr(candidate.content),
                    )
                )
        if response is not None:
            pieces.extend((repr(dict(response.headers)), repr(response.content)))
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            if frame.f_code.co_filename.replace("\\", "/").endswith("/scripts/tiktok_backfill.py"):
                frame_locals = dict(frame.f_locals)
                pieces.append(repr(frame_locals))
                for value in frame_locals.values():
                    if isinstance(value, BaseException):
                        pending.append(value)
            traceback = traceback.tb_next
        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
    return "\n".join(pieces)


def _script_traceback_locals(exc: BaseException) -> list[Mapping[str, object]]:
    frames: list[Mapping[str, object]] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            if frame.f_code.co_filename.replace("\\", "/").endswith("/scripts/tiktok_backfill.py"):
                frame_locals = dict(frame.f_locals)
                frames.append(frame_locals)
                pending.extend(
                    value for value in frame_locals.values() if isinstance(value, BaseException)
                )
            traceback = traceback.tb_next
        pending.extend(
            linked for linked in (current.__cause__, current.__context__) if linked is not None
        )
    return frames


def _assert_detached_failure(
    exc: BaseException,
    *,
    forbidden_tokens: tuple[str, ...] = (TOKEN,),
) -> None:
    serialized = _serialized_exception_graph(exc)
    for forbidden_token in forbidden_tokens:
        assert forbidden_token not in serialized
    assert exc.__cause__ is None
    assert exc.__context__ is None
    frames = _script_traceback_locals(exc)
    assert frames
    forbidden_names = {
        "client",
        "url",
        "json_body",
        "form_body",
        "raw_body",
        "extra_headers",
        "sensitive_values",
        "known_secrets",
        "request_kwargs",
        "headers",
        "response",
        "payload",
        "exc",
    }
    for frame_locals in frames:
        assert forbidden_names.isdisjoint(frame_locals)
        assert not any(
            isinstance(value, (httpx.Request, httpx.Response, httpx.Client))
            for value in frame_locals.values()
        )


@pytest.mark.parametrize(
    ("url", "request_kwargs"),
    (
        (
            "https://open-api.tiktokglobalshop.com/order/202309/orders/search",
            {"extra_headers": {"x-tts-access-token": TOKEN}},
        ),
        (
            "https://open-api.tiktokglobalshop.com/order/202309/orders/search",
            {"extra_headers": {"Authorization": f"Bearer {TOKEN}"}},
        ),
        (
            f"https://open-api.tiktokglobalshop.com/order/202309/orders/search?access_token={TOKEN}",
            {},
        ),
        (
            "https://open-api.tiktokglobalshop.com/order/202309/orders/search",
            {"json_body": {"refresh_token": TOKEN}},
        ),
    ),
)
def test_http_failure_discovers_credentials_and_detaches_originals(
    url: str,
    request_kwargs: dict[str, object],
) -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            401,
            headers={"x-debug-token": TOKEN},
            json={"code": TOKEN, "message": TOKEN},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.request_json(
                client,
                method="POST",
                url=url,
                **request_kwargs,
            )

    assert len(outgoing) == 1
    _assert_detached_failure(caught.value)
    assert caught.value.response.status_code == 401
    assert caught.value.request.url.host == "open-api.tiktokglobalshop.com"
    assert caught.value.request.url.path == "/order/202309/orders/search"
    assert not caught.value.request.url.query
    assert caught.value.response.content == b""
    assert getattr(caught.value, "tiktok_error_code", None) is None


@pytest.mark.parametrize("failure_mode", ("transport", "decode", "unexpected"))
def test_non_http_failure_detaches_sensitive_request_state(failure_mode: str) -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        if failure_mode == "transport":
            raise httpx.ConnectError(f"connect failed {TOKEN}", request=request)
        if failure_mode == "decode":
            return httpx.Response(200, content=f"not-json {TOKEN}", request=request)
        return httpx.Response(200, json=[TOKEN], request=request)

    with patch.object(tiktok_backfill.time, "sleep", return_value=None), httpx.Client(
        transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises((httpx.TransportError, RuntimeError)) as caught:
            tiktok_backfill.request_json(
                client,
                method="GET",
                url="https://open-api.tiktokglobalshop.com/product/202309/products",
                extra_headers={"x-tts-access-token": TOKEN},
            )

    assert len(outgoing) == (3 if failure_mode == "transport" else 1)
    _assert_detached_failure(caught.value)
    assert "open-api.tiktokglobalshop.com/product/202309/products" in str(caught.value)


def test_request_construction_failure_detaches_sensitive_request_state() -> None:
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.request_json(
                client,
                method="POST",
                url=(
                    "https://open-api.tiktokglobalshop.com/product/202309/products"
                    f"?access_token={TOKEN}"
                ),
                json_body={"access_token": TOKEN, "unserializable": object()},
                extra_headers={"x-tts-access-token": TOKEN},
            )

    _assert_detached_failure(caught.value)
    assert "request construction error (TypeError)" in str(caught.value)
    assert "open-api.tiktokglobalshop.com/product/202309/products" in str(caught.value)


def test_invalid_url_sensitive_discovery_detaches_query_credential() -> None:
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.request_json(
                client,
                method="GET",
                url=f"https://example.test:bad/path?access_token={INVALID_URL_TOKEN}",
            )

    _assert_detached_failure(caught.value, forbidden_tokens=(INVALID_URL_TOKEN,))
    assert "request construction error" in str(caught.value)
    assert "access_token" not in str(caught.value)


def test_raw_body_encoding_failure_detaches_body_credential() -> None:
    raw_body = f'{{"access_token":"{RAW_BODY_TOKEN}\ud800"}}'
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.request_json(
                client,
                method="POST",
                url="https://example.test/path",
                raw_body=raw_body,
            )

    _assert_detached_failure(caught.value, forbidden_tokens=(RAW_BODY_TOKEN,))
    assert "request construction error (UnicodeEncodeError)" in str(caught.value)
    assert "example.test/path" in str(caught.value)


def test_unexpected_client_exception_detaches_all_request_credentials() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        raise RuntimeError(
            f"hook failed {UNEXPECTED_QUERY_TOKEN} "
            f"{UNEXPECTED_HEADER_TOKEN} {UNEXPECTED_BODY_TOKEN}"
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.request_json(
                client,
                method="POST",
                url=(
                    "https://example.test/path"
                    f"?access_token={UNEXPECTED_QUERY_TOKEN}"
                ),
                raw_body=f'{{"client_secret":"{UNEXPECTED_BODY_TOKEN}"}}',
                extra_headers={"x-tts-access-token": UNEXPECTED_HEADER_TOKEN},
            )

    assert len(outgoing) == 1
    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            UNEXPECTED_QUERY_TOKEN,
            UNEXPECTED_HEADER_TOKEN,
            UNEXPECTED_BODY_TOKEN,
        ),
    )
    assert "TikTok request error (RuntimeError)" in str(caught.value)
    assert "example.test/path" in str(caught.value)


def test_non_finite_retry_after_uses_bounded_delay_and_detaches_credentials() -> None:
    attempts: list[httpx.Request] = []
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request)
        return httpx.Response(
            503,
            headers={"Retry-After": "inf", "x-debug-token": UNEXPECTED_HEADER_TOKEN},
            json={"code": 105001, "message": UNEXPECTED_BODY_TOKEN},
            request=request,
        )

    def guarded_sleep(seconds: float) -> None:
        if not math.isfinite(seconds):
            raise OverflowError(
                f"sleep overflow {UNEXPECTED_QUERY_TOKEN} "
                f"{UNEXPECTED_HEADER_TOKEN} {UNEXPECTED_BODY_TOKEN}"
            )
        assert 0 <= seconds <= 60
        sleeps.append(seconds)

    with patch.object(tiktok_backfill.time, "sleep", side_effect=guarded_sleep), httpx.Client(
        transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.request_json(
                client,
                method="POST",
                url=(
                    "https://example.test/path"
                    f"?access_token={UNEXPECTED_QUERY_TOKEN}"
                ),
                raw_body=f'{{"client_secret":"{UNEXPECTED_BODY_TOKEN}"}}',
                extra_headers={"Authorization": f"Bearer {UNEXPECTED_HEADER_TOKEN}"},
            )

    assert len(attempts) == 3
    assert sleeps == [0.5, 1.0]
    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            UNEXPECTED_QUERY_TOKEN,
            UNEXPECTED_HEADER_TOKEN,
            UNEXPECTED_BODY_TOKEN,
        ),
    )
    assert caught.value.response.status_code == 503


def test_tiktok_api_error_payload_omits_echoed_credentials_by_default() -> None:
    with pytest.raises(RuntimeError) as caught:
        tiktok_backfill.raise_for_tiktok_error(
            {"code": 105001, "message": f"access token rejected: {TOKEN}"},
            path="/order/202309/orders/search",
        )

    _assert_detached_failure(caught.value)
    assert getattr(caught.value, "tiktok_error_code", None) == "105001"
    assert "/order/202309/orders/search" in str(caught.value)


def test_http_failure_strips_percent_encoded_credential_query_from_diagnostics() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"code": 105001}, request=request)

    encoded_url = str(
        httpx.URL(
            "https://open-api.tiktokglobalshop.com/order/202309/orders/search",
            params={"api_token": ENCODED_TOKEN, "page_size": "50"},
        )
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.request_json(client, method="GET", url=encoded_url)

    serialized = _serialized_exception_graph(caught.value)
    assert ENCODED_TOKEN not in serialized
    assert "encoded-token-SENTINEL%2F7a%2B2b" not in serialized
    assert "api_token" not in str(caught.value)
    assert "?" not in str(caught.value)
    assert not caught.value.request.url.query


@pytest.mark.parametrize(
    "message",
    (
        "Access denied for this token",
        "Required access scope is missing",
        "Not authorized to access the endpoint",
    ),
)
def test_tiktok_api_error_preserves_safe_scope_missing_classification(message: str) -> None:
    with pytest.raises(RuntimeError) as caught:
        tiktok_backfill.raise_for_tiktok_error(
            {"code": 100001, "message": f"{message}: {TOKEN}"},
            path="/affiliate_seller/202410/orders/search",
        )

    _assert_detached_failure(caught.value)
    assert getattr(caught.value, "tiktok_scope_missing", False) is True
    assert tiktok_backfill.tiktok_affiliate_order_error_is_scope_missing(caught.value)
