from __future__ import annotations

import math
from collections.abc import Mapping
from types import SimpleNamespace
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
WRAPPER_APP_SECRET = "wrapper-app-secret-SENTINEL-64"
WRAPPER_ACCESS_TOKEN = "wrapper-access-token-SENTINEL-75"
WRAPPER_CREATOR_ACCESS_TOKEN = "wrapper-creator-access-token-SENTINEL-46"
WRAPPER_AUTH_CODE = "wrapper-auth-code-SENTINEL-86"
WRAPPER_REFRESH_TOKEN = "wrapper-refresh-token-SENTINEL-97"
WRAPPER_SHOP_CIPHER = "wrapper-shop-cipher-SENTINEL-08"
WRAPPER_QUERY_VALUE = "wrapper-query-SENTINEL-19"
WRAPPER_BODY_VALUE = "wrapper-body-SENTINEL-20"
WRAPPER_PAYLOAD_VALUE = "wrapper-payload-SENTINEL-31"
WRAPPER_BASE_URL = "https://open-api.tiktokglobalshop.com"


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
        "args",
        "kwargs",
        "caught",
        "failure",
        "original_error",
        "record",
        "session",
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


_REQUEST_JSON_WRAPPER_CASES = (
    (
        "exchange_authorized_code",
        {
            "base_url": "https://unused.example",
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "auth_code": WRAPPER_AUTH_CODE,
        },
    ),
    (
        "refresh_access_token",
        {
            "base_url": "https://unused.example",
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "refresh_token": WRAPPER_REFRESH_TOKEN,
        },
    ),
    (
        "fetch_tiktok_order_list_page",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "since": None,
            "until": None,
            "page_size": 1,
            "cursor": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_order_details",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "order_ids": [WRAPPER_QUERY_VALUE],
        },
    ),
    (
        "fetch_tiktok_seller_affiliate_orders_page",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "since": None,
            "until": None,
            "cursor": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_creator_affiliate_orders_page",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "since": None,
            "until": None,
            "cursor": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_product_list_page",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "page_size": 1,
            "cursor": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_categories",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "keyword": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_category_attributes",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "category_id": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "fetch_tiktok_brands",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "brand_name": WRAPPER_QUERY_VALUE,
        },
    ),
    (
        "create_tiktok_product",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "product_body": {"title": WRAPPER_BODY_VALUE},
        },
    ),
    (
        "edit_tiktok_product",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "product_id": WRAPPER_QUERY_VALUE,
            "product_body": {"title": WRAPPER_BODY_VALUE},
        },
    ),
    (
        "fetch_tiktok_product_detail",
        {
            "base_url": WRAPPER_BASE_URL,
            "app_key": "synthetic-app-key",
            "app_secret": WRAPPER_APP_SECRET,
            "access_token": WRAPPER_ACCESS_TOKEN,
            "shop_id": "synthetic-shop",
            "shop_cipher": WRAPPER_SHOP_CIPHER,
            "product_id": WRAPPER_QUERY_VALUE,
        },
    ),
)


_ANALYTICS_WRAPPER_CASES = (
    (
        "fetch_live_session_list",
        {
            "start_date": "2026-06-01",
            "end_date": WRAPPER_QUERY_VALUE,
        },
        {
            "code": 0,
            "data": {
                "live_stream_sessions": [
                    {
                        "id": "synthetic-live",
                        "sales_performance": {"items_sold": WRAPPER_PAYLOAD_VALUE},
                    }
                ]
            },
        },
    ),
    (
        "fetch_overview_performance_daily",
        {
            "start_date": "2026-06-01",
            "end_date": WRAPPER_QUERY_VALUE,
        },
        {
            "code": 0,
            "data": {
                "performance": {
                    "intervals": [
                        {
                            "start_date": "2026-06-01",
                            "sku_orders": WRAPPER_PAYLOAD_VALUE,
                        }
                    ]
                }
            },
        },
    ),
    (
        "fetch_stream_performance_per_minutes",
        {"live_id": WRAPPER_QUERY_VALUE},
        {
            "code": 0,
            "data": {
                "overall": {"start_time": WRAPPER_PAYLOAD_VALUE},
                "intervals": [],
            },
        },
    ),
    (
        "fetch_live_product_performance_list",
        {"live_id": WRAPPER_QUERY_VALUE},
        {
            "code": 0,
            "data": {
                "products": [
                    {
                        "id": "synthetic-product",
                        "sales": {"items_sold": WRAPPER_PAYLOAD_VALUE},
                    }
                ]
            },
        },
    ),
)


@pytest.mark.parametrize(("wrapper_name", "wrapper_kwargs"), _REQUEST_JSON_WRAPPER_CASES)
def test_request_json_wrapper_failure_detaches_all_wrapper_frames(
    wrapper_name: str,
    wrapper_kwargs: dict[str, object],
) -> None:
    def fail_request(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError(f"request failed {WRAPPER_PAYLOAD_VALUE}")

    wrapper = getattr(tiktok_backfill, wrapper_name)
    with patch.object(tiktok_backfill, "request_json", side_effect=fail_request), httpx.Client() as client:
        with pytest.raises(RuntimeError) as caught:
            wrapper(client, **wrapper_kwargs)

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_AUTH_CODE,
            WRAPPER_REFRESH_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_QUERY_VALUE,
            WRAPPER_BODY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert type(caught.value) is RuntimeError


@pytest.mark.parametrize(
    ("wrapper_name", "wrapper_kwargs", "malformed_payload"),
    _ANALYTICS_WRAPPER_CASES,
)
def test_analytics_wrapper_build_failure_detaches_all_wrapper_frames(
    wrapper_name: str,
    wrapper_kwargs: dict[str, object],
    malformed_payload: dict[str, object],
) -> None:
    wrapper = getattr(tiktok_backfill, wrapper_name)
    with patch.object(
        tiktok_backfill,
        "build_tiktok_request",
        side_effect=RuntimeError(f"build failed {WRAPPER_PAYLOAD_VALUE}"),
    ), httpx.Client() as client:
        with pytest.raises(RuntimeError) as caught:
            wrapper(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                **wrapper_kwargs,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_QUERY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert type(caught.value) is RuntimeError


@pytest.mark.parametrize(
    ("wrapper_name", "wrapper_kwargs", "malformed_payload"),
    _ANALYTICS_WRAPPER_CASES,
)
def test_analytics_wrapper_malformed_success_detaches_payload_and_credentials(
    wrapper_name: str,
    wrapper_kwargs: dict[str, object],
    malformed_payload: dict[str, object],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=malformed_payload, request=request)

    wrapper = getattr(tiktok_backfill, wrapper_name)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError) as caught:
            wrapper(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                **wrapper_kwargs,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_QUERY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert type(caught.value) is ValueError


def test_order_wrapper_http_failure_retries_and_preserves_safe_status_and_code() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(
            503,
            headers={"x-debug-value": WRAPPER_PAYLOAD_VALUE},
            json={"code": 105001, "message": WRAPPER_PAYLOAD_VALUE},
            request=request,
        )

    with patch.object(tiktok_backfill.time, "sleep", return_value=None), httpx.Client(
        transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.fetch_tiktok_order_list_page(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
                page_size=1,
                cursor=WRAPPER_QUERY_VALUE,
            )

    assert len(outgoing) == 3
    signed_url = str(outgoing[-1].url)
    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_QUERY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
            signed_url,
        ),
    )
    assert caught.value.response.status_code == 503
    assert getattr(caught.value, "tiktok_error_code", None) == "105001"


def test_order_wrapper_api_failure_preserves_safe_error_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": 105001,
                "message": WRAPPER_PAYLOAD_VALUE,
                "data": {"echo": WRAPPER_PAYLOAD_VALUE},
            },
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.fetch_tiktok_order_list_page(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
                page_size=1,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert type(caught.value) is RuntimeError
    assert getattr(caught.value, "tiktok_error_code", None) == "105001"


@pytest.mark.parametrize(
    "error_code",
    (
        WRAPPER_SHOP_CIPHER,
        WRAPPER_SHOP_CIPHER[:20],
        WRAPPER_SHOP_CIPHER[8:28],
    ),
    ids=("exact", "prefix", "substring"),
)
def test_order_http_error_rejects_shop_cipher_derived_error_code(
    error_code: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={"code": error_code, "message": "synthetic provider error"},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.fetch_tiktok_order_list_page(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
                page_size=1,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(WRAPPER_SHOP_CIPHER, error_code),
    )
    assert error_code not in capsys.readouterr().err
    assert getattr(caught.value, "tiktok_error_code", None) is None


@pytest.mark.parametrize(
    "error_code",
    (
        WRAPPER_CREATOR_ACCESS_TOKEN,
        WRAPPER_CREATOR_ACCESS_TOKEN[:24],
        WRAPPER_CREATOR_ACCESS_TOKEN[8:32],
    ),
    ids=("exact", "prefix", "substring"),
)
def test_live_analytics_rejects_creator_token_derived_error_code(error_code: str) -> None:
    inner_error = RuntimeError("synthetic provider error")
    inner_error.tiktok_error_code = error_code  # type: ignore[attr-defined]

    with patch.object(tiktok_backfill, "_try_live_core_stats", side_effect=inner_error), httpx.Client() as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.fetch_tiktok_live_analytics(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                creator_access_token=WRAPPER_CREATOR_ACCESS_TOKEN,
                live_room_id="synthetic-live-room",
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(WRAPPER_CREATOR_ACCESS_TOKEN, error_code),
    )
    assert getattr(caught.value, "tiktok_error_code", None) is None


def test_product_wrapper_api_failure_detaches_product_body_and_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": 105001, "message": WRAPPER_PAYLOAD_VALUE},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.create_tiktok_product(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                product_body={"title": WRAPPER_BODY_VALUE},
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_BODY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert getattr(caught.value, "tiktok_error_code", None) == "105001"


def test_live_analytics_wrapper_http_failure_detaches_response_and_credentials() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=WRAPPER_PAYLOAD_VALUE, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.fetch_tiktok_live_analytics(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert type(caught.value) is RuntimeError
    assert getattr(caught.value, "status_code", None) == 500


def test_image_upload_wrapper_http_failure_detaches_multipart_body_and_credentials() -> None:
    outgoing: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        outgoing.append(request)
        return httpx.Response(401, content=WRAPPER_PAYLOAD_VALUE, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            tiktok_backfill.upload_tiktok_product_image(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                image_data=WRAPPER_BODY_VALUE.encode("utf-8"),
            )

    assert len(outgoing) == 1
    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_BODY_VALUE,
            WRAPPER_PAYLOAD_VALUE,
            str(outgoing[0].url),
        ),
    )
    assert caught.value.response.status_code == 401


def test_seller_affiliate_wrapper_preserves_scope_missing_classification() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": 105005, "message": f"Access denied {WRAPPER_PAYLOAD_VALUE}"},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.fetch_tiktok_seller_affiliate_orders_page(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )
    assert getattr(caught.value, "tiktok_error_code", None) == "105005"
    assert tiktok_backfill.tiktok_affiliate_order_error_is_scope_missing(caught.value)


def test_order_wrapper_success_result_is_unchanged() -> None:
    expected_order = {"id": "synthetic-order-id"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": 0, "data": {"orders": [expected_order]}},
            request=request,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        payload, orders = tiktok_backfill.fetch_tiktok_order_list_page(
            client,
            base_url=WRAPPER_BASE_URL,
            app_key="synthetic-app-key",
            app_secret=WRAPPER_APP_SECRET,
            access_token=WRAPPER_ACCESS_TOKEN,
            shop_id="synthetic-shop",
            shop_cipher=WRAPPER_SHOP_CIPHER,
            since=None,
            until=None,
            page_size=1,
        )

    assert payload["code"] == 0
    assert orders == [expected_order]


@pytest.mark.parametrize("signal", (SystemExit(7), KeyboardInterrupt()))
def test_order_wrapper_does_not_catch_base_exceptions(signal: BaseException) -> None:
    with patch.object(tiktok_backfill, "request_json", side_effect=signal), httpx.Client() as client:
        with pytest.raises(type(signal)) as caught:
            tiktok_backfill.fetch_tiktok_order_list_page(
                client,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
                page_size=1,
            )

    assert caught.value is signal


@pytest.mark.parametrize(
    ("orchestrator_name", "fetch_name", "orchestrator_kwargs"),
    (
        (
            "backfill_tiktok_orders",
            "fetch_tiktok_order_list_page",
            {"affiliate_attribution": False},
        ),
        (
            "backfill_tiktok_products",
            "fetch_tiktok_product_list_page",
            {},
        ),
    ),
)
def test_backfill_orchestrator_failure_detaches_outer_credential_frame(
    orchestrator_name: str,
    fetch_name: str,
    orchestrator_kwargs: dict[str, object],
) -> None:
    inner_error = RuntimeError("sanitized endpoint failure")
    inner_error.tiktok_error_code = "105001"  # type: ignore[attr-defined]
    orchestrator = getattr(tiktok_backfill, orchestrator_name)

    with patch.object(tiktok_backfill, fetch_name, side_effect=inner_error):
        with pytest.raises(RuntimeError) as caught:
            orchestrator(
                object(),
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_id="synthetic-shop",
                shop_cipher=WRAPPER_SHOP_CIPHER,
                limit=1,
                **orchestrator_kwargs,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
        ),
    )
    assert type(caught.value) is RuntimeError
    assert getattr(caught.value, "tiktok_error_code", None) == "105001"


def _creator_auth_row() -> SimpleNamespace:
    return SimpleNamespace(
        creator_username="synthetic-creator",
        access_token=WRAPPER_CREATOR_ACCESS_TOKEN,
        app_key="synthetic-app-key",
        scopes_json=f'["{tiktok_backfill.TIKTOK_CREATOR_AFFILIATE_ORDER_READ_SCOPE}"]',
    )


class _AffiliateSession:
    def __init__(self, *, creator_rows: list[SimpleNamespace] | None = None) -> None:
        self.creator_rows = creator_rows or []

    def exec(self, statement: object) -> SimpleNamespace:
        return SimpleNamespace(all=lambda: self.creator_rows)

    def commit(self) -> None:
        raise RuntimeError(f"commit failed {WRAPPER_PAYLOAD_VALUE}")


def test_seller_affiliate_backfill_client_failure_detaches_credentials() -> None:
    with patch.object(
        tiktok_backfill.httpx,
        "Client",
        side_effect=RuntimeError(f"client failed {WRAPPER_PAYLOAD_VALUE}"),
    ):
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.backfill_tiktok_order_affiliate_attributions(
                object(),
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )


def test_creator_affiliate_backfill_query_failure_detaches_credentials() -> None:
    class QueryFailureSession:
        def exec(self, statement: object) -> object:
            raise RuntimeError(f"query failed {WRAPPER_PAYLOAD_VALUE}")

    with pytest.raises(RuntimeError) as caught:
        tiktok_backfill.backfill_tiktok_creator_affiliate_attributions(
            QueryFailureSession(),
            base_url=WRAPPER_BASE_URL,
            app_key="synthetic-app-key",
            app_secret=WRAPPER_APP_SECRET,
            since=None,
            until=None,
        )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(WRAPPER_APP_SECRET, WRAPPER_PAYLOAD_VALUE),
    )


def test_creator_affiliate_backfill_client_failure_detaches_loaded_token() -> None:
    session = _AffiliateSession(creator_rows=[_creator_auth_row()])
    with patch.object(
        tiktok_backfill.httpx,
        "Client",
        side_effect=RuntimeError(f"client failed {WRAPPER_PAYLOAD_VALUE}"),
    ):
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.backfill_tiktok_creator_affiliate_attributions(
                session,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                since=None,
                until=None,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_CREATOR_ACCESS_TOKEN,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )


def test_seller_affiliate_backfill_commit_failure_detaches_credentials() -> None:
    session = _AffiliateSession()
    with patch.object(
        tiktok_backfill,
        "fetch_tiktok_seller_affiliate_orders_page",
        return_value=({"code": 0}, [{"order_id": "synthetic-order"}]),
    ), patch.object(
        tiktok_backfill,
        "upsert_tiktok_order_affiliate_attribution",
        return_value="updated",
    ):
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.backfill_tiktok_order_affiliate_attributions(
                session,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_ACCESS_TOKEN,
            WRAPPER_SHOP_CIPHER,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )


def test_creator_affiliate_backfill_commit_failure_detaches_loaded_token() -> None:
    session = _AffiliateSession(creator_rows=[_creator_auth_row()])
    with patch.object(
        tiktok_backfill,
        "fetch_tiktok_creator_affiliate_orders_page",
        return_value=({"code": 0}, [{"order_id": "synthetic-order"}]),
    ), patch.object(
        tiktok_backfill,
        "upsert_tiktok_order_creator_affiliate_attribution",
        return_value="updated",
    ):
        with pytest.raises(RuntimeError) as caught:
            tiktok_backfill.backfill_tiktok_creator_affiliate_attributions(
                session,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                since=None,
                until=None,
            )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(
            WRAPPER_APP_SECRET,
            WRAPPER_CREATOR_ACCESS_TOKEN,
            WRAPPER_PAYLOAD_VALUE,
        ),
    )


@pytest.mark.parametrize("creator", (False, True), ids=("seller", "creator"))
def test_affiliate_backfill_scope_failure_still_returns_summary(creator: bool) -> None:
    scope_error = RuntimeError("synthetic scope failure")
    scope_error.tiktok_error_code = "105005"  # type: ignore[attr-defined]
    scope_error.tiktok_scope_missing = True  # type: ignore[attr-defined]
    session = _AffiliateSession(creator_rows=[_creator_auth_row()])
    fetch_name = (
        "fetch_tiktok_creator_affiliate_orders_page"
        if creator
        else "fetch_tiktok_seller_affiliate_orders_page"
    )

    with patch.object(tiktok_backfill, fetch_name, side_effect=scope_error):
        if creator:
            summary = tiktok_backfill.backfill_tiktok_creator_affiliate_attributions(
                session,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                since=None,
                until=None,
            )
        else:
            summary = tiktok_backfill.backfill_tiktok_order_affiliate_attributions(
                session,
                base_url=WRAPPER_BASE_URL,
                app_key="synthetic-app-key",
                app_secret=WRAPPER_APP_SECRET,
                access_token=WRAPPER_ACCESS_TOKEN,
                shop_cipher=WRAPPER_SHOP_CIPHER,
                since=None,
                until=None,
            )

    assert summary.affiliate_scope_missing is True
    assert summary.affiliate_failed == 0


def test_order_record_malformed_payload_detaches_payload_and_shop_cipher() -> None:
    malformed_payload = {
        "shop_cipher": WRAPPER_SHOP_CIPHER,
        "provider_message": WRAPPER_PAYLOAD_VALUE,
    }

    with pytest.raises(ValueError) as caught:
        tiktok_backfill.order_record_from_payload(
            malformed_payload,
            shop_id="synthetic-shop",
            shop_cipher=WRAPPER_SHOP_CIPHER,
            source="synthetic-test",
        )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(WRAPPER_SHOP_CIPHER, WRAPPER_PAYLOAD_VALUE),
    )
    assert type(caught.value) is ValueError
    assert tiktok_backfill.ORDER_DETAIL_PATH in str(caught.value)


def test_product_upsert_query_failure_detaches_session_record_and_payload() -> None:
    class QueryFailureSession:
        def exec(self, statement: object) -> object:
            raise RuntimeError(f"query failed {WRAPPER_PAYLOAD_VALUE}")

    product_payload = {
        "id": "synthetic-product",
        "title": WRAPPER_PAYLOAD_VALUE,
        "shop_cipher": WRAPPER_SHOP_CIPHER,
        "create_time": 1_700_000_000,
    }
    session = QueryFailureSession()

    with pytest.raises(RuntimeError) as caught:
        tiktok_backfill.upsert_tiktok_product_row(
            session,
            product_payload,
            shop_id="synthetic-shop",
            shop_cipher=WRAPPER_SHOP_CIPHER,
            source="synthetic-test",
        )

    _assert_detached_failure(
        caught.value,
        forbidden_tokens=(WRAPPER_SHOP_CIPHER, WRAPPER_PAYLOAD_VALUE),
    )
    assert all(
        session not in frame_locals.values()
        for frame_locals in _script_traceback_locals(caught.value)
    )
    assert type(caught.value) is RuntimeError
    assert tiktok_backfill.PRODUCT_DETAIL_PATH in str(caught.value)


def test_order_record_success_result_is_unchanged() -> None:
    record = tiktok_backfill.order_record_from_payload(
        {
            "id": "synthetic-order",
            "create_time": 1_700_000_000,
            "shop_cipher": "synthetic-result-cipher",
        },
        shop_id="synthetic-shop",
        shop_cipher="fallback-cipher",
        source="synthetic-test",
    )

    assert record["tiktok_order_id"] == "synthetic-order"
    assert record["shop_cipher"] == "synthetic-result-cipher"
    assert record["source"] == "synthetic-test"


def test_product_upsert_success_result_is_unchanged() -> None:
    class InsertSession:
        def __init__(self) -> None:
            self.added: list[object] = []

        def exec(self, statement: object) -> SimpleNamespace:
            return SimpleNamespace(first=lambda: None)

        def add(self, row: object) -> None:
            self.added.append(row)

    session = InsertSession()
    result = tiktok_backfill.upsert_tiktok_product_row(
        session,
        {
            "id": "synthetic-product",
            "title": "Synthetic Product",
            "create_time": 1_700_000_000,
        },
        shop_id="synthetic-shop",
        shop_cipher="synthetic-cipher",
        source="synthetic-test",
    )

    assert result == "inserted"
    assert len(session.added) == 1
    assert session.added[0].tiktok_product_id == "synthetic-product"
