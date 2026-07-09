from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

import httpx

from scripts import tiktok_backfill


APP_SECRET = "app-secret-SENTINEL-9fdb"
REFRESH_TOKEN = "refresh-token-SENTINEL-ae31"


def _serialized_http_error(exc: BaseException) -> str:
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
        pieces.extend((str(current), repr(current), repr(getattr(current, "doc", None))))
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
        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
    return "\n".join(pieces)


class TikTokRefreshRedactionTests(unittest.TestCase):
    def _assert_secrets_absent(self, text: str) -> None:
        self.assertNotIn(APP_SECRET, text)
        self.assertNotIn(REFRESH_TOKEN, text)

    def test_http_refresh_failure_keeps_outgoing_query_but_sanitizes_exception(self) -> None:
        outgoing: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            outgoing.append(request)
            return httpx.Response(
                400,
                headers={"x-debug-token": REFRESH_TOKEN},
                json={
                    "code": 105001,
                    "message": f"app_secret={APP_SECRET} refresh_token={REFRESH_TOKEN}",
                },
                request=request,
            )

        stderr = io.StringIO()
        with httpx.Client(transport=httpx.MockTransport(handler)) as client, redirect_stderr(stderr):
            with self.assertRaises(httpx.HTTPStatusError) as ctx:
                tiktok_backfill.refresh_access_token(
                    client,
                    base_url="https://unused.example",
                    app_key="app-key",
                    app_secret=APP_SECRET,
                    refresh_token=REFRESH_TOKEN,
                )

        self.assertEqual(len(outgoing), 1)
        self.assertEqual(outgoing[0].method, "GET")
        self.assertEqual(outgoing[0].url.host, "auth.tiktok-shops.com")
        self.assertEqual(outgoing[0].url.path, "/api/v2/token/refresh")
        self.assertEqual(outgoing[0].url.params["app_secret"], APP_SECRET)
        self.assertEqual(outgoing[0].url.params["refresh_token"], REFRESH_TOKEN)
        self.assertEqual(outgoing[0].url.params["grant_type"], "refresh_token")

        exc = ctx.exception
        serialized = _serialized_http_error(exc)
        self._assert_secrets_absent(serialized)
        self._assert_secrets_absent(stderr.getvalue())
        log_payload = json.loads(stderr.getvalue().strip())
        self.assertEqual(log_payload["action"], "tiktok.auth.http_failed")
        self.assertEqual(log_payload["error"], "TikTok authentication request failed")
        self.assertEqual(log_payload["error_type"], "HTTPStatusError")
        self.assertEqual(log_payload["error_code"], "105001")
        self.assertEqual(log_payload["method"], "GET")
        self.assertEqual(log_payload["status_code"], 400)
        self.assertEqual(log_payload["endpoint_host"], "auth.tiktok-shops.com")
        self.assertEqual(log_payload["endpoint_path"], "/api/v2/token/refresh")
        self.assertEqual(exc.response.status_code, 400)
        self.assertEqual(exc.request.method, "GET")
        self.assertEqual(exc.request.url.host, "auth.tiktok-shops.com")
        self.assertEqual(exc.request.url.path, "/api/v2/token/refresh")
        self.assertFalse(exc.request.url.query)
        self.assertFalse(exc.response.request.url.query)
        self.assertEqual(exc.request.content, b"")
        self.assertEqual(exc.response.content, b"")
        self.assertIn("105001", str(exc))

    def test_transport_refresh_failure_sanitizes_exception_request_and_cause_text(self) -> None:
        outgoing: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            outgoing.append(request)
            raise httpx.ConnectError(
                f"connect failed app_secret={APP_SECRET} refresh_token={REFRESH_TOKEN}",
                request=request,
            )

        stderr = io.StringIO()
        with patch.object(tiktok_backfill.time, "sleep", return_value=None), httpx.Client(
            transport=httpx.MockTransport(handler)
        ) as client, redirect_stderr(stderr):
            with self.assertRaises(httpx.TransportError) as ctx:
                tiktok_backfill.refresh_access_token(
                    client,
                    base_url="https://unused.example",
                    app_key="app-key",
                    app_secret=APP_SECRET,
                    refresh_token=REFRESH_TOKEN,
                )

        self.assertEqual(len(outgoing), 3)
        for request in outgoing:
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.url.params["app_secret"], APP_SECRET)
            self.assertEqual(request.url.params["refresh_token"], REFRESH_TOKEN)

        exc = ctx.exception
        serialized = _serialized_http_error(exc)
        self._assert_secrets_absent(serialized)
        self._assert_secrets_absent(stderr.getvalue())
        self.assertEqual(exc.request.method, "GET")
        self.assertEqual(exc.request.url.host, "auth.tiktok-shops.com")
        self.assertEqual(exc.request.url.path, "/api/v2/token/refresh")
        self.assertFalse(exc.request.url.query)
        self.assertEqual(exc.request.content, b"")
        self.assertIn("ConnectError", str(exc))

    def test_successful_http_response_with_tiktok_error_omits_echoed_secrets(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "code": "invalid_refresh_token",
                    "message": f"rejected {APP_SECRET} and {REFRESH_TOKEN}",
                },
                request=request,
            )

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(RuntimeError) as ctx:
                tiktok_backfill.refresh_access_token(
                    client,
                    base_url="https://unused.example",
                    app_key="app-key",
                    app_secret=APP_SECRET,
                    refresh_token=REFRESH_TOKEN,
                )

        text = f"{ctx.exception}\n{ctx.exception!r}"
        self._assert_secrets_absent(text)
        self.assertIn("invalid_refresh_token", text)
        self.assertIn("/api/v2/token/refresh", text)

    def test_invalid_json_refresh_response_does_not_retain_secret_body_in_exception_graph(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=f"not-json {APP_SECRET} {REFRESH_TOKEN}",
                request=request,
            )

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(RuntimeError) as ctx:
                tiktok_backfill.refresh_access_token(
                    client,
                    base_url="https://unused.example",
                    app_key="app-key",
                    app_secret=APP_SECRET,
                    refresh_token=REFRESH_TOKEN,
                )

        serialized = _serialized_http_error(ctx.exception)
        self._assert_secrets_absent(serialized)
        self.assertIsNone(ctx.exception.__context__)


if __name__ == "__main__":
    unittest.main()
