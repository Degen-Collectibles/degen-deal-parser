"""Small pure-ASGI request body limit for sensitive upload endpoints."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any


class RequestBodyTooLarge(Exception):
    pass


class ExactPathBodyLimitMiddleware:
    def __init__(self, app: Any, *, limits: dict[tuple[str, str], int]) -> None:
        self.app = app
        self.limits = {(method.upper(), path): int(limit) for (method, path), limit in limits.items()}

    async def __call__(self, scope: dict[str, Any], receive: Callable[[], Awaitable[dict[str, Any]]], send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        key = (str(scope.get("method") or "").upper(), str(scope.get("path") or ""))
        limit = self.limits.get(key)
        if limit is None:
            await self.app(scope, receive, send)
            return

        headers = {bytes(name).lower(): bytes(value) for name, value in scope.get("headers", [])}
        raw_length = headers.get(b"content-length")
        if raw_length is not None:
            try:
                if int(raw_length) > limit:
                    await self._reject(send)
                    return
            except ValueError:
                await self._reject(send)
                return

        seen = 0
        buffered: list[dict[str, Any]] = []
        while True:
            message = await receive()
            if message.get("type") == "http.request":
                seen += len(message.get("body", b""))
                if seen > limit:
                    await self._reject(send)
                    return
            buffered.append(message)
            if message.get("type") != "http.request" or not message.get("more_body", False):
                break

        next_message = 0

        async def replay_receive() -> dict[str, Any]:
            nonlocal next_message
            if next_message < len(buffered):
                message = buffered[next_message]
                next_message += 1
                return message
            return {"type": "http.request", "body": b"", "more_body": False}

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        body = b'{"detail":"request_body_too_large"}'
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
