from __future__ import annotations

import ipaddress
import threading
from datetime import timedelta
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete, func
from sqlalchemy.exc import OperationalError
from sqlmodel import select

from .config import get_settings
from .models import RateLimitHit, utcnow

_TABLE_READY = False
_TABLE_LOCK = threading.Lock()


def _ensure_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    with _TABLE_LOCK:
        if _TABLE_READY:
            return
        from .db import engine

        RateLimitHit.__table__.create(engine, checkfirst=True)
        _TABLE_READY = True


def check(key: str, *, max_requests: int, window_seconds: float) -> bool:
    """Return True if allowed; False if rate-limited.

    Hits are stored in the application database so auth-facing limits survive
    process restarts and multi-worker rotation.
    """
    if max_requests <= 0:
        return False
    _ensure_table()
    from .db import managed_session

    now = utcnow()
    window = timedelta(seconds=max(float(window_seconds), 0.0))
    cutoff = now - window
    try:
        with managed_session() as session:
            session.exec(delete(RateLimitHit).where(RateLimitHit.expires_at <= now))
            hits = int(
                session.exec(
                    select(func.count())
                    .select_from(RateLimitHit)
                    .where(
                        RateLimitHit.bucket_key == key,
                        RateLimitHit.created_at >= cutoff,
                    )
                ).one()
            )
            if hits >= max_requests:
                session.commit()
                return False
            session.add(
                RateLimitHit(
                    bucket_key=key,
                    created_at=now,
                    expires_at=now + window,
                )
            )
            session.commit()
            return True
    except OperationalError:
        return False


def reset(key: Optional[str] = None) -> None:
    _ensure_table()
    from .db import managed_session

    with managed_session() as session:
        stmt = delete(RateLimitHit)
        if key is not None:
            stmt = stmt.where(RateLimitHit.bucket_key == key)
        session.exec(stmt)
        session.commit()


def _client_ip(request: Request) -> str:
    settings = get_settings()
    client_host = request.client.host if request.client and request.client.host else ""
    if settings.trust_x_forwarded_for and settings.is_trusted_proxy(client_host):
        forwarded_for = request.headers.get("x-forwarded-for", "")
        first_hop = forwarded_for.split(",", 1)[0].strip()
        if first_hop:
            try:
                ipaddress.ip_address(first_hop)
            except ValueError:
                first_hop = ""
        if first_hop:
            return first_hop
    if client_host:
        return client_host
    return "unknown"


def rate_limited_or_429(
    request: Request,
    *,
    key_prefix: str,
    max_requests: int = 3,
    window_seconds: float = 900.0,
) -> Optional[JSONResponse]:
    key = f"{key_prefix}:{_client_ip(request)}"
    if check(key, max_requests=max_requests, window_seconds=window_seconds):
        return None
    retry_after = int(window_seconds)
    return JSONResponse(
        {"error": "rate_limited", "retry_after_seconds": retry_after},
        status_code=429,
        headers={"Retry-After": str(retry_after)},
    )
