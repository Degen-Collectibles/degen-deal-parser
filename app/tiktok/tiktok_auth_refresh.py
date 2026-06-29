"""Background TikTok token refresh logic.

Extracted from main.py so it can be called from both the FastAPI app
(main.py) and the background worker service (worker_service.py) without
circular imports.
"""

from __future__ import annotations

import functools
import json
import threading
import traceback
from collections.abc import Callable
from datetime import timedelta, timezone
from typing import Any, NoReturn, Optional

import httpx
from sqlmodel import Session

from ..config import get_settings
from ..models import TikTokAuth, TikTokCreatorAuth, utcnow
from .tiktok_ingest import (
    TIKTOK_DEFAULT_API_BASE_URL,
    TIKTOK_SHOP_AUTH_BASE_URL,
    TIKTOK_SHOP_TOKEN_REFRESH_PATH,
    TIKTOK_TOKEN_REFRESH_PATH,
    TikTokIngestError,
    build_tiktok_api_url,
    upsert_tiktok_creator_auth_from_callback,
    upsert_tiktok_auth_from_callback,
    validate_tiktok_api_response,
)

try:
    from scripts.tiktok_backfill import refresh_access_token as _refresh_fn
except Exception:
    _refresh_fn = None

settings = get_settings()

# Serializes shop + creator refresh so two tasks never consume the same refresh token
# or interleave commits on the shared TikTokAuth row.
_tiktok_auth_refresh_lock = threading.Lock()


def _safe_refresh_status_code(error: Exception) -> Optional[int]:
    response = getattr(error, "response", None)
    raw_status = getattr(response, "status_code", None)
    if not isinstance(raw_status, int):
        raw_status = getattr(error, "status_code", None)
    if isinstance(raw_status, int) and 100 <= raw_status <= 599:
        return raw_status
    return None


def _refresh_sensitive_values(error: Exception) -> tuple[str, ...]:
    values: set[str] = set()
    traceback_cursor = error.__traceback__
    while traceback_cursor is not None:
        frame = traceback_cursor.tb_frame
        normalized_filename = frame.f_code.co_filename.replace("\\", "/")
        if normalized_filename.endswith("/app/tiktok/tiktok_auth_refresh.py"):
            frame_locals = frame.f_locals
            for name in (
                "app_secret",
                "refresh_token",
                "creator_refresh",
                "access_token",
                "new_refresh",
            ):
                value = frame_locals.get(name)
                if isinstance(value, str) and value:
                    values.add(value)
            auth_row = frame_locals.get("auth_row")
            if isinstance(auth_row, TikTokAuth):
                for field_name in (
                    "access_token",
                    "refresh_token",
                    "creator_access_token",
                    "creator_refresh_token",
                ):
                    try:
                        value = getattr(auth_row, field_name, None)
                    except Exception:
                        value = None
                    if isinstance(value, str) and value:
                        values.add(value)
        traceback_cursor = traceback_cursor.tb_next
    return tuple(values)


def _safe_refresh_error_code(error: Exception, *, sensitive_values: tuple[str, ...]) -> Optional[str]:
    raw_code = getattr(error, "tiktok_error_code", None)
    if isinstance(raw_code, int):
        code = str(raw_code)
    elif isinstance(raw_code, str):
        code = raw_code.strip()
    else:
        return None
    if not code.isdigit() or not 1 <= len(code) <= 12:
        return None
    for sensitive_value in sensitive_values:
        if code == sensitive_value or code in sensitive_value or sensitive_value in code:
            return None
    return code


def _sanitized_refresh_error(
    error: Exception,
    *,
    operation: str,
    method: str,
    endpoint_url: str,
    sensitive_values: tuple[str, ...],
    preserve_error_code: bool,
) -> Exception:
    parsed_url = httpx.URL(endpoint_url)
    safe_url = str(parsed_url.copy_with(query=None, fragment=None))
    endpoint = f"{parsed_url.host or 'unknown'}{parsed_url.path}"
    status_code = _safe_refresh_status_code(error)
    error_code = (
        _safe_refresh_error_code(error, sensitive_values=sensitive_values)
        if preserve_error_code
        else None
    )
    status_suffix = f" HTTP {status_code}" if status_code is not None else ""
    code_suffix = f" code {error_code}" if error_code is not None else ""
    message = f"{operation} failed ({type(error).__name__}){status_suffix}{code_suffix} for {method} {endpoint}"

    safe_error: Exception
    if isinstance(error, httpx.HTTPStatusError):
        safe_request = httpx.Request(method, safe_url)
        safe_response = httpx.Response(status_code or 500, request=safe_request)
        safe_error = httpx.HTTPStatusError(
            message,
            request=safe_request,
            response=safe_response,
        )
    elif isinstance(error, httpx.TransportError):
        safe_request = httpx.Request(method, safe_url)
        try:
            safe_error = type(error)(message, request=safe_request)
        except Exception:
            safe_error = httpx.TransportError(message)
    elif isinstance(error, TikTokIngestError):
        safe_error = TikTokIngestError(message)
    else:
        try:
            safe_error = type(error)(message)
        except Exception:
            safe_error = RuntimeError(message)

    if error_code is not None:
        safe_error.tiktok_error_code = error_code  # type: ignore[attr-defined]
    if status_code is not None:
        safe_error.status_code = status_code  # type: ignore[attr-defined]
    safe_error.tiktok_scope_missing = bool(  # type: ignore[attr-defined]
        getattr(error, "tiktok_scope_missing", False)
    )
    return safe_error


def _clear_refresh_failure(error: Exception) -> None:
    try:
        if error.__traceback__ is not None:
            traceback.clear_frames(error.__traceback__)
        error.__traceback__ = None
        error.__cause__ = None
        error.__context__ = None
        error.args = ()
        error_state = getattr(error, "__dict__", None)
        if isinstance(error_state, dict):
            error_state.clear()
    except Exception:
        pass


def _raise_sanitized_refresh_error(error: Exception) -> NoReturn:
    raise error


def _sanitize_refresh_boundary(*, operation: str, method: str, endpoint_url: str) -> Callable:
    def decorate(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            failure: Optional[Exception] = None
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                failure = exc

            try:
                sensitive_values = _refresh_sensitive_values(failure)
                preserve_error_code = True
            except Exception as collection_error:
                _clear_refresh_failure(collection_error)
                sensitive_values = ()
                preserve_error_code = False
            try:
                safe_error = _sanitized_refresh_error(
                    failure,
                    operation=operation,
                    method=method,
                    endpoint_url=endpoint_url,
                    sensitive_values=sensitive_values,
                    preserve_error_code=preserve_error_code,
                )
            except Exception:
                safe_error = RuntimeError(f"{operation} failed for {method}")
            _clear_refresh_failure(failure)
            kwargs.clear()
            del args
            del kwargs
            del sensitive_values
            del preserve_error_code
            del failure
            _raise_sanitized_refresh_error(safe_error)

        return wrapped

    return decorate


@_sanitize_refresh_boundary(
    operation="TikTok Shop token refresh",
    method="GET",
    endpoint_url=f"{TIKTOK_SHOP_AUTH_BASE_URL}{TIKTOK_SHOP_TOKEN_REFRESH_PATH}",
)
def refresh_tiktok_auth_if_needed(
    session: Session,
    *,
    runtime_name: str,
    force: bool = False,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    resolve_base_url: Callable[[], str],
    update_state: Optional[Callable[..., None]] = None,
) -> Optional[dict]:
    """Refresh the TikTok access token if it is close to expiry.

    Parameters
    ----------
    resolve_base_url:
        Callable that returns the TikTok Shop API base URL.
        Passed in to avoid importing from main.py.
    update_state:
        Optional callable to update the in-memory TikTok integration state
        (main.update_tiktok_integration_state).  Safe to omit when called
        from a background thread where the state dict is not needed.
    """
    if _refresh_fn is None:
        return None

    if not _tiktok_auth_refresh_lock.acquire(blocking=False):
        return None

    try:
        from sqlmodel import select  # local import to keep top-level imports minimal

        result: Optional[dict] = None
        stmt = select(TikTokAuth)
        clean_shop_id = (shop_id or "").strip()
        clean_shop_cipher = (shop_cipher or "").strip()
        if clean_shop_id and not clean_shop_id.startswith("pending:"):
            stmt = stmt.where(TikTokAuth.tiktok_shop_id == clean_shop_id)
        elif clean_shop_cipher:
            stmt = stmt.where(TikTokAuth.shop_cipher == clean_shop_cipher)
        auth_row: Optional[TikTokAuth] = session.exec(
            stmt.order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())
        ).first()
        app_secret = (settings.tiktok_app_secret or "").strip()

        if auth_row is not None:
            app_key = (settings.tiktok_app_key or auth_row.app_key or "").strip()
            refresh_token = (auth_row.refresh_token or settings.tiktok_refresh_token or "").strip()
            if app_key and app_secret and refresh_token:
                token_expires_at = auth_row.access_token_expires_at
                if token_expires_at is not None and token_expires_at.tzinfo is None:
                    token_expires_at = token_expires_at.replace(tzinfo=timezone.utc)
                should_refresh = (
                    force
                    or not auth_row.access_token
                    or not token_expires_at
                    or token_expires_at <= utcnow() + timedelta(minutes=10)
                )
                if should_refresh:
                    with httpx.Client(timeout=40.0, follow_redirects=False) as client:
                        refreshed = _refresh_fn(
                            client,
                            base_url=resolve_base_url(),
                            app_key=app_key,
                            app_secret=app_secret,
                            refresh_token=refresh_token,
                        )

                    token_data = refreshed
                    if isinstance(refreshed, dict) and isinstance(refreshed.get("data"), dict):
                        token_data = refreshed["data"]

                    status, auth_record = upsert_tiktok_auth_from_callback(
                        session,
                        TikTokAuth,
                        token_result=token_data,
                        app_key=app_key,
                        redirect_uri=(auth_row.redirect_uri or settings.tiktok_redirect_uri or "").strip(),
                        fallback_shop_id=(settings.tiktok_shop_id or auth_row.tiktok_shop_id or "").strip(),
                        source="oauth_refresh",
                        received_at=utcnow(),
                        dry_run=False,
                    )

                    if update_state is not None:
                        update_state(
                            is_pull_running=False,
                            last_pull_started_at=utcnow(),
                            last_pull_finished_at=utcnow(),
                            last_error=None,
                            last_pull_at=utcnow(),
                            last_pull={
                                "status": "refresh",
                                "auth_status": status,
                                "shop_id": auth_record.get("tiktok_shop_id"),
                                "runtime": runtime_name,
                            },
                        )

                    session.commit()
                    result = {"status": status, "auth_record": auth_record}

        creator_auth_refreshed = 0
        if not clean_shop_id and not clean_shop_cipher and app_secret:
            creator_rows = list(
                session.exec(
                    select(TikTokCreatorAuth).order_by(
                        TikTokCreatorAuth.updated_at.desc(),
                        TikTokCreatorAuth.id.desc(),
                    )
                ).all()
            )
            for creator_row in creator_rows:
                creator_app_key = (settings.tiktok_app_key or creator_row.app_key or "").strip()
                creator_refresh_token = (creator_row.refresh_token or "").strip()
                if not creator_app_key or not creator_refresh_token:
                    continue
                token_expires_at = creator_row.access_token_expires_at
                if token_expires_at is not None and token_expires_at.tzinfo is None:
                    token_expires_at = token_expires_at.replace(tzinfo=timezone.utc)
                should_refresh_creator = (
                    force
                    or not creator_row.access_token
                    or not token_expires_at
                    or token_expires_at <= utcnow() + timedelta(minutes=10)
                )
                if not should_refresh_creator:
                    continue

                with httpx.Client(timeout=40.0, follow_redirects=False) as client:
                    refreshed = _refresh_fn(
                        client,
                        base_url=resolve_base_url(),
                        app_key=creator_app_key,
                        app_secret=app_secret,
                        refresh_token=creator_refresh_token,
                    )

                token_data = refreshed
                if isinstance(refreshed, dict) and isinstance(refreshed.get("data"), dict):
                    token_data = refreshed["data"]
                if isinstance(token_data, dict):
                    token_data = dict(token_data)
                    token_data.setdefault("open_id", creator_row.open_id)
                    try:
                        existing_scopes = json.loads(creator_row.scopes_json or "[]")
                    except Exception:
                        existing_scopes = []
                    token_data.setdefault("granted_scopes", existing_scopes)

                upsert_tiktok_creator_auth_from_callback(
                    session,
                    TikTokCreatorAuth,
                    token_result=token_data,
                    creator_username=creator_row.creator_username,
                    app_key=creator_app_key,
                    redirect_uri=(creator_row.redirect_uri or settings.tiktok_redirect_uri or "").strip(),
                    source="creator_oauth_refresh",
                    received_at=utcnow(),
                    dry_run=False,
                )
                session.commit()
                creator_auth_refreshed += 1

        if creator_auth_refreshed:
            if result is None:
                result = {"status": "creator_refresh", "auth_record": None}
            result["creator_auth_refreshed"] = creator_auth_refreshed

        return result
    finally:
        _tiktok_auth_refresh_lock.release()


def _parse_oauth_token_field(payload: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        raw = payload.get(key)
        if raw in (None, ""):
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def _parse_expires_in_seconds(payload: dict[str, Any]) -> int:
    raw = payload.get("expires_in")
    if raw in (None, ""):
        raw = payload.get("expiresIn")
    if raw in (None, ""):
        return 86400
    try:
        return max(int(raw), 1)
    except (TypeError, ValueError):
        return 86400


def _strict_tiktok_error_code(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    raw_code = payload.get("code")
    if raw_code in (None, ""):
        raw_code = payload.get("error_code")
    if isinstance(raw_code, bool):
        return None
    if isinstance(raw_code, int):
        code = str(raw_code)
    elif isinstance(raw_code, str):
        code = raw_code
    else:
        return None
    if not code.isascii() or not code.isdigit() or not 1 <= len(code) <= 12:
        return None
    return code


@_sanitize_refresh_boundary(
    operation="TikTok Creator token refresh",
    method="POST",
    endpoint_url=f"{TIKTOK_DEFAULT_API_BASE_URL}{TIKTOK_TOKEN_REFRESH_PATH}",
)
def refresh_tiktok_creator_token_if_needed(
    session: Session,
    *,
    runtime_name: str,
    force: bool = False,
) -> Optional[dict]:
    """Refresh the TikTok Creator (Open API) access token if close to expiry.

    Uses POST ``/v2/oauth/token/`` on ``open.tiktokapis.com`` with
    ``grant_type=refresh_token`` (not the Shop auth host).
    """
    if not _tiktok_auth_refresh_lock.acquire(blocking=False):
        return None

    try:
        from sqlmodel import select

        auth_row: Optional[TikTokAuth] = session.exec(
            select(TikTokAuth).order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())
        ).first()
        if auth_row is None:
            return None

        app_key = (settings.tiktok_app_key or auth_row.app_key or "").strip()
        app_secret = (settings.tiktok_app_secret or "").strip()
        creator_refresh = (auth_row.creator_refresh_token or "").strip()
        if not app_key or not app_secret or not creator_refresh:
            return None

        creator_expires = auth_row.creator_token_expires_at
        if creator_expires is not None and creator_expires.tzinfo is None:
            creator_expires = creator_expires.replace(tzinfo=timezone.utc)
        should_refresh = (
            force
            or not auth_row.creator_access_token
            or not creator_expires
            or creator_expires <= utcnow() + timedelta(minutes=10)
        )
        if not should_refresh:
            return None

        token_url = build_tiktok_api_url(
            api_base_url=TIKTOK_DEFAULT_API_BASE_URL,
            path=TIKTOK_TOKEN_REFRESH_PATH,
        )
        form = {
            "client_key": app_key,
            "client_secret": app_secret,
            "grant_type": "refresh_token",
            "refresh_token": creator_refresh,
        }

        with httpx.Client(timeout=40.0, follow_redirects=False) as client:
            response = client.post(
                token_url,
                data=form,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            response_payload = response.json()
            try:
                body = validate_tiktok_api_response(response_payload)
            except TikTokIngestError as validation_error:
                error_code = _strict_tiktok_error_code(response_payload)
                if error_code is not None:
                    validation_error.tiktok_error_code = error_code  # type: ignore[attr-defined]
                if isinstance(response_payload, (dict, list)):
                    response_payload.clear()
                del response_payload
                raise

        access_token = _parse_oauth_token_field(body, "access_token", "accessToken")
        if not access_token:
            raise RuntimeError("Creator token refresh: missing access_token in response")

        new_refresh = _parse_oauth_token_field(body, "refresh_token", "refreshToken")
        expires_in_seconds = _parse_expires_in_seconds(body)
        if response_payload is not body and isinstance(response_payload, (dict, list)):
            response_payload.clear()
        body.clear()
        del response_payload
        del body
        if new_refresh:
            auth_row.creator_refresh_token = new_refresh

        auth_row.creator_access_token = access_token
        auth_row.creator_token_expires_at = utcnow() + timedelta(seconds=expires_in_seconds)
        auth_row.updated_at = utcnow()
        session.add(auth_row)
        session.commit()

        return {
            "status": "creator_refreshed",
            "runtime": runtime_name,
            "expires_at": auth_row.creator_token_expires_at.isoformat()
            if auth_row.creator_token_expires_at
            else None,
        }
    finally:
        _tiktok_auth_refresh_lock.release()
