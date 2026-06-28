from __future__ import annotations

import argparse
import hashlib
import re
import hmac
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NoReturn, Optional
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
from sqlmodel import Session, select

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from app.db import init_db, is_sqlite_lock_error, managed_session  # noqa: E402
from app.models import TikTokAuth, TikTokCreatorAuth, TikTokOrder, TikTokProduct, utcnow  # noqa: E402
from app.runtime_logging import structured_log_line  # noqa: E402
from app.tiktok.tiktok_ingest import (  # noqa: E402
    TIKTOK_DEFAULT_API_BASE_URL,
    TIKTOK_SHOP_AUTH_BASE_URL,
    TIKTOK_SHOP_TOKEN_GET_PATH,
    TIKTOK_SHOP_TOKEN_REFRESH_PATH,
    TIKTOK_TOKEN_GET_PATH,
    TIKTOK_TOKEN_REFRESH_PATH,
    exchange_tiktok_authorization_code,
    sanitize_tiktok_token_payload,
    structured_tiktok_log_line,
)

DEFAULT_BASE_URL = TIKTOK_DEFAULT_API_BASE_URL
TOKEN_GET_PATH = TIKTOK_TOKEN_GET_PATH
TOKEN_REFRESH_PATH = TIKTOK_TOKEN_REFRESH_PATH
SHOP_AUTH_BASE_URL = TIKTOK_SHOP_AUTH_BASE_URL
SHOP_TOKEN_GET_PATH = TIKTOK_SHOP_TOKEN_GET_PATH
SHOP_TOKEN_REFRESH_PATH = TIKTOK_SHOP_TOKEN_REFRESH_PATH
TIKTOK_API_VERSION = "202309"
ORDER_SEARCH_PATH = f"/order/{TIKTOK_API_VERSION}/orders/search"
ORDER_DETAIL_PATH = f"/order/{TIKTOK_API_VERSION}/orders"
AFFILIATE_SELLER_ORDER_SEARCH_PATH = "/affiliate_seller/202410/orders/search"
AFFILIATE_CREATOR_ORDER_SEARCH_PATH = "/affiliate_creator/202410/orders/search"
TIKTOK_AFFILIATE_ORDER_READ_SCOPE = "seller.affiliate_collaboration.read"
TIKTOK_CREATOR_AFFILIATE_ORDER_READ_SCOPE = "creator.affiliate_collaboration.read"
PRODUCT_SEARCH_PATH = f"/product/{TIKTOK_API_VERSION}/products/search"
PRODUCT_CREATE_PATH = f"/product/{TIKTOK_API_VERSION}/products"
PRODUCT_DETAIL_PATH = f"/product/{TIKTOK_API_VERSION}/products"
PRODUCT_EDIT_PATH = f"/product/{TIKTOK_API_VERSION}/products"
IMAGE_UPLOAD_PATH = f"/product/{TIKTOK_API_VERSION}/images/upload"
CATEGORIES_PATH = f"/product/{TIKTOK_API_VERSION}/categories"
CATEGORY_ATTRIBUTES_PATH = f"/product/{TIKTOK_API_VERSION}/categories"
BRANDS_PATH = f"/product/{TIKTOK_API_VERSION}/brands"
LIVE_ANALYTICS_PATH = "/analytics/202509/shop_lives/overview_performance"
LIVE_CORE_STATS_PATH_TEMPLATE = "/analytics/202502/live_rooms/{live_room_id}/core_stats"
LIVE_SESSION_LIST_PATH = "/analytics/202509/shop_lives/performance"
LIVE_PER_MINUTES_PATH_TEMPLATE = "/analytics/202510/shop_lives/{live_id}/performance_per_minutes"
LIVE_PRODUCTS_PERFORMANCE_PATH_TEMPLATE = "/analytics/202512/shop/{live_id}/products_performance"
DEFAULT_SHOP_API_BASE_URL = "https://open-api.tiktokglobalshop.com"
MAX_TIKTOK_RETRY_AFTER_SECONDS = 60.0


@dataclass
class TikTokPullSummary:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed: int = 0
    detail_calls: int = 0
    auth_updated: int = 0
    affiliate_attributed: int = 0
    affiliate_missing: int = 0
    affiliate_failed: int = 0
    affiliate_scope_missing: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill TikTok Shop orders into tiktok_orders.")
    parser.add_argument("--since", type=str, default=None, help="Only fetch orders created on or after this ISO datetime/date.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of orders to fetch.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize orders without storing them.")
    parser.add_argument("--shop-id", type=str, default=None, help="Override TIKTOK_SHOP_ID for this run.")
    parser.add_argument("--shop-cipher", type=str, default=None, help="Override TIKTOK_SHOP_CIPHER for this run.")
    parser.add_argument("--access-token", type=str, default=None, help="Override TIKTOK_ACCESS_TOKEN for this run.")
    parser.add_argument("--refresh-token", type=str, default=None, help="Refresh the access token before pulling orders.")
    parser.add_argument("--auth-code", type=str, default=None, help="Exchange an authorization code for tokens before pulling orders.")
    parser.add_argument("--products", action="store_true", help="Sync TikTok Shop product catalog instead of orders.")
    return parser.parse_args()


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def resolve_shop_api_base_url() -> str:
    explicit_shop_api_base = optional_env("TIKTOK_SHOP_API_BASE_URL")
    if explicit_shop_api_base:
        return explicit_shop_api_base
    generic_base = optional_env("TIKTOK_BASE_URL")
    if generic_base and "open-api" in generic_base:
        return generic_base
    return DEFAULT_SHOP_API_BASE_URL


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"Invalid ISO datetime/date: {value!r}") from exc

    if len(text) == 10:
        parsed = parsed.replace(hour=0, minute=0, second=0, microsecond=0)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_epoch_seconds(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    return int(value.timestamp())


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def money_to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return utcnow()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.fromtimestamp(int(float(text)), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                return utcnow()

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def token_expiry_at(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        seconds = int(float(value))
    except (TypeError, ValueError):
        return parse_datetime(value)
    return utcnow() + timedelta(seconds=seconds)


def normalize_tiktok_line_items(line_items: Any) -> list[dict[str, Any]]:
    if isinstance(line_items, dict):
        line_items = [line_items]
    if not isinstance(line_items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in line_items:
        if not isinstance(item, dict):
            continue
        title = str(
            item.get("product_name")
            or item.get("title")
            or item.get("item_name")
            or item.get("sku_name")
            or item.get("name")
            or ""
        ).strip()
        if not title:
            continue

        quantity_raw = item.get("quantity") or item.get("sku_quantity") or item.get("count")
        try:
            quantity = int(quantity_raw or 0)
        except (TypeError, ValueError):
            quantity = 0

        normalized.append(
            {
                "title": title,
                "quantity": quantity if quantity > 0 else 1,
                "sku": str(item.get("sku") or item.get("seller_sku") or "").strip() or None,
                "product_id": str(item.get("product_id") or item.get("item_id") or "").strip() or None,
                "variant_id": str(item.get("sku_id") or item.get("variant_id") or "").strip() or None,
                "unit_price": money_to_float(
                    item.get("sale_price") or item.get("sku_sale_price") or item.get("price") or item.get("unit_price")
                ),
                "sku_image": str(item.get("sku_image") or item.get("product_image") or item.get("image_url") or "").strip() or None,
            }
        )
    return normalized


def extract_first_order_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    candidate_keys = (
        "order_list",
        "orders",
        "list",
        "orderList",
        "data",
        "result",
    )
    for key in candidate_keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = extract_first_order_list(value)
            if nested:
                return nested
    return []


def extract_order_ids(payload: Any) -> list[str]:
    ids: list[str] = []
    for order in extract_first_order_list(payload):
        order_id = str(
            order.get("order_id")
            or order.get("orderId")
            or order.get("id")
            or order.get("order_no")
            or ""
        ).strip()
        if order_id:
            ids.append(order_id)
    return ids


def extract_next_cursor(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in (
        "next_cursor",
        "nextCursor",
        "next_page_token",
        "nextPageToken",
        "cursor",
        "page_token",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    data = payload.get("data")
    if isinstance(data, dict):
        return extract_next_cursor(data)
    return None


def extract_tiktok_data(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return {}


SIGN_EXCLUDED_PARAMS = frozenset({"sign", "access_token"})


def build_tiktok_sign(*, path: str, query_params: dict[str, Any], body: str, app_secret: str) -> str:
    canonical_items = []
    for key in sorted(query_params.keys()):
        if key in SIGN_EXCLUDED_PARAMS:
            continue
        value = query_params[key]
        if value in (None, ""):
            continue
        canonical_items.append(f"{key}{value}")
    string_to_sign = f"{app_secret}{path}{''.join(canonical_items)}{body}{app_secret}"
    digest = hmac.new(app_secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest.lower()


def build_tiktok_request(
    *,
    base_url: str,
    path: str,
    app_key: str,
    app_secret: str,
    shop_id: str,
    shop_cipher: str,
    access_token: str,
    body: Optional[dict[str, Any]],
    extra_query: Optional[dict[str, Any]] = None,
    api_version: str = TIKTOK_API_VERSION,
) -> tuple[str, str, dict[str, str]]:
    """Build a signed v2 TikTok Shop API request.

    Returns (full_url, body_json, headers).
    """
    query_params: dict[str, Any] = {
        "app_key": app_key,
        "timestamp": int(time.time()),
    }
    if shop_id:
        query_params["shop_id"] = shop_id
    if shop_cipher:
        query_params["shop_cipher"] = shop_cipher
    if api_version:
        query_params["version"] = api_version
    if extra_query:
        for k, v in extra_query.items():
            if v not in (None, ""):
                query_params[k] = v
    # TikTok Shop signs GET detail requests with an empty body, while POST
    # search requests include the serialized JSON payload in the signature.
    body_json = json_dumps(body) if body is not None else ""
    query_params["sign"] = build_tiktok_sign(
        path=path,
        query_params=query_params,
        body=body_json,
        app_secret=app_secret,
    )
    query_params["access_token"] = access_token
    url = f"{base_url.rstrip('/')}{path}"
    headers = {"x-tts-access-token": access_token}
    return f"{url}?{urlencode(query_params)}", body_json, headers


def _normalized_sensitive_values(values: Optional[tuple[str, ...]]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in (values or ()) if str(value)}, key=len, reverse=True))


_CREDENTIAL_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "api_token",
        "app_secret",
        "auth_code",
        "authorization",
        "authorization_code",
        "client_secret",
        "refresh_token",
        "token",
        "x_tts_access_token",
    }
)


def _normalized_credential_name(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _credential_values_from_container(value: Any) -> tuple[str, ...]:
    found: list[str] = []

    def collect_sensitive(item: Any) -> None:
        if isinstance(item, dict):
            for nested in item.values():
                collect_sensitive(nested)
        elif isinstance(item, (list, tuple, set, frozenset)):
            for nested in item:
                collect_sensitive(nested)
        elif item not in (None, ""):
            found.append(str(item))

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                if _normalized_credential_name(key) in _CREDENTIAL_NAMES:
                    collect_sensitive(nested)
                else:
                    visit(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)

    visit(value)
    return tuple(found)


def _request_sensitive_values(
    *,
    url: str,
    json_body: Optional[dict[str, Any]],
    form_body: Optional[dict[str, Any]],
    raw_body: Optional[str],
    extra_headers: Optional[dict[str, str]],
    sensitive_values: Optional[tuple[str, ...]],
) -> tuple[str, ...]:
    found: list[str] = list(sensitive_values or ())
    for name, value in httpx.URL(url).params.multi_items():
        if _normalized_credential_name(name) in _CREDENTIAL_NAMES and value:
            found.append(str(value))
    for name, value in (extra_headers or {}).items():
        normalized_name = _normalized_credential_name(name)
        if normalized_name not in _CREDENTIAL_NAMES or not value:
            continue
        text = str(value)
        found.append(text)
        if normalized_name == "authorization":
            _scheme, separator, credential = text.partition(" ")
            if separator and credential:
                found.append(credential.strip())
    found.extend(_credential_values_from_container(json_body))
    found.extend(_credential_values_from_container(form_body))
    if raw_body:
        try:
            parsed_body = json.loads(raw_body)
        except (TypeError, ValueError):
            parsed_body = None
        found.extend(_credential_values_from_container(parsed_body))
    return _normalized_sensitive_values(tuple(found))


def _safe_error_code(value: Any, *, sensitive_values: tuple[str, ...] = ()) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text or len(text) > 64 or re.fullmatch(r"[A-Za-z0-9_.-]+", text) is None:
        return None
    if any(secret in text for secret in sensitive_values):
        return None
    return text


def _tiktok_error_payload_scope_missing(payload: dict[str, Any]) -> bool:
    code = str(payload.get("code") or payload.get("error") or "").strip()
    if code == "105005":
        return True
    message = str(payload.get("message") or payload.get("msg") or "").lower()
    return any(
        marker in message
        for marker in (
            "access denied",
            "required access scope",
            "not authorized to access the endpoint",
        )
    )


def raise_for_tiktok_error(
    payload: Any,
    *,
    path: str,
    sensitive_values: Optional[tuple[str, ...]] = None,
) -> None:
    if not isinstance(payload, dict):
        return
    code = payload.get("code")
    if code in (0, "0", None):
        return
    known_secrets = _normalized_sensitive_values(sensitive_values)
    scope_missing = _tiktok_error_payload_scope_missing(payload)
    safe_code = _safe_error_code(code, sensitive_values=known_secrets)
    if not known_secrets and safe_code is not None and not safe_code.isdigit():
        safe_code = None
    suffix = f" code {safe_code}" if safe_code else ""
    safe_error = RuntimeError(f"{path}: TikTok API error{suffix}")
    safe_error.tiktok_error_code = safe_code  # type: ignore[attr-defined]
    safe_error.tiktok_scope_missing = scope_missing  # type: ignore[attr-defined]
    del payload
    del sensitive_values
    del known_secrets
    del code
    _raise_sanitized_request_error(safe_error)


def tiktok_affiliate_order_error_is_scope_missing(exc: BaseException) -> bool:
    if bool(getattr(exc, "tiktok_scope_missing", False)):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    text = str(exc).lower()
    if status_code in (401, 403):
        return True
    return (
        "105005" in text
        or "access denied" in text
        or "required access scope" in text
        or "not authorized to access the endpoint" in text
    )


def redact_http_log_text(
    text: str,
    max_len: int = 500,
    *,
    sensitive_values: Optional[tuple[str, ...]] = None,
) -> str:
    """Truncate and redact tokens from text logged on HTTP errors."""
    if not text:
        return "(empty)"
    redacted = text
    for secret in _normalized_sensitive_values(sensitive_values):
        redacted = redacted.replace(secret, "[REDACTED]")
    redacted = re.sub(
        r"(access_token|refresh_token|auth_code|app_secret|client_secret|Authorization)[=:]\s*[^\s&\"']+",
        r"\1=REDACTED",
        redacted,
        flags=re.IGNORECASE,
    )
    if len(redacted) > max_len:
        return redacted[:max_len] + "…"
    return redacted


def _safe_request_for_diagnostics(*, method: str, url: str | httpx.URL) -> httpx.Request:
    parsed = httpx.URL(url)
    safe_url = parsed.copy_with(query=None, fragment=None)
    return httpx.Request(method.upper(), safe_url)


def _safe_response_error_code(
    response: httpx.Response,
    *,
    sensitive_values: tuple[str, ...],
) -> Optional[str]:
    try:
        payload = response.json()
    except (ValueError, UnicodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return _safe_error_code(
        payload.get("code") if payload.get("code") not in (None, "") else payload.get("error"),
        sensitive_values=sensitive_values,
    )


def _sanitized_http_status_error(
    response: httpx.Response,
    *,
    method: str,
    url: str,
    sensitive_values: tuple[str, ...],
) -> httpx.HTTPStatusError:
    safe_request = _safe_request_for_diagnostics(method=method, url=url)
    safe_response = httpx.Response(response.status_code, request=safe_request)
    safe_code = _safe_response_error_code(response, sensitive_values=sensitive_values)
    code_suffix = f" code={safe_code}" if safe_code else ""
    exc = httpx.HTTPStatusError(
        f"TikTok HTTP {response.status_code}{code_suffix} for "
        f"{safe_request.method} {safe_request.url.host}{safe_request.url.path}",
        request=safe_request,
        response=safe_response,
    )
    exc.tiktok_error_code = safe_code  # type: ignore[attr-defined]
    return exc


def _sanitized_transport_error(
    exc: httpx.TransportError,
    *,
    method: str,
    url: str,
) -> httpx.TransportError:
    safe_request = _safe_request_for_diagnostics(method=method, url=url)
    message = (
        f"TikTok transport error ({type(exc).__name__}) for "
        f"{safe_request.method} {safe_request.url.host}{safe_request.url.path}"
    )
    try:
        return type(exc)(message, request=safe_request)
    except TypeError:
        return httpx.TransportError(message, request=safe_request)


def _sanitized_request_construction_error(
    exc: Exception,
    *,
    method: str,
    url: str,
) -> RuntimeError:
    message = f"TikTok request construction error ({type(exc).__name__})"
    try:
        safe_request = _safe_request_for_diagnostics(method=method, url=url)
    except Exception:
        return RuntimeError(message)
    return RuntimeError(
        f"{message} for {safe_request.method} "
        f"{safe_request.url.host}{safe_request.url.path}"
    )


def _sanitized_unexpected_request_error(
    exc: Exception,
    *,
    method: str,
    url: str,
) -> RuntimeError:
    message = f"TikTok request error ({type(exc).__name__})"
    try:
        safe_request = _safe_request_for_diagnostics(method=method, url=url)
    except Exception:
        return RuntimeError(message)
    return RuntimeError(
        f"{message} for {safe_request.method} "
        f"{safe_request.url.host}{safe_request.url.path}"
    )


def _raise_sanitized_request_error(error: BaseException) -> NoReturn:
    raise error from None


def request_json(
    client: httpx.Client,
    *,
    method: str,
    url: str,
    json_body: Optional[dict[str, Any]] = None,
    form_body: Optional[dict[str, Any]] = None,
    raw_body: Optional[str] = None,
    extra_headers: Optional[dict[str, str]] = None,
    sensitive_values: Optional[tuple[str, ...]] = None,
) -> dict[str, Any]:
    known_secrets: tuple[str, ...] = ()
    request_kwargs: dict[str, Any] = {}
    headers: dict[str, str] = {}
    setup_error: Optional[RuntimeError] = None
    try:
        known_secrets = _request_sensitive_values(
            url=url,
            json_body=json_body,
            form_body=form_body,
            raw_body=raw_body,
            extra_headers=extra_headers,
            sensitive_values=sensitive_values,
        )
        headers = dict(extra_headers) if extra_headers else {}
        if raw_body is not None:
            headers.setdefault("Content-Type", "application/json")
            request_kwargs["content"] = raw_body.encode("utf-8")
        elif form_body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            request_kwargs["data"] = form_body
        elif json_body is not None:
            request_kwargs["json"] = json_body
        if headers:
            request_kwargs["headers"] = headers
    except Exception as exc:
        setup_error = _sanitized_request_construction_error(
            exc,
            method=method,
            url=url,
        )

    if setup_error is not None:
        safe_error = setup_error
        request_kwargs.clear()
        headers.clear()
        del setup_error
        del request_kwargs
        del headers
        del known_secrets
        del client
        del url
        del json_body
        del form_body
        del raw_body
        del extra_headers
        del sensitive_values
        _raise_sanitized_request_error(safe_error)

    max_attempts = 3
    backoff = 0.5
    for attempt in range(1, max_attempts + 1):
        sanitized_error: Optional[BaseException] = None
        response: Optional[httpx.Response] = None
        payload: Any = None
        retry_after_hdr: Optional[str] = None
        wait_s = 0.0
        safe_request: Optional[httpx.Request] = None
        try:
            response = client.request(method, url, **request_kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                retry_after_hdr = response.headers.get("Retry-After")
                wait_s = backoff
                if retry_after_hdr:
                    try:
                        retry_after_seconds = float(retry_after_hdr)
                        if math.isfinite(retry_after_seconds):
                            wait_s = max(
                                wait_s,
                                min(
                                    max(retry_after_seconds, 0.0),
                                    MAX_TIKTOK_RETRY_AFTER_SECONDS,
                                ),
                            )
                    except (TypeError, ValueError, OverflowError):
                        pass
                if attempt < max_attempts:
                    time.sleep(wait_s)
                    backoff *= 2
                    continue
            if not response.is_success:
                safe_request = _safe_request_for_diagnostics(method=method, url=url)
                safe_code = _safe_response_error_code(response, sensitive_values=known_secrets)
                print(
                    structured_log_line(
                        runtime="tiktok_backfill",
                        action="tiktok.auth.http_failed",
                        success=False,
                        error="TikTok authentication request failed",
                        error_type="HTTPStatusError",
                        error_code=safe_code,
                        method=safe_request.method,
                        status_code=response.status_code,
                        endpoint_host=safe_request.url.host,
                        endpoint_path=safe_request.url.path,
                    ),
                    file=sys.stderr,
                )
                sanitized_error = _sanitized_http_status_error(
                    response,
                    method=method,
                    url=url,
                    sensitive_values=known_secrets,
                )
            else:
                try:
                    payload = response.json()
                except (ValueError, UnicodeError) as exc:
                    safe_request = _safe_request_for_diagnostics(method=method, url=url)
                    sanitized_error = RuntimeError(
                        f"TikTok response decode error ({type(exc).__name__}) for "
                        f"{safe_request.method} {safe_request.url.host}{safe_request.url.path}"
                    )
                if sanitized_error is None:
                    if not isinstance(payload, dict):
                        safe_request = _safe_request_for_diagnostics(method=method, url=url)
                        sanitized_error = RuntimeError(
                            f"Unexpected TikTok response payload type: {type(payload).__name__} for "
                            f"{safe_request.method} {safe_request.url.host}{safe_request.url.path}"
                        )
                    else:
                        return payload
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt < max_attempts:
                try:
                    time.sleep(backoff)
                except Exception as retry_exc:
                    sanitized_error = _sanitized_unexpected_request_error(
                        retry_exc,
                        method=method,
                        url=url,
                    )
                else:
                    backoff *= 2
                    continue
            if sanitized_error is None:
                sanitized_error = _sanitized_transport_error(exc, method=method, url=url)
        except (TypeError, ValueError, UnicodeError) as exc:
            sanitized_error = _sanitized_request_construction_error(
                exc,
                method=method,
                url=url,
            )
        except Exception as exc:
            sanitized_error = _sanitized_unexpected_request_error(
                exc,
                method=method,
                url=url,
            )
        if sanitized_error is not None:
            safe_error = sanitized_error
            if isinstance(payload, (dict, list)):
                payload.clear()
            request_kwargs.clear()
            headers.clear()
            retry_after_hdr = None
            wait_s = 0.0
            del sanitized_error
            del payload
            del response
            del request_kwargs
            del headers
            del known_secrets
            del retry_after_hdr
            del wait_s
            del safe_request
            del client
            del url
            del json_body
            del form_body
            del raw_body
            del extra_headers
            del sensitive_values
            _raise_sanitized_request_error(safe_error)
    raise RuntimeError("request_json: exhausted retries without response")


def exchange_authorized_code(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    auth_code: str,
) -> dict[str, Any]:
    params = {
        "app_key": app_key,
        "app_secret": app_secret,
        "auth_code": auth_code,
        "grant_type": "authorized_code",
    }
    url = f"{SHOP_AUTH_BASE_URL}{SHOP_TOKEN_GET_PATH}?{urlencode(params)}"
    payload = request_json(client, method="GET", url=url)
    raise_for_tiktok_error(payload, path=SHOP_TOKEN_GET_PATH)
    return payload


def refresh_access_token(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    refresh_token: str,
) -> dict[str, Any]:
    params = {
        "app_key": app_key,
        "app_secret": app_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    url = f"{SHOP_AUTH_BASE_URL}{SHOP_TOKEN_REFRESH_PATH}?{urlencode(params)}"
    sensitive_values = (app_secret, refresh_token)
    payload = request_json(
        client,
        method="GET",
        url=url,
        sensitive_values=sensitive_values,
    )
    raise_for_tiktok_error(
        payload,
        path=SHOP_TOKEN_REFRESH_PATH,
        sensitive_values=sensitive_values,
    )
    return payload


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Optional[Any]:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean_affiliate_creator_username(value: Any) -> Optional[str]:
    text = str(value or "").strip().lstrip("@").lower()
    return text or None


def _unique_nonempty(values: list[Any], *, transform=str) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        if value in (None, ""):
            continue
        text = transform(value)
        if text in (None, ""):
            continue
        text = str(text).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def _affiliate_sku_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    skus = payload.get("skus") or payload.get("sku_list") or payload.get("line_items") or []
    if isinstance(skus, dict):
        return [skus]
    if not isinstance(skus, list):
        return []
    return [item for item in skus if isinstance(item, dict)]


def affiliate_order_attribution_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    order_id = str(
        _first_present(payload, ("id", "order_id", "orderId", "order_no", "order_number"))
        or ""
    ).strip()
    if not order_id:
        return {}

    skus = _affiliate_sku_list(payload)
    creators = _unique_nonempty(
        [sku.get("creator_username") or sku.get("creator_name") for sku in skus],
        transform=lambda value: _clean_affiliate_creator_username(value) or "",
    )
    content_types = _unique_nonempty(
        [sku.get("content_type") for sku in skus],
        transform=lambda value: str(value or "").strip().upper(),
    )
    content_ids = _unique_nonempty([sku.get("content_id") for sku in skus])
    attribution_types = _unique_nonempty([sku.get("attribution_type") for sku in skus])

    if not (creators or content_types or content_ids or attribution_types):
        return {}

    return {
        "tiktok_order_id": order_id,
        "affiliate_creator_username": creators[0] if creators else None,
        "affiliate_content_type": content_types[0] if content_types else None,
        "affiliate_content_id": content_ids[0] if content_ids else None,
        "affiliate_attribution_json": json_dumps(
            {
                "creator_usernames": creators,
                "content_types": content_types,
                "content_ids": content_ids,
                "attribution_types": attribution_types,
                "order": payload,
            }
        ),
    }


_TIKTOK_CREATED_AT_ALIASES = ("create_time", "created_time", "created_at", "order_create_time")


def order_record_from_payload(
    payload: dict[str, Any],
    *,
    shop_id: Optional[str],
    shop_cipher: Optional[str],
    source: str,
) -> dict[str, Any]:
    order_id = str(
        _first_present(payload, ("order_id", "orderId", "id", "order_no", "order_number"))
        or ""
    ).strip()
    if not order_id:
        raise ValueError("TikTok payload is missing order id")

    created_at = parse_datetime(_first_present(payload, _TIKTOK_CREATED_AT_ALIASES))
    updated_at = parse_datetime(
        _first_present(payload, ("update_time", "updated_time", "updated_at", "order_update_time"))
        or created_at
    )

    payment_info = payload.get("payment") or payload.get("payment_info") or {}
    if not isinstance(payment_info, dict):
        payment_info = {}

    total_price = money_to_float(
        _first_present(payment_info, ("total_amount", "sub_total"))
        or _first_present(
            payload,
            ("total_amount", "payment_amount", "pay_amount", "total_price", "actual_amount", "order_amount"),
        )
    )
    total_tax_raw = (
        _first_present(payment_info, ("tax", "taxes"))
        or _first_present(payload, ("tax_amount", "total_tax", "tax", "vat_amount"))
    )
    tax_value = money_to_float(total_tax_raw) if total_tax_raw is not None else None
    subtotal_price_raw = _first_present(payment_info, ("sub_total",)) or _first_present(
        payload, ("subtotal_price", "goods_amount", "item_amount", "sub_total")
    )
    subtotal_price = money_to_float(subtotal_price_raw)
    if subtotal_price_raw is None and tax_value is not None:
        subtotal_price = round(max(total_price - tax_value, 0.0), 2)
    subtotal_ex_tax = round(total_price - tax_value, 2) if tax_value is not None else None

    line_items = (
        payload.get("line_items")
        or payload.get("item_list")
        or payload.get("sku_list")
        or payload.get("order_line_items")
        or payload.get("items")
        or []
    )
    normalized_line_items = normalize_tiktok_line_items(line_items)
    affiliate_attribution = affiliate_order_attribution_from_payload(payload)

    return {
        "tiktok_order_id": order_id,
        "shop_id": str(_first_present(payload, ("shop_id", "shopId")) or shop_id or "").strip() or None,
        "shop_cipher": str(_first_present(payload, ("shop_cipher", "shopCipher")) or shop_cipher or "").strip() or None,
        "order_number": str(
            _first_present(payload, ("order_number", "order_no", "order_sn", "order_id", "id")) or order_id
        ).strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "customer_name": str(
            _first_present(
                payload,
                ("buyer_nickname", "buyer_name", "recipient_name", "consignee_name", "customer_name", "shipping_name"),
            )
            or (payload.get("recipient_address") or {}).get("name")
            or ""
        ).strip()
        or None,
        "customer_email": str(_first_present(payload, ("buyer_email", "customer_email", "email")) or "").strip() or None,
        "total_price": total_price,
        "subtotal_price": subtotal_price,
        "total_tax": tax_value,
        "subtotal_ex_tax": subtotal_ex_tax,
        "financial_status": str(
            _first_present(payload, ("payment_status", "financial_status", "pay_status")) or ""
        ).strip(),
        "fulfillment_status": str(_first_present(payload, ("fulfillment_status", "shipping_status")) or "").strip() or None,
        "order_status": str(_first_present(payload, ("order_status", "status")) or "").strip() or None,
        "affiliate_creator_username": affiliate_attribution.get("affiliate_creator_username"),
        "affiliate_content_type": affiliate_attribution.get("affiliate_content_type"),
        "affiliate_content_id": affiliate_attribution.get("affiliate_content_id"),
        "affiliate_attribution_json": affiliate_attribution.get("affiliate_attribution_json") or "{}",
        "line_items_json": json_dumps(line_items),
        "line_items_summary_json": json_dumps(normalized_line_items),
        "raw_payload": json_dumps(payload),
        "source": source,
        "received_at": utcnow(),
    }


def upsert_tiktok_order(
    session: Session,
    payload: dict[str, Any],
    *,
    shop_id: Optional[str],
    shop_cipher: Optional[str],
    source: str,
    dry_run: bool = False,
) -> str:
    record = order_record_from_payload(payload, shop_id=shop_id, shop_cipher=shop_cipher, source=source)
    existing = session.exec(
        select(TikTokOrder).where(TikTokOrder.tiktok_order_id == record["tiktok_order_id"])
    ).first()

    if existing is None:
        if not dry_run:
            session.add(TikTokOrder(**record))
        return "inserted"

    for field_name, value in record.items():
        if (
            field_name == "created_at"
            and existing.created_at is not None
            and _first_present(payload, _TIKTOK_CREATED_AT_ALIASES) is None
        ):
            continue
        # Thin webhooks/list payloads must not blank already-enriched fields.
        # Numeric money fields are tricky because missing payload values parse as
        # 0.0, but a literal 0.00 is a valid correction. Only treat 0.0 as blank
        # when the source payload did not explicitly include a field that feeds
        # the stored column.
        if field_name in _TIKTOK_ENRICHABLE_FIELDS:
            missing_in_payload = not _tiktok_payload_has_enrichable_field(payload, field_name)
            existing_value = getattr(existing, field_name, None)
            if missing_in_payload and not _tiktok_value_is_blank(field_name, existing_value, missing_in_payload=False):
                continue
            if _tiktok_value_is_blank(field_name, value, missing_in_payload=missing_in_payload):
                if not _tiktok_value_is_blank(field_name, existing_value, missing_in_payload=False):
                    continue
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


_TIKTOK_ENRICHABLE_FIELDS = (
    "shop_id",
    "shop_cipher",
    "order_number",
    "customer_name",
    "customer_email",
    "currency",
    "financial_status",
    "fulfillment_status",
    "order_status",
    "total_price",
    "subtotal_price",
    "total_tax",
    "subtotal_ex_tax",
    "affiliate_creator_username",
    "affiliate_content_type",
    "affiliate_content_id",
    "affiliate_attribution_json",
    "line_items_json",
    "line_items_summary_json",
)

_TIKTOK_EMPTY_LINE_ITEMS = {"[]", "", None}

_TIKTOK_PAYLOAD_FIELD_ALIASES = {
    "shop_id": ("shop_id", "shopId"),
    "shop_cipher": ("shop_cipher", "shopCipher"),
    "order_number": ("order_number", "order_no", "order_sn", "id"),
    "customer_name": ("buyer_nickname", "buyer_name", "recipient_name", "consignee_name", "customer_name", "shipping_name"),
    "customer_email": ("buyer_email", "customer_email", "email"),
    "currency": ("currency", "currency_code"),
    "financial_status": ("payment_status", "financial_status", "pay_status"),
    "fulfillment_status": ("fulfillment_status", "shipping_status"),
    "order_status": ("order_status", "status"),
    "affiliate_creator_username": ("affiliate_creator_username", "creator_username", "creator_name", "skus"),
    "affiliate_content_type": ("affiliate_content_type", "content_type", "skus"),
    "affiliate_content_id": ("affiliate_content_id", "content_id", "skus"),
    "affiliate_attribution_json": ("affiliate_attribution_json", "skus"),
    "total_price": ("total_amount", "payment_amount", "pay_amount", "total_price", "actual_amount", "order_amount"),
    "subtotal_price": ("subtotal_price", "goods_amount", "item_amount", "sub_total"),
    "total_tax": ("tax_amount", "total_tax", "tax", "vat_amount"),
    "subtotal_ex_tax": ("total_amount", "tax_amount", "total_tax", "tax", "vat_amount"),
    "line_items_json": ("line_items", "item_list", "sku_list", "order_line_items", "items"),
    "line_items_summary_json": ("line_items", "item_list", "sku_list", "order_line_items", "items"),
}

MAX_CONSECUTIVE_EMPTY_CURSOR_PAGES = 5


def _tiktok_payload_has_enrichable_field(payload: dict[str, Any], field_name: str) -> bool:
    aliases = _TIKTOK_PAYLOAD_FIELD_ALIASES.get(field_name, (field_name,))
    if field_name == "total_price":
        payment_info = payload.get("payment") or payload.get("payment_info") or {}
        if isinstance(payment_info, dict) and _first_present(payment_info, ("total_amount", "sub_total")) is not None:
            return True
    if field_name == "subtotal_price":
        payment_info = payload.get("payment") or payload.get("payment_info") or {}
        if isinstance(payment_info, dict) and _first_present(payment_info, ("sub_total",)) is not None:
            return True
    if field_name == "subtotal_ex_tax":
        payment_info = payload.get("payment") or payload.get("payment_info") or {}
        if isinstance(payment_info, dict) and _first_present(payment_info, ("total_amount",)) is not None and _first_present(payment_info, ("tax", "taxes")) is not None:
            return True
    if field_name == "total_tax":
        payment_info = payload.get("payment") or payload.get("payment_info") or {}
        if isinstance(payment_info, dict) and _first_present(payment_info, ("tax", "taxes")) is not None:
            return True
    return _first_present(payload, aliases) is not None


def _tiktok_value_is_blank(field_name: str, value: Any, *, missing_in_payload: bool = False) -> bool:
    """True if the incoming payload value is effectively missing/empty."""
    if value is None:
        return True
    if field_name in ("line_items_json", "line_items_summary_json"):
        return value in _TIKTOK_EMPTY_LINE_ITEMS
    if field_name in ("total_price", "subtotal_price", "total_tax", "subtotal_ex_tax"):
        return value in (None, "") or (missing_in_payload and value == 0.0)
    if isinstance(value, str):
        return value.strip() == ""
    return False


def upsert_tiktok_auth(
    session: Session,
    payload: dict[str, Any],
    *,
    app_key: str,
    shop_id: str,
    shop_cipher: Optional[str] = None,
    source: str,
    dry_run: bool = False,
) -> str:
    data = extract_tiktok_data(payload)
    record = {
        "tiktok_shop_id": str(
            _first_present(data, ("shop_id", "shopId", "shop_cipher", "shopCipher"))
            or shop_id
            or shop_cipher
            or ""
        ).strip(),
        "shop_cipher": str(_first_present(data, ("shop_cipher", "shopCipher")) or shop_cipher or "").strip() or None,
        "shop_name": str(_first_present(data, ("shop_name", "shopName", "shop_name_en")) or "").strip() or None,
        "shop_region": str(_first_present(data, ("shop_region", "region")) or "").strip() or None,
        "seller_name": str(_first_present(data, ("seller_name", "user_name", "seller")) or "").strip() or None,
        "app_key": app_key,
        "access_token": str(_first_present(data, ("access_token", "accessToken")) or "").strip() or None,
        "refresh_token": str(_first_present(data, ("refresh_token", "refreshToken")) or "").strip() or None,
        "access_token_expires_at": token_expiry_at(
            _first_present(data, ("access_token_expire_in", "access_token_expires_in", "expires_in"))
        ),
        "refresh_token_expires_at": token_expiry_at(
            _first_present(data, ("refresh_token_expire_in", "refresh_token_expires_in"))
        ),
        "scopes_json": json_dumps(
            _first_present(data, ("scopes", "scope", "granted_scopes")) or []
        ),
        "raw_payload": json_dumps(sanitize_tiktok_token_payload(payload)),
        "source": source,
        "created_at": utcnow(),
        "updated_at": utcnow(),
    }
    if not record["tiktok_shop_id"]:
        raise ValueError("TikTok auth response did not include a shop id")

    existing = session.exec(
        select(TikTokAuth).where(TikTokAuth.tiktok_shop_id == record["tiktok_shop_id"])
    ).first()

    if existing is None:
        if not dry_run:
            session.add(TikTokAuth(**record))
        return "inserted"

    for field_name, value in record.items():
        if field_name == "created_at":
            continue
        setattr(existing, field_name, value)
    existing.updated_at = utcnow()
    if not dry_run:
        session.add(existing)
    return "updated"


def fetch_tiktok_order_list_page(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    since: Optional[datetime],
    until: Optional[datetime],
    page_size: int,
    cursor: Optional[str] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    extra_query: dict[str, Any] = {
        "page_size": str(max(1, min(page_size, 50))),
    }
    if cursor:
        extra_query["page_token"] = cursor
    body: dict[str, Any] = {}
    # TikTok Shop order search documents create_time filters for this endpoint.
    # Refund/cancel updates are picked up by webhooks and detail enrichment; do
    # not send unsupported update_time_* fields here.
    if since:
        body["create_time_ge"] = to_epoch_seconds(since)
    if until:
        body["create_time_lt"] = to_epoch_seconds(until)

    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=ORDER_SEARCH_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=body,
        extra_query=extra_query,
    )
    payload = request_json(client, method="POST", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=ORDER_SEARCH_PATH)
    orders = extract_first_order_list(payload)
    return payload, orders


def fetch_tiktok_order_details(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    order_ids: list[str],
) -> list[dict[str, Any]]:
    if not order_ids:
        return []

    extra_query = {"ids": ",".join(order_ids)}
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=ORDER_DETAIL_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
    )
    payload = request_json(client, method="GET", url=url, extra_headers=headers)
    raise_for_tiktok_error(payload, path=ORDER_DETAIL_PATH)
    orders = extract_first_order_list(payload)
    if orders:
        return orders
    data = extract_tiktok_data(payload)
    for key in ("order_list", "orders"):
        candidate = data.get(key)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def fetch_tiktok_seller_affiliate_orders_page(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    since: Optional[datetime],
    until: Optional[datetime],
    page_size: int = 100,
    cursor: Optional[str] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    extra_query: dict[str, Any] = {
        "page_size": str(max(1, min(page_size, 100))),
    }
    if cursor:
        extra_query["page_token"] = cursor
    body: dict[str, Any] = {}
    if since:
        body["create_time_ge"] = to_epoch_seconds(since)
    if until:
        body["create_time_lt"] = to_epoch_seconds(until)

    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=AFFILIATE_SELLER_ORDER_SEARCH_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=body,
        extra_query=extra_query,
        api_version="",
    )
    payload = request_json(client, method="POST", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=AFFILIATE_SELLER_ORDER_SEARCH_PATH)
    data = extract_tiktok_data(payload)
    orders = data.get("orders") if isinstance(data, dict) else []
    if not isinstance(orders, list):
        orders = extract_first_order_list(payload)
    return payload, [item for item in orders if isinstance(item, dict)]


def fetch_tiktok_creator_affiliate_orders_page(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    since: Optional[datetime],
    until: Optional[datetime],
    page_size: int = 100,
    cursor: Optional[str] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    extra_query: dict[str, Any] = {
        "page_size": str(max(1, min(page_size, 100))),
    }
    if cursor:
        extra_query["page_token"] = cursor
    body: dict[str, Any] = {}
    if since:
        body["create_time_ge"] = to_epoch_seconds(since)
    if until:
        body["create_time_lt"] = to_epoch_seconds(until)

    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=AFFILIATE_CREATOR_ORDER_SEARCH_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher="",
        access_token=access_token,
        body=body,
        extra_query=extra_query,
        api_version="",
    )
    payload = request_json(client, method="POST", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=AFFILIATE_CREATOR_ORDER_SEARCH_PATH)
    data = extract_tiktok_data(payload)
    orders = data.get("orders") if isinstance(data, dict) else []
    if not isinstance(orders, list):
        orders = extract_first_order_list(payload)
    return payload, [item for item in orders if isinstance(item, dict)]


def _creator_affiliate_payload_with_creator(
    payload: dict[str, Any],
    *,
    creator_username: str,
) -> dict[str, Any]:
    enriched = dict(payload)
    creator = _clean_affiliate_creator_username(creator_username)
    skus = _affiliate_sku_list(enriched)
    if not skus:
        skus = [
            {
                "product_id": _first_present(enriched, ("product_id", "productId")),
                "content_type": _first_present(enriched, ("content_type", "contentType")),
                "content_id": _first_present(enriched, ("content_id", "contentId")),
            }
        ]
    enriched_skus: list[dict[str, Any]] = []
    for sku in skus:
        item = dict(sku)
        item.setdefault("creator_username", creator)
        if "content_type" not in item:
            item["content_type"] = _first_present(enriched, ("content_type", "contentType"))
        if "content_id" not in item:
            item["content_id"] = _first_present(enriched, ("content_id", "contentId"))
        enriched_skus.append(item)
    enriched["skus"] = enriched_skus
    return enriched


def upsert_tiktok_order_affiliate_attribution(
    session: Session,
    payload: dict[str, Any],
    *,
    dry_run: bool = False,
) -> str:
    attribution = affiliate_order_attribution_from_payload(payload)
    order_id = str(attribution.get("tiktok_order_id") or "").strip()
    if not order_id:
        return "skipped"
    existing = session.exec(
        select(TikTokOrder).where(TikTokOrder.tiktok_order_id == order_id)
    ).first()
    if existing is None:
        return "missing"
    changed = False
    for field_name in (
        "affiliate_creator_username",
        "affiliate_content_type",
        "affiliate_content_id",
        "affiliate_attribution_json",
    ):
        value = attribution.get(field_name)
        if field_name == "affiliate_attribution_json":
            value = value or "{}"
        if value in (None, ""):
            continue
        if getattr(existing, field_name, None) != value:
            setattr(existing, field_name, value)
            changed = True
    if changed and not dry_run:
        session.add(existing)
    return "updated" if changed else "unchanged"


def upsert_tiktok_order_creator_affiliate_attribution(
    session: Session,
    payload: dict[str, Any],
    *,
    creator_username: str,
    dry_run: bool = False,
) -> str:
    enriched = _creator_affiliate_payload_with_creator(payload, creator_username=creator_username)
    return upsert_tiktok_order_affiliate_attribution(session, enriched, dry_run=dry_run)


def _try_live_core_stats(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    creator_access_token: str,
    shop_cipher: str,
    live_room_id: str,
    currency: str = "USD",
) -> dict[str, Any] | None:
    """Try the real-time live_core_stats endpoint (Creator auth, needs live_room_id).
    Returns None if missing params, 403, or endpoint unavailable.
    """
    if not creator_access_token or not live_room_id:
        return None
    path = LIVE_CORE_STATS_PATH_TEMPLATE.format(live_room_id=live_room_id)
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=creator_access_token,
        body=None,
        extra_query={"currency": currency} if currency else None,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    resp = client.request("GET", url, headers=headers)
    try:
        payload = resp.json()
    except Exception:
        return None
    if resp.status_code == 403 or (isinstance(payload, dict) and payload.get("code") == 40006):
        return None
    if not resp.is_success:
        return None
    code = payload.get("code") if isinstance(payload, dict) else None
    if code not in (0, "0"):
        return None
    data = payload.get("data") or {}
    return {
        "source": "live_core_stats",
        "realtime": True,
        "raw": data,
    }


def _parse_live_core_stats(raw: dict, currency: str = "USD") -> dict[str, Any]:
    """Best-effort extraction from live_core_stats response shape."""
    gmv_obj = raw.get("gmv") or raw.get("live_gmv") or {}
    try:
        gmv = float(gmv_obj.get("amount") or gmv_obj.get("value") or 0)
    except (TypeError, ValueError):
        gmv = 0.0
    return {
        "gmv": gmv,
        "currency": gmv_obj.get("currency") or currency,
        "items_sold": int(raw.get("items_sold") or raw.get("total_items_sold") or 0),
        "sku_orders": int(raw.get("sku_orders") or raw.get("total_orders") or 0),
        "customers": int(raw.get("customers") or raw.get("total_buyers") or 0),
        "source": "live_core_stats",
        "realtime": True,
    }


def fetch_tiktok_live_analytics(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    currency: str = "USD",
    creator_access_token: str = "",
    live_room_id: str = "",
) -> dict[str, Any]:
    """Fetch LIVE stream analytics.

    Strategy:
      1. Try live_core_stats (real-time, needs Creator token + live_room_id)
      2. Fall back to overview_performance (delayed ~2 days)
    """
    core = _try_live_core_stats(
        client,
        base_url=base_url,
        app_key=app_key,
        app_secret=app_secret,
        creator_access_token=creator_access_token,
        shop_cipher=shop_cipher,
        live_room_id=live_room_id,
        currency=currency,
    )
    if core is not None:
        return _parse_live_core_stats(core["raw"], currency=currency)

    now = datetime.now(timezone.utc)
    start_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    extra_query: dict[str, Any] = {
        "start_date_ge": start_str,
        "end_date_lt": end_str,
        "granularity": "1D",
        "currency": currency,
    }
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=LIVE_ANALYTICS_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    resp = client.request("GET", url, headers=headers)
    try:
        payload = resp.json()
    except Exception:
        payload = None
    if not resp.is_success and payload is None:
        raise RuntimeError(
            f"{LIVE_ANALYTICS_PATH}: HTTP {resp.status_code} — {(resp.text or '')[:500]}"
        )
    if isinstance(payload, dict):
        code = payload.get("code")
        if code in (66007001, "66007001"):
            return {"gmv": 0.0, "items_sold": 0, "sku_orders": 0, "customers": 0,
                    "currency": currency, "rpc_error": True}
    if not resp.is_success:
        raise RuntimeError(
            f"{LIVE_ANALYTICS_PATH}: HTTP {resp.status_code} — {(resp.text or '')[:500]}"
        )
    raise_for_tiktok_error(payload, path=LIVE_ANALYTICS_PATH)
    data = extract_tiktok_data(payload)

    latest_date = data.get("latest_available_date") or ""
    perf = data.get("performance") or {}
    intervals = perf.get("intervals") or []

    best: dict[str, Any] | None = None
    for iv in intervals:
        gmv_obj = iv.get("gmv") or {}
        try:
            amt = float(gmv_obj.get("amount") or 0)
        except (TypeError, ValueError):
            amt = 0.0
        items = int(iv.get("items_sold") or 0)
        if amt > 0 or items > 0:
            candidate = {
                "gmv": amt,
                "currency": gmv_obj.get("currency") or currency,
                "items_sold": items,
                "sku_orders": int(iv.get("sku_orders") or 0),
                "customers": int(iv.get("customers") or 0),
                "date": iv.get("start_date") or "",
            }
            if best is None or (candidate.get("date") or "") > (best.get("date") or ""):
                best = candidate

    if best is None:
        return {"gmv": 0.0, "items_sold": 0, "sku_orders": 0, "customers": 0,
                "currency": currency, "source": "overview_delayed",
                "latest_available_date": latest_date}

    best["source"] = "overview_delayed"
    best["latest_available_date"] = latest_date
    best["realtime"] = False
    return best


def fetch_live_session_list(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    start_date: str,
    end_date: str,
    currency: str = "USD",
) -> list[dict[str, Any]]:
    """Fetch a list of LIVE stream sessions from the Performance List endpoint.

    Returns a list of dicts with keys:
        id, title, username, start_time, end_time, gmv, items_sold, customers, sku_orders
    Returns an empty list on any failure (403, rpc_error, etc.).
    """
    extra_query: dict[str, Any] = {
        "start_date_ge": start_date,
        "end_date_lt": end_date,
        "sort_field": "gmv",
        "sort_order": "DESC",
        "currency": currency,
        "page_size": 20,
    }
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=LIVE_SESSION_LIST_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    try:
        resp = client.request("GET", url, headers=headers)
    except Exception:
        return []
    try:
        payload = resp.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    if resp.status_code == 403 or payload.get("code") not in (0, "0"):
        return []

    data = payload.get("data") or {}
    sessions_raw = data.get("live_stream_sessions") or []
    results: list[dict[str, Any]] = []
    for s in sessions_raw:
        if not isinstance(s, dict):
            continue
        gmv_obj = (s.get("sales_performance") or {}).get("gmv") or {}
        try:
            gmv = float(gmv_obj.get("amount") or 0)
        except (TypeError, ValueError):
            gmv = 0.0
        sp = s.get("sales_performance") or {}
        try:
            start_ts = int(s.get("start_time") or 0)
        except (TypeError, ValueError):
            start_ts = 0
        try:
            end_ts = int(s.get("end_time") or 0)
        except (TypeError, ValueError):
            end_ts = 0
        avg_price_obj = sp.get("avg_price") or {}
        try:
            avg_price = float(avg_price_obj.get("amount") or 0)
        except (TypeError, ValueError):
            avg_price = 0.0
        results.append({
            "id": str(s.get("id") or ""),
            "title": str(s.get("title") or ""),
            "username": str(s.get("username") or ""),
            "start_time": start_ts,
            "end_time": end_ts,
            "gmv": gmv,
            "currency": str(gmv_obj.get("currency") or currency),
            "avg_price": avg_price,
            "items_sold": int(sp.get("items_sold") or 0),
            "customers": int(sp.get("customers") or 0),
            "created_sku_orders": int(sp.get("created_sku_orders") or 0),
            "sku_orders": int(sp.get("sku_orders") or 0),
            "different_products_sold": int(sp.get("different_products_sold") or 0),
            "products_added": int(sp.get("products_added") or 0),
        })
    return results


def fetch_overview_performance_daily(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    start_date: str,
    end_date: str,
    currency: str = "USD",
) -> list[dict[str, Any]]:
    """Fetch daily LIVE performance intervals from overview_performance with granularity=1D.

    Returns a list of dicts: {date, gmv, sku_orders, customers, items_sold,
    click_to_order_rate, click_through_rate}. Empty list on failure.
    """
    extra_query: dict[str, Any] = {
        "start_date_ge": start_date,
        "end_date_lt": end_date,
        "granularity": "1D",
        "currency": currency,
    }
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=LIVE_ANALYTICS_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    try:
        resp = client.request("GET", url, headers=headers)
    except Exception:
        return []
    try:
        payload = resp.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    if resp.status_code == 403 or payload.get("code") not in (0, "0"):
        return []

    data = payload.get("data") or {}
    perf = data.get("performance") or {}
    intervals = perf.get("intervals") or []
    results: list[dict[str, Any]] = []
    for iv in intervals:
        gmv_obj = iv.get("gmv") or {}
        try:
            amt = float(gmv_obj.get("amount") or 0)
        except (TypeError, ValueError):
            amt = 0.0
        results.append({
            "date": iv.get("start_date") or "",
            "gmv": amt,
            "currency": gmv_obj.get("currency") or currency,
            "sku_orders": int(iv.get("sku_orders") or 0),
            "customers": int(iv.get("customers") or 0),
            "items_sold": int(iv.get("items_sold") or 0),
            "click_to_order_rate": str(iv.get("click_to_order_rate") or ""),
            "click_through_rate": str(iv.get("click_through_rate") or ""),
        })
    return results


def fetch_stream_performance_per_minutes(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    live_id: str,
    currency: str = "USD",
) -> Optional[dict[str, Any]]:
    """Fetch per-minute performance for a single ended LIVE stream.

    Returns {overall: {start_time, end_time, duration, gmv}, intervals: [...]}
    or None on failure. Only works after a stream has ended.
    """
    path = LIVE_PER_MINUTES_PATH_TEMPLATE.format(live_id=live_id)
    extra_query: dict[str, Any] = {
        "currency": currency,
    }
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    try:
        resp = client.request("GET", url, headers=headers)
    except Exception:
        return None
    try:
        payload = resp.json()
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if resp.status_code == 403 or payload.get("code") not in (0, "0"):
        return None

    data = payload.get("data") or {}
    overall_raw = data.get("overall") or {}
    gmv_obj = overall_raw.get("gmv") or {}
    try:
        overall_gmv = float(gmv_obj.get("amount") or 0)
    except (TypeError, ValueError):
        overall_gmv = 0.0
    overall = {
        "start_time": int(overall_raw.get("start_time") or 0),
        "end_time": int(overall_raw.get("end_time") or 0),
        "duration": int(overall_raw.get("duration") or 0),
        "gmv": overall_gmv,
        "currency": gmv_obj.get("currency") or currency,
    }

    intervals_raw = data.get("intervals") or []
    intervals: list[dict[str, Any]] = []
    for iv in intervals_raw:
        iv_gmv_obj = iv.get("gmv") or {}
        try:
            iv_gmv = float(iv_gmv_obj.get("amount") or 0)
        except (TypeError, ValueError):
            iv_gmv = 0.0
        intervals.append({
            "timestamp": int(iv.get("timestamp") or 0),
            "gmv": iv_gmv,
            "sku_orders": int(iv.get("sku_orders") or 0),
            "customers": int(iv.get("customers") or 0),
            "items_sold": int(iv.get("items_sold") or 0),
        })

    return {"overall": overall, "intervals": intervals}


def fetch_live_product_performance_list(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    live_id: str,
    currency: str = "USD",
) -> list[dict[str, Any]]:
    """Fetch product-level sales attributed directly to one LIVE session.

    This endpoint is useful when multiple creators stream from the same shop
    at once: order payloads are shop-level, but this product performance data
    is scoped to the selected live_id.
    """
    live_id = str(live_id or "").strip()
    if not live_id:
        return []
    path = LIVE_PRODUCTS_PERFORMANCE_PATH_TEMPLATE.format(live_id=live_id)
    extra_query: dict[str, Any] = {
        "currency": currency,
        "page_size": 50,
    }
    url, _body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id="",
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
        api_version="",
    )
    headers["Content-Type"] = "application/json"
    try:
        resp = client.request("GET", url, headers=headers)
    except Exception:
        return []
    try:
        payload = resp.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    if resp.status_code == 403 or payload.get("code") not in (0, "0"):
        return []

    data = payload.get("data") or {}
    products_raw = data.get("products") or []
    if not isinstance(products_raw, list):
        return []

    results: list[dict[str, Any]] = []
    for product in products_raw:
        if not isinstance(product, dict):
            continue
        product_id = str(product.get("id") or product.get("product_id") or "").strip()
        if not product_id:
            continue
        sales = product.get("sales") or {}
        direct_gmv_obj = sales.get("direct_gmv") or {}
        try:
            direct_gmv = float(direct_gmv_obj.get("amount") or 0)
        except (TypeError, ValueError):
            direct_gmv = 0.0
        results.append({
            "id": product_id,
            "name": str(product.get("name") or product.get("title") or "").strip(),
            "direct_gmv": direct_gmv,
            "currency": str(direct_gmv_obj.get("currency") or currency),
            "items_sold": int(sales.get("items_sold") or 0),
            "customers": int(sales.get("customers") or 0),
            "created_sku_orders": int(sales.get("created_sku_orders") or 0),
            "sku_orders": int(sales.get("sku_orders") or 0),
            "main_orders": int(sales.get("main_orders") or 0),
        })
    return results


def fetch_tiktok_product_list_page(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    page_size: int,
    cursor: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    extra_query: dict[str, Any] = {
        "page_size": str(max(1, min(page_size, 50))),
    }
    if cursor:
        extra_query["page_token"] = cursor
    body: dict[str, Any] = {}
    if status_filter:
        body["status"] = status_filter

    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=PRODUCT_SEARCH_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=body,
        extra_query=extra_query,
    )
    payload = request_json(client, method="POST", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=PRODUCT_SEARCH_PATH)
    data = extract_tiktok_data(payload)
    products = data.get("products") or []
    if not isinstance(products, list):
        products = extract_first_order_list(payload)
    return payload, [p for p in products if isinstance(p, dict)]


def product_record_from_payload(
    payload: dict[str, Any],
    *,
    shop_id: Optional[str],
    shop_cipher: Optional[str],
    source: str,
) -> dict[str, Any]:
    product_id = str(
        _first_present(payload, ("id", "product_id"))
        or ""
    ).strip()
    if not product_id:
        raise ValueError("TikTok payload is missing product id")

    images = payload.get("main_images") or payload.get("images") or []
    if not isinstance(images, list):
        images = []
    image_urls = []
    for img in images:
        if isinstance(img, dict):
            thumb_urls = img.get("thumb_urls") or img.get("urls") or []
            if isinstance(thumb_urls, list) and thumb_urls:
                url = str(thumb_urls[0]).strip()
            else:
                url = str(img.get("url") or img.get("uri") or img.get("thumb_url") or "").strip()
            if url and url.startswith("http"):
                image_urls.append(url)
            elif url:
                image_urls.append(f"https://p16-oec-general-useast5.ttcdn-us.com/{url}")
        elif isinstance(img, str) and img.strip():
            val = img.strip()
            if val.startswith("http"):
                image_urls.append(val)
            else:
                image_urls.append(f"https://p16-oec-general-useast5.ttcdn-us.com/{val}")
    main_image_url = image_urls[0] if image_urls else None

    category_chains = payload.get("category_chains") or []
    category_id = None
    category_name = None
    if isinstance(category_chains, list) and category_chains:
        last_chain = category_chains[-1] if isinstance(category_chains[-1], dict) else {}
        cat_list = last_chain.get("categories") or last_chain.get("category_list") or []
        if isinstance(cat_list, list) and cat_list:
            leaf = cat_list[-1] if isinstance(cat_list[-1], dict) else {}
            category_id = str(leaf.get("id") or "").strip() or None
            category_name = str(leaf.get("local_name") or leaf.get("name") or "").strip() or None
    if not category_id:
        category_id = str(_first_present(payload, ("category_id",)) or "").strip() or None

    brand = payload.get("brand") or {}
    if not isinstance(brand, dict):
        brand = {}

    raw_skus = payload.get("skus") or payload.get("sku_list") or payload.get("variants") or []
    if not isinstance(raw_skus, list):
        raw_skus = []
    skus = []
    for sku in raw_skus:
        if not isinstance(sku, dict):
            continue
        sku_id = str(_first_present(sku, ("id", "sku_id")) or "").strip()
        seller_sku = str(_first_present(sku, ("seller_sku", "outer_sku_id")) or "").strip() or None
        price_info = sku.get("price") or {}
        if not isinstance(price_info, dict):
            price_info = {}
        price = money_to_float(
            _first_present(price_info, ("sale_price", "original_price", "tax_exclusive_price"))
            or _first_present(sku, ("price", "sale_price", "original_price"))
        )
        inventory_list = sku.get("inventory") or []
        total_inventory = 0
        if isinstance(inventory_list, list):
            for inv in inventory_list:
                if isinstance(inv, dict):
                    total_inventory += int(inv.get("quantity") or 0)
        elif isinstance(inventory_list, (int, float)):
            total_inventory = int(inventory_list)
        sales_attrs = sku.get("sales_attributes") or []
        if not isinstance(sales_attrs, list):
            sales_attrs = []
        skus.append({
            "sku_id": sku_id,
            "seller_sku": seller_sku,
            "price": price,
            "inventory": total_inventory,
            "sales_attributes": [
                {"id": str(a.get("id") or ""), "name": str(a.get("name") or ""),
                 "value_id": str(a.get("value_id") or ""), "value_name": str(a.get("value_name") or "")}
                for a in sales_attrs if isinstance(a, dict)
            ],
        })

    audit = payload.get("audit") or {}
    audit_status = None
    if isinstance(audit, dict):
        audit_status = str(audit.get("status") or "").strip() or None
    if not audit_status:
        audit_status = str(_first_present(payload, ("audit_status",)) or "").strip() or None

    created_at = parse_datetime(
        _first_present(payload, ("create_time", "created_at", "created_time"))
    )
    updated_at = parse_datetime(
        _first_present(payload, ("update_time", "updated_at", "updated_time"))
        or created_at
    )

    return {
        "tiktok_product_id": product_id,
        "shop_id": str(_first_present(payload, ("shop_id", "shopId")) or shop_id or "").strip() or None,
        "shop_cipher": str(_first_present(payload, ("shop_cipher", "shopCipher")) or shop_cipher or "").strip() or None,
        "title": str(_first_present(payload, ("title", "product_name", "name")) or "").strip(),
        "description": str(_first_present(payload, ("description", "product_description")) or "").strip() or None,
        "status": str(_first_present(payload, ("status",)) or "").strip() or None,
        "audit_status": audit_status,
        "category_id": category_id,
        "category_name": category_name,
        "brand_id": str(brand.get("id") or "").strip() or None,
        "brand_name": str(brand.get("name") or "").strip() or None,
        "main_image_url": main_image_url,
        "images_json": json_dumps(image_urls),
        "skus_json": json_dumps(skus),
        "sales_attributes_json": json_dumps(payload.get("sales_attributes") or []),
        "product_attributes_json": json_dumps(payload.get("product_attributes") or []),
        "raw_payload": json_dumps(payload),
        "source": source,
        "created_at": created_at,
        "updated_at": updated_at,
        "synced_at": utcnow(),
    }


def upsert_tiktok_product_row(
    session: Session,
    payload: dict[str, Any],
    *,
    shop_id: Optional[str],
    shop_cipher: Optional[str],
    source: str,
    dry_run: bool = False,
) -> str:
    record = product_record_from_payload(payload, shop_id=shop_id, shop_cipher=shop_cipher, source=source)
    existing = session.exec(
        select(TikTokProduct).where(TikTokProduct.tiktok_product_id == record["tiktok_product_id"])
    ).first()

    if existing is None:
        if not dry_run:
            session.add(TikTokProduct(**record))
        return "inserted"

    for field_name, value in record.items():
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


def backfill_tiktok_products(
    session: Session,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str = "",
    limit: Optional[int] = None,
    dry_run: bool = False,
    runtime_name: str = "tiktok_backfill",
) -> TikTokPullSummary:
    summary = TikTokPullSummary()
    if limit == 0:
        return summary

    remaining = limit if limit and limit > 0 else None
    cursor: Optional[str] = None

    with httpx.Client(timeout=40.0, follow_redirects=True) as client:
        while True:
            page_size = min(50, remaining) if remaining else 50
            payload, products = fetch_tiktok_product_list_page(
                client,
                base_url=base_url,
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                page_size=page_size,
                cursor=cursor,
            )

            if not products:
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.products.page_empty",
                        success=True,
                        dry_run=dry_run,
                    )
                )
                break

            for product_payload in products:
                if remaining is not None and remaining <= 0:
                    break
                summary.fetched += 1
                pid = str(
                    product_payload.get("id") or product_payload.get("product_id") or ""
                ).strip()
                if pid and not dry_run:
                    try:
                        detail = fetch_tiktok_product_detail(
                            client,
                            base_url=base_url,
                            app_key=app_key,
                            app_secret=app_secret,
                            access_token=access_token,
                            shop_id=shop_id,
                            shop_cipher=shop_cipher,
                            product_id=pid,
                        )
                        if isinstance(detail, dict) and detail:
                            product_payload = detail
                    except Exception as detail_exc:
                        print(
                            structured_tiktok_log_line(
                                runtime=runtime_name,
                                action="tiktok.products.detail_fetch_warning",
                                success=False,
                                error=str(detail_exc)[:200],
                                product_id=pid,
                            )
                        )
                try:
                    result = upsert_tiktok_product_row(
                        session,
                        product_payload,
                        shop_id=shop_id,
                        shop_cipher=shop_cipher,
                        source="backfill",
                        dry_run=dry_run,
                    )
                    if result == "inserted":
                        summary.inserted += 1
                    else:
                        summary.updated += 1
                    if not dry_run:
                        _commit_retry_delay = 0.4
                        for _commit_attempt in range(4):
                            try:
                                session.commit()
                                break
                            except Exception as _commit_exc:
                                if is_sqlite_lock_error(_commit_exc) and _commit_attempt < 3:
                                    time.sleep(_commit_retry_delay)
                                    _commit_retry_delay *= 2
                                    continue
                                raise
                    elif session.in_transaction():
                        session.rollback()
                except Exception as exc:
                    summary.failed += 1
                    if session.in_transaction():
                        session.rollback()
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.products.product_failed",
                            success=False,
                            error=str(exc),
                            product_id=product_payload.get("id")
                            or product_payload.get("product_id"),
                            shop_id=shop_id,
                            shop_cipher=shop_cipher or None,
                        )
                    )

                if summary.fetched % 25 == 0:
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.products.progress",
                            success=True,
                            fetched=summary.fetched,
                            inserted=summary.inserted,
                            updated=summary.updated,
                            failed=summary.failed,
                            dry_run=dry_run,
                        )
                    )
                if remaining is not None:
                    remaining -= 1

            cursor = extract_next_cursor(payload)
            if remaining is not None and remaining <= 0:
                break
            if not cursor:
                break

    return summary


def fetch_tiktok_categories(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    keyword: Optional[str] = None,
) -> list[dict[str, Any]]:
    extra_query: dict[str, Any] = {}
    if keyword:
        extra_query["keyword"] = keyword
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=CATEGORIES_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
    )
    payload = request_json(client, method="GET", url=url, extra_headers=headers)
    raise_for_tiktok_error(payload, path=CATEGORIES_PATH)
    data = extract_tiktok_data(payload)
    categories = data.get("categories") or []
    if not isinstance(categories, list):
        return []
    return [c for c in categories if isinstance(c, dict)]


def fetch_tiktok_category_attributes(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    category_id: str,
) -> list[dict[str, Any]]:
    path = f"{CATEGORY_ATTRIBUTES_PATH}/{category_id}/attributes"
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
    )
    payload = request_json(client, method="GET", url=url, extra_headers=headers)
    raise_for_tiktok_error(payload, path=path)
    data = extract_tiktok_data(payload)
    attributes = data.get("attributes") or []
    if not isinstance(attributes, list):
        return []
    return [a for a in attributes if isinstance(a, dict)]


def fetch_tiktok_brands(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    brand_name: Optional[str] = None,
    category_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    extra_query: dict[str, Any] = {}
    if brand_name:
        extra_query["brand_name"] = brand_name
    if category_id:
        extra_query["category_id"] = category_id
    extra_query["page_size"] = "50"
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=BRANDS_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
        extra_query=extra_query,
    )
    payload = request_json(client, method="GET", url=url, extra_headers=headers)
    raise_for_tiktok_error(payload, path=BRANDS_PATH)
    data = extract_tiktok_data(payload)
    brands = data.get("brands") or []
    if not isinstance(brands, list):
        return []
    return [b for b in brands if isinstance(b, dict)]


def upload_tiktok_product_image(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    image_data: bytes,
    file_name: str = "image.jpg",
) -> str:
    query_params: dict[str, Any] = {
        "app_key": app_key,
        "timestamp": int(time.time()),
        "version": TIKTOK_API_VERSION,
    }
    if shop_id:
        query_params["shop_id"] = shop_id
    if shop_cipher:
        query_params["shop_cipher"] = shop_cipher
    query_params["sign"] = build_tiktok_sign(
        path=IMAGE_UPLOAD_PATH,
        query_params=query_params,
        body="",
        app_secret=app_secret,
    )
    query_params["access_token"] = access_token
    url = f"{base_url.rstrip('/')}{IMAGE_UPLOAD_PATH}?{urlencode(query_params)}"
    content_type = "image/jpeg"
    if file_name.lower().endswith(".png"):
        content_type = "image/png"
    elif file_name.lower().endswith(".webp"):
        content_type = "image/webp"
    response = client.post(
        url,
        files={"data": (file_name, image_data, content_type)},
        headers={"x-tts-access-token": access_token},
    )
    response.raise_for_status()
    payload = response.json()
    raise_for_tiktok_error(payload, path=IMAGE_UPLOAD_PATH)
    data = extract_tiktok_data(payload)
    uri = data.get("uri") or data.get("url") or ""
    if not uri:
        raise RuntimeError("TikTok image upload did not return a URI")
    return str(uri).strip()


def create_tiktok_product(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    product_body: dict[str, Any],
) -> dict[str, Any]:
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=PRODUCT_CREATE_PATH,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=product_body,
    )
    payload = request_json(client, method="POST", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=PRODUCT_CREATE_PATH)
    return extract_tiktok_data(payload)


def edit_tiktok_product(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    product_id: str,
    product_body: dict[str, Any],
) -> dict[str, Any]:
    path = f"{PRODUCT_EDIT_PATH}/{product_id}"
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=product_body,
    )
    payload = request_json(client, method="PUT", url=url, raw_body=body_json, extra_headers=headers)
    raise_for_tiktok_error(payload, path=path)
    return extract_tiktok_data(payload)


def fetch_tiktok_product_detail(
    client: httpx.Client,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str,
    product_id: str,
) -> dict[str, Any]:
    path = f"{PRODUCT_DETAIL_PATH}/{product_id}"
    url, body_json, headers = build_tiktok_request(
        base_url=base_url,
        path=path,
        app_key=app_key,
        app_secret=app_secret,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        access_token=access_token,
        body=None,
    )
    payload = request_json(client, method="GET", url=url, extra_headers=headers)
    raise_for_tiktok_error(payload, path=path)
    return extract_tiktok_data(payload)


def backfill_tiktok_order_affiliate_attributions(
    session: Session,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_cipher: str,
    since: Optional[datetime],
    until: Optional[datetime],
    dry_run: bool = False,
    runtime_name: str = "tiktok_backfill",
) -> TikTokPullSummary:
    summary = TikTokPullSummary()
    if not shop_cipher:
        return summary

    cursor: Optional[str] = None
    with httpx.Client(timeout=40.0, follow_redirects=True) as client:
        while True:
            try:
                payload, affiliate_orders = fetch_tiktok_seller_affiliate_orders_page(
                    client,
                    base_url=base_url,
                    app_key=app_key,
                    app_secret=app_secret,
                    access_token=access_token,
                    shop_cipher=shop_cipher,
                    since=since,
                    until=until,
                    page_size=100,
                    cursor=cursor,
                )
            except Exception as exc:
                if tiktok_affiliate_order_error_is_scope_missing(exc):
                    summary.affiliate_scope_missing = True
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.affiliate_orders.scope_missing",
                            success=False,
                            error=str(exc)[:500],
                        )
                    )
                    return summary
                summary.affiliate_failed += 1
                print(
                    structured_tiktok_log_line(
                        runtime=runtime_name,
                        action="tiktok.affiliate_orders.failed",
                        success=False,
                        error=str(exc)[:500],
                    )
                )
                return summary

            for affiliate_order in affiliate_orders:
                result = upsert_tiktok_order_affiliate_attribution(
                    session,
                    affiliate_order,
                    dry_run=dry_run,
                )
                if result in ("updated", "unchanged"):
                    summary.affiliate_attributed += 1
                elif result == "missing":
                    summary.affiliate_missing += 1

            if not dry_run and affiliate_orders:
                _commit_retry_delay = 0.4
                for _commit_attempt in range(4):
                    try:
                        session.commit()
                        break
                    except Exception as _commit_exc:
                        if is_sqlite_lock_error(_commit_exc) and _commit_attempt < 3:
                            time.sleep(_commit_retry_delay)
                            _commit_retry_delay *= 2
                            continue
                        raise
            elif dry_run and session.in_transaction():
                session.rollback()

            cursor = extract_next_cursor(payload)
            if not cursor:
                break
            time.sleep(0.5)

    return summary


def _scope_json_contains(scopes_json: str, scope: str) -> bool:
    try:
        parsed = json.loads(scopes_json or "[]")
    except Exception:
        parsed = scopes_json or ""
    if isinstance(parsed, str):
        scopes = parsed.replace(",", " ").split()
    elif isinstance(parsed, list):
        scopes = [str(item or "") for item in parsed]
    else:
        scopes = []
    return scope in {item.strip() for item in scopes if item and item.strip()}


def backfill_tiktok_creator_affiliate_attributions(
    session: Session,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    since: Optional[datetime],
    until: Optional[datetime],
    dry_run: bool = False,
    runtime_name: str = "tiktok_backfill",
) -> TikTokPullSummary:
    summary = TikTokPullSummary()
    creator_auth_rows = list(
        session.exec(
            select(TikTokCreatorAuth).order_by(TikTokCreatorAuth.updated_at.desc(), TikTokCreatorAuth.id.desc())
        ).all()
    )
    if not creator_auth_rows:
        return summary

    with httpx.Client(timeout=40.0, follow_redirects=True) as client:
        for auth_row in creator_auth_rows:
            creator_username = _clean_affiliate_creator_username(auth_row.creator_username)
            access_token = str(auth_row.access_token or "").strip()
            row_app_key = str(auth_row.app_key or app_key or "").strip()
            if not creator_username or not access_token or not row_app_key:
                continue
            if not _scope_json_contains(auth_row.scopes_json, TIKTOK_CREATOR_AFFILIATE_ORDER_READ_SCOPE):
                summary.affiliate_scope_missing = True
                continue

            cursor: Optional[str] = None
            while True:
                try:
                    payload, affiliate_orders = fetch_tiktok_creator_affiliate_orders_page(
                        client,
                        base_url=base_url,
                        app_key=row_app_key,
                        app_secret=app_secret,
                        access_token=access_token,
                        since=since,
                        until=until,
                        page_size=100,
                        cursor=cursor,
                    )
                except Exception as exc:
                    if tiktok_affiliate_order_error_is_scope_missing(exc):
                        summary.affiliate_scope_missing = True
                        print(
                            structured_tiktok_log_line(
                                runtime=runtime_name,
                                action="tiktok.creator_affiliate_orders.scope_missing",
                                success=False,
                                creator_username=creator_username,
                                error=str(exc)[:500],
                            )
                        )
                        break
                    summary.affiliate_failed += 1
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.creator_affiliate_orders.failed",
                            success=False,
                            creator_username=creator_username,
                            error=str(exc)[:500],
                        )
                    )
                    break

                for affiliate_order in affiliate_orders:
                    result = upsert_tiktok_order_creator_affiliate_attribution(
                        session,
                        affiliate_order,
                        creator_username=creator_username,
                        dry_run=dry_run,
                    )
                    if result in ("updated", "unchanged"):
                        summary.affiliate_attributed += 1
                    elif result == "missing":
                        summary.affiliate_missing += 1

                if not dry_run and affiliate_orders:
                    _commit_retry_delay = 0.4
                    for _commit_attempt in range(4):
                        try:
                            session.commit()
                            break
                        except Exception as _commit_exc:
                            if is_sqlite_lock_error(_commit_exc) and _commit_attempt < 3:
                                time.sleep(_commit_retry_delay)
                                _commit_retry_delay *= 2
                                continue
                            raise
                elif dry_run and session.in_transaction():
                    session.rollback()

                cursor = extract_next_cursor(payload)
                if not cursor:
                    break
                time.sleep(0.5)

    return summary


def backfill_tiktok_orders(
    session: Session,
    *,
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    shop_id: str,
    shop_cipher: str = "",
    since: Optional[datetime] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    affiliate_attribution: bool = True,
    runtime_name: str = "tiktok_backfill",
) -> TikTokPullSummary:
    summary = TikTokPullSummary()
    # limit is "no cap" semantics: None or <=0 means scan exhaustively.
    if limit is not None and limit <= 0:
        limit = None

    remaining = limit if limit and limit > 0 else None
    cursor: Optional[str] = None
    consecutive_empty_cursor_pages = 0
    until = utcnow()

    with httpx.Client(timeout=40.0, follow_redirects=True) as client:
        while True:
            page_size = min(50, remaining) if remaining else 50
            payload, search_orders = fetch_tiktok_order_list_page(
                client,
                base_url=base_url,
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                since=since,
                until=until,
                page_size=page_size,
                cursor=cursor,
            )

            if not search_orders:
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.backfill.page_empty",
                        success=True,
                        dry_run=dry_run,
                    )
                )
                # Empty page: only stop when there is no continuation cursor.
                # TikTok sometimes returns an empty page mid-stream with more
                # pages to follow — keep paging while a cursor is present.
                next_cursor = extract_next_cursor(payload)
                if not next_cursor:
                    break
                if next_cursor == cursor:
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="tiktok.backfill.repeated_empty_cursor",
                            success=False,
                            dry_run=dry_run,
                            cursor=next_cursor,
                        )
                    )
                    break
                consecutive_empty_cursor_pages += 1
                if consecutive_empty_cursor_pages > MAX_CONSECUTIVE_EMPTY_CURSOR_PAGES:
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="tiktok.backfill.empty_cursor_page_limit",
                            success=False,
                            dry_run=dry_run,
                            empty_pages=consecutive_empty_cursor_pages,
                        )
                    )
                    break
                cursor = next_cursor
                continue

            consecutive_empty_cursor_pages = 0

            for order_payload in search_orders:
                if remaining is not None and remaining <= 0:
                    break
                summary.fetched += 1
                try:
                    result = upsert_tiktok_order(
                        session,
                        order_payload,
                        shop_id=shop_id,
                        shop_cipher=shop_cipher,
                        source="backfill",
                        dry_run=dry_run,
                    )
                    if result == "inserted":
                        summary.inserted += 1
                    else:
                        summary.updated += 1
                    if not dry_run:
                        _commit_retry_delay = 0.4
                        for _commit_attempt in range(4):
                            try:
                                session.commit()
                                break
                            except Exception as _commit_exc:
                                if is_sqlite_lock_error(_commit_exc) and _commit_attempt < 3:
                                    time.sleep(_commit_retry_delay)
                                    _commit_retry_delay *= 2
                                    continue
                                raise
                    elif session.in_transaction():
                        session.rollback()
                except Exception as exc:
                    summary.failed += 1
                    if session.in_transaction():
                        session.rollback()
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.backfill.order_failed",
                            success=False,
                            error=str(exc),
                            order_id=order_payload.get("order_id")
                            or order_payload.get("id")
                            or order_payload.get("order_no"),
                            shop_id=shop_id,
                            shop_cipher=shop_cipher or None,
                        )
                    )

                if summary.fetched % 25 == 0:
                    print(
                        structured_tiktok_log_line(
                            runtime=runtime_name,
                            action="tiktok.backfill.progress",
                            success=True,
                            fetched=summary.fetched,
                            inserted=summary.inserted,
                            updated=summary.updated,
                            failed=summary.failed,
                            detail_calls=summary.detail_calls,
                            dry_run=dry_run,
                        )
                    )
                if remaining is not None:
                    remaining -= 1

            cursor = extract_next_cursor(payload)
            if remaining is not None and remaining <= 0:
                break
            if not cursor:
                break
            time.sleep(0.5)

    if affiliate_attribution:
        affiliate_summary = backfill_tiktok_order_affiliate_attributions(
            session,
            base_url=base_url,
            app_key=app_key,
            app_secret=app_secret,
            access_token=access_token,
            shop_cipher=shop_cipher,
            since=since,
            until=until,
            dry_run=dry_run,
            runtime_name=runtime_name,
        )
        summary.affiliate_attributed += affiliate_summary.affiliate_attributed
        summary.affiliate_missing += affiliate_summary.affiliate_missing
        summary.affiliate_failed += affiliate_summary.affiliate_failed
        summary.affiliate_scope_missing = affiliate_summary.affiliate_scope_missing

        creator_affiliate_summary = backfill_tiktok_creator_affiliate_attributions(
            session,
            base_url=base_url,
            app_key=app_key,
            app_secret=app_secret,
            since=since,
            until=until,
            dry_run=dry_run,
            runtime_name=runtime_name,
        )
        summary.affiliate_attributed += creator_affiliate_summary.affiliate_attributed
        summary.affiliate_missing += creator_affiliate_summary.affiliate_missing
        summary.affiliate_failed += creator_affiliate_summary.affiliate_failed

    return summary


def main() -> int:
    args = parse_args()
    app_key = require_env("TIKTOK_APP_KEY")
    app_secret = require_env("TIKTOK_APP_SECRET")
    base_url = resolve_shop_api_base_url()
    configured_shop_id = optional_env("TIKTOK_SHOP_ID").strip()
    configured_shop_cipher = optional_env("TIKTOK_SHOP_CIPHER").strip()
    configured_access_token = optional_env("TIKTOK_ACCESS_TOKEN").strip()
    configured_refresh_token = optional_env("TIKTOK_REFRESH_TOKEN").strip()
    shop_id = (args.shop_id or "").strip()
    shop_cipher = (args.shop_cipher or "").strip()
    access_token = (args.access_token or "").strip()
    refresh_token = (args.refresh_token or "").strip()
    auth_code = (args.auth_code or optional_env("TIKTOK_AUTH_CODE")).strip()
    redirect_uri = (optional_env("TIKTOK_REDIRECT_URI") or "").strip()

    init_db()
    since = parse_iso_datetime(args.since)

    with managed_session() as session:
        saved_auth = None
        auth_stmt = select(TikTokAuth).where(TikTokAuth.app_key == app_key)
        identity_shop_id = shop_id or configured_shop_id
        identity_shop_cipher = shop_cipher or configured_shop_cipher
        if identity_shop_id:
            auth_stmt = auth_stmt.where(
                (TikTokAuth.tiktok_shop_id == identity_shop_id) | (TikTokAuth.shop_cipher == identity_shop_cipher)
            )
        elif identity_shop_cipher:
            auth_stmt = auth_stmt.where(TikTokAuth.shop_cipher == identity_shop_cipher)
        saved_auth = session.exec(auth_stmt.order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())).first()
        if saved_auth is not None:
            if not shop_id:
                candidate_shop_id = str(saved_auth.tiktok_shop_id or "").strip()
                if candidate_shop_id and not candidate_shop_id.startswith("pending:"):
                    shop_id = candidate_shop_id
            shop_cipher = shop_cipher or str(saved_auth.shop_cipher or "").strip() or configured_shop_cipher
            access_token = access_token or str(saved_auth.access_token or "").strip() or configured_access_token
            refresh_token = refresh_token or str(saved_auth.refresh_token or "").strip() or configured_refresh_token
        else:
            shop_id = shop_id or configured_shop_id
            shop_cipher = shop_cipher or configured_shop_cipher
            access_token = access_token or configured_access_token
            refresh_token = refresh_token or configured_refresh_token

        with httpx.Client(timeout=40.0, follow_redirects=True) as client:
            if auth_code:
                token_result = exchange_tiktok_authorization_code(
                    auth_code=auth_code,
                    app_key=app_key,
                    app_secret=app_secret,
                    redirect_uri=redirect_uri,
                    runtime_name="tiktok_backfill",
                )
                access_token = str(token_result.access_token or access_token or "").strip()
                refresh_token = str(token_result.refresh_token or refresh_token or "").strip()
                shop_id = str(token_result.shop_id or shop_id or "").strip()
                shop_cipher = str(token_result.shop_cipher or shop_cipher or "").strip()
                if shop_id:
                    status = upsert_tiktok_auth(
                        session,
                        token_result.raw_payload or {},
                        app_key=app_key,
                        shop_id=shop_id,
                        shop_cipher=shop_cipher or None,
                        source="oauth",
                        dry_run=args.dry_run,
                    )
                    print(
                        structured_tiktok_log_line(
                            runtime="tiktok_backfill",
                            action=f"tiktok.auth.{status}",
                            success=True,
                            shop_id=shop_id,
                            dry_run=args.dry_run,
                        )
                    )
                    if not args.dry_run:
                        session.commit()

            if refresh_token and (not access_token or args.auth_code):
                refreshed = refresh_access_token(
                    client,
                    base_url=base_url,
                    app_key=app_key,
                    app_secret=app_secret,
                    refresh_token=refresh_token,
                )
                data = extract_tiktok_data(refreshed)
                access_token = str(data.get("access_token") or access_token or "").strip()
                refresh_token = str(data.get("refresh_token") or refresh_token or "").strip()
                if shop_id:
                    status = upsert_tiktok_auth(
                        session,
                        refreshed,
                        app_key=app_key,
                        shop_id=shop_id,
                        shop_cipher=shop_cipher or None,
                        source="refresh",
                        dry_run=args.dry_run,
                    )
                    print(
                        structured_tiktok_log_line(
                            runtime="tiktok_backfill",
                            action=f"tiktok.auth.{status}",
                            success=True,
                            shop_id=shop_id,
                            dry_run=args.dry_run,
                        )
                    )
                    if not args.dry_run:
                        session.commit()
                print(
                    structured_tiktok_log_line(
                        runtime="tiktok_backfill",
                        action="tiktok.auth.refreshed",
                        success=True,
                        shop_id=shop_id or None,
                        dry_run=args.dry_run,
                    )
                )

        if not shop_id and not shop_cipher:
            raise SystemExit(
                "Missing required TikTok shop identity. Set TIKTOK_SHOP_ID or TIKTOK_SHOP_CIPHER, "
                "or pass --shop-id / --shop-cipher."
            )
        if not access_token:
            raise SystemExit(
                "Missing TikTok access token. Set TIKTOK_ACCESS_TOKEN, pass --access-token, or exchange a fresh auth code with --auth-code."
            )

        if args.products:
            summary = backfill_tiktok_products(
                session,
                base_url=base_url,
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            label = "TikTok product sync summary"
        else:
            summary = backfill_tiktok_orders(
                session,
                base_url=base_url,
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                since=since,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            label = "TikTok backfill summary"

    print(
        f"{label}: "
        f"fetched={summary.fetched}, "
        f"inserted={summary.inserted}, "
        f"updated={summary.updated}, "
        f"failed={summary.failed}, "
        f"detail_calls={summary.detail_calls}, "
        f"affiliate_attributed={summary.affiliate_attributed}, "
        f"affiliate_missing={summary.affiliate_missing}, "
        f"affiliate_failed={summary.affiliate_failed}, "
        f"affiliate_scope_missing={summary.affiliate_scope_missing}, "
        f"dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
