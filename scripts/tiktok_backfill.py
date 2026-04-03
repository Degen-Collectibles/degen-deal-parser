from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
from sqlmodel import Session, select

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from app.db import init_db, managed_session  # noqa: E402
from app.models import TikTokAuth, TikTokOrder, utcnow  # noqa: E402
from app.runtime_logging import structured_log_line  # noqa: E402
from app.tiktok_ingest import (  # noqa: E402
    TIKTOK_DEFAULT_API_BASE_URL,
    TIKTOK_SHOP_AUTH_BASE_URL,
    TIKTOK_SHOP_TOKEN_GET_PATH,
    TIKTOK_SHOP_TOKEN_REFRESH_PATH,
    TIKTOK_TOKEN_GET_PATH,
    TIKTOK_TOKEN_REFRESH_PATH,
    TikTokIngestError,
    exchange_tiktok_authorization_code,
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
DEFAULT_SHOP_API_BASE_URL = "https://open-api.tiktokglobalshop.com"


@dataclass
class TikTokPullSummary:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed: int = 0
    detail_calls: int = 0
    auth_updated: int = 0


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


def raise_for_tiktok_error(payload: Any, *, path: str) -> None:
    if not isinstance(payload, dict):
        return
    code = payload.get("code")
    if code in (0, "0", None):
        return
    message = payload.get("message") or payload.get("msg") or "TikTok API error"
    raise RuntimeError(f"{path}: {code} {message}")


def request_json(
    client: httpx.Client,
    *,
    method: str,
    url: str,
    json_body: Optional[dict[str, Any]] = None,
    form_body: Optional[dict[str, Any]] = None,
    raw_body: Optional[str] = None,
    extra_headers: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    request_kwargs: dict[str, Any] = {}
    headers: dict[str, str] = dict(extra_headers) if extra_headers else {}
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
    response = client.request(method, url, **request_kwargs)
    if not response.is_success:
        body_text = response.text[:2000] if response.text else "(empty)"
        print(f"[tiktok_backfill] HTTP {response.status_code} response body: {body_text}", file=sys.stderr)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected TikTok response payload type: {type(payload).__name__}")
    return payload


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
    payload = request_json(client, method="GET", url=url)
    raise_for_tiktok_error(payload, path=SHOP_TOKEN_REFRESH_PATH)
    return payload


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Optional[Any]:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


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

    created_at = parse_datetime(
        _first_present(payload, ("create_time", "created_time", "created_at", "order_create_time"))
    )
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
    subtotal_price = money_to_float(
        _first_present(payment_info, ("sub_total",))
        or _first_present(payload, ("subtotal_price", "goods_amount", "item_amount", "sub_total"))
    )
    if subtotal_price == 0.0 and tax_value is not None:
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
            _first_present(payload, ("payment_status", "financial_status", "pay_status", "order_status")) or ""
        ).strip(),
        "fulfillment_status": str(_first_present(payload, ("fulfillment_status", "shipping_status")) or "").strip() or None,
        "order_status": str(_first_present(payload, ("order_status", "status")) or "").strip() or None,
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
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


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
        "raw_payload": json_dumps(payload),
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
    runtime_name: str = "tiktok_backfill",
) -> TikTokPullSummary:
    summary = TikTokPullSummary()
    if limit == 0:
        return summary

    remaining = limit if limit and limit > 0 else None
    cursor: Optional[str] = None
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
                break

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
                        session.commit()
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
                    api_base_url=base_url,
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

    print(
        "TikTok backfill summary: "
        f"fetched={summary.fetched}, "
        f"inserted={summary.inserted}, "
        f"updated={summary.updated}, "
        f"failed={summary.failed}, "
        f"detail_calls={summary.detail_calls}, "
        f"dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
