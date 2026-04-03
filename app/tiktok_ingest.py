from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, MutableMapping, Optional

import httpx
from sqlmodel import Session, select

from .runtime_logging import structured_log_line

TIKTOK_DEFAULT_API_BASE_URL = "https://open.tiktokapis.com"
TIKTOK_TOKEN_GET_PATH = "/v2/oauth/token/"
TIKTOK_TOKEN_REFRESH_PATH = "/v2/oauth/token/"
TIKTOK_TOKEN_REVOKE_PATH = "/v2/oauth/revoke/"

TIKTOK_SHOP_AUTH_BASE_URL = "https://auth.tiktok-shops.com"
TIKTOK_SHOP_TOKEN_GET_PATH = "/api/v2/token/get"
TIKTOK_SHOP_TOKEN_REFRESH_PATH = "/api/v2/token/refresh"
TIKTOK_DEFAULT_TIMEOUT_SECONDS = 20.0
TIKTOK_WEBHOOK_SIGNATURE_HEADERS = (
    "x-tiktok-signature",
    "x-tt-signature",
    "x-signature",
)
TIKTOK_WEBHOOK_TIMESTAMP_HEADERS = (
    "x-tiktok-timestamp",
    "x-tt-timestamp",
    "x-signature-timestamp",
)


class TikTokIngestError(RuntimeError):
    pass


@dataclass
class TikTokTokenExchangeResult:
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token_expires_at: Optional[datetime] = None
    refresh_token_expires_at: Optional[datetime] = None
    seller_id: Optional[str] = None
    shop_id: Optional[str] = None
    shop_cipher: Optional[str] = None
    open_id: Optional[str] = None
    raw_payload: Optional[dict[str, Any]] = None


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def _safe_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _safe_json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not value:
        return []
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return []
        if not isinstance(loaded, list):
            return []
        return [item for item in loaded if isinstance(item, dict)]
    return []


def money_to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        try:
            return round(float(str(value)), 2)
        except (TypeError, ValueError):
            return 0.0


def parse_tiktok_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            # TikTok payloads sometimes expose epoch seconds or milliseconds.
            epoch_value = int(text)
            if epoch_value > 10_000_000_000:
                epoch_value //= 1000
            parsed = datetime.fromtimestamp(epoch_value, tz=timezone.utc)
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pick_first(payload: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = payload
        for part in path.split("."):
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
            if current is None:
                break
        if current not in (None, ""):
            return current
    return None


def build_tiktok_api_url(*, api_base_url: str = TIKTOK_DEFAULT_API_BASE_URL, path: str) -> str:
    normalized_base = (api_base_url or "").strip().rstrip("/")
    normalized_path = (path or "").strip()
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"
    if not normalized_base:
        normalized_base = TIKTOK_DEFAULT_API_BASE_URL
    return f"{normalized_base}{normalized_path}"


def build_tiktok_request_signature(
    params: MutableMapping[str, Any],
    app_secret: str,
    *,
    digest_mode: str = "hex",
) -> str:
    if not app_secret:
        raise TikTokIngestError("TikTok app secret is required to sign requests")

    canonical_parts: list[str] = []
    for key in sorted(params.keys()):
        value = params[key]
        if value in (None, ""):
            continue
        canonical_parts.append(f"{key}{value}")
    canonical_string = "".join(canonical_parts)
    digest = hmac.new(
        app_secret.encode("utf-8"),
        canonical_string.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    if digest_mode == "base64":
        return base64.b64encode(digest).decode("utf-8")
    return digest.hex()


def _coerce_api_error_message(payload: Any, response_text: str | None = None) -> str:
    if isinstance(payload, dict):
        for key in ("error_description", "error"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        for key in ("message", "msg", "error_message", "error", "detail"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        if payload:
            return json_dumps(payload)
    if response_text:
        return response_text.strip()
    return "TikTok API returned an error response"


def extract_tiktok_api_data(response_payload: Any) -> dict[str, Any]:
    payload = _safe_json_obj(response_payload)
    if not payload:
        if isinstance(response_payload, dict):
            payload = response_payload
        else:
            raise TikTokIngestError("TikTok API response was not a JSON object")

    for candidate in ("data", "result", "response"):
        nested = payload.get(candidate)
        if isinstance(nested, dict):
            if nested.get("data") and isinstance(nested["data"], dict):
                return nested["data"]
            return nested

    if payload.get("data") and isinstance(payload["data"], dict):
        return payload["data"]
    return payload


def validate_tiktok_api_response(response_payload: Any) -> dict[str, Any]:
    payload = _safe_json_obj(response_payload)
    if not payload:
        raise TikTokIngestError("TikTok API response was empty or not JSON")

    explicit_error = payload.get("error")
    explicit_error_description = payload.get("error_description")
    if explicit_error not in (None, "", 0, "0", False):
        raise TikTokIngestError(_coerce_api_error_message(payload))
    if explicit_error_description not in (None, ""):
        raise TikTokIngestError(_coerce_api_error_message(payload))

    code = payload.get("code")
    success = payload.get("success")
    if code not in (None, 0, "0") and success is not True:
        raise TikTokIngestError(_coerce_api_error_message(payload))
    if success is False:
        raise TikTokIngestError(_coerce_api_error_message(payload))
    return extract_tiktok_api_data(payload)


def _parse_token_exchange_data(api_data: dict[str, Any]) -> TikTokTokenExchangeResult:
    expires_in_raw = _pick_first(
        api_data,
        "access_token_expire_in",
        "access_token_expires_in",
        "expire_in",
        "expires_in",
        "access_token_expired_in",
    )
    refresh_expires_in_raw = _pick_first(
        api_data,
        "refresh_token_expire_in",
        "refresh_token_expires_in",
        "refresh_expires_in",
        "refresh_expire_in",
    )
    now = datetime.now(timezone.utc)

    return TikTokTokenExchangeResult(
        access_token=str(_pick_first(api_data, "access_token", "accessToken") or "").strip() or None,
        refresh_token=str(_pick_first(api_data, "refresh_token", "refreshToken") or "").strip() or None,
        access_token_expires_at=(
            now + timedelta(seconds=int(expires_in_raw or 0))
            if str(expires_in_raw or "").strip().isdigit() or isinstance(expires_in_raw, (int, float))
            else None
        ),
        refresh_token_expires_at=(
            now + timedelta(seconds=int(refresh_expires_in_raw or 0))
            if str(refresh_expires_in_raw or "").strip().isdigit()
            or isinstance(refresh_expires_in_raw, (int, float))
            else None
        ),
        seller_id=str(_pick_first(api_data, "seller_id", "sellerId", "user_id", "userId") or "").strip() or None,
        shop_id=str(_pick_first(api_data, "shop_id", "shopId", "shop_cipher", "shopCipher") or "").strip() or None,
        shop_cipher=str(_pick_first(api_data, "shop_cipher", "shopCipher") or "").strip() or None,
        open_id=str(_pick_first(api_data, "open_id", "openId") or "").strip() or None,
        raw_payload=api_data,
    )


def exchange_tiktok_authorization_code(
    *,
    auth_code: str,
    app_key: str,
    app_secret: str,
    redirect_uri: str = "",
    api_base_url: str = TIKTOK_SHOP_AUTH_BASE_URL,
    token_path: str = TIKTOK_SHOP_TOKEN_GET_PATH,
    client: Optional[httpx.Client] = None,
    timeout_seconds: float = TIKTOK_DEFAULT_TIMEOUT_SECONDS,
    request_signer: Optional[Callable[[MutableMapping[str, Any]], MutableMapping[str, Any]]] = None,
    runtime_name: str = "tiktok_ingest",
) -> TikTokTokenExchangeResult:
    if not auth_code:
        raise TikTokIngestError("TikTok authorization code is required")
    if not app_key:
        raise TikTokIngestError("TikTok app key is required")
    if not app_secret:
        raise TikTokIngestError("TikTok app secret is required")

    query_params: dict[str, str] = {
        "app_key": app_key,
        "app_secret": app_secret,
        "auth_code": auth_code,
        "grant_type": "authorized_code",
    }

    url = build_tiktok_api_url(api_base_url=api_base_url, path=token_path)
    close_client = client is None
    http_client = client or httpx.Client(timeout=timeout_seconds)
    try:
        response = http_client.get(url, params=query_params)
        response.raise_for_status()
        api_data = validate_tiktok_api_response(response.json())
    except Exception as exc:
        raise TikTokIngestError(f"TikTok token exchange failed: {exc}") from exc
    finally:
        if close_client:
            http_client.close()

    return _parse_token_exchange_data(api_data)


def refresh_tiktok_shop_token(
    *,
    app_key: str,
    app_secret: str,
    refresh_token: str,
    api_base_url: str = TIKTOK_SHOP_AUTH_BASE_URL,
    token_path: str = TIKTOK_SHOP_TOKEN_REFRESH_PATH,
    client: Optional[httpx.Client] = None,
    timeout_seconds: float = TIKTOK_DEFAULT_TIMEOUT_SECONDS,
    runtime_name: str = "tiktok_ingest",
) -> TikTokTokenExchangeResult:
    if not app_key:
        raise TikTokIngestError("TikTok app key is required")
    if not app_secret:
        raise TikTokIngestError("TikTok app secret is required")
    if not refresh_token:
        raise TikTokIngestError("TikTok refresh token is required")

    query_params: dict[str, str] = {
        "app_key": app_key,
        "app_secret": app_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    url = build_tiktok_api_url(api_base_url=api_base_url, path=token_path)
    close_client = client is None
    http_client = client or httpx.Client(timeout=timeout_seconds)
    try:
        response = http_client.get(url, params=query_params)
        response.raise_for_status()
        api_data = validate_tiktok_api_response(response.json())
    except Exception as exc:
        raise TikTokIngestError(f"TikTok token refresh failed: {exc}") from exc
    finally:
        if close_client:
            http_client.close()

    return _parse_token_exchange_data(api_data)


def normalize_tiktok_line_items(value: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in _safe_json_list(value):
        title = str(_pick_first(item, "product_name", "sku_name", "title", "item_name") or "").strip()
        if not title:
            continue
        quantity_raw = _pick_first(item, "quantity", "qty", "order_quantity")
        try:
            quantity = int(quantity_raw or 0)
        except (TypeError, ValueError):
            quantity = 0
        items.append(
            {
                "title": title,
                "quantity": quantity if quantity > 0 else 1,
                "sku": str(_pick_first(item, "sku", "seller_sku", "sku_id") or "").strip() or None,
                "product_id": str(_pick_first(item, "product_id", "item_id") or "").strip() or None,
                "variant_id": str(_pick_first(item, "sku_id", "variant_id") or "").strip() or None,
                "unit_price": money_to_float(
                    _pick_first(item, "price", "sale_price", "item_price", "unit_price")
                ),
                "sku_image": str(_pick_first(item, "sku_image", "product_image", "image_url") or "").strip() or None,
            }
        )
    return items


def _extract_tiktok_order_payload(raw_payload: Any) -> dict[str, Any]:
    payload = _safe_json_obj(raw_payload)
    if not payload:
        raise TikTokIngestError("TikTok order payload must be a JSON object")

    for candidate in ("data", "order", "orders"):
        nested = payload.get(candidate)
        if isinstance(nested, dict):
            return nested

    order_list = payload.get("order_list") or payload.get("list")
    if isinstance(order_list, list) and order_list:
        first = order_list[0]
        if isinstance(first, dict):
            return first

    return payload


def normalize_tiktok_order_payload(
    raw_payload: Any,
    *,
    source: str = "webhook",
    received_at: Optional[datetime] = None,
) -> dict[str, Any]:
    payload = _extract_tiktok_order_payload(raw_payload)
    line_items = payload.get("line_items")
    if line_items in (None, ""):
        line_items = (
            payload.get("sku_list")
            or payload.get("order_line_items")
            or payload.get("items")
            or payload.get("product_list")
            or []
        )
    normalized_line_items = normalize_tiktok_line_items(line_items)
    created_at = parse_tiktok_datetime(
        _pick_first(
            payload,
            "create_time",
            "created_time",
            "created_at",
            "order_create_time",
            "update_time",
        )
    )
    updated_at = parse_tiktok_datetime(
        _pick_first(
            payload,
            "update_time",
            "updated_time",
            "updated_at",
            "modify_time",
            "modified_time",
        )
    ) or created_at or received_at or datetime.now(timezone.utc)
    total_price = money_to_float(
        _pick_first(
            payload,
            "total_price",
            "pay_amount",
            "total_amount",
            "order_amount",
            "payment_amount",
        )
    )
    subtotal_price = money_to_float(
        _pick_first(
            payload,
            "subtotal_price",
            "sub_total",
            "original_amount",
            "items_amount",
        )
    )
    total_tax = _pick_first(payload, "tax_amount", "total_tax", "vat_amount")
    total_tax_value = money_to_float(total_tax) if total_tax not in (None, "") else None
    subtotal_ex_tax = (
        round(subtotal_price - total_tax_value, 2)
        if total_tax_value is not None and subtotal_price
        else None
    )
    financial_status = str(
        _pick_first(
            payload,
            "financial_status",
            "payment_status",
            "order_status",
            "status",
        )
        or ""
    ).strip()
    fulfillment_status = str(
        _pick_first(
            payload,
            "fulfillment_status",
            "shipping_status",
            "logistics_status",
            "package_status",
        )
        or ""
    ).strip() or None
    order_id = _pick_first(
        payload,
        "order_id",
        "id",
        "order_sn",
        "order_number",
        "order_no",
    )
    if order_id in (None, ""):
        raise TikTokIngestError("TikTok order payload is missing an order identifier")

    order_number = str(
        _pick_first(
            payload,
            "order_sn",
            "order_number",
            "order_no",
            "id",
        )
        or order_id
    ).strip()

    customer_name = str(
        _pick_first(
            payload,
            "buyer_name",
            "recipient_name",
            "shipping_address.name",
            "shipping_name",
            "buyer_nickname",
            "customer_name",
        )
        or ""
    ).strip() or None
    customer_email = str(_pick_first(payload, "buyer_email", "email", "contact_email") or "").strip() or None

    return {
        "tiktok_order_id": str(order_id).strip(),
        "order_number": order_number,
        "created_at": created_at or received_at or datetime.now(timezone.utc),
        "updated_at": updated_at,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "total_price": total_price,
        "subtotal_price": subtotal_price,
        "total_tax": total_tax_value,
        "subtotal_ex_tax": subtotal_ex_tax,
        "financial_status": financial_status,
        "fulfillment_status": fulfillment_status,
        "line_items_json": json_dumps(line_items if isinstance(line_items, list) else []),
        "line_items_summary_json": json_dumps(normalized_line_items),
        "raw_payload": json_dumps(payload),
        "source": source,
        "received_at": received_at or datetime.now(timezone.utc),
        "shop_id": str(_pick_first(payload, "shop_id", "shopId") or "").strip() or None,
        "shop_cipher": str(_pick_first(payload, "shop_cipher", "shopCipher") or "").strip() or None,
        "seller_id": str(_pick_first(payload, "seller_id", "sellerId") or "").strip() or None,
        "currency": str(_pick_first(payload, "currency", "currency_code") or "").strip() or None,
    }


def build_tiktok_reconciliation_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    normalized_line_items = normalize_tiktok_line_items(record.get("line_items_json"))
    return {
        "tiktok_order_id": record.get("tiktok_order_id"),
        "order_number": record.get("order_number"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "customer_name": record.get("customer_name"),
        "customer_email": record.get("customer_email"),
        "financial_status": record.get("financial_status"),
        "fulfillment_status": record.get("fulfillment_status"),
        "total_price": record.get("total_price"),
        "subtotal_price": record.get("subtotal_price"),
        "total_tax": record.get("total_tax"),
        "line_item_count": len(normalized_line_items),
        "line_items": normalized_line_items,
    }


def build_tiktok_auth_record(
    token_result: TikTokTokenExchangeResult | dict[str, Any],
    *,
    app_key: str,
    redirect_uri: str,
    fallback_shop_id: Optional[str] = None,
    pending_key_seed: Optional[str] = None,
    source: str = "oauth_callback",
    received_at: Optional[datetime] = None,
) -> dict[str, Any]:
    if isinstance(token_result, dict):
        token_result = TikTokTokenExchangeResult(
            access_token=str(_pick_first(token_result, "access_token", "accessToken") or "").strip() or None,
            refresh_token=str(_pick_first(token_result, "refresh_token", "refreshToken") or "").strip() or None,
            access_token_expires_at=parse_tiktok_datetime(
                _pick_first(token_result, "access_token_expires_at", "accessTokenExpiresAt")
            ),
            refresh_token_expires_at=parse_tiktok_datetime(
                _pick_first(token_result, "refresh_token_expires_at", "refreshTokenExpiresAt")
            ),
            seller_id=str(_pick_first(token_result, "seller_id", "sellerId", "shop_id", "shopId") or "").strip() or None,
            shop_id=str(_pick_first(token_result, "shop_id", "shopId") or "").strip() or None,
            shop_cipher=str(_pick_first(token_result, "shop_cipher", "shopCipher") or "").strip() or None,
            open_id=str(_pick_first(token_result, "open_id", "openId") or "").strip() or None,
            raw_payload=token_result,
        )

    raw_payload = token_result.raw_payload or {}
    resolved_shop_id = (
        token_result.shop_id
        or token_result.shop_cipher
        or token_result.seller_id
        or token_result.open_id
        or (fallback_shop_id or "").strip()
    )
    auth_source = source
    if not resolved_shop_id:
        token_seed = pending_key_seed or token_result.refresh_token or token_result.access_token
        if not token_seed:
            raise TikTokIngestError("TikTok auth response did not include a stable shop identifier")
        token_fingerprint = hashlib.sha256(
            f"{app_key}:{token_seed}".encode("utf-8")
        ).hexdigest()[:24]
        resolved_shop_id = f"pending:{token_fingerprint}"
        auth_source = f"{source}_pending"

    return {
        "tiktok_shop_id": resolved_shop_id,
        "shop_name": str(_pick_first(raw_payload, "shop_name", "shopName", "shop_name_en") or "").strip() or None,
        "app_key": app_key,
        "redirect_uri": redirect_uri,
        "access_token": token_result.access_token,
        "refresh_token": token_result.refresh_token,
        "access_token_expires_at": token_result.access_token_expires_at,
        "refresh_token_expires_at": token_result.refresh_token_expires_at,
        "seller_id": token_result.seller_id,
        "shop_cipher": token_result.shop_cipher,
        "open_id": token_result.open_id,
        "shop_region": str(_pick_first(raw_payload, "shop_region", "shopRegion") or "").strip() or None,
        "seller_name": str(_pick_first(raw_payload, "seller_name", "sellerName", "user_name", "userName") or "").strip() or None,
        "scopes_json": json_dumps(_pick_first(raw_payload, "scopes", "scope", "granted_scopes") or []),
        "raw_payload": json_dumps(raw_payload),
        "source": auth_source,
        "received_at": received_at or datetime.now(timezone.utc),
        "updated_at": received_at or datetime.now(timezone.utc),
    }


def _resolve_model_fields(model_type: type[Any]) -> set[str]:
    model_fields = getattr(model_type, "model_fields", None)
    if isinstance(model_fields, dict) and model_fields:
        return set(model_fields.keys())

    annotations = getattr(model_type, "__annotations__", None)
    if isinstance(annotations, dict) and annotations:
        return set(annotations.keys())

    return set()


def _build_model_kwargs(model_type: type[Any], record: dict[str, Any]) -> dict[str, Any]:
    allowed_fields = _resolve_model_fields(model_type)
    if not allowed_fields:
        return dict(record)
    return {key: value for key, value in record.items() if key in allowed_fields}


def upsert_model_row(
    session: Session,
    model_type: type[Any],
    record: dict[str, Any],
    *,
    lookup_field: str,
    dry_run: bool = False,
) -> str:
    lookup_value = record.get(lookup_field)
    if lookup_value in (None, ""):
        raise TikTokIngestError(f"Missing lookup value for {lookup_field}")

    lookup_column = getattr(model_type, lookup_field, None)
    if lookup_column is None:
        raise TikTokIngestError(f"Model {model_type.__name__} has no field named {lookup_field}")

    existing = session.exec(select(model_type).where(lookup_column == lookup_value)).first()
    model_kwargs = _build_model_kwargs(model_type, record)

    if existing is None:
        if not dry_run:
            session.add(model_type(**model_kwargs))
        return "inserted"

    for field_name, value in model_kwargs.items():
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


def upsert_tiktok_auth(
    session: Session,
    auth_model_type: type[Any],
    auth_record: dict[str, Any],
    *,
    lookup_field: str = "tiktok_shop_id",
    dry_run: bool = False,
) -> str:
    return upsert_model_row(
        session,
        auth_model_type,
        auth_record,
        lookup_field=lookup_field,
        dry_run=dry_run,
    )


def upsert_tiktok_order(
    session: Session,
    order_model_type: type[Any],
    order_record: dict[str, Any],
    *,
    lookup_field: str = "tiktok_order_id",
    dry_run: bool = False,
) -> str:
    return upsert_model_row(
        session,
        order_model_type,
        order_record,
        lookup_field=lookup_field,
        dry_run=dry_run,
    )


def upsert_tiktok_auth_from_callback(
    session: Session,
    auth_model_type: type[Any],
    *,
    token_result: TikTokTokenExchangeResult | dict[str, Any],
    app_key: str,
    redirect_uri: str,
    fallback_shop_id: Optional[str] = None,
    pending_key_seed: Optional[str] = None,
    source: str = "oauth_callback",
    received_at: Optional[datetime] = None,
    dry_run: bool = False,
) -> tuple[str, dict[str, Any]]:
    auth_record = build_tiktok_auth_record(
        token_result,
        app_key=app_key,
        redirect_uri=redirect_uri,
        fallback_shop_id=fallback_shop_id,
        pending_key_seed=pending_key_seed,
        source=source,
        received_at=received_at,
    )
    status = upsert_tiktok_auth(
        session,
        auth_model_type,
        auth_record,
        dry_run=dry_run,
    )
    return status, auth_record


def upsert_tiktok_order_from_payload(
    session: Session,
    order_model_type: type[Any],
    payload: Any,
    *,
    source: str = "webhook",
    received_at: Optional[datetime] = None,
    dry_run: bool = False,
) -> tuple[str, dict[str, Any]]:
    order_record = normalize_tiktok_order_payload(
        payload,
        source=source,
        received_at=received_at,
    )
    status = upsert_tiktok_order(
        session,
        order_model_type,
        order_record,
        dry_run=dry_run,
    )
    return status, order_record


def parse_tiktok_webhook_headers(headers: Any) -> dict[str, Optional[str]]:
    normalized_headers: dict[str, Optional[str]] = {}
    header_get = getattr(headers, "get", None)
    if not callable(header_get):
        header_get = lambda _name, _default=None: _default  # type: ignore[assignment]

    for header_name in TIKTOK_WEBHOOK_SIGNATURE_HEADERS:
        normalized_headers["signature"] = header_get(header_name)
        if normalized_headers["signature"]:
            break
    for header_name in TIKTOK_WEBHOOK_TIMESTAMP_HEADERS:
        normalized_headers["timestamp"] = header_get(header_name)
        if normalized_headers["timestamp"]:
            break
    combined_signature = header_get("tiktok-signature") or header_get("TikTok-Signature")
    if combined_signature and (not normalized_headers.get("signature") or not normalized_headers.get("timestamp")):
        parts: dict[str, str] = {}
        for chunk in str(combined_signature).split(","):
            if "=" not in chunk:
                continue
            key, value = chunk.split("=", 1)
            parts[key.strip().lower()] = value.strip()
        if not normalized_headers.get("timestamp"):
            normalized_headers["timestamp"] = parts.get("t")
        if not normalized_headers.get("signature"):
            normalized_headers["signature"] = parts.get("s")
    normalized_headers["event"] = (
        header_get("x-tiktok-topic")
        or header_get("X-TikTok-Topic")
        or header_get("x-event-type")
        or header_get("X-Event-Type")
        or header_get("x-tt-event")
        or header_get("X-TT-Event")
    )
    return normalized_headers


def verify_tiktok_webhook_signature(
    *,
    raw_body: bytes,
    app_secret: str,
    received_signature: Optional[str] = None,
    received_timestamp: Optional[str] = None,
) -> bool:
    normalized_secret = (app_secret or "").strip()
    normalized_signature = (received_signature or "").strip()
    if not normalized_secret or not normalized_signature:
        return False

    candidate_payloads: list[bytes] = [raw_body]
    if received_timestamp:
        raw_body_text = raw_body.decode("utf-8", errors="ignore")
        candidate_payloads.append(f"{received_timestamp}.{raw_body_text}".encode("utf-8"))
        candidate_payloads.append(f"{received_timestamp}{raw_body_text}".encode("utf-8"))

    expected_signatures: set[str] = set()
    for candidate_payload in candidate_payloads:
        digest = hmac.new(normalized_secret.encode("utf-8"), candidate_payload, hashlib.sha256).digest()
        expected_signatures.add(digest.hex())
        expected_signatures.add(base64.b64encode(digest).decode("utf-8"))

    return any(hmac.compare_digest(normalized_signature, expected) for expected in expected_signatures)


def parse_tiktok_webhook_payload(
    raw_body: bytes,
    *,
    app_secret: str,
    headers: Optional[MutableMapping[str, str]] = None,
) -> dict[str, Any]:
    normalized_secret = (app_secret or "").strip()
    if not normalized_secret:
        raise TikTokIngestError("TikTok webhook secret is not configured")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise TikTokIngestError(f"TikTok webhook payload was not valid JSON: {exc}") from exc

    header_signature = None
    header_timestamp = None
    if headers:
        parsed_headers = parse_tiktok_webhook_headers(headers)
        header_signature = parsed_headers.get("signature")
        header_timestamp = parsed_headers.get("timestamp")

    payload_signature = None
    payload_timestamp = None
    if isinstance(payload, dict):
        payload_signature = str(_pick_first(payload, "signature", "sign") or "").strip() or None
        payload_timestamp = str(_pick_first(payload, "timestamp", "ts") or "").strip() or None

    signature = header_signature or payload_signature
    timestamp = header_timestamp or payload_timestamp
    if not signature:
        raise TikTokIngestError("TikTok webhook signature is missing")
    if not verify_tiktok_webhook_signature(
        raw_body=raw_body,
        app_secret=normalized_secret,
        received_signature=signature,
        received_timestamp=timestamp,
    ):
        raise TikTokIngestError("TikTok webhook signature verification failed")

    if not isinstance(payload, dict):
        raise TikTokIngestError("TikTok webhook payload must be a JSON object")
    return payload


def structured_tiktok_log_line(
    *,
    runtime: str,
    action: str,
    success: bool,
    error: Optional[str] = None,
    **fields: Any,
) -> str:
    return structured_log_line(
        runtime=runtime,
        action=action,
        success=success,
        error=error,
        **fields,
    )


__all__ = [
    "TIKTOK_DEFAULT_API_BASE_URL",
    "TIKTOK_DEFAULT_TIMEOUT_SECONDS",
    "TIKTOK_SHOP_AUTH_BASE_URL",
    "TIKTOK_SHOP_TOKEN_GET_PATH",
    "TIKTOK_SHOP_TOKEN_REFRESH_PATH",
    "TIKTOK_TOKEN_GET_PATH",
    "TIKTOK_TOKEN_REFRESH_PATH",
    "TIKTOK_TOKEN_REVOKE_PATH",
    "TIKTOK_WEBHOOK_SIGNATURE_HEADERS",
    "TIKTOK_WEBHOOK_TIMESTAMP_HEADERS",
    "TikTokIngestError",
    "TikTokTokenExchangeResult",
    "build_tiktok_api_url",
    "build_tiktok_auth_record",
    "build_tiktok_reconciliation_snapshot",
    "build_tiktok_request_signature",
    "exchange_tiktok_authorization_code",
    "extract_tiktok_api_data",
    "json_dumps",
    "money_to_float",
    "normalize_tiktok_line_items",
    "normalize_tiktok_order_payload",
    "parse_tiktok_datetime",
    "parse_tiktok_webhook_headers",
    "parse_tiktok_webhook_payload",
    "refresh_tiktok_shop_token",
    "structured_tiktok_log_line",
    "upsert_model_row",
    "upsert_tiktok_auth",
    "upsert_tiktok_auth_from_callback",
    "upsert_tiktok_order",
    "upsert_tiktok_order_from_payload",
    "validate_tiktok_api_response",
    "verify_tiktok_webhook_signature",
]
