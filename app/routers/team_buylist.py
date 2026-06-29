"""
Staff buylist routes.

This is the employee-facing quote builder used at the counter when a customer
wants to sell cards to the store. Pricing is calculated server-side from a
manager-owned JSON config stored in AppSetting so the first version does not
need a schema migration.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy import update
from sqlmodel import Session, select

from ..auth import has_permission
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..financial_values import InvalidFinancialValueError, validate_optional_money
from ..models import (
    AppSetting,
    AuditLog,
    BuylistSubmission,
    InventoryStockMovement,
    User,
    utcnow,
)
from ..inventory.pokemon_scanner import text_search_cards
from ..shared import templates
from ..inventory.tcgplayer_sales import fetch_tcgplayer_public_sales, tcgplayer_product_id_from_url
from .team import _nav_context
from .team_admin import _permission_gate

router = APIRouter()
logger = logging.getLogger(__name__)


BUYLIST_CONFIG_KEY = "staff_buylist_config"
BUYLIST_SEARCH_RESULT_LIMIT = 12
BUYLIST_SEARCH_CACHE_TTL_SECONDS = 300
BUYLIST_SEARCH_CACHE_MAX = 128
BUYLIST_SEARCH_MIN_CHARS = 2
BUYLIST_SEARCH_MAX_CHARS = 500
BUYLIST_ALL_GAMES_VALUE = "__all__"
BUYLIST_PRODUCT_TYPE_CARD = "card"
BUYLIST_PRODUCT_TYPE_SEALED = "sealed"
BUYLIST_PRODUCT_TYPES = {BUYLIST_PRODUCT_TYPE_CARD, BUYLIST_PRODUCT_TYPE_SEALED}
CONDITION_PRICING_PERCENTAGE = "percentage_modifiers"
CONDITION_PRICING_TCGPLAYER = "tcgplayer_market"
CONDITION_PRICING_MODES = {CONDITION_PRICING_PERCENTAGE, CONDITION_PRICING_TCGPLAYER}
BUYLIST_SUBMISSION_STATUSES = ("submitted", "approved", "paid", "rejected")
NO_MARKET_PRICE_NOTE = "No market price found. Manager review required."
BUYLIST_EDIT_PERMISSION = "admin.buylist.edit"

_BUYLIST_SEARCH_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
BUYLIST_SEARCH_CACHE_VERSION = "buylist-search-v4"
BUYLIST_QUOTE_TOKEN_VERSION = 1
BUYLIST_CANDIDATE_TOKEN_MAX_AGE_SECONDS = 15 * 60
BUYLIST_TOKEN_FUTURE_SKEW_SECONDS = 60
BUYLIST_TOKEN_MAX_BYTES = 64 * 1024
BUYLIST_TOKEN_MAX_ENCODED_BYTES = ((BUYLIST_TOKEN_MAX_BYTES + 2) // 3) * 4
BUYLIST_SIGNING_KEY_MIN_CHARS = 32
BUYLIST_MAX_VARIANTS = 32
BUYLIST_MAX_QUOTE_ITEMS = 100
BUYLIST_QUOTE_UNAVAILABLE_ERROR = (
    "Buylist quoting is unavailable until BUYLIST_QUOTE_SIGNING_KEYS is configured."
)
BUYLIST_QUOTE_INVALID_ERROR = "This buylist search result is invalid or expired. Search again."
BUYLIST_ATTESTATION_INVALID_ERROR = (
    "This submission has an invalid or legacy quote. Re-quote it before approval."
)
BUYLIST_APPROVAL_ATOMIC_ERROR = (
    "Buylist approval could not be completed. No inventory changes were saved."
)

_BUYLIST_IDENTITY_FIELDS = (
    "id",
    "product_id",
    "tcgplayer_product_id",
    "item_type",
    "game",
    "category_id",
    "name",
    "set_name",
    "set_code",
    "number",
    "upc",
    "sealed_product_kind",
    "rarity",
    "image_url",
    "external_url",
    "tcgplayer_url",
)
_BUYLIST_ATTESTED_LINE_FIELDS = (
    *_BUYLIST_IDENTITY_FIELDS,
    "quantity",
    "condition",
    "language",
    "variant",
    "market_price",
    "base_market_price",
    "condition_market_price",
    "condition_price_source",
    "unit_cash",
    "unit_trade",
    "line_cash",
    "line_trade",
    "pricing_notes",
    "blocked",
    "quote_source",
    "candidate_issued_at",
    "quoted_at",
)


class BuylistQuoteConfigurationError(RuntimeError):
    pass


class BuylistQuoteTokenError(ValueError):
    pass

BUYLIST_GAMES: tuple[dict[str, str], ...] = (
    {"game": "Pokemon", "label": "Pokemon", "category_id": "3"},
    {"game": "Pokemon JP", "label": "Pokemon JP", "category_id": "85"},
    {"game": "Magic", "label": "Magic: The Gathering", "category_id": "1"},
    {"game": "Yu-Gi-Oh", "label": "Yu-Gi-Oh", "category_id": "2"},
    {"game": "One Piece", "label": "One Piece", "category_id": "68"},
    {"game": "Lorcana", "label": "Disney Lorcana", "category_id": "71"},
    {"game": "Riftbound", "label": "Riftbound", "category_id": "89"},
)

_GAME_BY_NAME = {row["game"].lower(): row for row in BUYLIST_GAMES}
_GAME_BY_CATEGORY = {row["category_id"]: row for row in BUYLIST_GAMES}

DEFAULT_BUYLIST_CONFIG: dict[str, Any] = {
    "enabled_games": ["Pokemon", "Pokemon JP", "Magic", "Yu-Gi-Oh", "One Piece", "Lorcana", "Riftbound"],
    "default_game": "Pokemon",
    "default_payment": "cash",
    "condition_pricing_mode": CONDITION_PRICING_TCGPLAYER,
    "cash_ranges": [
        {"min": 0.0, "max": 0.49, "type": "fixed", "value": 0.01},
        {"min": 0.5, "max": 0.99, "type": "fixed", "value": 0.10},
        {"min": 1.0, "max": 2.99, "type": "fixed", "value": 0.20},
        {"min": 3.0, "max": 24.99, "type": "percentage", "value": 50.0},
        {"min": 25.0, "max": 99.99, "type": "percentage", "value": 60.0},
        {"min": 100.0, "max": 999999.0, "type": "percentage", "value": 65.0},
    ],
    "trade_ranges": [
        {"min": 0.0, "max": 0.49, "type": "fixed", "value": 0.02},
        {"min": 0.5, "max": 0.99, "type": "fixed", "value": 0.15},
        {"min": 1.0, "max": 2.99, "type": "fixed", "value": 0.25},
        {"min": 3.0, "max": 24.99, "type": "percentage", "value": 60.0},
        {"min": 25.0, "max": 99.99, "type": "percentage", "value": 70.0},
        {"min": 100.0, "max": 999999.0, "type": "percentage", "value": 75.0},
    ],
    "condition_modifiers": {"NM": 100.0, "LP": 85.0, "MP": 65.0, "HP": 45.0, "DMG": 25.0},
    "language_modifiers": {"English": 100.0, "Japanese": 90.0, "Other": 80.0},
    "printing_modifiers": {
        "Normal": 100.0,
        "Holofoil": 100.0,
        "Reverse Holofoil": 95.0,
        "Foil": 100.0,
        "1st Edition": 100.0,
        "Unlimited": 100.0,
    },
    "hotlist_rules": [],
    "darklist_rules": [],
    "checkout_note": "",
}

CONDITION_OPTIONS = ("NM", "LP", "MP", "HP", "DMG")
LANGUAGE_OPTIONS = ("English", "Japanese", "Other")
_CONDITION_ALIASES: dict[str, tuple[str, ...]] = {
    "NM": ("NM", "NEAR MINT", "NEARMINT"),
    "LP": ("LP", "LIGHTLY PLAYED", "LIGHT PLAY", "EXCELLENT"),
    "MP": ("MP", "MODERATELY PLAYED", "MODERATE PLAY", "PLAYED"),
    "HP": ("HP", "HEAVILY PLAYED", "HEAVY PLAY"),
    "DMG": ("DMG", "DM", "DAMAGED"),
}


def _portal_or_404() -> None:
    if not get_settings().employee_portal_enabled:
        raise HTTPException(status_code=404)


def _require_team_user(
    request: Request,
    session: Session,
) -> tuple[Optional[Response], Optional[User]]:
    _portal_or_404()
    user: Optional[User] = getattr(request.state, "current_user", None)
    if user is None:
        return RedirectResponse("/team/login", status_code=303), None
    return None, user


def _require_buylist_admin(
    request: Request,
    session: Session,
) -> tuple[Optional[Response], Optional[User]]:
    return _permission_gate(request, session, BUYLIST_EDIT_PERMISSION)


def _can_manage_buylist_pricing(session: Session, user: Optional[User]) -> bool:
    if user is None or getattr(user, "role", None) not in {"admin", "manager", "reviewer"}:
        return False
    return has_permission(session, user, BUYLIST_EDIT_PERMISSION)


def _buylist_signing_keys() -> list[bytes]:
    settings = get_settings()
    raw = str(getattr(settings, "buylist_quote_signing_keys", "") or "")
    keys = [part.strip() for part in raw.split(",") if part.strip()]
    session_secret = str(getattr(settings, "session_secret", "") or "").strip()
    if (
        not keys
        or any(len(key) < BUYLIST_SIGNING_KEY_MIN_CHARS for key in keys)
        or (session_secret and any(hmac.compare_digest(key, session_secret) for key in keys))
    ):
        raise BuylistQuoteConfigurationError(BUYLIST_QUOTE_UNAVAILABLE_ERROR)
    return [key.encode("utf-8") for key in keys]


def _buylist_unavailable_response() -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": BUYLIST_QUOTE_UNAVAILABLE_ERROR},
        status_code=503,
    )


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    if not value or len(value) > BUYLIST_TOKEN_MAX_ENCODED_BYTES:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    try:
        return base64.b64decode(
            value + ("=" * (-len(value) % 4)),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR) from exc


def _sign_buylist_payload(payload: dict[str, Any]) -> str:
    keys = _buylist_signing_keys()
    try:
        encoded_payload = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR) from exc
    if len(encoded_payload) > BUYLIST_TOKEN_MAX_BYTES:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    signature = hmac.new(keys[0], encoded_payload, hashlib.sha256).digest()
    return f"{_base64url_encode(encoded_payload)}.{_base64url_encode(signature)}"


def _verify_buylist_payload(
    token: Any,
    *,
    purpose: str,
    employee_id: int,
    max_age_seconds: Optional[int],
) -> dict[str, Any]:
    if (
        not isinstance(token, str)
        or len(token) > BUYLIST_TOKEN_MAX_ENCODED_BYTES + 64
        or token.count(".") != 1
    ):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    encoded_payload, encoded_signature = token.split(".", 1)
    payload_bytes = _base64url_decode(encoded_payload)
    supplied_signature = _base64url_decode(encoded_signature)
    if len(payload_bytes) > BUYLIST_TOKEN_MAX_BYTES or len(supplied_signature) != hashlib.sha256().digest_size:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    keys = _buylist_signing_keys()
    if not any(
        hmac.compare_digest(
            supplied_signature,
            hmac.new(key, payload_bytes, hashlib.sha256).digest(),
        )
        for key in keys
    ):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR) from exc
    if not isinstance(payload, dict):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    if payload.get("version") != BUYLIST_QUOTE_TOKEN_VERSION or payload.get("purpose") != purpose:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    bound_employee_id = payload.get("employee_id")
    if (
        isinstance(bound_employee_id, bool)
        or not isinstance(bound_employee_id, int)
        or bound_employee_id <= 0
        or bound_employee_id != employee_id
    ):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    issued_at = payload.get("issued_at")
    if isinstance(issued_at, bool) or not isinstance(issued_at, int) or issued_at < 0:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    now = int(time.time())
    if issued_at > now + BUYLIST_TOKEN_FUTURE_SKEW_SECONDS:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    if max_age_seconds is not None and now - issued_at > max_age_seconds:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    if not isinstance(payload.get("source"), str) or not payload["source"]:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    return payload


def _bounded_buylist_text(value: Any, *, max_chars: int = 500) -> str:
    return str(value or "").strip()[:max_chars]


def _trusted_buylist_price(value: Any, *, field_name: str) -> float:
    try:
        price = validate_optional_money(value, field_name=field_name)
    except InvalidFinancialValueError as exc:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR) from exc
    if price is None or price < 0:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    return _money(price)


def _deep_merge(defaults: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(defaults)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_buylist_config(session: Session) -> dict[str, Any]:
    row = session.get(AppSetting, BUYLIST_CONFIG_KEY)
    if row is None or not (row.value or "").strip():
        return deepcopy(DEFAULT_BUYLIST_CONFIG)
    try:
        saved = json.loads(row.value)
    except json.JSONDecodeError:
        saved = {}
    if not isinstance(saved, dict):
        saved = {}
    config = _deep_merge(DEFAULT_BUYLIST_CONFIG, saved)
    config["enabled_games"] = [
        game for game in config.get("enabled_games", []) if game.lower() in _GAME_BY_NAME
    ] or list(DEFAULT_BUYLIST_CONFIG["enabled_games"])
    if str(config.get("default_game") or "").lower() not in _GAME_BY_NAME:
        config["default_game"] = "Pokemon"
    if config["default_game"] not in config["enabled_games"]:
        config["default_game"] = config["enabled_games"][0]
    if config.get("condition_pricing_mode") not in CONDITION_PRICING_MODES:
        config["condition_pricing_mode"] = DEFAULT_BUYLIST_CONFIG["condition_pricing_mode"]
    return config


def save_buylist_config(session: Session, config: dict[str, Any]) -> None:
    row = session.get(AppSetting, BUYLIST_CONFIG_KEY)
    if row is None:
        row = AppSetting(key=BUYLIST_CONFIG_KEY)
        session.add(row)
    row.value = json.dumps(config, sort_keys=True)
    session.commit()
    _BUYLIST_SEARCH_CACHE.clear()


def _buylist_config_fingerprint(config: dict[str, Any]) -> str:
    raw = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _buylist_search_cache_key(
    query: str,
    category_id: str,
    config: dict[str, Any],
    *,
    product_type: str = BUYLIST_PRODUCT_TYPE_CARD,
) -> str:
    normalized = re.sub(r"\s+", " ", query.strip().lower())
    product_type = _normalize_product_type(product_type)
    return (
        f"{BUYLIST_SEARCH_CACHE_VERSION}:{product_type}:{category_id}:"
        f"{normalized}:{_buylist_config_fingerprint(config)}"
    )


def _buylist_search_cache_get(key: str) -> Optional[dict[str, Any]]:
    cached = _BUYLIST_SEARCH_CACHE.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if expires_at <= time.monotonic():
        _BUYLIST_SEARCH_CACHE.pop(key, None)
        return None
    response = deepcopy(payload)
    response["cached"] = True
    return response


def _buylist_search_cache_set(key: str, payload: dict[str, Any]) -> None:
    if len(_BUYLIST_SEARCH_CACHE) >= BUYLIST_SEARCH_CACHE_MAX:
        oldest_key = min(_BUYLIST_SEARCH_CACHE, key=lambda item: _BUYLIST_SEARCH_CACHE[item][0])
        _BUYLIST_SEARCH_CACHE.pop(oldest_key, None)
    cached_payload = deepcopy(payload)
    cached_payload["cached"] = False
    for card in cached_payload.get("cards") or []:
        if isinstance(card, dict):
            card.pop("candidate_token", None)
    _BUYLIST_SEARCH_CACHE[key] = (
        time.monotonic() + BUYLIST_SEARCH_CACHE_TTL_SECONDS,
        cached_payload,
    )


def _enabled_game_options(config: dict[str, Any]) -> list[dict[str, str]]:
    enabled = {str(game).lower() for game in config.get("enabled_games", [])}
    return [row for row in BUYLIST_GAMES if row["game"].lower() in enabled]


def _default_buylist_game(config: dict[str, Any]) -> str:
    enabled = _enabled_game_options(config)
    configured = str(config.get("default_game") or "Pokemon").strip()
    if _is_all_games(configured):
        configured = "Pokemon"
    row = _GAME_BY_NAME.get(configured.lower())
    if row and any(row["game"] == option["game"] for option in enabled):
        return row["game"]
    pokemon = _GAME_BY_NAME["pokemon"]
    if any(pokemon["game"] == option["game"] for option in enabled):
        return pokemon["game"]
    return (enabled[0] if enabled else pokemon)["game"]


def _is_all_games(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {
        BUYLIST_ALL_GAMES_VALUE,
        "all",
        "all games",
        "all-games",
    }


def _normalize_product_type(value: str | None) -> str:
    normalized = (value or BUYLIST_PRODUCT_TYPE_CARD).strip().lower()
    return normalized if normalized in BUYLIST_PRODUCT_TYPES else BUYLIST_PRODUCT_TYPE_CARD


def _category_for_game(game: str | None, config: dict[str, Any]) -> str:
    enabled = _enabled_game_options(config)
    default_game = _default_buylist_game(config)
    selected = (game or default_game or "Pokemon").strip().lower()
    if _is_all_games(selected):
        selected = default_game.lower()
    row = _GAME_BY_NAME.get(selected)
    if row and any(row["game"] == option["game"] for option in enabled):
        return row["category_id"]
    return (enabled[0] if enabled else _GAME_BY_NAME["pokemon"])["category_id"]


def _game_for_category(category_id: str) -> str:
    return _GAME_BY_CATEGORY.get(str(category_id), _GAME_BY_NAME["pokemon"])["game"]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


def _money(value: float) -> float:
    return round(max(0.0, float(value or 0.0)) + 1e-9, 2)


def _range_offer(market_price: float, ranges: list[dict[str, Any]]) -> tuple[float, str]:
    price = max(0.0, float(market_price or 0.0))
    selected: Optional[dict[str, Any]] = None
    for row in ranges or []:
        low = _float(row.get("min"), 0.0)
        high = _float(row.get("max"), 999999.0)
        if low <= price <= high:
            selected = row
            break
    if selected is None and ranges:
        selected = ranges[-1]
    if not selected:
        return 0.0, "No range"
    mode = str(selected.get("type") or "percentage").lower()
    value = _float(selected.get("value"), 0.0)
    if mode == "fixed":
        return value, f"Fixed ${value:g}"
    if mode == "by_appointment":
        return 0.0, "By appointment"
    return price * (value / 100.0), f"{value:g}%"


def _modifier_percent(config: dict[str, Any], group: str, key: str, fallback: float = 100.0) -> float:
    options = config.get(group) or {}
    if key in options:
        return _float(options.get(key), fallback)
    normalized = key.strip().lower()
    for opt_key, opt_value in options.items():
        if str(opt_key).strip().lower() == normalized:
            return _float(opt_value, fallback)
    return _float(options.get("Other"), fallback)


def _normalize_condition(condition: str | None) -> str:
    normalized = (condition or "NM").strip().upper()
    if normalized == "DM":
        return "DMG"
    for canonical, aliases in _CONDITION_ALIASES.items():
        if normalized in aliases:
            return canonical
    return normalized if normalized in CONDITION_OPTIONS else "NM"


def _condition_alias_tokens(condition: str) -> tuple[str, ...]:
    return _CONDITION_ALIASES.get(_normalize_condition(condition), (condition,))


def _condition_price_value(value: Any) -> Optional[float]:
    if isinstance(value, dict):
        for key in ("mkt", "market", "market_price", "price"):
            if key in value:
                price = _float(value.get(key), -1.0)
                if price > 0:
                    return price
        return None
    price = _float(value, -1.0)
    return price if price > 0 else None


def _condition_raw_value_from_conditions(
    conditions: dict[str, Any],
    condition: str,
) -> Any:
    if not isinstance(conditions, dict):
        return None
    aliases = {alias.upper() for alias in _condition_alias_tokens(condition)}
    for raw_key, raw_value in conditions.items():
        key = str(raw_key or "").strip().upper()
        if key in aliases:
            return raw_value
    for raw_key, raw_value in conditions.items():
        compact = re.sub(r"[^A-Z]", "", str(raw_key or "").upper())
        for alias in aliases:
            alias_compact = re.sub(r"[^A-Z]", "", alias)
            if compact and (
                compact == alias_compact
                or (len(alias_compact) >= 6 and alias_compact in compact)
            ):
                return raw_value
    return None


def _condition_market_price_from_conditions(
    conditions: dict[str, Any],
    condition: str,
) -> Optional[float]:
    return _condition_price_value(_condition_raw_value_from_conditions(conditions, condition))


def _condition_price_metric(value: Any) -> dict[str, Any]:
    metric: dict[str, Any] = {}
    if isinstance(value, dict):
        for out_key, source_keys in (
            ("market", ("mkt", "market", "market_price", "price")),
            ("low", ("low", "lowest", "lowest_price", "low_price")),
            ("high", ("hi", "high", "highest", "highest_price", "high_price")),
        ):
            for source_key in source_keys:
                if source_key in value:
                    price = _condition_price_value(value.get(source_key))
                    if price is not None:
                        metric[out_key] = _money(price)
                        break
        for source_key in ("cnt", "count", "price_count", "listing_count"):
            if source_key in value:
                count = int(max(0, _float(value.get(source_key), 0.0)))
                if count:
                    metric["listing_count"] = count
                    break
        for source_key in ("sku", "sku_id", "skuId"):
            if value.get(source_key):
                metric["sku_id"] = str(value.get(source_key))
                break
    else:
        price = _condition_price_value(value)
        if price is not None:
            metric["market"] = _money(price)
    return metric


def _variant_matches(variant: dict[str, Any], selected_variant: str) -> bool:
    return str(variant.get("name") or "").strip().lower() == selected_variant.strip().lower()


def _condition_market_prices_for_variant(
    variants: list[dict[str, Any]],
    selected_variant: str,
) -> dict[str, float]:
    variant = _select_variant([row for row in variants if isinstance(row, dict)], selected_variant)
    if not variant:
        return {}
    conditions = variant.get("conditions") or {}
    prices: dict[str, float] = {}
    for condition in CONDITION_OPTIONS:
        price = _condition_market_price_from_conditions(conditions, condition)
        if price is not None:
            prices[condition] = _money(price)
    return prices


def _condition_price_metrics_for_variant(
    variants: list[dict[str, Any]],
    selected_variant: str,
) -> dict[str, dict[str, Any]]:
    variant = _select_variant([row for row in variants if isinstance(row, dict)], selected_variant)
    if not variant:
        return {}
    conditions = variant.get("conditions") or {}
    metrics: dict[str, dict[str, Any]] = {}
    for condition in CONDITION_OPTIONS:
        raw_value = _condition_raw_value_from_conditions(conditions, condition)
        metric = _condition_price_metric(raw_value)
        if metric:
            metrics[condition] = metric
    return metrics


def _condition_market_price_from_item(
    item: dict[str, Any],
    condition: str,
    variant: str,
) -> Optional[float]:
    prices = item.get("condition_market_prices") or item.get("condition_prices") or {}
    if isinstance(prices, dict):
        price = _condition_market_price_from_conditions(prices, condition)
        if price is not None:
            return price
        normalized = _normalize_condition(condition)
        if normalized in prices:
            return _condition_price_value(prices.get(normalized))

    variants = item.get("available_variants") or []
    if isinstance(variants, list):
        return _condition_market_prices_for_variant(
            [row for row in variants if isinstance(row, dict)],
            variant,
        ).get(_normalize_condition(condition))
    return None


def _condition_pricing_mode(config: dict[str, Any]) -> str:
    mode = str(config.get("condition_pricing_mode") or CONDITION_PRICING_TCGPLAYER).strip().lower()
    return mode if mode in CONDITION_PRICING_MODES else CONDITION_PRICING_TCGPLAYER


def _pattern_matches(pattern: str, product: dict[str, Any]) -> bool:
    needle = (pattern or "").strip().lower()
    if not needle:
        return False
    haystacks = [
        str(product.get("id") or ""),
        str(product.get("product_id") or ""),
        str(product.get("name") or ""),
        str(product.get("set_name") or ""),
        str(product.get("number") or ""),
        str(product.get("upc") or ""),
        str(product.get("sealed_product_kind") or ""),
        str(product.get("item_type") or ""),
    ]
    return any(needle in value.lower() for value in haystacks if value)


def _list_adjustment(config: dict[str, Any], product: dict[str, Any]) -> tuple[float, list[str], bool]:
    multiplier = 1.0
    notes: list[str] = []
    blocked = False
    for rule in config.get("hotlist_rules") or []:
        pattern = str(rule.get("pattern") or "")
        if _pattern_matches(pattern, product):
            boost = _float(rule.get("percent"), 0.0)
            multiplier *= 1.0 + boost / 100.0
            notes.append(f"Hotlist +{boost:g}%")
    for rule in config.get("darklist_rules") or []:
        pattern = str(rule.get("pattern") or "")
        if _pattern_matches(pattern, product):
            penalty = _float(rule.get("percent"), 0.0)
            if penalty >= 100:
                blocked = True
            multiplier *= max(0.0, 1.0 - penalty / 100.0)
            notes.append(f"Darklist -{penalty:g}%")
    return multiplier, notes, blocked


def calculate_buylist_offer(
    config: dict[str, Any],
    *,
    market_price: float,
    condition_market_price: Optional[float] = None,
    condition: str = "NM",
    language: str = "English",
    printing: str = "Normal",
    product: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    product = product or {}
    is_sealed = str(product.get("item_type") or "").strip().lower() == BUYLIST_PRODUCT_TYPE_SEALED
    condition = "Sealed" if is_sealed else _normalize_condition(condition)
    language = (language or "English").strip() or "English"
    printing = (printing or "Normal").strip() or "Normal"

    pricing_mode = _condition_pricing_mode(config)
    condition_market = _float(condition_market_price, -1.0)
    using_condition_market = (
        not is_sealed
        and pricing_mode == CONDITION_PRICING_TCGPLAYER
        and condition_market > 0
    )
    effective_market_price = condition_market if using_condition_market else market_price

    base_cash, cash_rule = _range_offer(effective_market_price, config.get("cash_ranges") or [])
    base_trade, trade_rule = _range_offer(effective_market_price, config.get("trade_ranges") or [])

    condition_mod = 100.0 if (is_sealed or using_condition_market) else _modifier_percent(config, "condition_modifiers", condition)
    language_mod = 100.0 if is_sealed else _modifier_percent(config, "language_modifiers", language)
    printing_mod = 100.0 if is_sealed else _modifier_percent(config, "printing_modifiers", printing)
    list_multiplier, list_notes, blocked = _list_adjustment(config, product)
    missing_market_price = effective_market_price <= 0
    if missing_market_price:
        blocked = True
    total_multiplier = (condition_mod / 100.0) * (language_mod / 100.0) * (printing_mod / 100.0) * list_multiplier

    cash = 0.0 if blocked else base_cash * total_multiplier
    trade = 0.0 if blocked else base_trade * total_multiplier
    if is_sealed:
        condition_note = "Sealed product TCGPlayer market"
        condition_source = BUYLIST_PRODUCT_TYPE_SEALED
    elif using_condition_market:
        condition_note = f"{condition} TCGPlayer market"
        condition_source = CONDITION_PRICING_TCGPLAYER
    elif pricing_mode == CONDITION_PRICING_TCGPLAYER:
        condition_note = f"{condition} modifier fallback {condition_mod:g}%"
        condition_source = "modifier_fallback"
    else:
        condition_note = f"{condition} {condition_mod:g}%"
        condition_source = CONDITION_PRICING_PERCENTAGE
    notes = [condition_note, *list_notes] if is_sealed else [
        condition_note,
        f"{language} {language_mod:g}%",
        f"{printing} {printing_mod:g}%",
        *list_notes,
    ]
    if missing_market_price:
        notes.append(NO_MARKET_PRICE_NOTE)
    if blocked:
        notes.append("Not buying")

    return {
        "market_price": _money(effective_market_price),
        "base_market_price": _money(market_price),
        "condition_market_price": _money(condition_market) if using_condition_market else None,
        "condition_price_source": condition_source,
        "cash_offer": _money(cash),
        "trade_offer": _money(trade),
        "cash_rule": cash_rule,
        "trade_rule": trade_rule,
        "modifier_percent": round(total_multiplier * 100.0, 2),
        "notes": notes,
        "blocked": blocked,
    }


def _select_variant(
    variants: list[dict[str, Any]],
    selected_variant: str | None = None,
) -> dict[str, Any] | None:
    selected = (selected_variant or "").strip()
    if selected:
        for variant in variants:
            if str(variant.get("name") or "").strip().lower() == selected.lower():
                return variant
    for variant in variants:
        if str(variant.get("name") or "").strip().lower() == "normal":
            return variant
    for variant in variants:
        if variant.get("price") is not None:
            return variant
    return variants[0] if variants else None


def _variant_price(candidate: dict[str, Any], selected_variant: str | None = None) -> tuple[float, str]:
    variants = [row for row in (candidate.get("available_variants") or []) if isinstance(row, dict)]
    selected = (selected_variant or candidate.get("variant") or "").strip()
    variant = _select_variant(variants, selected)
    if variant:
        price = variant.get("price")
        if price is not None:
            return _float(price), str(variant.get("name") or selected or "Market")
    return _float(candidate.get("market_price")), selected or "Normal"


def _tcgplayer_product_id_from_candidate(candidate: dict[str, Any]) -> str:
    for key in ("tcgplayer_product_id", "tcgplayer_id", "product_id", "external_id"):
        value = str(candidate.get(key) or "").strip()
        if re.fullmatch(r"\d{1,12}", value):
            return value
    for key in ("tcgplayer_url", "external_url", "url"):
        product_id = tcgplayer_product_id_from_url(str(candidate.get(key) or ""))
        if product_id:
            return product_id
    return ""


def _candidate_payload(
    candidate: dict[str, Any],
    config: dict[str, Any],
    *,
    category_id: str,
) -> dict[str, Any]:
    market_price, variant = _variant_price(candidate)
    variants = candidate.get("available_variants") or []
    condition_market_prices = _condition_market_prices_for_variant(
        [row for row in variants if isinstance(row, dict)],
        variant,
    )
    condition_price_metrics = _condition_price_metrics_for_variant(
        [row for row in variants if isinstance(row, dict)],
        variant,
    )
    tcgplayer_url = str(candidate.get("tcgplayer_url") or candidate.get("external_url") or "").strip()
    tcgplayer_product_id = _tcgplayer_product_id_from_candidate(candidate)
    product = {
        "id": candidate.get("id") or "",
        "product_id": tcgplayer_product_id or candidate.get("product_id") or candidate.get("id") or "",
        "name": candidate.get("name") or "",
        "set_name": candidate.get("set_name") or "",
        "number": candidate.get("number") or "",
    }
    offer = calculate_buylist_offer(
        config,
        market_price=market_price,
        condition_market_price=condition_market_prices.get("NM"),
        condition="NM",
        language="Japanese" if category_id == "85" else "English",
        printing=variant,
        product=product,
    )
    return {
        "id": candidate.get("id") or "",
        "product_id": tcgplayer_product_id or candidate.get("product_id") or candidate.get("id") or "",
        "tcgplayer_product_id": tcgplayer_product_id,
        "item_type": BUYLIST_PRODUCT_TYPE_CARD,
        "game": _game_for_category(category_id),
        "category_id": category_id,
        "name": candidate.get("name") or "",
        "set_name": candidate.get("set_name") or "",
        "number": candidate.get("number") or "",
        "rarity": candidate.get("rarity") or "",
        "variant": variant,
        "available_variants": variants,
        "condition_market_prices": condition_market_prices,
        "condition_price_metrics": condition_price_metrics,
        "condition_pricing_mode": _condition_pricing_mode(config),
        "image_url": candidate.get("image_url") or candidate.get("image_url_small") or "",
        "image_url_small": candidate.get("image_url_small") or candidate.get("image_url") or "",
        "external_url": tcgplayer_url,
        "tcgplayer_url": tcgplayer_url,
        "market_price": offer["market_price"],
        "base_market_price": offer["base_market_price"],
        "cash_offer": offer["cash_offer"],
        "trade_offer": offer["trade_offer"],
        "pricing_notes": offer["notes"],
        "blocked": offer["blocked"],
    }


async def _search_buylist_sealed_products(
    query: str,
    *,
    game: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    from ..inventory.routes import _cached_add_stock_sealed_search

    return await _cached_add_stock_sealed_search(query, game=game, limit=limit)


def _dedupe_key(payload: dict[str, Any]) -> str:
    display_name = str(payload.get("name") or "").strip().lower()
    display_set = str(payload.get("set_name") or "").strip().lower()
    display_number = str(payload.get("number") or "").strip().lower()
    display_number_core = display_number.split("/", 1)[0].lstrip("0") or display_number
    display_game = str(payload.get("game") or "").strip().lower()
    if display_name and display_set and display_number_core:
        return f"display:{display_game}:{display_name}:{display_set}:{display_number_core}"
    for key in ("tcgplayer_product_id", "product_id", "id", "external_id"):
        value = str(payload.get(key) or "").strip().lower()
        if value:
            return f"{key}:{value}"
    return "|".join(
        str(payload.get(key) or "").strip().lower()
        for key in ("game", "name", "set_name", "number", "upc")
    )


def _dedupe_payloads(payloads: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for payload in payloads:
        key = _dedupe_key(payload)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
        if len(deduped) >= limit:
            break
    return deduped


def _sealed_product_payload(
    product: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    market_price = _float(product.get("market_price"), 0.0)
    product_ref = {
        "id": product.get("external_id") or "",
        "product_id": product.get("external_id") or "",
        "item_type": BUYLIST_PRODUCT_TYPE_SEALED,
        "name": product.get("name") or "",
        "set_name": product.get("set_name") or "",
        "number": product.get("upc") or "",
        "upc": product.get("upc") or "",
        "sealed_product_kind": product.get("kind") or "",
    }
    offer = calculate_buylist_offer(
        config,
        market_price=market_price,
        condition="Sealed",
        language="",
        printing="Sealed Product",
        product=product_ref,
    )
    product_id = product.get("external_id") or _sealed_product_fallback_id(product)
    return {
        "id": product_id,
        "product_id": product_id,
        "tcgplayer_product_id": product_id if re.fullmatch(r"\d{1,12}", str(product_id)) else "",
        "item_type": BUYLIST_PRODUCT_TYPE_SEALED,
        "game": product.get("game") or "",
        "category_id": str(product.get("category_id") or ""),
        "name": product.get("name") or "",
        "set_name": product.get("set_name") or "",
        "number": product.get("upc") or "",
        "upc": product.get("upc") or "",
        "sealed_product_kind": product.get("kind") or "",
        "rarity": product.get("kind") or "",
        "variant": "Sealed Product",
        "available_variants": [],
        "condition_market_prices": {},
        "condition_price_metrics": {},
        "condition_pricing_mode": _condition_pricing_mode(config),
        "image_url": product.get("image_url") or product.get("image_url_small") or "",
        "image_url_small": product.get("image_url_small") or product.get("image_url") or "",
        "external_url": product.get("external_url") or "",
        "market_price": offer["market_price"],
        "base_market_price": offer["base_market_price"],
        "cash_offer": offer["cash_offer"],
        "trade_offer": offer["trade_offer"],
        "pricing_notes": offer["notes"],
        "blocked": offer["blocked"],
    }


def _sealed_product_fallback_id(product: dict[str, Any]) -> str:
    parts = [
        str(product.get("game") or ""),
        str(product.get("name") or ""),
        str(product.get("set_name") or ""),
        str(product.get("kind") or ""),
        str(product.get("upc") or ""),
    ]
    return "sealed:" + hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _trusted_condition_price(
    conditions: dict[str, Any],
    condition: str,
    *,
    field_name: str,
) -> Optional[float]:
    raw_value = _condition_raw_value_from_conditions(conditions, condition)
    if raw_value is None:
        return None
    if isinstance(raw_value, dict):
        raw_value = next(
            (
                raw_value[key]
                for key in ("mkt", "market", "market_price", "price")
                if key in raw_value
            ),
            None,
        )
        if raw_value is None:
            return None
    return _trusted_buylist_price(raw_value, field_name=field_name)


def _candidate_snapshot(card: dict[str, Any]) -> dict[str, Any]:
    item_type = _normalize_product_type(str(card.get("item_type") or BUYLIST_PRODUCT_TYPE_CARD))
    identity: dict[str, str] = {}
    for field in _BUYLIST_IDENTITY_FIELDS:
        if field == "item_type":
            identity[field] = item_type
        else:
            max_chars = 2_000 if field in {"image_url", "external_url", "tcgplayer_url"} else 500
            identity[field] = _bounded_buylist_text(card.get(field), max_chars=max_chars)

    base_value = card.get("base_market_price")
    if base_value is None:
        base_value = card.get("market_price")
    base_market_price = _trusted_buylist_price(
        base_value,
        field_name="candidate base market price",
    )
    default_variant = (
        "Sealed Product"
        if item_type == BUYLIST_PRODUCT_TYPE_SEALED
        else _bounded_buylist_text(card.get("variant") or "Normal", max_chars=100)
    )

    canonical_variants: list[dict[str, Any]] = []
    seen_variants: set[str] = set()
    raw_variants = card.get("available_variants")
    if isinstance(raw_variants, list):
        for raw_variant in raw_variants[:BUYLIST_MAX_VARIANTS]:
            if not isinstance(raw_variant, dict):
                continue
            name = _bounded_buylist_text(raw_variant.get("name"), max_chars=100)
            if not name or name.casefold() in seen_variants:
                continue
            raw_price = raw_variant.get("price")
            if raw_price is None and name.casefold() == default_variant.casefold():
                raw_price = base_market_price
            if raw_price is None:
                continue
            price = _trusted_buylist_price(
                raw_price,
                field_name=f"candidate {name} price",
            )
            condition_prices: dict[str, float] = {}
            raw_conditions = raw_variant.get("conditions")
            if isinstance(raw_conditions, dict):
                for condition in CONDITION_OPTIONS:
                    condition_price = _trusted_condition_price(
                        raw_conditions,
                        condition,
                        field_name=f"candidate {name} {condition} price",
                    )
                    if condition_price is not None:
                        condition_prices[condition] = condition_price
            canonical_variants.append(
                {
                    "name": name,
                    "price": price,
                    "condition_prices": condition_prices,
                }
            )
            seen_variants.add(name.casefold())

    if item_type == BUYLIST_PRODUCT_TYPE_SEALED:
        canonical_variants = [
            {
                "name": "Sealed Product",
                "price": base_market_price,
                "condition_prices": {},
            }
        ]
        default_variant = "Sealed Product"
    elif not canonical_variants:
        canonical_variants = [
            {
                "name": default_variant or "Normal",
                "price": base_market_price,
                "condition_prices": {},
            }
        ]

    selected_variant = _select_variant(canonical_variants, default_variant)
    if selected_variant is None:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    default_variant = str(selected_variant["name"])
    top_level_conditions = card.get("condition_market_prices")
    if isinstance(top_level_conditions, dict):
        for condition in CONDITION_OPTIONS:
            if condition in selected_variant["condition_prices"]:
                continue
            condition_price = _trusted_condition_price(
                top_level_conditions,
                condition,
                field_name=f"candidate {default_variant} {condition} price",
            )
            if condition_price is not None:
                selected_variant["condition_prices"][condition] = condition_price

    return {
        "identity": identity,
        "base_market_price": _trusted_buylist_price(
            selected_variant["price"],
            field_name="candidate selected market price",
        ),
        "default_variant": default_variant,
        "available_variants": canonical_variants,
        "condition_prices": dict(selected_variant["condition_prices"]),
    }


def _bind_buylist_search_payload(
    payload: dict[str, Any],
    *,
    employee_id: int,
) -> dict[str, Any]:
    response = deepcopy(payload)
    bound_cards: list[dict[str, Any]] = []
    issued_at = int(time.time())
    for raw_card in response.get("cards") or []:
        if not isinstance(raw_card, dict):
            continue
        try:
            snapshot = _candidate_snapshot(raw_card)
            source = f"search:{snapshot['identity']['item_type']}"
            token = _sign_buylist_payload(
                {
                    "version": BUYLIST_QUOTE_TOKEN_VERSION,
                    "purpose": "buylist_candidate",
                    "source": source,
                    "issued_at": issued_at,
                    "employee_id": employee_id,
                    "candidate": snapshot,
                }
            )
        except BuylistQuoteTokenError:
            logger.warning("Dropping buylist search result with unsafe pricing data")
            continue
        card = deepcopy(raw_card)
        card["candidate_token"] = token
        card["base_market_price"] = snapshot["base_market_price"]
        card["available_variants"] = [
            {
                "name": variant["name"],
                "price": variant["price"],
                "conditions": dict(variant["condition_prices"]),
            }
            for variant in snapshot["available_variants"]
        ]
        card["condition_market_prices"] = dict(snapshot["condition_prices"])
        bound_cards.append(card)
    response["cards"] = bound_cards
    return response


def _parse_ranges(form: dict[str, Any], prefix: str) -> list[dict[str, Any]]:
    ranges: list[dict[str, Any]] = []
    for index in range(8):
        min_value = str(form.get(f"{prefix}_min_{index}") or "").strip()
        max_value = str(form.get(f"{prefix}_max_{index}") or "").strip()
        value = str(form.get(f"{prefix}_value_{index}") or "").strip()
        mode = str(form.get(f"{prefix}_type_{index}") or "percentage").strip().lower()
        if not (min_value or max_value or value):
            continue
        ranges.append(
            {
                "min": _float(min_value, 0.0),
                "max": _float(max_value, 999999.0),
                "type": mode if mode in {"percentage", "fixed", "by_appointment"} else "percentage",
                "value": _float(value, 0.0),
            }
        )
    return ranges


def _parse_modifier_group(form: dict[str, Any], prefix: str, defaults: dict[str, float]) -> dict[str, float]:
    values: dict[str, float] = {}
    for key, default in defaults.items():
        field = f"{prefix}_{re.sub(r'[^a-z0-9]+', '_', key.lower()).strip('_')}"
        values[key] = _float(form.get(field), _float(default, 100.0))
    return values


def _parse_list_rules(raw: str) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    for line in (raw or "").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if "," in text:
            pattern, percent = text.split(",", 1)
        else:
            pattern, percent = text, "0"
        pattern = pattern.strip()
        if pattern:
            rules.append({"pattern": pattern, "percent": _float(percent, 0.0)})
    return rules


def _rules_to_text(rules: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{str(rule.get('pattern') or '').strip()}, {float(rule.get('percent') or 0):g}"
        for rule in rules or []
        if str(rule.get("pattern") or "").strip()
    )


def _json_loads(value: str, fallback: Any, *, label: str = "buylist_json") -> Any:
    try:
        parsed = json.loads(value or "")
    except (TypeError, json.JSONDecodeError):
        logger.warning("Invalid %s; using fallback.", label, exc_info=True)
        return deepcopy(fallback)
    return parsed if parsed is not None else deepcopy(fallback)


def _actor_label(user: Optional[User]) -> str:
    if user is None:
        return "buylist"
    return (
        getattr(user, "display_name", None)
        or getattr(user, "username", None)
        or f"user:{getattr(user, 'id', '')}"
        or "buylist"
    )


def _inventory_game_for_buylist(game: str | None) -> str:
    value = (game or "Pokemon").strip()
    if value == "Pokemon JP":
        return "Pokemon"
    if value == "MTG":
        return "Magic"
    return value or "Pokemon"


def _line_selected_unit_cost(line: dict[str, Any], payment_view: str) -> float:
    if payment_view == "trade":
        return _money(_float(line.get("unit_trade"), _float(line.get("trade_offer"), 0.0)))
    return _money(_float(line.get("unit_cash"), _float(line.get("cash_offer"), 0.0)))


def _verify_stored_buylist_line(
    line: Any,
    *,
    employee_id: int,
) -> dict[str, Any]:
    expected_keys = set(_BUYLIST_ATTESTED_LINE_FIELDS) | {"attestation"}
    if not isinstance(line, dict) or set(line) != expected_keys:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    payload = _verify_buylist_payload(
        line.get("attestation"),
        purpose="buylist_line",
        employee_id=employee_id,
        max_age_seconds=None,
    )
    if set(payload) != {
        "version",
        "purpose",
        "source",
        "issued_at",
        "employee_id",
        "line",
    } or payload.get("source") != "server_quote":
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    canonical = _canonical_attested_line(line)
    if payload.get("line") != canonical:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    if (
        canonical["line_cash"] != _money(canonical["unit_cash"] * canonical["quantity"])
        or canonical["line_trade"] != _money(canonical["unit_trade"] * canonical["quantity"])
    ):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    return {**canonical, "attestation": line["attestation"]}


def _verify_submission_quote(
    submission: BuylistSubmission,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    employee_id = submission.submitted_by_user_id
    if isinstance(employee_id, bool) or not isinstance(employee_id, int) or employee_id <= 0:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    raw_lines = _json_loads(
        submission.lines_json,
        [],
        label=f"buylist_submission.{submission.id}.lines_json",
    )
    if not isinstance(raw_lines, list) or not raw_lines:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    lines = [
        _verify_stored_buylist_line(line, employee_id=employee_id)
        for line in raw_lines
    ]
    if any(line["blocked"] for line in lines):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    trusted_totals = {
        "cash": _money(sum(line["line_cash"] for line in lines)),
        "trade": _money(sum(line["line_trade"] for line in lines)),
        "quantity": sum(line["quantity"] for line in lines),
        "items": len(lines),
    }
    stored_totals = _json_loads(
        submission.totals_json,
        {},
        label=f"buylist_submission.{submission.id}.totals_json",
    )
    if not isinstance(stored_totals, dict) or set(stored_totals) != {
        "cash",
        "trade",
        "quantity",
        "items",
    }:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    try:
        stored_cash = _trusted_buylist_price(
            stored_totals.get("cash"),
            field_name="stored buylist cash total",
        )
        stored_trade = _trusted_buylist_price(
            stored_totals.get("trade"),
            field_name="stored buylist trade total",
        )
    except BuylistQuoteTokenError as exc:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR) from exc
    stored_quantity = stored_totals.get("quantity")
    stored_items = stored_totals.get("items")
    if (
        isinstance(stored_quantity, bool)
        or not isinstance(stored_quantity, int)
        or isinstance(stored_items, bool)
        or not isinstance(stored_items, int)
        or stored_cash != trusted_totals["cash"]
        or stored_trade != trusted_totals["trade"]
        or stored_quantity != trusted_totals["quantity"]
        or stored_items != trusted_totals["items"]
    ):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    return lines, trusted_totals


def _submission_to_view(row: BuylistSubmission, submitter: Optional[User]) -> dict[str, Any]:
    return {
        "row": row,
        "submitter": submitter,
        "totals": _json_loads(row.totals_json, {}, label=f"buylist_submission.{row.id}.totals_json"),
        "lines": _json_loads(row.lines_json, [], label=f"buylist_submission.{row.id}.lines_json"),
        "inventory_result": _json_loads(
            row.inventory_result_json,
            {},
            label=f"buylist_submission.{row.id}.inventory_result_json",
        ),
    }


def _buylist_submission_status_counts(session: Session) -> tuple[dict[str, int], int]:
    counts = {status_key: 0 for status_key in BUYLIST_SUBMISSION_STATUSES}
    unknown_count = 0
    for raw_status in session.exec(select(BuylistSubmission.status)).all():
        status_key = str(raw_status or "").strip()
        if status_key in counts:
            counts[status_key] += 1
        else:
            unknown_count += 1
    return counts, unknown_count


def _receive_submission_inventory(
    session: Session,
    submission: BuylistSubmission,
    *,
    actor: User,
    location: str = "",
) -> dict[str, Any]:
    from ..inventory.routes import _receive_sealed_stock, _receive_single_stock

    payment_view = (submission.payment_view or "cash").strip().lower()
    if payment_view not in {"cash", "trade"}:
        payment_view = "cash"
    lines = _json_loads(
        submission.lines_json,
        [],
        label=f"buylist_submission.{submission.id}.lines_json",
    )
    if not isinstance(lines, list) or not lines:
        raise ValueError("Submission has no line items.")

    created_items: list[dict[str, Any]] = []
    actor_label = _actor_label(actor)
    location = (location or "Buylist intake").strip()
    source = f"Staff Buylist #{submission.id}"
    notes = f"Customer: {submission.customer_name or 'Customer'}"
    if submission.notes:
        notes = f"{notes}; {submission.notes[:400]}"
    legacy_receipt = session.exec(
        select(InventoryStockMovement.id).where(
            InventoryStockMovement.source == source,
            InventoryStockMovement.dedupe_key == None,  # noqa: E711
        )
    ).first()
    if legacy_receipt is not None:
        raise ValueError(
            "Legacy inventory receipt evidence exists for this submission; manual review is required."
        )

    for line_index, line in enumerate(lines):
        if not isinstance(line, dict):
            raise ValueError("Submission contains an invalid line item.")
        receipt_key = f"staff-buylist:{submission.id}:line:{line_index}"
        if session.exec(
            select(InventoryStockMovement.id).where(
                InventoryStockMovement.dedupe_key == receipt_key
            )
        ).first() is not None:
            raise ValueError("Submission line was already received.")
        quantity = max(1, min(int(_float(line.get("quantity"), 1)), 999))
        unit_cost = _line_selected_unit_cost(line, payment_view)
        market_price = _float(line.get("market_price"), _float(line.get("base_market_price"), 0.0))
        item_type = _normalize_product_type(str(line.get("item_type") or BUYLIST_PRODUCT_TYPE_CARD))
        if item_type == BUYLIST_PRODUCT_TYPE_SEALED:
            item, movement, created = _receive_sealed_stock(
                session,
                game=_inventory_game_for_buylist(str(line.get("game") or "")),
                product_name=str(line.get("name") or ""),
                set_name=str(line.get("set_name") or ""),
                sealed_product_kind=str(line.get("sealed_product_kind") or line.get("rarity") or ""),
                upc=str(line.get("upc") or line.get("number") or ""),
                image_url=str(line.get("image_url") or ""),
                quantity=quantity,
                unit_cost=unit_cost,
                list_price=market_price or None,
                location=location,
                source=source,
                notes=notes,
                actor_label=actor_label,
                dedupe_key=receipt_key,
                commit=False,
            )
        else:
            item, movement, created = _receive_single_stock(
                session,
                game=_inventory_game_for_buylist(str(line.get("game") or "")),
                card_name=str(line.get("name") or ""),
                set_name=str(line.get("set_name") or ""),
                set_code=str(line.get("set_code") or ""),
                card_number=str(line.get("number") or ""),
                variant=str(line.get("variant") or "Normal"),
                condition=str(line.get("condition") or "NM"),
                image_url=str(line.get("image_url") or ""),
                quantity=quantity,
                unit_cost=unit_cost,
                list_price=market_price or None,
                auto_price=market_price or None,
                location=location,
                source=source,
                notes=notes,
                price_payload={"buylist_line": line},
                actor_label=actor_label,
                dedupe_key=receipt_key,
                commit=False,
            )
            language = str(line.get("language") or "").strip()
            if not language:
                language = "Japanese" if str(line.get("game") or "").strip() == "Pokemon JP" else "English"
            if language not in LANGUAGE_OPTIONS:
                language = "Other"
            if language and item.language != language:
                item.language = language
                session.add(item)
        created_items.append(
            {
                "inventory_item_id": item.id,
                "movement_id": movement.id,
                "name": item.card_name,
                "item_type": item.item_type,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "created": created,
            }
        )

    if not created_items:
        raise ValueError("No valid line items were available to receive.")
    return {"items": created_items, "location": location, "payment_view": payment_view}


@router.get("/team/buylist", response_class=HTMLResponse)
def staff_buylist_page(request: Request, session: Session = Depends(get_session)):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    config = get_buylist_config(session)
    can_manage_buylist_pricing = _can_manage_buylist_pricing(session, user)
    return templates.TemplateResponse(
        request,
        "team/buylist.html",
        {
            "request": request,
            "title": "Buylist",
            "active": "buylist",
            "current_user": user,
            "config": config,
            "game_options": _enabled_game_options(config),
            "search_default_game": _default_buylist_game(config),
            "condition_options": CONDITION_OPTIONS,
            "language_options": LANGUAGE_OPTIONS,
            "csrf_token": issue_token(request),
            "can_manage_buylist_pricing": can_manage_buylist_pricing,
            **_nav_context(session, user),
        },
    )


@router.get("/team/buylist/search")
async def staff_buylist_search(
    request: Request,
    q: str = Query(default=""),
    game: str = Query(default="Pokemon"),
    product_type: str = Query(default=BUYLIST_PRODUCT_TYPE_CARD),
    session: Session = Depends(get_session),
):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    try:
        _buylist_signing_keys()
    except BuylistQuoteConfigurationError:
        return _buylist_unavailable_response()
    employee_id = getattr(user, "id", None)
    if isinstance(employee_id, bool) or not isinstance(employee_id, int) or employee_id <= 0:
        return JSONResponse({"ok": False, "error": "Employee session is invalid."}, status_code=403)
    config = get_buylist_config(session)
    query = re.sub(r"\s+", " ", (q or "").strip())
    if not query:
        return JSONResponse({"ok": True, "cards": [], "message": "Enter a card name or number."})
    if len(query) < BUYLIST_SEARCH_MIN_CHARS:
        return JSONResponse(
            {
                "ok": True,
                "cards": [],
                "message": f"Type at least {BUYLIST_SEARCH_MIN_CHARS} characters.",
            }
        )
    if len(query) > BUYLIST_SEARCH_MAX_CHARS:
        return JSONResponse(
            {
                "ok": False,
                "cards": [],
                "error": f"Search is too long. Keep it under {BUYLIST_SEARCH_MAX_CHARS} characters.",
            },
            status_code=400,
        )
    selected_game = _default_buylist_game(config) if _is_all_games(game) else (game or _default_buylist_game(config))
    category_id = _category_for_game(selected_game, config)
    product_type = _normalize_product_type(product_type)
    cache_key = _buylist_search_cache_key(
        query,
        category_id,
        config,
        product_type=product_type,
    )
    cached = _buylist_search_cache_get(cache_key)
    if cached:
        return JSONResponse(
            _bind_buylist_search_payload(cached, employee_id=employee_id)
        )

    if product_type == BUYLIST_PRODUCT_TYPE_SEALED:
        game_name = _game_for_category(category_id)
        raw_products, warning = await _search_buylist_sealed_products(
            query,
            game=game_name,
            limit=BUYLIST_SEARCH_RESULT_LIMIT,
        )
        cards = [
            _sealed_product_payload(product, config)
            for product in raw_products
            if isinstance(product, dict)
        ]
        if cards:
            warning = ""
        payload = {
            "ok": True,
            "status": "MATCHED" if cards else "NO_MATCH",
            "product_type": product_type,
            "game": game_name,
            "category_id": category_id,
            "cards": cards,
            "error": warning or ("" if cards else f"No sealed products found for '{query}'."),
            "processing_time_ms": None,
            "cached": False,
        }
        _buylist_search_cache_set(cache_key, payload)
        return JSONResponse(
            _bind_buylist_search_payload(payload, employee_id=employee_id)
        )

    result = await text_search_cards(
        query,
        category_id=category_id,
        use_ai_parse=False,
        max_results=BUYLIST_SEARCH_RESULT_LIMIT,
        include_pokemontcg_supplement=False,
        allow_cross_category_pricing=False,
        allow_pokemontcg_price_fallback=False,
    )
    raw_cards = result.get("candidates") or [] if isinstance(result, dict) else []
    cards = [
        _candidate_payload(card, config, category_id=category_id)
        for card in raw_cards
        if isinstance(card, dict)
    ]
    cards = _dedupe_payloads(cards, limit=BUYLIST_SEARCH_RESULT_LIMIT)
    warning = result.get("error") if isinstance(result, dict) else None
    processing_time_ms = result.get("processing_time_ms") if isinstance(result, dict) else None
    status = result.get("status") if isinstance(result, dict) else None
    game_name = _game_for_category(category_id)
    payload = {
        "ok": True,
        "status": status,
        "product_type": product_type,
        "game": game_name,
        "category_id": category_id,
        "cards": cards,
        "error": warning,
        "processing_time_ms": processing_time_ms,
        "cached": False,
    }
    _buylist_search_cache_set(cache_key, payload)
    return JSONResponse(
        _bind_buylist_search_payload(payload, employee_id=employee_id)
    )


@router.get("/team/buylist/sales-history")
async def staff_buylist_sales_history(
    request: Request,
    product_id: str = Query(default=""),
    tcgplayer_url: str = Query(default=""),
    condition: str = Query(default="NM"),
    variant: str = Query(default=""),
    language: str = Query(default="English"),
    session: Session = Depends(get_session),
):
    denial, _user = _require_team_user(request, session)
    if denial:
        return denial

    resolved_product_id = str(product_id or "").strip()
    if not re.fullmatch(r"\d{1,12}", resolved_product_id):
        resolved_product_id = tcgplayer_product_id_from_url(tcgplayer_url)
    if not resolved_product_id:
        return JSONResponse(
            {"ok": False, "error": "Missing TCGplayer product id", "history": None},
            status_code=400,
        )

    history = await fetch_tcgplayer_public_sales(
        resolved_product_id,
        selected_condition=condition,
        selected_variant=variant,
        selected_language=language,
        product_url=tcgplayer_url,
    )
    errors = history.get("errors") or []
    return JSONResponse(
        {
            "ok": bool(history.get("ok")),
            "history": history,
            "error": "; ".join(str(error) for error in errors if error),
        }
    )


def _validated_candidate_snapshot(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "identity",
        "base_market_price",
        "default_variant",
        "available_variants",
        "condition_prices",
    }:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    identity = value.get("identity")
    if not isinstance(identity, dict) or set(identity) != set(_BUYLIST_IDENTITY_FIELDS):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    canonical_identity: dict[str, str] = {}
    for field in _BUYLIST_IDENTITY_FIELDS:
        field_value = identity.get(field)
        if not isinstance(field_value, str):
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        canonical_identity[field] = field_value
    canonical_identity["item_type"] = _normalize_product_type(canonical_identity["item_type"])

    raw_variants = value.get("available_variants")
    if not isinstance(raw_variants, list) or not raw_variants or len(raw_variants) > BUYLIST_MAX_VARIANTS:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    variants: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_variant in raw_variants:
        if not isinstance(raw_variant, dict) or set(raw_variant) != {
            "name",
            "price",
            "condition_prices",
        }:
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        name = raw_variant.get("name")
        if not isinstance(name, str) or not name.strip() or len(name) > 100:
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        normalized_name = name.strip()
        if normalized_name.casefold() in seen:
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        raw_conditions = raw_variant.get("condition_prices")
        if not isinstance(raw_conditions, dict) or any(
            condition not in CONDITION_OPTIONS for condition in raw_conditions
        ):
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        conditions = {
            condition: _trusted_buylist_price(
                raw_conditions[condition],
                field_name=f"candidate {normalized_name} {condition} price",
            )
            for condition in raw_conditions
        }
        variants.append(
            {
                "name": normalized_name,
                "price": _trusted_buylist_price(
                    raw_variant.get("price"),
                    field_name=f"candidate {normalized_name} price",
                ),
                "condition_prices": conditions,
            }
        )
        seen.add(normalized_name.casefold())

    default_variant = value.get("default_variant")
    if not isinstance(default_variant, str):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    selected = _select_variant(variants, default_variant)
    if selected is None or selected["name"].casefold() != default_variant.casefold():
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    condition_prices = value.get("condition_prices")
    if not isinstance(condition_prices, dict) or any(
        condition not in CONDITION_OPTIONS for condition in condition_prices
    ):
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    canonical_conditions = {
        condition: _trusted_buylist_price(
            condition_prices[condition],
            field_name=f"candidate {condition} price",
        )
        for condition in condition_prices
    }
    return {
        "identity": canonical_identity,
        "base_market_price": _trusted_buylist_price(
            value.get("base_market_price"),
            field_name="candidate base market price",
        ),
        "default_variant": selected["name"],
        "available_variants": variants,
        "condition_prices": canonical_conditions,
    }


def _selected_candidate_variant(
    snapshot: dict[str, Any],
    requested_variant: Any,
) -> dict[str, Any]:
    requested = str(requested_variant or snapshot["default_variant"]).strip()
    for variant in snapshot["available_variants"]:
        if variant["name"].casefold() == requested.casefold():
            return variant
    raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)


def _canonical_attested_line(line: dict[str, Any]) -> dict[str, Any]:
    if any(field not in line for field in _BUYLIST_ATTESTED_LINE_FIELDS):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    canonical = {field: deepcopy(line[field]) for field in _BUYLIST_ATTESTED_LINE_FIELDS}
    for field in (
        "market_price",
        "base_market_price",
        "unit_cash",
        "unit_trade",
        "line_cash",
        "line_trade",
    ):
        canonical[field] = _trusted_buylist_price(
            canonical[field],
            field_name=f"buylist line {field}",
        )
    condition_market = canonical.get("condition_market_price")
    if condition_market is not None:
        canonical["condition_market_price"] = _trusted_buylist_price(
            condition_market,
            field_name="buylist line condition market price",
        )
    quantity = canonical.get("quantity")
    if isinstance(quantity, bool) or not isinstance(quantity, int) or not 1 <= quantity <= 999:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    if not isinstance(canonical.get("blocked"), bool):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    notes = canonical.get("pricing_notes")
    if not isinstance(notes, list) or any(not isinstance(note, str) for note in notes):
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    return canonical


def _quote_signed_candidate(
    raw: dict[str, Any],
    *,
    employee_id: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    payload = _verify_buylist_payload(
        raw.get("candidate_token"),
        purpose="buylist_candidate",
        employee_id=employee_id,
        max_age_seconds=BUYLIST_CANDIDATE_TOKEN_MAX_AGE_SECONDS,
    )
    snapshot = _validated_candidate_snapshot(payload.get("candidate"))
    identity = snapshot["identity"]
    item_type = identity["item_type"]
    quantity = raw.get("quantity", 1)
    if isinstance(quantity, bool) or not isinstance(quantity, int) or not 1 <= quantity <= 999:
        raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)

    selected_variant = _selected_candidate_variant(snapshot, raw.get("variant"))
    variant = selected_variant["name"]
    if item_type == BUYLIST_PRODUCT_TYPE_SEALED:
        condition = "Sealed"
        language = ""
        if raw.get("condition") not in (None, "", "Sealed") or raw.get("language") not in (None, ""):
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
    else:
        condition = str(raw.get("condition") or "NM").strip().upper()
        if condition == "DM":
            condition = "DMG"
        if condition not in CONDITION_OPTIONS:
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)
        language = str(raw.get("language") or "English").strip()
        if language not in LANGUAGE_OPTIONS:
            raise BuylistQuoteTokenError(BUYLIST_QUOTE_INVALID_ERROR)

    base_market_price = _trusted_buylist_price(
        selected_variant["price"],
        field_name="selected variant price",
    )
    condition_market_price = selected_variant["condition_prices"].get(condition)
    offer = calculate_buylist_offer(
        config,
        market_price=base_market_price,
        condition_market_price=condition_market_price,
        condition=condition,
        language=language,
        printing=variant,
        product=identity,
    )
    unit_cash = _trusted_buylist_price(offer["cash_offer"], field_name="cash offer")
    unit_trade = _trusted_buylist_price(offer["trade_offer"], field_name="trade offer")
    line_cash = _trusted_buylist_price(unit_cash * quantity, field_name="cash line total")
    line_trade = _trusted_buylist_price(unit_trade * quantity, field_name="trade line total")
    quoted_at_epoch = int(time.time())
    line: dict[str, Any] = {
        **identity,
        "quantity": quantity,
        "condition": condition,
        "language": language,
        "variant": variant,
        "market_price": _trusted_buylist_price(offer["market_price"], field_name="market price"),
        "base_market_price": _trusted_buylist_price(
            offer["base_market_price"],
            field_name="base market price",
        ),
        "condition_market_price": (
            _trusted_buylist_price(
                offer["condition_market_price"],
                field_name="condition market price",
            )
            if offer["condition_market_price"] is not None
            else None
        ),
        "condition_price_source": _bounded_buylist_text(
            offer["condition_price_source"], max_chars=100
        ),
        "unit_cash": unit_cash,
        "unit_trade": unit_trade,
        "line_cash": line_cash,
        "line_trade": line_trade,
        "pricing_notes": [
            _bounded_buylist_text(note, max_chars=300) for note in offer["notes"]
        ],
        "blocked": bool(offer["blocked"]),
        "quote_source": payload["source"],
        "candidate_issued_at": datetime.fromtimestamp(
            payload["issued_at"], tz=timezone.utc
        ).isoformat(),
        "quoted_at": datetime.fromtimestamp(quoted_at_epoch, tz=timezone.utc).isoformat(),
    }
    canonical_line = _canonical_attested_line(line)
    line["attestation"] = _sign_buylist_payload(
        {
            "version": BUYLIST_QUOTE_TOKEN_VERSION,
            "purpose": "buylist_line",
            "source": "server_quote",
            "issued_at": quoted_at_epoch,
            "employee_id": employee_id,
            "line": canonical_line,
        }
    )
    line["candidate_token"] = raw["candidate_token"]
    return line


def _stored_buylist_line(line: dict[str, Any]) -> dict[str, Any]:
    canonical = _canonical_attested_line(line)
    attestation = line.get("attestation")
    if not isinstance(attestation, str) or not attestation:
        raise BuylistQuoteTokenError(BUYLIST_ATTESTATION_INVALID_ERROR)
    return {**canonical, "attestation": attestation}


@router.post("/team/buylist/quote")
async def staff_buylist_quote(request: Request, session: Session = Depends(get_session)):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    try:
        _buylist_signing_keys()
    except BuylistQuoteConfigurationError:
        return _buylist_unavailable_response()
    employee_id = getattr(user, "id", None)
    if isinstance(employee_id, bool) or not isinstance(employee_id, int) or employee_id <= 0:
        return JSONResponse({"ok": False, "error": "Employee session is invalid."}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)
    raw_items = body.get("items") if isinstance(body, dict) else None
    if (
        not isinstance(raw_items, list)
        or not 1 <= len(raw_items) <= BUYLIST_MAX_QUOTE_ITEMS
    ):
        return JSONResponse({"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR}, status_code=400)
    config = get_buylist_config(session)
    lines: list[dict[str, Any]] = []
    totals = {"cash": 0.0, "trade": 0.0, "quantity": 0}
    for raw in raw_items:
        if not isinstance(raw, dict):
            return JSONResponse({"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR}, status_code=400)
        try:
            line = _quote_signed_candidate(
                raw,
                employee_id=employee_id,
                config=config,
            )
        except BuylistQuoteConfigurationError:
            return _buylist_unavailable_response()
        except BuylistQuoteTokenError:
            return JSONResponse(
                {"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR},
                status_code=400,
            )
        totals["cash"] += line["line_cash"]
        totals["trade"] += line["line_trade"]
        totals["quantity"] += line["quantity"]
        lines.append(line)
    try:
        trusted_cash_total = _trusted_buylist_price(
            totals["cash"],
            field_name="buylist cash total",
        )
        trusted_trade_total = _trusted_buylist_price(
            totals["trade"],
            field_name="buylist trade total",
        )
    except BuylistQuoteTokenError:
        return JSONResponse(
            {"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR},
            status_code=400,
        )
    return JSONResponse(
        {
            "ok": True,
            "lines": lines,
            "totals": {
                "cash": trusted_cash_total,
                "trade": trusted_trade_total,
                "quantity": int(totals["quantity"]),
                "items": len(lines),
            },
        }
    )


@router.post("/team/buylist/save", dependencies=[Depends(require_csrf)])
async def staff_buylist_save(request: Request, session: Session = Depends(get_session)):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)

    quote_response = await staff_buylist_quote(request, session)
    if getattr(quote_response, "status_code", 200) >= 400:
        return quote_response
    quote_payload = json.loads(quote_response.body.decode("utf-8"))
    if not quote_payload.get("ok"):
        return quote_response
    blocked_lines = [
        line for line in quote_payload.get("lines") or [] if isinstance(line, dict) and line.get("blocked")
    ]
    if blocked_lines:
        return JSONResponse(
            {
                "ok": False,
                "error": "Remove manager-review items before saving this quote.",
            },
            status_code=400,
        )
    try:
        stored_lines = [
            _stored_buylist_line(line)
            for line in quote_payload.get("lines") or []
            if isinstance(line, dict)
        ]
    except BuylistQuoteTokenError:
        return JSONResponse(
            {"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR},
            status_code=400,
        )
    if len(stored_lines) != len(quote_payload.get("lines") or []):
        return JSONResponse(
            {"ok": False, "error": BUYLIST_QUOTE_INVALID_ERROR},
            status_code=400,
        )

    details = {
        "customer_name": str(body.get("customer_name") or "").strip()[:200],
        "customer_contact": str(body.get("customer_contact") or "").strip()[:200],
        "payment_view": str(body.get("payment_view") or "").strip()[:20],
        "notes": str(body.get("notes") or "").strip()[:2000],
        "totals": quote_payload.get("totals") or {},
        "lines": stored_lines,
    }
    payment_view = details["payment_view"] if details["payment_view"] in {"cash", "trade"} else "cash"
    submission = BuylistSubmission(
        submitted_by_user_id=user.id if user else 0,
        customer_name=details["customer_name"],
        customer_contact=details["customer_contact"],
        payment_view=payment_view,
        status="submitted",
        totals_json=json.dumps(details["totals"], sort_keys=True),
        lines_json=json.dumps(details["lines"], sort_keys=True),
        notes=details["notes"],
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    session.add(submission)
    session.commit()
    session.refresh(submission)
    audit_details = {
        "buylist_submission_id": submission.id,
        "submitted_by_user_id": user.id if user else None,
        "payment_view": payment_view,
        "totals": details["totals"],
        "line_count": len(details["lines"]),
        "has_customer_name": bool(details["customer_name"]),
        "has_customer_contact": bool(details["customer_contact"]),
        "has_notes": bool(details["notes"]),
    }
    session.add(
        AuditLog(
            actor_user_id=user.id if user else None,
            action="staff_buylist.quote_saved",
            resource_key="team.buylist",
            details_json=json.dumps(audit_details, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return JSONResponse(
        {
            "ok": True,
            "quote": quote_payload,
            "submission_id": submission.id,
            "message": "Buylist submitted",
        }
    )


@router.get("/team/admin/buylist/submissions", response_class=HTMLResponse)
def admin_buylist_submissions_page(
    request: Request,
    status: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.view")
    if denial:
        return denial
    filter_status = status if status in BUYLIST_SUBMISSION_STATUSES else None
    stmt = select(BuylistSubmission)
    if filter_status:
        stmt = stmt.where(BuylistSubmission.status == filter_status)
    stmt = stmt.order_by(BuylistSubmission.created_at.desc(), BuylistSubmission.id.desc())
    rows = list(session.exec(stmt).all())

    submitter_ids = {row.submitted_by_user_id for row in rows}
    submitters: dict[int, User] = {}
    if submitter_ids:
        submitters = {
            row.id: row
            for row in session.exec(select(User).where(User.id.in_(submitter_ids))).all()
        }
    counts, unknown_status_count = _buylist_submission_status_counts(session)

    return templates.TemplateResponse(
        request,
        "team/admin/buylist_submissions.html",
        {
            "request": request,
            "title": "Buylist queue",
            "active": "buylist-submissions",
            "current_user": user,
            "submissions": [
                _submission_to_view(row, submitters.get(row.submitted_by_user_id))
                for row in rows
            ],
            "filter_status": filter_status,
            "statuses": BUYLIST_SUBMISSION_STATUSES,
            "counts": counts,
            "unknown_status_count": unknown_status_count,
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/approve",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_approve(
    request: Request,
    submission_id: int,
    location: str = Form(default="Buylist intake"),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "submitted":
        raise HTTPException(status_code=409, detail=f"Cannot approve {row.status} submission")

    try:
        claim_time = utcnow()
        claim_result = session.exec(
            update(BuylistSubmission)
            .where(
                BuylistSubmission.id == submission_id,
                BuylistSubmission.status == "submitted",
            )
            .values(status="approving", updated_at=claim_time)
            .execution_options(synchronize_session=False)
        )
        if claim_result.rowcount != 1:
            session.rollback()
            raise HTTPException(status_code=409, detail="Submission is already being processed")
        row.status = "approving"
        _buylist_signing_keys()
        _verified_lines, trusted_totals = _verify_submission_quote(row)
        inventory_result = _receive_submission_inventory(
            session,
            row,
            actor=user,
            location=location,
        )
        now = utcnow()
        row.status = "approved"
        row.approved_by_user_id = user.id
        row.approved_at = now
        row.status_changed_at = now
        row.updated_at = now
        row.inventory_result_json = json.dumps(inventory_result, sort_keys=True)
        session.add(row)
        session.add(
            AuditLog(
                actor_user_id=user.id,
                action="staff_buylist.approved",
                resource_key=f"team.buylist.{row.id}",
                details_json=json.dumps(
                    {
                        "buylist_submission_id": row.id,
                        "inventory_result": inventory_result,
                        "quote_attestation_verified": True,
                        "trusted_totals": trusted_totals,
                    },
                    sort_keys=True,
                ),
                ip_address=(request.client.host if request.client else None),
            )
        )
        session.commit()
    except BuylistQuoteConfigurationError as exc:
        session.rollback()
        raise HTTPException(status_code=503, detail=BUYLIST_QUOTE_UNAVAILABLE_ERROR) from exc
    except BuylistQuoteTokenError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=BUYLIST_ATTESTATION_INVALID_ERROR) from exc
    except HTTPException:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        logger.exception("Atomic buylist approval failed for submission %s", submission_id)
        raise HTTPException(status_code=409, detail=BUYLIST_APPROVAL_ATOMIC_ERROR) from exc
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Approved+and+received+into+inventory.",
        status_code=303,
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/reject",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_reject(
    request: Request,
    submission_id: int,
    decision_notes: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "submitted":
        raise HTTPException(status_code=409, detail=f"Cannot reject {row.status} submission")
    now = utcnow()
    clean_notes = decision_notes.strip()[:2000]
    claim = session.exec(
        update(BuylistSubmission)
        .where(
            BuylistSubmission.id == submission_id,
            BuylistSubmission.status == "submitted",
        )
        .values(
            status="rejected",
            rejected_by_user_id=user.id,
            rejected_at=now,
            status_changed_at=now,
            updated_at=now,
            decision_notes=clean_notes,
        )
        .execution_options(synchronize_session=False)
    )
    if claim.rowcount != 1:
        session.rollback()
        raise HTTPException(status_code=409, detail="Submission is already being processed")
    session.add(
        AuditLog(
            actor_user_id=user.id,
            action="staff_buylist.rejected",
            resource_key=f"team.buylist.{row.id}",
            details_json=json.dumps(
                {"buylist_submission_id": row.id, "notes": clean_notes},
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    session.expire(row)
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Rejected.",
        status_code=303,
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/mark-paid",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_mark_paid(
    request: Request,
    submission_id: int,
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "approved":
        raise HTTPException(status_code=409, detail=f"Cannot mark {row.status} submission paid")
    now = utcnow()
    claim = session.exec(
        update(BuylistSubmission)
        .where(
            BuylistSubmission.id == submission_id,
            BuylistSubmission.status == "approved",
        )
        .values(
            status="paid",
            paid_by_user_id=user.id,
            paid_at=now,
            status_changed_at=now,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if claim.rowcount != 1:
        session.rollback()
        raise HTTPException(status_code=409, detail="Submission payment status already changed")
    session.add(
        AuditLog(
            actor_user_id=user.id,
            action="staff_buylist.paid",
            resource_key=f"team.buylist.{row.id}",
            details_json=json.dumps({"buylist_submission_id": row.id}, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    session.expire(row)
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Marked+paid.",
        status_code=303,
    )


@router.get("/team/admin/buylist", response_class=HTMLResponse)
def staff_buylist_admin_page(
    request: Request,
    saved: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_buylist_admin(request, session)
    if denial:
        return denial
    config = get_buylist_config(session)
    return templates.TemplateResponse(
        request,
        "team/admin/buylist.html",
        {
            "request": request,
            "title": "Buylist Pricing",
            "active": "buylist",
            "current_user": user,
            "config": config,
            "game_options": BUYLIST_GAMES,
            "condition_options": CONDITION_OPTIONS,
            "language_options": LANGUAGE_OPTIONS,
            "cash_ranges": (config.get("cash_ranges") or [])[:8],
            "trade_ranges": (config.get("trade_ranges") or [])[:8],
            "hotlist_text": _rules_to_text(config.get("hotlist_rules") or []),
            "darklist_text": _rules_to_text(config.get("darklist_rules") or []),
            "saved": saved,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/admin/buylist", dependencies=[Depends(require_csrf)])
async def staff_buylist_admin_save(
    request: Request,
    enabled_games: list[str] = Form(default=[]),
    default_game: str = Form(default="Pokemon"),
    default_payment: str = Form(default="cash"),
    condition_pricing_mode: str = Form(default=CONDITION_PRICING_TCGPLAYER),
    checkout_note: str = Form(default=""),
    hotlist_rules: str = Form(default=""),
    darklist_rules: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, _user = _require_buylist_admin(request, session)
    if denial:
        return denial
    form = dict(await request.form())
    enabled = [game for game in enabled_games if game.lower() in _GAME_BY_NAME]
    if not enabled:
        enabled = list(DEFAULT_BUYLIST_CONFIG["enabled_games"])
    if default_game.lower() not in _GAME_BY_NAME or default_game not in enabled:
        default_game = enabled[0]
    default_payment = (default_payment or "cash").strip().lower()
    if default_payment not in {"cash", "trade"}:
        default_payment = "cash"
    condition_pricing_mode = (condition_pricing_mode or CONDITION_PRICING_TCGPLAYER).strip().lower()
    if condition_pricing_mode not in CONDITION_PRICING_MODES:
        condition_pricing_mode = CONDITION_PRICING_TCGPLAYER

    config = get_buylist_config(session)
    config.update(
        {
            "enabled_games": enabled,
            "default_game": default_game,
            "default_payment": default_payment,
            "condition_pricing_mode": condition_pricing_mode,
            "cash_ranges": _parse_ranges(form, "cash"),
            "trade_ranges": _parse_ranges(form, "trade"),
            "condition_modifiers": _parse_modifier_group(
                form,
                "condition",
                DEFAULT_BUYLIST_CONFIG["condition_modifiers"],
            ),
            "language_modifiers": _parse_modifier_group(
                form,
                "language",
                DEFAULT_BUYLIST_CONFIG["language_modifiers"],
            ),
            "printing_modifiers": _parse_modifier_group(
                form,
                "printing",
                DEFAULT_BUYLIST_CONFIG["printing_modifiers"],
            ),
            "hotlist_rules": _parse_list_rules(hotlist_rules),
            "darklist_rules": _parse_list_rules(darklist_rules),
            "checkout_note": checkout_note.strip()[:2000],
        }
    )
    if not config["cash_ranges"]:
        config["cash_ranges"] = deepcopy(DEFAULT_BUYLIST_CONFIG["cash_ranges"])
    if not config["trade_ranges"]:
        config["trade_ranges"] = deepcopy(DEFAULT_BUYLIST_CONFIG["trade_ranges"])
    save_buylist_config(session, config)
    return RedirectResponse("/team/admin/buylist?saved=1", status_code=303)
