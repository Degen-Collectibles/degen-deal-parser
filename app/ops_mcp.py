from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html import unescape
import json
import os
import re
from typing import Any, Callable, Iterator
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
from sqlalchemy import text
from sqlmodel import Session, func, select

from .db import Session as DbSession
from .db import database_url
from .db import engine
from .models import (
    BuylistSubmission,
    ClockifyTimeEntry,
    EmployeeProfile,
    InventoryItem,
    OpsBotMemory,
    PriceHistory,
    ShopifyOrder,
    SupplyRequest,
    TikTokOrder,
    TikTokProduct,
    TimeOffRequest,
    User,
)
from .ops_agent import READ_ONLY_GUARDRAILS, build_ops_agent_context, build_ops_agent_recommendation
from .reporting import (
    build_tiktok_buyer_insights,
    build_tiktok_product_performance,
    build_tiktok_reporting_summary,
    external_order_net_revenue,
    get_financial_rows,
    get_shopify_reporting_rows,
    get_tiktok_order_rows,
    load_paid_tiktok_orders,
    normalize_item,
    parse_line_items,
)


DEGEN_OPS_BUSINESS_TOOL_NAMES = [
    "get_ops_agent_manifest",
    "get_ops_memory",
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_sales_summary",
    "get_discord_sales_summary",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
    "generate_weekly_partner_update_draft",
]

TIKTOK_MCP_TOOL_NAMES = [
    "get_tiktok_agent_manifest",
    "get_tiktok_status",
    "get_tiktok_orders",
    "get_tiktok_products",
    "get_tiktok_product_sales",
    "get_tiktok_top_products",
    "get_price_lookup",
    "get_market_trend_lookup",
    "get_tiktok_buyer_insights",
    "get_tiktok_product_performance",
    "get_tiktok_live_snapshot",
]

SHOPIFY_MCP_TOOL_NAMES = [
    "get_shopify_product_sales",
    "get_shopify_top_products",
]

WEB_MCP_TOOL_NAMES = [
    "get_web_search",
]

DEGEN_OPS_MCP_TOOL_NAMES = [
    *DEGEN_OPS_BUSINESS_TOOL_NAMES,
    *TIKTOK_MCP_TOOL_NAMES,
    *SHOPIFY_MCP_TOOL_NAMES,
    "get_employee_clock_status",
    "get_employee_ops_status",
    "propose_ops_memory",
    *WEB_MCP_TOOL_NAMES,
]

DEGEN_OPS_SCOPE_TOOL_NAMES = {
    "owner": DEGEN_OPS_MCP_TOOL_NAMES,
    "tiktok": [
        "get_ops_agent_manifest",
        "get_ops_memory",
        *TIKTOK_MCP_TOOL_NAMES,
        *WEB_MCP_TOOL_NAMES,
    ],
    "partner": [
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "get_sales_summary",
        "get_discord_sales_summary",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_price_lookup",
        "get_market_trend_lookup",
        "get_web_search",
        "evaluate_inventory_buy",
        "generate_partner_update",
        "generate_weekly_partner_update_draft",
    ],
    "manager": [
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "get_sales_summary",
        "get_discord_sales_summary",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_price_lookup",
        "get_market_trend_lookup",
        "get_web_search",
        "get_employee_clock_status",
        "get_employee_ops_status",
    ],
    "employee": [
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "get_sales_summary",
        "get_discord_sales_summary",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_price_lookup",
        "get_market_trend_lookup",
        "get_web_search",
    ],
}

OWNER_FINANCIAL_TOOL_NAMES = [
    "get_cash_snapshot",
    "get_loan_and_payback_snapshot",
]

OWNER_ONLY_TOOL_NAMES = [
    *OWNER_FINANCIAL_TOOL_NAMES,
    "get_employee_clock_status",
    "get_employee_ops_status",
    "propose_ops_memory",
]

PARTNER_REDACTION_NOTE = (
    "Partner scope hides raw cash balances, account balances, reserve-gap dollars, "
    "and owner loan/payback totals. Use owner scope for exact cash and loan ledger evidence."
)

TIKTOK_READ_ONLY_GUARDRAILS = [
    "Read-only TikTok decision support",
    "No TikTok product creates, updates, deletes, uploads, or token changes",
    "No webhook replay or order mutation",
    "Use existing local TikTok tables, reporting helpers, and live-status cache first",
]

TIKTOK_API_ENDPOINT_CATALOG = [
    {
        "id": "shop_token_get",
        "method": "GET",
        "path": "/api/v2/token/get",
        "token": "none",
        "purpose": "Exchange a TikTok Shop authorization code for a seller token.",
        "enabled": False,
        "approval_required": True,
        "reason": "OAuth/token mutation is not exposed through this read-only MCP.",
    },
    {
        "id": "shop_token_refresh",
        "method": "GET",
        "path": "/api/v2/token/refresh",
        "token": "none",
        "purpose": "Refresh a TikTok Shop seller token.",
        "enabled": False,
        "approval_required": True,
        "reason": "Token refresh is handled by the app background flow, not this agent.",
    },
    {
        "id": "creator_token",
        "method": "POST",
        "path": "/v2/oauth/token/",
        "token": "none",
        "purpose": "Exchange or refresh a TikTok Creator token.",
        "enabled": False,
        "approval_required": True,
        "reason": "Creator OAuth changes are not exposed through this read-only MCP.",
    },
    {
        "id": "order_search",
        "method": "POST",
        "path": "/order/202309/orders/search",
        "token": "shop",
        "purpose": "Search TikTok Shop orders.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_orders",
        "source": "local_tiktok_orders",
    },
    {
        "id": "order_detail",
        "method": "GET",
        "path": "/order/202309/orders",
        "token": "shop",
        "purpose": "Fetch full TikTok Shop order details.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_orders",
        "source": "local_tiktok_orders",
    },
    {
        "id": "seller_affiliate_order_search",
        "method": "POST",
        "path": "/affiliate_seller/202410/orders/search",
        "token": "shop",
        "purpose": "Search seller-side affiliate order attribution.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_orders",
        "source": "local_tiktok_orders.affiliate_creator_username",
    },
    {
        "id": "creator_affiliate_order_search",
        "method": "POST",
        "path": "/affiliate_creator/202410/orders/search",
        "token": "creator",
        "purpose": "Search creator-owned affiliate order traces.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_orders",
        "source": "local_tiktok_orders.affiliate_creator_username",
    },
    {
        "id": "product_search",
        "method": "POST",
        "path": "/product/202309/products/search",
        "token": "shop",
        "purpose": "Search TikTok Shop products.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_products",
        "source": "local_tiktok_products",
    },
    {
        "id": "product_detail",
        "method": "GET",
        "path": "/product/202309/products",
        "token": "shop",
        "purpose": "Fetch TikTok Shop product details.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_products",
        "source": "local_tiktok_products",
    },
    {
        "id": "product_create",
        "method": "POST",
        "path": "/product/202309/products",
        "token": "shop",
        "purpose": "Create a TikTok Shop product.",
        "enabled": False,
        "approval_required": True,
        "reason": "Product mutation is not exposed through this read-only MCP.",
    },
    {
        "id": "product_update",
        "method": "PUT",
        "path": "/product/202309/products",
        "token": "shop",
        "purpose": "Update a TikTok Shop product.",
        "enabled": False,
        "approval_required": True,
        "reason": "Product mutation is not exposed through this read-only MCP.",
    },
    {
        "id": "product_image_upload",
        "method": "POST",
        "path": "/product/202309/images/upload",
        "token": "shop",
        "purpose": "Upload a TikTok Shop product image.",
        "enabled": False,
        "approval_required": True,
        "reason": "Media/product mutation is not exposed through this read-only MCP.",
    },
    {
        "id": "product_categories",
        "method": "GET",
        "path": "/product/202309/categories",
        "token": "shop",
        "purpose": "List TikTok Shop product categories.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_agent_manifest",
        "source": "documented_endpoint_catalog",
    },
    {
        "id": "product_brands",
        "method": "GET",
        "path": "/product/202309/brands",
        "token": "shop",
        "purpose": "List TikTok Shop brands.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_agent_manifest",
        "source": "documented_endpoint_catalog",
    },
    {
        "id": "live_session_list",
        "method": "GET",
        "path": "/analytics/202509/shop_lives/performance",
        "token": "shop",
        "purpose": "List TikTok live sessions and per-stream GMV.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_live_snapshot",
        "source": "local_live_cache",
    },
    {
        "id": "live_overview_performance",
        "method": "GET",
        "path": "/analytics/202509/shop_lives/overview_performance",
        "token": "shop",
        "purpose": "Fetch TikTok live overview performance.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_live_snapshot",
        "source": "local_live_cache",
    },
    {
        "id": "live_per_minute_performance",
        "method": "GET",
        "path": "/analytics/202510/shop_lives/{live_id}/performance_per_minutes",
        "token": "shop",
        "purpose": "Fetch per-minute performance for an ended live stream.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_live_snapshot",
        "source": "local_live_cache",
    },
    {
        "id": "live_product_performance",
        "method": "GET",
        "path": "/analytics/202512/shop/{live_id}/products_performance",
        "token": "shop",
        "purpose": "Fetch product-level live performance.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_live_snapshot",
        "source": "local_live_cache",
    },
    {
        "id": "live_core_stats",
        "method": "GET",
        "path": "/analytics/202502/live_rooms/{live_room_id}/core_stats",
        "token": "creator",
        "purpose": "Fetch real-time Creator-token live GMV stats.",
        "enabled": True,
        "approval_required": False,
        "tool": "get_tiktok_live_snapshot",
        "source": "local_live_cache",
    },
    {
        "id": "order_webhook",
        "method": "POST",
        "path": "/webhooks/tiktok/orders",
        "token": "webhook_signature",
        "purpose": "Receive TikTok order webhook events.",
        "enabled": False,
        "approval_required": True,
        "reason": "Webhook receive/replay is not exposed through this read-only MCP.",
    },
]

TIKTOK_LOCAL_ROUTE_CATALOG = [
    {"method": "GET", "path": "/tiktok", "purpose": "TikTok landing redirect"},
    {"method": "GET", "path": "/tiktok/orders", "purpose": "Order list UI"},
    {"method": "GET", "path": "/tiktok/orders/poll", "purpose": "Order poll API"},
    {"method": "POST", "path": "/tiktok/orders/sync-form", "purpose": "Manual order sync trigger", "mutating": True},
    {"method": "GET", "path": "/tiktok/products", "purpose": "Product list UI"},
    {"method": "GET", "path": "/tiktok/products/poll", "purpose": "Product poll API"},
    {"method": "POST", "path": "/tiktok/products/sync-form", "purpose": "Manual product sync trigger", "mutating": True},
    {"method": "GET", "path": "/tiktok/products/categories", "purpose": "Category lookup API"},
    {"method": "GET", "path": "/tiktok/products/categories/{category_id}/attributes", "purpose": "Category attributes API"},
    {"method": "GET", "path": "/tiktok/products/brands", "purpose": "Brand lookup API"},
    {"method": "POST", "path": "/tiktok/products/upload-image", "purpose": "Product image upload", "mutating": True},
    {"method": "GET", "path": "/tiktok/products/new", "purpose": "New product UI", "mutating": True},
    {"method": "POST", "path": "/tiktok/products/create", "purpose": "Product create", "mutating": True},
    {"method": "GET", "path": "/tiktok/products/{product_id}", "purpose": "Product detail UI"},
    {"method": "GET", "path": "/tiktok/analytics/api/debug", "purpose": "Analytics debug API"},
    {"method": "GET", "path": "/tiktok/analytics", "purpose": "Analytics UI"},
    {"method": "GET", "path": "/tiktok/analytics/api/daily", "purpose": "Daily analytics API"},
    {"method": "GET", "path": "/tiktok/analytics/api/streams", "purpose": "Stream list API"},
    {"method": "GET", "path": "/tiktok/analytics/api/stream/{live_id}", "purpose": "Stream detail API"},
    {"method": "GET", "path": "/tiktok/analytics/api/buyers", "purpose": "Buyer analytics API"},
    {"method": "GET", "path": "/tiktok/analytics/api/products", "purpose": "Product analytics API"},
    {"method": "GET", "path": "/tiktok/analytics/api/compare", "purpose": "Stream comparison API"},
    {"method": "GET", "path": "/tiktok/clients", "purpose": "Client intelligence UI"},
    {"method": "GET", "path": "/tiktok/clients/api/buyers", "purpose": "Client buyer list API"},
    {"method": "GET", "path": "/tiktok/clients/api/products", "purpose": "Client product list API"},
    {"method": "GET", "path": "/tiktok/clients/api/buyers/{buyer_key}", "purpose": "Buyer detail API"},
    {"method": "GET", "path": "/tiktok/clients/api/products/{product_key:path}", "purpose": "Product detail analytics API"},
    {"method": "GET", "path": "/tiktok/streamer", "purpose": "Streamer dashboard UI"},
    {"method": "GET", "path": "/tiktok/streamer/poll", "purpose": "Streamer poll API"},
    {"method": "GET", "path": "/tiktok/streamer/goal", "purpose": "GMV goal read API"},
    {"method": "POST", "path": "/tiktok/streamer/goal", "purpose": "GMV goal update", "mutating": True},
    {"method": "GET", "path": "/tiktok/streamer/surprise-set-prices", "purpose": "Surprise-set price editor UI"},
    {"method": "POST", "path": "/tiktok/streamer/surprise-set-price", "purpose": "Manual surprise-set price update", "mutating": True},
    {"method": "GET", "path": "/tiktok/streamer/config", "purpose": "Streamer config UI"},
    {"method": "POST", "path": "/tiktok/streamer/config", "purpose": "Streamer config update", "mutating": True},
    {"method": "POST", "path": "/tiktok/streamer/config/auto", "purpose": "Auto-set streamer config", "mutating": True},
    {"method": "GET", "path": "/tiktok/streamer/chat/poll", "purpose": "Live chat poll API"},
    {"method": "POST", "path": "/tiktok/streamer/giveaway/start", "purpose": "Giveaway start", "mutating": True},
    {"method": "GET", "path": "/tiktok/streamer/giveaway/status", "purpose": "Giveaway status API"},
    {"method": "POST", "path": "/tiktok/streamer/giveaway/cancel", "purpose": "Giveaway cancel", "mutating": True},
    {"method": "GET", "path": "/tiktok/streamer/giveaway/health", "purpose": "Giveaway health API"},
    {"method": "OPTIONS", "path": "/public/tiktok/live-status", "purpose": "Public live-status CORS preflight"},
    {"method": "GET", "path": "/public/tiktok/live-status", "purpose": "Public read-only live status"},
    {"method": "POST", "path": "/webhooks/tiktok/orders", "purpose": "TikTok order webhook receiver", "mutating": True},
]


@contextmanager
def _default_session_factory() -> Iterator[Session]:
    with DbSession(engine) as session:
        _apply_session_read_only(session, database_url)
        try:
            yield session
        finally:
            _reset_session_read_only(session, database_url)


def _is_postgres_url(db_url: str) -> bool:
    normalized = (db_url or "").lower()
    return normalized.startswith(("postgresql://", "postgresql+", "postgres://"))


def _apply_session_read_only(session: Any, db_url: str) -> None:
    if not hasattr(session, "exec"):
        return
    if _is_postgres_url(db_url):
        session.exec(text("SET TRANSACTION READ ONLY"))
    elif db_url.startswith("sqlite"):
        session.exec(text("PRAGMA query_only = ON"))


def _reset_session_read_only(session: Any, db_url: str) -> None:
    if not hasattr(session, "exec"):
        return
    if db_url.startswith("sqlite"):
        session.exec(text("PRAGMA query_only = OFF"))


def _money(value: Any) -> float:
    try:
        return round(float(value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _format_money(value: Any) -> str:
    amount = _money(value)
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.0f}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bounded_limit(value: Any, *, default: int = 50, maximum: int = 250) -> int:
    return max(1, min(_safe_int(value, default), maximum))


def _safe_json_list(value: Any) -> list[dict[str, Any]]:
    try:
        loaded = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def _safe_json_strings(value: Any) -> list[str]:
    try:
        loaded = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item).strip() for item in loaded if str(item).strip()]


def _query_terms(value: str) -> list[str]:
    terms = re.findall(r"[a-z0-9]+", str(value or "").lower())
    normalized = []
    for term in terms:
        if len(term) > 3 and term.endswith("s"):
            term = term[:-1]
        normalized.append(term)
    return normalized


def _match_product_query(product_query: str, item: dict[str, Any]) -> bool:
    raw_query = str(product_query or "").strip()
    terms = _query_terms(product_query)
    if not terms:
        return False
    exact_id_values = {
        str(item.get(key) or "")
        for key in ("product_id", "sku_id")
        if str(item.get(key) or "")
    }
    if raw_query.isdigit() and len(raw_query) >= 8 and raw_query in exact_id_values:
        return True
    haystack = " ".join(str(item.get(key) or "") for key in ("title", "seller_sku", "sku_name")).lower()
    haystack_terms = set(_query_terms(haystack))
    return all(term in haystack_terms or term in haystack for term in terms)


def _text_matches_query(query: str, *values: Any) -> bool:
    terms = _query_terms(query)
    if not terms:
        return True
    haystack = " ".join(str(value or "") for value in values).lower()
    haystack_terms = set(_query_terms(haystack))
    return all(term in haystack_terms or term in haystack for term in terms)


def _inventory_query_text(item: InventoryItem) -> str:
    return " ".join(
        str(value or "")
        for value in (
            item.barcode,
            item.item_type,
            item.game,
            item.card_name,
            item.set_name,
            item.set_code,
            item.card_number,
            item.variant,
            item.sealed_product_kind,
            item.upc,
            item.condition,
            item.grading_company,
            item.grade,
            item.cert_number,
        )
    )


def _inventory_item_matches_query(item: InventoryItem, query: str) -> bool:
    terms = _query_terms(query)
    if not terms:
        return False
    haystack = _inventory_query_text(item).lower()
    haystack_terms = set(_query_terms(haystack))
    return all(term in haystack_terms or term in haystack for term in terms)


def _inventory_effective_price(item: InventoryItem) -> tuple[float | None, str]:
    if item.list_price is not None:
        return _money(item.list_price), "list_price"
    if item.auto_price is not None:
        return _money(item.auto_price), "auto_price"
    return None, "missing"


def _trend_direction(current: float | None, previous: float | None, *, threshold_pct: float = 3.0) -> str:
    if current is None or previous is None or previous <= 0:
        return "unknown"
    delta_pct = ((current - previous) / previous) * 100.0
    if delta_pct >= threshold_pct:
        return "up"
    if delta_pct <= -threshold_pct:
        return "down"
    return "flat"


def _trend_delta(current: float | None, previous: float | None) -> dict[str, Any]:
    if current is None or previous is None or previous <= 0:
        return {"delta": None, "delta_pct": None}
    delta = _money(current - previous)
    return {"delta": delta, "delta_pct": round((delta / previous) * 100.0, 1)}


WEB_SEARCH_PROVIDER = "duckduckgo_html"
WEB_SEARCH_ENDPOINT = "https://duckduckgo.com/html/"


def _collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def _strip_html(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _collapse_ws(value)


def _decode_search_result_url(href: str) -> str:
    raw = unescape(str(href or "").strip())
    if not raw:
        return ""
    absolute = urljoin(WEB_SEARCH_ENDPOINT, raw)
    parsed = urlparse(absolute)
    query = parse_qs(parsed.query)
    if "uddg" in query and query["uddg"]:
        return unquote(query["uddg"][0])
    return absolute


def _parse_duckduckgo_html_results(html_text: str, *, limit: int) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    pattern = re.compile(
        r'<a\b[^>]*class="[^"]*\bresult__a\b[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        re.I | re.S,
    )
    matches = list(pattern.finditer(html_text or ""))
    for index, match in enumerate(matches):
        if len(results) >= limit:
            break
        url = _decode_search_result_url(match.group(1))
        title = _strip_html(match.group(2))
        if not url or not title:
            continue
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(html_text or "")
        between = (html_text or "")[match.end() : next_start]
        snippet_match = re.search(
            r'<[^>]*class="[^"]*\bresult__snippet\b[^"]*"[^>]*>(.*?)</[^>]+>',
            between,
            re.I | re.S,
        )
        snippet = _strip_html(snippet_match.group(1)) if snippet_match else ""
        results.append({"title": title, "url": url, "snippet": snippet})
    return results


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _tiktok_order_to_mcp_row(row: TikTokOrder) -> dict[str, Any]:
    line_items = _safe_json_list(row.line_items_summary_json if row.line_items_summary_json != "[]" else row.line_items_json)
    return {
        "tiktok_order_id": row.tiktok_order_id,
        "order_number": row.order_number,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "customer_name": row.customer_name,
        "total_price": _money(row.total_price),
        "subtotal_price": _money(row.subtotal_price),
        "total_tax": row.total_tax,
        "financial_status": row.financial_status,
        "fulfillment_status": row.fulfillment_status,
        "order_status": row.order_status,
        "currency": row.currency,
        "shop_id": row.shop_id,
        "seller_id": row.seller_id,
        "affiliate_creator_username": row.affiliate_creator_username,
        "affiliate_content_type": row.affiliate_content_type,
        "affiliate_content_id": row.affiliate_content_id,
        "line_items": line_items[:25],
    }


def _tiktok_product_to_mcp_row(row: TikTokProduct) -> dict[str, Any]:
    skus = _safe_json_list(row.skus_json)
    images = _safe_json_list(row.images_json)
    return {
        "tiktok_product_id": row.tiktok_product_id,
        "title": row.title,
        "status": row.status,
        "audit_status": row.audit_status,
        "category_id": row.category_id,
        "category_name": row.category_name,
        "brand_id": row.brand_id,
        "brand_name": row.brand_name,
        "main_image_url": row.main_image_url,
        "sku_count": len(skus),
        "skus": skus[:25],
        "image_count": len(images),
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "synced_at": _iso(row.synced_at),
    }


def _external_order_is_paid(row: Any) -> bool:
    return str(getattr(row, "financial_status", "") or "").strip().lower() == "paid"


def _external_line_items(row: Any) -> list[dict[str, Any]]:
    value = getattr(row, "line_items_summary_json", "")
    fallback = getattr(row, "line_items_json", "")
    raw_items = _safe_json_list(value if value != "[]" else fallback)
    normalized: list[dict[str, Any]] = []
    for raw in raw_items:
        title = str(raw.get("product_name") or raw.get("title") or raw.get("name") or "Unknown").strip() or "Unknown"
        quantity = _safe_int(raw.get("quantity", raw.get("qty")), 1)
        if quantity <= 0:
            quantity = 1
        unit_price = _money(
            raw.get("sale_price", raw.get("unit_price", raw.get("price", raw.get("variant_price", 0.0))))
        )
        normalized.append(
            {
                "title": title,
                "product_id": str(raw.get("product_id") or ""),
                "sku_id": str(raw.get("sku_id") or raw.get("variant_id") or ""),
                "seller_sku": str(raw.get("seller_sku") or raw.get("sku") or raw.get("sku_code") or ""),
                "sku_name": str(raw.get("sku_name") or raw.get("variant_title") or raw.get("variant_name") or ""),
                "quantity": quantity,
                "unit_price": unit_price,
                "revenue": _money(unit_price * quantity),
            }
        )
    return normalized


def _external_order_id(row: Any, *, channel: str) -> str:
    if channel == "shopify":
        return str(getattr(row, "shopify_order_id", "") or getattr(row, "order_number", "") or getattr(row, "id", ""))
    return str(getattr(row, "tiktok_order_id", "") or getattr(row, "order_number", "") or getattr(row, "id", ""))


def _sort_product_rows(rows: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
    normalized = str(sort_by or "").strip().lower()
    if normalized in {"qty", "quantity", "units"}:
        key = lambda row: (row["quantity"], row["revenue"], row["order_count"], row["title"].lower())
    else:
        key = lambda row: (row["revenue"], row["quantity"], row["order_count"], row["title"].lower())
    return sorted(rows, key=key, reverse=True)


def _clock_entry_to_mcp_row(row: ClockifyTimeEntry | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "clockify_entry_id": row.clockify_entry_id,
        "description": row.description,
        "start_at": _iso(row.start_at),
        "end_at": _iso(row.end_at),
        "duration_seconds": row.duration_seconds,
        "is_running": row.is_running,
        "updated_at": _iso(row.updated_at),
    }


def _employee_matches_query(user: User, profile: EmployeeProfile | None, query: str) -> bool:
    clean_query = str(query or "").strip().lower()
    if not clean_query:
        return True
    haystack = " ".join(
        str(value or "")
        for value in (
            user.display_name,
            user.username,
            profile.clockify_user_id if profile else "",
        )
    ).lower()
    return all(term in haystack for term in _query_terms(clean_query))


def _increment_status(counter: dict[str, int], status: str) -> None:
    key = str(status or "unknown").strip().lower() or "unknown"
    counter[key] = counter.get(key, 0) + 1


def _visible_memory_scopes(scope: str) -> list[str]:
    normalized = _normalize_scope(scope)
    if normalized == "owner":
        return ["public", "employee", "partner", "tiktok", "owner"]
    return ["public", normalized]


def _safe_tags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    try:
        parsed = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _aggregate_external_product_rows(rows: list[Any], *, channel: str, product_query: str = "") -> dict[str, Any]:
    query = str(product_query or "").strip()
    matches: list[dict[str, Any]] = []
    candidates: dict[str, dict[str, Any]] = {}
    matched_order_ids: set[str] = set()
    paid_orders_scanned = 0

    for order in rows:
        if not _external_order_is_paid(order):
            continue
        paid_orders_scanned += 1
        order_id = _external_order_id(order, channel=channel)
        for item in _external_line_items(order):
            title = str(item.get("title") or "Unknown")
            quantity = _safe_int(item.get("quantity"), 1)
            unit_price = _money(item.get("unit_price"))
            revenue = _money(item.get("revenue"))
            candidate = candidates.setdefault(
                title.lower(),
                {
                    "title": title,
                    "quantity": 0,
                    "revenue": 0.0,
                    "order_ids": set(),
                },
            )
            candidate["quantity"] += quantity
            candidate["revenue"] = _money(candidate["revenue"] + revenue)
            candidate["order_ids"].add(order_id)

            if query and not _match_product_query(query, item):
                continue
            if query:
                matched_order_ids.add(order_id)
                matches.append(
                    {
                        "title": title,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "revenue": revenue,
                        "order_id": order_id,
                        "order_number": getattr(order, "order_number", ""),
                        "created_at": _iso(getattr(order, "created_at", None)),
                        "product_id": item.get("product_id") or "",
                        "sku_id": item.get("sku_id") or "",
                        "seller_sku": item.get("seller_sku") or "",
                    }
                )

    product_rows = []
    for row in candidates.values():
        order_count = len(row["order_ids"])
        quantity = _safe_int(row["quantity"], 0)
        revenue = _money(row["revenue"])
        product_rows.append(
            {
                "title": row["title"],
                "quantity": quantity,
                "revenue": revenue,
                "order_count": order_count,
                "avg_price": _money(revenue / quantity) if quantity else 0.0,
            }
        )
    matches.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return {
        "matches": matches,
        "products": product_rows,
        "matched_order_ids": matched_order_ids,
        "paid_orders_scanned": paid_orders_scanned,
    }


def _cash_safety_status(cash_flow: dict[str, Any]) -> str:
    reserve_gap = _money(cash_flow.get("reserve_gap"))
    return "below_minimum_reserve" if reserve_gap < 0 else "at_or_above_minimum_reserve"


def _cash_safety_sentence(cash_flow: dict[str, Any]) -> str:
    if _cash_safety_status(cash_flow) == "below_minimum_reserve":
        return "The modeled buy falls below the configured reserve."
    return "The modeled buy stays at or above the configured reserve."


def _redacted_partner_update(result: dict[str, Any], scenario: dict[str, Any]) -> str:
    unit_economics = result.get("unit_economics") or {}
    sell_through = result.get("sell_through") or {}
    payback_plan = result.get("payback_plan") or {}
    cash_flow = result.get("cash_flow") or {}
    risk_flags = result.get("risk_flags") or []
    lot_name = str(scenario.get("lot_name") or "Proposed lot").strip()
    sell_weeks = sell_through.get("estimated_weeks")
    sell_label = f"{sell_weeks} week(s)" if sell_weeks is not None else "unknown"
    risks = "; ".join(risk_flags) if risk_flags else "No major cash-flow flags in this model."
    return (
        "Weekly business update\n"
        f"Buy decision: {str(result.get('verdict') or '').upper()} for {lot_name}.\n"
        f"Expected profit: {_format_money(unit_economics.get('expected_profit'))} "
        f"at {_money(unit_economics.get('expected_margin_pct'))}% margin.\n"
        f"Estimated sell-through: {sell_label} using matched channel evidence.\n"
        f"Cash safety: {_cash_safety_sentence(cash_flow)} "
        "Exact cash balances and reserve-gap dollars are owner-scope only.\n"
        f"Weekly payback plan: {_format_money(payback_plan.get('weekly_payback'))}/week "
        f"for {payback_plan.get('target_weeks') or 0} week(s).\n"
        f"Risks: {risks}\n"
        "This is read-only decision support; no payments, listings, inventory, or messages were changed."
    )


def _money_line(value: Any) -> str:
    return f"${_money(value):,.0f}"


def _redact_partner_recommendation(result: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    redacted = deepcopy(result)
    cash_flow = result.get("cash_flow") or {}
    redacted["cash_flow"] = {
        "purchase_cost": _money(cash_flow.get("purchase_cost")),
        "cash_safety": _cash_safety_status(cash_flow),
        "cash_safety_summary": _cash_safety_sentence(cash_flow),
        "owner_scope_required_for": [
            "cash_on_hand",
            "post_buy_cash",
            "minimum_cash_reserve",
            "reserve_gap",
            "account_balances",
        ],
    }

    payback_plan = redacted.get("payback_plan")
    if isinstance(payback_plan, dict):
        payback_plan["weeks"] = [
            {
                "week": week.get("week"),
                "planned_payback": week.get("planned_payback"),
                "below_reserve": week.get("below_reserve"),
            }
            for week in payback_plan.get("weeks", [])
            if isinstance(week, dict)
        ]

    evidence = []
    for row in redacted.get("evidence", []):
        if not isinstance(row, dict):
            continue
        if row.get("source") == "loan_snapshot":
            safe_row = dict(row)
            safe_row["detail"] = "Loan/payback context checked. Exact owner loan/payback totals are owner-scope only."
            safe_row["url"] = ""
            evidence.append(safe_row)
        else:
            evidence.append(row)
    redacted["evidence"] = evidence
    redacted["partner_update"] = _redacted_partner_update(result, scenario)
    redacted["redaction_note"] = PARTNER_REDACTION_NOTE
    redacted["owner_scope_required_for"] = OWNER_FINANCIAL_TOOL_NAMES[:]
    redacted["read_only"] = True
    return redacted


class DegenOpsMcpHarness:
    def __init__(
        self,
        session_factory: Callable[[], Any] | None = None,
        web_client_factory: Callable[[], httpx.Client] | None = None,
    ):
        self._session_factory = session_factory or _default_session_factory
        self._web_client_factory = web_client_factory or (
            lambda: httpx.Client(
                timeout=10.0,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; DegenOpsAgent/1.0; "
                        "+https://ops.degencollectibles.com)"
                    )
                },
            )
        )

    @contextmanager
    def _session(self):
        session_or_cm = self._session_factory()
        if hasattr(session_or_cm, "__enter__") and hasattr(session_or_cm, "__exit__"):
            with session_or_cm as session:
                yield session
            return
        yield session_or_cm

    def get_context(self, days: int = 90) -> dict[str, Any]:
        with self._session() as session:
            return build_ops_agent_context(session, days=days)

    def get_manifest(self, *, scope: str, tools: list[str]) -> dict[str, Any]:
        return {
            "name": "degen-ops-readonly",
            "scope": scope,
            "tools": tools,
            "read_only": True,
            "guardrails": READ_ONLY_GUARDRAILS[:],
            "notes": [
                "This MCP server exposes bounded Degen business tools only.",
                "It does not expose arbitrary SQL or mutation tools.",
                "Use separate Hermes instances or configs for owner, partner, and employee scopes.",
                "Partner scope can evaluate buys but hides raw cash and owner loan/payback details by default.",
            ],
            "owner_only_tools": OWNER_ONLY_TOOL_NAMES[:],
        }

    def get_ops_memory(
        self,
        query: str = "",
        limit: int = 20,
        *,
        audience_scope: str = "employee",
    ) -> dict[str, Any]:
        normalized_scope = _normalize_scope(audience_scope)
        safe_limit = _bounded_limit(limit, default=20, maximum=100)
        clean_query = str(query or "").strip()
        visible_scopes = _visible_memory_scopes(normalized_scope)
        with self._session() as session:
            stmt = (
                select(OpsBotMemory)
                .where(OpsBotMemory.is_active == True)
                .where(OpsBotMemory.scope.in_(visible_scopes))
                .order_by(OpsBotMemory.updated_at.desc(), OpsBotMemory.created_at.desc())
                .limit(500)
            )
            rows = session.exec(stmt).all()
        terms = _query_terms(clean_query)
        memories = []
        for row in rows:
            haystack = " ".join([row.key, row.value, row.scope, " ".join(_safe_tags(row.tags_json))]).lower()
            if terms and not all(term in haystack for term in terms):
                continue
            memories.append(
                {
                    "id": row.id,
                    "scope": row.scope,
                    "key": row.key,
                    "value": row.value,
                    "tags": _safe_tags(row.tags_json),
                    "source": row.source,
                    "updated_at": _iso(row.updated_at),
                }
            )
            if len(memories) >= safe_limit:
                break
        return {
            "summary": {
                "query": clean_query,
                "audience_scope": normalized_scope,
                "visible_scopes": visible_scopes,
                "returned": len(memories),
            },
            "memories": memories,
            "data_gaps": [] if memories else [f"No active scoped Degen Ops memories matched query={clean_query!r}."],
            "read_only": True,
        }

    def propose_ops_memory(
        self,
        key: str,
        value: str,
        scope: str = "owner",
        tags: list[str] | None = None,
        proposed_by: str = "",
    ) -> dict[str, Any]:
        clean_scope = str(scope or "owner").strip().lower()
        allowed_scopes = {"public", "employee", "partner", "tiktok", "owner"}
        if clean_scope not in allowed_scopes:
            clean_scope = "owner"
        clean_tags = _safe_tags(tags or [])
        return {
            "proposal": {
                "scope": clean_scope,
                "key": str(key or "").strip()[:120],
                "value": str(value or "").strip()[:2000],
                "tags": clean_tags[:20],
                "proposed_by": str(proposed_by or "").strip()[:120],
            },
            "requires_owner_approval": True,
            "write_performed": False,
            "approval_note": (
                "This is a read-only draft. Persisting memory should be done only through an owner-approved write path."
            ),
            "read_only": True,
        }

    def get_tiktok_agent_manifest(self) -> dict[str, Any]:
        return {
            "name": "degen-tiktok-readonly",
            "tools": TIKTOK_MCP_TOOL_NAMES[:],
            "read_only": True,
            "guardrails": TIKTOK_READ_ONLY_GUARDRAILS[:],
            "tiktok_api_endpoints": deepcopy(TIKTOK_API_ENDPOINT_CATALOG),
            "local_routes": deepcopy(TIKTOK_LOCAL_ROUTE_CATALOG),
            "notes": [
                "Read tools use local TikTok tables, reporting helpers, and live-status cache.",
                "Endpoints that mutate TikTok, local inventory, token state, goals, prices, or webhook state are cataloged but not callable.",
                "Run app-owned sync/backfill flows through the existing admin UI or approved deployment workflow, not this MCP agent.",
            ],
        }

    def get_web_search(self, query: str, limit: int = 5, freshness: str = "") -> dict[str, Any]:
        safe_query = _collapse_ws(str(query or ""))[:240]
        safe_limit = _bounded_limit(limit, default=5, maximum=10)
        safe_freshness = str(freshness or "").strip().lower()
        freshness_map = {"day": "d", "week": "w", "month": "m", "year": "y"}
        if not safe_query:
            return {
                "summary": {"query": "", "result_count": 0, "provider": WEB_SEARCH_PROVIDER},
                "results": [],
                "data_gaps": ["A non-empty query is required."],
                "read_only": True,
            }
        params: dict[str, str] = {"q": safe_query, "kl": "us-en"}
        if safe_freshness in freshness_map:
            params["df"] = freshness_map[safe_freshness]
        try:
            with self._web_client_factory() as client:
                response = client.get(WEB_SEARCH_ENDPOINT, params=params)
                response.raise_for_status()
                search_url = str(response.request.url)
                results = _parse_duckduckgo_html_results(response.text, limit=safe_limit)
        except httpx.HTTPError as exc:
            return {
                "summary": {"query": safe_query, "result_count": 0, "provider": WEB_SEARCH_PROVIDER},
                "results": [],
                "filters": {"limit": safe_limit, "freshness": safe_freshness},
                "data_gaps": [f"Web search failed: {type(exc).__name__}."],
                "read_only": True,
            }

        data_gaps = []
        if not results:
            data_gaps.append(f"No public web search results matched query={safe_query!r}.")
        return {
            "summary": {
                "query": safe_query,
                "result_count": len(results),
                "provider": WEB_SEARCH_PROVIDER,
            },
            "results": results,
            "filters": {"limit": safe_limit, "freshness": safe_freshness},
            "search_url": search_url,
            "evidence": [
                {
                    "source": "web_search",
                    "provider": WEB_SEARCH_PROVIDER,
                    "url": search_url,
                    "detail": "Search results only; no page content was fetched.",
                }
            ],
            "data_gaps": data_gaps,
            "read_only": True,
        }

    def get_tiktok_status(self) -> dict[str, Any]:
        from .shared import build_tiktok_status_snapshot

        with self._session() as session:
            status = build_tiktok_status_snapshot(session)
        return {
            "status": status,
            "evidence": [{"source": "tiktok_status", "url": "/tiktok/orders"}],
            "read_only": True,
        }

    def get_tiktok_orders(
        self,
        days: int = 7,
        limit: int = 50,
        status: str = "",
        search: str = "",
    ) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit)
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        status_filter = (status or "").strip()
        with self._session() as session:
            summary_rows = get_tiktok_order_rows(session, start=start)
            rows = get_tiktok_order_rows(
                session,
                start=start,
                financial_status=status_filter or None,
                search=(search or "").strip() or None,
                limit=safe_limit,
            )
        return {
            "orders": [_tiktok_order_to_mcp_row(row) for row in rows],
            "summary": build_tiktok_reporting_summary(summary_rows),
            "range": {"days": safe_days, "limit": safe_limit},
            "filters": {"financial_status": status_filter, "search": search or ""},
            "evidence": [{"source": "tiktok_orders", "url": "/tiktok/orders"}],
            "read_only": True,
        }

    def get_tiktok_products(
        self,
        limit: int = 50,
        status: str = "",
        search: str = "",
    ) -> dict[str, Any]:
        safe_limit = _bounded_limit(limit)
        status_filter = (status or "").strip()
        search_filter = (search or "").strip().lower()
        with self._session() as session:
            stmt = select(TikTokProduct)
            if status_filter:
                stmt = stmt.where(func.lower(func.coalesce(TikTokProduct.status, "")) == status_filter.lower())
            if search_filter:
                pattern = f"%{search_filter}%"
                stmt = stmt.where(func.lower(TikTokProduct.title).like(pattern))
            total = session.exec(select(func.count()).select_from(TikTokProduct)).one()
            active = session.exec(
                select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "ACTIVATE")
            ).one()
            rows = session.exec(stmt.order_by(TikTokProduct.updated_at.desc()).limit(safe_limit)).all()
        return {
            "products": [_tiktok_product_to_mcp_row(row) for row in rows],
            "summary": {
                "total": int(total or 0),
                "active": int(active or 0),
                "returned": len(rows),
            },
            "filters": {"status": status_filter, "search": search or "", "limit": safe_limit},
            "evidence": [{"source": "tiktok_products", "url": "/tiktok/products"}],
            "read_only": True,
        }

    def get_sales_summary(self, days: int = 7) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        non_operating = {
            "loan_draw",
            "loan_repayment",
            "transfer",
            "loan_owner_payments",
            "loan_proceeds",
            "partner_paybacks",
            "transfers",
        }
        with self._session() as session:
            tiktok_rows = load_paid_tiktok_orders(session, days=safe_days)
            shopify_rows = [
                row
                for row in get_shopify_reporting_rows(session, start=start)
                if _external_order_is_paid(row)
            ]
            discord_rows = [
                row
                for row in get_financial_rows(session, start=start)
                if _money(getattr(row, "money_in", 0.0)) > 0
                and str(getattr(row, "entry_kind", "") or "").strip().lower() not in non_operating
                and str(getattr(row, "expense_category", "") or "").strip().lower() not in non_operating
            ]

        channel_rows = [
            {
                "channel": "shopify",
                "orders": len(shopify_rows),
                "revenue": _money(sum(external_order_net_revenue(row) for row in shopify_rows)),
            },
            {
                "channel": "tiktok",
                "orders": len(tiktok_rows),
                "revenue": _money(sum(external_order_net_revenue(row) for row in tiktok_rows)),
            },
            {
                "channel": "discord",
                "orders": len(discord_rows),
                "revenue": _money(sum(_money(getattr(row, "money_in", 0.0)) for row in discord_rows)),
            },
        ]
        for row in channel_rows:
            row["avg_order_value"] = _money(row["revenue"] / row["orders"]) if row["orders"] else 0.0
        channel_rows.sort(key=lambda row: row["revenue"], reverse=True)
        total_revenue = _money(sum(row["revenue"] for row in channel_rows))
        total_orders = sum(_safe_int(row["orders"], 0) for row in channel_rows)
        return {
            "summary": {
                "total_revenue": total_revenue,
                "total_orders": total_orders,
                "avg_order_value": _money(total_revenue / total_orders) if total_orders else 0.0,
                "top_channel_by_revenue": channel_rows[0]["channel"] if channel_rows and channel_rows[0]["revenue"] else "",
            },
            "channels": channel_rows,
            "range": {"days": safe_days},
            "evidence": [
                {"source": "shopify_orders", "url": "/shopify/orders", "row_count": len(shopify_rows)},
                {"source": "tiktok_orders", "url": "/tiktok/orders", "row_count": len(tiktok_rows)},
                {"source": "discord_financial_rows", "url": "/reports", "row_count": len(discord_rows)},
            ],
            "data_gaps": [] if total_orders else [f"No paid sales rows found in the last {safe_days} day(s)."],
            "read_only": True,
        }

    def get_discord_sales_summary(
        self,
        product_query: str = "",
        days: int = 7,
        limit: int = 25,
    ) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit, default=25, maximum=100)
        query = str(product_query or "").strip()
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        non_operating = {
            "loan_draw",
            "loan_repayment",
            "transfer",
            "loan_owner_payments",
            "loan_proceeds",
            "partner_paybacks",
            "transfers",
        }
        with self._session() as session:
            rows = [
                row
                for row in get_financial_rows(session, start=start)
                if _money(getattr(row, "money_in", 0.0)) > 0
                and str(getattr(row, "entry_kind", "") or "").strip().lower() not in non_operating
                and str(getattr(row, "expense_category", "") or "").strip().lower() not in non_operating
            ]
        matched_rows = []
        for row in rows:
            item_names = _safe_json_strings(getattr(row, "item_names_json", "[]"))
            items_in = _safe_json_strings(getattr(row, "items_in_json", "[]"))
            items_out = _safe_json_strings(getattr(row, "items_out_json", "[]"))
            if not _text_matches_query(
                query,
                getattr(row, "content", ""),
                getattr(row, "notes", ""),
                getattr(row, "trade_summary", ""),
                getattr(row, "category", ""),
                getattr(row, "channel_name", ""),
                " ".join(item_names + items_in + items_out),
            ):
                continue
            matched_rows.append((row, item_names or items_out or items_in))
        matched_rows.sort(key=lambda pair: getattr(pair[0], "created_at", datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        channel_totals: dict[str, dict[str, Any]] = {}
        category_totals: dict[str, dict[str, Any]] = {}
        matches: list[dict[str, Any]] = []
        for row, item_names in matched_rows:
            revenue = _money(getattr(row, "money_in", 0.0))
            channel = str(getattr(row, "channel_name", "") or getattr(row, "channel_id", "") or "unknown")
            category = str(getattr(row, "category", "") or "uncategorized")
            channel_row = channel_totals.setdefault(channel, {"channel": channel, "sales": 0, "revenue": 0.0})
            channel_row["sales"] += 1
            channel_row["revenue"] = _money(channel_row["revenue"] + revenue)
            category_row = category_totals.setdefault(category, {"category": category, "sales": 0, "revenue": 0.0})
            category_row["sales"] += 1
            category_row["revenue"] = _money(category_row["revenue"] + revenue)
            if len(matches) < safe_limit:
                matches.append(
                    {
                        "discord_message_id": getattr(row, "discord_message_id", ""),
                        "created_at": _iso(getattr(row, "created_at", None)),
                        "channel": channel,
                        "category": category,
                        "revenue": revenue,
                        "payment_method": getattr(row, "payment_method", "") or "",
                        "items": item_names[:10],
                        "content_preview": str(getattr(row, "content", "") or "")[:240],
                    }
                )
        channels = sorted(channel_totals.values(), key=lambda row: row["revenue"], reverse=True)
        categories = sorted(category_totals.values(), key=lambda row: row["revenue"], reverse=True)
        for row in channels:
            row["avg_sale_value"] = _money(row["revenue"] / row["sales"]) if row["sales"] else 0.0
        for row in categories:
            row["avg_sale_value"] = _money(row["revenue"] / row["sales"]) if row["sales"] else 0.0
        matched_revenue = _money(sum(row["revenue"] for row in channels))
        matched_sales = sum(_safe_int(row["sales"], 0) for row in channels)
        data_gaps = []
        if not matched_rows:
            data_gaps.append(
                f"No Discord sales rows matched product_query={query!r} in the last {safe_days} day(s)."
                if query
                else f"No Discord sales rows found in the last {safe_days} day(s)."
            )
        return {
            "summary": {
                "matched_sales": matched_sales,
                "matched_revenue": matched_revenue,
                "avg_sale_value": _money(matched_revenue / matched_sales) if matched_sales else 0.0,
                "top_channel_by_revenue": channels[0]["channel"] if channels else "",
                "top_category_by_revenue": categories[0]["category"] if categories else "",
            },
            "channels": channels,
            "categories": categories,
            "matches": matches,
            "filters": {"product_query": query, "days": safe_days, "limit": safe_limit},
            "range": {"days": safe_days},
            "evidence": [{"source": "discord_financial_rows", "url": "/reports", "row_count": len(rows)}],
            "data_gaps": data_gaps,
            "read_only": True,
        }

    def get_employee_clock_status(
        self,
        person_query: str = "",
        days: int = 1,
        limit: int = 20,
    ) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 1), 1)
        safe_limit = _bounded_limit(limit, default=20, maximum=100)
        query = str(person_query or "").strip()
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        employees: list[dict[str, Any]] = []
        with self._session() as session:
            users = session.exec(select(User).where(User.is_active == True).order_by(User.display_name.asc(), User.username.asc())).all()
            profiles = {
                profile.user_id: profile
                for profile in session.exec(select(EmployeeProfile)).all()
            }
            for user in users:
                profile = profiles.get(user.id or 0)
                if not _employee_matches_query(user, profile, query):
                    continue
                entry_filters = []
                if user.id is not None:
                    entry_filters.append(ClockifyTimeEntry.user_id == user.id)
                if profile and profile.clockify_user_id:
                    entry_filters.append(ClockifyTimeEntry.clockify_user_id == profile.clockify_user_id)
                if not entry_filters:
                    continue
                stmt = (
                    select(ClockifyTimeEntry)
                    .where(ClockifyTimeEntry.is_deleted == False)
                    .where(ClockifyTimeEntry.start_at >= start)
                    .where(entry_filters[0] if len(entry_filters) == 1 else (entry_filters[0] | entry_filters[1]))
                    .order_by(ClockifyTimeEntry.start_at.desc().nullslast(), ClockifyTimeEntry.updated_at.desc())
                )
                entries = session.exec(stmt).all()
                latest = entries[0] if entries else None
                running = next((entry for entry in entries if entry.is_running and entry.end_at is None), None)
                status = "clocked_in" if running else ("clocked_out" if latest else "no_recent_entries")
                employees.append(
                    {
                        "user_id": user.id,
                        "display_name": user.display_name or user.username,
                        "username": user.username,
                        "clockify_user_id": profile.clockify_user_id if profile else "",
                        "clock_status": status,
                        "latest_entry": _clock_entry_to_mcp_row(running or latest),
                        "recent_entry_count": len(entries),
                    }
                )
                if len(employees) >= safe_limit:
                    break
        data_gaps = []
        if query and not employees:
            data_gaps.append(f"No active employee with recent cached Clockify entries matched person_query={query!r}.")
        elif not employees:
            data_gaps.append(f"No active employees with recent cached Clockify entries found in the last {safe_days} day(s).")
        return {
            "summary": {
                "person_query": query,
                "matched_employee_count": len(employees),
                "clocked_in_count": sum(1 for row in employees if row["clock_status"] == "clocked_in"),
            },
            "employees": employees,
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {
                    "source": "clockify_time_entry_cache",
                    "url": "/team/admin/shift-tracker",
                    "detail": "Uses cached Clockify rows already stored by the app; does not call Clockify live.",
                }
            ],
            "read_only": True,
        }

    def get_employee_ops_status(
        self,
        person_query: str = "",
        days: int = 30,
        limit: int = 50,
    ) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 30), 1)
        safe_limit = _bounded_limit(limit, default=50, maximum=100)
        query = str(person_query or "").strip()
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        items: list[dict[str, Any]] = []
        status_counts = {
            "supply_requests": {},
            "buylist_submissions": {},
            "time_off_requests": {},
        }
        with self._session() as session:
            users = session.exec(select(User).where(User.is_active == True).order_by(User.display_name.asc(), User.username.asc())).all()
            profiles = {profile.user_id: profile for profile in session.exec(select(EmployeeProfile)).all()}
            matched_user_ids = [
                user.id
                for user in users
                if user.id is not None and _employee_matches_query(user, profiles.get(user.id), query)
            ]
            if matched_user_ids:
                supply_rows = session.exec(
                    select(SupplyRequest)
                    .where(SupplyRequest.created_at >= start)
                    .where(SupplyRequest.submitted_by_user_id.in_(matched_user_ids))
                    .order_by(SupplyRequest.created_at.desc())
                    .limit(safe_limit)
                ).all()
                buylist_rows = session.exec(
                    select(BuylistSubmission)
                    .where(BuylistSubmission.created_at >= start)
                    .where(BuylistSubmission.submitted_by_user_id.in_(matched_user_ids))
                    .order_by(BuylistSubmission.created_at.desc())
                    .limit(safe_limit)
                ).all()
                timeoff_rows = session.exec(
                    select(TimeOffRequest)
                    .where(TimeOffRequest.created_at >= start)
                    .where(TimeOffRequest.submitted_by_user_id.in_(matched_user_ids))
                    .order_by(TimeOffRequest.created_at.desc())
                    .limit(safe_limit)
                ).all()
            else:
                supply_rows = []
                buylist_rows = []
                timeoff_rows = []

        for row in supply_rows:
            _increment_status(status_counts["supply_requests"], row.status)
            items.append(
                {
                    "kind": "supply_request",
                    "id": row.id,
                    "title": row.title,
                    "status": row.status,
                    "urgency": row.urgency,
                    "created_at": _iso(row.created_at),
                }
            )
        for row in buylist_rows:
            _increment_status(status_counts["buylist_submissions"], row.status)
            items.append(
                {
                    "kind": "buylist_submission",
                    "id": row.id,
                    "customer_name": row.customer_name,
                    "status": row.status,
                    "created_at": _iso(row.created_at),
                }
            )
        for row in timeoff_rows:
            _increment_status(status_counts["time_off_requests"], row.status)
            items.append(
                {
                    "kind": "time_off_request",
                    "id": row.id,
                    "status": row.status,
                    "start_date": row.start_date.isoformat() if row.start_date else None,
                    "end_date": row.end_date.isoformat() if row.end_date else None,
                    "created_at": _iso(row.created_at),
                }
            )
        items.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        data_gaps = []
        if query and not matched_user_ids:
            data_gaps.append(f"No active employee matched person_query={query!r}.")
        elif not items:
            data_gaps.append(f"No tracked employee ops items matched the filters in the last {safe_days} day(s).")
        return {
            "summary": {
                "person_query": query,
                "matched_user_count": len(matched_user_ids),
                **status_counts,
            },
            "items": items[:safe_limit],
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {"source": "supply_requests", "url": "/team/admin/supply"},
                {"source": "buylist_submission", "url": "/team/admin/buylist/submissions"},
                {"source": "time_off_request", "url": "/team/admin/timeoff"},
            ],
            "read_only": True,
        }

    def get_tiktok_buyer_insights(self, days: int = 90, limit: int = 50) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 90), 1)
        safe_limit = _bounded_limit(limit)
        with self._session() as session:
            rows = build_tiktok_buyer_insights(session, days=safe_days)[:safe_limit]
        return {
            "buyers": rows,
            "range": {"days": safe_days, "limit": safe_limit},
            "evidence": [{"source": "tiktok_clients", "url": "/tiktok/clients"}],
            "read_only": True,
        }

    def get_tiktok_product_performance(self, days: int = 30, limit: int = 50) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 30), 1)
        safe_limit = _bounded_limit(limit)
        with self._session() as session:
            rows = build_tiktok_product_performance(session, days=safe_days)[:safe_limit]
        return {
            "products": rows,
            "range": {"days": safe_days, "limit": safe_limit},
            "evidence": [{"source": "tiktok_analytics_products", "url": "/tiktok/analytics/api/products"}],
            "read_only": True,
        }

    def get_tiktok_top_products(self, days: int = 7, limit: int = 10, sort_by: str = "quantity") -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit, default=10, maximum=100)
        with self._session() as session:
            orders = load_paid_tiktok_orders(session, days=safe_days)
        aggregate = _aggregate_external_product_rows(orders, channel="tiktok")
        products = _sort_product_rows(aggregate["products"], sort_by)[:safe_limit]
        data_gaps = [] if products else [f"No paid TikTok line items found in the last {safe_days} day(s)."]
        return {
            "summary": {
                "channel": "tiktok",
                "paid_orders_scanned": aggregate["paid_orders_scanned"],
                "product_count": len(aggregate["products"]),
                "returned": len(products),
                "sort_by": str(sort_by or "quantity"),
            },
            "products": products,
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {
                    "source": "tiktok_orders.line_items",
                    "url": "/tiktok/orders",
                    "status_filter": "paid-like local TikTok orders",
                    "row_count": aggregate["paid_orders_scanned"],
                }
            ],
            "read_only": True,
        }

    def get_tiktok_product_sales(self, product_query: str, days: int = 7, limit: int = 25) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit, default=25, maximum=100)
        query = str(product_query or "").strip()
        matches: list[dict[str, Any]] = []
        candidates: dict[str, dict[str, Any]] = {}
        matched_order_ids: set[str] = set()
        total_quantity = 0
        total_revenue = 0.0

        with self._session() as session:
            orders = load_paid_tiktok_orders(session, days=safe_days)

        for order in orders:
            order_id = str(order.tiktok_order_id or order.order_number or order.id or "")
            for raw_item in parse_line_items(order):
                item = normalize_item(raw_item)
                title = str(item.get("title") or "Unknown")
                quantity = _safe_int(item.get("qty"), 1)
                unit_price = _money(item.get("price"))
                revenue = _money(unit_price * quantity)

                candidate = candidates.setdefault(
                    title.lower(),
                    {
                        "title": title,
                        "quantity": 0,
                        "revenue": 0.0,
                        "order_count": 0,
                        "order_ids": set(),
                    },
                )
                candidate["quantity"] += quantity
                candidate["revenue"] = _money(candidate["revenue"] + revenue)
                candidate["order_ids"].add(order_id)
                candidate["order_count"] = len(candidate["order_ids"])

                if not _match_product_query(query, item):
                    continue

                total_quantity += quantity
                total_revenue = _money(total_revenue + revenue)
                matched_order_ids.add(order_id)
                matches.append(
                    {
                        "title": title,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "revenue": revenue,
                        "order_id": order_id,
                        "order_number": order.order_number,
                        "created_at": _iso(order.created_at),
                        "product_id": item.get("product_id") or "",
                        "sku_id": item.get("sku_id") or "",
                    }
                )

        matches.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        candidate_rows = []
        for row in candidates.values():
            candidate_rows.append(
                {
                    "title": row["title"],
                    "quantity": row["quantity"],
                    "revenue": _money(row["revenue"]),
                    "order_count": row["order_count"],
                }
            )
        candidate_rows.sort(key=lambda row: (row["quantity"], row["revenue"]), reverse=True)

        data_gaps = []
        if query and not matches:
            data_gaps.append(f"No TikTok line items matched product_query={query!r}.")
        elif not query:
            data_gaps.append("Missing product_query; provide a product title, SKU, or keyword.")

        return {
            "summary": {
                "product_query": query,
                "matched_quantity": total_quantity,
                "matched_order_count": len(matched_order_ids),
                "matched_revenue": _money(total_revenue),
                "matched_line_items": len(matches),
                "paid_orders_scanned": len(orders),
            },
            "matches": matches[:safe_limit],
            "candidates": candidate_rows[:safe_limit],
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {
                    "source": "tiktok_orders.line_items",
                    "url": "/tiktok/orders",
                    "status_filter": "paid-like local TikTok orders",
                    "row_count": len(orders),
                }
            ],
            "read_only": True,
        }

    def get_shopify_product_sales(self, product_query: str, days: int = 7, limit: int = 25) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit, default=25, maximum=100)
        query = str(product_query or "").strip()
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        with self._session() as session:
            rows = session.exec(
                select(ShopifyOrder)
                .where(ShopifyOrder.created_at >= start)
                .order_by(ShopifyOrder.created_at.desc(), ShopifyOrder.id.desc())
            ).all()
        aggregate = _aggregate_external_product_rows(rows, channel="shopify", product_query=query)
        matches = aggregate["matches"]
        matched_quantity = sum(_safe_int(row.get("quantity"), 0) for row in matches)
        matched_revenue = _money(sum(_money(row.get("revenue")) for row in matches))
        data_gaps = []
        if query and not matches:
            data_gaps.append(f"No Shopify line items matched product_query={query!r}.")
        elif not query:
            data_gaps.append("Missing product_query; provide a product title, SKU, or keyword.")
        candidates = _sort_product_rows(aggregate["products"], "quantity")[:safe_limit]
        return {
            "summary": {
                "product_query": query,
                "matched_quantity": matched_quantity,
                "matched_order_count": len(aggregate["matched_order_ids"]),
                "matched_revenue": matched_revenue,
                "matched_line_items": len(matches),
                "paid_orders_scanned": aggregate["paid_orders_scanned"],
            },
            "matches": matches[:safe_limit],
            "candidates": candidates,
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {
                    "source": "shopify_orders.line_items",
                    "url": "/shopify/orders",
                    "status_filter": "paid local Shopify orders",
                    "row_count": aggregate["paid_orders_scanned"],
                }
            ],
            "read_only": True,
        }

    def get_shopify_top_products(self, days: int = 7, limit: int = 10, sort_by: str = "quantity") -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        safe_limit = _bounded_limit(limit, default=10, maximum=100)
        start = datetime.now(timezone.utc) - timedelta(days=safe_days)
        with self._session() as session:
            rows = session.exec(
                select(ShopifyOrder)
                .where(ShopifyOrder.created_at >= start)
                .order_by(ShopifyOrder.created_at.desc(), ShopifyOrder.id.desc())
            ).all()
        aggregate = _aggregate_external_product_rows(rows, channel="shopify")
        products = _sort_product_rows(aggregate["products"], sort_by)[:safe_limit]
        data_gaps = [] if products else [f"No paid Shopify line items found in the last {safe_days} day(s)."]
        return {
            "summary": {
                "channel": "shopify",
                "paid_orders_scanned": aggregate["paid_orders_scanned"],
                "product_count": len(aggregate["products"]),
                "returned": len(products),
                "sort_by": str(sort_by or "quantity"),
            },
            "products": products,
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {
                    "source": "shopify_orders.line_items",
                    "url": "/shopify/orders",
                    "status_filter": "paid local Shopify orders",
                    "row_count": aggregate["paid_orders_scanned"],
                }
            ],
            "read_only": True,
        }

    def get_price_lookup(self, query: str, days: int = 30, limit: int = 10) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 30), 1)
        safe_limit = _bounded_limit(limit, default=10, maximum=50)
        clean_query = str(query or "").strip()
        inventory_matches: list[dict[str, Any]] = []

        with self._session() as session:
            inventory_rows = session.exec(
                select(InventoryItem)
                .where(InventoryItem.archived_at == None)  # noqa: E711
                .order_by(InventoryItem.updated_at.desc().nullslast(), InventoryItem.created_at.desc())
                .limit(500)
            ).all()
            matched_items = [item for item in inventory_rows if _inventory_item_matches_query(item, clean_query)]
            for item in matched_items[:safe_limit]:
                effective, source = _inventory_effective_price(item)
                latest_history = session.exec(
                    select(PriceHistory)
                    .where(PriceHistory.item_id == item.id)
                    .order_by(PriceHistory.fetched_at.desc())
                    .limit(1)
                ).first()
                inventory_matches.append(
                    {
                        "item_id": item.id,
                        "barcode": item.barcode,
                        "item_type": item.item_type,
                        "game": item.game,
                        "name": item.card_name,
                        "set_name": item.set_name,
                        "card_number": item.card_number,
                        "variant": item.variant,
                        "condition": item.condition,
                        "quantity": item.quantity,
                        "status": item.status,
                        "effective_price": effective,
                        "effective_price_source": source,
                        "list_price": _money(item.list_price) if item.list_price is not None else None,
                        "auto_price": _money(item.auto_price) if item.auto_price is not None else None,
                        "cost_basis": _money(item.cost_basis) if item.cost_basis is not None else None,
                        "last_priced_at": _iso(item.last_priced_at),
                        "latest_price_history": (
                            {
                                "source": latest_history.source,
                                "market_price": _money(latest_history.market_price),
                                "low_price": _money(latest_history.low_price),
                                "high_price": _money(latest_history.high_price),
                                "fetched_at": _iso(latest_history.fetched_at),
                            }
                            if latest_history
                            else None
                        ),
                        "evidence_url": f"/inventory/{item.id}" if item.id else "/inventory",
                    }
                )

        recent_sales = self.get_tiktok_product_sales(product_query=clean_query, days=safe_days, limit=safe_limit)
        sale_summary = recent_sales.get("summary") or {}
        recent_avg_sale_price = None
        matched_quantity = _safe_int(sale_summary.get("matched_quantity"), 0)
        matched_revenue = _money(sale_summary.get("matched_revenue"))
        if matched_quantity:
            recent_avg_sale_price = _money(matched_revenue / matched_quantity)
        recent_shopify_sales = self.get_shopify_product_sales(product_query=clean_query, days=safe_days, limit=safe_limit)
        shopify_summary = recent_shopify_sales.get("summary") or {}
        shopify_quantity = _safe_int(shopify_summary.get("matched_quantity"), 0)
        shopify_revenue = _money(shopify_summary.get("matched_revenue"))
        recent_discord_sales = self.get_discord_sales_summary(product_query=clean_query, days=safe_days, limit=safe_limit)
        discord_summary = recent_discord_sales.get("summary") or {}
        discord_quantity = _safe_int(discord_summary.get("matched_sales"), 0)
        discord_revenue = _money(discord_summary.get("matched_revenue"))
        recent_channel_prices = []
        for channel, quantity, revenue in (
            ("tiktok", matched_quantity, matched_revenue),
            ("shopify", shopify_quantity, shopify_revenue),
            ("discord", discord_quantity, discord_revenue),
        ):
            if quantity <= 0:
                continue
            recent_channel_prices.append(
                {
                    "channel": channel,
                    "quantity": quantity,
                    "revenue": revenue,
                    "avg_sale_price": _money(revenue / quantity),
                }
            )
        cross_channel_quantity = sum(_safe_int(row["quantity"], 0) for row in recent_channel_prices)
        cross_channel_revenue = _money(sum(_money(row["revenue"]) for row in recent_channel_prices))
        recent_cross_channel_avg_sale_price = (
            _money(cross_channel_revenue / cross_channel_quantity) if cross_channel_quantity else None
        )

        inventory_price = next(
            (row["effective_price"] for row in inventory_matches if row.get("effective_price") is not None),
            None,
        )
        recommended_price = inventory_price if inventory_price is not None else recent_cross_channel_avg_sale_price
        recommended_source = "inventory_effective_price" if inventory_price is not None else (
            "recent_cross_channel_avg_sale_price" if recent_cross_channel_avg_sale_price is not None else "missing"
        )

        data_gaps = []
        if clean_query and not inventory_matches:
            data_gaps.append(f"No stored inventory price matched query={clean_query!r}.")
        if cross_channel_quantity == 0:
            data_gaps.append(f"No recent cross-channel sale price matched query={clean_query!r}.")
        if matched_quantity == 0:
            data_gaps.append(f"No recent TikTok sale price matched query={clean_query!r}.")
        if not clean_query:
            data_gaps.append("Missing query; provide a card, sealed product, barcode, SKU, or product keyword.")

        return {
            "summary": {
                "query": clean_query,
                "recommended_price": recommended_price,
                "recommended_price_source": recommended_source,
                "inventory_match_count": len(inventory_matches),
                "recent_tiktok_avg_sale_price": recent_avg_sale_price,
                "recent_tiktok_quantity": matched_quantity,
                "recent_tiktok_revenue": matched_revenue,
                "recent_cross_channel_avg_sale_price": recent_cross_channel_avg_sale_price,
                "recent_cross_channel_quantity": cross_channel_quantity,
                "recent_cross_channel_revenue": cross_channel_revenue,
            },
            "inventory_matches": inventory_matches,
            "recent_tiktok_sales": recent_sales,
            "recent_shopify_sales": recent_shopify_sales,
            "recent_discord_sales": recent_discord_sales,
            "recent_channel_prices": recent_channel_prices,
            "range": {"days": safe_days, "limit": safe_limit},
            "data_gaps": data_gaps,
            "evidence": [
                {"source": "inventory_items", "url": "/inventory", "rows_scanned_limit": 500},
                {"source": "price_history", "url": "/inventory"},
                {"source": "tiktok_orders.line_items", "url": "/tiktok/orders"},
                {"source": "shopify_orders.line_items", "url": "/shopify/orders"},
                {"source": "discord_financial_rows", "url": "/reports"},
            ],
            "read_only": True,
        }

    def _tiktok_product_sales_between(
        self,
        *,
        query: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        quantity = 0
        revenue = 0.0
        order_ids: set[str] = set()
        line_items = 0
        with self._session() as session:
            rows = session.exec(
                select(TikTokOrder)
                .where(TikTokOrder.created_at >= start)
                .where(TikTokOrder.created_at < end)
            ).all()
        for order in rows:
            status = str(order.financial_status or order.order_status or "").strip().lower()
            if status not in {"paid", "completed", "awaiting_shipment", "awaiting_collection", "delivered"}:
                continue
            order_id = str(order.tiktok_order_id or order.order_number or order.id or "")
            for raw_item in parse_line_items(order):
                item = normalize_item(raw_item)
                if not _match_product_query(query, item):
                    continue
                qty = _safe_int(item.get("qty"), 1)
                unit_price = _money(item.get("price"))
                quantity += qty
                revenue = _money(revenue + unit_price * qty)
                order_ids.add(order_id)
                line_items += 1
        avg_price = _money(revenue / quantity) if quantity else None
        return {
            "start": _iso(start),
            "end": _iso(end),
            "quantity": quantity,
            "revenue": revenue,
            "avg_price": avg_price,
            "order_count": len(order_ids),
            "line_items": line_items,
        }

    def _cross_channel_product_sales_between(
        self,
        *,
        query: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        windows: dict[str, dict[str, Any]] = {}
        tiktok = self._tiktok_product_sales_between(query=query, start=start, end=end)
        if _safe_int(tiktok.get("quantity"), 0) > 0:
            windows["tiktok"] = tiktok
        with self._session() as session:
            shopify_rows = session.exec(
                select(ShopifyOrder)
                .where(ShopifyOrder.created_at >= start)
                .where(ShopifyOrder.created_at < end)
            ).all()
            discord_rows = [
                row
                for row in get_financial_rows(session, start=start, end=end)
                if _money(getattr(row, "money_in", 0.0)) > 0
            ]
        shopify_aggregate = _aggregate_external_product_rows(shopify_rows, channel="shopify", product_query=query)
        shopify_matches = shopify_aggregate.get("matches", [])
        shopify_quantity = sum(_safe_int(row.get("quantity"), 0) for row in shopify_matches)
        shopify_revenue = _money(sum(_money(row.get("revenue")) for row in shopify_matches))
        if shopify_quantity > 0:
            windows["shopify"] = {
                "start": _iso(start),
                "end": _iso(end),
                "quantity": shopify_quantity,
                "revenue": shopify_revenue,
                "avg_price": _money(shopify_revenue / shopify_quantity),
                "order_count": len({str(row.get("order_id") or "") for row in shopify_matches if row.get("order_id")}),
                "line_items": len(shopify_matches),
            }
        discord_matches = []
        for row in discord_rows:
            item_names = _safe_json_strings(getattr(row, "item_names_json", "[]"))
            items_in = _safe_json_strings(getattr(row, "items_in_json", "[]"))
            items_out = _safe_json_strings(getattr(row, "items_out_json", "[]"))
            if _text_matches_query(
                query,
                getattr(row, "content", ""),
                getattr(row, "notes", ""),
                getattr(row, "trade_summary", ""),
                getattr(row, "category", ""),
                getattr(row, "channel_name", ""),
                " ".join(item_names + items_in + items_out),
            ):
                discord_matches.append(row)
        discord_quantity = len(discord_matches)
        discord_revenue = _money(sum(_money(getattr(row, "money_in", 0.0)) for row in discord_matches))
        if discord_quantity > 0:
            windows["discord"] = {
                "start": _iso(start),
                "end": _iso(end),
                "quantity": discord_quantity,
                "revenue": discord_revenue,
                "avg_price": _money(discord_revenue / discord_quantity),
                "order_count": discord_quantity,
                "line_items": discord_quantity,
            }
        total_quantity = sum(_safe_int(row.get("quantity"), 0) for row in windows.values())
        total_revenue = _money(sum(_money(row.get("revenue")) for row in windows.values()))
        return {
            "start": _iso(start),
            "end": _iso(end),
            "quantity": total_quantity,
            "revenue": total_revenue,
            "avg_price": _money(total_revenue / total_quantity) if total_quantity else None,
            "order_count": sum(_safe_int(row.get("order_count"), 0) for row in windows.values()),
            "line_items": sum(_safe_int(row.get("line_items"), 0) for row in windows.values()),
            "channels": list(windows.keys()),
            "channel_windows": windows,
        }

    def get_market_trend_lookup(self, query: str, days: int = 7, limit: int = 10) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        clean_query = str(query or "").strip()
        now = datetime.now(timezone.utc)
        current_start = now - timedelta(days=safe_days)
        previous_start = now - timedelta(days=safe_days * 2)
        current_window = self._tiktok_product_sales_between(query=clean_query, start=current_start, end=now)
        previous_window = self._tiktok_product_sales_between(query=clean_query, start=previous_start, end=current_start)
        tiktok_direction = _trend_direction(current_window.get("avg_price"), previous_window.get("avg_price"))
        tiktok_delta = _trend_delta(current_window.get("avg_price"), previous_window.get("avg_price"))
        cross_current_window = self._cross_channel_product_sales_between(query=clean_query, start=current_start, end=now)
        cross_previous_window = self._cross_channel_product_sales_between(query=clean_query, start=previous_start, end=current_start)
        cross_channel_direction = _trend_direction(
            cross_current_window.get("avg_price"),
            cross_previous_window.get("avg_price"),
        )
        cross_channel_delta = _trend_delta(
            cross_current_window.get("avg_price"),
            cross_previous_window.get("avg_price"),
        )

        price_history_points: list[dict[str, Any]] = []
        with self._session() as session:
            inventory_rows = session.exec(
                select(InventoryItem)
                .where(InventoryItem.archived_at == None)  # noqa: E711
                .order_by(InventoryItem.updated_at.desc().nullslast(), InventoryItem.created_at.desc())
                .limit(500)
            ).all()
            matched_items = [item for item in inventory_rows if _inventory_item_matches_query(item, clean_query)]
            matched_item_ids = [item.id for item in matched_items if item.id is not None]
            if matched_item_ids:
                history_rows = session.exec(
                    select(PriceHistory)
                    .where(PriceHistory.item_id.in_(matched_item_ids))
                    .order_by(PriceHistory.fetched_at.asc())
                    .limit(250)
                ).all()
                for row in history_rows:
                    price = _money(row.market_price)
                    if price <= 0:
                        continue
                    price_history_points.append(
                        {
                            "item_id": row.item_id,
                            "source": row.source,
                            "market_price": price,
                            "low_price": _money(row.low_price) if row.low_price is not None else None,
                            "high_price": _money(row.high_price) if row.high_price is not None else None,
                            "fetched_at": _iso(row.fetched_at),
                        }
                    )

        first_history = price_history_points[0] if price_history_points else None
        latest_history = price_history_points[-1] if price_history_points else None
        history_previous = first_history.get("market_price") if first_history else None
        history_current = latest_history.get("market_price") if latest_history else None
        history_direction = _trend_direction(history_current, history_previous)
        history_delta = _trend_delta(history_current, history_previous)

        direction = cross_channel_direction if cross_channel_direction != "unknown" else (
            tiktok_direction if tiktok_direction != "unknown" else history_direction
        )
        data_gaps = []
        if cross_channel_direction == "unknown":
            data_gaps.append(f"No recent cross-channel comparison windows matched query={clean_query!r}.")
        if tiktok_direction == "unknown":
            data_gaps.append(f"No recent TikTok comparison windows matched query={clean_query!r}.")
        if history_direction == "unknown":
            data_gaps.append(f"No stored price-history trend matched query={clean_query!r}.")
        if not clean_query:
            data_gaps.append("Missing query; provide a product/card keyword.")

        return {
            "summary": {
                "query": clean_query,
                "trend_direction": direction,
                "cross_channel_direction": cross_channel_direction,
                "tiktok_direction": tiktok_direction,
                "price_history_direction": history_direction,
            },
            "cross_channel_trend": {
                "window_days": safe_days,
                "current_window": cross_current_window,
                "previous_window": cross_previous_window,
                **cross_channel_delta,
                "direction": cross_channel_direction,
                "channels": sorted(set(cross_current_window.get("channels", []) + cross_previous_window.get("channels", []))),
            },
            "tiktok_trend": {
                "window_days": safe_days,
                "current_window": current_window,
                "previous_window": previous_window,
                **tiktok_delta,
                "direction": tiktok_direction,
            },
            "price_history_trend": {
                "direction": history_direction,
                "first_market_price": history_previous,
                "latest_market_price": history_current,
                **history_delta,
                "points": price_history_points[: _bounded_limit(limit, default=10, maximum=50)],
            },
            "range": {"days": safe_days},
            "data_gaps": data_gaps,
            "evidence": [
                {"source": "tiktok_orders.line_items", "url": "/tiktok/orders"},
                {"source": "price_history", "url": "/inventory"},
                {"source": "inventory_items", "url": "/inventory"},
            ],
            "read_only": True,
        }

    def get_tiktok_live_snapshot(self) -> dict[str, Any]:
        from .shared import _get_live_analytics_snapshot, _get_live_session_snapshot, _get_live_sessions_list

        return {
            "live_session": _get_live_session_snapshot(),
            "live_analytics": _get_live_analytics_snapshot(),
            "recent_sessions": _get_live_sessions_list()[:25],
            "evidence": [
                {"source": "tiktok_streamer", "url": "/tiktok/streamer"},
                {"source": "public_live_status", "url": "/public/tiktok/live-status"},
            ],
            "read_only": True,
        }

    def get_finance_snapshot(self, days: int = 90) -> dict[str, Any]:
        context = self.get_context(days=days)
        return {
            "finance_statement": context["finance_statement"],
            "range": context["range"],
            "evidence": [{"source": "finance", "url": "/finance"}],
            "read_only": True,
        }

    def get_cash_snapshot(self) -> dict[str, Any]:
        context = self.get_context(days=90)
        return {
            "cash_snapshot": context["cash_snapshot"],
            "evidence": [{"source": "bank_transactions", "url": context["cash_snapshot"].get("evidence_url", "/bookkeeping/bank")}],
            "read_only": True,
        }

    def get_inventory_snapshot(self) -> dict[str, Any]:
        context = self.get_context(days=90)
        return {
            "inventory_snapshot": context["inventory_snapshot"],
            "evidence": [{"source": "inventory_items", "url": context["inventory_snapshot"].get("evidence_url", "/inventory")}],
            "read_only": True,
        }

    def get_channel_velocity(self, days: int = 90, category: str = "") -> dict[str, Any]:
        context = self.get_context(days=days)
        rows = context["channel_velocity"]
        category_norm = (category or "").strip().lower()
        if category_norm:
            rows = [
                row
                for row in rows
                if category_norm in str(row.get("matched_category") or "").strip().lower()
            ]
        return {
            "channel_velocity": rows,
            "range": context["range"],
            "evidence": [{"source": "orders_and_transactions", "url": "/reports"}],
            "read_only": True,
        }

    def get_loan_and_payback_snapshot(self, days: int = 90) -> dict[str, Any]:
        context = self.get_context(days=days)
        return {
            "loan_snapshot": context["loan_snapshot"],
            "range": context["range"],
            "evidence": [
                {
                    "source": "bank_transactions",
                    "url": context["loan_snapshot"].get("evidence_url", "/bookkeeping/bank"),
                }
            ],
            "read_only": True,
        }

    def evaluate_inventory_buy(
        self,
        scenario: dict[str, Any],
        days: int = 90,
        *,
        audience_scope: str = "owner",
    ) -> dict[str, Any]:
        context = self.get_context(days=days)
        enriched_scenario = dict(scenario or {})
        if not enriched_scenario.get("cash_on_hand"):
            enriched_scenario["cash_on_hand"] = (context.get("cash_snapshot") or {}).get("latest_known_cash", 0.0)
        result = build_ops_agent_recommendation(enriched_scenario, context)
        result["read_only"] = True
        if _normalize_scope(audience_scope) == "partner":
            return _redact_partner_recommendation(result, enriched_scenario)
        return result

    def generate_partner_update(
        self,
        scenario: dict[str, Any],
        days: int = 90,
        *,
        audience_scope: str = "owner",
    ) -> dict[str, Any]:
        result = self.evaluate_inventory_buy(scenario, days=days, audience_scope=audience_scope)
        return {
            "partner_update": result["partner_update"],
            "verdict": result.get("verdict"),
            "risk_flags": result.get("risk_flags", []),
            "evidence": result.get("evidence", []),
            "read_only_guardrails": result.get("read_only_guardrails", []),
            "redaction_note": result.get("redaction_note", ""),
            "read_only": True,
        }

    def generate_weekly_partner_update_draft(
        self,
        days: int = 7,
        *,
        audience_scope: str = "partner",
    ) -> dict[str, Any]:
        safe_days = max(_safe_int(days, 7), 1)
        normalized_scope = _normalize_scope(audience_scope)
        finance = self.get_finance_snapshot(days=safe_days)
        sales = self.get_sales_summary(days=safe_days)
        inventory = self.get_inventory_snapshot()
        finance_statement = finance.get("finance_statement") or {}
        sales_summary = sales.get("summary") or {}
        inventory_snapshot = inventory.get("inventory_snapshot") or {}
        channel_lines = []
        for row in sales.get("channels", [])[:5]:
            if not isinstance(row, dict):
                continue
            channel_lines.append(
                f"- {row.get('channel')}: {_money_line(row.get('revenue'))} across {row.get('orders', 0)} order(s)"
            )
        if not channel_lines:
            channel_lines.append("- No channel sales rows found for this window.")
        draft = "\n".join(
            [
                f"Weekly Degen Ops Update ({safe_days}-day read-only draft)",
                "",
                "Business snapshot",
                f"- Revenue: {_money_line(finance_statement.get('revenue'))}",
                f"- Operating profit: {_money_line(finance_statement.get('operating_profit'))}",
                f"- Operating expenses: {_money_line(finance_statement.get('operating_expenses'))}",
                "",
                "Sales channels",
                f"- Cross-channel sales tracked: {_money_line(sales_summary.get('total_revenue'))}",
                f"- Orders tracked: {sales_summary.get('total_orders', 0)}",
                f"- Top channel: {sales_summary.get('top_channel_by_revenue') or 'unknown'}",
                *channel_lines,
                "",
                "Inventory",
                f"- Active items: {inventory_snapshot.get('active_items', 0)}",
                f"- Estimated list value: {_money_line(inventory_snapshot.get('estimated_list_value'))}",
                "",
                "Notes",
                "- This is a draft only. No partner message was sent.",
                "- Figures come from current read-only app data and may use synced platform rows rather than payout cash.",
            ]
        )
        return {
            "draft": draft,
            "audience_scope": normalized_scope,
            "range": {"days": safe_days},
            "approval_required": True,
            "write_performed": False,
            "evidence": [
                {"source": "finance_snapshot", "url": "/finance"},
                {"source": "sales_summary", "url": "/reports"},
                {"source": "inventory_snapshot", "url": "/inventory"},
            ],
            "read_only": True,
        }


def _normalize_scope(scope: str | None = None) -> str:
    raw = (scope if scope is not None else os.getenv("DEGEN_OPS_MCP_SCOPE", "employee")).strip().lower()
    normalized = raw or "employee"
    if normalized not in DEGEN_OPS_SCOPE_TOOL_NAMES:
        supported = ", ".join(sorted(DEGEN_OPS_SCOPE_TOOL_NAMES))
        raise ValueError(f"Unsupported Degen Ops MCP scope {normalized!r}. Supported scopes: {supported}.")
    return normalized


def register_degen_ops_tools(
    mcp: Any,
    *,
    harness: DegenOpsMcpHarness | None = None,
    scope: str | None = None,
) -> Any:
    ops = harness or DegenOpsMcpHarness()
    normalized_scope = _normalize_scope(scope)
    allowed_tools = DEGEN_OPS_SCOPE_TOOL_NAMES[normalized_scope]

    def should_register(tool_name: str) -> bool:
        return tool_name in allowed_tools

    @mcp.tool(description="Read-only manifest listing Degen Ops MCP scope, exposed tools, and guardrails.")
    def get_ops_agent_manifest() -> dict[str, Any]:
        return ops.get_manifest(scope=normalized_scope, tools=allowed_tools)

    if should_register("get_ops_memory"):
        @mcp.tool(description="Read-only scoped Degen Ops memory lookup for preferences, assumptions, and operating notes.")
        def get_ops_memory(query: str = "", limit: int = 20) -> dict[str, Any]:
            return ops.get_ops_memory(query=query, limit=limit, audience_scope=normalized_scope)

    if should_register("get_finance_snapshot"):
        @mcp.tool(description="Read-only Degen finance snapshot from existing finance/reporting helpers.")
        def get_finance_snapshot(days: int = 90) -> dict[str, Any]:
            return ops.get_finance_snapshot(days=days)

    if should_register("get_cash_snapshot"):
        @mcp.tool(description="Read-only latest known cash snapshot from bank rows with balances.")
        def get_cash_snapshot() -> dict[str, Any]:
            return ops.get_cash_snapshot()

    if should_register("get_inventory_snapshot"):
        @mcp.tool(description="Read-only inventory count, cost basis, and list-value snapshot.")
        def get_inventory_snapshot() -> dict[str, Any]:
            return ops.get_inventory_snapshot()

    if should_register("get_channel_velocity"):
        @mcp.tool(description="Read-only sell-through velocity by TikTok, Shopify, Discord, and show/channel sales.")
        def get_channel_velocity(days: int = 90, category: str = "") -> dict[str, Any]:
            return ops.get_channel_velocity(days=days, category=category)

    if should_register("get_sales_summary"):
        @mcp.tool(description="Read-only cross-channel sales summary for TikTok, Shopify, and Discord store sales.")
        def get_sales_summary(days: int = 7) -> dict[str, Any]:
            return ops.get_sales_summary(days=days)

    if should_register("get_discord_sales_summary"):
        @mcp.tool(description="Read-only Discord/show sales summary filtered by product keyword, category, or channel text.")
        def get_discord_sales_summary(product_query: str = "", days: int = 7, limit: int = 25) -> dict[str, Any]:
            return ops.get_discord_sales_summary(product_query=product_query, days=days, limit=limit)

    if should_register("get_loan_and_payback_snapshot"):
        @mcp.tool(description="Read-only loan proceeds, owner payback, and payout timing evidence.")
        def get_loan_and_payback_snapshot(days: int = 90) -> dict[str, Any]:
            return ops.get_loan_and_payback_snapshot(days=days)

    if should_register("get_employee_clock_status"):
        @mcp.tool(description="Owner-only read-only employee clock-in/out status from cached Clockify rows.")
        def get_employee_clock_status(person_query: str = "", days: int = 1, limit: int = 20) -> dict[str, Any]:
            return ops.get_employee_clock_status(person_query=person_query, days=days, limit=limit)

    if should_register("get_employee_ops_status"):
        @mcp.tool(description="Owner-only read-only employee ops request status from supply, buylist, and time-off queues.")
        def get_employee_ops_status(person_query: str = "", days: int = 30, limit: int = 50) -> dict[str, Any]:
            return ops.get_employee_ops_status(person_query=person_query, days=days, limit=limit)

    if should_register("propose_ops_memory"):
        @mcp.tool(description="Owner-only read-only draft for a Degen Ops memory change; does not persist memory.")
        def propose_ops_memory(
            key: str,
            value: str,
            scope: str = "owner",
            tags: list[str] | None = None,
            proposed_by: str = "",
        ) -> dict[str, Any]:
            return ops.propose_ops_memory(
                key=key,
                value=value,
                scope=scope,
                tags=tags,
                proposed_by=proposed_by,
            )

    if should_register("evaluate_inventory_buy"):
        @mcp.tool(description="Read-only proposed inventory-buy evaluation with evidence-backed routing and cash plan.")
        def evaluate_inventory_buy(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.evaluate_inventory_buy(scenario=scenario, days=days, audience_scope=normalized_scope)

    if should_register("generate_partner_update"):
        @mcp.tool(description="Read-only partner-ready weekly update generated from a proposed buy scenario.")
        def generate_partner_update(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.generate_partner_update(scenario=scenario, days=days, audience_scope=normalized_scope)

    if should_register("generate_weekly_partner_update_draft"):
        @mcp.tool(description="Read-only weekly partner update draft from current finance, sales, and inventory snapshots; does not post.")
        def generate_weekly_partner_update_draft(days: int = 7) -> dict[str, Any]:
            return ops.generate_weekly_partner_update_draft(days=days, audience_scope=normalized_scope)

    if should_register("get_tiktok_agent_manifest"):
        @mcp.tool(description="Read-only TikTok agent manifest with callable tools and blocked write endpoints.")
        def get_tiktok_agent_manifest() -> dict[str, Any]:
            return ops.get_tiktok_agent_manifest()

    if should_register("get_tiktok_status"):
        @mcp.tool(description="Read-only TikTok integration status, sync status, webhook, and enrichment queue snapshot.")
        def get_tiktok_status() -> dict[str, Any]:
            return ops.get_tiktok_status()

    if should_register("get_tiktok_orders"):
        @mcp.tool(description="Read-only TikTok order snapshot from local synced/enriched order rows.")
        def get_tiktok_orders(days: int = 7, limit: int = 50, status: str = "", search: str = "") -> dict[str, Any]:
            return ops.get_tiktok_orders(days=days, limit=limit, status=status, search=search)

    if should_register("get_tiktok_products"):
        @mcp.tool(description="Read-only TikTok product snapshot from local synced product rows.")
        def get_tiktok_products(limit: int = 50, status: str = "", search: str = "") -> dict[str, Any]:
            return ops.get_tiktok_products(limit=limit, status=status, search=search)

    if should_register("get_tiktok_product_sales"):
        @mcp.tool(description="Read-only TikTok product sales by product title, SKU, or keyword from local paid order line items.")
        def get_tiktok_product_sales(product_query: str, days: int = 7, limit: int = 25) -> dict[str, Any]:
            return ops.get_tiktok_product_sales(product_query=product_query, days=days, limit=limit)

    if should_register("get_tiktok_top_products"):
        @mcp.tool(description="Read-only top TikTok products by paid local order line-item quantity or revenue.")
        def get_tiktok_top_products(days: int = 7, limit: int = 10, sort_by: str = "quantity") -> dict[str, Any]:
            return ops.get_tiktok_top_products(days=days, limit=limit, sort_by=sort_by)

    if should_register("get_shopify_product_sales"):
        @mcp.tool(description="Read-only Shopify product sales by product title, SKU, or keyword from local paid order line items.")
        def get_shopify_product_sales(product_query: str, days: int = 7, limit: int = 25) -> dict[str, Any]:
            return ops.get_shopify_product_sales(product_query=product_query, days=days, limit=limit)

    if should_register("get_shopify_top_products"):
        @mcp.tool(description="Read-only top Shopify products by paid local order line-item quantity or revenue.")
        def get_shopify_top_products(days: int = 7, limit: int = 10, sort_by: str = "quantity") -> dict[str, Any]:
            return ops.get_shopify_top_products(days=days, limit=limit, sort_by=sort_by)

    if should_register("get_price_lookup"):
        @mcp.tool(description="Read-only price lookup from stored inventory prices, price history, and recent TikTok sale prices.")
        def get_price_lookup(query: str, days: int = 30, limit: int = 10) -> dict[str, Any]:
            return ops.get_price_lookup(query=query, days=days, limit=limit)

    if should_register("get_market_trend_lookup"):
        @mcp.tool(description="Read-only market trend lookup comparing recent TikTok sale prices and stored price history.")
        def get_market_trend_lookup(query: str, days: int = 7, limit: int = 10) -> dict[str, Any]:
            return ops.get_market_trend_lookup(query=query, days=days, limit=limit)

    if should_register("get_web_search"):
        @mcp.tool(description="Read-only public web search returning cited search results without fetching result pages.")
        def get_web_search(query: str, limit: int = 5, freshness: str = "") -> dict[str, Any]:
            return ops.get_web_search(query=query, limit=limit, freshness=freshness)

    if should_register("get_tiktok_buyer_insights"):
        @mcp.tool(description="Read-only TikTok buyer intelligence from existing local reporting helpers.")
        def get_tiktok_buyer_insights(days: int = 90, limit: int = 50) -> dict[str, Any]:
            return ops.get_tiktok_buyer_insights(days=days, limit=limit)

    if should_register("get_tiktok_product_performance"):
        @mcp.tool(description="Read-only TikTok product performance from existing local reporting helpers.")
        def get_tiktok_product_performance(days: int = 30, limit: int = 50) -> dict[str, Any]:
            return ops.get_tiktok_product_performance(days=days, limit=limit)

    if should_register("get_tiktok_live_snapshot"):
        @mcp.tool(description="Read-only TikTok live-session, live analytics, and public-status cache snapshot.")
        def get_tiktok_live_snapshot() -> dict[str, Any]:
            return ops.get_tiktok_live_snapshot()

    return mcp


def create_mcp_server():
    try:
        from mcp.server.fastmcp import FastMCP
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The 'mcp' package is required to run the Degen Ops MCP server. "
            "Install repo requirements first: .\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt"
        ) from exc

    return register_degen_ops_tools(FastMCP("degen-ops-readonly"))


def main() -> None:
    server = create_mcp_server()
    server.run()


if __name__ == "__main__":
    main()
