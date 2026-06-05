from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Callable, Iterator

from sqlalchemy import text
from sqlmodel import Session, func, select

from .db import Session as DbSession
from .db import database_url
from .db import engine
from .models import TikTokOrder, TikTokProduct
from .ops_agent import READ_ONLY_GUARDRAILS, build_ops_agent_context, build_ops_agent_recommendation
from .reporting import (
    build_tiktok_buyer_insights,
    build_tiktok_product_performance,
    build_tiktok_reporting_summary,
    get_tiktok_order_rows,
)


DEGEN_OPS_BUSINESS_TOOL_NAMES = [
    "get_ops_agent_manifest",
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
]

TIKTOK_MCP_TOOL_NAMES = [
    "get_tiktok_agent_manifest",
    "get_tiktok_status",
    "get_tiktok_orders",
    "get_tiktok_products",
    "get_tiktok_buyer_insights",
    "get_tiktok_product_performance",
    "get_tiktok_live_snapshot",
]

DEGEN_OPS_MCP_TOOL_NAMES = [
    *DEGEN_OPS_BUSINESS_TOOL_NAMES,
    *TIKTOK_MCP_TOOL_NAMES,
]

DEGEN_OPS_SCOPE_TOOL_NAMES = {
    "owner": DEGEN_OPS_MCP_TOOL_NAMES,
    "tiktok": [
        "get_ops_agent_manifest",
        *TIKTOK_MCP_TOOL_NAMES,
    ],
    "partner": [
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "evaluate_inventory_buy",
        "generate_partner_update",
    ],
    "employee": [
        "get_ops_agent_manifest",
        "get_inventory_snapshot",
        "get_channel_velocity",
    ],
}

OWNER_ONLY_TOOL_NAMES = [
    "get_cash_snapshot",
    "get_loan_and_payback_snapshot",
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
    redacted["owner_scope_required_for"] = OWNER_ONLY_TOOL_NAMES[:]
    redacted["read_only"] = True
    return redacted


class DegenOpsMcpHarness:
    def __init__(self, session_factory: Callable[[], Any] | None = None):
        self._session_factory = session_factory or _default_session_factory

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

    if should_register("get_loan_and_payback_snapshot"):
        @mcp.tool(description="Read-only loan proceeds, owner payback, and payout timing evidence.")
        def get_loan_and_payback_snapshot(days: int = 90) -> dict[str, Any]:
            return ops.get_loan_and_payback_snapshot(days=days)

    if should_register("evaluate_inventory_buy"):
        @mcp.tool(description="Read-only proposed inventory-buy evaluation with evidence-backed routing and cash plan.")
        def evaluate_inventory_buy(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.evaluate_inventory_buy(scenario=scenario, days=days, audience_scope=normalized_scope)

    if should_register("generate_partner_update"):
        @mcp.tool(description="Read-only partner-ready weekly update generated from a proposed buy scenario.")
        def generate_partner_update(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.generate_partner_update(scenario=scenario, days=days, audience_scope=normalized_scope)

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
