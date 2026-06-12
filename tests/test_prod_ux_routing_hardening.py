from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

import app.main as main_module
from app.discord.bookkeeping import list_detected_bookkeeping_posts
from app.models import DiscordMessage, TikTokOrder
from app.reporting import build_buyer_profiles
from app.routers.bookkeeping import bookkeeping_page
from app.routers.messages import messages_table, review_table
from app.routers.reports import reports_page
from app.routers.tiktok_analytics import tiktok_clients_page
from app.shared import REPORT_SOURCE_ALL


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "app" / "templates"


def _template(name: str) -> str:
    return (TEMPLATE_DIR / name).read_text(encoding="utf-8")


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _request(path: str, role: str = "admin") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        id=1,
        username="ops",
        display_name="Ops User",
        role=role,
    )
    return request


def test_inventory_shopify_sync_uses_current_shared_stylesheet() -> None:
    source = _template("inventory_shopify_sync.html")

    assert '/static/linear.css' in source
    assert "linear-theme.css" not in source
    assert "linear-sidebar.css" not in source


def test_sidebar_logout_is_a_post_form_without_javascript_fetch_link() -> None:
    source = _template("_linear_sidebar.html")

    assert 'method="post" action="/logout"' in source.lower()
    assert 'href="/logout"' not in source
    assert "fetch('/logout'" not in source


def test_shared_sidebar_pages_pass_current_user_to_templates() -> None:
    engine = _engine()
    try:
        with Session(engine) as session, patch(
            "app.routers.messages.require_role_response", return_value=None
        ), patch(
            "app.routers.messages.get_available_channel_choices", return_value=([], False)
        ), patch(
            "app.routers.reports.require_role_response", return_value=None
        ), patch(
            "app.routers.bookkeeping.require_role_response", return_value=None
        ), patch(
            "app.routers.tiktok_analytics.require_role_response", return_value=None
        ):
            calls = [
                (
                    "/table",
                    lambda request: messages_table(
                        request,
                        status=None,
                        channel_id=None,
                        expense_category=None,
                        source=REPORT_SOURCE_ALL,
                        after=None,
                        before=None,
                        sort_by="time",
                        sort_dir="desc",
                        page=1,
                        limit=100,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                (
                    "/review-table",
                    lambda request: review_table(
                        request,
                        channel_id=None,
                        expense_category=None,
                        after=None,
                        before=None,
                        sort_by="time",
                        sort_dir="desc",
                        page=1,
                        limit=100,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                (
                    "/reports",
                    lambda request: reports_page(
                        request,
                        start=None,
                        end=None,
                        channel_id=None,
                        entry_kind=None,
                        source=REPORT_SOURCE_ALL,
                        session=session,
                    ),
                ),
                (
                    "/bookkeeping",
                    lambda request: bookkeeping_page(
                        request,
                        import_id=None,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                ("/tiktok/clients", tiktok_clients_page),
            ]

            for path, call in calls:
                request = _request(path)
                response = call(request)
                assert response.context.get("current_user") is request.state.current_user, path
    finally:
        engine.dispose()


def test_admin_home_alias_redirects_to_admin() -> None:
    client = TestClient(main_module.app)
    response = client.get("/admin/home", follow_redirects=False)

    assert response.status_code in {301, 302, 303, 307, 308}
    assert response.headers["location"] == "/admin"


def test_logout_get_renders_friendly_post_confirmation() -> None:
    client = TestClient(main_module.app)
    response = client.get("/logout", follow_redirects=False)

    assert response.status_code == 200
    assert "method=\"post\"" in response.text.lower()
    assert "action=\"/logout\"" in response.text.lower()
    assert "Method Not Allowed" not in response.text


def test_detected_bookkeeping_posts_expose_clean_display_content() -> None:
    engine = _engine()
    sheet_url = "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0"
    try:
        with Session(engine) as session:
            session.add(
                DiscordMessage(
                    discord_message_id="bookkeeping-post-1",
                    channel_id="financials",
                    channel_name="Financials",
                    author_id="author-1",
                    author_name="Jeffrey",
                    content=f"**24th** <{sheet_url}>",
                    attachment_urls_json="[]",
                    created_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
                )
            )
            session.commit()

            [post] = list_detected_bookkeeping_posts(session)

        assert post["display_content"] == "24th"
        assert post["sheet_url"] == sheet_url
        assert "docs.google.com" not in post["display_content"]
        assert "<" not in post["display_content"]
        assert "**" not in post["display_content"]
    finally:
        engine.dispose()


def test_unknown_tiktok_buyer_display_name_is_human_readable() -> None:
    engine = _engine()
    try:
        with Session(engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="unknown-buyer-order",
                    shop_id="shop-1",
                    order_number="#TT-UNKNOWN",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                    customer_name="-",
                    financial_status="paid",
                    subtotal_price=25.0,
                    total_price=25.0,
                )
            )
            session.commit()

            [profile] = build_buyer_profiles(session, days=180)

        assert profile["name"] == "Unknown buyer"
        assert profile["buyer_key"] == "unknown-buyer"
    finally:
        engine.dispose()


def test_tiktok_analytics_duration_formatter_normalizes_full_day_rollover() -> None:
    source = _template("tiktok_analytics.html")

    assert "var roundedHours = Math.round(h);" in source
    assert "roundedHours % 24 === 0" in source
    assert "Math.floor(roundedHours / 24)" in source


def test_ops_metric_cards_and_review_shortcuts_have_resilient_layout_hooks() -> None:
    messages_source = _template("messages_table.html")
    reports_source = _template("reports.html")

    assert "overflow-wrap:anywhere" in messages_source
    assert "font-size:clamp" in messages_source
    assert "decoding=\"async\"" in messages_source
    assert "review-rail review-shortcuts-rail" in messages_source
    assert "review-shortcut-actions" in messages_source

    assert "overflow-wrap:anywhere" in reports_source
    assert "font-size:clamp" in reports_source
