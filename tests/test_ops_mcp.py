from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta, timezone
import json

import pytest
import httpx
from sqlmodel import SQLModel, Session, create_engine, select

from app.models import (
    ClockifyTimeEntry,
    BuylistSubmission,
    DiscordMessage,
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
from app.ops_mcp import (
    DegenOpsMcpHarness,
    DEGEN_OPS_MCP_TOOL_NAMES,
    DEGEN_OPS_SCOPE_TOOL_NAMES,
    TIKTOK_MCP_TOOL_NAMES,
    _normalize_scope,
    _apply_session_read_only,
    _reset_session_read_only,
    register_degen_ops_tools,
)


class FakeMcp:
    def __init__(self):
        self.tools = {}

    def tool(self, name=None, description=None):
        def decorator(func):
            tool_name = name or func.__name__
            self.tools[tool_name] = {"func": func, "description": description}
            return func

        return decorator


class FakeSession:
    pass


class FakeSqlSession:
    def __init__(self):
        self.statements = []

    def exec(self, statement):
        self.statements.append(str(statement))


def test_read_only_session_guard_uses_sqlite_query_only():
    session = FakeSqlSession()

    _apply_session_read_only(session, "sqlite:///data/test.db")
    _reset_session_read_only(session, "sqlite:///data/test.db")

    assert session.statements == ["PRAGMA query_only = ON", "PRAGMA query_only = OFF"]


def test_read_only_session_guard_uses_postgres_transaction_read_only():
    session = FakeSqlSession()

    _apply_session_read_only(session, "postgresql+psycopg://example/db")
    _reset_session_read_only(session, "postgresql+psycopg://example/db")

    assert session.statements == ["SET TRANSACTION READ ONLY"]


@pytest.mark.parametrize("url", ["postgresql://example/db", "postgres://example/db"])
def test_read_only_session_guard_recognizes_common_postgres_urls(url):
    session = FakeSqlSession()

    _apply_session_read_only(session, url)

    assert session.statements == ["SET TRANSACTION READ ONLY"]


def test_register_degen_ops_tools_exposes_bounded_read_only_tool_names():
    fake = FakeMcp()
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    register_degen_ops_tools(fake, harness=harness, scope="owner")

    assert sorted(fake.tools) == sorted(DEGEN_OPS_MCP_TOOL_NAMES)
    assert "run_sql" not in fake.tools
    assert "execute_sql" not in fake.tools
    assert all("read-only" in (tool["description"] or "").lower() for tool in fake.tools.values())
    assert set(TIKTOK_MCP_TOOL_NAMES).issubset(fake.tools)


def test_degen_ops_mcp_launcher_imports_from_outside_repo(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "degen_ops_mcp.py"
    probe = f"import runpy; runpy.run_path({str(script)!r})"

    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_register_degen_ops_tools_employee_scope_limits_sensitive_tools():
    fake = FakeMcp()
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    register_degen_ops_tools(fake, harness=harness, scope="employee")

    assert sorted(fake.tools) == sorted(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"])
    assert "get_finance_snapshot" not in fake.tools
    assert "get_cash_snapshot" not in fake.tools
    assert "get_loan_and_payback_snapshot" not in fake.tools
    assert "evaluate_inventory_buy" not in fake.tools
    assert "get_tiktok_orders" not in fake.tools
    assert "get_web_search" in fake.tools


def test_manager_scope_includes_employee_ops_but_excludes_owner_cash_and_loan_tools():
    tools = set(DEGEN_OPS_SCOPE_TOOL_NAMES["manager"])

    assert set(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"]).issubset(tools)
    assert set(TIKTOK_MCP_TOOL_NAMES).issubset(tools)
    assert "get_employee_clock_status" in tools
    assert "get_employee_ops_status" in tools
    assert "get_cash_snapshot" not in tools
    assert "get_loan_and_payback_snapshot" not in tools
    assert "evaluate_inventory_buy" not in tools


def test_role_scopes_are_hierarchical_for_employee_manager_owner():
    employee = set(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"])
    manager = set(DEGEN_OPS_SCOPE_TOOL_NAMES["manager"])
    owner = set(DEGEN_OPS_SCOPE_TOOL_NAMES["owner"])

    assert employee.issubset(manager)
    assert manager.issubset(owner)
    assert {"get_price_lookup", "get_market_trend_lookup"}.issubset(employee)


def test_register_degen_ops_tools_partner_scope_excludes_raw_cash_and_loan_tools():
    fake = FakeMcp()
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    register_degen_ops_tools(fake, harness=harness, scope="partner")

    assert sorted(fake.tools) == sorted(DEGEN_OPS_SCOPE_TOOL_NAMES["partner"])
    assert "get_finance_snapshot" in fake.tools
    assert "get_inventory_snapshot" in fake.tools
    assert "get_channel_velocity" in fake.tools
    assert "evaluate_inventory_buy" in fake.tools
    assert "generate_partner_update" in fake.tools
    assert "get_cash_snapshot" not in fake.tools
    assert "get_loan_and_payback_snapshot" not in fake.tools
    assert "get_tiktok_orders" not in fake.tools
    assert "get_web_search" in fake.tools


def test_register_degen_ops_tools_tiktok_scope_is_dedicated_read_only_agent():
    fake = FakeMcp()
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    register_degen_ops_tools(fake, harness=harness, scope="tiktok")

    assert sorted(fake.tools) == sorted(DEGEN_OPS_SCOPE_TOOL_NAMES["tiktok"])
    assert "get_ops_agent_manifest" in fake.tools
    assert set(TIKTOK_MCP_TOOL_NAMES).issubset(fake.tools)
    assert "get_finance_snapshot" not in fake.tools
    assert "get_cash_snapshot" not in fake.tools
    assert all("read-only" in (tool["description"] or "").lower() for tool in fake.tools.values())


def test_inventory_snapshot_redacts_cost_basis_outside_owner_scope(monkeypatch):
    context = {
        "inventory_snapshot": {
            "active_items": 4,
            "estimated_list_value": 1200.0,
            "cost_basis_total": 700.0,
            "evidence_url": "/inventory",
        }
    }
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(harness, "get_context", lambda days=90: context)

    owner = harness.get_inventory_snapshot(audience_scope="owner")
    employee = harness.get_inventory_snapshot(audience_scope="employee")

    assert owner["inventory_snapshot"]["cost_basis_total"] == 700.0
    assert "cost_basis_total" not in employee["inventory_snapshot"]
    assert "cost_basis_total hidden outside owner scope" in employee["redactions"]


def test_register_degen_ops_tools_uses_env_scope(monkeypatch):
    monkeypatch.setenv("DEGEN_OPS_MCP_SCOPE", "employee")
    fake = FakeMcp()

    register_degen_ops_tools(fake, harness=DegenOpsMcpHarness(session_factory=lambda: FakeSession()))

    assert sorted(fake.tools) == sorted(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"])


def test_missing_scope_defaults_to_employee(monkeypatch):
    monkeypatch.delenv("DEGEN_OPS_MCP_SCOPE", raising=False)
    fake = FakeMcp()

    register_degen_ops_tools(fake, harness=DegenOpsMcpHarness(session_factory=lambda: FakeSession()))

    assert _normalize_scope(None) == "employee"
    assert sorted(fake.tools) == sorted(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"])
    assert "get_cash_snapshot" not in fake.tools


def test_register_degen_ops_tools_rejects_unknown_scope():
    fake = FakeMcp()

    with pytest.raises(ValueError, match="Unsupported Degen Ops MCP scope"):
        register_degen_ops_tools(fake, harness=DegenOpsMcpHarness(session_factory=lambda: FakeSession()), scope="adminish")


def test_mcp_manifest_reports_scope_tools_and_read_only_guardrails():
    fake = FakeMcp()
    register_degen_ops_tools(fake, harness=DegenOpsMcpHarness(session_factory=lambda: FakeSession()), scope="employee")

    manifest = fake.tools["get_ops_agent_manifest"]["func"]()

    assert manifest["scope"] == "employee"
    assert manifest["tools"] == DEGEN_OPS_SCOPE_TOOL_NAMES["employee"]
    assert manifest["read_only"] is True
    assert "No money movement" in manifest["guardrails"]


def test_tiktok_agent_manifest_lists_read_endpoints_and_blocks_writes():
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    manifest = harness.get_tiktok_agent_manifest()

    endpoint_ids = {endpoint["id"] for endpoint in manifest["tiktok_api_endpoints"]}
    assert "order_search" in endpoint_ids
    assert "product_create" in endpoint_ids
    assert manifest["read_only"] is True
    assert any(
        endpoint["id"] == "product_create"
        and endpoint["enabled"] is False
        and endpoint["approval_required"] is True
        for endpoint in manifest["tiktok_api_endpoints"]
    )
    assert "get_tiktok_orders" in manifest["tools"]


def test_tiktok_order_snapshot_uses_local_rows_without_raw_payloads():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            TikTokOrder(
                tiktok_order_id="tt-1",
                order_number="order-1",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                customer_name="Buyer One",
                customer_email="buyer@example.com",
                total_price=110.0,
                subtotal_price=100.0,
                total_tax=10.0,
                financial_status="PAID",
                fulfillment_status="AWAITING_SHIPMENT",
                order_status="AWAITING_SHIPMENT",
                currency="USD",
                line_items_json='[{"product_name":"Pokemon Pack","quantity":2,"sale_price":50}]',
                raw_payload='{"secret":"do not leak"}',
            )
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_orders(days=30, limit=10)

    assert result["read_only"] is True
    assert result["summary"]["paid_orders"] == 1
    assert result["orders"][0]["tiktok_order_id"] == "tt-1"
    assert result["orders"][0]["customer_name"] == "Buyer One"
    assert "customer_email" not in result["orders"][0]
    assert "raw_payload" not in result["orders"][0]


def test_tiktok_product_snapshot_uses_local_rows_without_raw_payloads():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            TikTokProduct(
                tiktok_product_id="prod-1",
                title="Charizard Surprise Set",
                status="ACTIVATE",
                audit_status="APPROVED",
                category_id="cat-1",
                category_name="Trading Cards",
                main_image_url="https://example.com/image.jpg",
                skus_json='[{"sku_id":"sku-1","seller_sku":"DGN-1","price":25,"inventory":4}]',
                raw_payload='{"secret":"do not leak"}',
            )
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_products(limit=10, status="ACTIVATE")

    assert result["read_only"] is True
    assert result["summary"]["total"] == 1
    assert result["products"][0]["tiktok_product_id"] == "prod-1"
    assert result["products"][0]["sku_count"] == 1
    assert "raw_payload" not in result["products"][0]


def test_tiktok_product_sales_matches_query_and_counts_paid_recent_line_items():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                TikTokOrder(
                    tiktok_order_id="tt-151-a",
                    order_number="order-151-a",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=40.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [
                            {
                                "title": "Pokemon 151 Booster Pack",
                                "quantity": 3,
                                "unit_price": 8.0,
                                "sku_id": "sku-151",
                            },
                            {"title": "Obsidian Flames Pack", "quantity": 1, "unit_price": 6.0},
                        ]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-151-b",
                    order_number="order-151-b",
                    created_at=now - timedelta(days=5),
                    updated_at=now - timedelta(days=5),
                    subtotal_price=16.0,
                    financial_status="PAID",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Packs", "quantity": 2, "unit_price": 8.0}]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-pending",
                    order_number="order-pending",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=80.0,
                    financial_status="pending",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 10, "unit_price": 8.0}]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-old",
                    order_number="order-old",
                    created_at=now - timedelta(days=20),
                    updated_at=now - timedelta(days=20),
                    subtotal_price=8.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 1, "unit_price": 8.0}]
                    ),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_product_sales(product_query="151 packs", days=7)

    assert result["read_only"] is True
    assert result["summary"]["matched_quantity"] == 5
    assert result["summary"]["matched_order_count"] == 2
    assert result["summary"]["matched_revenue"] == 40.0
    assert result["matches"][0]["title"] == "Pokemon 151 Booster Pack"
    assert result["matches"][0]["quantity"] == 3
    assert result["range"]["days"] == 7
    assert result["evidence"][0]["source"] == "tiktok_orders.line_items"


def test_tiktok_product_sales_returns_candidates_when_no_match():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add(
            TikTokOrder(
                tiktok_order_id="tt-candidate",
                order_number="order-candidate",
                created_at=now - timedelta(days=1),
                updated_at=now - timedelta(days=1),
                subtotal_price=12.0,
                financial_status="paid",
                line_items_summary_json=json.dumps(
                    [{"title": "Pokemon 151 Booster Pack", "quantity": 1, "unit_price": 12.0}]
                ),
            )
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_product_sales(product_query="lorcana", days=7)

    assert result["summary"]["matched_quantity"] == 0
    assert result["data_gaps"] == ["No TikTok line items matched product_query='lorcana'."]
    assert result["candidates"][0]["title"] == "Pokemon 151 Booster Pack"


def test_tiktok_product_sales_does_not_match_short_query_terms_inside_numeric_ids():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add(
            TikTokOrder(
                tiktok_order_id="tt-false-positive",
                order_number="order-false-positive",
                created_at=now - timedelta(days=1),
                updated_at=now - timedelta(days=1),
                subtotal_price=2.0,
                financial_status="paid",
                line_items_summary_json=json.dumps(
                    [
                        {
                            "title": "Temporal Forces Booster Pack",
                            "quantity": 1,
                            "unit_price": 2.0,
                            "product_id": "1732438364215415112",
                            "sku_id": "1732438347754869064",
                        }
                    ]
                ),
            )
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_product_sales(product_query="151 packs", days=7)

    assert result["summary"]["matched_quantity"] == 0
    assert result["matches"] == []


def test_tiktok_top_products_returns_quantity_sorted_paid_line_items():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                TikTokOrder(
                    tiktok_order_id="tt-top-1",
                    order_number="order-top-1",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=90.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [
                            {"title": "Pokemon 151 Booster Pack", "quantity": 5, "unit_price": 10.0},
                            {"title": "One Piece Pack", "quantity": 2, "unit_price": 20.0},
                        ]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-top-2",
                    order_number="order-top-2",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=30.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 3, "unit_price": 10.0}]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-top-refund",
                    order_number="order-top-refund",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=999.0,
                    financial_status="refunded",
                    line_items_summary_json=json.dumps(
                        [{"title": "Refunded Product", "quantity": 99, "unit_price": 99.0}]
                    ),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_tiktok_top_products(days=7, limit=5, sort_by="quantity")

    assert result["read_only"] is True
    assert result["summary"]["paid_orders_scanned"] == 2
    assert result["products"][0]["title"] == "Pokemon 151 Booster Pack"
    assert result["products"][0]["quantity"] == 8
    assert result["products"][0]["revenue"] == 80.0
    assert result["products"][0]["order_count"] == 2
    assert all(product["title"] != "Refunded Product" for product in result["products"])


def test_shopify_product_sales_matches_paid_line_items_by_keyword():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                ShopifyOrder(
                    shopify_order_id="shopify-1",
                    order_number="S1001",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=60.0,
                    total_price=66.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 3, "unit_price": 20.0, "sku": "151-PACK"}]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-2",
                    order_number="S1002",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=40.0,
                    total_price=44.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 2, "unit_price": 20.0, "sku": "151-PACK"}]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-unpaid",
                    order_number="S1003",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=999.0,
                    total_price=999.0,
                    financial_status="pending",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 99, "unit_price": 99.0}]
                    ),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_shopify_product_sales(product_query="151 pack", days=7, limit=5)

    assert result["summary"]["matched_quantity"] == 5
    assert result["summary"]["matched_order_count"] == 2
    assert result["summary"]["matched_revenue"] == 100.0
    assert result["matches"][0]["title"] == "Pokemon 151 Booster Pack"
    assert result["read_only"] is True


def test_shopify_top_products_returns_revenue_sorted_paid_line_items():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                ShopifyOrder(
                    shopify_order_id="shopify-top-1",
                    order_number="S2001",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=120.0,
                    total_price=130.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [
                            {"title": "Pokemon 151 Booster Pack", "quantity": 4, "unit_price": 20.0},
                            {"title": "Premium Slab", "quantity": 1, "unit_price": 40.0},
                        ]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-top-2",
                    order_number="S2002",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=200.0,
                    total_price=210.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Premium Slab", "quantity": 1, "unit_price": 200.0}]
                    ),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_shopify_top_products(days=7, limit=5, sort_by="revenue")

    assert result["products"][0]["title"] == "Premium Slab"
    assert result["products"][0]["revenue"] == 240.0
    assert result["products"][0]["quantity"] == 2
    assert result["products"][1]["title"] == "Pokemon 151 Booster Pack"


def test_sales_summary_combines_paid_tiktok_shopify_and_discord_sales():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                TikTokOrder(
                    tiktok_order_id="tt-sales-summary",
                    order_number="tt-sales-summary",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=80.0,
                    total_price=88.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 4, "unit_price": 20.0}]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-sales-summary",
                    order_number="S3001",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=100.0,
                    total_price=110.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Premium Slab", "quantity": 1, "unit_price": 100.0}]
                    ),
                ),
                DiscordMessage(
                    discord_message_id="discord-sale-1",
                    channel_id="sales",
                    channel_name="store-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell 40 cash",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=40.0,
                    money_out=0.0,
                    category="Singles",
                ),
                DiscordMessage(
                    discord_message_id="discord-buy-1",
                    channel_id="buys",
                    channel_name="store-buys",
                    author_id="42",
                    author_name="Jeff",
                    content="buy 500 cash",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="buy",
                    money_in=0.0,
                    money_out=500.0,
                    category="Inventory",
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_sales_summary(days=7)

    assert result["summary"]["total_revenue"] == 220.0
    assert result["channels"] == [
        {"channel": "shopify", "orders": 1, "revenue": 100.0, "avg_order_value": 100.0},
        {"channel": "tiktok", "orders": 1, "revenue": 80.0, "avg_order_value": 80.0},
        {"channel": "discord", "orders": 1, "revenue": 40.0, "avg_order_value": 40.0},
    ]
    assert result["summary"]["top_channel_by_revenue"] == "shopify"
    assert result["read_only"] is True


def test_discord_sales_summary_filters_by_keyword_and_breaks_down_channels():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                DiscordMessage(
                    discord_message_id="discord-151-pack-1",
                    channel_id="show-1",
                    channel_name="collect-a-con-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell Pokemon 151 booster packs 60 cash",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=60.0,
                    money_out=0.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Packs"]),
                ),
                DiscordMessage(
                    discord_message_id="discord-151-pack-2",
                    channel_id="discord-store",
                    channel_name="discord-store-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="151 pack bundle sell 40 venmo",
                    created_at=now - timedelta(days=2),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=40.0,
                    money_out=0.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Pack Bundle"]),
                ),
                DiscordMessage(
                    discord_message_id="discord-slab",
                    channel_id="discord-store",
                    channel_name="discord-store-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell slab 200",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=200.0,
                    money_out=0.0,
                    category="Slabs",
                    item_names_json=json.dumps(["Premium Slab"]),
                ),
                DiscordMessage(
                    discord_message_id="discord-buy",
                    channel_id="discord-store",
                    channel_name="discord-store-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="buy 151 packs 500",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="buy",
                    money_in=0.0,
                    money_out=500.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Pack"]),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_discord_sales_summary(product_query="151 packs", days=7)

    assert result["read_only"] is True
    assert result["summary"] == {
        "matched_sales": 2,
        "matched_revenue": 100.0,
        "avg_sale_value": 50.0,
        "top_channel_by_revenue": "collect-a-con-sales",
        "top_category_by_revenue": "Sealed",
    }
    assert result["channels"] == [
        {"channel": "collect-a-con-sales", "sales": 1, "revenue": 60.0, "avg_sale_value": 60.0},
        {"channel": "discord-store-sales", "sales": 1, "revenue": 40.0, "avg_sale_value": 40.0},
    ]
    assert result["categories"] == [
        {"category": "Sealed", "sales": 2, "revenue": 100.0, "avg_sale_value": 50.0}
    ]
    assert result["matches"][0]["discord_message_id"] == "discord-151-pack-1"
    assert result["evidence"][0]["source"] == "discord_financial_rows"


def test_employee_clock_status_returns_latest_running_entry_for_owner_scope_data():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        user = User(
            username="alex",
            password_hash="hash",
            display_name="Alex",
            role="viewer",
            is_active=True,
        )
        session.add(user)
        session.commit()
        session.add(EmployeeProfile(user_id=user.id, clockify_user_id="clockify-alex"))
        session.add_all(
            [
                ClockifyTimeEntry(
                    clockify_entry_id="old-entry",
                    clockify_user_id="clockify-alex",
                    user_id=user.id,
                    description="Morning shift",
                    start_at=now - timedelta(hours=6),
                    end_at=now - timedelta(hours=2),
                    duration_seconds=4 * 3600,
                    is_running=False,
                ),
                ClockifyTimeEntry(
                    clockify_entry_id="running-entry",
                    clockify_user_id="clockify-alex",
                    user_id=user.id,
                    description="Afternoon shift",
                    start_at=now - timedelta(minutes=45),
                    end_at=None,
                    duration_seconds=0,
                    is_running=True,
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_employee_clock_status(person_query="Alex", days=1)

    assert result["summary"]["matched_employee_count"] == 1
    assert result["employees"][0]["display_name"] == "Alex"
    assert result["employees"][0]["clock_status"] == "clocked_in"
    assert result["employees"][0]["latest_entry"]["description"] == "Afternoon shift"
    assert result["read_only"] is True


def test_employee_ops_status_summarizes_supply_buylist_and_timeoff_queues():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    today = now.date()
    with Session(engine) as session:
        user = User(
            username="alex",
            password_hash="hash",
            display_name="Alex",
            role="viewer",
            is_active=True,
        )
        session.add(user)
        session.commit()
        session.add_all(
            [
                SupplyRequest(
                    submitted_by_user_id=user.id,
                    title="Top loaders",
                    description="Need more 35pt top loaders",
                    urgency="high",
                    status="submitted",
                    created_at=now - timedelta(hours=2),
                ),
                BuylistSubmission(
                    submitted_by_user_id=user.id,
                    customer_name="Walk-in seller",
                    status="submitted",
                    totals_json=json.dumps({"offer_total": 125.0}),
                    created_at=now - timedelta(hours=1),
                ),
                TimeOffRequest(
                    submitted_by_user_id=user.id,
                    start_date=today + timedelta(days=3),
                    end_date=today + timedelta(days=4),
                    status="approved",
                    reason="Family",
                    created_at=now - timedelta(days=1),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_employee_ops_status(person_query="Alex", days=7, limit=10)

    assert result["summary"]["person_query"] == "Alex"
    assert result["summary"]["supply_requests"] == {"submitted": 1}
    assert result["summary"]["buylist_submissions"] == {"submitted": 1}
    assert result["summary"]["time_off_requests"] == {"approved": 1}
    assert {row["kind"] for row in result["items"]} == {
        "supply_request",
        "buylist_submission",
        "time_off_request",
    }
    assert result["read_only"] is True


def test_ops_memory_respects_scope_and_active_filter():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                OpsBotMemory(
                    scope="public",
                    key="default_channel_question",
                    value="Ask whether top-products questions mean TikTok, Shopify, Discord, or all channels.",
                    tags_json=json.dumps(["preference"]),
                    created_at=now,
                ),
                OpsBotMemory(
                    scope="owner",
                    key="reserve_floor",
                    value="Owner cash reserve floor must be configured before safety verdicts.",
                    tags_json=json.dumps(["cash"]),
                    created_at=now,
                ),
                OpsBotMemory(
                    scope="employee",
                    key="stream_room_note",
                    value="Employees can ask for TikTok product sales but not owner cash.",
                    tags_json=json.dumps(["scope"]),
                    created_at=now,
                ),
                OpsBotMemory(
                    scope="public",
                    key="inactive",
                    value="Do not show this.",
                    is_active=False,
                    created_at=now,
                ),
            ]
        )
        session.commit()

        employee_result = DegenOpsMcpHarness(session_factory=lambda: session).get_ops_memory(
            query="",
            audience_scope="employee",
        )
        owner_result = DegenOpsMcpHarness(session_factory=lambda: session).get_ops_memory(
            query="reserve",
            audience_scope="owner",
        )

    assert [row["key"] for row in employee_result["memories"]] == [
        "stream_room_note",
        "default_channel_question",
    ]
    assert "reserve_floor" not in [row["key"] for row in employee_result["memories"]]
    assert owner_result["memories"][0]["key"] == "reserve_floor"
    assert owner_result["read_only"] is True


def test_ops_memory_proposal_is_read_only_and_does_not_write():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.propose_ops_memory(
            key="weekly_update_day",
            value="Partner update cadence is Monday morning.",
            scope="partner",
            tags=["partner-update"],
            proposed_by="Jeff",
        )
        rows = session.exec(select(OpsBotMemory)).all()

    assert rows == []
    assert result["proposal"]["key"] == "weekly_update_day"
    assert result["proposal"]["scope"] == "partner"
    assert result["requires_owner_approval"] is True
    assert result["read_only"] is True


def test_weekly_partner_update_draft_uses_read_only_business_snapshots(monkeypatch):
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(
        harness,
        "get_finance_snapshot",
        lambda days=7: {
            "finance_statement": {
                "revenue": 10000.0,
                "operating_profit": 1500.0,
                "operating_expenses": 2500.0,
            },
            "read_only": True,
        },
    )
    monkeypatch.setattr(
        harness,
        "get_sales_summary",
        lambda days=7: {
            "summary": {"total_revenue": 2200.0, "total_orders": 30, "top_channel_by_revenue": "tiktok"},
            "channels": [{"channel": "tiktok", "revenue": 1200.0, "orders": 20}],
            "read_only": True,
        },
    )
    monkeypatch.setattr(
        harness,
        "get_inventory_snapshot",
        lambda: {"inventory_snapshot": {"active_items": 42, "estimated_list_value": 9000.0}, "read_only": True},
    )

    result = harness.generate_weekly_partner_update_draft(days=7, audience_scope="partner")

    assert result["read_only"] is True
    assert result["write_performed"] is False
    assert result["approval_required"] is True
    assert "Weekly Degen Ops Update" in result["draft"]
    assert "Revenue: $10,000" in result["draft"]
    assert "Top channel: tiktok" in result["draft"]
    assert result["evidence"][0]["source"] == "finance_snapshot"


def test_price_lookup_returns_inventory_price_history_and_recent_tiktok_sales():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        item = InventoryItem(
            barcode="DGN-151001",
            item_type="sealed",
            game="Pokemon",
            card_name="Scarlet & Violet 151 Booster Pack",
            set_name="Scarlet & Violet 151",
            sealed_product_kind="booster_pack",
            quantity=12,
            auto_price=28.0,
            list_price=29.99,
            cost_basis=18.0,
            last_priced_at=now - timedelta(hours=2),
        )
        session.add(item)
        session.commit()
        session.add(
            PriceHistory(
                item_id=item.id,
                source="tcgtracking",
                market_price=28.0,
                low_price=25.0,
                high_price=32.0,
                fetched_at=now - timedelta(hours=2),
            )
        )
        session.add(
            TikTokOrder(
                tiktok_order_id="tt-151-price",
                order_number="order-151-price",
                created_at=now - timedelta(days=1),
                updated_at=now - timedelta(days=1),
                subtotal_price=59.98,
                financial_status="paid",
                line_items_summary_json=json.dumps(
                    [{"title": "Pokemon Scarlet & Violet 151 Booster Pack", "quantity": 2, "unit_price": 29.99}]
                ),
            )
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_price_lookup(query="151 booster pack", days=7)

    assert result["read_only"] is True
    assert result["summary"]["recommended_price"] == 29.99
    assert result["inventory_matches"][0]["barcode"] == "DGN-151001"
    assert result["inventory_matches"][0]["effective_price"] == 29.99
    assert result["inventory_matches"][0]["latest_price_history"]["source"] == "tcgtracking"
    assert result["recent_tiktok_sales"]["summary"]["matched_quantity"] == 2
    assert result["evidence"][0]["source"] == "inventory_items"


def test_price_lookup_uses_cross_channel_recent_sales_when_inventory_missing():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                TikTokOrder(
                    tiktok_order_id="tt-151-price-cross",
                    order_number="order-151-price-cross",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=30.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 2, "unit_price": 15.0}]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-151-price-cross",
                    order_number="S151",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=40.0,
                    total_price=44.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 2, "unit_price": 20.0}]
                    ),
                ),
                DiscordMessage(
                    discord_message_id="discord-151-price-cross",
                    channel_id="store-sales",
                    channel_name="discord-store-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell Pokemon 151 booster packs 25 cash",
                    created_at=now - timedelta(days=1),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=25.0,
                    money_out=0.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Pack"]),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_price_lookup(query="151 booster pack", days=7)

    assert result["summary"]["recommended_price"] == 19.0
    assert result["summary"]["recommended_price_source"] == "recent_cross_channel_avg_sale_price"
    assert result["summary"]["recent_cross_channel_quantity"] == 5
    assert result["summary"]["recent_cross_channel_revenue"] == 95.0
    assert result["recent_channel_prices"] == [
        {"channel": "tiktok", "quantity": 2, "revenue": 30.0, "avg_sale_price": 15.0},
        {"channel": "shopify", "quantity": 2, "revenue": 40.0, "avg_sale_price": 20.0},
        {"channel": "discord", "quantity": 1, "revenue": 25.0, "avg_sale_price": 25.0},
    ]


def test_price_lookup_reports_data_gap_when_no_price_sources_match():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_price_lookup(query="missing card", days=7)

    assert result["summary"]["recommended_price"] is None
    assert "No stored inventory price matched query='missing card'." in result["data_gaps"]
    assert "No recent TikTok sale price matched query='missing card'." in result["data_gaps"]


def test_market_trend_lookup_compares_recent_and_previous_tiktok_windows_with_price_history():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        item = InventoryItem(
            barcode="DGN-151002",
            item_type="sealed",
            game="Pokemon",
            card_name="Scarlet & Violet 151 Booster Pack",
            set_name="Scarlet & Violet 151",
            quantity=4,
            auto_price=32.0,
            last_priced_at=now,
        )
        session.add(item)
        session.commit()
        session.add_all(
            [
                PriceHistory(
                    item_id=item.id,
                    source="tcgtracking",
                    market_price=24.0,
                    fetched_at=now - timedelta(days=14),
                ),
                PriceHistory(
                    item_id=item.id,
                    source="tcgtracking",
                    market_price=32.0,
                    fetched_at=now - timedelta(days=1),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-current",
                    order_number="order-current",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(days=2),
                    subtotal_price=64.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon Scarlet & Violet 151 Booster Pack", "quantity": 2, "unit_price": 32.0}]
                    ),
                ),
                TikTokOrder(
                    tiktok_order_id="tt-previous",
                    order_number="order-previous",
                    created_at=now - timedelta(days=10),
                    updated_at=now - timedelta(days=10),
                    subtotal_price=48.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon Scarlet & Violet 151 Booster Pack", "quantity": 2, "unit_price": 24.0}]
                    ),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_market_trend_lookup(query="151 booster pack", days=7)

    assert result["read_only"] is True
    assert result["summary"]["trend_direction"] == "up"
    assert result["tiktok_trend"]["current_window"]["avg_price"] == 32.0
    assert result["tiktok_trend"]["previous_window"]["avg_price"] == 24.0
    assert result["price_history_trend"]["direction"] == "up"
    assert result["price_history_trend"]["latest_market_price"] == 32.0


def test_market_trend_lookup_uses_cross_channel_sales_when_tiktok_is_not_enough():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all(
            [
                ShopifyOrder(
                    shopify_order_id="shopify-current-trend",
                    order_number="S-current",
                    created_at=now - timedelta(days=1),
                    updated_at=now - timedelta(days=1),
                    subtotal_price=60.0,
                    total_price=66.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 2, "unit_price": 30.0}]
                    ),
                ),
                ShopifyOrder(
                    shopify_order_id="shopify-previous-trend",
                    order_number="S-previous",
                    created_at=now - timedelta(days=10),
                    updated_at=now - timedelta(days=10),
                    subtotal_price=40.0,
                    total_price=44.0,
                    financial_status="paid",
                    line_items_summary_json=json.dumps(
                        [{"title": "Pokemon 151 Booster Pack", "quantity": 2, "unit_price": 20.0}]
                    ),
                ),
                DiscordMessage(
                    discord_message_id="discord-current-trend",
                    channel_id="show-sales",
                    channel_name="show-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell Pokemon 151 booster pack 35",
                    created_at=now - timedelta(days=2),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=35.0,
                    money_out=0.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Pack"]),
                ),
                DiscordMessage(
                    discord_message_id="discord-previous-trend",
                    channel_id="show-sales",
                    channel_name="show-sales",
                    author_id="42",
                    author_name="Jeff",
                    content="sell Pokemon 151 booster pack 25",
                    created_at=now - timedelta(days=11),
                    parse_status="parsed",
                    entry_kind="sale",
                    money_in=25.0,
                    money_out=0.0,
                    category="Sealed",
                    item_names_json=json.dumps(["Pokemon 151 Booster Pack"]),
                ),
            ]
        )
        session.commit()

        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_market_trend_lookup(query="151 booster pack", days=7)

    assert result["summary"]["trend_direction"] == "up"
    assert result["summary"]["cross_channel_direction"] == "up"
    assert result["cross_channel_trend"]["current_window"]["quantity"] == 3
    assert result["cross_channel_trend"]["current_window"]["avg_price"] == 31.67
    assert result["cross_channel_trend"]["previous_window"]["avg_price"] == 21.67
    assert result["cross_channel_trend"]["channels"] == ["discord", "shopify"]
    assert "No recent TikTok comparison windows matched query='151 booster pack'." in result["data_gaps"]


def test_market_trend_lookup_reports_data_gap_without_comparison_points():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        harness = DegenOpsMcpHarness(session_factory=lambda: session)
        result = harness.get_market_trend_lookup(query="missing card", days=7)

    assert result["summary"]["trend_direction"] == "unknown"
    assert "No recent TikTok comparison windows matched query='missing card'." in result["data_gaps"]
    assert "No stored price-history trend matched query='missing card'." in result["data_gaps"]


def test_web_search_returns_cited_results_from_search_page():
    html = """
    <html><body>
      <a class="result__a" href="https://www.pricecharting.com/game/pokemon-scarlet-&-violet-151/booster-pack">
        Pokemon 151 Booster Pack Prices
      </a>
      <a class="result__snippet">Recent sold prices and market chart.</a>
    </body></html>
    """
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text=html, request=request)

    harness = DegenOpsMcpHarness(
        session_factory=lambda: FakeSession(),
        web_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = harness.get_web_search(query="pokemon 151 booster pack price", limit=3)

    assert result["summary"] == {
        "query": "pokemon 151 booster pack price",
        "result_count": 1,
        "provider": "duckduckgo_html",
    }
    assert result["results"][0]["title"] == "Pokemon 151 Booster Pack Prices"
    assert result["results"][0]["url"].startswith("https://www.pricecharting.com/")
    assert result["results"][0]["snippet"] == "Recent sold prices and market chart."
    assert result["evidence"] == [
        {
            "source": "web_search",
            "provider": "duckduckgo_html",
            "url": result["search_url"],
            "detail": "Search results only; no page content was fetched.",
        }
    ]
    assert result["read_only"] is True
    assert requests[0].url.params["q"] == "pokemon 151 booster pack price"


def test_mcp_harness_evaluate_inventory_buy_uses_context_and_returns_evidence(monkeypatch):
    context = {
        "finance_statement": {
            "revenue": 18000.0,
            "operating_profit": 2400.0,
            "operating_expenses": 1200.0,
            "inventory_spend": 6200.0,
            "avg_daily_profit": 240.0,
            "revenue_display": "$18,000",
            "operating_profit_display": "$2,400",
            "operating_expenses_display": "$1,200",
            "inventory_spend_display": "$6,200",
        },
        "cash_snapshot": {"latest_known_cash": 12000.0, "accounts": []},
        "inventory_snapshot": {"active_items": 10, "estimated_list_value": 5000.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "avg_price": 75.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics/api/products?days=90",
            }
        ],
        "loan_snapshot": {"observed_loan_proceeds": 0.0, "observed_paybacks": 1000.0},
        "range": {"days": 90},
    }
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(harness, "get_context", lambda days=90: context)

    result = harness.evaluate_inventory_buy(
        {
            "lot_name": "Pokemon sealed",
            "purchase_cost": 2000.0,
            "expected_revenue": 3600.0,
            "unit_count": 40,
            "cash_on_hand": 12000.0,
            "minimum_cash_reserve": 6000.0,
            "target_payback_weeks": 4,
            "categories": ["Pokemon sealed"],
        }
    )

    assert result["verdict"] == "safe"
    assert result["evidence"]
    assert result["read_only_guardrails"][0] == "No money movement"


def test_mcp_harness_partner_evaluate_redacts_owner_cash_and_loan_details(monkeypatch):
    context = {
        "finance_statement": {
            "revenue": 18000.0,
            "operating_profit": 2400.0,
            "operating_expenses": 1200.0,
            "inventory_spend": 6200.0,
            "avg_daily_profit": 240.0,
            "revenue_display": "$18,000",
            "operating_profit_display": "$2,400",
            "operating_expenses_display": "$1,200",
            "inventory_spend_display": "$6,200",
        },
        "cash_snapshot": {"latest_known_cash": 7000.0, "accounts": []},
        "inventory_snapshot": {"active_items": 10, "estimated_list_value": 5000.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "avg_price": 75.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics/api/products?days=90",
            }
        ],
        "loan_snapshot": {"observed_loan_proceeds": 5000.0, "observed_paybacks": 1000.0},
        "range": {"days": 90},
    }
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(harness, "get_context", lambda days=90: context)

    result = harness.evaluate_inventory_buy(
        {
            "lot_name": "Pokemon sealed",
            "purchase_cost": 2000.0,
            "expected_revenue": 3600.0,
            "unit_count": 40,
            "minimum_cash_reserve": 6000.0,
            "target_payback_weeks": 4,
            "categories": ["Pokemon sealed"],
        },
        audience_scope="partner",
    )

    assert result["read_only"] is True
    assert result["cash_flow"]["cash_safety"] == "below_minimum_reserve"
    assert "cash_on_hand" not in result["cash_flow"]
    assert "post_buy_cash" not in result["cash_flow"]
    assert "reserve_gap" not in result["cash_flow"]
    assert "Cash after buy" not in result["partner_update"]
    assert "owner-scope only" in result["partner_update"]
    assert "Observed loan proceeds" not in str(result["evidence"])
    assert "owner loan/payback totals are owner-scope only" in str(result["evidence"])
    assert result["owner_scope_required_for"] == ["get_cash_snapshot", "get_loan_and_payback_snapshot"]


def test_mcp_harness_generate_partner_update_uses_evaluate_result(monkeypatch):
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(
        harness,
        "evaluate_inventory_buy",
        lambda scenario, days=90, audience_scope="owner": {
            "partner_update": "Weekly business update\nBuy decision: SAFE.",
            "evidence": [{"source": "finance_statement"}],
        },
    )

    result = harness.generate_partner_update({"lot_name": "Test"})

    assert result["partner_update"].startswith("Weekly business update")
    assert result["evidence"][0]["source"] == "finance_statement"
