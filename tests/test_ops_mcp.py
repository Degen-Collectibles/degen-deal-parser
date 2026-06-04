from pathlib import Path
import subprocess
import sys

import pytest

from app.ops_mcp import (
    DegenOpsMcpHarness,
    DEGEN_OPS_MCP_TOOL_NAMES,
    DEGEN_OPS_SCOPE_TOOL_NAMES,
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
