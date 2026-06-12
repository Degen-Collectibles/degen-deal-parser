from pathlib import Path

from scripts.degen_ops_mcp_smoke import (
    EXPECTED_TOOLS_BY_SCOPE,
    load_codex_servers,
    load_hermes_servers,
    main,
    run_smoke,
    sanitize,
)


def test_smoke_expected_employee_tools_exclude_finance_and_cash():
    tools = EXPECTED_TOOLS_BY_SCOPE["employee"]

    assert "get_ops_agent_manifest" in tools
    assert "get_inventory_snapshot" in tools
    assert "get_channel_velocity" in tools
    assert "get_sales_summary" in tools
    assert "get_discord_sales_summary" in tools
    assert "get_tiktok_product_sales" in tools
    assert "get_shopify_product_sales" in tools
    assert "get_price_lookup" in tools
    assert "get_market_trend_lookup" in tools
    assert "get_web_search" in tools
    assert "get_cash_snapshot" not in tools
    assert "get_loan_and_payback_snapshot" not in tools
    assert "evaluate_inventory_buy" not in tools


def test_smoke_expected_partner_tools_exclude_owner_cash_and_loan():
    tools = EXPECTED_TOOLS_BY_SCOPE["partner"]

    assert "get_finance_snapshot" in tools
    assert "evaluate_inventory_buy" in tools
    assert "generate_partner_update" in tools
    assert "generate_weekly_partner_update_draft" in tools
    assert "get_discord_sales_summary" in tools
    assert "get_price_lookup" in tools
    assert "get_market_trend_lookup" in tools
    assert "get_cash_snapshot" not in tools
    assert "get_loan_and_payback_snapshot" not in tools


def test_smoke_expected_tiktok_tools_exclude_business_tools():
    tools = EXPECTED_TOOLS_BY_SCOPE["tiktok"]

    assert "get_tiktok_orders" in tools
    assert "get_tiktok_products" in tools
    assert "get_tiktok_live_snapshot" in tools
    assert "get_tiktok_product_sales" in tools
    assert "get_tiktok_top_products" in tools
    assert "get_price_lookup" in tools
    assert "get_market_trend_lookup" in tools
    assert "get_cash_snapshot" not in tools
    assert "evaluate_inventory_buy" not in tools


def test_smoke_sanitize_redacts_database_credentials():
    text = "postgresql+psycopg://user:secret@db.example.com/app?sslmode=require password=hunter2"

    sanitized = sanitize(text)

    assert "secret" not in sanitized
    assert "hunter2" not in sanitized
    assert "postgresql+psycopg://***:***@db.example.com/app" in sanitized
    assert "password=***" in sanitized


def test_smoke_loads_only_degen_hermes_servers(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text(
        """
mcp_servers:
  degen_ops_employee:
    command: python
    args: ["scripts/degen_ops_mcp.py"]
    env:
      DEGEN_OPS_MCP_SCOPE: employee
  unrelated:
    command: node
""",
        encoding="utf-8",
    )

    servers = load_hermes_servers(config)

    assert sorted(servers) == ["degen_ops_employee"]
    assert servers["degen_ops_employee"]["env"]["DEGEN_OPS_MCP_SCOPE"] == "employee"


def test_smoke_loads_only_degen_codex_servers(tmp_path: Path):
    config = tmp_path / "config.toml"
    config.write_text(
        """
[mcp_servers.degen_ops_partner]
command = "python"
args = ["scripts/degen_ops_mcp.py"]

[mcp_servers.degen_ops_partner.env]
DEGEN_OPS_MCP_SCOPE = "partner"

[mcp_servers.render]
url = "https://example.com/mcp"
""",
        encoding="utf-8",
    )

    servers = load_codex_servers(config)

    assert sorted(servers) == ["degen_ops_partner"]
    assert servers["degen_ops_partner"]["env"]["DEGEN_OPS_MCP_SCOPE"] == "partner"


def test_smoke_scope_filter_skips_nonmatching_servers(monkeypatch):
    called = []

    async def fake_probe(config_name, server_name, server, *, read_check, database_url=None):
        called.append((config_name, server_name, server["env"]["DEGEN_OPS_MCP_SCOPE"], database_url))
        return True

    monkeypatch.setattr("scripts.degen_ops_mcp_smoke.probe_server", fake_probe)

    import asyncio

    ok = asyncio.run(
        run_smoke(
            {
                "hermes": {
                    "degen_ops_owner": {"env": {"DEGEN_OPS_MCP_SCOPE": "owner"}},
                    "degen_ops_employee": {"env": {"DEGEN_OPS_MCP_SCOPE": "employee"}},
                }
            },
            read_check=True,
            database_url="sqlite:///tmp/live.db",
            scope="employee",
        )
    )

    assert ok is True
    assert called == [("hermes", "degen_ops_employee", "employee", "sqlite:///tmp/live.db")]


def test_smoke_database_url_env_missing_fails(monkeypatch, capsys):
    monkeypatch.delenv("MISSING_DEGEN_DB_URL", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["degen_ops_mcp_smoke.py", "--config", "hermes", "--database-url-env", "MISSING_DEGEN_DB_URL"],
    )

    result = main()

    assert result == 1
    assert "MISSING_DEGEN_DB_URL" in capsys.readouterr().out
