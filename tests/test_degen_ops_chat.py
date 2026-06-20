from scripts.degen_ops_chat import build_preflight_report, configure_environment, parse_args

from app.ops_chat import (
    DEGEN_OPS_CHAT_SYSTEM_PROMPT,
    DegenOpsChatToolRunner,
    initial_chat_messages,
    run_chat_turn,
    tool_schemas_for_scope,
)
from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES, TIKTOK_MCP_TOOL_NAMES


class FakeHarness:
    def get_manifest(self, *, scope, tools):
        return {"scope": scope, "tools": tools, "read_only": True}

    def get_ops_memory(self, query="", limit=20, audience_scope="employee"):
        return {
            "summary": {"query": query, "audience_scope": audience_scope},
            "memories": [{"key": "default_channel_question", "value": "Ask which channel."}],
            "read_only": True,
        }

    def propose_ops_memory(self, key="", value="", scope="owner", tags=None, proposed_by=""):
        return {
            "proposal": {"key": key, "value": value, "scope": scope, "tags": tags or [], "proposed_by": proposed_by},
            "requires_owner_approval": True,
            "write_performed": False,
            "read_only": True,
        }

    def get_inventory_snapshot(self, audience_scope="employee"):
        return {
            "inventory_snapshot": {
                "active_items": 12,
                "estimated_list_value": 3456.0,
                "audience_scope": audience_scope,
            },
            "evidence": [{"source": "inventory_items", "url": "/inventory"}],
            "read_only": True,
        }

    def get_finance_snapshot(self, days=90):
        return {"finance_statement": {"revenue": 1000.0}, "read_only": True}

    def get_cash_snapshot(self):
        return {"cash_snapshot": {"latest_known_cash": 1000.0}, "read_only": True}

    def get_channel_velocity(self, days=90, category=""):
        return {"channel_velocity": [], "read_only": True}

    def get_sales_summary(self, days=7):
        return {"summary": {"total_revenue": 220.0}, "range": {"days": days}, "read_only": True}

    def get_discord_sales_summary(self, product_query="", days=7, limit=25):
        return {
            "summary": {"matched_sales": 2, "matched_revenue": 100.0},
            "filters": {"product_query": product_query, "days": days, "limit": limit},
            "read_only": True,
        }

    def get_loan_and_payback_snapshot(self, days=90):
        return {"loan_snapshot": {}, "read_only": True}

    def get_employee_clock_status(self, person_query="", days=1, limit=20):
        return {
            "summary": {"person_query": person_query, "matched_employee_count": 1},
            "employees": [{"display_name": "Alex", "clock_status": "clocked_in"}],
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_employee_ops_status(self, person_query="", days=30, limit=50):
        return {
            "summary": {"person_query": person_query, "supply_requests": {"submitted": 1}},
            "items": [{"kind": "supply_request", "title": "Top loaders"}],
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def evaluate_inventory_buy(self, scenario, days=90, audience_scope="owner"):
        return {"verdict": "safe", "audience_scope": audience_scope, "read_only": True}

    def generate_partner_update(self, scenario, days=90, audience_scope="owner"):
        return {"partner_update": "Weekly business update", "audience_scope": audience_scope, "read_only": True}

    def generate_weekly_partner_update_draft(self, days=7, audience_scope="partner"):
        return {
            "draft": f"Weekly Degen Ops Update ({days}-day draft)",
            "audience_scope": audience_scope,
            "approval_required": True,
            "write_performed": False,
            "read_only": True,
        }

    def get_tiktok_agent_manifest(self):
        return {"name": "degen-tiktok-readonly", "tools": ["get_tiktok_orders"], "read_only": True}

    def get_tiktok_status(self):
        return {"status": {"status_label": "Ready"}, "read_only": True}

    def get_tiktok_orders(self, days=7, limit=50, status="", search=""):
        return {"orders": [], "range": {"days": days, "limit": limit}, "read_only": True}

    def get_tiktok_products(self, limit=50, status="", search=""):
        return {"products": [], "filters": {"limit": limit}, "read_only": True}

    def get_tiktok_product_sales(self, product_query="", days=7, limit=25):
        return {
            "summary": {"product_query": product_query, "matched_quantity": 3},
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_tiktok_top_products(self, days=7, limit=10, sort_by="quantity"):
        return {
            "summary": {"channel": "tiktok", "sort_by": sort_by},
            "products": [{"title": "Pokemon 151 Booster Pack", "quantity": 8}],
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_shopify_product_sales(self, product_query="", days=7, limit=25):
        return {
            "summary": {"product_query": product_query, "matched_quantity": 5},
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_shopify_top_products(self, days=7, limit=10, sort_by="quantity"):
        return {
            "summary": {"channel": "shopify", "sort_by": sort_by},
            "products": [{"title": "Premium Slab", "revenue": 240.0}],
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_price_lookup(self, query="", days=30, limit=10, audience_scope="employee"):
        return {
            "summary": {"query": query, "recommended_price": 29.99, "audience_scope": audience_scope},
            "range": {"days": days, "limit": limit},
            "read_only": True,
        }

    def get_market_trend_lookup(self, query="", days=7, limit=10):
        return {
            "summary": {"query": query, "trend_direction": "up"},
            "range": {"days": days},
            "read_only": True,
        }

    def get_web_search(self, query="", limit=5, freshness=""):
        return {
            "summary": {"query": query, "result_count": 1},
            "results": [{"title": "Pokemon 151 price guide", "url": "https://example.com/151"}],
            "filters": {"limit": limit, "freshness": freshness},
            "read_only": True,
        }

    def get_tiktok_buyer_insights(self, days=90, limit=50):
        return {"buyers": [], "range": {"days": days, "limit": limit}, "read_only": True}

    def get_tiktok_product_performance(self, days=30, limit=50):
        return {"products": [], "range": {"days": days, "limit": limit}, "read_only": True}

    def get_tiktok_live_snapshot(self):
        return {"live_session": {}, "live_analytics": {}, "read_only": True}


class FailingHarness(FakeHarness):
    def get_inventory_snapshot(self, audience_scope="employee"):
        raise RuntimeError("postgresql+psycopg://user:secret@db.example.com/degen read failed")


class FakeCompletions:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class FakeClient:
    def __init__(self, responses):
        self.chat = type("Chat", (), {})()
        self.chat.completions = FakeCompletions(responses)


def _tool_call_response(name: str, arguments: str = "{}"):
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": name, "arguments": arguments},
                        }
                    ],
                }
            }
        ]
    }


def _final_response(text: str):
    return {"choices": [{"message": {"content": text}}]}


def test_chat_tool_schemas_follow_employee_scope():
    schemas = tool_schemas_for_scope("employee")
    names = {schema["function"]["name"] for schema in schemas}

    assert names == {
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
    }
    assert "get_cash_snapshot" not in names
    assert "evaluate_inventory_buy" not in names


def test_chat_system_prompt_names_partner_cash_redaction():
    assert "Partner scope can evaluate buys" in DEGEN_OPS_CHAT_SYSTEM_PROMPT
    assert "does not expose raw cash balances" in DEGEN_OPS_CHAT_SYSTEM_PROMPT
    assert "redacted cash-safety status" in DEGEN_OPS_CHAT_SYSTEM_PROMPT


def test_chat_tool_schemas_follow_partner_scope_without_owner_cash_tools():
    schemas = tool_schemas_for_scope("partner")
    names = {schema["function"]["name"] for schema in schemas}

    assert names == {
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
    }
    assert "get_cash_snapshot" not in names
    assert "get_loan_and_payback_snapshot" not in names


def test_chat_tool_schemas_follow_manager_scope_without_owner_cash_tools():
    schemas = tool_schemas_for_scope("manager")
    names = {schema["function"]["name"] for schema in schemas}

    assert set(DEGEN_OPS_SCOPE_TOOL_NAMES["employee"]).issubset(names)
    assert set(TIKTOK_MCP_TOOL_NAMES).issubset(names)
    assert "get_employee_clock_status" in names
    assert "get_employee_ops_status" in names
    assert "get_cash_snapshot" not in names
    assert "get_loan_and_payback_snapshot" not in names
    assert "evaluate_inventory_buy" not in names


def test_chat_tool_schemas_follow_tiktok_scope_without_business_tools():
    schemas = tool_schemas_for_scope("tiktok")
    names = {schema["function"]["name"] for schema in schemas}

    assert names == {
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_tiktok_agent_manifest",
        "get_tiktok_status",
        "get_tiktok_orders",
        "get_tiktok_products",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_price_lookup",
        "get_market_trend_lookup",
        "get_web_search",
        "get_tiktok_buyer_insights",
        "get_tiktok_product_performance",
        "get_tiktok_live_snapshot",
    }
    assert "get_cash_snapshot" not in names
    assert "evaluate_inventory_buy" not in names


def test_chat_role_scopes_are_hierarchical_for_employee_manager_owner():
    employee = {schema["function"]["name"] for schema in tool_schemas_for_scope("employee")}
    manager = {schema["function"]["name"] for schema in tool_schemas_for_scope("manager")}
    owner = {schema["function"]["name"] for schema in tool_schemas_for_scope("owner")}

    assert employee.issubset(manager)
    assert manager.issubset(owner)
    assert {"get_price_lookup", "get_market_trend_lookup"}.issubset(employee)
    assert set(TIKTOK_MCP_TOOL_NAMES).issubset(manager)


def test_chat_runner_refuses_out_of_scope_tool():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_cash_snapshot", {})

    assert result["read_only"] is True
    assert "not available" in result["error"]

    result = runner.call_tool("get_employee_clock_status", {"person_query": "Alex"})
    assert result["read_only"] is True
    assert "not available" in result["error"]

    result = runner.call_tool("get_employee_ops_status", {"person_query": "Alex"})
    assert result["read_only"] is True
    assert "not available" in result["error"]

    result = runner.call_tool("propose_ops_memory", {"key": "x", "value": "y"})
    assert result["read_only"] is True
    assert "not available" in result["error"]

    result = runner.call_tool("generate_weekly_partner_update_draft", {"days": 7})
    assert result["read_only"] is True
    assert "not available" in result["error"]


def test_chat_runner_passes_scope_to_buy_evaluation():
    runner = DegenOpsChatToolRunner(scope="partner", harness=FakeHarness())

    result = runner.call_tool("evaluate_inventory_buy", {"scenario": {"lot_name": "Test"}})

    assert result["audience_scope"] == "partner"


def test_chat_runner_dispatches_weekly_partner_update_draft():
    runner = DegenOpsChatToolRunner(scope="partner", harness=FakeHarness())

    result = runner.call_tool("generate_weekly_partner_update_draft", {"days": 14})

    assert result["draft"] == "Weekly Degen Ops Update (14-day draft)"
    assert result["audience_scope"] == "partner"
    assert result["approval_required"] is True
    assert result["write_performed"] is False


def test_chat_runner_dispatches_owner_employee_clock_status():
    runner = DegenOpsChatToolRunner(scope="owner", harness=FakeHarness())

    result = runner.call_tool("get_employee_clock_status", {"person_query": "Alex", "days": 1, "limit": 5})

    assert result["summary"] == {"person_query": "Alex", "matched_employee_count": 1}
    assert result["employees"][0]["clock_status"] == "clocked_in"
    assert result["range"] == {"days": 1, "limit": 5}


def test_chat_runner_dispatches_scoped_ops_memory_lookup():
    runner = DegenOpsChatToolRunner(scope="partner", harness=FakeHarness())

    result = runner.call_tool("get_ops_memory", {"query": "channel", "limit": 3})

    assert result["summary"] == {"query": "channel", "audience_scope": "partner"}
    assert result["memories"][0]["key"] == "default_channel_question"
    assert result["read_only"] is True


def test_chat_runner_dispatches_discord_sales_summary():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_discord_sales_summary", {"product_query": "151 packs", "days": 7, "limit": 5})

    assert result["summary"] == {"matched_sales": 2, "matched_revenue": 100.0}
    assert result["filters"] == {"product_query": "151 packs", "days": 7, "limit": 5}
    assert result["read_only"] is True


def test_chat_runner_dispatches_owner_memory_proposal_without_write():
    runner = DegenOpsChatToolRunner(scope="owner", harness=FakeHarness())

    result = runner.call_tool(
        "propose_ops_memory",
        {"key": "weekly_update_day", "value": "Monday morning", "scope": "partner", "tags": ["partner"]},
    )

    assert result["proposal"]["key"] == "weekly_update_day"
    assert result["proposal"]["scope"] == "partner"
    assert result["write_performed"] is False


def test_chat_runner_dispatches_owner_employee_ops_status():
    runner = DegenOpsChatToolRunner(scope="owner", harness=FakeHarness())

    result = runner.call_tool("get_employee_ops_status", {"person_query": "Alex", "days": 7, "limit": 5})

    assert result["summary"] == {"person_query": "Alex", "supply_requests": {"submitted": 1}}
    assert result["items"][0]["kind"] == "supply_request"
    assert result["range"] == {"days": 7, "limit": 5}


def test_chat_runner_dispatches_sales_summary():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_sales_summary", {"days": 7})

    assert result["summary"]["total_revenue"] == 220.0
    assert result["range"] == {"days": 7}
    assert result["read_only"] is True


def test_chat_runner_dispatches_tiktok_orders_with_filters():
    runner = DegenOpsChatToolRunner(scope="tiktok", harness=FakeHarness())

    result = runner.call_tool("get_tiktok_orders", {"days": 14, "limit": 5, "status": "PAID", "search": "charizard"})

    assert result["range"] == {"days": 14, "limit": 5}
    assert result["read_only"] is True


def test_chat_runner_dispatches_tiktok_product_sales():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_tiktok_product_sales", {"product_query": "151 packs", "days": 7, "limit": 10})

    assert result["summary"]["product_query"] == "151 packs"
    assert result["range"] == {"days": 7, "limit": 10}
    assert result["read_only"] is True


def test_chat_runner_dispatches_tiktok_top_products():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_tiktok_top_products", {"days": 7, "limit": 5, "sort_by": "quantity"})

    assert result["summary"] == {"channel": "tiktok", "sort_by": "quantity"}
    assert result["products"][0]["title"] == "Pokemon 151 Booster Pack"
    assert result["range"] == {"days": 7, "limit": 5}


def test_chat_runner_dispatches_shopify_product_sales():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_shopify_product_sales", {"product_query": "151 pack", "days": 7, "limit": 5})

    assert result["summary"]["product_query"] == "151 pack"
    assert result["summary"]["matched_quantity"] == 5
    assert result["range"] == {"days": 7, "limit": 5}


def test_chat_runner_dispatches_shopify_top_products():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_shopify_top_products", {"days": 7, "limit": 5, "sort_by": "revenue"})

    assert result["summary"] == {"channel": "shopify", "sort_by": "revenue"}
    assert result["products"][0]["title"] == "Premium Slab"
    assert result["range"] == {"days": 7, "limit": 5}


def test_chat_runner_dispatches_price_lookup():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_price_lookup", {"query": "151 packs", "days": 30, "limit": 5})

    assert result["summary"]["query"] == "151 packs"
    assert result["summary"]["recommended_price"] == 29.99
    assert result["range"] == {"days": 30, "limit": 5}


def test_chat_runner_dispatches_market_trend_lookup():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_market_trend_lookup", {"query": "151 packs", "days": 7, "limit": 5})

    assert result["summary"]["query"] == "151 packs"
    assert result["summary"]["trend_direction"] == "up"
    assert result["range"] == {"days": 7}


def test_chat_runner_dispatches_web_search():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_web_search", {"query": "pokemon 151 booster pack market price", "limit": 3})

    assert result["summary"]["query"] == "pokemon 151 booster pack market price"
    assert result["filters"] == {"limit": 3, "freshness": ""}
    assert result["read_only"] is True


def test_run_chat_turn_executes_read_only_tool_and_returns_final_answer():
    client = FakeClient(
        [
            _tool_call_response("get_inventory_snapshot"),
            _final_response("Inventory checked from /inventory. No changes made."),
        ]
    )
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())
    messages = initial_chat_messages()
    messages.append({"role": "user", "content": "What does inventory look like?"})

    answer, history = run_chat_turn(
        client=client,
        model="fake-model",
        messages=messages,
        runner=runner,
    )

    assert answer == "Inventory checked from /inventory. No changes made."
    assert client.chat.completions.calls[0]["tools"]
    tool_messages = [message for message in history if message["role"] == "tool"]
    assert len(tool_messages) == 1
    assert "inventory_snapshot" in tool_messages[0]["content"]
    assert "read_only" in tool_messages[0]["content"]


def test_run_chat_turn_returns_tool_error_for_invalid_json_arguments():
    client = FakeClient(
        [
            _tool_call_response("get_inventory_snapshot", "{bad json"),
            _final_response("The tool arguments were invalid, so I cannot use that result."),
        ]
    )
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    _, history = run_chat_turn(
        client=client,
        model="fake-model",
        messages=[{"role": "user", "content": "Check inventory"}],
        runner=runner,
    )

    tool_messages = [message for message in history if message["role"] == "tool"]
    assert "Invalid JSON tool arguments" in tool_messages[0]["content"]


def test_chat_script_missing_database_url_env_fails(monkeypatch):
    args = type("Args", (), {"scope": "employee", "database_url": "", "database_url_env": "MISSING_DEGEN_DB_URL"})()
    monkeypatch.delenv("MISSING_DEGEN_DB_URL", raising=False)

    assert configure_environment(args) == 1


def test_preflight_report_lists_scope_tools_without_model_call():
    report = build_preflight_report(
        scope="employee",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="employee", harness=FakeHarness()),
        read_check=False,
    )

    assert report["ok"] is True
    assert report["scope"] == "employee"
    assert report["provider"] == "nvidia"
    assert report["model"] == "aws/anthropic/claude-haiku-4-5-v1"
    assert report["api_key_configured"] is True
    assert report["read_check"] == "skipped"
    assert report["tools"] == [
        "get_channel_velocity",
        "get_discord_sales_summary",
        "get_inventory_snapshot",
        "get_market_trend_lookup",
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_price_lookup",
        "get_sales_summary",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_web_search",
    ]


def test_preflight_report_supports_tiktok_scope_without_business_tools():
    report = build_preflight_report(
        scope="tiktok",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="tiktok", harness=FakeHarness()),
        read_check=True,
    )

    assert report["ok"] is True
    assert report["scope"] == "tiktok"
    assert "get_tiktok_orders" in report["tools"]
    assert "get_cash_snapshot" not in report["tools"]


def test_preflight_report_read_check_exercises_partner_workflow_without_owner_cash_tools():
    report = build_preflight_report(
        scope="partner",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="partner", harness=FakeHarness()),
        read_check=True,
    )

    checked_tools = [check["tool"] for check in report["read_checks"]]
    assert report["ok"] is True
    assert report["read_check"] == "passed"
    assert checked_tools == [
        "evaluate_inventory_buy",
        "generate_partner_update",
        "generate_weekly_partner_update_draft",
        "get_channel_velocity",
        "get_discord_sales_summary",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_market_trend_lookup",
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_price_lookup",
        "get_sales_summary",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_web_search",
    ]
    assert "get_cash_snapshot" not in checked_tools
    assert "get_loan_and_payback_snapshot" not in checked_tools


def test_chat_script_defaults_to_employee_scope(monkeypatch):
    monkeypatch.setattr("sys.argv", ["degen_ops_chat.py"])

    args = parse_args()

    assert args.scope == "employee"


def test_preflight_report_read_check_redacts_database_errors():
    report = build_preflight_report(
        scope="employee",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="employee", harness=FailingHarness()),
        read_check=True,
    )

    assert report["ok"] is False
    assert report["read_check"] == "failed"
    assert report["read_checks"]
    assert "secret" not in report["read_check_error"]
    assert "postgresql+psycopg://***:***@db.example.com/degen" in report["read_check_error"]
