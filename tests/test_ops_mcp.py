from app.ops_mcp import DegenOpsMcpHarness, DEGEN_OPS_MCP_TOOL_NAMES, register_degen_ops_tools


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


def test_register_degen_ops_tools_exposes_bounded_read_only_tool_names():
    fake = FakeMcp()
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())

    register_degen_ops_tools(fake, harness=harness)

    assert sorted(fake.tools) == sorted(DEGEN_OPS_MCP_TOOL_NAMES)
    assert "run_sql" not in fake.tools
    assert "execute_sql" not in fake.tools
    assert all("read-only" in (tool["description"] or "").lower() for tool in fake.tools.values())


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


def test_mcp_harness_generate_partner_update_uses_evaluate_result(monkeypatch):
    harness = DegenOpsMcpHarness(session_factory=lambda: FakeSession())
    monkeypatch.setattr(
        harness,
        "evaluate_inventory_buy",
        lambda scenario, days=90: {
            "partner_update": "Weekly business update\nBuy decision: SAFE.",
            "evidence": [{"source": "finance_statement"}],
        },
    )

    result = harness.generate_partner_update({"lot_name": "Test"})

    assert result["partner_update"].startswith("Weekly business update")
    assert result["evidence"][0]["source"] == "finance_statement"
