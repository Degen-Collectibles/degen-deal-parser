from pathlib import Path
import json
import subprocess
import sys

import pytest

from scripts.degen_ops_pilot_demo import _render_markdown, build_pilot_demo_report, stdout_safe


class DemoRunner:
    def __init__(self, scope="partner"):
        self.scope = scope
        self.allowed_tools = {
            "get_ops_agent_manifest",
            "get_finance_snapshot",
            "get_inventory_snapshot",
            "get_channel_velocity",
            "evaluate_inventory_buy",
            "generate_partner_update",
        }
        if scope == "owner":
            self.allowed_tools.update({"get_cash_snapshot", "get_loan_and_payback_snapshot"})
        self.calls = []

    def call_tool(self, name, args=None):
        self.calls.append((name, args or {}))
        if name == "get_ops_agent_manifest":
            return {"scope": self.scope, "tools": sorted(self.allowed_tools), "read_only": True}
        if name == "evaluate_inventory_buy":
            result = {
                "verdict": "risky",
                "risk_flags": ["No matching sell-through evidence found"],
                "evidence": [{"source": "finance_statement", "url": "/finance"}],
                "routing": [{"channel": "Manual review", "recommended_units": 40}],
                "cash_flow": {
                    "cash_safety": "below_minimum_reserve",
                    "cash_safety_summary": "The modeled buy falls below the configured reserve.",
                },
                "sell_through": {"estimated_weeks": None, "confidence": "low"},
                "payback_plan": {"weekly_payback": 500.0, "target_weeks": 4},
                "input_warnings": ["Scenario input was ignored in favor of authoritative server data."],
                "read_only_guardrails": ["No money movement"],
            }
            if self.scope == "owner":
                result["reserve_floor"] = {
                    "configured": True,
                    "source": "DEGEN_OPS_MIN_CASH_RESERVE_USD",
                    "amount": 6000.0,
                }
            return result
        if name == "generate_partner_update":
            return {
                "partner_update": "Weekly business update\nBuy decision: RISKY.",
                "evidence": [{"source": "finance_statement"}],
                "read_only": True,
            }
        return {"read_only": True, "evidence": [{"source": name}]}


class EmployeeRunner(DemoRunner):
    def __init__(self):
        super().__init__(scope="employee")
        self.allowed_tools = {
            "get_ops_agent_manifest",
            "get_inventory_snapshot",
            "get_channel_velocity",
        }


def test_partner_pilot_demo_runs_core_buy_workflow_read_only():
    runner = DemoRunner(scope="partner")

    report = build_pilot_demo_report(scope="partner", runner=runner)

    assert report["ok"] is True
    assert report["scope"] == "partner"
    assert report["read_only"] is True
    assert report["verdict"] == "risky"
    assert report["partner_update"].startswith("Weekly business update")
    assert report["evidence_count"] == 1
    assert report["evidence"] == [{"source": "finance_statement", "url": "/finance"}]
    assert report["routing"][0]["channel"] == "Manual review"
    assert report["cash_flow"]["cash_safety"] == "below_minimum_reserve"
    assert report["sell_through"]["confidence"] == "low"
    assert report["payback_plan"]["weekly_payback"] == 500.0
    assert report["input_warnings"] == ["Scenario input was ignored in favor of authoritative server data."]
    assert "reserve_floor" not in report
    assert [call[0] for call in runner.calls] == [
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "evaluate_inventory_buy",
        "generate_partner_update",
    ]


def test_owner_pilot_demo_surfaces_reserve_metadata_and_filters_caller_floor():
    runner = DemoRunner(scope="owner")

    report = build_pilot_demo_report(
        scope="owner",
        runner=runner,
        scenario={
            "lot_name": "Caller scenario",
            "purchase_cost": 1000.0,
            "expected_revenue": 1800.0,
            "unit_count": 10,
            "minimum_cash_reserve": 100.0,
        },
    )
    evaluation_call = next(call for call in runner.calls if call[0] == "evaluate_inventory_buy")

    assert "minimum_cash_reserve" not in report["scenario"]
    assert "minimum_cash_reserve" not in evaluation_call[1]["scenario"]
    assert report["reserve_floor"] == {
        "configured": True,
        "source": "DEGEN_OPS_MIN_CASH_RESERVE_USD",
        "amount": 6000.0,
    }
    assert report["input_warnings"] == ["Scenario input was ignored in favor of authoritative server data."]


@pytest.mark.parametrize("scope", ["owner", "partner"])
def test_pilot_demo_strips_caller_cash_from_forwarded_and_echoed_json(scope):
    runner = DemoRunner(scope=scope)

    report = build_pilot_demo_report(
        scope=scope,
        runner=runner,
        scenario={
            "lot_name": "Caller scenario",
            "purchase_cost": 1000.0,
            "expected_revenue": 1800.0,
            "unit_count": 10,
            "cash_on_hand": 987654.32,
        },
    )

    assert "cash_on_hand" not in report["scenario"]
    for tool_name in ("evaluate_inventory_buy", "generate_partner_update"):
        tool_call = next(call for call in runner.calls if call[0] == tool_name)
        assert "cash_on_hand" not in tool_call[1]["scenario"]
    assert "987654.32" not in json.dumps(report, sort_keys=True)


def test_pilot_demo_caps_history_days_before_forwarding_calls():
    runner = DemoRunner(scope="owner")

    build_pilot_demo_report(
        scope="owner",
        runner=runner,
        days=10**10000,
    )

    calls_with_days = [args for _, args in runner.calls if "days" in args]
    assert calls_with_days
    assert all(args["days"] == 365 for args in calls_with_days)


def test_owner_pilot_markdown_surfaces_reserve_metadata_and_input_warnings():
    report = {
        "ok": True,
        "scope": "owner",
        "read_only": True,
        "verdict": "risky",
        "evidence": [],
        "evidence_count": 0,
        "routing": [],
        "cash_flow": {},
        "payback_plan": {},
        "partner_update": "Weekly business update",
        "reserve_floor": {
            "configured": True,
            "source": "DEGEN_OPS_MIN_CASH_RESERVE_USD",
            "amount": 6000.0,
        },
        "input_warnings": ["Caller input was ignored in favor of authoritative server data."],
    }

    rendered = _render_markdown(report)

    assert "reserve_floor_configured: true" in rendered
    assert "reserve_floor_amount: 6000.0" in rendered
    assert "## Input Warnings" in rendered
    assert "Caller input was ignored" in rendered


def test_employee_pilot_demo_refuses_buy_workflow_scope():
    runner = EmployeeRunner()

    report = build_pilot_demo_report(scope="employee", runner=runner)

    assert report["ok"] is False
    assert report["scope"] == "employee"
    assert report["reason"] == "employee scope does not expose buy-decision finance tools"
    assert runner.calls == [("get_ops_agent_manifest", {})]


def test_pilot_demo_script_missing_database_env_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_pilot_demo.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--scope",
            "partner",
            "--database-url-env",
            "MISSING_DEGEN_DB_URL",
            "--json",
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert result.stdout.startswith("{")
    assert "[logging]" not in result.stdout
    assert '"ok": false' in result.stdout
    assert "MISSING_DEGEN_DB_URL" in result.stdout


def test_stdout_safe_replaces_unencodable_channel_names():
    text = "channel: \u2551store-sales-and-trades"

    safe = stdout_safe(text, encoding="cp1252")

    assert "\u2551" not in safe
    assert "store-sales-and-trades" in safe
