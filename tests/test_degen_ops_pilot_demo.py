from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_pilot_demo import build_pilot_demo_report, stdout_safe


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
            return {
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
                "read_only_guardrails": ["No money movement"],
            }
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
    assert [call[0] for call in runner.calls] == [
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "evaluate_inventory_buy",
        "generate_partner_update",
    ]


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
