from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_live_data import build_live_data_report


class RecordingRunner:
    def __init__(self, *, scope="employee", failing_tool=""):
        self.scope = scope
        self.allowed_tools = {
            "get_ops_agent_manifest",
            "get_inventory_snapshot",
            "get_channel_velocity",
        }
        if scope == "owner":
            self.allowed_tools.update(
                {
                    "get_finance_snapshot",
                    "get_cash_snapshot",
                    "get_loan_and_payback_snapshot",
                    "evaluate_inventory_buy",
                    "generate_partner_update",
                }
            )
        if scope == "partner":
            self.allowed_tools.update(
                {
                    "get_finance_snapshot",
                    "evaluate_inventory_buy",
                    "generate_partner_update",
                }
            )
        self.failing_tool = failing_tool
        self.calls = []

    def call_tool(self, name, args=None):
        self.calls.append((name, args or {}))
        if name == self.failing_tool:
            raise RuntimeError("postgresql+psycopg://user:secret@db.example.com/degen failed")
        if name == "get_ops_agent_manifest":
            return {"scope": self.scope, "tools": sorted(self.allowed_tools), "read_only": True}
        return {"read_only": True, "tool": name}


def test_employee_live_data_report_only_checks_employee_safe_tools():
    runner = RecordingRunner(scope="employee")

    report = build_live_data_report(
        scope="employee",
        runner=runner,
        database_url_source="DEGEN_OPS_READONLY_DATABASE_URL",
    )

    assert report["ok"] is True
    assert report["scope"] == "employee"
    assert report["database_url_source"] == "DEGEN_OPS_READONLY_DATABASE_URL"
    assert [call[0] for call in runner.calls] == [
        "get_ops_agent_manifest",
        "get_inventory_snapshot",
        "get_channel_velocity",
    ]
    assert "get_cash_snapshot" not in [check["tool"] for check in report["checks"]]
    assert all(check["status"] == "pass" for check in report["checks"])


def test_partner_live_data_report_checks_buy_workflow_without_raw_cash_or_loan():
    runner = RecordingRunner(scope="partner", failing_tool="evaluate_inventory_buy")

    report = build_live_data_report(
        scope="partner",
        runner=runner,
        database_url_source="DEGEN_OPS_READONLY_DATABASE_URL",
    )

    checked_tools = [check["tool"] for check in report["checks"]]
    assert report["ok"] is False
    assert "get_finance_snapshot" in checked_tools
    assert "evaluate_inventory_buy" in checked_tools
    assert "generate_partner_update" in checked_tools
    assert "get_cash_snapshot" not in checked_tools
    assert "get_loan_and_payback_snapshot" not in checked_tools
    failed = [check for check in report["checks"] if check["status"] == "fail"][0]
    assert failed["tool"] == "evaluate_inventory_buy"
    assert "secret" not in failed["error"]
    assert "postgresql+psycopg://***:***@db.example.com/degen" in failed["error"]


def test_live_data_script_missing_database_env_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_live_data.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--scope",
            "employee",
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
    assert '"database_url_configured": false' in result.stdout
    assert "MISSING_DEGEN_DB_URL" in result.stdout
