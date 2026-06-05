from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_readiness import build_readiness_report


def test_readiness_report_confirms_scoped_read_only_code_and_flags_live_data_decision():
    report = build_readiness_report(repo_root=Path.cwd())

    assert report["code_ready"] is True
    assert report["team_rollout_ready"] is False
    assert "live_data_access_topology" in report["required_decisions"]
    assert "partner_scope_finance_visibility" not in report["required_decisions"]
    assert report["scopes"]["employee"]["tools"] == [
        "get_channel_velocity",
        "get_inventory_snapshot",
        "get_ops_agent_manifest",
    ]
    assert "get_cash_snapshot" not in report["scopes"]["employee"]["tools"]
    assert "evaluate_inventory_buy" not in report["scopes"]["employee"]["tools"]
    assert report["scopes"]["owner"]["tool_count"] == 15
    assert report["scopes"]["partner"]["tool_count"] == 6
    assert report["scopes"]["tiktok"]["tool_count"] == 8
    assert "get_tiktok_orders" in report["scopes"]["tiktok"]["tools"]
    assert "get_cash_snapshot" not in report["scopes"]["tiktok"]["tools"]
    assert "evaluate_inventory_buy" in report["scopes"]["partner"]["tools"]
    assert "get_cash_snapshot" not in report["scopes"]["partner"]["tools"]
    assert "get_loan_and_payback_snapshot" not in report["scopes"]["partner"]["tools"]
    assert all(check["status"] == "pass" for check in report["checks"])
    assert any(check["name"] == "missing_scope_defaults_to_employee" for check in report["checks"])
    assert any(check["name"] == "partner_access_package_is_redacted_and_secret_free" for check in report["checks"])


def test_readiness_report_requires_expected_docs_and_scripts():
    report = build_readiness_report(repo_root=Path.cwd())

    artifacts = {artifact["path"]: artifact["exists"] for artifact in report["artifacts"]}

    assert artifacts["app/ops_mcp.py"] is True
    assert artifacts["app/ops_chat.py"] is True
    assert artifacts["scripts/degen_ops_chat.py"] is True
    assert artifacts["scripts/degen_ops_change_manifest.py"] is True
    assert artifacts["scripts/degen_ops_completion_audit.py"] is True
    assert artifacts["scripts/degen_ops_mcp_smoke.py"] is True
    assert artifacts["scripts/degen_ops_live_data.py"] is True
    assert artifacts["scripts/degen_ops_local_gate.py"] is True
    assert artifacts["scripts/degen_ops_approval_packet.py"] is True
    assert artifacts["scripts/degen_ops_green_pilot_packet.py"] is True
    assert artifacts["scripts/degen_ops_launch_checklist.py"] is True
    assert artifacts["scripts/degen_ops_pilot_demo.py"] is True
    assert artifacts["scripts/degen_ops_mvp_audit.py"] is True
    assert artifacts["scripts/degen_ops_scope_audit.py"] is True
    assert artifacts["scripts/degen_ops_rollout_gate.py"] is True
    assert artifacts["scripts/degen_ops_topology_plan.py"] is True
    assert artifacts["scripts/degen_ops_team_package.py"] is True
    assert artifacts["docs/ops/degen-ops-agent-instructions.md"] is True
    assert artifacts["docs/ops/degen-ops-answer-examples.json"] is True
    assert artifacts["docs/ops/degen-ops-team-rollout-prd.md"] is True
    assert artifacts["docs/ops/degen-ops-readonly-db-role.sql"] is True


def test_readiness_script_outputs_clean_json_when_run_directly():
    script = Path.cwd() / "scripts" / "degen_ops_readiness.py"

    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert "[logging]" not in result.stdout
    assert '"code_ready": true' in result.stdout
    assert '"team_rollout_ready": false' in result.stdout
