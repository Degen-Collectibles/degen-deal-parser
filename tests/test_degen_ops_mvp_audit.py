from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_mvp_audit import build_mvp_audit


def test_mvp_audit_maps_original_success_criteria_to_evidence():
    audit = build_mvp_audit(repo_root=Path.cwd())

    criteria = {item["id"]: item for item in audit["criteria"]}

    assert criteria["cash_revenue_profit_expenses"]["status"] == "satisfied"
    assert "get_finance_snapshot" in criteria["cash_revenue_profit_expenses"]["evidence"]
    assert "get_cash_snapshot" in criteria["cash_revenue_profit_expenses"]["evidence"]

    assert criteria["sell_through_by_channel"]["status"] == "satisfied"
    assert "get_channel_velocity" in criteria["sell_through_by_channel"]["evidence"]

    assert criteria["loan_repayment_timing"]["status"] == "satisfied"
    assert "get_loan_and_payback_snapshot" in criteria["loan_repayment_timing"]["evidence"]
    assert "payback_plan" in criteria["loan_repayment_timing"]["evidence"]

    assert criteria["cash_flow_risks"]["status"] == "satisfied"
    assert "risk_flags" in criteria["cash_flow_risks"]["evidence"]

    assert criteria["partner_weekly_update"]["status"] == "satisfied"
    assert "generate_partner_update" in criteria["partner_weekly_update"]["evidence"]

    assert criteria["buy_recommendation"]["status"] == "satisfied"
    assert "safe/risky/not worth doing" in criteria["buy_recommendation"]["evidence"]

    assert criteria["evidence_backed_recommendations"]["status"] == "satisfied"
    assert "scripts/degen_ops_pilot_demo.py" in criteria["evidence_backed_recommendations"]["evidence"]


def test_mvp_audit_keeps_team_live_rollout_pending_until_topology_approved():
    audit = build_mvp_audit(repo_root=Path.cwd())
    criteria = {item["id"]: item for item in audit["criteria"]}

    assert audit["mvp_code_ready"] is True
    assert audit["team_live_rollout_ready"] is False
    assert criteria["team_live_rollout"]["status"] == "pending_decision"
    assert "live_data_access_topology" in criteria["team_live_rollout"]["remaining"]
    assert "partner_scope_finance_visibility" not in criteria["team_live_rollout"]["remaining"]
    assert "partner_scope_finance_visibility" not in audit["remaining_decisions"]


def test_mvp_audit_script_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_mvp_audit.py"

    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert "[logging]" not in result.stdout
    assert '"mvp_code_ready": true' in result.stdout
    assert '"team_live_rollout_ready": false' in result.stdout
