from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_rollout_gate import build_rollout_gate_report


def test_rollout_gate_aggregates_static_audits_and_skips_live_by_default():
    report = build_rollout_gate_report(repo_root=Path.cwd())

    assert report["static_ok"] is True
    assert report["live_data"]["status"] == "skipped"
    assert report["pilot_demo"]["status"] == "skipped"
    assert report["team_live_rollout_ready"] is False
    assert "live_data_access_topology" in report["required_decisions"]
    assert "partner_scope_finance_visibility" not in report["required_decisions"]
    assert report["readiness"]["code_ready"] is True
    assert report["completion"]["code_ready"] is True
    assert report["completion"]["goal_complete"] is False
    assert report["completion"]["pending"] == ["team_live_rollout"]
    assert report["mvp"]["mvp_code_ready"] is True
    assert report["scope"]["ok"] is True


def test_rollout_gate_can_include_passed_live_and_pilot_reports():
    report = build_rollout_gate_report(
        repo_root=Path.cwd(),
        live_data_report={"ok": True, "scope": "employee"},
        pilot_demo_report={"ok": True, "scope": "partner"},
    )

    assert report["static_ok"] is True
    assert report["live_data"]["status"] == "pass"
    assert report["pilot_demo"]["status"] == "pass"
    assert report["live_data"]["report"]["scope"] == "employee"
    assert report["pilot_demo"]["report"]["scope"] == "partner"


def test_rollout_gate_script_missing_live_database_env_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_rollout_gate.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--run-live",
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
    assert '"status": "fail"' in result.stdout
    assert "MISSING_DEGEN_DB_URL" in result.stdout
