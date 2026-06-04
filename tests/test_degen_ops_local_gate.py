from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_local_gate import _parse_step_summary, build_local_gate_plan, run_local_gate


def test_local_gate_plan_contains_expected_read_only_steps():
    plan = build_local_gate_plan(repo_root=Path.cwd())

    assert list(plan) == [
        "compile",
        "tests",
        "mcp_smoke",
        "rollout_gate",
        "change_manifest",
        "diff_check",
    ]
    assert "-m" in plan["compile"]
    assert "compileall" in plan["compile"]
    assert "tests/test_degen_ops_answer_eval.py" in plan["tests"]
    assert "scripts/degen_ops_mcp_smoke.py" in plan["mcp_smoke"]
    assert "--read-check" in plan["mcp_smoke"]
    assert "scripts/degen_ops_change_manifest.py" in plan["change_manifest"]
    assert "--summary" in plan["change_manifest"]
    assert "--sample-limit" in plan["change_manifest"]
    assert "5" in plan["change_manifest"]
    assert plan["diff_check"][:4] == ["git", "diff", "--check", "--"]


def test_local_gate_dry_run_reports_steps_without_running():
    report = run_local_gate(repo_root=Path.cwd(), dry_run=True)

    assert report["ok"] is True
    assert report["dry_run"] is True
    assert report["team_live_rollout_ready"] is False
    assert [step["name"] for step in report["steps"]] == [
        "compile",
        "tests",
        "mcp_smoke",
        "rollout_gate",
        "change_manifest",
        "diff_check",
    ]


def test_local_gate_script_dry_run_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_local_gate.py"

    result = subprocess.run(
        [sys.executable, str(script), "--dry-run", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert '"name": "degen_ops_local_gate"' in result.stdout
    assert '"dry_run": true' in result.stdout


def test_local_gate_parses_rollout_summary():
    summary = _parse_step_summary(
        "rollout_gate",
        """
        {
          "static_ok": true,
          "team_live_rollout_ready": false,
          "completion": {
            "goal_complete": false,
            "pending": ["team_live_rollout"]
          }
        }
        """,
    )

    assert summary == {
        "static_ok": True,
        "team_live_rollout_ready": False,
        "goal_complete": False,
        "pending": ["team_live_rollout"],
    }


def test_local_gate_parses_change_manifest_summary():
    summary = _parse_step_summary(
        "change_manifest",
        """
        {
          "intended_file_count": 46,
          "unrelated_file_count": 14831,
          "safe_to_stage_intended_only": true,
          "stage_command_available": true
        }
        """,
    )

    assert summary == {
        "intended_file_count": 46,
        "unrelated_file_count": 14831,
        "safe_to_stage_intended_only": True,
        "stage_command_available": True,
    }
