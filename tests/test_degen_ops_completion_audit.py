from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_completion_audit import build_completion_audit


def test_completion_audit_maps_chatbot_mcp_and_green_pilot_requirements():
    audit = build_completion_audit(repo_root=Path.cwd())
    requirements = {item["id"]: item for item in audit["requirements"]}

    assert audit["code_ready"] is True
    assert audit["goal_complete"] is False
    assert audit["team_live_rollout_ready"] is False
    assert audit["missing"] == []
    assert audit["pending"] == ["team_live_rollout"]
    assert requirements["chatbot_surface"]["status"] == "satisfied"
    assert requirements["mcp_surface"]["status"] == "satisfied"
    assert requirements["scoped_access"]["status"] == "satisfied"
    assert requirements["partner_redaction"]["status"] == "satisfied"
    assert requirements["read_only_guardrails"]["status"] == "satisfied"
    assert requirements["buy_decision_workflow"]["status"] == "satisfied"
    assert requirements["nvidia_openai_compatible_llm"]["status"] == "satisfied"
    assert requirements["hermes_codex_config_and_smoke"]["status"] == "satisfied"
    assert requirements["green_pilot_handoff"]["status"] == "satisfied"
    assert requirements["team_live_rollout"]["status"] == "pending_decision"
    assert requirements["prompt_tool_coverage"]["status"] == "satisfied"
    assert requirements["deploy_preflight"]["status"] == "satisfied"


def test_completion_audit_names_live_evidence_required_before_goal_complete():
    audit = build_completion_audit(repo_root=Path.cwd())
    rollout = next(item for item in audit["requirements"] if item["id"] == "team_live_rollout")

    assert "Jeffrey explicit approval phrase" in rollout["remaining"]
    assert "live-data verifier pass from the approved environment" in rollout["remaining"]
    assert "rollout gate pass with --run-live --run-pilot" in rollout["remaining"]
    assert audit["next_approval_phrase"] == "proceed with Green-hosted Degen Ops pilot for partner via hermes"
    assert "not considered complete" in rollout["note"]


def test_completion_audit_script_outputs_clean_json_and_markdown():
    script = Path.cwd() / "scripts" / "degen_ops_completion_audit.py"

    json_result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )
    markdown_result = subprocess.run(
        [sys.executable, str(script)],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert json_result.returncode == 0, json_result.stderr
    assert json_result.stdout.startswith("{")
    assert '"goal_complete": false' in json_result.stdout
    assert "[logging]" not in json_result.stdout
    assert markdown_result.returncode == 0, markdown_result.stderr
    assert markdown_result.stdout.startswith("# Degen Ops Completion Audit")
    assert "pending_decision: Partners and employees can use live data safely" in markdown_result.stdout
