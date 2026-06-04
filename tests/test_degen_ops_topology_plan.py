from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_topology_plan import build_topology_plan


def test_topology_plan_defaults_to_green_hosted_but_not_ready_without_approval():
    plan = build_topology_plan(audience="partner", client="hermes")

    assert plan["recommended_topology"] == "green_hosted_client"
    assert plan["ready_for_team_pilot"] is False
    assert plan["required_before_pilot"] == [
        "green_hosted_session_boundaries_approved",
        "green_live_read_gate_passed",
    ]
    assert plan["read_only"] is True
    assert "postgresql://" not in str(plan)
    assert "postgresql+psycopg://" not in str(plan)


def test_topology_plan_marks_local_direct_db_ready_only_after_approval_and_verification():
    plan = build_topology_plan(
        audience="employee",
        client="codex",
        direct_db_approved=True,
        direct_db_verified=True,
    )

    assert plan["recommended_topology"] == "local_direct_db"
    assert plan["ready_for_team_pilot"] is True
    assert plan["required_before_pilot"] == []
    local = next(option for option in plan["options"] if option["id"] == "local_direct_db")
    assert local["ready"] is True
    assert all("DEGEN_OPS_READONLY_DATABASE_URL" in command for command in local["verification_commands"])
    assert "--scope employee" in " ".join(local["verification_commands"])


def test_topology_plan_can_prefer_green_when_green_is_approved_and_verified():
    plan = build_topology_plan(
        audience="partner",
        client="hermes",
        green_session_approved=True,
        green_live_read_verified=True,
    )

    assert plan["recommended_topology"] == "green_hosted_client"
    assert plan["ready_for_team_pilot"] is True
    assert plan["required_before_pilot"] == []
    green = next(option for option in plan["options"] if option["id"] == "green_hosted_client")
    assert green["ready"] is True
    assert "scripts\\degen_ops_rollout_gate.py" in " ".join(green["verification_commands"])


def test_topology_plan_keeps_gateway_not_ready_without_separate_approval():
    plan = build_topology_plan(audience="partner", client="hermes", prefer="gateway")

    assert plan["recommended_topology"] == "hosted_mcp_gateway"
    assert plan["ready_for_team_pilot"] is False
    assert "hosted_gateway_security_and_deploy_plan_approved" in plan["required_before_pilot"]
    gateway = next(option for option in plan["options"] if option["id"] == "hosted_mcp_gateway")
    assert gateway["ready"] is False


def test_topology_plan_script_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_topology_plan.py"

    result = subprocess.run(
        [sys.executable, str(script), "--audience", "partner", "--client", "hermes", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert result.stdout.startswith("{")
    assert "[logging]" not in result.stdout
    assert '"recommended_topology": "green_hosted_client"' in result.stdout
