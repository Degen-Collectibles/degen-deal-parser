from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_green_pilot_packet import build_green_pilot_packet


def test_green_pilot_packet_names_exact_approval_target_and_boundaries():
    packet = build_green_pilot_packet(audience="partner", client="hermes")

    assert "Degen Ops Green-Hosted Pilot Packet" in packet
    assert "topology: green_hosted_client" in packet
    assert "audience: partner" in packet
    assert "client: hermes" in packet
    assert "app_dir: /opt/degen/app" in packet
    assert "proceed with Green-hosted Degen Ops pilot for partner via hermes" in packet
    assert "Do not treat this packet" in packet
    assert "No production writes" in packet
    assert "No service restarts" in packet
    assert "No database migrations" in packet
    assert "No direct live database credential copied" in packet
    assert "postgresql://" not in packet
    assert "postgresql+psycopg://" not in packet
    assert "powershell" not in packet.lower()


def test_green_pilot_packet_includes_verification_and_rollback_commands():
    packet = build_green_pilot_packet(
        audience="partner",
        client="codex",
        database_url_env="DEGEN_OPS_READONLY_DATABASE_URL",
    )

    assert "cd /opt/degen/app" in packet
    assert "export LOG_TO_FILE=false" in packet
    assert "${DEGEN_OPS_READONLY_DATABASE_URL:?Set DEGEN_OPS_READONLY_DATABASE_URL" in packet
    assert "scripts/degen_ops_topology_plan.py --audience partner --client codex" in packet
    assert "scripts/degen_ops_rollout_gate.py --run-live --run-pilot --live-scope partner" in packet
    assert "scripts/degen_ops_live_data.py --scope partner" in packet
    assert "scripts/degen_ops_pilot_demo.py --scope partner" in packet
    assert "ready_for_team_pilot: true" in packet
    assert "Remove or unset `DEGEN_OPS_READONLY_DATABASE_URL`" in packet
    assert packet.index("scripts/degen_ops_live_data.py") < packet.index("scripts/degen_ops_topology_plan.py")


def test_green_pilot_employee_packet_keeps_employee_live_scope_and_partner_pilot_scope():
    packet = build_green_pilot_packet(audience="employee", client="hermes")

    assert "--live-scope employee" in packet
    assert "--pilot-scope partner" in packet
    assert "Employee scope shows exactly 3 tools" in packet


def test_green_pilot_packet_script_outputs_clean_markdown():
    script = Path.cwd() / "scripts" / "degen_ops_green_pilot_packet.py"

    result = subprocess.run(
        [sys.executable, str(script), "--audience", "partner", "--client", "hermes"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0
    assert result.stdout.startswith("# Degen Ops Green-Hosted Pilot Packet")
    assert "[logging]" not in result.stdout
    assert "DEGEN_OPS_READONLY_DATABASE_URL" in result.stdout


def test_green_pilot_packet_accepts_custom_green_runtime():
    packet = build_green_pilot_packet(
        audience="partner",
        client="hermes",
        app_dir="/tmp/degen",
        python_command="/opt/venv/bin/python",
    )

    assert "cd /tmp/degen" in packet
    assert "/opt/venv/bin/python scripts/degen_ops_readiness.py --json" in packet
