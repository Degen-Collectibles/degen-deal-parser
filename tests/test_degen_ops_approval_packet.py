from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_approval_packet import build_approval_packet


def test_approval_packet_lists_live_data_options_and_required_approval():
    packet = build_approval_packet(database_url_env="DEGEN_OPS_READONLY_DATABASE_URL")

    assert "Option A: Local Clients With Read-Only DB URL" in packet
    assert "Option B: Green-Hosted Chat Client" in packet
    assert "Option C: Hosted Read-Only MCP Gateway" in packet
    assert "Requires explicit proceed before implementation" in packet
    assert "No production writes" in packet
    assert "No database migrations" in packet
    assert "No service restarts" in packet
    assert "Do not build Option C" in packet
    assert "DEGEN_OPS_READONLY_DATABASE_URL" in packet
    assert "scripts\\degen_ops_topology_plan.py --audience partner --client hermes --json" in packet
    assert "scripts\\degen_ops_green_pilot_packet.py --audience partner --client hermes" in packet
    assert "scripts\\degen_ops_live_data.py --scope employee" in packet
    assert "scripts\\degen_ops_readiness.py --json" in packet
    assert "postgresql://" not in packet
    assert "postgresql+psycopg://" not in packet


def test_approval_packet_has_rollback_and_success_criteria():
    packet = build_approval_packet(database_url_env="DEGEN_OPS_READONLY_DATABASE_URL")

    assert "Rollback" in packet
    assert "Remove scoped MCP config" in packet
    assert "Revoke the read-only credential" in packet
    assert "Success Criteria" in packet
    assert "employee scope exposes exactly 3 tools" in packet
    assert "partner scope excludes raw cash and owner loan/payback tools" in packet
    assert "missing or blank scope defaults to employee tools" in packet
    assert "partner or owner scope can answer the buy-decision workflow" in packet
    assert "Use owner scope for any partner only if owner-level cash" in packet


def test_approval_packet_script_outputs_clean_markdown():
    script = Path.cwd() / "scripts" / "degen_ops_approval_packet.py"

    result = subprocess.run(
        [sys.executable, str(script), "--database-url-env", "DEGEN_OPS_READONLY_DATABASE_URL"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("# Degen Ops Live-Data Approval Packet")
    assert "[logging]" not in result.stdout
    assert "DEGEN_OPS_READONLY_DATABASE_URL" in result.stdout
