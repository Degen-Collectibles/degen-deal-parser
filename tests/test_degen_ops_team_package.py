from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_team_package import build_team_package


def test_employee_team_package_uses_employee_scope_only_and_env_reference():
    package = build_team_package(
        scope="employee",
        clients=["hermes"],
        database_url_env="DEGEN_OPS_READONLY_DATABASE_URL",
    )

    assert "scope: employee" in package
    assert "audience: staff / stream operators" in package
    assert "degen_ops_employee" in package
    assert "DEGEN_OPS_READONLY_DATABASE_URL" in package
    assert "get_inventory_snapshot" in package
    assert "get_channel_velocity" in package
    assert "get_cash_snapshot" not in package
    assert "get_loan_and_payback_snapshot" not in package
    assert "degen_ops_owner" not in package
    assert "degen_ops_partner" not in package
    assert "postgresql://" not in package
    assert "postgresql+psycopg://" not in package


def test_partner_team_package_includes_chat_preflight_and_no_message_sending_claims():
    package = build_team_package(
        scope="partner",
        clients=["codex"],
        database_url_env="DEGEN_OPS_READONLY_DATABASE_URL",
    )

    assert "scope: partner" in package
    assert "degen_ops_partner" in package
    assert "--scope partner --preflight --read-check" in package
    assert "--scope partner --prompt" in package
    assert "No money movement" in package
    assert "No customer or partner messages without approval" in package
    assert "Draft only" in package
    assert "degen_ops_owner" not in package
    assert "get_cash_snapshot" not in package
    assert "get_loan_and_payback_snapshot" not in package
    assert "Partner redaction" in package
    assert "owner-scope only" in package


def test_team_package_script_runs_directly_from_repo_root():
    script = Path(__file__).resolve().parents[1] / "scripts" / "degen_ops_team_package.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--scope",
            "employee",
            "--client",
            "hermes",
            "--database-url-env",
            "DEGEN_OPS_READONLY_DATABASE_URL",
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("# Degen Ops Access Package: employee")
    assert "[logging]" not in result.stdout
    assert "scope: employee" in result.stdout
    assert "degen_ops_employee" in result.stdout
