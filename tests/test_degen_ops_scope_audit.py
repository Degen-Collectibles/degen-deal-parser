from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_scope_audit import build_scope_audit


def test_scope_audit_confirms_employee_boundary_and_partner_owner_tools():
    audit = build_scope_audit(database_url_env="DEGEN_OPS_READONLY_DATABASE_URL")

    assert audit["ok"] is True
    assert audit["default_scope_check"]["status"] == "pass"
    assert audit["default_scope_check"]["default_scope"] == "employee"
    assert audit["scopes"]["employee"]["status"] == "pass"
    assert audit["scopes"]["employee"]["tools"] == [
        "get_channel_velocity",
        "get_inventory_snapshot",
        "get_ops_agent_manifest",
    ]
    assert audit["scopes"]["employee"]["forbidden_tools_present"] == []
    assert audit["scopes"]["partner"]["tool_count"] == 6
    assert "evaluate_inventory_buy" in audit["scopes"]["partner"]["tools"]
    assert "get_cash_snapshot" not in audit["scopes"]["partner"]["tools"]
    assert "get_loan_and_payback_snapshot" not in audit["scopes"]["partner"]["tools"]
    assert audit["scopes"]["owner"]["tool_count"] == 8


def test_scope_audit_checks_generated_employee_package_is_secret_free():
    audit = build_scope_audit(database_url_env="DEGEN_OPS_READONLY_DATABASE_URL")

    package_check = audit["package_checks"]["employee"]

    assert package_check["status"] == "pass"
    assert package_check["contains_database_env_reference"] is True
    assert package_check["contains_raw_database_url"] is False
    assert package_check["contains_owner_scope"] is False
    assert package_check["contains_partner_scope"] is False
    assert package_check["contains_forbidden_tools"] is False


def test_scope_audit_checks_generated_partner_package_is_redacted_and_secret_free():
    audit = build_scope_audit(database_url_env="DEGEN_OPS_READONLY_DATABASE_URL")

    package_check = audit["package_checks"]["partner"]

    assert package_check["status"] == "pass"
    assert package_check["contains_database_env_reference"] is True
    assert package_check["contains_raw_database_url"] is False
    assert package_check["contains_owner_scope"] is False
    assert package_check["contains_owner_only_tools"] is False
    assert package_check["contains_buy_workflow"] is True


def test_scope_audit_script_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_scope_audit.py"

    result = subprocess.run(
        [sys.executable, str(script), "--database-url-env", "DEGEN_OPS_READONLY_DATABASE_URL", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert "[logging]" not in result.stdout
    assert '"ok": true' in result.stdout
    assert "DEGEN_OPS_READONLY_DATABASE_URL" in result.stdout
