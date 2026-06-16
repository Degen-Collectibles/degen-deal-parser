from pathlib import Path

from scripts.degen_ops_mcp_config import render_codex, render_hermes


def test_render_hermes_employee_uses_scope_and_database_env_reference():
    rendered = render_hermes(
        server_name="degen_ops_employee",
        scope="employee",
        python_path=Path("C:/repo/.venv/Scripts/python.exe"),
        script_path=Path("C:/repo/scripts/degen_ops_mcp.py"),
        database_url="${DEGEN_OPS_READONLY_DATABASE_URL}",
    )

    assert "degen_ops_employee:" in rendered
    assert 'command: "C:/repo/.venv/Scripts/python.exe"' in rendered
    assert '- "C:/repo/scripts/degen_ops_mcp.py"' in rendered
    assert 'DEGEN_OPS_MCP_SCOPE: "employee"' in rendered
    assert 'DATABASE_URL: "${DEGEN_OPS_READONLY_DATABASE_URL}"' in rendered
    assert 'LOG_TO_FILE: "false"' in rendered


def test_render_codex_partner_uses_scope_and_no_owner_name():
    rendered = render_codex(
        server_name="degen_ops_partner",
        scope="partner",
        python_path=Path("C:/repo/.venv/Scripts/python.exe"),
        script_path=Path("C:/repo/scripts/degen_ops_mcp.py"),
        database_url="${DEGEN_OPS_READONLY_DATABASE_URL}",
    )

    assert "[mcp_servers.degen_ops_partner]" in rendered
    assert 'command = "C:/repo/.venv/Scripts/python.exe"' in rendered
    assert 'args = ["C:/repo/scripts/degen_ops_mcp.py"]' in rendered
    assert '[mcp_servers.degen_ops_partner.env]' in rendered
    assert 'DEGEN_OPS_MCP_SCOPE = "partner"' in rendered
    assert 'DATABASE_URL = "${DEGEN_OPS_READONLY_DATABASE_URL}"' in rendered
    assert "degen_ops_owner" not in rendered


def test_render_hermes_manager_uses_manager_scope():
    rendered = render_hermes(
        server_name="degen_ops_manager",
        scope="manager",
        python_path=Path("C:/repo/.venv/Scripts/python.exe"),
        script_path=Path("C:/repo/scripts/degen_ops_mcp.py"),
        database_url="${DEGEN_OPS_READONLY_DATABASE_URL}",
    )

    assert "degen_ops_manager:" in rendered
    assert 'DEGEN_OPS_MCP_SCOPE: "manager"' in rendered
