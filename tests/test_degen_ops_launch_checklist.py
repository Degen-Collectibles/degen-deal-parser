from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_launch_checklist import build_launch_checklist


def test_launch_checklist_is_preapproval_only_for_partner_hermes():
    checklist = build_launch_checklist(audience="partner", client="hermes")

    assert checklist["name"] == "degen_ops_launch_checklist"
    assert checklist["audience"] == "partner"
    assert checklist["client"] == "hermes"
    assert checklist["recommended_topology"] == "green_hosted_client"
    assert checklist["pre_approval_ready"] is True
    assert checklist["team_live_rollout_ready"] is False
    assert checklist["approval_phrase"] == "proceed with Green-hosted Degen Ops pilot for partner via hermes"
    assert "explicit approval phrase" in checklist["reason_team_rollout_not_ready"]


def test_launch_checklist_keeps_partner_scope_redacted_and_secret_free():
    checklist = build_launch_checklist(audience="partner", client="hermes")
    rendered = str(checklist)

    assert checklist["scope"]["tool_count"] == 17
    assert "evaluate_inventory_buy" in checklist["scope"]["tools"]
    assert "generate_partner_update" in checklist["scope"]["tools"]
    assert "generate_weekly_partner_update_draft" in checklist["scope"]["tools"]
    assert "get_price_lookup" in checklist["scope"]["tools"]
    assert "get_market_trend_lookup" in checklist["scope"]["tools"]
    assert "get_web_search" in checklist["scope"]["tools"]
    assert "get_cash_snapshot" not in checklist["scope"]["tools"]
    assert "get_loan_and_payback_snapshot" not in checklist["scope"]["tools"]
    assert checklist["scope"]["owner_only_tools_hidden"] == [
        "get_cash_snapshot",
        "get_loan_and_payback_snapshot",
    ]
    assert "postgresql://" not in rendered
    assert "postgresql+psycopg://" not in rendered
    assert "Do not use owner scope for partner or employee pilots." in checklist["must_not_do_before_approval"]


def test_launch_checklist_after_approval_commands_are_green_bash_commands():
    checklist = build_launch_checklist(
        audience="partner",
        client="codex",
        app_dir="/srv/degen",
        python_command="/srv/degen/.venv/bin/python",
    )
    commands = checklist["after_approval_green_commands"]

    assert commands[0] == "cd /srv/degen"
    assert "export LOG_TO_FILE=false" in commands
    assert any(command.startswith(": \"${DEGEN_OPS_READONLY_DATABASE_URL:?") for command in commands)
    assert any("scripts/degen_ops_live_data.py --scope partner" in command for command in commands)
    assert any("scripts/degen_ops_pilot_demo.py --scope partner" in command for command in commands)
    assert any("--green-session-approved --green-live-read-verified" in command for command in commands)
    assert not any(".\\.venv\\Scripts\\python.exe" in command for command in commands)


def test_launch_checklist_employee_scope_stays_employee_but_buy_demo_uses_partner_redaction():
    checklist = build_launch_checklist(audience="employee", client="hermes")

    assert checklist["scope"]["tool_count"] == 13
    assert checklist["scope"]["tools"] == [
        "get_channel_velocity",
        "get_discord_sales_summary",
        "get_inventory_snapshot",
        "get_market_trend_lookup",
        "get_ops_agent_manifest",
        "get_ops_memory",
        "get_price_lookup",
        "get_sales_summary",
        "get_shopify_product_sales",
        "get_shopify_top_products",
        "get_tiktok_product_sales",
        "get_tiktok_top_products",
        "get_web_search",
    ]
    assert any("scripts/degen_ops_live_data.py --scope employee" in command for command in checklist["after_approval_green_commands"])
    assert any("scripts/degen_ops_pilot_demo.py --scope partner" in command for command in checklist["after_approval_green_commands"])


def test_launch_checklist_script_outputs_clean_json_and_markdown():
    script = Path.cwd() / "scripts" / "degen_ops_launch_checklist.py"

    json_result = subprocess.run(
        [sys.executable, str(script), "--audience", "partner", "--client", "hermes", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )
    markdown_result = subprocess.run(
        [sys.executable, str(script), "--audience", "partner", "--client", "hermes"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert json_result.returncode == 0, json_result.stderr
    assert json_result.stdout.startswith("{")
    assert '"team_live_rollout_ready": false' in json_result.stdout
    assert "[logging]" not in json_result.stdout
    assert markdown_result.returncode == 0, markdown_result.stderr
    assert markdown_result.stdout.startswith("# Degen Ops Launch Checklist")
    assert "```bash" in markdown_result.stdout
