from pathlib import Path
import subprocess
import sys

from scripts.degen_ops_change_manifest import build_change_manifest, intended_path_set, summarize_manifest


def test_change_manifest_separates_intended_ops_files_from_unrelated_changes():
    manifest = build_change_manifest(
        changed_paths=[
            "app/ops_mcp.py",
            "app/ops_chat.py",
            "app/db.py",
            "app/degen_ops_discord_auth.py",
            "app/models.py",
            "app/routers/team_admin_employees.py",
            "app/templates/team/admin/employee_detail.html",
            "app/templates/team/admin/employees_list.html",
            "scripts/degen_ops_discord_bot.py",
            "scripts/degen_ops_answer_eval.py",
            "scripts/degen_ops_prompt_coverage.py",
            "scripts/degen_ops_deploy_preflight.py",
            "tests/test_admin_employee_list_search.py",
            "tests/test_degen_ops_answer_eval.py",
            "tests/test_degen_ops_discord_auth.py",
            "tests/test_degen_ops_prompt_coverage.py",
            "tests/test_degen_ops_deploy_preflight.py",
            "tests/test_wave47_admin_tools.py",
            "docs/ops/degen-ops-bot-improvement-plan.md",
            "docs/ops/degen-ops-discord-employee-auth-prd.md",
            "docs/ops/degen-ops-team-rollout-prd.md",
            "docs/superpowers/plans/2026-06-12-degen-ops-discord-employee-auth.md",
            "requirements.txt",
            "app/routers/team.py",
            "tests/test_schedule_save_preserves_untouched.py",
            "outputs/generated.log",
        ]
    )

    assert manifest["safe_to_stage_intended_only"] is True
    assert manifest["intended_files"] == [
        "app/db.py",
        "app/degen_ops_discord_auth.py",
        "app/models.py",
        "app/ops_chat.py",
        "app/ops_mcp.py",
        "app/routers/team_admin_employees.py",
        "app/templates/team/admin/employee_detail.html",
        "app/templates/team/admin/employees_list.html",
        "docs/ops/degen-ops-bot-improvement-plan.md",
        "docs/ops/degen-ops-discord-employee-auth-prd.md",
        "docs/ops/degen-ops-team-rollout-prd.md",
        "docs/superpowers/plans/2026-06-12-degen-ops-discord-employee-auth.md",
        "requirements.txt",
        "scripts/degen_ops_answer_eval.py",
        "scripts/degen_ops_deploy_preflight.py",
        "scripts/degen_ops_discord_bot.py",
        "scripts/degen_ops_prompt_coverage.py",
        "tests/test_admin_employee_list_search.py",
        "tests/test_degen_ops_answer_eval.py",
        "tests/test_degen_ops_deploy_preflight.py",
        "tests/test_degen_ops_discord_auth.py",
        "tests/test_degen_ops_prompt_coverage.py",
        "tests/test_wave47_admin_tools.py",
    ]
    assert manifest["unrelated_files"] == [
        "app/routers/team.py",
        "outputs/generated.log",
        "tests/test_schedule_save_preserves_untouched.py",
    ]
    assert manifest["generated_noise"] == ["outputs/generated.log"]
    assert "git add --" in manifest["stage_command"]
    assert "outputs/generated.log" not in manifest["stage_command"]


def test_intended_path_set_includes_required_ops_artifacts_and_not_outputs():
    paths = intended_path_set()

    assert "app/ops_mcp.py" in paths
    assert "app/db.py" in paths
    assert "app/degen_ops_discord_auth.py" in paths
    assert "app/models.py" in paths
    assert "app/routers/team_admin_employees.py" in paths
    assert "app/templates/team/admin/employee_detail.html" in paths
    assert "app/templates/team/admin/employees_list.html" in paths
    assert "scripts/degen_ops_discord_bot.py" in paths
    assert "scripts/degen_ops_prompt_coverage.py" in paths
    assert "scripts/degen_ops_deploy_preflight.py" in paths
    assert "scripts/degen_ops_change_manifest.py" in paths
    assert "scripts/degen_ops_readiness.py" in paths
    assert "tests/test_ops_mcp.py" in paths
    assert "tests/test_admin_employee_list_search.py" in paths
    assert "tests/test_degen_ops_discord_auth.py" in paths
    assert "tests/test_wave47_admin_tools.py" in paths
    assert "docs/ops/degen-ops-bot-improvement-plan.md" in paths
    assert "docs/ops/degen-ops-discord-employee-auth-prd.md" in paths
    assert "docs/ops/degen-ops-hermes-mcp-pilot.md" in paths
    assert "docs/superpowers/plans/2026-06-12-degen-ops-discord-employee-auth.md" in paths
    assert "outputs/generated.log" not in paths
    assert "app/routers/team.py" not in paths


def test_change_manifest_summary_limits_large_lists_but_keeps_counts():
    manifest = build_change_manifest(
        changed_paths=[
            "app/ops_mcp.py",
            "scripts/degen_ops_change_manifest.py",
            "outputs/generated-1.log",
            "outputs/generated-2.log",
            "app/routers/team.py",
        ]
    )
    summary = summarize_manifest(manifest, sample_limit=1)

    assert summary["summary"] is True
    assert summary["intended_file_count"] == 2
    assert summary["unrelated_file_count"] == 3
    assert summary["generated_noise_count"] == 2
    assert len(summary["intended_files_sample"]) == 1
    assert len(summary["unrelated_files_sample"]) == 1
    assert len(summary["generated_noise_sample"]) == 1
    assert summary["stage_command_available"] is True
    assert "git add --" not in str(summary)


def test_change_manifest_script_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_change_manifest.py"

    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert '"name": "degen_ops_change_manifest"' in result.stdout
    assert "git add -A" not in result.stdout


def test_change_manifest_script_summary_outputs_compact_json():
    script = Path.cwd() / "scripts" / "degen_ops_change_manifest.py"

    result = subprocess.run(
        [sys.executable, str(script), "--summary", "--sample-limit", "2", "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert '"summary": true' in result.stdout
    assert "intended_files_sample" in result.stdout
    assert "stage_command_note" in result.stdout
