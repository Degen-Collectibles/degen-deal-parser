from pathlib import Path
import json
import subprocess
import sys

from scripts.degen_ops_deploy_preflight import build_deploy_preflight


def test_deploy_preflight_is_read_only_and_names_exact_targets():
    report = build_deploy_preflight()

    assert report["name"] == "degen_ops_deploy_preflight"
    assert report["read_only"] is True
    assert report["performs_changes"] is False
    assert report["requires_explicit_approval"] is True
    assert report["approval_phrase"] == "proceed with Degen Ops bot commit and Green Discord rollout"
    assert report["targets"] == {
        "repository": "https://github.com/Degen-Collectibles/degen-deal-parser.git",
        "production_host": "Green/Brev openclaw-9902ae",
        "production_app_dir": "/opt/degen/app",
        "discord_surface": "Degen Ops Bot",
    }
    assert "app/ops_mcp.py" in report["intended_files"]
    assert "scripts/degen_ops_discord_bot.py" in report["intended_files"]
    assert "tests/test_degen_ops_prompt_coverage.py" in report["intended_files"]
    assert "app/routers/bookkeeping.py" in report["known_unrelated_files"]
    assert "commit/push to origin/main or reviewed branch" in report["externally_visible_changes"]
    assert "restart Degen Ops Discord bot service" in report["externally_visible_changes"]


def test_deploy_preflight_has_rollback_and_postchecks():
    report = build_deploy_preflight()

    assert any("previous commit" in step.lower() for step in report["rollback_plan"])
    assert any("degen_ops_local_gate.py --json" in check for check in report["pre_deploy_checks"])
    assert any("degen_ops_live_data.py" in check for check in report["post_deploy_checks"])
    assert any("Discord smoke prompts" in check for check in report["post_deploy_checks"])
    assert "No production writes are performed by this preflight." in report["notes"]


def test_deploy_preflight_script_outputs_clean_json_and_markdown():
    script = Path.cwd() / "scripts" / "degen_ops_deploy_preflight.py"

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
    parsed = json.loads(json_result.stdout)
    assert parsed["read_only"] is True
    assert "[logging]" not in json_result.stdout
    assert markdown_result.returncode == 0, markdown_result.stderr
    assert markdown_result.stdout.startswith("# Degen Ops Deploy Preflight")
    assert "## Rollback" in markdown_result.stdout
