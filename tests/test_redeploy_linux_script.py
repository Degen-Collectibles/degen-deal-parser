from pathlib import Path

from app.config import Settings


def test_redeploy_checks_worker_unit_after_restart():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assert 'systemctl is-active --quiet "$unit"' in script

    worker_restart = 'sudo -n systemctl restart "$WORKER_UNIT"'
    assert worker_restart in script
    assert 'wait_for_systemd_unit "$WORKER_UNIT"' in script[script.index(worker_restart) :]


def test_redeploy_restarts_discord_bot_unit_after_worker():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assert 'BOT_UNIT="${DEGEN_OPS_DISCORD_BOT_UNIT:-degen-ops-discord-bot.service}"' in script
    assert 'BOT_SYSTEMD_SCOPE="${DEGEN_OPS_DISCORD_BOT_SYSTEMD_SCOPE:-user}"' in script
    assert 'bot_systemctl restart "$BOT_UNIT"' in script

    worker_restart = 'sudo -n systemctl restart "$WORKER_UNIT"'
    assert 'restart_discord_bot' in script[script.index(worker_restart) :]


def test_redeploy_restores_user_systemd_bus_for_noninteractive_runner():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")
    block_start = script.index("bot_systemctl() {")
    block_end = script.index("\nbot_unit_installed() {", block_start)
    bot_systemctl = script[block_start:block_end]

    assert 'local runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"' in bot_systemctl
    assert (
        'local bus_address="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${runtime_dir}/bus}"'
        in bot_systemctl
    )
    assert 'XDG_RUNTIME_DIR="$runtime_dir"' in bot_systemctl
    assert 'DBUS_SESSION_BUS_ADDRESS="$bus_address"' in bot_systemctl
    assert bot_systemctl.index('XDG_RUNTIME_DIR="$runtime_dir"') < bot_systemctl.index(
        'systemctl --user "$@"'
    )


def test_redeploy_fails_closed_when_discord_bot_unit_probe_errors():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")
    probe_start = script.index("bot_unit_installed() {")
    probe_end = script.index("\nwait_for_bot_unit() {", probe_start)
    probe = script[probe_start:probe_end]

    assert 'if ! load_state="$(bot_systemctl show "$BOT_UNIT" -p LoadState --value)"' in probe
    assert '"loaded")' in probe
    assert '"not-found")' in probe
    assert "unable to query Discord bot unit" in probe
    assert "unexpected load state" in probe
    assert "return 2" in probe
    assert "2>/dev/null" not in probe


def test_redeploy_bot_restart_control_paths_are_explicit_and_fail_closed():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")
    restart_start = script.index("restart_discord_bot() {")
    restart_end = script.index("\nif [[ ! -d \"$APP_DIR/.git\" ]]", restart_start)
    restart = script[restart_start:restart_end]
    call_site = script[restart_end:]

    disable_check = 'if [[ "$RESTART_BOT" == "0" ]]; then'
    probe_call = 'bot_unit_installed || probe_status=$?'
    assert disable_check in restart
    assert probe_call in restart
    assert 'return 0' in restart[restart.index(disable_check) : restart.index(probe_call)]
    assert 'case "$probe_status" in' in restart
    assert 'bot_systemctl restart "$BOT_UNIT"' in restart
    assert "skipping bot restart" in restart
    assert 'return "$probe_status"' in restart
    assert "\nrestart_discord_bot\n" in call_site
    assert "if restart_discord_bot" not in call_site


def test_redeploy_defaults_to_approved_eccn_primary_model():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assert (
        'PRIMARY_NVIDIA_MODEL="${DEGEN_PRIMARY_NVIDIA_MODEL:-us/azure/openai/eccn-gpt-5.5}"'
        in script
    )
    assert "openai/openai/gpt-5.5" not in script


def test_redeploy_disables_periodic_inference_in_all_env_files():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assert 'PARSER_REPROCESS_VALUE="${DEGEN_PARSER_REPROCESS_ENABLED:-false}"' in script
    assert 'STITCH_AUDIT_VALUE="${DEGEN_PERIODIC_STITCH_AUDIT_ENABLED:-false}"' in script
    assert 'require_bool DEGEN_PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"' in script
    assert 'require_bool DEGEN_PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"' in script

    for env_file in ("/opt/degen/web.env", "/opt/degen/worker.env", "/opt/degen/.env"):
        assert f'set_env_var {env_file} PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"' in script
        assert f'set_env_var {env_file} PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"' in script


def test_periodic_inference_defaults_are_disabled():
    settings = Settings(_env_file=None)

    assert settings.parser_reprocess_enabled is False
    assert settings.periodic_stitch_audit_enabled is False


def test_redeploy_expected_sha_mode_is_fail_closed_before_deploy_mutations():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assignment = 'EXPECTED_GIT_SHA="${DEGEN_EXPECTED_GIT_SHA:-}"'
    mode_start = 'if [[ -n "$EXPECTED_GIT_SHA" ]]; then'
    dirty_guard = 'if ! git diff --quiet || ! git diff --cached --quiet; then'
    install_start = 'if [[ "$INSTALL_DEPS" != "0" ]]; then'

    assert assignment in script
    assert mode_start in script
    assert 'if [[ ! "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then' in script
    assert 'actual_sha="$(git rev-parse HEAD)"' in script
    assert 'if [[ "$actual_sha" != "$EXPECTED_GIT_SHA" ]]; then' in script
    assert dirty_guard in script
    assert script.index(dirty_guard) < script.index(mode_start)
    assert script.index(mode_start) < script.index(install_start)


def test_redeploy_expected_sha_mode_skips_sync_but_manual_mode_keeps_it():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    block_start = script.index('if [[ -n "$EXPECTED_GIT_SHA" ]]; then')
    block_end = script.index('\nfi\n\nif [[ "$INSTALL_DEPS"', block_start)
    sync_block = script[block_start:block_end]
    expected_section, manual_section = sync_block.split("\nelse\n", 1)

    assert "git fetch origin main" not in expected_section
    assert "git pull --rebase origin main" not in expected_section
    assert 'log "Using workflow-synchronized checkout at $actual_sha"' in expected_section
    assert "git fetch origin main" in manual_section
    assert "git pull --rebase origin main" in manual_section
