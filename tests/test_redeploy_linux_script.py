from pathlib import Path


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
