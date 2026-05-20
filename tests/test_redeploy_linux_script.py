from pathlib import Path


def test_redeploy_checks_worker_unit_after_restart():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assert 'systemctl is-active --quiet "$unit"' in script

    worker_restart = 'sudo -n systemctl restart "$WORKER_UNIT"'
    assert worker_restart in script
    assert 'wait_for_systemd_unit "$WORKER_UNIT"' in script[script.index(worker_restart) :]
