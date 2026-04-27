#!/usr/bin/env bash
# Redeploy Degen on the Linux/Green host.
# Intended to be run by the GitHub Actions self-hosted runner after cutover.
set -Eeuo pipefail

APP_DIR="${DEGEN_APP_DIR:-/opt/degen/app}"
WEB_UNIT="${DEGEN_WEB_UNIT:-degen-web.service}"
WORKER_UNIT="${DEGEN_WORKER_UNIT:-degen-worker.service}"
HEALTH_URL="${DEGEN_HEALTH_URL:-http://127.0.0.1:8000/health}"
MAX_WAIT_SECONDS="${DEGEN_HEALTH_MAX_WAIT_SECONDS:-120}"
INTERVAL_SECONDS="${DEGEN_HEALTH_INTERVAL_SECONDS:-5}"
INSTALL_DEPS="${DEGEN_INSTALL_DEPS:-1}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    exit 127
  }
}

require_cmd git
require_cmd systemctl
require_cmd curl

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "ERROR: APP_DIR is not a git checkout: $APP_DIR" >&2
  exit 2
fi

cd "$APP_DIR"

log "Starting Linux redeploy in $APP_DIR"
log "Fetching origin/main"
git fetch origin main

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "main" ]]; then
  echo "ERROR: expected branch main, got $current_branch" >&2
  exit 3
fi

log "Rebasing onto origin/main"
git pull --rebase origin main

if [[ "$INSTALL_DEPS" != "0" ]]; then
  if [[ ! -x .venv/bin/pip ]]; then
    echo "ERROR: missing virtualenv pip at $APP_DIR/.venv/bin/pip" >&2
    exit 4
  fi
  log "Installing Python dependencies"
  .venv/bin/pip install -r requirements.txt
fi

mkdir -p logs
stamp_path="logs/deploy.stamp"
git_sha="$(git rev-parse HEAD)"
git_branch="$(git rev-parse --abbrev-ref HEAD)"
python3 - <<PY
import json, os, socket, datetime
stamp = {
    "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "git_sha": "$git_sha",
    "git_branch": "$git_branch",
    "host": socket.gethostname(),
    "user": os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown",
    "reason": "redeploy-linux.sh",
}
with open("$stamp_path", "w", encoding="utf-8") as f:
    json.dump(stamp, f, separators=(",", ":"))
    f.write("\n")
PY
log "Wrote deploy stamp: $APP_DIR/$stamp_path ($git_sha)"

log "Restarting $WEB_UNIT"
sudo -n systemctl restart "$WEB_UNIT"

if systemctl list-unit-files "$WORKER_UNIT" --no-legend 2>/dev/null | grep -q "^$WORKER_UNIT"; then
  log "Restarting $WORKER_UNIT"
  sudo -n systemctl restart "$WORKER_UNIT"
else
  log "Worker unit $WORKER_UNIT not installed; skipping worker restart"
fi

log "Waiting for health: $HEALTH_URL"
elapsed=0
sleep 10
elapsed=10
while (( elapsed <= MAX_WAIT_SECONDS )); do
  status="$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 10 "$HEALTH_URL" || true)"
  if [[ "$status" =~ ^[234][0-9][0-9]$ ]]; then
    log "Health check passed after ${elapsed}s (status $status)"
    exit 0
  fi
  log "Health check attempt at ${elapsed}s returned '${status:-no-response}'"
  sleep "$INTERVAL_SECONDS"
  elapsed=$((elapsed + INTERVAL_SECONDS))
done

echo "ERROR: health check failed: server did not respond healthy within ${MAX_WAIT_SECONDS}s" >&2
sudo -n systemctl status "$WEB_UNIT" --no-pager || true
exit 5
