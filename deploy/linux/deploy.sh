#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/degen/app}"
REPO_URL="${REPO_URL:-https://github.com/jmanballa/degen-deal-parser.git}"
BRANCH="${BRANCH:-main}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
SERVICE_WEB="${SERVICE_WEB:-degen-web}"
SERVICE_WORKER="${SERVICE_WORKER:-degen-worker}"
STATE_DIR="${STATE_DIR:-/opt/degen/deploy/state}"
RESTART_WORKER="${RESTART_WORKER:-1}"

fail() { echo "deploy: ERROR: $*" >&2; exit 1; }
info() { echo "deploy: $*"; }

mkdir -p "$STATE_DIR"

if [[ ! -d "$APP_DIR/.git" ]]; then
  fail "$APP_DIR is not a git checkout yet. Clone manually first during Phase 1/2."
fi

cd "$APP_DIR"
prev_sha="$(git rev-parse HEAD)"
printf '%s\n' "$prev_sha" > "$STATE_DIR/previous_sha"
info "previous sha: $prev_sha"

info "fetching origin/$BRANCH"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"
new_sha="$(git rev-parse HEAD)"
printf '%s\n' "$new_sha" > "$STATE_DIR/current_sha"
info "new sha: $new_sha"

if [[ ! -x .venv/bin/python ]]; then
  info "creating venv"
  python3 -m venv .venv
fi

info "installing dependencies"
.venv/bin/python -m pip install --upgrade pip
if [[ -f requirements.txt ]]; then
  .venv/bin/python -m pip install -r requirements.txt
else
  fail "requirements.txt missing"
fi

info "import check"
.venv/bin/python - <<'PY'
import app.main
print('import app.main ok')
PY

if command -v systemctl >/dev/null 2>&1; then
  info "restarting $SERVICE_WEB"
  sudo systemctl restart "$SERVICE_WEB"
  info "waiting for health"
  for i in {1..30}; do
    if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null; then
      break
    fi
    sleep 2
    if [[ "$i" == 30 ]]; then
      fail "health check failed after web restart"
    fi
  done

  if [[ "$RESTART_WORKER" == "1" ]]; then
    info "restarting $SERVICE_WORKER"
    sudo systemctl restart "$SERVICE_WORKER"
  else
    info "skipping worker restart because RESTART_WORKER=$RESTART_WORKER"
  fi
else
  info "systemctl not available; deploy checkout/deps/import completed only"
fi

info "deploy ok: $prev_sha -> $new_sha"
