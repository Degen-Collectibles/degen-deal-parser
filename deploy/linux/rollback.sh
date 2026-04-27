#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/degen/app}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
SERVICE_WEB="${SERVICE_WEB:-degen-web}"
SERVICE_WORKER="${SERVICE_WORKER:-degen-worker}"
STATE_DIR="${STATE_DIR:-/opt/degen/deploy/state}"
TARGET_SHA="${1:-}"

fail() { echo "rollback: ERROR: $*" >&2; exit 1; }
info() { echo "rollback: $*"; }

[[ -d "$APP_DIR/.git" ]] || fail "$APP_DIR is not a git checkout"
if [[ -z "$TARGET_SHA" ]]; then
  [[ -f "$STATE_DIR/previous_sha" ]] || fail "no target SHA argument and no $STATE_DIR/previous_sha"
  TARGET_SHA="$(cat "$STATE_DIR/previous_sha")"
fi

cd "$APP_DIR"
current_sha="$(git rev-parse HEAD)"
info "rolling back $current_sha -> $TARGET_SHA"
git fetch origin || true
git reset --hard "$TARGET_SHA"

if [[ -x .venv/bin/python && -f requirements.txt ]]; then
  .venv/bin/python -m pip install -r requirements.txt
fi

if command -v systemctl >/dev/null 2>&1; then
  sudo systemctl restart "$SERVICE_WEB"
  for i in {1..30}; do
    if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null; then
      break
    fi
    sleep 2
    if [[ "$i" == 30 ]]; then
      fail "health check failed after rollback"
    fi
  done
  sudo systemctl restart "$SERVICE_WORKER"
fi

info "rollback ok"
