#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/degen/app}"
ENV_FILE="${ENV_FILE:-/opt/degen/.env}"
DATA_ROOT="${DATA_ROOT:-/opt/degen/data}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"

fail() { echo "preflight: ERROR: $*" >&2; exit 1; }
info() { echo "preflight: $*"; }

[[ -d "$APP_DIR" ]] || fail "APP_DIR missing: $APP_DIR"
[[ -f "$ENV_FILE" ]] || fail "ENV_FILE missing: $ENV_FILE"
[[ -f "$APP_DIR/app/main.py" ]] || fail "app/main.py missing under $APP_DIR"
[[ -x "$APP_DIR/.venv/bin/python" ]] || fail "venv python missing: $APP_DIR/.venv/bin/python"
[[ -d "$DATA_ROOT" ]] || fail "DATA_ROOT missing: $DATA_ROOT (run deploy.sh or create manually)"

info "python version: $($APP_DIR/.venv/bin/python --version)"
info "checking imports"
(cd "$APP_DIR" && "$APP_DIR/.venv/bin/python" - <<'PY'
import app.main
import app.worker_service
print('imports ok')
PY
)

if command -v systemctl >/dev/null 2>&1; then
  info "systemd available"
else
  info "systemd not available in this shell/container context"
fi

if command -v curl >/dev/null 2>&1; then
  info "curl available; health URL target is $HEALTH_URL"
fi

info "preflight ok"
