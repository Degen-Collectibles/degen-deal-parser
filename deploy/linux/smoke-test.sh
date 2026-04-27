#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

fail() { echo "smoke-test: ERROR: $*" >&2; exit 1; }
info() { echo "smoke-test: $*"; }

command -v curl >/dev/null 2>&1 || fail "curl is required"

info "checking $BASE_URL/health"
health_json="$(curl -fsS --max-time 10 "$BASE_URL/health")" || fail "health endpoint failed"
printf '%s\n' "$health_json"

HEALTH_JSON="$health_json" "$PYTHON_BIN" - <<'PY'
import json, os, sys
payload = json.loads(os.environ['HEALTH_JSON'])
if not payload.get('ok') or not payload.get('db_ok'):
    print(f"health not ok: {payload}", file=sys.stderr)
    sys.exit(1)
print('health ok')
PY

# Lightweight page checks. These may redirect to login; HTTP success/redirect is acceptable.
for path in / /login; do
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "$BASE_URL$path")"
  case "$code" in
    200|302|303) info "$path -> $code" ;;
    *) fail "$path returned HTTP $code" ;;
  esac
done

info "smoke test ok"
