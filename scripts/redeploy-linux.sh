#!/usr/bin/env bash
# Redeploy Degen on the Linux/Green host.
# Intended to be run by the GitHub Actions self-hosted runner after cutover.
set -Eeuo pipefail

APP_DIR="${DEGEN_APP_DIR:-/opt/degen/app}"
WEB_UNIT="${DEGEN_WEB_UNIT:-degen-web.service}"
WORKER_UNIT="${DEGEN_WORKER_UNIT:-degen-worker.service}"
BOT_UNIT="${DEGEN_OPS_DISCORD_BOT_UNIT:-degen-ops-discord-bot.service}"
BOT_SYSTEMD_SCOPE="${DEGEN_OPS_DISCORD_BOT_SYSTEMD_SCOPE:-user}"
RESTART_BOT="${DEGEN_OPS_DISCORD_BOT_RESTART:-1}"
HEALTH_URL="${DEGEN_HEALTH_URL:-http://127.0.0.1:8000/health}"
MAX_WAIT_SECONDS="${DEGEN_HEALTH_MAX_WAIT_SECONDS:-120}"
INTERVAL_SECONDS="${DEGEN_HEALTH_INTERVAL_SECONDS:-5}"
INSTALL_DEPS="${DEGEN_INSTALL_DEPS:-1}"
EXPECTED_GIT_SHA="${DEGEN_EXPECTED_GIT_SHA:-}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    exit 127
  }
}

require_bool() {
  local key="$1"
  local value="$2"
  if [[ "$value" != "true" && "$value" != "false" ]]; then
    echo "ERROR: $key must be true or false" >&2
    exit 2
  fi
}

require_cmd git
require_cmd systemctl
require_cmd curl

set_env_var() {
  local env_file="$1"
  local key="$2"
  local value="$3"
  if [[ ! -f "$env_file" ]]; then
    log "Env file $env_file not present; skipping $key sync"
    return 0
  fi

  # Keep secrets out of logs. This only prints the key/file, never values.
  log "Ensuring $key is set in $env_file"
  if [[ -w "$env_file" ]]; then
    python3 - "$env_file" "$key" "$value" <<'PYENV'
import os, sys
path, key, value = sys.argv[1:4]
with open(path, 'r', encoding='utf-8') as f:
    lines = f.read().splitlines()
out = []
seen = False
for line in lines:
    if line.startswith(f"{key}="):
        if not seen:
            out.append(f"{key}={value}")
            seen = True
        continue
    out.append(line)
if not seen:
    out.append(f"{key}={value}")
with open(path, 'w', encoding='utf-8') as f:
    f.write("\n".join(out) + "\n")
PYENV
  else
    sudo -n python3 - "$env_file" "$key" "$value" <<'PYENV'
import os, sys
path, key, value = sys.argv[1:4]
with open(path, 'r', encoding='utf-8') as f:
    lines = f.read().splitlines()
out = []
seen = False
for line in lines:
    if line.startswith(f"{key}="):
        if not seen:
            out.append(f"{key}={value}")
            seen = True
        continue
    out.append(line)
if not seen:
    out.append(f"{key}={value}")
with open(path, 'w', encoding='utf-8') as f:
    f.write("\n".join(out) + "\n")
PYENV
  fi
}

wait_for_systemd_unit() {
  local unit="$1"
  local elapsed=0
  local stable_seconds="${DEGEN_UNIT_STABLE_SECONDS:-10}"
  local state

  while (( elapsed <= MAX_WAIT_SECONDS )); do
    if systemctl is-active --quiet "$unit"; then
      sleep "$stable_seconds"
      elapsed=$((elapsed + stable_seconds))
      if systemctl is-active --quiet "$unit"; then
        log "$unit is active after ${elapsed}s"
        return 0
      fi
    fi

    state="$(systemctl is-active "$unit" 2>/dev/null || true)"
    log "$unit state at ${elapsed}s: ${state:-unknown}"
    sleep "$INTERVAL_SECONDS"
    elapsed=$((elapsed + INTERVAL_SECONDS))
  done

  echo "ERROR: $unit did not become active within ${MAX_WAIT_SECONDS}s" >&2
  sudo -n systemctl status "$unit" --no-pager || true
  return 1
}

bot_systemctl() {
  if [[ "$BOT_SYSTEMD_SCOPE" == "user" ]]; then
    # The system-service Actions runner does not inherit PAM's user-bus variables.
    # Reconnect to the same login user's lingering systemd manager when they are absent.
    local runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
    local bus_address="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${runtime_dir}/bus}"
    XDG_RUNTIME_DIR="$runtime_dir" \
      DBUS_SESSION_BUS_ADDRESS="$bus_address" \
      systemctl --user "$@"
  else
    sudo -n systemctl "$@"
  fi
}

bot_unit_installed() {
  local load_state

  if ! load_state="$(bot_systemctl show "$BOT_UNIT" -p LoadState --value)"; then
    echo "ERROR: unable to query Discord bot unit $BOT_UNIT ($BOT_SYSTEMD_SCOPE scope)" >&2
    return 2
  fi

  case "$load_state" in
    "loaded")
      return 0
      ;;
    "not-found")
      return 1
      ;;
    *)
      echo "ERROR: Discord bot unit $BOT_UNIT returned unexpected load state: ${load_state:-empty}" >&2
      return 2
      ;;
  esac
}

wait_for_bot_unit() {
  local unit="$1"
  local elapsed=0
  local stable_seconds="${DEGEN_UNIT_STABLE_SECONDS:-10}"
  local state

  while (( elapsed <= MAX_WAIT_SECONDS )); do
    if bot_systemctl is-active --quiet "$unit"; then
      sleep "$stable_seconds"
      elapsed=$((elapsed + stable_seconds))
      if bot_systemctl is-active --quiet "$unit"; then
        log "$unit is active after ${elapsed}s"
        return 0
      fi
    fi

    state="$(bot_systemctl is-active "$unit" 2>/dev/null || true)"
    log "$unit state at ${elapsed}s: ${state:-unknown}"
    sleep "$INTERVAL_SECONDS"
    elapsed=$((elapsed + INTERVAL_SECONDS))
  done

  echo "ERROR: $unit did not become active within ${MAX_WAIT_SECONDS}s" >&2
  bot_systemctl status "$unit" --no-pager || true
  return 1
}

restart_discord_bot() {
  local probe_status=0

  if [[ "$RESTART_BOT" == "0" ]]; then
    log "Discord bot restart disabled by DEGEN_OPS_DISCORD_BOT_RESTART=0"
    return 0
  fi

  bot_unit_installed || probe_status=$?
  case "$probe_status" in
    0)
      log "Restarting $BOT_UNIT ($BOT_SYSTEMD_SCOPE scope)"
      bot_systemctl restart "$BOT_UNIT"
      wait_for_bot_unit "$BOT_UNIT"
      ;;
    1)
      log "Discord bot unit $BOT_UNIT not installed in $BOT_SYSTEMD_SCOPE scope; skipping bot restart"
      ;;
    *)
      return "$probe_status"
      ;;
  esac
}

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "ERROR: APP_DIR is not a git checkout: $APP_DIR" >&2
  exit 2
fi

cd "$APP_DIR"

log "Starting Linux redeploy in $APP_DIR"
current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "main" ]]; then
  echo "ERROR: expected branch main, got $current_branch" >&2
  exit 3
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "ERROR: tracked production checkout changes must be resolved before deploy" >&2
  git status --short --untracked-files=no >&2
  exit 6
fi

if [[ -n "$EXPECTED_GIT_SHA" ]]; then
  if [[ ! "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: DEGEN_EXPECTED_GIT_SHA must be a 40-character lowercase Git SHA" >&2
    exit 7
  fi

  actual_sha="$(git rev-parse HEAD)"
  if [[ "$actual_sha" != "$EXPECTED_GIT_SHA" ]]; then
    echo "ERROR: expected checkout $EXPECTED_GIT_SHA, got $actual_sha" >&2
    exit 8
  fi
  log "Using workflow-synchronized checkout at $actual_sha"
else
  log "Fetching origin/main"
  git fetch origin main
  log "Rebasing onto origin/main"
  git pull --rebase origin main
fi

if [[ "$INSTALL_DEPS" != "0" ]]; then
  if [[ ! -x .venv/bin/pip ]]; then
    echo "ERROR: missing virtualenv pip at $APP_DIR/.venv/bin/pip" >&2
    exit 4
  fi
  log "Installing Python dependencies"
  .venv/bin/pip install -r requirements.txt
fi

mkdir -p logs
if id degen >/dev/null 2>&1; then
  log "Ensuring runtime log directories are writable by degen"
  sudo -n install -d -o degen -g degen -m 750 /var/log/degen
  sudo -n install -d -o degen -g degen -m 775 "$APP_DIR/logs"
  sudo -n chown -R degen:degen /var/log/degen
fi
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

# Keep production web/worker on the approved export-controlled model and stop
# periodic inference unless a deploy explicitly opts back in. Values are never
# printed because set_env_var logs only the key and target file.
PRIMARY_NVIDIA_MODEL="${DEGEN_PRIMARY_NVIDIA_MODEL:-us/azure/openai/eccn-gpt-5.5}"
PARSER_REPROCESS_VALUE="${DEGEN_PARSER_REPROCESS_ENABLED:-false}"
STITCH_AUDIT_VALUE="${DEGEN_PERIODIC_STITCH_AUDIT_ENABLED:-false}"
require_bool DEGEN_PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"
require_bool DEGEN_PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"

set_env_var /opt/degen/web.env NVIDIA_MODEL "$PRIMARY_NVIDIA_MODEL"
set_env_var /opt/degen/web.env PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"
set_env_var /opt/degen/web.env PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"
set_env_var /opt/degen/worker.env NVIDIA_MODEL "$PRIMARY_NVIDIA_MODEL"
set_env_var /opt/degen/worker.env PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"
set_env_var /opt/degen/worker.env PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"
# Older service templates used /opt/degen/.env; keep it aligned if present.
set_env_var /opt/degen/.env NVIDIA_MODEL "$PRIMARY_NVIDIA_MODEL"
set_env_var /opt/degen/.env PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"
set_env_var /opt/degen/.env PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"

log "Restarting $WEB_UNIT"
sudo -n systemctl restart "$WEB_UNIT"

if systemctl list-unit-files "$WORKER_UNIT" --no-legend 2>/dev/null | grep -q "^$WORKER_UNIT"; then
  log "Restarting $WORKER_UNIT"
  sudo -n systemctl restart "$WORKER_UNIT"
  wait_for_systemd_unit "$WORKER_UNIT"
else
  log "Worker unit $WORKER_UNIT not installed; skipping worker restart"
fi

restart_discord_bot

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
