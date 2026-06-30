# Green PostgreSQL backup operations runbook

Status date: **2026-06-29**. This runbook is specific to the Green production host and the repository-managed PostgreSQL backup assets. It does not authorize production work by itself.

## Policy and hard boundaries

The backup job publishes unique timestamped `.dump` and `.dump.sha256` pairs. A pair is complete only when both files exist; a new pair is accepted only after dump, checksum, remote size, and remote sidecar verification.

- Local retention keeps exactly the newest 2 recognized complete verified pairs; unknown, incomplete, manual, and temporary files remain protected and do not count toward those two.
- Remote retention keeps the union representing the newest 7 distinct UTC dates, 4 ISO weeks, and 3 months. Unknown, incomplete, manual, and temporary objects remain protected.
- `REMOTE_PRUNE_ENABLED=0` is mandatory for installation and dry-run review. Remote deletion is enabled only through the candidate gate below.
- Manual preservation belongs in `/opt/degen/backups/manual/`; configuration snapshots belong in `/opt/degen/backups/config/`. Both directories are excluded from the flat `/opt/degen/backups/db` inventory.
- Never overwrite the real environment file from the template. `/etc/degen/prod-db-backup.env` is edited and preserved in place as `root:root 0600`.
- Do not start or restart PostgreSQL, the web service, the worker, or the bot. Metadata reload is the only systemd manager mutation in this procedure.
- Do not run a manual full backup without Jeffrey's explicit approval. The only manual script modes permitted here are `preflight` and `remote-retention-dry-run`.
- Do not purge the OneDrive recycle bin. Provider recovery behavior is not a rollback plan.

## Repository-to-host mapping

| Reviewed repository asset | Green disposition |
|---|---|
| `deploy/linux/degen-prod-db-backup.sh` | `/usr/local/sbin/degen-prod-db-backup`, `root:root 0755` |
| `deploy/linux/degen-prod-db-retention.py` | `/usr/local/sbin/degen-prod-db-retention`, `root:root 0755` |
| `deploy/systemd/degen-prod-db-backup.service` | `/etc/systemd/system/degen-prod-db-backup.service`, `root:root 0644` |
| `deploy/systemd/degen-prod-db-backup.timer` | `/etc/systemd/system/degen-prod-db-backup.timer`, `root:root 0644` |
| `deploy/systemd/degen-prod-db-backup.env.example` | Reference only. Never copy it over the real `/etc/degen/prod-db-backup.env`; that host-owned file remains `root:root 0600`. |

The standard snapshot location is `/opt/degen/backups/config/<UTC timestamp>/`. The commands below generate that timestamp directly.

## Mandatory preflight and approval

Before any write, state this preflight and wait for Jeffrey's explicit `proceed`:

- **Exact targets:** the two `/usr/local/sbin/degen-prod-db-*` executables, the two `/etc/systemd/system/degen-prod-db-backup.*` units, selected policy keys in `/etc/degen/prod-db-backup.env`, and one new root-only snapshot directory.
- **What changes:** install reviewed bytes, leave pruning disabled, reload systemd metadata, and run non-mutating validation. Existing timer enablement is inspected, not changed.
- **Reversible effects:** installed local files, policy-key edits, and metadata reload can be restored from the snapshot.
- **Irreversible effects:** none before the flag gate. After the flag becomes `1`, remote deletion is potentially irreversible except for provider recycle behavior.
- **Rollback:** restore exact saved files and the real environment file, conditionally remove a first-install planner, and reload metadata.
- **Post-action verification:** hashes, owners, modes, next trigger, environment variable names only, unchanged MainPID values, Windows OneDrive remaining off, and rollback readiness.

Run this read-only inventory from the reviewed checkout. It produces no secret output and must not print environment values or rclone configuration content.

Do not run any rclone command before Jeffrey's explicit `proceed`. Even an ordinary rclone access command may refresh or rewrite `/etc/degen/rclone.conf` while rotating access credentials. Remote inventory is therefore deferred until that file has been captured in the approved root-only snapshot.

```bash
set -euo pipefail
cd /opt/degen/app
git status --short --branch
git rev-parse HEAD
sha256sum -- \
  deploy/linux/degen-prod-db-backup.sh \
  deploy/linux/degen-prod-db-retention.py \
  deploy/systemd/degen-prod-db-backup.service \
  deploy/systemd/degen-prod-db-backup.timer \
  deploy/systemd/degen-prod-db-backup.env.example

for target in \
  /usr/local/sbin/degen-prod-db-backup \
  /usr/local/sbin/degen-prod-db-retention \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env
do
  if sudo test -e "$target"; then
    sudo stat -c '%n owner=%U:%G mode=%a size=%s' -- "$target"
    sudo sha256sum -- "$target"
  else
    printf 'MISSING %s\n' "$target"
  fi
done

systemctl is-enabled degen-prod-db-backup.timer || true
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
systemctl show degen-prod-db-backup.timer \
  -p ActiveState -p SubState -p NextElapseUSecRealtime -p LastTriggerUSec
df -B1 /opt/degen/backups/db
sudo find /opt/degen/backups/db -mindepth 1 -maxdepth 1 -type f \
  -printf '%f\t%s bytes\n' | sort
```

Identify the active PostgreSQL unit instead of assuming that the umbrella unit owns the server process. Stop unless this produces exactly one active database unit. The bot check must run in the login session that owns its live user-systemd manager.

```bash
mapfile -t POSTGRES_UNITS < <(
  systemctl list-units --type=service --state=running --no-legend 'postgresql*.service' |
    awk '{print $1}'
)
test "${#POSTGRES_UNITS[@]}" -eq 1
POSTGRES_UNIT=${POSTGRES_UNITS[0]}
POSTGRES_PID_BEFORE=$(systemctl show "$POSTGRES_UNIT" -p MainPID --value)
WEB_PID_BEFORE=$(systemctl show degen-web.service -p MainPID --value)
WORKER_PID_BEFORE=$(systemctl show degen-worker.service -p MainPID --value)

BOT_UNIT=degen-ops-discord-bot.service
systemctl --user is-active "$BOT_UNIT"
systemctl --user list-unit-files "$BOT_UNIT" --no-legend
BOT_PID_BEFORE=$(systemctl --user show "$BOT_UNIT" -p MainPID --value)

printf 'PostgreSQL unit=%s MainPID=%s\n' "$POSTGRES_UNIT" "$POSTGRES_PID_BEFORE"
printf 'web MainPID=%s\nworker MainPID=%s\nbot unit=%s MainPID=%s\n' \
  "$WEB_PID_BEFORE" "$WORKER_PID_BEFORE" "$BOT_UNIT" "$BOT_PID_BEFORE"
test "$POSTGRES_PID_BEFORE" -gt 0
test "$WEB_PID_BEFORE" -gt 0
test "$WORKER_PID_BEFORE" -gt 0
test "$BOT_PID_BEFORE" -gt 0
```

If `degen-ops-discord-bot.service` is not confirmed there, stop and identify the real owning account, scope, and unit from live state. Do not guess or continue with a zero MainPID.

## Back up current host configuration

After approval and before installation, keep the same shell so the PID variables remain available. The current script, service, timer, real environment file, and rclone token configuration are required and copied exactly. A missing planner is expected on the first install and is recorded with a root-only explicit `.absent` marker; an existing planner is saved instead. The two planner states are mutually exclusive.

```bash
set -euo pipefail
umask 077
CONFIG_BACKUP_DIR="/opt/degen/backups/config/$(date -u +%Y%m%dT%H%M%SZ)"
sudo test ! -e "$CONFIG_BACKUP_DIR"
sudo mkdir -m 0700 -- "$CONFIG_BACKUP_DIR"
sudo chown root:root -- "$CONFIG_BACKUP_DIR"

for required in \
  /usr/local/sbin/degen-prod-db-backup \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env \
  /etc/degen/rclone.conf
do
  sudo test -f "$required"
  sudo test ! -L "$required"
done

sudo sha256sum -- \
  /usr/local/sbin/degen-prod-db-backup \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env \
  /etc/degen/rclone.conf |
  sudo tee "$CONFIG_BACKUP_DIR/preinstall-source-files.sha256" >/dev/null
sudo cp -a -- /usr/local/sbin/degen-prod-db-backup "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/systemd/system/degen-prod-db-backup.service "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/systemd/system/degen-prod-db-backup.timer "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/degen/prod-db-backup.env "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/degen/rclone.conf "$CONFIG_BACKUP_DIR/rclone.conf.audit"
sudo chown root:root -- "$CONFIG_BACKUP_DIR/rclone.conf.audit"
sudo chmod 0600 -- "$CONFIG_BACKUP_DIR/rclone.conf.audit"
sudo sha256sum -- /etc/degen/rclone.conf |
  sudo tee "$CONFIG_BACKUP_DIR/rclone-before.sha256" >/dev/null
sudo stat -c 'mtime_epoch=%Y mtime=%y owner=%U:%G mode=%a size=%s' \
  /etc/degen/rclone.conf |
  sudo tee "$CONFIG_BACKUP_DIR/rclone-before.stat" >/dev/null
if sudo test -e /usr/local/sbin/degen-prod-db-retention; then
  sudo test -f /usr/local/sbin/degen-prod-db-retention
  sudo test ! -L /usr/local/sbin/degen-prod-db-retention
  sudo sha256sum -- /usr/local/sbin/degen-prod-db-retention |
    sudo tee -a "$CONFIG_BACKUP_DIR/preinstall-source-files.sha256" >/dev/null
  sudo cp -a -- /usr/local/sbin/degen-prod-db-retention "$CONFIG_BACKUP_DIR/"
else
  sudo install -o root -g root -m 0600 /dev/null \
    "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent"
fi

printf 'PostgreSQL unit=%s MainPID=%s\nweb MainPID=%s\nworker MainPID=%s\nbot unit=%s MainPID=%s\n' \
  "$POSTGRES_UNIT" "$POSTGRES_PID_BEFORE" "$WEB_PID_BEFORE" "$WORKER_PID_BEFORE" \
  "$BOT_UNIT" "$BOT_PID_BEFORE" |
  sudo tee "$CONFIG_BACKUP_DIR/mainpids.before" >/dev/null
sudo find "$CONFIG_BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f \
  -printf '%f owner=%u:%g mode=%m size=%s\n' |
  sudo tee "$CONFIG_BACKUP_DIR/backup-files.stat" >/dev/null
sudo find "$CONFIG_BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f \
  -exec chmod 0600 -- {} +
sudo sh -c 'cd "$1" && find . -mindepth 1 -maxdepth 1 -type f ! -name SHA256SUMS -printf "%P\0" | LC_ALL=C sort -z | xargs -0 sha256sum -- > SHA256SUMS' \
  sh "$CONFIG_BACKUP_DIR"
sudo chmod 0600 "$CONFIG_BACKUP_DIR/SHA256SUMS"
sudo sh -c 'cd "$1" && sha256sum -c SHA256SUMS' sh "$CONFIG_BACKUP_DIR"
sudo test "$(sudo stat -c '%U:%G:%a' "$CONFIG_BACKUP_DIR")" = 'root:root:700'
```

`rclone.conf.audit`, its before-use hash, and its mtime are audit and emergency-recovery evidence. Do not automatically restore `rclone.conf.audit`: a refresh may have rotated credentials and made the older token configuration unusable. Any recovery from that copy requires a separate explicit decision after current authentication state is understood.

## Install reviewed bytes and update only policy keys

Install executables as `root:root 0755` and units as `root:root 0644`. Do not install the example over the real environment file.

```bash
set -euo pipefail
cd /opt/degen/app
sudo install -o root -g root -m 0755 \
  deploy/linux/degen-prod-db-backup.sh \
  /usr/local/sbin/degen-prod-db-backup
sudo install -o root -g root -m 0755 \
  deploy/linux/degen-prod-db-retention.py \
  /usr/local/sbin/degen-prod-db-retention
sudo install -o root -g root -m 0644 \
  deploy/systemd/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.service
sudo install -o root -g root -m 0644 \
  deploy/systemd/degen-prod-db-backup.timer \
  /etc/systemd/system/degen-prod-db-backup.timer
```

The orchestrator accepts a validated `BACKUP_PREFIX` override. Derive that non-sensitive namespace from the newest existing complete pair whose sidecar exactly matches its dump. An incomplete or checksum-invalid pair is not evidence. Stop if no verified complete pair exists or if the newest timestamp is ambiguous across prefixes. This prints only the safe prefix, never environment contents.

```bash
set -euo pipefail
BACKUP_PREFIX=$(
  sudo env BACKUP_DIR=/opt/degen/backups/db bash <<'BASH'
# BEGIN INSTALL_PREFIX_DERIVATION
set -uo pipefail
shopt -s nullglob
prefix_candidates=()
for dump_path in "$BACKUP_DIR"/*.dump; do
  [[ -f "$dump_path" && ! -L "$dump_path" ]] || continue
  dump_name=${dump_path##*/}
  if [[ "$dump_name" =~ ^([A-Za-z0-9._-]+)([0-9]{8}T[0-9]{6}Z)\.dump$ ]]; then
    candidate_prefix=${BASH_REMATCH[1]}
    candidate_stamp=${BASH_REMATCH[2]}
  else
    continue
  fi
  sidecar_path="$dump_path.sha256"
  [[ -f "$sidecar_path" && ! -L "$sidecar_path" ]] || continue
  sidecar_line=$(<"$sidecar_path") || continue
  checksum_output=$(sha256sum -- "$dump_path") || continue
  checksum=${checksum_output%% *}
  [[ "$sidecar_line" == "$checksum  $dump_name" ]] || continue
  prefix_candidates+=("$candidate_stamp"$'\t'"$candidate_prefix")
done
if (( ${#prefix_candidates[@]} == 0 )); then
  printf '%s\n' 'ERROR: no verified complete backup pair exists for prefix derivation' >&2
  exit 1
fi
mapfile -t sorted_prefix_candidates < <(
  printf '%s\n' "${prefix_candidates[@]}" | LC_ALL=C sort -r
)
IFS=$'\t' read -r newest_stamp BACKUP_PREFIX <<< "${sorted_prefix_candidates[0]}"
[[ "$BACKUP_PREFIX" =~ ^[A-Za-z0-9._-]+$ ]] || {
  printf '%s\n' 'ERROR: derived backup prefix is unsafe' >&2
  exit 1
}
for candidate in "${sorted_prefix_candidates[@]}"; do
  IFS=$'\t' read -r candidate_stamp candidate_prefix <<< "$candidate"
  [[ "$candidate_stamp" == "$newest_stamp" ]] || break
  if [[ "$candidate_prefix" != "$BACKUP_PREFIX" ]]; then
    printf '%s\n' 'ERROR: newest verified backup timestamp has multiple prefixes' >&2
    exit 1
  fi
done
printf '%s\n' "$BACKUP_PREFIX"
# END INSTALL_PREFIX_DERIVATION
BASH
)
[[ "$BACKUP_PREFIX" =~ ^[A-Za-z0-9._-]+$ ]]
```

This exact editor preserves unrelated lines and comments byte-for-byte where possible and adds or replaces only the new non-sensitive policy keys, including the derived `BACKUP_PREFIX`. For managed keys it follows systemd `EnvironmentFile` assignment semantics: leading horizontal whitespace and whitespace immediately before `=` do not create a different key. Every semantic duplicate is removed, exactly one canonical `KEY=value` remains, and malformed managed-key lines fail closed. It also refuses a symlink, incorrect ownership/mode, an unsafe prefix, or an existing temporary path. `REMOTE_PRUNE_ENABLED=0` remains the initial state.

```bash
sudo env BACKUP_PREFIX="$BACKUP_PREFIX" python3 - <<'PY'
# BEGIN INSTALL_MANAGED_ENV_NORMALIZATION
from pathlib import Path
import os
import re
import stat

path = Path(os.environ.get("BACKUP_ENV_FILE", "/etc/degen/prod-db-backup.env"))
temporary = path.with_name(path.name + ".retention-update")
backup_prefix = os.environ.get("BACKUP_PREFIX", "")
if re.fullmatch(r"[A-Za-z0-9._-]+", backup_prefix) is None:
    raise SystemExit("derived backup prefix is unsafe")
updates = {
    "BACKUP_PREFIX": backup_prefix,
    "KEEP_LOCAL_COUNT": "2",
    "KEEP_REMOTE_DAILY": "7",
    "KEEP_REMOTE_WEEKLY": "4",
    "KEEP_REMOTE_MONTHLY": "3",
    "REMOTE_PRUNE_ENABLED": "0",
    "MIN_FREE_AFTER_BYTES": "10737418240",
    "RETENTION_PLANNER": "/usr/local/sbin/degen-prod-db-retention",
    "LOCK_FILE": "/run/lock/degen-prod-db-backup.lock",
}
assignment = re.compile(r"^[ \t]*(?P<key>[A-Za-z_][A-Za-z0-9_]*)[ \t]*=(?P<value>.*)$")


def body_and_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def semantic_assignment(line: str) -> tuple[str, str] | None:
    body, _ = body_and_ending(line)
    stripped = body.lstrip(" \t")
    if not stripped or stripped.startswith(("#", ";")):
        return None
    match = assignment.fullmatch(body)
    if match is not None:
        return match.group("key"), match.group("value")
    token = re.match(r"^(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)", stripped)
    if token is not None and token.group(1) in updates:
        raise SystemExit(f"malformed managed environment assignment: {token.group(1)}")
    return None


def validate_canonical(lines: list[str]) -> None:
    found: dict[str, list[str]] = {key: [] for key in updates}
    for line in lines:
        parsed = semantic_assignment(line)
        if parsed is not None and parsed[0] in found:
            found[parsed[0]].append(body_and_ending(line)[0])
    for key, value in updates.items():
        if found[key] != [f"{key}={value}"]:
            raise SystemExit(f"managed environment key was not canonicalized exactly once: {key}")


if path.is_symlink() or not path.is_file():
    raise SystemExit("real environment file must be a regular file")
metadata = path.stat()
if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit("real environment file must remain root:root 0600")
if temporary.exists() or temporary.is_symlink():
    raise SystemExit("refusing a pre-existing policy-update temporary path")

with path.open("r", encoding="utf-8", newline="") as handle:
    lines = handle.readlines()
newline = next((ending for line in lines if (ending := body_and_ending(line)[1])), "\n")
output = []
seen = set()
for line in lines:
    parsed = semantic_assignment(line)
    if parsed is not None and parsed[0] in updates:
        key = parsed[0]
        if key not in seen:
            output.append(f"{key}={updates[key]}{newline}")
            seen.add(key)
        continue
    output.append(line)
for key, value in updates.items():
    if key not in seen:
        if output and not output[-1].endswith(("\n", "\r")):
            output[-1] += newline
        output.append(f"{key}={value}{newline}")

validate_canonical(output)

descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
    handle.writelines(output)
    handle.flush()
    os.fsync(handle.fileno())
os.chown(temporary, 0, 0)
os.chmod(temporary, 0o600)
with temporary.open("r", encoding="utf-8", newline="") as handle:
    validate_canonical(handle.readlines())
os.replace(temporary, path)
directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
metadata = path.stat()
if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit("real environment file metadata changed unexpectedly")
with path.open("r", encoding="utf-8", newline="") as handle:
    validate_canonical(handle.readlines())
# END INSTALL_MANAGED_ENV_NORMALIZATION
PY
sudo test "$(sudo stat -c '%U:%G:%a' /etc/degen/prod-db-backup.env)" = 'root:root:600'
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' /etc/degen/prod-db-backup.env | sort
```

The last command displays environment variable names only. Never inspect, diff, log, or paste the file's values.

## Validate without running a backup

Validate unit syntax, reload metadata, and confirm the existing timer remains enabled with the intended next trigger. Do not invoke the service and do not use `--now`.

```bash
set -euo pipefail
sudo systemd-analyze verify \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer
sudo systemctl daemon-reload
systemctl is-enabled degen-prod-db-backup.timer
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
systemctl show degen-prod-db-backup.timer \
  -p ActiveState -p SubState -p NextElapseUSecRealtime -p LastTriggerUSec

REVIEW_DIR="$CONFIG_BACKUP_DIR/review"
sudo test ! -e "$REVIEW_DIR"
sudo mkdir -m 0700 -- "$REVIEW_DIR"
sudo chown root:root -- "$REVIEW_DIR"
sudo /usr/local/sbin/degen-prod-db-backup preflight |
  sudo tee "$REVIEW_DIR/preflight.txt"
sudo /usr/local/sbin/degen-prod-db-backup remote-retention-dry-run |
  sudo tee "$REVIEW_DIR/remote-retention-dry-run.txt"
sudo sha256sum -- /etc/degen/rclone.conf |
  sudo tee "$REVIEW_DIR/rclone-after.sha256" >/dev/null
sudo stat -c 'mtime_epoch=%Y mtime=%y owner=%U:%G mode=%a size=%s' \
  /etc/degen/rclone.conf |
  sudo tee "$REVIEW_DIR/rclone-after.stat" >/dev/null
sudo sh -c '
  before_hash=$(awk "NR == 1 {print \$1}" "$1/rclone-before.sha256")
  after_hash=$(awk "NR == 1 {print \$1}" "$2/rclone-after.sha256")
  before_mtime=$(sed -n "s/^mtime_epoch=\([0-9][0-9]*\).*/\1/p" "$1/rclone-before.stat")
  after_mtime=$(sed -n "s/^mtime_epoch=\([0-9][0-9]*\).*/\1/p" "$2/rclone-after.stat")
  test -n "$before_hash" && test -n "$after_hash" && test -n "$before_mtime" && test -n "$after_mtime"
  test "$before_hash" = "$after_hash" && hash_changed=no || hash_changed=yes
  test "$before_mtime" = "$after_mtime" && mtime_changed=no || mtime_changed=yes
  printf "hash_changed=%s mtime_changed=%s\n" "$hash_changed" "$mtime_changed" > "$2/rclone-change.txt"
' sh "$CONFIG_BACKUP_DIR" "$REVIEW_DIR"
sudo chmod 0600 "$REVIEW_DIR"/rclone-after.sha256 \
  "$REVIEW_DIR"/rclone-after.stat "$REVIEW_DIR"/rclone-change.txt
```

`remote-retention-dry-run` does not delete even when REMOTE_PRUNE_ENABLED=1; it also does not create a dump. The host flag must still be `0` here.

Review `rclone-change.txt` to record whether the hash and mtime changed during remote preflight/dry-run. A change is evidence of credential refresh, not permission to restore the audit copy.

## Exact dry-run candidate review gate

Extract exact dry-run objects and independently regenerate the keep/delete/protected plan from the complete remote inventory:

```bash
set -euo pipefail
sudo sed -n 's/^.*Remote retention dry run: would delete //p' \
  "$REVIEW_DIR/remote-retention-dry-run.txt" |
  sudo tee "$REVIEW_DIR/remote-retention-candidates.txt" >/dev/null
sudo rclone --config /etc/degen/rclone.conf lsf \
  onedrive:backups/degen-db --files-only --max-depth 1 | sort |
  sudo tee "$REVIEW_DIR/remote-inventory.txt" >/dev/null
BACKUP_PREFIX=$(sudo sed -n 's/^.*Preflight passed for mode=remote-retention-dry-run prefix=//p' \
  "$REVIEW_DIR/remote-retention-dry-run.txt" | tail -n 1)
test -n "$BACKUP_PREFIX"
REVIEW_NOW=$(date -u +%Y%m%dT%H%M%SZ)
sudo sh -c '/usr/local/sbin/degen-prod-db-retention --mode remote --prefix "$1" --now "$2" --daily 7 --weekly 4 --monthly 3 --format json < "$3" > "$4"' \
  sh "$BACKUP_PREFIX" "$REVIEW_NOW" \
  "$REVIEW_DIR/remote-inventory.txt" \
  "$REVIEW_DIR/remote-retention-plan.json"
sudo python3 - "$REVIEW_DIR" <<'PY'
from pathlib import Path
import json
import sys

root = Path(sys.argv[1])
inventory = set((root / "remote-inventory.txt").read_text(encoding="utf-8").splitlines())
candidates = (root / "remote-retention-candidates.txt").read_text(encoding="utf-8").splitlines()
plan = json.loads((root / "remote-retention-plan.json").read_text(encoding="utf-8"))
planned_delete = {
    name
    for record in plan["delete"]
    for name in (record["dump"], record["checksum"])
}
keep = {
    name
    for record in plan["keep"]
    for name in (record["dump"], record["checksum"])
}
if len(candidates) != len(set(candidates)) or set(candidates) != planned_delete:
    raise SystemExit("dry-run candidates differ from independent plan")
if not set(candidates) <= inventory or set(candidates) & keep:
    raise SystemExit("candidate absent from inventory or inside keep set")
for name in candidates:
    counterpart = name.removesuffix(".sha256") if name.endswith(".sha256") else name + ".sha256"
    if counterpart not in candidates:
        raise SystemExit(f"candidate is not a complete pair: {name}")
print(f"candidate_objects={len(candidates)} candidate_pairs={len(candidates) // 2}")
PY
sudo cat "$REVIEW_DIR/remote-retention-candidates.txt"
```

Do not cross this gate until every candidate is manually recognized as an automation-owned complete pair and is outside the keep set in `remote-retention-plan.json`. Protected objects must never appear as candidates. Record the decision beside the snapshot.

If there are zero candidates, record `zero candidates` in the review evidence. Zero candidates is not approval: future scheduled runs can still produce deletable objects. If candidates exist, record the reviewed object list and keep-set comparison. In both cases, leave `REMOTE_PRUNE_ENABLED=0` unchanged and stop.

Both zero-candidate and nonzero-candidate reviews require explicit Jeffrey/operator approval before remote deletion may be enabled. Only after that approval is recorded may the operator run the gated edit below. It recognizes all whitespace-equivalent systemd assignments, requires every effective pre-edit value to be `0`, removes duplicates, and writes exactly one canonical `REMOTE_PRUNE_ENABLED=1` line while preserving unrelated content:

```bash
sudo python3 - <<'PY'
# BEGIN PRUNE_FLAG_NORMALIZATION
from pathlib import Path
import os
import re
import stat

path = Path(os.environ.get("BACKUP_ENV_FILE", "/etc/degen/prod-db-backup.env"))
temporary = path.with_name(path.name + ".prune-enable")
managed_key = "REMOTE_PRUNE_ENABLED"
assignment = re.compile(r"^[ \t]*(?P<key>[A-Za-z_][A-Za-z0-9_]*)[ \t]*=(?P<value>.*)$")


def body_and_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def semantic_assignment(line: str) -> tuple[str, str] | None:
    body, _ = body_and_ending(line)
    stripped = body.lstrip(" \t")
    if not stripped or stripped.startswith(("#", ";")):
        return None
    match = assignment.fullmatch(body)
    if match is not None:
        return match.group("key"), match.group("value")
    token = re.match(r"^(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)", stripped)
    if token is not None and token.group(1) == managed_key:
        raise SystemExit("malformed REMOTE_PRUNE_ENABLED assignment")
    return None


if path.is_symlink() or not path.is_file():
    raise SystemExit("real environment file must be a regular file")
metadata = path.stat()
if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit("real environment file must remain root:root 0600")
if temporary.exists() or temporary.is_symlink():
    raise SystemExit("refusing a pre-existing prune-enable temporary path")

with path.open("r", encoding="utf-8", newline="") as handle:
    lines = handle.readlines()
newline = next((ending for line in lines if (ending := body_and_ending(line)[1])), "\n")
output = []
disabled_values = []
emitted = False
for line in lines:
    parsed = semantic_assignment(line)
    if parsed is not None and parsed[0] == managed_key:
        disabled_values.append(parsed[1].strip(" \t"))
        if not emitted:
            output.append(f"{managed_key}=1{newline}")
            emitted = True
        continue
    output.append(line)
if not disabled_values or any(value != "0" for value in disabled_values):
    raise SystemExit("every systemd-effective REMOTE_PRUNE_ENABLED entry must be disabled before approval")
if [body_and_ending(line)[0] for line in output if (parsed := semantic_assignment(line)) is not None and parsed[0] == managed_key] != [f"{managed_key}=1"]:
    raise SystemExit("REMOTE_PRUNE_ENABLED was not canonicalized exactly once")

descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
    handle.writelines(output)
    handle.flush()
    os.fsync(handle.fileno())
os.chown(temporary, 0, 0)
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
metadata = path.stat()
if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit("real environment file metadata changed unexpectedly")
with path.open("r", encoding="utf-8", newline="") as handle:
    final_lines = handle.readlines()
effective = [body_and_ending(line)[0] for line in final_lines if (parsed := semantic_assignment(line)) is not None and parsed[0] == managed_key]
if effective != [f"{managed_key}=1"]:
    raise SystemExit("REMOTE_PRUNE_ENABLED post-write validation failed")
# END PRUNE_FLAG_NORMALIZATION
PY
sudo test "$(sudo stat -c '%U:%G:%a' /etc/degen/prod-db-backup.env)" = 'root:root:600'
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' /etc/degen/prod-db-backup.env | sort
```

Do not run another script mode after the flag change. Let the next scheduled timer activation exercise the full path. No OneDrive recycle bin purge is part of this procedure.

## Immediate post-install verification

Verify installed bytes and modes against the reviewed checkout, confirm the correct next trigger (03:15 America/Los_Angeles plus at most 20 minutes randomized delay), and compare all process IDs in the same shell:

```bash
set -euo pipefail
cd /opt/degen/app
sha256sum -- \
  deploy/linux/degen-prod-db-backup.sh \
  deploy/linux/degen-prod-db-retention.py \
  deploy/systemd/degen-prod-db-backup.service \
  deploy/systemd/degen-prod-db-backup.timer
sudo sha256sum -- \
  /usr/local/sbin/degen-prod-db-backup \
  /usr/local/sbin/degen-prod-db-retention \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer
sudo stat -c '%n owner=%U:%G mode=%a' -- \
  /usr/local/sbin/degen-prod-db-backup \
  /usr/local/sbin/degen-prod-db-retention \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env
systemctl is-enabled degen-prod-db-backup.timer
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
systemctl show degen-prod-db-backup.timer -p NextElapseUSecRealtime -p LastTriggerUSec
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' /etc/degen/prod-db-backup.env | sort

POSTGRES_PID_AFTER=$(systemctl show "$POSTGRES_UNIT" -p MainPID --value)
WEB_PID_AFTER=$(systemctl show degen-web.service -p MainPID --value)
WORKER_PID_AFTER=$(systemctl show degen-worker.service -p MainPID --value)
BOT_PID_AFTER=$(systemctl --user show "$BOT_UNIT" -p MainPID --value)
test "$POSTGRES_PID_AFTER" = "$POSTGRES_PID_BEFORE"
test "$WEB_PID_AFTER" = "$WEB_PID_BEFORE"
test "$WORKER_PID_AFTER" = "$WORKER_PID_BEFORE"
test "$BOT_PID_AFTER" = "$BOT_PID_BEFORE"
printf 'unchanged MainPID values: PostgreSQL=%s web=%s worker=%s bot=%s\n' \
  "$POSTGRES_PID_AFTER" "$WEB_PID_AFTER" "$WORKER_PID_AFTER" "$BOT_PID_AFTER"

sudo sh -c 'cd "$1" && sha256sum -c SHA256SUMS' sh "$CONFIG_BACKUP_DIR"
```

On the Windows workstation, confirm OneDrive stays off before and after this Green-only operation. Do not enable the client, sync this backup directory, or use it to inspect remote backup content. Record that operational check with the rollback-ready snapshot path.

## Required observation after the next scheduled run

Do not claim success before this scheduled observation. After the next scheduled run has actually fired, perform every check below:

```bash
set -euo pipefail
REVIEW_NOW=$(date -u +%Y%m%dT%H%M%SZ)
OBSERVATION_DIR="/opt/degen/backups/config/${REVIEW_NOW}-scheduled-observation"
sudo test ! -e "$OBSERVATION_DIR"
sudo mkdir -m 0700 -- "$OBSERVATION_DIR"
sudo chown root:root -- "$OBSERVATION_DIR"

BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env
BACKUP_PREFIX=$(
  sudo env BACKUP_ENV_FILE="$BACKUP_ENV_FILE" bash <<'BASH'
# BEGIN FRESH_SHELL_PREFIX_RETRIEVAL
set -uo pipefail
mapfile -t persisted_prefixes < <(
  awk -F= '$1 == "BACKUP_PREFIX" { print substr($0, index($0, "=") + 1) }' \
    "$BACKUP_ENV_FILE"
)
if (( ${#persisted_prefixes[@]} != 1 )); then
  printf '%s\n' 'ERROR: expected exactly one persisted BACKUP_PREFIX entry' >&2
  exit 1
fi
BACKUP_PREFIX=${persisted_prefixes[0]}
if [[ ! "$BACKUP_PREFIX" =~ ^[A-Za-z0-9._-]+$ ]]; then
  printf '%s\n' 'ERROR: persisted backup prefix is unsafe' >&2
  exit 1
fi
printf '%s\n' "$BACKUP_PREFIX"
# END FRESH_SHELL_PREFIX_RETRIEVAL
BASH
)
[[ "$BACKUP_PREFIX" =~ ^[A-Za-z0-9._-]+$ ]]

BACKUP_DIR=/opt/degen/backups/db
RETENTION_PLANNER=/usr/local/sbin/degen-prod-db-retention
sudo env \
  SYSTEMCTL_BIN=/usr/bin/systemctl \
  JOURNALCTL_BIN=/usr/bin/journalctl \
  DATE_BIN=/usr/bin/date \
  STAT_BIN=/usr/bin/stat \
  SERVICE_UNIT=degen-prod-db-backup.service \
  TIMER_UNIT=degen-prod-db-backup.timer \
  BACKUP_DIR="$BACKUP_DIR" \
  BACKUP_PREFIX="$BACKUP_PREFIX" \
  bash <<'BASH' | sudo tee "$OBSERVATION_DIR/freshness-gate.txt"
# BEGIN POST_RUN_FRESHNESS_GATE
set -euo pipefail

read_property() {
  local property=$1
  local source=$2
  local -a values=()
  mapfile -t values < <(printf '%s\n' "$source" | sed -n "s/^${property}=//p")
  if (( ${#values[@]} != 1 )) || [[ -z "${values[0]}" ]]; then
    printf 'ERROR: expected one nonempty %s property\n' "$property" >&2
    exit 1
  fi
  printf '%s\n' "${values[0]}"
}

service_properties=$("$SYSTEMCTL_BIN" show "$SERVICE_UNIT" --no-pager \
  -p Result -p ExecMainCode -p ExecMainStatus -p ExecMainStartTimestamp)
service_result=$(read_property Result "$service_properties")
service_code=$(read_property ExecMainCode "$service_properties")
service_status=$(read_property ExecMainStatus "$service_properties")
service_start=$(read_property ExecMainStartTimestamp "$service_properties")
[[ "$service_result" == success ]] || { printf '%s\n' 'ERROR: latest backup service Result is not success' >&2; exit 1; }
[[ "$service_code" == exited ]] || { printf '%s\n' 'ERROR: latest backup service ExecMainCode is not exited' >&2; exit 1; }
[[ "$service_status" == 0 ]] || { printf '%s\n' 'ERROR: latest backup service ExecMainStatus is not zero' >&2; exit 1; }

timer_properties=$("$SYSTEMCTL_BIN" show "$TIMER_UNIT" --no-pager -p LastTriggerUSec)
last_trigger=$(read_property LastTriggerUSec "$timer_properties")
service_start_epoch=$("$DATE_BIN" --date="$service_start" +%s)
last_trigger_epoch=$("$DATE_BIN" --date="$last_trigger" +%s)
trigger_to_start_seconds=$((service_start_epoch - last_trigger_epoch))
if (( trigger_to_start_seconds < 0 || trigger_to_start_seconds > 300 )); then
  printf '%s\n' 'ERROR: latest service start does not correspond to the timer LastTrigger' >&2
  exit 1
fi

service_journal=$("$JOURNALCTL_BIN" -u "$SERVICE_UNIT" --since "$service_start" --no-pager -o cat)
if ! grep -Fq 'Backup completed successfully' <<< "$service_journal"; then
  printf '%s\n' 'ERROR: no backup success log exists since the exact service start' >&2
  exit 1
fi

shopt -s nullglob
recognized_pairs=()
for dump_path in "$BACKUP_DIR"/"$BACKUP_PREFIX"*.dump; do
  [[ -f "$dump_path" && ! -L "$dump_path" ]] || continue
  dump_name=${dump_path##*/}
  remainder=${dump_name#"$BACKUP_PREFIX"}
  [[ "$remainder" =~ ^([0-9]{8}T[0-9]{6}Z)\.dump$ ]] || continue
  sidecar_path="$dump_path.sha256"
  [[ -f "$sidecar_path" && ! -L "$sidecar_path" ]] || continue
  recognized_pairs+=("${BASH_REMATCH[1]}"$'\t'"$dump_name")
done
if (( ${#recognized_pairs[@]} == 0 )); then
  printf '%s\n' 'ERROR: no recognized complete local backup pair exists' >&2
  exit 1
fi
mapfile -t sorted_pairs < <(printf '%s\n' "${recognized_pairs[@]}" | LC_ALL=C sort -r)
IFS=$'\t' read -r newest_stamp newest_dump <<< "${sorted_pairs[0]}"
dump_mtime=$("$STAT_BIN" -c %Y -- "$BACKUP_DIR/$newest_dump")
sidecar_mtime=$("$STAT_BIN" -c %Y -- "$BACKUP_DIR/$newest_dump.sha256")
for artifact_mtime in "$dump_mtime" "$sidecar_mtime"; do
  [[ "$artifact_mtime" =~ ^[0-9]+$ ]] || { printf '%s\n' 'ERROR: backup artifact mtime is invalid' >&2; exit 1; }
  if (( artifact_mtime < service_start_epoch || artifact_mtime < last_trigger_epoch )); then
    printf '%s\n' 'ERROR: newest backup pair is stale relative to the latest scheduled service' >&2
    exit 1
  fi
done
printf 'service_start=%s last_trigger=%s fresh_backup=%s dump_mtime=%s sidecar_mtime=%s\n' \
  "$service_start" "$last_trigger" "$newest_dump" "$dump_mtime" "$sidecar_mtime"
# END POST_RUN_FRESHNESS_GATE
BASH

sudo journalctl -u degen-prod-db-backup.service -n 200 --no-pager |
  sudo tee "$OBSERVATION_DIR/service-journal.txt"
systemctl show degen-prod-db-backup.service \
  -p Result -p ExecMainCode -p ExecMainStatus -p ExecMainStartTimestamp -p InactiveEnterTimestamp |
  sudo tee "$OBSERVATION_DIR/service-result.txt"
systemctl show degen-prod-db-backup.timer -p LastTriggerUSec |
  sudo tee "$OBSERVATION_DIR/timer-trigger.txt"
sudo tail -n 200 /var/log/degen/prod-db-backup.log |
  sudo tee "$OBSERVATION_DIR/backup-log-tail.txt"
systemctl list-timers --all degen-prod-db-backup.timer --no-pager

df -B1 "$BACKUP_DIR" | sudo tee "$OBSERVATION_DIR/disk-free.txt"
sudo find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f \
  -printf '%f\t%s bytes\n' | LC_ALL=C sort |
  sudo tee "$OBSERVATION_DIR/local-files.txt"

sudo env BACKUP_DIR="$BACKUP_DIR" BACKUP_PREFIX="$BACKUP_PREFIX" bash <<'BASH' |
  sudo tee "$OBSERVATION_DIR/local-checksums.txt"
# BEGIN POST_RUN_CHECKSUM_VERIFICATION
set -uo pipefail
checksum_status=0
mapfile -d '' -t checksum_sidecars < <(
  find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f \
    -name "${BACKUP_PREFIX}[0-9]*T[0-9]*Z.dump.sha256" -print0 |
    LC_ALL=C sort -z
)
if (( ${#checksum_sidecars[@]} == 0 )); then
  printf '%s\n' 'ERROR: no recognized checksum sidecars were found' >&2
  checksum_status=1
fi
for sidecar_path in "${checksum_sidecars[@]}"; do
  sidecar_name=${sidecar_path##*/}
  if ! (cd "$BACKUP_DIR" && sha256sum -c -- "$sidecar_name"); then
    checksum_status=1
  fi
done
exit "$checksum_status"
# END POST_RUN_CHECKSUM_VERIFICATION
BASH

LOCAL_INVENTORY_FILE="$OBSERVATION_DIR/local-inventory.txt"
REMOTE_INVENTORY_FILE="$OBSERVATION_DIR/remote-inventory.txt"
LOCAL_PLAN_FILE="$OBSERVATION_DIR/local-retention-plan.json"
REMOTE_PLAN_FILE="$OBSERVATION_DIR/remote-retention-plan.json"
sudo find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' |
  LC_ALL=C sort | sudo tee "$LOCAL_INVENTORY_FILE" >/dev/null
sudo rclone --config /etc/degen/rclone.conf lsf \
  onedrive:backups/degen-db --files-only --max-depth 1 |
  LC_ALL=C sort | sudo tee "$REMOTE_INVENTORY_FILE" >/dev/null

sudo env \
  RETENTION_PLANNER="$RETENTION_PLANNER" \
  BACKUP_PREFIX="$BACKUP_PREFIX" \
  REVIEW_NOW="$REVIEW_NOW" \
  LOCAL_INVENTORY_FILE="$LOCAL_INVENTORY_FILE" \
  REMOTE_INVENTORY_FILE="$REMOTE_INVENTORY_FILE" \
  LOCAL_PLAN_FILE="$LOCAL_PLAN_FILE" \
  REMOTE_PLAN_FILE="$REMOTE_PLAN_FILE" \
  bash <<'BASH'
# BEGIN POST_RUN_PLANNER_REPORTS
set -euo pipefail
"$RETENTION_PLANNER" \
  --mode local \
  --prefix "$BACKUP_PREFIX" \
  --now "$REVIEW_NOW" \
  --local-count 2 \
  --format json \
  < "$LOCAL_INVENTORY_FILE" \
  > "$LOCAL_PLAN_FILE"
"$RETENTION_PLANNER" \
  --mode remote \
  --prefix "$BACKUP_PREFIX" \
  --now "$REVIEW_NOW" \
  --daily 7 \
  --weekly 4 \
  --monthly 3 \
  --format json \
  < "$REMOTE_INVENTORY_FILE" \
  > "$REMOTE_PLAN_FILE"
# END POST_RUN_PLANNER_REPORTS
BASH

sudo python3 - "$LOCAL_PLAN_FILE" "$REMOTE_PLAN_FILE" "$BACKUP_PREFIX" <<'PY'
from pathlib import Path
import json
import sys

local = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
remote = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
prefix = sys.argv[3]
if local.get("mode") != "local" or local.get("prefix") != prefix:
    raise SystemExit("local retention report identity mismatch")
if len(local.get("keep", [])) != 2 or local.get("delete") != []:
    raise SystemExit("local retention is not exactly two kept pairs with zero eligible deletions")
if remote.get("mode") != "remote" or remote.get("prefix") != prefix:
    raise SystemExit("remote retention report identity mismatch")
if remote.get("delete") != []:
    raise SystemExit("remote eligible deletions remain after the scheduled run")
print(f"local_keep_pairs={len(local['keep'])} local_delete_pairs=0")
print(f"remote_keep_pairs={len(remote['keep'])} remote_delete_pairs=0")
PY

NEWEST_DUMP=$(sudo python3 - "$LOCAL_PLAN_FILE" <<'PY'
from pathlib import Path
import json
import re
import sys

plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
keep = plan.get("keep", [])
if len(keep) != 2:
    raise SystemExit("local report does not contain exactly two kept pairs")
name = keep[0].get("dump", "")
if re.fullmatch(r"[A-Za-z0-9._-]+[0-9]{8}T[0-9]{6}Z\.dump", name) is None:
    raise SystemExit("newest local dump name is unsafe")
print(name)
PY
)
LOCAL_SIZE=$(sudo stat -c '%s' "$BACKUP_DIR/$NEWEST_DUMP")
REMOTE_SIZE=$(sudo rclone --config /etc/degen/rclone.conf lsjson \
  "onedrive:backups/degen-db/$NEWEST_DUMP" --stat |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["Size"])')
test "$REMOTE_SIZE" = "$LOCAL_SIZE"
sudo rclone --config /etc/degen/rclone.conf cat \
  "onedrive:backups/degen-db/$NEWEST_DUMP.sha256" |
  sudo cmp -s - "$BACKUP_DIR/$NEWEST_DUMP.sha256"
printf 'remote_final_dump=%s local_bytes=%s remote_bytes=%s sidecar_match=true\n' \
  "$NEWEST_DUMP" "$LOCAL_SIZE" "$REMOTE_SIZE" |
  sudo tee "$OBSERVATION_DIR/remote-final-pair.txt"
sudo find "$OBSERVATION_DIR" -mindepth 1 -maxdepth 1 -type f \
  -exec chmod 0600 -- {} +
```

The fresh-shell block reads only the persisted `BACKUP_PREFIX` key; it never sources or prints the real environment file. The generated local report must prove exactly 2 complete pairs in `keep` and no eligible pair in `delete`. The generated remote report applies the exact 7 distinct UTC dates/4 ISO weeks/3 months policy and must have no remaining eligible pair in `delete`; protected objects may remain. Preserve both JSON reports with the journal exit status, aggregate checksum output, remote final-pair size/sidecar result, and disk-free evidence.

Any nonzero `ExecMainStatus`, missing success line, checksum failure, remote mismatch, unexpected candidate, or retention mismatch is a failed scheduled observation even if a dump file exists.

## Rollback

Rollback requires the exact `CONFIG_BACKUP_DIR` created above and separate approval. Start in a fresh shell and set that variable explicitly. Validation of path, ownership, mode, required files, exclusive planner state, manifest completeness, and every digest finishes before the first install/remove/reload side effect. The rclone audit copy is intentionally not restored.

```bash
# BEGIN FAIL_CLOSED_ROLLBACK
set -euo pipefail
: "${CONFIG_BACKUP_DIR:?set exact snapshot path}"
if [[ ! "$CONFIG_BACKUP_DIR" =~ ^/opt/degen/backups/config/[0-9]{8}T[0-9]{6}Z$ ]]; then
  printf '%s\n' 'ERROR: snapshot path is outside the exact timestamped config root' >&2
  exit 1
fi
[[ -d "$CONFIG_BACKUP_DIR" && ! -L "$CONFIG_BACKUP_DIR" ]] || {
  printf '%s\n' 'ERROR: snapshot path must be a real directory, not a symlink' >&2
  exit 1
}
resolved_snapshot=$(readlink -f -- "$CONFIG_BACKUP_DIR")
[[ "$resolved_snapshot" == "$CONFIG_BACKUP_DIR" ]] || {
  printf '%s\n' 'ERROR: snapshot path did not resolve exactly' >&2
  exit 1
}
snapshot_owner=$(sudo stat -c '%u:%g' -- "$CONFIG_BACKUP_DIR")
snapshot_mode=$(sudo stat -c '%a' -- "$CONFIG_BACKUP_DIR")
[[ "$snapshot_owner" == 0:0 && "$snapshot_mode" == 700 ]] || {
  printf '%s\n' 'ERROR: snapshot directory must be root:root mode 0700' >&2
  exit 1
}

required_snapshot_files=(
  degen-prod-db-backup
  degen-prod-db-backup.service
  degen-prod-db-backup.timer
  prod-db-backup.env
  rclone.conf.audit
  SHA256SUMS
)
for snapshot_name in "${required_snapshot_files[@]}"; do
  snapshot_path="$CONFIG_BACKUP_DIR/$snapshot_name"
  sudo test -f "$snapshot_path"
  sudo test ! -L "$snapshot_path"
done

if sudo test -f "$CONFIG_BACKUP_DIR/degen-prod-db-retention" && \
   sudo test ! -e "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent"; then
  sudo test ! -L "$CONFIG_BACKUP_DIR/degen-prod-db-retention"
  planner_state=saved
elif sudo test ! -e "$CONFIG_BACKUP_DIR/degen-prod-db-retention" && \
     sudo test -f "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent" && \
     sudo test ! -L "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent"; then
  planner_state=absent
else
  printf '%s\n' 'ERROR: snapshot must contain saved planner xor explicit absence marker' >&2
  exit 1
fi

sudo sh -c '
  cd "$1"
  manifest_names=$(awk "{print \$2}" SHA256SUMS | LC_ALL=C sort)
  snapshot_names=$(find . -mindepth 1 -maxdepth 1 -type f ! -name SHA256SUMS -printf "%P\n" | LC_ALL=C sort)
  test "$manifest_names" = "$snapshot_names"
  sha256sum -c SHA256SUMS
' sh "$CONFIG_BACKUP_DIR"

sudo install -o root -g root -m 0755 \
  "$CONFIG_BACKUP_DIR/degen-prod-db-backup" \
  /usr/local/sbin/degen-prod-db-backup
if [[ "$planner_state" == saved ]]; then
  sudo install -o root -g root -m 0755 \
    "$CONFIG_BACKUP_DIR/degen-prod-db-retention" \
    /usr/local/sbin/degen-prod-db-retention
elif [[ "$planner_state" == absent ]]; then
  sudo rm -f -- /usr/local/sbin/degen-prod-db-retention
else
  printf '%s\n' 'ERROR: validated planner state was lost' >&2
  exit 1
fi
sudo install -o root -g root -m 0644 \
  "$CONFIG_BACKUP_DIR/degen-prod-db-backup.service" \
  /etc/systemd/system/degen-prod-db-backup.service
sudo install -o root -g root -m 0644 \
  "$CONFIG_BACKUP_DIR/degen-prod-db-backup.timer" \
  /etc/systemd/system/degen-prod-db-backup.timer
sudo install -o root -g root -m 0600 \
  "$CONFIG_BACKUP_DIR/prod-db-backup.env" \
  /etc/degen/prod-db-backup.env
sudo systemctl daemon-reload
systemctl is-enabled degen-prod-db-backup.timer
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
# END FAIL_CLOSED_ROLLBACK
```

Local rollback cannot restore remote objects deleted by a completed retention run. Remote deletion is potentially irreversible except for provider recycle behavior, which is neither guaranteed nor to be purged during this work. The saved `rclone.conf.audit` is audit/emergency evidence only; recovering it requires separate explicit approval and current-token analysis because an automatic restore can invalidate rotated refresh credentials.

## Troubleshooting and no-silent-failure rules

- Check both `journalctl -u degen-prod-db-backup.service` and `/var/log/degen/prod-db-backup.log`. A stale or absent success line is not success.
- `WARNING: backup cleanup failed` is actionable. Capture the exact warning and inventory the named local or remote temporary object before any cleanup. Never broaden cleanup to a wildcard.
- A lock-unavailable message means another run owns `/run/lock/degen-prod-db-backup.lock`; inspect the unit and process state rather than removing the lock file.
- Capacity failure requires database-size and `df -B1` evidence. Do not lower `MIN_FREE_AFTER_BYTES` to force a run.
- Rclone access, size, or sidecar failures block publication and retention. Do not bypass `--immutable` or manually rename temporary objects.
- The accepted rclone identical-byte TOCTOU caveat is narrow: after an immutable upload reports success, the script cannot distinguish its uploaded temporary object from an identically named object raced into place with identical bytes. Final size and checksum-sidecar verification detect content differences but cannot prove upload ownership. A collision or unexpected temporary object remains protected for investigation.
- Unknown, incomplete, manual, and temporary names are deliberately protected. Their presence is a review item, not permission to delete them.
- No OneDrive recycle-bin purge, application-process restart, database-process restart, or manual full backup is a troubleshooting step in this runbook.
