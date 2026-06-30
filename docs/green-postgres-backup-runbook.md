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
sudo rclone --config /etc/degen/rclone.conf lsf \
  onedrive:backups/degen-db --files-only --max-depth 1 | sort
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

After approval and before installation, keep the same shell so the PID variables remain available. The current script, service, timer, and real environment file are required and copied exactly. A missing planner is expected on the first install and is recorded; an existing planner is preserved.

```bash
umask 077
CONFIG_BACKUP_DIR="/opt/degen/backups/config/$(date -u +%Y%m%dT%H%M%SZ)"
sudo test ! -e "$CONFIG_BACKUP_DIR"
sudo mkdir -m 0700 -- "$CONFIG_BACKUP_DIR"
sudo chown root:root -- "$CONFIG_BACKUP_DIR"

for required in \
  /usr/local/sbin/degen-prod-db-backup \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env
do
  sudo test -f "$required"
  sudo test ! -L "$required"
done

sudo sha256sum -- \
  /usr/local/sbin/degen-prod-db-backup \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer \
  /etc/degen/prod-db-backup.env |
  sudo tee "$CONFIG_BACKUP_DIR/preinstall-source-files.sha256" >/dev/null
sudo cp -a -- /usr/local/sbin/degen-prod-db-backup "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/systemd/system/degen-prod-db-backup.service "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/systemd/system/degen-prod-db-backup.timer "$CONFIG_BACKUP_DIR/"
sudo cp -a -- /etc/degen/prod-db-backup.env "$CONFIG_BACKUP_DIR/"
if sudo test -e /usr/local/sbin/degen-prod-db-retention; then
  sudo test -f /usr/local/sbin/degen-prod-db-retention
  sudo test ! -L /usr/local/sbin/degen-prod-db-retention
  sudo sha256sum -- /usr/local/sbin/degen-prod-db-retention |
    sudo tee -a "$CONFIG_BACKUP_DIR/preinstall-source-files.sha256" >/dev/null
  sudo cp -a -- /usr/local/sbin/degen-prod-db-retention "$CONFIG_BACKUP_DIR/"
else
  printf '%s\n' 'missing planner: expected on the first install' |
    sudo tee "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent" >/dev/null
fi

sudo sh -c 'cd "$1" && sha256sum -- degen-prod-db-backup degen-prod-db-backup.service degen-prod-db-backup.timer prod-db-backup.env > backup-files.sha256' \
  sh "$CONFIG_BACKUP_DIR"
if sudo test -f "$CONFIG_BACKUP_DIR/degen-prod-db-retention"; then
  sudo sh -c 'cd "$1" && sha256sum -- degen-prod-db-retention >> backup-files.sha256' \
    sh "$CONFIG_BACKUP_DIR"
fi
sudo find "$CONFIG_BACKUP_DIR" -mindepth 1 -maxdepth 1 -type f \
  -printf '%f owner=%u:%g mode=%m size=%s\n' |
  sudo tee "$CONFIG_BACKUP_DIR/backup-files.stat" >/dev/null
printf 'PostgreSQL unit=%s MainPID=%s\nweb MainPID=%s\nworker MainPID=%s\nbot unit=%s MainPID=%s\n' \
  "$POSTGRES_UNIT" "$POSTGRES_PID_BEFORE" "$WEB_PID_BEFORE" "$WORKER_PID_BEFORE" \
  "$BOT_UNIT" "$BOT_PID_BEFORE" |
  sudo tee "$CONFIG_BACKUP_DIR/mainpids.before" >/dev/null
sudo chmod 0600 "$CONFIG_BACKUP_DIR"/backup-files.sha256 \
  "$CONFIG_BACKUP_DIR"/backup-files.stat \
  "$CONFIG_BACKUP_DIR"/preinstall-source-files.sha256 \
  "$CONFIG_BACKUP_DIR"/mainpids.before
sudo sh -c 'cd "$1" && sha256sum -c backup-files.sha256' sh "$CONFIG_BACKUP_DIR"
sudo test "$(sudo stat -c '%U:%G:%a' "$CONFIG_BACKUP_DIR")" = 'root:root:700'
```

## Install reviewed bytes and update only policy keys

Install executables as `root:root 0755` and units as `root:root 0644`. Do not install the example over the real environment file.

```bash
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

This exact editor preserves every unrelated line in the real file and adds or replaces only the new non-sensitive policy keys. It refuses a symlink, incorrect ownership/mode, or an existing temporary path. `REMOTE_PRUNE_ENABLED=0` remains the initial state.

```bash
sudo python3 - <<'PY'
from pathlib import Path
import os
import stat

path = Path("/etc/degen/prod-db-backup.env")
temporary = path.with_name(path.name + ".retention-update")
updates = {
    "KEEP_LOCAL_COUNT": "2",
    "KEEP_REMOTE_DAILY": "7",
    "KEEP_REMOTE_WEEKLY": "4",
    "KEEP_REMOTE_MONTHLY": "3",
    "REMOTE_PRUNE_ENABLED": "0",
    "MIN_FREE_AFTER_BYTES": "10737418240",
    "RETENTION_PLANNER": "/usr/local/sbin/degen-prod-db-retention",
    "LOCK_FILE": "/run/lock/degen-prod-db-backup.lock",
}
if path.is_symlink() or not path.is_file():
    raise SystemExit("real environment file must be a regular file")
metadata = path.stat()
if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit("real environment file must remain root:root 0600")
if temporary.exists() or temporary.is_symlink():
    raise SystemExit("refusing a pre-existing policy-update temporary path")

output = []
seen = set()
for line in path.read_text(encoding="utf-8").splitlines():
    key = line.split("=", 1)[0] if "=" in line else ""
    if key in updates:
        if key not in seen:
            output.append(f"{key}={updates[key]}")
            seen.add(key)
        continue
    output.append(line)
for key, value in updates.items():
    if key not in seen:
        output.append(f"{key}={value}")

descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
    handle.write("\n".join(output) + "\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chown(temporary, 0, 0)
os.chmod(temporary, 0o600)
os.replace(temporary, path)
PY
sudo test "$(sudo stat -c '%U:%G:%a' /etc/degen/prod-db-backup.env)" = 'root:root:600'
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' /etc/degen/prod-db-backup.env | sort
```

The last command displays environment variable names only. Never inspect, diff, log, or paste the file's values.

## Validate without running a backup

Validate unit syntax, reload metadata, and confirm the existing timer remains enabled with the intended next trigger. Do not invoke the service and do not use `--now`.

```bash
sudo systemd-analyze verify \
  /etc/systemd/system/degen-prod-db-backup.service \
  /etc/systemd/system/degen-prod-db-backup.timer
sudo systemctl daemon-reload
systemctl is-enabled degen-prod-db-backup.timer
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
systemctl show degen-prod-db-backup.timer \
  -p ActiveState -p SubState -p NextElapseUSecRealtime -p LastTriggerUSec

set -o pipefail
sudo /usr/local/sbin/degen-prod-db-backup preflight |
  sudo tee "$CONFIG_BACKUP_DIR/preflight.txt"
sudo /usr/local/sbin/degen-prod-db-backup remote-retention-dry-run |
  sudo tee "$CONFIG_BACKUP_DIR/remote-retention-dry-run.txt"
```

`remote-retention-dry-run` does not delete even when REMOTE_PRUNE_ENABLED=1; it also does not create a dump. The host flag must still be `0` here.

## Exact dry-run candidate review gate

Extract exact dry-run objects and independently regenerate the keep/delete/protected plan from the complete remote inventory:

```bash
sudo sed -n 's/^.*Remote retention dry run: would delete //p' \
  "$CONFIG_BACKUP_DIR/remote-retention-dry-run.txt" |
  sudo tee "$CONFIG_BACKUP_DIR/remote-retention-candidates.txt" >/dev/null
sudo rclone --config /etc/degen/rclone.conf lsf \
  onedrive:backups/degen-db --files-only --max-depth 1 | sort |
  sudo tee "$CONFIG_BACKUP_DIR/remote-inventory.txt" >/dev/null
BACKUP_PREFIX=$(sudo sed -n 's/^.*Preflight passed for mode=remote-retention-dry-run prefix=//p' \
  "$CONFIG_BACKUP_DIR/remote-retention-dry-run.txt" | tail -n 1)
test -n "$BACKUP_PREFIX"
REVIEW_NOW=$(date -u +%Y%m%dT%H%M%SZ)
sudo sh -c '/usr/local/sbin/degen-prod-db-retention --mode remote --prefix "$1" --now "$2" --daily 7 --weekly 4 --monthly 3 --format json < "$3" > "$4"' \
  sh "$BACKUP_PREFIX" "$REVIEW_NOW" \
  "$CONFIG_BACKUP_DIR/remote-inventory.txt" \
  "$CONFIG_BACKUP_DIR/remote-retention-plan.json"
sudo python3 - "$CONFIG_BACKUP_DIR" <<'PY'
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
sudo cat "$CONFIG_BACKUP_DIR/remote-retention-candidates.txt"
```

Do not cross this gate until every candidate is manually recognized as an automation-owned complete pair and is outside the keep set in `remote-retention-plan.json`. Protected objects must never appear as candidates. Record the decision beside the snapshot.

If there are zero candidates, record `zero candidates` and then enable the reviewed future policy. If candidates exist, enable it only after complete review and explicit approval. The gated edit below refuses anything other than one current `REMOTE_PRUNE_ENABLED=0` line and changes no other line:

```bash
sudo python3 - <<'PY'
from pathlib import Path
import os

path = Path("/etc/degen/prod-db-backup.env")
temporary = path.with_name(path.name + ".prune-enable")
lines = path.read_text(encoding="utf-8").splitlines()
matches = [index for index, line in enumerate(lines) if line.startswith("REMOTE_PRUNE_ENABLED=")]
if len(matches) != 1 or lines[matches[0]] != "REMOTE_PRUNE_ENABLED=0":
    raise SystemExit("remote prune flag is not exactly one disabled entry")
if temporary.exists() or temporary.is_symlink():
    raise SystemExit("refusing a pre-existing prune-enable temporary path")
lines[matches[0]] = "REMOTE_PRUNE_ENABLED=1"
descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
    handle.write("\n".join(lines) + "\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chown(temporary, 0, 0)
os.chmod(temporary, 0o600)
os.replace(temporary, path)
PY
sudo test "$(sudo stat -c '%U:%G:%a' /etc/degen/prod-db-backup.env)" = 'root:root:600'
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' /etc/degen/prod-db-backup.env | sort
```

Do not run another script mode after the flag change. Let the next scheduled timer activation exercise the full path. No OneDrive recycle bin purge is part of this procedure.

## Immediate post-install verification

Verify installed bytes and modes against the reviewed checkout, confirm the correct next trigger (03:15 America/Los_Angeles plus at most 20 minutes randomized delay), and compare all process IDs in the same shell:

```bash
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

sudo sh -c 'cd "$1" && sha256sum -c backup-files.sha256' sh "$CONFIG_BACKUP_DIR"
```

On the Windows workstation, confirm OneDrive stays off before and after this Green-only operation. Do not enable the client, sync this backup directory, or use it to inspect remote backup content. Record that operational check with the rollback-ready snapshot path.

## Required observation after the next scheduled run

Do not claim success before this scheduled observation. After the next scheduled run has actually fired, perform every check below:

```bash
sudo journalctl -u degen-prod-db-backup.service -n 200 --no-pager
systemctl show degen-prod-db-backup.service \
  -p Result -p ExecMainCode -p ExecMainStatus -p InactiveEnterTimestamp
sudo tail -n 200 /var/log/degen/prod-db-backup.log
systemctl list-timers --all degen-prod-db-backup.timer --no-pager
df -B1 /opt/degen/backups/db

sudo find /opt/degen/backups/db -mindepth 1 -maxdepth 1 -type f \
  -printf '%f\t%s bytes\n' | sort
sudo sh -c 'cd /opt/degen/backups/db && for sidecar in "$1"[0-9]*T[0-9]*Z.dump.sha256; do test -f "$sidecar" && sha256sum -c -- "$sidecar"; done' \
  sh "$BACKUP_PREFIX"

NEWEST_DUMP=$(sudo find /opt/degen/backups/db -mindepth 1 -maxdepth 1 -type f \
  -name "${BACKUP_PREFIX}*.dump" -printf '%f\n' | sort | tail -n 1)
test -n "$NEWEST_DUMP"
LOCAL_SIZE=$(sudo stat -c '%s' "/opt/degen/backups/db/$NEWEST_DUMP")
REMOTE_SIZE=$(sudo rclone --config /etc/degen/rclone.conf lsjson \
  "onedrive:backups/degen-db/$NEWEST_DUMP" --stat |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["Size"])')
test "$REMOTE_SIZE" = "$LOCAL_SIZE"
sudo rclone --config /etc/degen/rclone.conf cat \
  "onedrive:backups/degen-db/$NEWEST_DUMP.sha256" |
  sudo cmp -s - "/opt/degen/backups/db/$NEWEST_DUMP.sha256"
```

Rebuild local and remote planner reports with the same `BACKUP_PREFIX` and a fresh UTC `REVIEW_NOW`. The local report must show exactly 2 complete pairs in `keep`, no recognized complete pair in `delete`, and passing local sha256 checks. The remote report must show no object in `delete` after enabled retention, with `keep` implementing the exact 7 distinct UTC dates/4 ISO weeks/3 months union; protected objects may remain. Record remote final-pair size and sidecar equality, disk free bytes, journal exit status, and both reports.

Any nonzero `ExecMainStatus`, missing success line, checksum failure, remote mismatch, unexpected candidate, or retention mismatch is a failed scheduled observation even if a dump file exists.

## Rollback

Rollback requires the exact `CONFIG_BACKUP_DIR` created above and separate approval. It restores the original script, service, timer, and real environment file. If the first-install marker says the planner did not exist, remove only the newly installed planner; otherwise restore its saved bytes.

```bash
sudo sh -c 'cd "$1" && sha256sum -c backup-files.sha256' sh "$CONFIG_BACKUP_DIR"
sudo install -o root -g root -m 0755 \
  "$CONFIG_BACKUP_DIR/degen-prod-db-backup" \
  /usr/local/sbin/degen-prod-db-backup
if sudo test -f "$CONFIG_BACKUP_DIR/degen-prod-db-retention"; then
  sudo install -o root -g root -m 0755 \
    "$CONFIG_BACKUP_DIR/degen-prod-db-retention" \
    /usr/local/sbin/degen-prod-db-retention
else
  sudo test -f "$CONFIG_BACKUP_DIR/degen-prod-db-retention.absent"
  sudo rm -f -- /usr/local/sbin/degen-prod-db-retention
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
```

Local rollback cannot restore remote objects deleted by a completed retention run. Remote deletion is potentially irreversible except for provider recycle behavior, which is neither guaranteed nor to be purged during this work.

## Troubleshooting and no-silent-failure rules

- Check both `journalctl -u degen-prod-db-backup.service` and `/var/log/degen/prod-db-backup.log`. A stale or absent success line is not success.
- `WARNING: backup cleanup failed` is actionable. Capture the exact warning and inventory the named local or remote temporary object before any cleanup. Never broaden cleanup to a wildcard.
- A lock-unavailable message means another run owns `/run/lock/degen-prod-db-backup.lock`; inspect the unit and process state rather than removing the lock file.
- Capacity failure requires database-size and `df -B1` evidence. Do not lower `MIN_FREE_AFTER_BYTES` to force a run.
- Rclone access, size, or sidecar failures block publication and retention. Do not bypass `--immutable` or manually rename temporary objects.
- The accepted rclone identical-byte TOCTOU caveat is narrow: after an immutable upload reports success, the script cannot distinguish its uploaded temporary object from an identically named object raced into place with identical bytes. Final size and checksum-sidecar verification detect content differences but cannot prove upload ownership. A collision or unexpected temporary object remains protected for investigation.
- Unknown, incomplete, manual, and temporary names are deliberately protected. Their presence is a review item, not permission to delete them.
- No OneDrive recycle-bin purge, application-process restart, database-process restart, or manual full backup is a troubleshooting step in this runbook.
