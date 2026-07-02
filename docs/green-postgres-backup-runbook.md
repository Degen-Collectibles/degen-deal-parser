# Green PostgreSQL backup retention runbook

Last reviewed: 2026-06-30

This is the production execution guide for Green/Brev host `openclaw-9902ae`.
The application directory remains `/opt/degen/app`, but no production source is
read or installed from that live checkout. The only install source is an
uncompressed, path-limited Git archive made from one exact pushed commit.

The policy is:

- create one unique timestamped PostgreSQL custom-format dump and its SHA-256
  sidecar per successful scheduled run;
- retain the newest 2 verified complete local pairs;
- retain remote representatives for 7 distinct UTC dates, 4 ISO weeks, and 3
  months;
- fail closed for unknown, incomplete, malformed, replaced, or unverified
  objects;
- install with `REMOTE_PRUNE_ENABLED=0`, prove a remote dry run, and only then
  seek the separate pruning approval.

Do not improvise an environment editor, copy files from `/opt/degen/app`, run
`rclone` directly, manually start the protected backup service, or hand-write a
rollback. The reviewed operations helper owns those actions and their durable
receipts. Keep OneDrive on this Windows computer off throughout this workflow;
the production remote is accessed by Green, not by the desktop sync client.

## Fixed reviewed source contract

The archive contains exactly the manifest plus these seven non-secret assets.
It excludes the runbook, tests, plans, real environment files, database dumps,
operation snapshots/state, and `/etc/degen/rclone.conf`.

```bash
ARCHIVE_PATHS=(
  deploy/linux/degen-prod-db-backup-assets.sha256
  deploy/linux/degen-prod-db-backup-env.py
  deploy/linux/degen-prod-db-backup-ops.py
  deploy/linux/degen-prod-db-backup.sh
  deploy/linux/degen-prod-db-retention.py
  deploy/systemd/degen-prod-db-backup.env.example
  deploy/systemd/degen-prod-db-backup.service
  deploy/systemd/degen-prod-db-backup.timer
)

EXPECTED_ARCHIVE_MEMBERS=(
  deploy/
  deploy/linux/
  deploy/linux/degen-prod-db-backup-assets.sha256
  deploy/linux/degen-prod-db-backup-env.py
  deploy/linux/degen-prod-db-backup-ops.py
  deploy/linux/degen-prod-db-backup.sh
  deploy/linux/degen-prod-db-retention.py
  deploy/systemd/
  deploy/systemd/degen-prod-db-backup.env.example
  deploy/systemd/degen-prod-db-backup.service
  deploy/systemd/degen-prod-db-backup.timer
)
```

Start one WSL Bash shell, evaluate both arrays above, and keep that shell open
through archive creation and transfer so the exact contract is not retyped.

The corresponding install targets are:

```text
/usr/local/sbin/degen-prod-db-backup
/usr/local/sbin/degen-prod-db-retention
/usr/local/sbin/degen-prod-db-backup-env
/usr/local/sbin/degen-prod-db-backup-ops
/etc/systemd/system/degen-prod-db-backup.service
/etc/systemd/system/degen-prod-db-backup.timer
/etc/degen/prod-db-backup.env
```

## Gate 1: push the exact reviewed commit

This gate authorizes only a normal, non-force push of the reviewed local commit
to one exact branch ref. It does not authorize merge, deployment, Green access,
operation-directory creation, archive transfer, service/timer changes, rclone
access, or database writes.

Before asking for `proceed`, record the local branch, clean/dirty state, exact
40-character commit, remote URL, destination ref, intended push command, and
the rollback (delete or supersede the remote branch after a separate approval).
After approval, run in WSL Bash from the reviewed worktree:

```bash
set -euo pipefail
umask 077
export LC_ALL=C

REVIEWED_SHA="${APPROVED_REVIEWED_SHA:?set the exact approved 40-character commit}"
REMOTE_REF="${APPROVED_REMOTE_REF:?set the exact approved refs/heads/... ref}"
CANONICAL_REMOTE_URL="https://github.com/Degen-Collectibles/degen-deal-parser.git"
[[ "$REVIEWED_SHA" =~ ^[0-9a-f]{40}$ ]]
git check-ref-format "$REMOTE_REF"
case "$REMOTE_REF" in
  refs/heads/codex/*) ;;
  *) printf '%s\n' 'ERROR: destination must be an approved codex branch ref' >&2; exit 1 ;;
esac
test "$REMOTE_REF" = refs/heads/codex/backup-retention-hardening
mapfile -t ORIGIN_FETCH_URLS < <(git remote get-url --all origin)
mapfile -t ORIGIN_PUSH_URLS < <(git remote get-url --push --all origin)
test "${#ORIGIN_FETCH_URLS[@]}" -eq 1
test "${#ORIGIN_PUSH_URLS[@]}" -eq 1
test "${ORIGIN_FETCH_URLS[0]}" = "$CANONICAL_REMOTE_URL"
test "${ORIGIN_PUSH_URLS[0]}" = "$CANONICAL_REMOTE_URL"
test "$(git rev-parse --verify HEAD^{commit})" = "$REVIEWED_SHA"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
git push origin "$REVIEWED_SHA:$REMOTE_REF"
REMOTE_BRANCH_SHA="$(git ls-remote --exit-code --refs origin "$REMOTE_REF" | awk -v ref="$REMOTE_REF" '$2 == ref { print $1 }')"
test "$REMOTE_BRANCH_SHA" = "$REVIEWED_SHA"

EVIDENCE_DIR="$(mktemp -d /tmp/degen-backup-evidence.XXXXXXXX)"
ARCHIVE_LOCAL="$EVIDENCE_DIR/source.tar"
git -c tar.umask=0002 archive --format=tar --output "$ARCHIVE_LOCAL" "$REVIEWED_SHA" -- "${ARCHIVE_PATHS[@]}"

test "$(git get-tar-commit-id < "$ARCHIVE_LOCAL")" = "$REVIEWED_SHA"
mapfile -t LOCAL_ARCHIVE_NAMES < <(tar --list --file "$ARCHIVE_LOCAL")
test "${#LOCAL_ARCHIVE_NAMES[@]}" -eq "${#EXPECTED_ARCHIVE_MEMBERS[@]}"
cmp --silent \
  <(printf '%s\n' "${EXPECTED_ARCHIVE_MEMBERS[@]}") \
  <(printf '%s\n' "${LOCAL_ARCHIVE_NAMES[@]}" | sort)
while IFS= read -r member_record; do
  case "${member_record:0:1}" in
    d|-) ;;
    *) printf '%s\n' 'ERROR: archive contains a link or special member' >&2; exit 1 ;;
  esac
done < <(tar --list --verbose --file "$ARCHIVE_LOCAL")

LOCAL_VERIFY_DIR="$(mktemp -d /tmp/degen-backup-archive.XXXXXXXX)"
trap 'rm -rf -- "$LOCAL_VERIFY_DIR"' EXIT
tar --extract --file "$ARCHIVE_LOCAL" --directory "$LOCAL_VERIFY_DIR" --no-same-owner --no-same-permissions
(
  cd "$LOCAL_VERIFY_DIR"
  sha256sum --check --strict deploy/linux/degen-prod-db-backup-assets.sha256
)

REVIEWED_ARCHIVE_SHA256="$(sha256sum "$ARCHIVE_LOCAL" | awk '{ print $1 }')"
REVIEWED_MANIFEST_SHA256="$(git show "$REVIEWED_SHA:deploy/linux/degen-prod-db-backup-assets.sha256" | sha256sum | awk '{ print $1 }')"
printf 'reviewed_sha=%s\narchive_sha256=%s\nmanifest_sha256=%s\narchive_local=%s\n' \
  "$REVIEWED_SHA" "$REVIEWED_ARCHIVE_SHA256" "$REVIEWED_MANIFEST_SHA256" "$ARCHIVE_LOCAL"
```

The three printed values are the immutable evidence for Gate 2. If any command
fails, stop. Do not create a different archive from the display branch name or
from working-tree content.

## Gate 2: approve production installation

Resume the same WSL controller shell. If it was closed, re-evaluate the two
fixed arrays, restore the printed Gate 1 values, and repeat only the read-only
validations (never the push) before fixing these exact non-secret values for
the production preflight:

```bash
UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"
SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"
MANIFEST_SHA256="${APPROVED_MANIFEST_SHA256:?set the approved reviewed-manifest SHA-256}"
REMOTE_REF="${APPROVED_REMOTE_REF:?set the exact approved refs/heads/... ref}"
CANONICAL_REMOTE_URL="https://github.com/Degen-Collectibles/degen-deal-parser.git"
REVIEWED_SHA="${APPROVED_REVIEWED_SHA:?set the approved reviewed commit}"
ARCHIVE_SHA256="${APPROVED_ARCHIVE_SHA256:?set the approved source.tar SHA-256}"
ARCHIVE_LOCAL="${APPROVED_ARCHIVE_LOCAL:?set the private Gate 1 source.tar path}"
TRANSFER_TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
TRANSFER_DIR="/tmp/degen-backup-transfer-$TRANSFER_TOKEN"
REMOTE_ARCHIVE="$TRANSFER_DIR/source.tar"
SOURCE_DIR="$OPERATION_DIR/source"
SOURCE_MANIFEST="$SOURCE_DIR/deploy/linux/degen-prod-db-backup-assets.sha256"
[[ "$UTC_STAMP" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
[[ "$REVIEWED_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]
[[ "$MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]]
[[ "$TRANSFER_TOKEN" =~ ^[0-9a-f]{32}$ ]]
test "$OPERATION_DIR" = "/opt/degen/backups/config/$UTC_STAMP"
test "$SOURCE_OPS" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"
test "$REMOTE_REF" = refs/heads/codex/backup-retention-hardening
mapfile -t ORIGIN_FETCH_URLS < <(git remote get-url --all origin)
mapfile -t ORIGIN_PUSH_URLS < <(git remote get-url --push --all origin)
test "${#ORIGIN_FETCH_URLS[@]}" -eq 1
test "${#ORIGIN_PUSH_URLS[@]}" -eq 1
test "${ORIGIN_FETCH_URLS[0]}" = "$CANONICAL_REMOTE_URL"
test "${ORIGIN_PUSH_URLS[0]}" = "$CANONICAL_REMOTE_URL"
test "$(sha256sum "$ARCHIVE_LOCAL" | awk '{ print $1 }')" = "$ARCHIVE_SHA256"
test "$(git get-tar-commit-id < "$ARCHIVE_LOCAL")" = "$REVIEWED_SHA"
REMOTE_BRANCH_SHA="$(git ls-remote --exit-code --refs origin "$REMOTE_REF" | awk -v ref="$REMOTE_REF" '$2 == ref { print $1 }')"
test "$REMOTE_BRANCH_SHA" = "$REVIEWED_SHA"
```

Before any Green write, re-verify the current Brev host routing with this
read-only preflight. It prints no environment or configuration contents:

```bash
brev exec --help | grep -F -- '--host'
brev copy --help | grep -F -- '--host'
brev exec openclaw-9902ae --host 'set -eu; printf "host=%s uid=%s gid=%s\n" "$(hostname -s)" "$(id -u)" "$(id -g)"; test -d /opt/degen/app; test -d /opt/degen/backups; command -v git tar sha256sum python3 find stat cmp >/dev/null'
```

Validate both digests as lowercase 64-hex, the commit as lowercase 40-hex, the
local archive hash, the embedded commit, the exact archive arrays above, and
the still-equal remote branch SHA. Then present this exact preflight and wait
for a new explicit `proceed`:

- Target: host `openclaw-9902ae`, exact `OPERATION_DIR`, temporary
  `REMOTE_ARCHIVE`, and the seven install targets listed above.
- Changes: create a root-only operation directory; transfer `source.tar`;
  bootstrap-verify it; snapshot current host state; install the reviewed
  assets with pruning disabled; let the helper quiesce/restore the timer;
  perform a disposable remote probe; and record a remote-prune dry run. Once a
  previously active timer is restored, either a persistent catch-up or any
  ordinary scheduled run before Gate 3 can run a backup and local retention.
- Reversible: the operation-local source/staging/snapshot/state evidence and
  all installed targets are covered by the helper's verified snapshot and
  recovery/rollback contract. Database dump files are not part of that
  snapshot.
- Irreversible: remote retention deletion remains disabled in this gate, but
  any catch-up or later scheduled run after timer restoration can apply the
  approved local newest-2 policy before Gate 3 and irreversibly delete older
  local pairs. Recovery and rollback cannot restore those deleted dumps. The
  probe also briefly creates and deletes disposable objects at its unique
  probe prefix.
- Credentials: no secret crosses argv. The rclone configuration may refresh a
  token during the helper-owned probe. Its root-only audit copy is evidence,
  not an automatic rollback target.
- Service impact: the helper owns timer stop/start and never restarts the app,
  worker, bot, or PostgreSQL. `RefuseManualStart=yes` remains enforced for the
  backup service.
- Verification: durable phase receipts, exact target hashes/modes/ownership,
  timer restoration (including whether a catch-up run occurred), remote probe
  cleanup, and the zero-remote-deletion dry-run report.
- Rollback: interrupted mutations use only Conditional recovery below. A
  stable completed transaction needs the later, separately approved manual
  rollback gate.

No production mutation described below this point is run before that
production approval.

### Create private transfer and root-only operation directories, then transfer

Build a non-secret preparation script locally. It atomically creates a random,
mode-0700 transfer directory as the actual Brev SSH user, verifies that user
owns it, then creates the separate operation directory as root. Copying into a
private directory avoids the dangling-symlink and check-to-copy races of a
predictable file directly under world-writable `/tmp`.

```bash
PREPARE_SCRIPT="$(mktemp /tmp/degen-backup-prepare.XXXXXXXX)"
{
  printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'umask 077'
  printf 'OPERATION_DIR=%q\n' "$OPERATION_DIR"
  printf 'TRANSFER_DIR=%q\n' "$TRANSFER_DIR"
  printf 'REMOTE_ARCHIVE=%q\n' "$REMOTE_ARCHIVE"
  cat <<'REMOTE_PREPARE'
TRANSFER_UID="$(id -u)"
mkdir -m 0700 -- "$TRANSFER_DIR"
test -d "$TRANSFER_DIR"
test ! -L "$TRANSFER_DIR"
test "$(stat -c %u "$TRANSFER_DIR")" -eq "$TRANSFER_UID"
test "$(stat -c %a "$TRANSFER_DIR")" = 700
test -z "$(find "$TRANSFER_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
sudo -- /bin/bash -c '
set -euo pipefail
umask 077
OPERATION_DIR="$1"
TRANSFER_DIR="$2"
TRANSFER_UID="$3"
PARENT="${OPERATION_DIR%/*}"
if [[ ! -e "$PARENT" ]]; then mkdir -m 0700 -- "$PARENT"; fi
test -d "$PARENT"
test ! -L "$PARENT"
test "$(stat -c %u "$PARENT")" -eq 0
PARENT_MODE="$(stat -c %a "$PARENT")"
(( (8#$PARENT_MODE & 0022) == 0 ))
test ! -e "$OPERATION_DIR"
test -d "$TRANSFER_DIR"
test ! -L "$TRANSFER_DIR"
test "$(stat -c %u "$TRANSFER_DIR")" -eq "$TRANSFER_UID"
test "$(stat -c %a "$TRANSFER_DIR")" = 700
mkdir -m 0700 -- "$OPERATION_DIR"
test "$(stat -c %u "$OPERATION_DIR")" -eq 0
test "$(stat -c %a "$OPERATION_DIR")" = 700
' -- "$OPERATION_DIR" "$TRANSFER_DIR" "$TRANSFER_UID"
REMOTE_PREPARE
} > "$PREPARE_SCRIPT"
chmod 0700 "$PREPARE_SCRIPT"
brev exec openclaw-9902ae --host "@$PREPARE_SCRIPT"
brev copy --host "$ARCHIVE_LOCAL" "openclaw-9902ae:$REMOTE_ARCHIVE"
```

### Standard-tool bootstrap and normal source-helper path

The following local controller builds a remote script containing only approved
non-secret values and the fixed member-name contract. The remote script first
verifies the archive with `sha256sum`, `git`, `tar`, `find`, `stat`, and `cmp`.
It does not execute the new helper until all bootstrap checks pass.

```bash
BOOTSTRAP_SCRIPT="$(mktemp /tmp/degen-backup-bootstrap.XXXXXXXX)"
{
  printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'umask 077' 'export LC_ALL=C'
  printf 'OPERATION_DIR=%q\n' "$OPERATION_DIR"
  printf 'SOURCE_OPS=%q\n' "$SOURCE_OPS"
  printf 'SOURCE_DIR=%q\n' "$SOURCE_DIR"
  printf 'SOURCE_MANIFEST=%q\n' "$SOURCE_MANIFEST"
  printf 'TRANSFER_DIR=%q\n' "$TRANSFER_DIR"
  printf 'REMOTE_ARCHIVE=%q\n' "$REMOTE_ARCHIVE"
  printf 'REVIEWED_SHA=%q\n' "$REVIEWED_SHA"
  printf 'ARCHIVE_SHA256=%q\n' "$ARCHIVE_SHA256"
  printf 'MANIFEST_SHA256=%q\n' "$MANIFEST_SHA256"
  declare -p EXPECTED_ARCHIVE_MEMBERS
  cat <<'REMOTE_BOOTSTRAP'
if (( EUID != 0 )); then exec sudo -- /bin/bash "$0"; fi
test -d "$OPERATION_DIR"
test ! -L "$OPERATION_DIR"
test "$(stat -c %u "$OPERATION_DIR")" -eq 0
test "$(stat -c %a "$OPERATION_DIR")" = 700
test -d "$TRANSFER_DIR"
test ! -L "$TRANSFER_DIR"
test "$(stat -c %a "$TRANSFER_DIR")" = 700
test -f "$REMOTE_ARCHIVE"
test ! -L "$REMOTE_ARCHIVE"
test "$(stat -c %u "$REMOTE_ARCHIVE")" -eq "$(stat -c %u "$TRANSFER_DIR")"
test "$(stat -c %h "$REMOTE_ARCHIVE")" -eq 1
REMOTE_ARCHIVE_MODE="$(stat -c %a "$REMOTE_ARCHIVE")"
(( (8#$REMOTE_ARCHIVE_MODE & 0022) == 0 ))
test ! -e "$OPERATION_DIR/source.tar"
mv --no-target-directory -- "$REMOTE_ARCHIVE" "$OPERATION_DIR/source.tar"
rmdir -- "$TRANSFER_DIR"
test -f "$OPERATION_DIR/source.tar"
test ! -L "$OPERATION_DIR/source.tar"
test "$(stat -c %h "$OPERATION_DIR/source.tar")" -eq 1
chown root:root "$OPERATION_DIR/source.tar"
chmod 0600 "$OPERATION_DIR/source.tar"
test "$(stat -c %u "$OPERATION_DIR/source.tar")" -eq 0
test "$(stat -c %h "$OPERATION_DIR/source.tar")" -eq 1
printf '%s  %s\n' "$ARCHIVE_SHA256" "$OPERATION_DIR/source.tar" | sha256sum --check --strict -
test "$(git get-tar-commit-id < "$OPERATION_DIR/source.tar")" = "$REVIEWED_SHA"

BOOTSTRAP_WORK="$(mktemp -d /tmp/degen-backup-bootstrap.XXXXXXXX)"
trap 'rm -rf -- "$BOOTSTRAP_WORK"' EXIT
printf '%s\n' "${EXPECTED_ARCHIVE_MEMBERS[@]}" > "$BOOTSTRAP_WORK/expected"
mapfile -t ACTUAL_ARCHIVE_MEMBERS < <(tar --list --file "$OPERATION_DIR/source.tar")
test "${#ACTUAL_ARCHIVE_MEMBERS[@]}" -eq "${#EXPECTED_ARCHIVE_MEMBERS[@]}"
printf '%s\n' "${ACTUAL_ARCHIVE_MEMBERS[@]}" | sort > "$BOOTSTRAP_WORK/actual"
cmp --silent "$BOOTSTRAP_WORK/expected" "$BOOTSTRAP_WORK/actual"
tar --list --verbose --file "$OPERATION_DIR/source.tar" > "$BOOTSTRAP_WORK/verbose"
while IFS= read -r member_record; do
  case "${member_record:0:1}" in
    d|-) ;;
    *) printf '%s\n' 'ERROR: archive contains a link or special member' >&2; exit 1 ;;
  esac
done < "$BOOTSTRAP_WORK/verbose"

test ! -e "$SOURCE_DIR"
mkdir -m 0700 -- "$SOURCE_DIR"
tar --extract --file "$OPERATION_DIR/source.tar" --directory "$SOURCE_DIR" --no-same-owner --no-same-permissions
if find "$SOURCE_DIR" -xdev \( -type l -o \( ! -type f ! -type d \) \) -print -quit | grep -q .; then
  printf '%s\n' 'ERROR: extracted source contains a link or special entry' >&2
  exit 1
fi
if find "$SOURCE_DIR" -xdev ! -user root -print -quit | grep -q .; then
  printf '%s\n' 'ERROR: extracted source is not root-owned' >&2
  exit 1
fi
if find "$SOURCE_DIR" -xdev -type f -links +1 -print -quit | grep -q .; then
  printf '%s\n' 'ERROR: extracted source contains a hard-linked file' >&2
  exit 1
fi
if find "$SOURCE_DIR" -xdev -perm /7022 -print -quit | grep -q .; then
  printf '%s\n' 'ERROR: extracted source mode is unsafe' >&2
  exit 1
fi
printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
(
  cd "$SOURCE_DIR"
  sha256sum --check --strict deploy/linux/degen-prod-db-backup-assets.sha256
)

/usr/bin/python3 "$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR" --archive "$OPERATION_DIR/source.tar" --expected-commit "$REVIEWED_SHA" --expected-archive-sha256 "$ARCHIVE_SHA256" --expected-manifest-sha256 "$MANIFEST_SHA256"
/usr/bin/python3 "$SOURCE_OPS" prepare-staging --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" snapshot --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" install --operation-dir "$OPERATION_DIR"

printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
EXPECTED_INSTALLED_OPS_SHA256="$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { print $1 }' "$SOURCE_MANIFEST")"
test "$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { count += 1 } END { print count + 0 }' "$SOURCE_MANIFEST")" -eq 1
[[ "$EXPECTED_INSTALLED_OPS_SHA256" =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n' "$EXPECTED_INSTALLED_OPS_SHA256" /usr/local/sbin/degen-prod-db-backup-ops | sha256sum --check --strict -
/usr/local/sbin/degen-prod-db-backup-ops probe-remote --operation-dir "$OPERATION_DIR"
printf '%s  %s\n' "$EXPECTED_INSTALLED_OPS_SHA256" /usr/local/sbin/degen-prod-db-backup-ops | sha256sum --check --strict -
/usr/local/sbin/degen-prod-db-backup-ops record-dry-run --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops show-state --operation-dir "$OPERATION_DIR"
REMOTE_BOOTSTRAP
} > "$BOOTSTRAP_SCRIPT"
chmod 0700 "$BOOTSTRAP_SCRIPT"
brev exec openclaw-9902ae --host "@$BOOTSTRAP_SCRIPT"
```

Stop if the helper fails. Do not skip to pruning. Preserve `OPERATION_DIR`, the
approved hashes, all helper output, and the dry-run report for review.

## Conditional recovery only

This is not part of the normal success path. Use it only when source-routed
`show-state` reports one of the explicitly recoverable in-progress phases
below. Recovery authority comes from the original approval for the exact
interrupted transaction: Gate 2 for install/probe/dry-run phases, Gate 3 for
policy/observation phases, or the separately approved manual rollback for its
rollback phases. An earlier Gate 2 approval never authorizes recovery of a
later Gate 3 or manual-rollback transaction. A different operation directory,
commit, archive, manifest, host, or stable phase needs a new preflight and
approval.

Run on Green as root through a non-secret `brev exec ... @script.sh` wrapper.
Before executing source code again, bind both the manifest and source helper to
the originally approved manifest digest:

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077
(( EUID == 0 ))
: "${OPERATION_DIR:?set the exact approved operation directory}"
: "${SOURCE_OPS:?set the exact approved source helper path}"
: "${SOURCE_MANIFEST:?set the exact approved source manifest path}"
: "${MANIFEST_SHA256:?set the approved manifest SHA-256}"
test "$SOURCE_OPS" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"
test "$SOURCE_MANIFEST" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-assets.sha256"
printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
EXPECTED_SOURCE_OPS_SHA256="$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { print $1 }' "$SOURCE_MANIFEST")"
test "$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { count += 1 } END { print count + 0 }' "$SOURCE_MANIFEST")" -eq 1
[[ "$EXPECTED_SOURCE_OPS_SHA256" =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n' "$EXPECTED_SOURCE_OPS_SHA256" "$SOURCE_OPS" | sha256sum --check --strict -
RECORDED_PHASE="$(
  /usr/bin/python3 "$SOURCE_OPS" show-state --operation-dir "$OPERATION_DIR" |
    /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["phase"])'
)"
case "$RECORDED_PHASE" in
  installing|recovering|probing|dry_run_recording|policy_enabling|observing|recovery_required|recovering_policy|manual_rollback|recovering_probe|recovering_guard)
    /usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"
    ;;
  *)
    printf '%s\n' "ERROR: recovery refused for non-interrupted phase: $RECORDED_PHASE" >&2
    exit 1
    ;;
esac
```

Never resolve interrupted recovery through a possibly mixed installed binary.
If verified source is unavailable or `show-state` cannot prove the exact bound
transaction, stop and investigate; do not reconstruct state manually.

## Stable checkpoint resume

Do not run `recover` for a stable phase and do not blindly replay the whole
normal block. First repeat the approved-manifest/source-helper check from the
recovery wrapper, run source-routed `show-state`, and continue only with the
single next action shown here:

| Stable phase | Next action |
|---|---|
| `source_verified` | source `prepare-staging` |
| `staging_prepared` | source `snapshot` |
| `snapshotted` | source `install` |
| `installed` | reverify installed helper, then `probe-remote` |
| `probed` | reverify installed helper, then `record-dry-run` and print `show-state` |
| `dry_run_recorded` | stop and seek Gate 3 approval |
| `policy_enabled` | wait for the next scheduled run, then reverify and `observe` |
| `observed` | no mutation; retain the final evidence |

Any other phase must match the Conditional recovery allowlist or stop for a
new investigation and preflight.

## Gate 3: approve remote pruning

Review the recorded dry-run report and durable operation state. Zero candidates still require approval because this changes future scheduled behavior. Before
asking for `proceed`, disclose:

- enabling changes only the helper-managed prune flag and preserves all other
  verified configuration;
- the helper may perform timer stop/start while applying the policy;
- rclone configuration may refresh its token during later scheduled access;
- the remote probe creates and deletes only disposable objects at its unique
  probe prefix;
- remote deletion is potentially irreversible; local deletion can also be
  irreversible. Later scheduled runs can delete verified out-of-policy backup
  objects even when today's candidate count is zero;
- rollback cannot restore deleted local backups or deleted OneDrive objects.

After a new explicit approval, put the following commands in a non-secret root
script with the exact approved values and run it on Green using the same
`brev exec openclaw-9902ae --host @script.sh` pattern. Re-check both the
approved manifest and installed helper immediately before enabling, then invoke
only the installed helper:

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077
(( EUID == 0 ))
: "${OPERATION_DIR:?set the exact approved operation directory}"
: "${SOURCE_MANIFEST:?set the exact approved source manifest path}"
: "${MANIFEST_SHA256:?set the approved manifest SHA-256}"
test "$SOURCE_MANIFEST" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-assets.sha256"
printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
EXPECTED_INSTALLED_OPS_SHA256="$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { print $1 }' "$SOURCE_MANIFEST")"
test "$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { count += 1 } END { print count + 0 }' "$SOURCE_MANIFEST")" -eq 1
[[ "$EXPECTED_INSTALLED_OPS_SHA256" =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n' "$EXPECTED_INSTALLED_OPS_SHA256" /usr/local/sbin/degen-prod-db-backup-ops | sha256sum --check --strict -
/usr/local/sbin/degen-prod-db-backup-ops enable-prune --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops show-state --operation-dir "$OPERATION_DIR"
```

Do not manually start the backup service. Wait for the next scheduled timer run
and record its actual completion evidence. Then re-check the installed helper
and observe that scheduled run:

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077
(( EUID == 0 ))
: "${OPERATION_DIR:?set the exact approved operation directory}"
: "${SOURCE_MANIFEST:?set the exact approved source manifest path}"
: "${MANIFEST_SHA256:?set the approved manifest SHA-256}"
test "$SOURCE_MANIFEST" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-assets.sha256"
printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
EXPECTED_INSTALLED_OPS_SHA256="$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { print $1 }' "$SOURCE_MANIFEST")"
test "$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { count += 1 } END { print count + 0 }' "$SOURCE_MANIFEST")" -eq 1
[[ "$EXPECTED_INSTALLED_OPS_SHA256" =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n' "$EXPECTED_INSTALLED_OPS_SHA256" /usr/local/sbin/degen-prod-db-backup-ops | sha256sum --check --strict -
/usr/local/sbin/degen-prod-db-backup-ops observe --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops show-state --operation-dir "$OPERATION_DIR"
```

The observation must prove a fresh successful scheduled run, exact local and
remote inventory receipts, checksum verification, policy decisions, timer
restoration, and the final durable phase. A failed or stale run is not success.

## Evidence and accepted limitation

Retain the root-only operation directory and the non-secret summary supplied by
the helper. Do not print or copy the real app environment, managed backup
environment, database URL, rclone configuration, token material, database dump,
or `rclone.conf.audit` contents into tickets or chat.

`pg_restore --list` proves that a custom-format archive is structurally
readable. `pg_restore --list` does not prove an end-to-end logical restore. The
canceled full restore rehearsal remains an explicit recovery limitation and
must not be described as restore proof.

## Separately approved manual rollback

Stable-phase rollback is a new production mutation. Present a fresh preflight
with the exact operation directory, current phase, targets, snapshot evidence,
timer state, effects, verification, and limits, then wait for a new explicit
`proceed`. In the non-secret Green root wrapper, re-bind the manifest and source
helper first, then invoke only the verified source helper:

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077
(( EUID == 0 ))
: "${OPERATION_DIR:?set the exact approved operation directory}"
: "${SOURCE_OPS:?set the exact approved source helper path}"
: "${SOURCE_MANIFEST:?set the exact approved source manifest path}"
: "${MANIFEST_SHA256:?set the approved manifest SHA-256}"
test "$SOURCE_OPS" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"
test "$SOURCE_MANIFEST" = "$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-assets.sha256"
printf '%s  %s\n' "$MANIFEST_SHA256" "$SOURCE_MANIFEST" | sha256sum --check --strict -
EXPECTED_SOURCE_OPS_SHA256="$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { print $1 }' "$SOURCE_MANIFEST")"
test "$(awk '$2 == "deploy/linux/degen-prod-db-backup-ops.py" { count += 1 } END { print count + 0 }' "$SOURCE_MANIFEST")" -eq 1
[[ "$EXPECTED_SOURCE_OPS_SHA256" =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n' "$EXPECTED_SOURCE_OPS_SHA256" "$SOURCE_OPS" | sha256sum --check --strict -
/usr/bin/python3 "$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"
```

Rollback restores only targets represented in the verified host snapshot. It
does not automatically restore `rclone.conf.audit`, because that file is audit
evidence rather than a rollback target. It cannot restore deleted local backups
or deleted OneDrive objects, and it does not restart the application, worker,
bot, or PostgreSQL.
If rollback fails or leaves a recovery phase, use Conditional recovery only;
never copy snapshot files into place by hand.
