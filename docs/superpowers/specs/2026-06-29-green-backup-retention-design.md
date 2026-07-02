# Green PostgreSQL Backup Retention Design

Dates: 2026-06-29; revised 2026-07-01
Status: Revised design approved by Jeffrey; written-spec review completed

## Problem

Green's host-local job keeps seven days of PostgreSQL dumps while each current dump is about 17.3 GB. Seven local dumps would require roughly 121 GB before temporary output and manual preservation copies, but Green had about 101 GB free during the audit. The current remote policy keeps 30 daily dumps, trending toward roughly 520 GB.

Overwriting one fixed filename would reduce storage but could replace the only good recovery point with a partial, corrupt, or logically bad backup. Retention must remain timestamped, verified, and transactional.

## Confirmed Current State

- Production is Green (`openclaw-9902ae`); the application remains under `/opt/degen/app`.
- The timer is enabled and active. It runs nightly at 03:15 America/Los_Angeles with up to 20 minutes of randomized delay and persistent catch-up.
- The current job writes custom-format PostgreSQL dumps under `/opt/degen/backups/db` and uploads dump/checksum pairs to `onedrive:backups/degen-db` with rclone 1.74.1.
- Live retention is `KEEP_LOCAL_DAYS=7` and `KEEP_REMOTE_DAYS=30`.
- Green has only Python 3.10.12 available through its system `python3` paths.
- `/run/lock` is root-owned but mode `1777`; the proposed lock must not live directly there under a predictable filename.
- `/etc/degen/rclone.conf` is a regular root-owned `0600` file under a root-owned `0750` directory.
- `/var/log` is `root:syslog` mode `0775` on Green. The `degen` account belongs only to group `degen`, not `syslog`; the dedicated backup-log child therefore remains outside the app account's writable boundary.
- `/etc/degen/prod-db-backup.env` currently contains only simple, single-line assignments, no continuations, multiline quotes, or duplicate keys. Its legacy `LOG_DIR=/var/log/degen` value points into the app-owned log directory; staging must migrate exactly that value to the dedicated root-only backup log directory and reject every other override.
- `/opt/degen/backups/db` is a root-owned, non-symlink `0750` directory with one complete dump/checksum pair.
- The installed script, service, timer, environment contract, and runbook are not yet tracked on `main`.
- Manual preservation directories are outside automatic retention and must never be deleted by this change.

## Success Criteria

1. Every successful backup remains a uniquely timestamped dump plus checksum sidecar.
2. Green keeps exactly the two newest completed and verified dump/checksum pairs.
3. A new backup is validated locally and remotely before any prior local pair is deleted.
4. The second local pair that would remain after pruning is revalidated before older pairs are removed.
5. OneDrive keeps the union of the newest backup from each of the latest seven distinct UTC dates, four distinct ISO weeks, and three distinct calendar months.
6. Retention pruning never deletes unknown names, incomplete pairs, manual backups, unowned temporary objects, future timestamps, or unparseable objects. State-tracked temporary objects created by the current run or disposable probe may be cleaned up.
7. Remote pruning emits a deterministic exact candidate list and remains disabled until a separate approval after dry-run review.
8. Concurrent manual, timer, and deployment activity is serialized with a validated root-only lock.
9. Backup, cleanup, validation, and rollback failures remain visible in journal/operator output without replacing the primary failure status. The backup logger never follows a symlink or writes through a multiply linked, wrongly owned, or non-private log path.
10. Manual validation and the scheduled service use the same effective configuration.
11. Repo-managed installation assets use only bytes exported from an immutable reviewed Git commit and compared to a reviewed manifest. The host-derived environment is staged separately, preserves unrelated assignments, and receives its own manifest entry.
12. Installation is transactional: staged and validated before mutation, protected from timer races, and automatically restored on an installation failure.
13. Every production phase is resumable from validated root-only operation state rather than relying on one long-lived shell.
14. The next scheduled-run proof must be newer than installation and policy enablement; stale successful backups cannot satisfy it.
15. The Linux scripts, service, timer, sanitized template, tests, design, plan, and runbook are tracked in Git.
16. No application, PostgreSQL, web, worker, or bot service restart is required.

## Scope

- Make the planner and tests compatible with Green's Python 3.10 runtime.
- Add a repo-managed backup orchestrator and credential-free deterministic retention planner.
- Add the systemd service/timer, a secret-free environment template, focused tests, and a production runbook.
- Replace the predictable `/run/lock` file with a lock below a validated root-only runtime directory.
- Replace the shared app-owned backup log path with `/var/log/degen-prod-db-backup`, a root-owned mode-`0700` directory containing only a root-owned mode-`0600`, single-link regular log file opened with no-follow and path/descriptor identity checks.
- Make the script load and validate the same managed service environment for timer and direct validation modes.
- Reject complex `EnvironmentFile` constructs before mutation; preserve unrelated simple assignments and normalize every managed key to one canonical assignment.
- Replace the disproven rclone `--immutable` assumption with the locally verified rclone 1.74.1 no-clobber composition `--ignore-existing --error-on-no-transfer`, plus case-insensitive inventory checks and post-operation verification.
- Publish the reviewed branch without merging it, export the exact reviewed commit into a root-owned staging directory, and verify a fixed manifest before installation.
- After explicit production approval, temporarily stop only the backup timer, verify the backup service is inactive, acquire the backup lock, install staged files transactionally, reload systemd metadata, and restore the timer's exact prior active/enabled state.
- Snapshot current host configuration and persist a root-only machine-readable operation record for installation, prune enablement, rollback, and next-run proof.
- Treat rclone OAuth refresh as a possible approved credential-file mutation after the production checkpoint. Snapshot metadata and contents for audit/emergency recovery, but do not automatically restore a potentially stale rotated token.
- After explicit approval, run a disposable remote-prefix probe to confirm OneDrive behavior before enabling deletion on the production prefix.

## Non-Scope

- Merging this branch into `main` during installation staging.
- Changing PostgreSQL credentials, schema, data, or service configuration.
- Restarting or redeploying the Degen application or its web, worker, bot, or PostgreSQL services.
- Deleting manual backup directories or OneDrive recycle-bin contents.
- Supporting arbitrary multiline/continued systemd environment files; the installer fails closed and requires a separate migration if those constructs appear.
- Supporting another writer in `onedrive:backups/degen-db`; the backup job owns that namespace exclusively.
- Implementing physical/incremental backup tooling such as pgBackRest or WAL-G.
- Repairing the separately discovered legacy database credential exposure; that remains a distinct merge blocker.

## Runtime Architecture

### Retention planner

The Python 3.10-compatible planner accepts only inventory metadata and policy counts. It recognizes exact owned names, forms complete pairs, protects malformed/incomplete/future objects, and emits stable keep/delete/protected decisions. It never receives database or rclone credentials.

### Backup orchestrator

The Bash orchestrator performs this ordered flow:

1. Load the same root-owned managed environment used by systemd. Reject duplicates, unsafe values, continuations, or multiline constructs.
2. Validate the effective database identity, host-derived prefix, local paths, remote path, policy, rclone config metadata, and free space.
3. Validate and acquire a lock under `/run/degen-prod-db-backup/`, whose parent is root-owned, non-symlinked, and not group/world writable.
4. Create random same-directory partial files with `mktemp`.
5. Run `pg_dump -Fc -Z6`, validate with `pg_restore --list`, and calculate SHA-256.
6. Publish the local pair with no-replace semantics.
7. Upload both objects under unpredictable temporary remote names using strict no-existing flags.
8. Verify remote temporary object size and checksum-sidecar content.
9. Move to final names with the same strict flags; verify source disappearance, final size, and sidecar content.
10. Revalidate the previous pair that will remain locally. If it fails, stop without pruning.
11. Prune only planner-approved local names to the two verified pairs.
12. Emit the remote 7/4/3 plan and delete exact candidates only in normal run mode when the explicit flag is enabled.

Cleanup claims ownership only after successful creation/upload. Signals force a nonzero exit, owned cleanup failures produce secret-safe warnings, and all cleanup attempts preserve the primary status.

### Effective configuration

The service and direct modes use `/etc/degen/prod-db-backup.env` through one parser. The supported host file is intentionally limited to comments, blank lines, and simple one-line assignments. The parser fails before mutation on continuations, multiline quoting, malformed managed keys, unsafe paths, or semantic duplicates.

The deployment requires the audited effective values unless a new design is approved:

- `APP_ENV_FILE=/opt/degen/web.env`
- `BACKUP_DIR=/opt/degen/backups/db`
- `LOG_DIR=/var/log/degen-prod-db-backup`
- `RCLONE_CONFIG=/etc/degen/rclone.conf`
- `RCLONE_REMOTE_PATH=onedrive:backups/degen-db`

`BACKUP_PREFIX` is derived on-host from a verified existing complete pair, checked against the actual database name and short hostname, and persisted as a non-secret managed key.

The operations staging helper may normalize only the audited live legacy value
`LOG_DIR=/var/log/degen` to the dedicated value above. The installed runtime
environment parser accepts only the dedicated value; arbitrary log paths remain
fail-closed. `LogsDirectory=degen-prod-db-backup` and
`LogsDirectoryMode=0700` establish the systemd ownership contract, while the
runtime logger independently validates directory and file type, owner, mode,
link count, and path/descriptor identity before every write session.

The fixed parent `/var/log` must remain a real effective-UID-owned directory
with no world-write bit. Its audited `root:syslog` mode `0775` is accepted:
group-write on the parent cannot redirect writes through the held child/file
descriptors, while the app account is not a `syslog` member. A world-writable,
wrong-owner, symlinked, or path-replaced parent remains fail-closed.

## Transactional Installation

1. Complete local tests and whole-change review.
2. Push the reviewed feature branch without merging it and record the exact commit SHA and manifest.
3. Present a production preflight naming every target and possible side effect. Obtain explicit `proceed`.
4. Export only the required files from that exact commit into a root-owned staging directory outside `/opt/degen/app`; verify the commit and manifest.
5. Snapshot the current script, service, timer, environment, planner presence/absence, rclone config audit copy, timer state, PIDs, hashes, modes, and non-secret effective configuration. Validate the snapshot manifest immediately.
6. Precompute the final environment and all install files in staging. Run syntax, policy, path, and metadata checks before the first host-file replacement. Disclose that service validation can create `/var/log/degen-prod-db-backup` as root:root mode `0700` and `prod-db-backup.log` as root:root mode `0600`; these paths are outside the seven snapshotted install targets.
7. Verify the service is inactive, stop only the backup timer, acquire the deployment/backup lock, and install with a rollback trap active.
8. Reload systemd metadata, run non-destructive validation with the exact service configuration, restore the timer's prior state, and record the installation epoch and installed hashes in operation state.
9. Run remote access only after the approved rclone audit snapshot. Bracket every rclone command group with config hash/mtime evidence because OAuth refresh may rewrite the file.
10. Run and pass the state-tracked disposable remote-prefix collision/cleanup probe, then produce the production-prefix dry-run and present exact candidates.
11. A second explicit approval is required even when the candidate set is empty, because future scheduled runs may delete.
12. In a fresh phase, revalidate installed hashes, environment, rclone evidence, and inventory. Stop only the backup timer, verify the service is inactive, reacquire the backup lock, stage the environment change and policy epoch transactionally, then enable pruning and restore the timer's prior state. A failure restores the prior environment and removes the incomplete epoch record before releasing the lock.

No checkout switch, app deployment, or application/database service restart occurs.

## Safety Rules

- No in-place overwrite of the only backup.
- No local prune before local archive validation, verified remote publication, and validation of both pairs that will remain.
- No retention deletion of unknown, incomplete, unowned temporary, future, or newest objects. State-tracked current-run and disposable-probe temporary objects may be cleaned up.
- No remote command before the explicit production checkpoint and root-only rclone audit snapshot.
- No reliance on `--immutable` for direct rclone operations; local v1.74.1 probes proved it can overwrite a different destination.
- The production remote prefix is single-writer. If another writer is discovered, stop and redesign.
- The rclone no-existing composition is combined with case-insensitive prechecks, unpredictable temp names, exclusive host locking, and final verification. OneDrive does not expose a universal atomic create-if-absent guarantee, so a hostile post-check race remains a documented platform limit.
- A failed dump, validation, upload, move, verification, planner, cleanup, environment parse, staging, or manifest check stops destructive work.
- The timer is restored to its exact prior state on success or rollback.
- Every later policy mutation repeats the timer-stop, service-inactive, lock, staged-write, rollback, and timer-restoration controls; no approval phase relies on the installation shell remaining alive.
- Production secrets remain only in root-owned host files; tracked executable/configuration assets contain no secret values or host-derived backup prefix.
- Backup logging is isolated from `/var/log/degen`; the logger rejects symlinked directories/files, non-regular or multiply linked files, wrong ownership, non-private modes, and pathname/open-descriptor identity drift before external backup work.

## Verification

- Planner tests cover policy overlap, ISO boundaries, deterministic output, malformed/incomplete/future objects, and Python 3.10 execution.
- Orchestrator tests model real rclone 1.74.1 direct-operation semantics, strict no-existing flags, signals, cleanup failures, symlink/no-clobber paths, secure log-directory creation and reuse, service/direct configuration parity, and destructive failure ordering.
- A local official-rclone 1.74.1 probe verifies absent/existing behavior for `copyto` and `moveto` with the selected flags.
- Shell syntax, systemd directives, timer calendar, focused tests, and the full repository suite pass before each commit.
- The production preflight verifies Green's Python version, exact source SHA/manifest, effective non-secret configuration, the live `root:syslog` mode-`0775` non-world-writable `/var/log` parent and `degen` non-membership in `syslog`, lock/runtime directory, backup directory, rclone metadata, timer/service state, disk, existing pair integrity, and unchanged application/database PIDs.
- A disposable approved OneDrive prefix verifies no-clobber and cleanup behavior without touching production backup objects.
- The next-run gate requires service success, a trigger/start after the recorded policy epoch, a fresh success log, fresh dump/sidecar mtimes, valid sidecar grammar, SHA-256, archive listing, installed hash parity, local two-pair policy, remote integrity, and unchanged application/database PIDs.

## Rollback

The root-only snapshot records current files, absence markers, hashes, metadata, timer state, PIDs, effective configuration, and operation epochs. Installation rollback validates the snapshot before any restore, restores saved script/planner/unit/environment contents and modes, reloads systemd metadata, restores the timer's exact prior state, and verifies installed hashes and unchanged application/database PIDs.

The dedicated `/var/log/degen-prod-db-backup` directory and its log are
operational evidence outside the seven snapshotted install targets. Automatic
recovery and rollback do not delete them. Removing them requires a separate
explicit cleanup decision after confirming no retained evidence is needed.

The rclone config audit copy is not restored automatically because OAuth refresh-token rotation may invalidate the old copy. Credential recovery requires a separate explicit decision after comparing current authentication state and audit evidence.

Rollback cannot recover local pairs already pruned or remote objects already deleted. These irreversible effects are why installation is deployed with pruning disabled and why exact candidate review has a second approval gate.

## Risks and Accepted Limits

- OneDrive object metadata may not expose SHA-256; the job verifies size plus uploaded checksum-sidecar content.
- OneDrive/rclone has no universal atomic create-if-absent primitive for direct file operations. Strict flags, exclusive ownership, unpredictable temporary names, locks, and verification reduce this to a documented external-writer race.
- Ordinary rclone access may refresh/rewrite `/etc/degen/rclone.conf`; the audit copy can itself become stale and is not automatic rollback material.
- Stopping the backup timer during installation temporarily skips trigger delivery, but the timer remains enabled and persistent and is restored immediately. The application and database continue running.
- A database-growth event can increase temporary disk pressure; preflight requires current database size plus a free-space reserve.
- Installation modifies host-level systemd/configuration files even though application processes are not restarted.
- If Green's simple environment-file structure changes, deployment stops rather than guessing at systemd multiline semantics.
- `pg_restore --list` proves archive readability, not a full logical restore. The previously canceled full restore rehearsal remains an accepted recovery risk; this change does not claim end-to-end restore proof.

## Alternatives Considered

1. **Overwrite one fixed daily file:** rejected because a failed run can destroy the only recovery point.
2. **Keep two local and 30 remote days:** rejected because remote storage still trends toward about 520 GB.
3. **Use direct rclone operations with `--immutable`:** rejected after official rclone 1.74.1 local probes showed different existing destinations are overwritten.
4. **Install directly from `/opt/degen/app`:** rejected because the checkout may be stale or dirty and is not immutable reviewed provenance.
5. **Leave the timer active during installation:** rejected because a trigger can execute mixed old/new state.
6. **Use a full systemd environment parser:** rejected as unnecessary for the audited simple host file; fail-closed detection of complex constructs is smaller and safer.
7. **Two local with 7/4/3 remote tiers plus revised safeguards:** approved in conversation. Steady local use is about 34.7 GB, peak about 52 GB, and remote use at most roughly 243 GB at the current dump size.

## Open Questions

None for the design. Production execution still requires the documented preflight and two explicit approvals: installation/rclone validation, then remote-prune enablement.
