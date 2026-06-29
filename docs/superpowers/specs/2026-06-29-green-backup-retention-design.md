# Green PostgreSQL Backup Retention Design

Date: 2026-06-29
Status: Approved for direct implementation by Jeffrey

## Problem

Green's host-local backup job keeps seven days of PostgreSQL dumps while each current dump is about 17.3 GB. Seven local dumps would require roughly 121 GB before accounting for temporary output and manual backups, but Green currently has about 101 GB free. The current remote policy keeps 30 daily dumps, which trends toward roughly 520 GB.

Overwriting one fixed backup filename would reduce visible storage, but it could replace the only good recovery point with a partial, corrupt, or logically bad backup. Retention must therefore remain timestamped and transactional.

## Current State

- Production runs on Green (`openclaw-9902ae`).
- The host-local timer runs nightly at 03:15 America/Los_Angeles with up to 20 minutes of randomized delay.
- The installed job writes custom-format PostgreSQL dumps under `/opt/degen/backups/db` and uploads dump/checksum pairs to OneDrive with `rclone`.
- Live retention is `KEEP_LOCAL_DAYS=7` and `KEEP_REMOTE_DAYS=30`.
- The installed Green script, service, timer, and environment template are not tracked in Git; the repository only contains the legacy Windows backup implementation and planning documents.
- Manual preservation directories are outside automatic retention and must not be deleted by this change.

## Success Criteria

1. Every successful backup remains a uniquely timestamped dump plus checksum sidecar.
2. Green keeps exactly the two newest completed and verified dump/checksum pairs.
3. A new backup is validated locally and remotely before any prior local pair is deleted.
4. OneDrive keeps the union of:
   - the newest backup for each of the latest seven UTC dates;
   - the newest backup for each of the latest four ISO weeks;
   - the newest backup for each of the latest three calendar months.
5. Unknown filenames, incomplete pairs, manual backups, and unparseable objects are never deleted automatically.
6. Remote pruning supports a deterministic dry-run that lists exact candidates before deletion is enabled.
7. Concurrent backup invocations are serialized with a host lock.
8. Backup failures are visible in systemd/journal logs and leave the last good backups intact.
9. The Linux backup script, service, timer, sanitized environment template, tests, and runbook are tracked in Git.
10. No application, worker, bot, or PostgreSQL service restart is required.

## Scope

- Add a repo-managed Green backup orchestrator and deterministic retention planner.
- Add systemd service/timer files and a secret-free environment template.
- Add focused tests for retention selection, pair safety, dry-run behavior, and failure ordering.
- Back up the current host-local backup configuration before installation.
- Install the reviewed files on Green and reload only systemd metadata/timer configuration.
- Inventory OneDrive read-only, run remote pruning in dry-run mode, verify exact candidates, then enable the approved policy.

## Non-Scope

- Changing PostgreSQL credentials, schema, data, or service configuration.
- Restarting or redeploying the Degen application.
- Deleting manual backup directories.
- Purging OneDrive recycle-bin contents.
- Implementing PostgreSQL physical/incremental backup tooling such as pgBackRest or WAL-G.
- Repairing separately discovered legacy credential exposure; that remains a merge blocker for the security branch.

## Architecture and Data Flow

The job keeps the existing custom-dump approach and changes retention from age-based deletion to verified count/tier selection:

1. Acquire an exclusive backup lock.
2. Preflight required commands, database configuration, destination paths, and free space.
3. Write `.<timestamp>.dump.partial` on Green with `pg_dump -Fc -Z6`.
4. Validate the archive with `pg_restore --list` and calculate SHA-256.
5. Atomically publish the local timestamped dump and checksum sidecar.
6. Upload both files under temporary remote names.
7. Verify remote object sizes and checksum-sidecar content, then atomically move them to final remote names.
8. Prune local recognized pairs to the newest two.
9. Calculate the remote 7-daily/4-weekly/3-monthly keep set. Log exact delete candidates; delete only when the explicit remote-prune flag is enabled.

The retention planner accepts inventory metadata and emits deterministic keep/delete decisions. It never receives database credentials.

## Safety Rules

- No in-place overwrite of the only backup.
- No local prune before local archive validation and verified remote publication.
- No remote deletion of unknown names, incomplete pairs, or the newest successful backup.
- A failed upload, size mismatch, sidecar mismatch, or retention-planner error stops pruning.
- Local retention is count-based, avoiding `find -mtime` rounding surprises.
- Production secrets remain only in the root-owned host environment file; the tracked template contains placeholders.
- Remote pruning is initially deployed disabled and enabled only after exact dry-run review.

## Alternatives Considered

1. **Overwrite one fixed daily file:** smallest storage footprint, rejected because a failed run can destroy the only recovery point.
2. **Keep two local and retain 30 remote days:** smallest implementation change, but remote storage still trends toward about 520 GB and age-only pruning is imprecise.
3. **Keep two local with 7/4/3 remote tiers:** approved. It uses about 34.7 GB steady-state locally, about 52 GB at peak during the next dump, and at most roughly 243 GB remotely at today's dump size.

## Verification

- Unit tests cover daily/weekly/monthly overlap, malformed names, incomplete pairs, newest-backup protection, and deterministic output.
- Integration-style tests use temporary directories and stubbed `pg_dump`, `pg_restore`, and `rclone` commands to prove pruning happens only after verification.
- Run shell syntax/static checks and the relevant pytest module, followed by the full repository suite.
- On Green, verify installed hashes, ownership, modes, timer schedule, dry-run remote candidates, and unchanged application/database PIDs.
- Allow one successful scheduled or explicitly approved manual backup, then verify local count, remote pair integrity, disk space, and journal status.

## Rollback

Before installation, copy the current script, service, timer, and environment file into a root-only timestamped configuration backup. Rollback restores those exact files, reloads systemd metadata, and returns the timer to its prior state. No database rollback is involved.

Remote deletion is reversible only through OneDrive retention/recycle behavior, so exact candidate review precedes enabling it. Local automatic pruning affects only recognized backup pairs after a verified remote copy exists.

## Risks

- OneDrive object metadata may not expose SHA-256 directly; the job therefore verifies size plus the uploaded checksum-sidecar content.
- OneDrive recycle-bin retention may continue consuming quota after remote deletion.
- A large database-growth event could increase temporary disk pressure; preflight and post-run free-space checks remain mandatory.
- Installing host-level systemd files is production-visible configuration work even though it does not restart the application.

## Open Questions

None. The approved policy is two local backups and remote 7-daily/4-weekly/3-monthly retention, with remote deletion gated by an exact dry-run review.
