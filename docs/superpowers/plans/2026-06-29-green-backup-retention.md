# Green PostgreSQL Backup Retention Implementation Plan

> **SUPERSEDED FOR PRODUCTION EXECUTION:** Do not execute Tasks 5-6 in this file. The reviewed remediation and production workflow is `docs/superpowers/plans/2026-06-30-green-backup-retention-hardening.md`. This file remains as implementation history for the initial branch.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Green's untracked age-based PostgreSQL backup job with a tested, repo-managed job that retains the two newest verified local pairs and a deterministic 7-daily/4-weekly/3-monthly OneDrive set.

**Architecture:** A credential-free Python planner classifies recognized dump/checksum pairs and emits deterministic keep/delete/protected decisions. A strict Bash orchestrator owns database dumping, archive validation, checksum creation, temporary remote upload, remote verification, atomic publication, and post-verification pruning. Systemd assets preserve the current schedule and root-only secret boundary; remote deletion remains disabled until its exact dry-run candidates are reviewed.

**Tech Stack:** Python 3.11+, Bash 5.1, PostgreSQL 17 client tools, rclone 1.74, systemd 249, pytest.

---

### Task 1: Build the credential-free retention planner

**Files:**
- Create: `deploy/linux/degen-prod-db-retention.py`
- Create: `tests/test_degen_prod_db_retention.py`

- [ ] **Step 1: Write the failing planner tests**

Create `tests/test_degen_prod_db_retention.py` with an `importlib.util` loader for the standalone script and helpers that produce `<prefix><timestamp>.dump` plus `.sha256`. Add these concrete tests:

```python
from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLANNER = ROOT / "deploy" / "linux" / "degen-prod-db-retention.py"
PREFIX = "degen_green_prod_green_"
NOW = datetime(2026, 6, 29, 23, 0, tzinfo=UTC)


def load_planner():
    spec = importlib.util.spec_from_file_location("degen_prod_db_retention", PLANNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pair(stamp: str) -> list[str]:
    dump = f"{PREFIX}{stamp}.dump"
    return [dump, f"{dump}.sha256"]


def test_local_keeps_exactly_two_newest_complete_pairs() -> None:
    module = load_planner()
    names = pair("20260627T031500Z") + pair("20260628T031500Z") + pair("20260629T031500Z")

    plan = module.plan_inventory(names, mode="local", prefix=PREFIX, now=NOW, local_count=2)

    assert [item["timestamp"] for item in plan["keep"]] == ["20260629T031500Z", "20260628T031500Z"]
    assert [item["timestamp"] for item in plan["delete"]] == ["20260627T031500Z"]


def test_remote_unions_seven_dates_four_iso_weeks_and_three_months() -> None:
    module = load_planner()
    stamps = [
        "20260430T031500Z", "20260503T031500Z", "20260531T031500Z",
        "20260601T031500Z", "20260607T031500Z", "20260614T031500Z",
        "20260621T031500Z", "20260622T031500Z", "20260623T031500Z",
        "20260624T031500Z", "20260625T031500Z", "20260626T031500Z",
        "20260627T031500Z", "20260628T031500Z", "20260629T031500Z",
    ]
    names = [name for stamp in stamps for name in pair(stamp)]

    plan = module.plan_inventory(
        names,
        mode="remote",
        prefix=PREFIX,
        now=NOW,
        daily=7,
        weekly=4,
        monthly=3,
    )

    kept = {item["timestamp"]: set(item["reasons"]) for item in plan["keep"]}
    assert "20260629T031500Z" in kept and "newest" in kept["20260629T031500Z"]
    assert len({stamp[:8] for stamp, reasons in kept.items() if "daily" in reasons}) == 7
    assert sum("weekly" in reasons for reasons in kept.values()) == 4
    assert sum("monthly" in reasons for reasons in kept.values()) == 3


def test_remote_handles_iso_year_boundary() -> None:
    module = load_planner()
    names = pair("20251228T031500Z") + pair("20251229T031500Z") + pair("20260104T031500Z") + pair("20260105T031500Z")
    plan = module.plan_inventory(
        names,
        mode="remote",
        prefix=PREFIX,
        now=datetime(2026, 1, 6, tzinfo=UTC),
        daily=0,
        weekly=2,
        monthly=0,
    )
    weekly = {item["timestamp"] for item in plan["keep"] if "weekly" in item["reasons"]}
    assert weekly == {"20260104T031500Z", "20260105T031500Z"}


def test_unknown_incomplete_temporary_and_future_objects_are_protected() -> None:
    module = load_planner()
    complete = pair("20260629T031500Z")
    incomplete_dump = f"{PREFIX}20260628T031500Z.dump"
    future = pair("20260701T031500Z")
    names = complete + [incomplete_dump, "manual-preserve.dump", ".upload-partial"] + future

    plan = module.plan_inventory(names, mode="local", prefix=PREFIX, now=NOW, local_count=2)

    protected = {item["name"]: item["reason"] for item in plan["protected"]}
    assert protected[incomplete_dump] == "incomplete-pair"
    assert protected["manual-preserve.dump"] == "unknown-name"
    assert protected[".upload-partial"] == "unknown-name"
    assert protected[future[0]] == "future-timestamp"
    assert protected[future[1]] == "future-timestamp"
    assert not plan["delete"]


def test_decision_is_stable_across_input_order() -> None:
    module = load_planner()
    names = pair("20260627T031500Z") + pair("20260628T031500Z") + pair("20260629T031500Z") + ["unknown"]
    kwargs = {"mode": "remote", "prefix": PREFIX, "now": NOW, "daily": 1, "weekly": 1, "monthly": 1}
    assert module.plan_inventory(names, **kwargs) == module.plan_inventory(reversed(names), **kwargs)
```

- [ ] **Step 2: Run the planner tests and verify the missing-file failure**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py -q
```

Expected: collection or import fails because `deploy/linux/degen-prod-db-retention.py` does not exist.

- [ ] **Step 3: Implement the planner**

Create an executable `deploy/linux/degen-prod-db-retention.py` with:

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from typing import Iterable


def _stamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def _record(dump: str, timestamp: datetime, reasons: set[str]) -> dict[str, object]:
    return {
        "dump": dump,
        "checksum": f"{dump}.sha256",
        "timestamp": timestamp.strftime("%Y%m%dT%H%M%SZ"),
        "reasons": sorted(reasons),
    }


def plan_inventory(
    names: Iterable[str],
    *,
    mode: str,
    prefix: str,
    now: datetime,
    local_count: int = 2,
    daily: int = 7,
    weekly: int = 4,
    monthly: int = 3,
) -> dict[str, object]:
    if mode not in {"local", "remote"}:
        raise ValueError("mode must be local or remote")
    for label, value in {"local_count": local_count, "daily": daily, "weekly": weekly, "monthly": monthly}.items():
        if value < 0:
            raise ValueError(f"{label} must be non-negative")

    unique = sorted({name.strip() for name in names if name.strip()})
    pattern = re.compile(rf"^{re.escape(prefix)}(?P<stamp>\d{{8}}T\d{{6}}Z)\.dump$")
    dumps: dict[str, tuple[str, datetime]] = {}
    checksums: set[str] = set()
    recognized: set[str] = set()
    protected: list[dict[str, str]] = []

    for name in unique:
        base = name[:-7] if name.endswith(".sha256") else name
        match = pattern.fullmatch(base)
        if not match:
            protected.append({"name": name, "reason": "unknown-name"})
            continue
        try:
            parsed = _stamp(match.group("stamp"))
        except ValueError:
            protected.append({"name": name, "reason": "unparseable-timestamp"})
            continue
        recognized.add(name)
        if name.endswith(".sha256"):
            checksums.add(name)
        else:
            dumps[name] = (match.group("stamp"), parsed)

    complete: list[tuple[str, str, datetime]] = []
    future_names: set[str] = set()
    for dump, (stamp, parsed) in dumps.items():
        checksum = f"{dump}.sha256"
        if parsed > now:
            future_names.update({dump, checksum} & recognized)
        elif checksum in checksums:
            complete.append((dump, stamp, parsed))

    for name in sorted(future_names):
        protected.append({"name": name, "reason": "future-timestamp"})

    complete_names = {name for dump, _, _ in complete for name in (dump, f"{dump}.sha256")}
    paired_or_future = complete_names | future_names
    for name in sorted(recognized - paired_or_future):
        protected.append({"name": name, "reason": "incomplete-pair"})

    complete.sort(key=lambda item: (item[2], item[0]), reverse=True)
    reasons: dict[str, set[str]] = defaultdict(set)
    if complete:
        reasons[complete[0][0]].add("newest")

    if mode == "local":
        for dump, _, _ in complete[:local_count]:
            reasons[dump].add("local-newest")
    else:
        group_specs = [
            ("daily", daily, lambda value: value.date()),
            ("weekly", weekly, lambda value: value.isocalendar()[:2]),
            ("monthly", monthly, lambda value: (value.year, value.month)),
        ]
        for reason, count, key in group_specs:
            seen: set[object] = set()
            for dump, _, parsed in complete:
                bucket = key(parsed)
                if bucket in seen:
                    continue
                if len(seen) >= count:
                    break
                seen.add(bucket)
                reasons[dump].add(reason)

    keep = [_record(dump, parsed, reasons[dump]) for dump, _, parsed in complete if dump in reasons]
    delete = [_record(dump, parsed, {"expired"}) for dump, _, parsed in reversed(complete) if dump not in reasons]
    return {
        "mode": mode,
        "prefix": prefix,
        "keep": keep,
        "delete": delete,
        "protected": sorted(protected, key=lambda item: (item["name"], item["reason"])),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("local", "remote"), required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--now")
    parser.add_argument("--local-count", type=int, default=2)
    parser.add_argument("--daily", type=int, default=7)
    parser.add_argument("--weekly", type=int, default=4)
    parser.add_argument("--monthly", type=int, default=3)
    parser.add_argument("--format", choices=("json", "delete-names"), default="json")
    args = parser.parse_args(argv)
    now = _stamp(args.now) if args.now else datetime.now(UTC)
    plan = plan_inventory(
        sys.stdin,
        mode=args.mode,
        prefix=args.prefix,
        now=now,
        local_count=args.local_count,
        daily=args.daily,
        weekly=args.weekly,
        monthly=args.monthly,
    )
    if args.format == "delete-names":
        for item in plan["delete"]:
            print(item["dump"])
            print(item["checksum"])
    else:
        json.dump(plan, sys.stdout, sort_keys=True, separators=(",", ":"))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run focused tests and make the script executable**

Run:

```powershell
git update-index --add --chmod=+x deploy/linux/degen-prod-db-retention.py
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py -q
```

Expected: all planner tests pass.

- [ ] **Step 5: Run the full suite and commit Task 1**

Run the full repository suite, then stage only the plan, planner, and planner tests. Commit with:

```text
feat: add Green backup retention planner
```

### Task 2: Build the verify-before-prune backup orchestrator

**Files:**
- Create: `deploy/linux/degen-prod-db-backup.sh`
- Create: `tests/test_degen_prod_db_backup_script.py`

- [ ] **Step 1: Write failing source-contract and behavior tests**

Create tests that always inspect source order and, on POSIX with Bash available, run the script with temporary fake `psql`, `pg_dump`, `pg_restore`, and `rclone` executables. The fake command log must prove this order:

```text
psql-size
remote-list
pg_dump
pg_restore-list
rclone-copy-dump-temp
rclone-copy-checksum-temp
rclone-stat-dump-temp
rclone-cat-checksum-temp
rclone-move-dump-final
rclone-move-checksum-final
rclone-stat-dump-final
rclone-cat-checksum-final
local-prune
remote-plan
```

Add these assertions:

```python
def test_script_has_verify_before_prune_order() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert source.index('pg_restore --list "$tmp_dump"') < source.index("publish_remote_pair")
    assert source.index("verify_remote_pair") < source.index("prune_local_pairs")
    assert source.index("prune_local_pairs") < source.index("apply_remote_retention")


def test_remote_pruning_is_opt_in() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert 'REMOTE_PRUNE_ENABLED="${REMOTE_PRUNE_ENABLED:-0}"' in source
    assert '[[ "$REMOTE_PRUNE_ENABLED" == "1" ]]' in source


def test_failure_before_remote_verification_never_prunes(posix_backup_harness) -> None:
    result = posix_backup_harness.run(fail_at="rclone-stat-dump-temp")
    assert result.returncode != 0
    assert "deletefile" not in posix_backup_harness.events
    assert posix_backup_harness.old_local_pair_exists()


def test_success_keeps_two_local_pairs_and_remote_dry_run_deletes_nothing(posix_backup_harness) -> None:
    result = posix_backup_harness.run(remote_prune_enabled="0")
    assert result.returncode == 0
    assert len(posix_backup_harness.complete_local_pairs()) == 2
    assert "remote prune disabled; candidate=" in result.stdout
    assert "deletefile" not in posix_backup_harness.events


def test_enabled_remote_prune_deletes_only_planner_candidates(posix_backup_harness) -> None:
    result = posix_backup_harness.run(remote_prune_enabled="1")
    assert result.returncode == 0
    assert posix_backup_harness.deleted_remote_names == posix_backup_harness.expected_expired_pair_names
    assert posix_backup_harness.unknown_remote_name_exists()


def test_capacity_failure_happens_before_pg_dump(posix_backup_harness) -> None:
    result = posix_backup_harness.run(database_size="300", free_bytes="399", reserve_bytes="100")
    assert result.returncode != 0
    assert "pg_dump" not in posix_backup_harness.events


def test_nonblocking_lock_rejects_overlap(posix_backup_harness) -> None:
    with posix_backup_harness.held_lock():
        result = posix_backup_harness.run()
    assert result.returncode != 0
    assert "another backup invocation holds" in result.stdout
    assert "pg_dump" not in posix_backup_harness.events
```

- [ ] **Step 2: Run the orchestrator tests and verify they fail**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_script.py -q
```

Expected: failure because `deploy/linux/degen-prod-db-backup.sh` is absent.

- [ ] **Step 3: Implement the Bash orchestrator**

Create executable `deploy/linux/degen-prod-db-backup.sh` with these exact interfaces and phases:

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077

MODE="${1:-run}"
APP_ENV_FILE="${APP_ENV_FILE:-/opt/degen/web.env}"
BACKUP_DIR="${BACKUP_DIR:-/opt/degen/backups/db}"
LOG_DIR="${LOG_DIR:-/var/log/degen}"
RCLONE_CONFIG="${RCLONE_CONFIG:-/etc/degen/rclone.conf}"
RCLONE_REMOTE_PATH="${RCLONE_REMOTE_PATH:-onedrive:backups/degen-db}"
KEEP_LOCAL_COUNT="${KEEP_LOCAL_COUNT:-2}"
KEEP_REMOTE_DAILY="${KEEP_REMOTE_DAILY:-7}"
KEEP_REMOTE_WEEKLY="${KEEP_REMOTE_WEEKLY:-4}"
KEEP_REMOTE_MONTHLY="${KEEP_REMOTE_MONTHLY:-3}"
REMOTE_PRUNE_ENABLED="${REMOTE_PRUNE_ENABLED:-0}"
MIN_FREE_AFTER_BYTES="${MIN_FREE_AFTER_BYTES:-10737418240}"
RETENTION_PLANNER="${RETENTION_PLANNER:-/usr/local/sbin/degen-prod-db-retention}"
LOCK_FILE="${LOCK_FILE:-/run/lock/degen-prod-db-backup.lock}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/prod-db-backup.log}"

fail() { printf '[%s] ERROR: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; exit 1; }
info() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
```

The completed file must also implement these concrete behaviors:

1. Validate `MODE` as `run`, `preflight`, or `remote-retention-dry-run`; validate all count/byte flags as non-negative integers and `REMOTE_PRUNE_ENABLED` as `0` or `1`.
2. Create only the configured backup/log directories, then redirect combined output through `tee -a "$LOG_FILE"`; never enable shell tracing.
3. Acquire FD 9 with `exec 9>"$LOCK_FILE"` and fail immediately when `flock -n 9` cannot acquire it.
4. Load `DATABASE_URL` from the root-owned `APP_ENV_FILE` only when it is not already set, preserving the live variable contract.
5. Derive the owned prefix as `${BACKUP_PREFIX:-${db_name}_$(hostname -s)_}` and accept only `[A-Za-z0-9._-]+`.
6. Preflight `python3`, `psql`, `pg_dump`, `pg_restore`, `sha256sum`, `stat`, `df`, `flock`, `tee`, and `rclone`; verify remote listing access without printing configuration or credentials.
7. Query `pg_database_size(current_database())`; require `free_bytes >= database_size + MIN_FREE_AFTER_BYTES` before `pg_dump`.
8. Dump to `.$dump_name.partial`, validate that file with `pg_restore --list "$tmp_dump"`, calculate SHA-256, write `.$dump_name.sha256.partial`, and use `mv` to publish the local pair.
9. Upload both files with `rclone copyto` under `.degen-upload-$timestamp-$$-*` names; verify temporary dump size using `rclone lsjson --stat` and temporary sidecar content using `rclone cat`; use `rclone moveto` for final names; repeat size/sidecar verification at final names.
10. Feed local basenames to the planner in `local` mode and remove only returned exact basenames after validating that no returned name contains `/`.
11. Feed remote basenames to the planner in `remote` mode; log every exact candidate. Call `rclone deletefile` only when `REMOTE_PRUNE_ENABLED=1` and `MODE=run`.
12. In `preflight`, stop before creating a dump. In `remote-retention-dry-run`, inventory and print candidates but never delete, regardless of the environment flag.
13. Trap `EXIT`, remove only owned local partials, and best-effort remove owned remote temporary objects; never remove completed pairs in the trap.

- [ ] **Step 4: Run focused behavior and syntax tests**

Run:

```powershell
git update-index --add --chmod=+x deploy/linux/degen-prod-db-backup.sh
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_script.py -q
wsl bash -n '/mnt/c/Users/jeffr/OneDrive/Apps/Documents/Degen App/.worktrees/backup-retention-hardening/deploy/linux/degen-prod-db-backup.sh'
```

Expected: focused tests pass and Bash syntax exits 0.

- [ ] **Step 5: Run the full suite and commit Task 2**

Run the full repository suite, stage only the orchestrator and its test module, and commit with:

```text
feat: verify Green backups before pruning
```

### Task 3: Track the systemd contract, environment template, and runbook

**Files:**
- Create: `deploy/systemd/degen-prod-db-backup.service`
- Create: `deploy/systemd/degen-prod-db-backup.timer`
- Create: `deploy/systemd/degen-prod-db-backup.env.example`
- Create: `docs/green-postgres-backup-runbook.md`
- Modify: `tests/test_degen_prod_db_backup_script.py`

- [ ] **Step 1: Add failing deployment-asset tests**

Add static tests asserting:

```python
def test_systemd_service_is_oneshot_without_app_or_database_restart() -> None:
    unit = SERVICE.read_text(encoding="utf-8")
    assert "Type=oneshot" in unit
    assert "EnvironmentFile=/etc/degen/prod-db-backup.env" in unit
    assert "ExecStart=/usr/local/sbin/degen-prod-db-backup" in unit
    assert "NoNewPrivileges=true" in unit
    assert "PrivateTmp=true" in unit
    assert "ProtectSystem=full" in unit
    assert "ReadWritePaths=/etc/degen" in unit
    assert "[Install]" not in unit
    assert "systemctl restart" not in unit
    assert "degen-web" not in unit and "degen-worker" not in unit


def test_timer_preserves_live_schedule() -> None:
    timer = TIMER.read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 03:15:00 America/Los_Angeles" in timer
    assert "RandomizedDelaySec=20m" in timer
    assert "AccuracySec=1m" in timer
    assert "Persistent=true" in timer
    assert "Unit=degen-prod-db-backup.service" in timer


def test_env_template_is_secret_free_and_pruning_defaults_disabled() -> None:
    template = ENV_TEMPLATE.read_text(encoding="utf-8")
    assert "KEEP_LOCAL_COUNT=2" in template
    assert "KEEP_REMOTE_DAILY=7" in template
    assert "KEEP_REMOTE_WEEKLY=4" in template
    assert "KEEP_REMOTE_MONTHLY=3" in template
    assert "REMOTE_PRUNE_ENABLED=0" in template
    assert "DATABASE_URL=" not in template
    assert "token" not in template.lower()
    assert "password" not in template.lower()


def test_runbook_requires_backup_dry_run_and_pid_verification() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    assert "/opt/degen/backups/config/" in runbook
    assert "remote-retention-dry-run" in runbook
    assert "REMOTE_PRUNE_ENABLED=0" in runbook
    assert "REMOTE_PRUNE_ENABLED=1" in runbook
    assert "MainPID" in runbook
    assert "rollback" in runbook.lower()
```

- [ ] **Step 2: Run the asset tests and verify missing-file failures**

Run the focused test module and expect failures naming the four absent assets.

- [ ] **Step 3: Add the service and timer**

Create `deploy/systemd/degen-prod-db-backup.service`:

```ini
[Unit]
Description=Degen PostgreSQL verified backup
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=oneshot
User=root
Group=root
EnvironmentFile=/etc/degen/prod-db-backup.env
ExecStart=/usr/local/sbin/degen-prod-db-backup
TimeoutStartSec=infinity
TimeoutStopSec=90
KillMode=control-group
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
PrivateDevices=true
ProtectHome=true
ProtectSystem=full
ReadWritePaths=/etc/degen
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
```

Create `deploy/systemd/degen-prod-db-backup.timer`:

```ini
[Unit]
Description=Nightly Degen PostgreSQL verified backup

[Timer]
Unit=degen-prod-db-backup.service
OnCalendar=*-*-* 03:15:00 America/Los_Angeles
RandomizedDelaySec=20m
AccuracySec=1m
Persistent=true

[Install]
WantedBy=timers.target
```

- [ ] **Step 4: Add the secret-free environment template**

Create `deploy/systemd/degen-prod-db-backup.env.example`:

```dotenv
APP_ENV_FILE=/opt/degen/web.env
BACKUP_DIR=/opt/degen/backups/db
LOG_DIR=/var/log/degen
RCLONE_CONFIG=/etc/degen/rclone.conf
RCLONE_REMOTE_PATH=onedrive:backups/degen-db
KEEP_LOCAL_COUNT=2
KEEP_REMOTE_DAILY=7
KEEP_REMOTE_WEEKLY=4
KEEP_REMOTE_MONTHLY=3
REMOTE_PRUNE_ENABLED=0
MIN_FREE_AFTER_BYTES=10737418240
RETENTION_PLANNER=/usr/local/sbin/degen-prod-db-retention
LOCK_FILE=/run/lock/degen-prod-db-backup.lock
```

- [ ] **Step 5: Add the production runbook**

Document exact repository-to-host mappings, root ownership/modes, configuration backup under `/opt/degen/backups/config/<UTC timestamp>/`, install commands, `daemon-reload` without application/database restarts, preflight mode, remote dry-run review, the explicit flag flip from `REMOTE_PRUNE_ENABLED=0` to `1`, timer verification, journal checks, local/remote pair checks, unchanged `MainPID` capture for PostgreSQL/web/worker/bot, and exact-file rollback.

- [ ] **Step 6: Run deployment-asset validation**

Run the focused pytest modules, `git diff --check`, Bash syntax, and `systemd-analyze verify` inside WSL when available. Expected: all pass; the service may emit only environment-file absence warnings in the local workspace.

- [ ] **Step 7: Run the full suite and commit Task 3**

Run the full repository suite, stage only the service, timer, environment template, runbook, and intended test update, and commit with:

```text
docs: track Green backup operations
```

### Task 4: Verify and review the complete local change

**Files:**
- All files from Tasks 1-3
- Plan: `docs/superpowers/plans/2026-06-29-green-backup-retention.md`

- [ ] **Step 1: Run focused tests**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py tests/test_degen_prod_db_backup_script.py --tb=short -q
```

Expected: all focused tests pass; POSIX-only tests may skip on native Windows and must then be run explicitly through WSL.

- [ ] **Step 2: Run repository checks**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m compileall app
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q -p no:cacheprovider
git diff --check
```

Expected: compile succeeds, the full suite has no failures, and the diff check is clean.

- [ ] **Step 3: Obtain two-stage independent review**

Have a spec-compliance reviewer compare every success criterion and safety rule in the approved design to the diff and test evidence. Then have a code-quality reviewer inspect shell quoting, path ownership, planner determinism, subprocess failure behavior, secret exposure, unit hardening compatibility, and production rollback. Resolve all material findings and rerun affected tests.

- [ ] **Step 4: Confirm task commits and intended file scope**

Confirm the three task commits contain only the eight intended implementation/test/runbook files plus this plan, and that `git status --short` is clean. If review fixes are required, rerun the full suite before committing those exact fixes.

### Task 5: Production preflight and installation checkpoint

**Files:** No repository edits expected.

- [ ] **Step 1: Present the mandatory production-change preflight**

State exact targets, changes, reversible and irreversible effects, backup/rollback path, and post-action verification. Obtain Jeffrey's explicit `proceed` before writing Green files or reloading systemd.

- [ ] **Step 2: Capture pre-change state without secrets**

Record SHA-256, owner, mode, timer schedule, service state, local/remote inventory, disk free, and `MainPID` for PostgreSQL/web/worker/bot. Do not print environment values.

- [ ] **Step 3: Back up and install exact assets**

Create a root-only timestamped directory below `/opt/degen/backups/config/`; copy the existing script, service, timer, and environment file there. Install the two executables as `root:root 0755`, units as `root:root 0644`, and preserve the existing secret-bearing environment while adding the new policy keys with `REMOTE_PRUNE_ENABLED=0`.

- [ ] **Step 4: Reload metadata and run non-mutating validation**

Run `systemd-analyze verify`, `systemctl daemon-reload`, `systemctl is-enabled`, `systemctl list-timers`, the script's `preflight` mode, and `remote-retention-dry-run`. Do not start a backup, restart any service, or delete remote objects.

- [ ] **Step 5: Review exact remote candidates**

Compare planner output to the complete remote inventory. Only if every candidate is a recognized complete pair outside the 7/4/3 keep set, change the host environment to `REMOTE_PRUNE_ENABLED=1`. If there are no candidates, record that fact and still enable the reviewed policy for future scheduled runs.

- [ ] **Step 6: Verify unchanged production processes and rollback readiness**

Confirm PostgreSQL/web/worker/bot `MainPID` values are unchanged, timer next-run schedule is correct, units are healthy, installed hashes match the reviewed branch, OneDrive on the Windows workstation remains off, and the config backup can restore every replaced host file.

### Task 6: Publish the reviewed branch

**Files:** No additional edits expected.

- [ ] **Step 1: Push the implementation branch**

Push `codex/backup-retention-hardening` to the canonical origin without merging to `main`.

- [ ] **Step 2: Report final evidence**

Provide the commit SHA, test counts, installed hashes, timer schedule, local/remote retention settings, dry-run candidate count, production PID comparison, rollback path, and any remaining operational caveat. Do not claim the next scheduled backup succeeded until its journal and both local/remote pair integrity are observed.
