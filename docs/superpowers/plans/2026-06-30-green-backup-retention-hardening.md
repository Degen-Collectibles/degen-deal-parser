# Green Backup Final Integration Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the final reviewed production blockers so Green can install and operate the two-local plus 7/4/3 PostgreSQL backup policy from immutable reviewed artifacts without timer races, configuration drift, silent credential mutation, or unsafe rollback.

**Architecture:** Keep the credential-free retention planner and Bash backup orchestrator focused on nightly runtime behavior. Add one Python 3.10-compatible environment helper as the only parser/renderer for the managed service file, and one Python operations state machine for immutable source verification, host staging, transactional installation, rclone evidence/probing, prune enablement, scheduled observation, and rollback. A standard-tool bootstrap verifies the archive digest, embedded commit, exact member list/types, and manifest digest before the new helper is ever executed. Production phases persist root-only JSON state and repeat timer, service, lock, validation, and rollback controls in every fresh invocation.

**Tech Stack:** Python 3.10+, Bash 5.1, PostgreSQL 17 client tools, rclone 1.74.1, systemd 249, pytest, Git archives, SHA-256 manifests.

**Supersedes:** Tasks 5-6 of `docs/superpowers/plans/2026-06-29-green-backup-retention.md`.

---

## File responsibilities

- `deploy/linux/degen-prod-db-retention.py`: deterministic, credential-free keep/delete planner; Python 3.10 compatible; emits delete and keep names.
- `deploy/linux/degen-prod-db-backup-env.py`: sole parser/validator/renderer for the simple managed environment file; never emits unrelated values.
- `deploy/linux/degen-prod-db-backup.sh`: nightly dump, verification, publication, retained-pair validation, and pruning.
- `deploy/linux/degen-prod-db-backup-ops.py`: privileged operation state machine for source verification, install, probe, dry-run evidence, enablement, observation, and rollback.
- `deploy/linux/degen-prod-db-backup-assets.sha256`: fixed manifest of repo-managed production assets, excluding itself.
- `deploy/systemd/degen-prod-db-backup.service`: root oneshot using the script's environment parser and a root-only runtime directory.
- `deploy/systemd/degen-prod-db-backup.timer`: existing 03:15 Pacific persistent schedule.
- `deploy/systemd/degen-prod-db-backup.env.example`: secret-free managed-key reference.
- `docs/green-postgres-backup-runbook.md`: approval boundaries and exact helper invocations, not duplicated implementation algorithms.
- `tests/test_degen_prod_db_retention.py`: planner and Python 3.10 contract.
- `tests/test_degen_prod_db_backup_env.py`: environment grammar, rendering, and metadata behavior.
- `tests/test_degen_prod_db_backup_script.py`: nightly runtime behavior and external-command fakes.
- `tests/test_degen_prod_db_backup_ops.py`: source, state, install, probe, enablement, observation, and rollback behavior.

---

### Task 0: Commit the approved superseding plan

**Files:**
- Modify: `docs/superpowers/plans/2026-06-29-green-backup-retention.md`
- Create: `docs/superpowers/plans/2026-06-30-green-backup-retention-hardening.md`

- [ ] **Step 1: Validate the plan artifacts**

Run placeholder, spec-coverage, exact-path, and `git diff --check` reviews. Require the old production Tasks 5-6 banner and every approved design control in this plan.

- [ ] **Step 2: Run the full suite and commit only the two plans**

Per the repository contract, run the full repository suite even for this documentation commit. Stage only the two plan files and commit:

```text
docs: supersede Green backup rollout plan
```

Do not push, merge, write Green, access OneDrive, or change any timer/service.

---

### Task 1: Make the planner and test harness Python 3.10 compatible

**Files:**
- Modify: `deploy/linux/degen-prod-db-retention.py:9-14,147-182`
- Modify: `tests/test_degen_prod_db_retention.py`
- Modify: `tests/test_degen_prod_db_backup_script.py:48-60`

- [ ] **Step 1: Write failing Python 3.10 contract tests**

Add tests that parse the planner and both backup test modules with Python 3.10 grammar and verify the planner's keep-name CLI:

```python
def test_sources_parse_with_python_310_grammar() -> None:
    for path in (PLANNER, Path(__file__), ROOT / "tests/test_degen_prod_db_backup_script.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 10))


def test_cli_keep_names_emits_newest_complete_pairs_first(tmp_path: Path) -> None:
    names = pair("20260628T031500Z") + pair("20260629T031500Z")
    result = subprocess.run(
        [sys.executable, str(PLANNER), "--mode", "local", "--prefix", PREFIX,
         "--now", "20260630T000000Z", "--local-count", "2", "--format", "keep-names"],
        input="\n".join(names) + "\n",
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.splitlines() == pair("20260629T031500Z") + pair("20260628T031500Z")
```

Change the Windows path conversion test fixture so no backslash appears inside an f-string expression:

```python
stripped_tail = tail.lstrip("\\/").replace("\\", "/")
return f"/mnt/{drive[0].lower()}/{stripped_tail}"
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py -q
```

Expected: failures for `datetime.UTC`, missing `keep-names`, and the Python-3.12-only f-string expression.

- [ ] **Step 3: Implement Python 3.10 compatibility and `keep-names`**

Use this import and UTC contract:

```python
from datetime import datetime, timezone


def _stamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
```

Extend the CLI format choices to `("json", "delete-names", "keep-names")`. For `keep-names`, print each kept dump followed by its checksum in existing newest-first plan order. Keep `delete-names` oldest-first.

- [ ] **Step 4: Verify with Python 3.10 and the repo interpreter**

Run focused pytest locally, then send the planner source through Green's read-only `python3` to compile and exercise one `keep-names` input without writing a host file. Expected: Python 3.10.12 exits 0 and returns the same names as local pytest.

- [ ] **Step 5: Run the full suite and commit**

Run the full repository suite once, stage only the three files, and commit:

```text
fix: support Python 3.10 backup retention
```

---

### Task 2: Add one fail-closed managed-environment helper

**Files:**
- Create: `deploy/linux/degen-prod-db-backup-env.py`
- Create: `tests/test_degen_prod_db_backup_env.py`

- [ ] **Step 1: Write failing parser and renderer tests**

The tests must cover this API:

```python
module.parse_simple_environment(path) -> module.ParsedEnvironment
module.validate_effective_configuration(values, *, effective_uid) -> dict[str, str]
module.render_managed_environment(source, destination, updates, *, effective_uid) -> None
module.emit_runtime_configuration(path, *, effective_uid) -> str
```

Add concrete tests for:

- blank/comment/simple literal assignments;
- CRLF and no-final-newline input;
- semantic duplicate keys with whitespace around key or `=`;
- single/double quotes, trailing backslashes, multiline constructs, `export`, `$()`, control bytes, malformed assignments, and whitespace-bearing values;
- private regular-file ownership/mode and non-symlink requirements;
- exact audited paths and policy values;
- preservation of unrelated valid lines in staged output;
- exactly one canonical managed assignment after rendering;
- source bytes and metadata unchanged by every success and failure;
- runtime output containing only allowlisted non-secret keys.

Use the exact managed contract:

```python
MANAGED_DEFAULTS = {
    "APP_ENV_FILE": "/opt/degen/web.env",
    "BACKUP_DIR": "/opt/degen/backups/db",
    "LOG_DIR": "/var/log/degen",
    "RCLONE_CONFIG": "/etc/degen/rclone.conf",
    "RCLONE_REMOTE_PATH": "onedrive:backups/degen-db",
    "KEEP_LOCAL_COUNT": "2",
    "KEEP_REMOTE_DAILY": "7",
    "KEEP_REMOTE_WEEKLY": "4",
    "KEEP_REMOTE_MONTHLY": "3",
    "REMOTE_PRUNE_ENABLED": "0",  # initial staging value only
    "MIN_FREE_AFTER_BYTES": "10737418240",
    "RETENTION_PLANNER": "/usr/local/sbin/degen-prod-db-retention",
    "LOCK_FILE": "/run/degen-prod-db-backup/backup.lock",
}
```

`BACKUP_PREFIX` is managed but has no tracked default; it must match `^[A-Za-z0-9._-]+_$` when present.

The parser accepts `REMOTE_PRUNE_ENABLED` only as `0` or `1`. Initial staging and installation require exactly `0`, enablement requires the current value to be exactly `0`, and only the operations helper's `enable-prune` transaction may render `1`.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_env.py --tb=short -q -p no:cacheprovider
```

Expected: collection fails because `deploy/linux/degen-prod-db-backup-env.py` and its required parser/renderer interfaces do not exist.

- [ ] **Step 3: Implement the parser and renderer**

Create an executable Python 3.10 script with subcommands:

```text
inspect --file PATH
emit --file PATH
render --source PATH --destination PATH --set KEY=VALUE [--set ...]
```

`inspect` returns compact sorted JSON containing only managed keys and structural metadata. `emit` writes canonical `KEY=VALUE` lines for the Bash orchestrator and never prints unrelated values. `render` writes only a separately staged destination with `O_EXCL`, mode `0600`, fsyncs file and parent, and leaves the source untouched.

Reject the full file before emitting or rendering when any unsupported grammar appears. Validate values with per-key rules rather than shell evaluation. Never use `source`, `eval`, `shlex`, environment expansion, or command substitution.

- [ ] **Step 4: Verify focused behavior and Python 3.10**

Run the new test module locally. Send only the helper source to Green's Python 3.10 `compile()` through stdin so no Green file is created; functional `inspect` execution remains in the local tests against private temporary files.

- [ ] **Step 5: Run the full suite and commit**

Stage only the helper and its test module after full-suite success. Commit:

```text
feat: add fail-closed Green backup environment contract
```

---

### Task 3: Unify runtime configuration and secure the lock

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup.sh`
- Modify: `deploy/systemd/degen-prod-db-backup.service`
- Modify: `deploy/systemd/degen-prod-db-backup.env.example`
- Modify: `tests/test_degen_prod_db_backup_script.py`
- Modify: `tests/test_degen_prod_db_backup_env.py`

- [ ] **Step 1: Write failing service/direct parity and lock tests**

Add tests proving:

```text
service invocation == direct preflight == direct dry-run managed configuration
BACKUP_ENV_FILE and ENV_HELPER accept absent or identical fixed inherited values and reject alternates
inherited managed values cannot override the parsed file
complex or duplicate environment input fails before mkdir/log/lock/database/rclone
runtime directory rejects symlink, wrong owner, and group/world write
lock file rejects symlink without changing the target sentinel
existing private regular lock supports overlap detection
preflight/dry-run accepts only a validated inherited lock FD when an operations transaction owns the lock
```

Update the harness so fake controls remain in the process environment while every managed backup value is written to a temporary `prod-db-backup.env` and selected with `BACKUP_ENV_FILE`.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_env.py tests/test_degen_prod_db_backup_script.py --tb=short -q -p no:cacheprovider
```

Expected: current eager Bash defaults, service `EnvironmentFile=`, and `/run/lock` behavior fail the new tests.

- [ ] **Step 3: Load the helper before runtime side effects**

Replace the eager assignments with fixed production paths:

```bash
BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env
ENV_HELPER=/usr/local/sbin/degen-prod-db-backup-env
```

Treat `/etc/degen/prod-db-backup.env` and `/usr/local/sbin/degen-prod-db-backup-env` as compile-time production constants. Before creating directories, starting logging, opening the lock, reading the app environment, or invoking rclone, resolve `BACKUP_ENV_FILE` and `ENV_HELPER` with this exact rule: if the inherited variable is absent, assign the fixed constant; if it is present and byte-for-byte equal to the fixed constant, accept it; otherwise fail before side effects. The service-provided `BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env` is therefore valid, while direct and service production modes both reject alternate paths. Then run the fixed helper's `emit --file "$BACKUP_ENV_FILE"`, capture its complete output, and assign only explicit case-matched managed keys. Require every managed key exactly once and reject inherited managed-value overrides.

Tests may inject temporary paths only through an explicit harness-only interface that is rejected for root/production execution. Add separate direct/service cases for absent, identical-fixed, and alternate inherited values for both `BACKUP_ENV_FILE` and `ENV_HELPER`; prove alternates and inherited managed-value overrides fail before any mutation or external command.

- [ ] **Step 4: Validate the root-only runtime lock**

Use `LOCK_FILE=/run/degen-prod-db-backup/backup.lock`. Validate the parent and lock before and after opening FD 9:

```text
parent: real directory, owner == EUID, mode 0700, no symlink
lock: absent or regular non-symlink, owner == EUID, no group/world permissions
post-open inode/type/owner still match the validated path
```

Then call `flock -n 9`. A failed validation must not truncate or touch a sentinel target.

For operations-helper integration, add `--lock-fd N` only to `preflight` and `remote-retention-dry-run`. Validate that the inherited descriptor is open, regular, owner/mode/inode-identical to `LOCK_FILE`, and successfully flocked on the inherited open-file description. Normal `run` rejects this option. The runtime never trusts a boolean "lock already held" environment variable and never skips descriptor/inode/lock validation.

Update the service:

```ini
Environment=BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env
RuntimeDirectory=degen-prod-db-backup
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=yes
```

Remove `EnvironmentFile=` so service and direct modes use the same parser, but retain the exact `Environment=BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env` line above; the script accepts it only because it equals the fixed production path. Change the template lock path and document that `BACKUP_PREFIX` is added on-host from verified identity.

- [ ] **Step 5: Validate rclone config metadata**

Before the first rclone call, require the config to be a regular non-symlink owned by EUID with mode `0600`, link count one, and a real parent owned by EUID without group/world write. Tests use private temporary paths; Green's audited `/etc/degen` and `rclone.conf` satisfy the contract.

- [ ] **Step 6: Run focused/systemd/full verification and commit**

Run the environment and script modules, WSL `bash -n`, `systemd-analyze verify`, the full suite, and commit the five intended files:

```text
fix: unify Green backup configuration and secure locking
```

---

### Task 4: Match real rclone semantics, bind backup identity, and record runtime evidence

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup.sh:227-245,353-423`
- Modify: `tests/test_degen_prod_db_backup_script.py`

- [ ] **Step 1: Write failing real-semantics tests**

Change the fake so direct `copyto`/`moveto --immutable` overwrites an existing different destination, matching the official rclone 1.74.1 probe. Add tests requiring all four publication calls to use both flags:

```text
--ignore-existing --error-on-no-transfer
```

Existing identical or different destinations must exit nonzero, remain byte-for-byte unchanged, and leave a skipped move source intact. Case-insensitive inventory checks remain mandatory.

Add identity tests requiring a configured `BACKUP_PREFIX` to equal the actual `${current_database}_${hostname -s}_` value and proving the identity queries always run. Add tests that the scheduled runtime brackets its complete rclone command group with safe config metadata markers containing hash, inode, owner/group, mode, size, and nanosecond mtime, but never config contents.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_script.py --tb=short -q -p no:cacheprovider
```

Expected: the current `--immutable` commands overwrite in the realistic fake and arbitrary persisted prefixes bypass identity queries.

- [ ] **Step 3: Implement strict direct-file publication**

Replace each direct operation with:

```bash
rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer copyto SOURCE DESTINATION
rclone --config "$RCLONE_CONFIG" --ignore-existing --error-on-no-transfer moveto SOURCE DESTINATION
```

Keep the pre-upload and pre-publish casefold inventories, ownership-after-success, source-disappearance assertions, and final size/sidecar verification. Remove every claim that `--immutable` supplies no-clobber behavior.

Before the first scheduled rclone call and after the final scheduled rclone call, validate and log one structured metadata receipt for `/etc/degen/rclone.conf`. A changed hash or mtime is recorded as possible OAuth refresh, not treated as failure and never used to restore the audit copy. Any early exit after the first marker must still emit a final marker from the exit trap. These receipts are later bound to the observed post-policy journal run.

- [ ] **Step 4: Bind the prefix to live identity**

Always query `current_database()` and `hostname -s`, validate them, and build the expected prefix. If `BACKUP_PREFIX` exists, require exact equality; otherwise use the expected prefix. A mismatch fails before dump or rclone.

- [ ] **Step 5: Reproduce the official local rclone probe**

Run official rclone 1.74.1 against disposable local paths and record:

```text
absent destination: exit 0 and transfer
existing identical/different: exit 9, destination unchanged
moveto skipped destination: source remains
```

- [ ] **Step 6: Run focused/full verification and commit**

Commit after the full suite:

```text
fix: enforce strict Green remote publication
```

---

### Task 5: Revalidate the retained local pair before pruning

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup.sh:426-547,695-701`
- Modify: `deploy/linux/degen-prod-db-retention.py`
- Modify: `tests/test_degen_prod_db_backup_script.py`
- Modify: `tests/test_degen_prod_db_retention.py`

- [ ] **Step 1: Write failing retained-pair tests**

Seed prior pairs with genuine matching sidecars and fake archives. Add tests proving:

```text
remote final verification precedes retained-pair validation
retained-pair validation precedes the first local rm
corrupt digest, wrong basename, missing LF, symlink, or pg_restore failure blocks every local delete
first backup with no prior pair succeeds without a second-pair requirement
```

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py tests/test_degen_prod_db_backup_script.py --tb=short -q -p no:cacheprovider
```

Expected: current local retention deletes without reading the kept prior pair.

- [ ] **Step 3: Add strict local-pair verification**

Use planner `keep-names` and the same captured local inventory used for delete decisions. For every kept pair other than the newly created current pair:

1. Require regular non-symlink dump and sidecar.
2. Require one exact LF-terminated sidecar record: lowercase 64-hex, two spaces, exact dump basename.
3. Recompute SHA-256 and compare.
4. Run `pg_restore --list` on the dump.

Only after all retained prior pairs pass may the delete loop begin. Exclude symlinks from `collect_local_names()`.

- [ ] **Step 4: Run focused/full verification and commit**

Commit:

```text
fix: revalidate retained Green backup before pruning
```

---

### Task 6: Add immutable source and root-only operation state

**Files:**
- Create: `deploy/linux/degen-prod-db-backup-ops.py`
- Create: `tests/test_degen_prod_db_backup_ops.py`

- [ ] **Step 1: Write failing source/state tests**

Define and test these Python 3.10 interfaces:

```python
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import subprocess


CommandRunner = Callable[
    [Sequence[str], tuple[int, ...]],
    subprocess.CompletedProcess[str],
]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class OperationPaths:
    operation_dir: Path
    source_archive: Path
    source_dir: Path
    snapshot_dir: Path
    staged_dir: Path
    state_file: Path


@dataclass(frozen=True)
class OperationsContext:
    operation_id: str
    paths: OperationPaths
    effective_uid: int
    command_runner: CommandRunner
    clock: Clock
    expected_commit: str
    expected_archive_sha256: str
    expected_manifest_sha256: str
    host_root: Path


def validate_operation_dir(path: Path, *, effective_uid: int) -> None: ...
def verify_source_archive(archive: Path, expected_commit: str, expected_sha256: str,
                          asset_manifest: Path, destination: Path) -> dict[str, str]: ...
def load_operation_state(path: Path, *, effective_uid: int) -> dict[str, object]: ...
def atomic_write_operation_state(path: Path, state: dict[str, object], *, effective_uid: int) -> None: ...
def snapshot_host_state(context: OperationsContext) -> dict[str, object]: ...
```

The production CLI always constructs `host_root=Path("/")`; no production argument may override it. Tests may construct a context with a private temporary host root. `command_runner` receives the argv and an explicit tuple of inherited file descriptors, and `clock` is the sole source of operation epochs.

Use this exact strict JSON top-level contract. Every listed object has exactly the stated keys, rejects extras, and uses JSON `null` only where shown:

```text
schema_version: int, exactly 1
operation_id: non-empty str
operation_dir: absolute str
phase: str
phase_history: list[{phase: str, epoch: int, evidence_sha256: lowercase-hex str}]
reviewed_source: {commit: str, archive_sha256: str, manifest_sha256: str, asset_hashes: dict[str, str]}
effective_config: null|dict[str, str]
host_stage: null|{manifest_sha256: str, asset_hashes: dict[str, str], environment_sha256: str}
snapshot: null|{
  manifest_sha256: str,
  targets: dict[str, {present: bool, sha256: str|null, mode: int|null, uid: int|null, gid: int|null}],
  rclone_audit: {path: str, sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int}
}
prior_runtime: null|{timer_enabled: bool, timer_active: bool, pids: dict[str, int], preinstall_trigger_epoch: int|null}
install: null|{
  next_target_index: int, current_target: str|null, previous_sha256: str|null, intended_sha256: str|null,
  installed_hashes: dict[str, str], started_epoch: int, completed_epoch: int|null
}
rclone_evidence_groups: list[{
  group_id: str, purpose: str,
  before: {sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int},
  after: {sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int},
  evidence_sha256: str
}]
probe: null|{prefix: str, owned_names: list[str], cleanup_proven: bool, evidence_sha256: str}
dry_run: null|{
  inventory_names: list[str], casefold_names: list[str], keep_names: list[str],
  protected_names: list[str], delete_names: list[str], candidate_sha256: str, evidence_sha256: str
}
policy: null|{environment_sha256: str, enabled_epoch: int}
observation: null|{run_epoch: int, journal_sha256: str, local_sha256: str, remote_sha256: str, evidence_sha256: str}
active_transaction: null|{
  kind: "probe"|"dry_run"|"observe"|"policy",
  prior_stable_phase: "installed"|"probed"|"dry_run_recorded"|"policy_enabled",
  prior_timer_enabled: bool, prior_timer_active: bool,
  guard: {
    timer_stopped: bool, service_inactive_verified: bool,
    legacy_lock_acquired: bool, runtime_lock_acquired: bool,
    locks_released: bool, timer_restored: bool
  },
  started_epoch: int,
  probe: null|{
    prefix: str,
    objects: list[{
      name: str, expected_sha256: str, expected_size: int,
      created: bool, verified: bool, cleaned: bool
    }]
  }
}
failure: null|{phase: str, primary_error: sanitized/redacted str, epoch: int, evidence_sha256: str}
secondary_errors: list[{stage: str, error: sanitized/redacted str, epoch: int, evidence_sha256: str}]
recovery: null|{
  kind: "install"|"policy"|"manual_rollback"|"probe"|"guard",
  next_target_index: int, started_epoch: int, completed_epoch: int|null, evidence_sha256: str
}
```

Use these phase-valid receipt invariants. `reviewed_source` is required from `source_verified`; `effective_config` and `host_stage` become required at `staging_prepared`; `snapshot` and `prior_runtime` become required at `snapshotted`; and `install` start/progress becomes required at `installing`. `install.installed_hashes` must remain empty and `completed_epoch` null until `installed`, when complete exact hashes and the epoch become required. `probe` remains null through `probing` and becomes required at `probed`; `dry_run` remains null through `dry_run_recording` and becomes required at `dry_run_recorded`; `policy` remains null through `policy_enabling` and becomes required at `policy_enabled`; `observation` remains null through `observing` and becomes required at `observed`. Earlier phases require every later receipt to be null, never fabricated. Stable later phases preserve all earlier receipts. `rclone_evidence_groups` and `secondary_errors` are empty lists until evidence or a secondary failure exists; they are never replaced with synthetic entries. Tests construct every phase and reject a missing required receipt, an early non-null later receipt, premature installed hashes/epochs, and a receipt that regresses to null.

Allow only these forward transitions: `source_verified -> staging_prepared -> snapshotted -> installing -> installed -> probing -> probed -> dry_run_recording -> dry_run_recorded -> policy_enabling -> policy_enabled -> observing -> observed`; `installing -> recovering -> rolled_back|recovery_required`; `policy_enabling -> recovering_policy -> installed|recovery_required`; `probing|dry_run_recording|observing -> recovery_required`; and, after separate explicit manual-rollback approval, `installed|probed|dry_run_recorded|policy_enabled|observed -> manual_rollback -> rolled_back|recovery_required`. A `recovery_required` state transitions only to the phase matching recorded recovery kind: `recovering` for `install`, `recovering_policy` for `policy`, `manual_rollback` for `manual_rollback`, `recovering_probe` for `probe`, or `recovering_guard` for `guard`. `recovering_probe -> installed|recovery_required`; `recovering_guard -> active_transaction.prior_stable_phase|recovery_required`; and `rolled_back` is terminal.

Require `active_transaction` to be non-null throughout `probing`, `dry_run_recording`, `policy_enabling`, `observing`, and their `recovery_required`/recovery phases, with a kind and prior stable phase matching the transition; require it null in stable phases after successful cleanup and timer restoration. Require the final `phase_history` entry to equal `phase`, phase history, rclone evidence, and `secondary_errors` to be append-only with stable evidence digests, all epochs to be monotonic, and `operation_id`/`operation_dir` plus reviewed source digests to remain bound to `OperationsContext`. The first `failure.primary_error` is immutable once written. Sanitize/redact every error before serialization; rollback, recovery, cleanup, and timer-restoration errors append to `secondary_errors` in occurrence order and can never replace the primary error. Schema validation rejects secret-like keys, values, primary error text, or secondary error text and never serializes database URLs, tokens, passwords, environment-file contents, or rclone contents.

Tests reject wrong operation roots, symlinks, owner/mode mismatch, extra/missing/archive symlink entries, wrong embedded Git commit, wrong archive/asset hashes, state secrets, corrupt JSON, non-atomic state writes, and ambiguous saved-or-absent target state. They also reject missing or extra fields at every schema level, operation-path rebinding, altered or truncated phase/evidence/error history, replacement or reordering of `secondary_errors`, mutation of the first primary error, regressed phases, phase-invalid nullability, forbidden transitions, secret-like error content, and impossible epoch ordering.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider
```

Expected: collection fails because `deploy/linux/degen-prod-db-backup-ops.py`, `OperationsContext`, strict phase-valid state loading, and source/state commands do not exist.

- [ ] **Step 3: Implement source verification and atomic state**

Add CLI subcommands:

```text
verify-source --operation-dir DIR --archive FILE --expected-commit SHA --expected-archive-sha256 DIGEST
prepare-staging --operation-dir DIR
snapshot --operation-dir DIR
show-state --operation-dir DIR
```

Require `/opt/degen/backups/config/<YYYYMMDDTHHMMSSZ>` in production, root ownership, mode `0700`, no symlink, and `operation-state.json` mode `0600`. Use `git get-tar-commit-id`, reject any archive member outside the exact manifest plus manifest file, and extract without following links.

Avoid circular trust with this bootstrap sequence before executing the new helper:

1. Locally create a path-limited uncompressed `git archive` containing only the fixed manifest and seven reviewed assets; record archive SHA-256, embedded commit, and manifest SHA-256.
2. After production approval, set `UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"`, `OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"`, and `SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"`; create the root-only operation directory and transfer the archive without touching `/opt/degen/app`.
3. With existing Green tools only, verify the expected archive SHA-256 and `git get-tar-commit-id`, list the exact expected member names/types, reject links or extras, and extract into a new root-only source directory.
4. Verify the extracted manifest SHA-256 supplied in the approved preflight.
5. Only then invoke `/usr/bin/python3 "$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR" --archive "$OPERATION_DIR/source.tar" --expected-commit "$REVIEWED_SHA" --expected-archive-sha256 "$ARCHIVE_SHA256"` for the full manifest and state validation.

Before installed-helper verification, invoke `/usr/bin/python3 "$SOURCE_OPS"` for every `verify-source`, `prepare-staging`, `snapshot`, and `install` command. Every `recover` or automatic/resumed recovery invocation remains source-routed even after install, because a crash may leave the installed target mixed. Only after `install` succeeds and the installed helper's SHA-256 matches its reviewed manifest entry may the later non-recovery stable-phase commands use the installed path.

`prepare-staging` validates the newest existing complete local pair with canonical sidecar grammar, recomputed SHA-256, and `pg_restore --list`; derives its prefix; independently queries `current_database()` and `hostname -s`; requires the two identities to match; renders every managed key with `REMOTE_PRUNE_ENABLED=0`; and creates a separate host-stage manifest. It fails on no verified existing pair or any identity/path/policy mismatch.

Snapshot every install target below plus a root-only rclone audit copy. For each install target, save exact bytes/mode/owner or an explicit absence marker. Build and immediately verify `SHA256SUMS`; never store credential contents in JSON and never designate `rclone.conf.audit` for automatic rollback.

```text
/usr/local/sbin/degen-prod-db-backup
/usr/local/sbin/degen-prod-db-retention
/usr/local/sbin/degen-prod-db-backup-env
/usr/local/sbin/degen-prod-db-backup-ops
/etc/systemd/system/degen-prod-db-backup.service
/etc/systemd/system/degen-prod-db-backup.timer
/etc/degen/prod-db-backup.env
```

- [ ] **Step 4: Run focused/Python3.10/full verification and commit**

Commit:

```text
feat: add immutable Green backup operation state
```

---

### Task 7: Implement transactional install and rollback

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup-ops.py`
- Modify: `tests/test_degen_prod_db_backup_ops.py`

- [ ] **Step 1: Write the failure matrix first**

Use a fake command runner and temporary host root to test every phase:

```text
all source/staged/snapshot/manifest checks occur before mutation
timer enabled/active state captured exactly
only the backup timer is stopped
service must be inactive/dead/MainPID=0
legacy then new migration locks are acquired in stable order and held through replacement and daemon reload
service inactivity is rechecked after both migration locks are held
old-runtime contenders fail at pre-lock, between-lock, post-lock, replacement, and release boundaries
each install failure restores all prior bytes/modes/absence and timer state
installation completed hashes/epoch written only after daemon-reload, validation, lock release, and exact timer restoration
rollback never restores rclone.conf.audit
rollback/timer restoration errors are visible without replacing the primary error
first failure remains immutable while sanitized secondary errors append in exact occurrence order with evidence digests
secret-like primary or secondary error content is rejected before state persistence
both new helper targets restore exact prior bytes or prior absence
both migration locks are released before any persistent-timer restoration
runtime-directory creation is race-safe, root-owned 0700, non-symlinked, and recorded in state
write-ahead state is durable before the first mutation and every target rename
crash/restart recovery succeeds after every state-write, stage-fsync, rename, parent-fsync, daemon-reload, validation, lock-release, and timer-restoration boundary
fresh install refuses incomplete state and directs the operator to source-helper recovery
timer-restoration failure enters durable recovery before any quiesced recovery result
rollback and recovery reacquire the same dual migration guard whenever legacy bytes may be restored
```

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider
```

Expected: failures naming missing `recover`, `acquire_migration_locks`, durable per-target write-ahead progress, atomic replacement/fsync behavior, incomplete-install refusal, and restart recovery at the injected mutation boundaries.

- [ ] **Step 3: Implement `install`, `recover`, and `rollback` subcommands**

Expose:

```text
install --operation-dir DIR
recover --operation-dir DIR
rollback --operation-dir DIR
```

Define this lock result and use one acquisition function for install, rollback, and recovery:

```python
@dataclass(frozen=True)
class MigrationLocks:
    legacy_fd: int
    runtime_fd: int


def acquire_migration_locks(context: OperationsContext) -> MigrationLocks: ...
```

`acquire_migration_locks()` always acquires in this order. First, safely open `/run/lock/degen-prod-db-backup.lock`: create an absent file with `O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC` and mode `0600`, or open an existing file with `O_NOFOLLOW|O_CLOEXEC`; before and after open, require a regular non-symlink root-owned file with link count one, no group/world permissions, and matching path/FD inode, type, owner, and mode. Take nonblocking `flock` on that legacy FD and fail the transaction if an old runtime owns it. Second, call `ensure_runtime_directory()`, which safely creates or validates `/run/degen-prod-db-backup` as root-owned, non-symlink `0700`, rejects owner/mode/symlink/inode races, and records whether the operation created it; then safely open/validate/flock `/run/degen-prod-db-backup/backup.lock` with the same no-follow, exclusive-create, path/FD metadata checks. Recheck the backup service is inactive/dead with `MainPID=0` after both locks are held. Hold both FDs through every target replacement, validation, and `daemon-reload`, and release them in reverse order before timer restoration. A contender at either lock fails and exits rather than running. Rollback or recovery uses the same dual guard whenever it may restore legacy bytes.

Before the first replacement, fully validate source, host stage, snapshot, state, and manifests; capture timer state; stop only the timer; require the service inactive; acquire both migration locks; and atomically fsync a write-ahead `installing` state. A fresh `install` refuses `installing`, `recovering`, `recovery_required`, or any other incomplete phase and prints the exact verified-source `recover --operation-dir` command instead of guessing or continuing.

Replace each target transactionally. Create a same-directory owned staging file with `O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC`, write exact reviewed bytes, set and verify exact owner/mode, fsync the file, and close it. Before the first host mutation and immediately before each target rename, atomically write and fsync state containing `phase`, `next_target_index`, `current_target`, and the intended and previous hashes. Then use `os.replace(staged, target)` and fsync an open descriptor for the target's parent directory. For a snapshot absence marker, recovery records the same write-ahead progress before unlinking an owned target and fsyncing its parent. State-file replacement itself uses a same-directory exclusive temporary file, file fsync, `os.replace`, and parent-directory fsync. Tests inject process death and reconstruct a fresh helper after every state write, staged-file fsync, target rename/unlink, parent fsync, daemon reload, validation, lock release, and timer restoration.

Install the four executable targets `0755`, the two units `0644`, and the host-derived environment `0600`; do not install the tracked environment example. Reload systemd, then run `/usr/local/sbin/degen-prod-db-backup preflight --lock-fd N`, where `N` is `MigrationLocks.runtime_fd` explicitly inherited through `pass_fds` while the parent continues holding `legacy_fd`. The child validates and shares the inherited new-lock open-file description rather than opening a competing lock. Never start the oneshot unit during install validation, and tests must prove no dump, prune, delete, or normal `ExecStart` occurs. Verify hashes/modes, but while phase remains `installing` keep `install.installed_hashes` empty and `install.completed_epoch` null.

`recover` must be invoked with the helper under the previously verified `context.paths.source_dir`, never whichever installed helper may be mixed after a crash. It revalidates the archive, reviewed source, host stage, snapshot manifest, strict state, and operation binding; captures or reuses the exact prior timer state; quiesces the timer; requires service inactivity; acquires both migration locks; and atomically restores every snapshot target or absence marker with the same write-ahead and fsync protocol. It then runs `daemon-reload`, verifies target bytes/modes/owners/absence and unchanged application/database PIDs, removes only operation-owned staging temporaries, and persists restored-but-incomplete `recovering` evidence. Only after releasing both locks and restoring the timer's exact prior enabled/active state may it append the terminal `rolled_back` phase and `completed_epoch`. If target restoration, lock release, timer restoration, or final state persistence cannot complete, stop/quiesce the timer when possible, preserve the first immutable `failure.primary_error`, append each sanitized recovery failure to `secondary_errors` in occurrence order, persist `recovery_required`, and do not claim rollback complete.

Before restoring an active persistent timer after install, atomically record provisional install evidence, release both migration locks, revalidate that the backup service remains inactive, and restore the timer's exact prior state. Only after exact timer restoration may state atomically populate complete `install.installed_hashes` and `install.completed_epoch` and transition from `installing` to `installed`. Any timer-restoration failure creates `failure.primary_error` only when no failure exists; otherwise it appends a sanitized `secondary_errors` entry without changing the first failure, then enters the same verified-source recovery flow. It does not attempt an ad hoc in-process rollback. The initial install/enable approval explicitly authorizes automatic or resumed recovery of that transaction. An unrelated later manual `rollback` still requires its own mutation preflight and approval.

Manual `rollback` revalidates the snapshot and state, quiesces the timer, requires service inactivity, acquires both migration locks, restores with the same atomic protocol, reloads systemd, verifies destinations and PIDs, persists provisional recovery evidence, releases both locks, and restores the exact prior timer state before recording terminal `rolled_back`. Locally pruned and remotely deleted backup objects remain explicitly unrecoverable.

- [ ] **Step 4: Run focused/full verification and commit**

Commit:

```text
feat: add transactional Green backup installation
```

---

### Task 8: Add rclone evidence, disposable probe, enablement, and observation

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup-ops.py`
- Modify: `tests/test_degen_prod_db_backup_ops.py`

- [ ] **Step 1: Write failing remote/state tests**

Cover:

- rclone config and parent root-only metadata;
- audit copy created before first rclone call;
- before/after hash, inode, size, owner, mode, and nanosecond mtime evidence for every command group;
- OAuth refresh changes recorded without contents or automatic restore;
- unique non-production probe prefix and strict no-existing flags;
- exact/casefold collision refusal;
- cleanup of state-tracked probe objects only and empty-prefix proof;
- dry-run inventory/candidate hashes bound to state;
- probe and dry-run serialized against scheduled/manual backup work with the same timer/service/lock guard;
- durable `probing`, `dry_run_recording`, and `observing` phases with phase-valid `active_transaction` state;
- state fsynced before timer stop, each service/lock/rclone/cleanup action, lock release, and timer restoration;
- process-death/restart recovery before and after timer stop, service check, each lock, every rclone operation, cleanup, each lock release, and timer restoration;
- probe prefix plus every intended name/digest/size persisted before the first create, with created/verified/cleaned progress persisted after each result;
- verified-source probe recovery deletes only matching state-tracked objects and proves the unique prefix empty;
- dry-run/observation guard recovery performs no remote deletion and restores the exact prior timer state;
- fresh-shell enablement repeating timer/service/lock/rollback controls;
- enablement re-inventory exactly matching the reviewed inventory and candidate hashes;
- failed enablement restoring env and removing incomplete epoch;
- scheduled cutoff `max(install_completed_epoch, policy_enabled_epoch)`;
- observation serialized with the same timer/service/lock guard and lock-before-timer restoration order;
- success properties, trigger/start/log/artifact freshness, installed hashes, runtime rclone metadata receipts, sidecar grammar, SHA, archive listing for both retained local pairs, remote integrity, and PID parity.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider -k "probe or dry_run or enable or observe or rclone"
```

Expected: failures naming the missing remote probe, durable in-progress phases/active transaction, crash-safe probe and guard recovery, state-bound dry run, guarded policy enablement, fresh scheduled observation, and rclone evidence-group behavior.

- [ ] **Step 3: Implement the remaining subcommands**

Expose:

```text
probe-remote --operation-dir DIR
record-dry-run --operation-dir DIR
enable-prune --operation-dir DIR
observe --operation-dir DIR
```

Every guarded command atomically writes and fsyncs `active_transaction` before its first external mutation and before every later timer, service, lock, rclone, cleanup, lock-release, or timer-restoration action. The record includes exact prior stable phase and timer enabled/active state plus monotonic guard progress. It quiesces the timer, verifies the service inactive, acquires legacy then runtime locks through `acquire_migration_locks()`, performs the command, releases both locks, restores the exact prior timer state, writes the stable receipt, clears `active_transaction`, and only then enters the next stable phase. A crash in `probing`, `dry_run_recording`, or `observing` persists `recovery_required` without discarding guard progress. Tests inject process death before and after timer stop, service check, each lock, every rclone operation, each cleanup action, each lock release, and timer restoration, then resume in a fresh process through the verified source helper.

`probe-remote` transitions `installed -> probing` and uses a random sibling namespace such as `onedrive:backups/degen-db-probe/<operation-id>-<token>/`. Before its first rclone create, atomically persist and fsync the unique prefix and the complete intended object list with each exact name, expected SHA-256, expected size, and `created=false`, `verified=false`, `cleaned=false`. Immediately before each strict no-existing create, verification, or cleanup call, fsync the next progress state; after each successful result, fsync the corresponding flag before another external mutation. Delete only an object named in that precommitted list whose live size/digest evidence matches state and whose strict no-existing ownership evidence is valid. Prove the prefix empty, restore the guard, then persist the stable `probe` receipt and transition `probing -> probed`. No production-prefix object is ever a probe cleanup target.

If probing crashes, verified-source `recover` revalidates archive/source/state, the unique prefix, pre-create absence evidence, strict no-existing command evidence, and each live object's exact state-tracked name/digest/size. It deletes only matching operation-owned objects; an untracked name, metadata mismatch, or missing ownership evidence stops recovery without deletion. After proving the prefix empty, it releases any owned locks, restores the exact prior timer state, clears `active_transaction`, and transitions `recovering_probe -> installed`. Failure preserves the first primary error, appends sanitized secondary errors, and returns to `recovery_required`.

`record-dry-run` transitions `probed -> dry_run_recording`, invokes the exact installed runtime configuration, and binds the complete case-preserving/casefolded inventory, planner keep/protected/delete sets, and candidate hashes to state. It performs no remote deletion. After releasing locks and restoring the timer it persists `dry_run` and transitions to `dry_run_recorded`. Verified-source recovery from `dry_run_recording` performs no remote deletion; it releases only operation-owned guard/lock state, restores the exact prior timer state, clears `active_transaction`, and transitions `recovering_guard -> probed`.

`enable-prune` is a separate fresh transaction after approval and must re-list the remote inventory under the same guard. It refuses any byte/name/casefold inventory change, independently reruns the planner, and proves candidates are current complete-pair inventory members disjoint from keep/protected sets; any change requires a new dry-run and approval. It requires current `REMOTE_PRUNE_ENABLED=0`, and only its atomic environment transaction may write `1`. Keep the existing `policy_enabling -> recovering_policy -> installed|recovery_required` recovery path and do not reuse probe/guard recovery for an environment mutation.

`observe` transitions `policy_enabled -> observing`, captures complete local/service/journal/remote evidence under the same durable guard, performs no remote deletion, and only after lock release and exact timer restoration persists `observation` and transitions to `observed`. Verified-source recovery from `observing` performs no remote deletion and returns through `recovering_guard -> policy_enabled`. Observation refuses any trigger/start, success journal record, dump filename timestamp, dump mtime, or sidecar mtime at or before `max(install_completed_epoch, policy_enabled_epoch)` or the recorded pre-install trigger. It correlates all evidence to the same scheduled run; validates the runtime rclone before/after receipts; and applies exact lowercase one-record sidecar grammar, recomputed SHA-256, and `pg_restore --list` independently to both locally retained pairs.

- [ ] **Step 4: Run focused/full verification and commit**

Commit:

```text
feat: gate Green pruning on probe and fresh state
```

---

### Task 9: Rewrite the production runbook and pin the asset manifest

**Files:**
- Modify: `docs/green-postgres-backup-runbook.md`
- Create: `deploy/linux/degen-prod-db-backup-assets.sha256`
- Modify: `tests/test_degen_prod_db_backup_ops.py`
- Modify: `tests/test_degen_prod_db_backup_script.py`
- Read/verify only: `docs/superpowers/plans/2026-06-29-green-backup-retention.md` (banner already owned by Task 0)
- Read/hash only: `deploy/systemd/degen-prod-db-backup.env.example` (content already owned by Task 3)

- [ ] **Step 1: Write failing documentation/manifest contract tests**

Tests require the old plan's superseded banner, exact reviewed-SHA push-before-install ordering, standard-tool archive bootstrap, archive manifest parity, separate push and production approvals plus the later prune approval, explicit timer/rclone mutation disclosure, exact `OPERATION_DIR`/`SOURCE_OPS` construction, source-helper routing for verification/staging/snapshot/install/recovery, installed-helper use only after reviewed-manifest hash verification, no direct inline environment editor/install/rollback algorithms, and no rclone command before audit snapshot.

- [ ] **Step 2: Rewrite the runbook around tested helpers**

After the standard-tool bootstrap, the runbook defines the exact operation-local helper path and uses it for every phase that may run before the installed helper is proven:

```bash
UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"
SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"

/usr/bin/python3 "$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR" --archive "$OPERATION_DIR/source.tar" --expected-commit "$REVIEWED_SHA" --expected-archive-sha256 "$ARCHIVE_SHA256"
/usr/bin/python3 "$SOURCE_OPS" prepare-staging --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" snapshot --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" install --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"
```

After install succeeds, the runbook compares `/usr/local/sbin/degen-prod-db-backup-ops` to the exact reviewed manifest hash. Only on an exact match may it use these installed-helper commands:

```bash
/usr/local/sbin/degen-prod-db-backup-ops probe-remote --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops record-dry-run --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops enable-prune --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops observe --operation-dir "$OPERATION_DIR"
```

Every recovery from an already interrupted install, probe, dry run, policy enablement, observation, timer restoration, rollback, or recovery transaction with matching durable state uses `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"`; `recover` refuses absent or mismatched interrupted state and never resolves through a possibly mixed installed binary. A newly approved stable-phase manual rollback instead invokes `/usr/bin/python3 "$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"` after its separate mutation preflight and approval. The runbook documents exact expected evidence, the standard-tool bootstrap, separate push/install/prune gates, source transfer, timer/rclone effects, operation-directory recovery, and irreversible local/remote deletion limits. It explains incomplete-phase refusal, automatic/resumed recovery authorized by the original transaction approval, and the verified-source recovery command. The runbook does not duplicate parser, transaction, or rollback logic. `pg_restore --list` proves archive readability only; the canceled full logical restore rehearsal remains an accepted recovery risk, and this work must not claim end-to-end restore proof.

- [ ] **Step 3: Generate the fixed asset manifest**

Hash exactly these seven assets, with repo-relative paths and no manifest self-entry:

```text
deploy/linux/degen-prod-db-backup.sh
deploy/linux/degen-prod-db-retention.py
deploy/linux/degen-prod-db-backup-env.py
deploy/linux/degen-prod-db-backup-ops.py
deploy/systemd/degen-prod-db-backup.service
deploy/systemd/degen-prod-db-backup.timer
deploy/systemd/degen-prod-db-backup.env.example
```

Tests recompute every digest and reject any extra/missing entry.

- [ ] **Step 4: Run all focused, syntax, systemd, Python3.10, and full checks**

Commit:

```text
docs: finalize Green backup production workflow
```

---

### Task 10: Final whole-change review and immutable publication checkpoint

**Files:** All files in this plan.

- [ ] **Step 1: Run verification-before-completion**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m compileall app
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_retention.py tests/test_degen_prod_db_backup_env.py tests/test_degen_prod_db_backup_script.py tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q -p no:cacheprovider
git diff --check origin/main..HEAD
```

Also run Python 3.10 planner/helper/ops compatibility, WSL Bash syntax, systemd 249-compatible verification, official local rclone 1.74.1 strict-flag probes, and manifest recomputation.

- [ ] **Step 2: Run final spec and security reviews**

Obtain independent whole-change spec compliance and security/code-quality reviews. Resolve every Critical and Important issue, rerun affected focused tests, then rerun the full suite before each fix commit.

- [ ] **Step 3: Present the push-only external-action preflight**

Before pushing, report:

- exact branch and final reviewed commit;
- push without merge, deployment, or Green writes;
- exact branch and remote target;
- reversible branch deletion/remote follow-up and absence of production impact;
- post-push SHA verification.

Wait for Jeffrey's explicit `proceed`.

- [ ] **Step 4: Push the exact reviewed commit and prepare immutable local evidence**

After push approval, push `codex/backup-retention-hardening` and verify the remote SHA. Create the path-limited `git archive` from that exact SHA locally, verify `git get-tar-commit-id`, and record archive and manifest SHA-256. Do not transfer anything to Green yet.

- [ ] **Step 5: Present the production preflight and obtain separate approval**

Report the exact reviewed SHA, archive/manifest digests, operation directory, seven install targets, timer quiescing/restoration, service-inactive and lock controls, rclone audit/token-refresh possibility, disposable remote mutation/cleanup, rollback scope, irreversible limits, and post-action verification. Wait for a new explicit production `proceed` before any Green directory creation, transfer, install, rclone call, or timer change.

---

### Task 11: Approval-gated Green execution

**Files:** No repository edits expected.

- [ ] **Step 1: Install transactionally**

After the production `proceed`, perform the standard-tool archive bootstrap, define `OPERATION_DIR` and `SOURCE_OPS`, then invoke `/usr/bin/python3 "$SOURCE_OPS"` for `verify-source`, `prepare-staging`, `snapshot`, and `install`. Any incomplete phase or recovery before installed-helper verification also invokes `/usr/bin/python3 "$SOURCE_OPS" recover`. Verify exact installed hashes/modes, timer state restoration, inactive backup service, unchanged application/PostgreSQL/web/worker/bot PIDs, operation-state completeness, and an exact SHA-256 match between `/usr/local/sbin/degen-prod-db-backup-ops` and its reviewed manifest entry.

- [ ] **Step 2: Run the disposable probe and production dry-run**

Only after the installed helper hash matches, run `/usr/local/sbin/degen-prod-db-backup-ops probe-remote --operation-dir "$OPERATION_DIR"` and `/usr/local/sbin/degen-prod-db-backup-ops record-dry-run --operation-dir "$OPERATION_DIR"`; bracket rclone metadata, verify probe cleanup, and present exact production candidates. Keep `REMOTE_PRUNE_ENABLED=0`. Any interrupted guard transaction resumes through `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"`.

- [ ] **Step 3: Obtain the second approval and enable pruning**

Even with zero candidates, wait for explicit approval. Reverify the installed helper hash, then run `/usr/local/sbin/degen-prod-db-backup-ops enable-prune --operation-dir "$OPERATION_DIR"` in a fresh invocation and verify the policy epoch, environment hash, timer state, and unchanged application/database PIDs. Recovery still uses `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"`.

- [ ] **Step 4: Observe the next scheduled run**

Do not claim operational success until hash-verified `/usr/local/sbin/degen-prod-db-backup-ops observe --operation-dir "$OPERATION_DIR"` proves a post-policy timer run, current service success, fresh verified local pair, two validated retained pairs, remote final integrity, no remaining planner deletions, disk space, installed hash parity, and unchanged application/database PIDs. An interrupted observation uses `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"` before another observation attempt.
