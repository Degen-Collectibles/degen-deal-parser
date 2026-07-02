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

Task 6 is intentionally executed as four independently reviewed TDD slices. For each slice: write the named failing tests first and observe the expected RED result; implement only that slice; run its focused tests; obtain fresh spec-compliance and security/code-quality reviews; resolve every Critical and Important issue; run the full repository suite; and only then commit the two Task 6 files with the slice-specific message. Do not start Task 7 until Task 6D is committed and all four Task 6 reviews are clean.

#### Task 6A: Add strict operation paths and state

- [ ] **Step 1: Write failing path/state tests**

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
def load_operation_state(path: Path, *, effective_uid: int) -> dict[str, object]: ...
def atomic_write_operation_state(path: Path, state: dict[str, object], *, effective_uid: int) -> None: ...
def build_operation_paths(operation_dir: Path) -> OperationPaths: ...
def validate_operation_state(
    state: object,
    operation_dir: Path,
    previous_state: dict[str, object] | None = None,
) -> None: ...
def validate_operation_state_for_context(
    state: dict[str, object],
    context: OperationsContext,
) -> None: ...
def sanitize_error_text(value: object) -> str: ...
```

The approved `load_operation_state()` and `atomic_write_operation_state()` signatures remain context-free storage primitives. `validate_operation_state_for_context()` is the mandatory 6B-6D boundary that additionally binds `operation_id`, `operation_dir`, and the reviewed commit/archive/manifest digests to one `OperationsContext`; no later command may treat a context-free load as sufficient provenance validation. `build_operation_paths()` derives the fixed `source.tar`, `source`, `snapshot`, `staged`, and `operation-state.json` paths from one operation directory. `sanitize_error_text()` is the only constructor boundary for exception-derived receipt text.

The production CLI always constructs `host_root=Path("/")`; no production argument, environment variable, or test-mode switch may override it. It separately requires the exact lexical production path `/opt/degen/backups/config/<YYYYMMDDTHHMMSSZ>`. Direct test APIs may validate a private temporary operation tree and tests may construct a context with a private temporary host root, but that testability creates no production CLI override. `validate_operation_dir()` and every state-file operation walk the absolute path root-to-leaf with held no-follow directory descriptors, compare named-component and opened-FD identities, and require the final operation directory to be effective-UID-owned mode `0700`; they never trust `Path.resolve()`. Production fails closed if the required POSIX no-follow/directory/close-on-exec primitives are unavailable. `command_runner` retains the exact two-argument `CommandRunner(argv, pass_fds)` contract above, receives argv without a shell and an explicit tuple of inherited file descriptors, and `clock` is the sole source of operation epochs.

Use one fixed no-shell inherited-FD shim for the two commands that need data which must not appear in argv. For `git get-tar-commit-id`, open the verified archive with no-follow semantics, inherit only that archive FD, and have the fixed child shim duplicate it to stdin before `execve()` of Git. For `psql`, place the validated `DATABASE_URL` in a bounded inherited pipe FD, have the same fixed shim read it, set only child-process `PGDATABASE`, close the FD, and `execve()` psql. The secret value must never appear in argv, state, logs, exception text, or any field of the returned `CompletedProcess`; raw command stderr/stdout must be sanitized before operator output or persistence. All other commands pass `pass_fds=()` unless a later task explicitly requires an inherited lock FD.

Use this exact strict JSON top-level contract. Every listed object has exactly the stated keys, rejects extras, and uses JSON `null` only where shown:

```text
schema_version: int, exactly 1
operation_id: non-empty str
operation_dir: absolute str
phase: str
phase_history: list[{phase: str, epoch: int, evidence_sha256: lowercase-hex str}]
reviewed_source: {commit: str, archive_sha256: str, manifest_sha256: str, asset_hashes: dict[str, str]}
effective_config: null|dict[str, str]
host_stage: null|{
  manifest_sha256: str, asset_hashes: dict[str, str],
  environment_sha256: str, enabled_environment_sha256: str
}
snapshot: null|{
  manifest_sha256: str,
  targets: dict[str, {present: bool, sha256: str|null, mode: int|null, uid: int|null, gid: int|null}],
  rclone_audit: {path: str, sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int}
}
prior_runtime: null|{timer_enabled: bool, timer_active: bool, pids: dict[str, int], preinstall_trigger_epoch: int|null}
install: null|{
  next_target_index: int, current_target: str|null, previous_sha256: str|null, intended_sha256: str|null,
  installed_hashes: dict[str, str], started_epoch: int, completed_epoch: int|null,
  runtime_directory_created: bool, validated_epoch: int|null, validation_evidence_sha256: str|null
}
rclone_evidence_groups: list[{
  group_id: str, purpose: str,
  before: {sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int},
  after: null|{sha256: str, inode: int, uid: int, gid: int, mode: int, size: int, mtime_ns: int},
  evidence_sha256: str|null
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
  runtime_baseline: {timer_enabled: bool, timer_active: bool, pids: dict[str, int], preinstall_trigger_epoch: int|null},
  guard: {
    timer_stopped: bool, service_inactive_verified: bool,
    legacy_lock_acquired: bool, runtime_lock_acquired: bool,
    locks_released: bool, timer_restored: bool
  },
  started_epoch: int,
  policy_environment_sha256: null|lowercase SHA-256,
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
  next_target_index: int, current_target: str|null,
  previous_sha256: str|null, intended_sha256: str|null,
  started_epoch: int, completed_epoch: int|null, evidence_sha256: str,
  runtime_directory_created: bool,
  runtime_baseline: {timer_enabled: bool, timer_active: bool, pids: dict[str, int], preinstall_trigger_epoch: int|null},
  restored_epoch: int|null, restore_evidence_sha256: str|null
}
```

When phase history has reached `installed`, `install.installed_hashes` has exactly these seven logical path keys with lowercase SHA-256 values, and the original non-null `install.completed_epoch` remains immutable in every later state, including a later manual-rollback `rolled_back` state. Before history reaches `installed`, `install.installed_hashes` is exactly empty and `install.completed_epoch` is null; therefore a failed initial `installing -> recovering -> rolled_back` history preserves the incomplete install receipt rather than fabricating installed evidence. For the six repo-managed executable/unit targets, each installed hash equals the corresponding reviewed-source asset hash and its corresponding host-stage asset hash. `/etc/degen/prod-db-backup.env` instead equals `host_stage.environment_sha256`; it is host-derived and must never be attributed to `reviewed_source.asset_hashes`.

```text
/usr/local/sbin/degen-prod-db-backup
/usr/local/sbin/degen-prod-db-retention
/usr/local/sbin/degen-prod-db-backup-env
/usr/local/sbin/degen-prod-db-backup-ops
/etc/systemd/system/degen-prod-db-backup.service
/etc/systemd/system/degen-prod-db-backup.timer
/etc/degen/prod-db-backup.env
```

Every `phase_history.evidence_sha256` is an opaque digest of the phase-command evidence bytes whose exact construction is owned by the later command that produces that phase. Task 6A validates only exact lowercase SHA-256 syntax and append-only stability; it never fabricates an entry or recomputes a digest without the producing command's evidence bytes.

Use these phase-valid receipt invariants. `reviewed_source` is required from `source_verified`; `effective_config` and `host_stage` become required at `staging_prepared`; `snapshot` and `prior_runtime` become required at `snapshotted`; and `install` start/progress becomes required at `installing`. `install.installed_hashes` remains empty and `completed_epoch` null until history first reaches `installed`, when the provenance-bound exact seven hashes and completion epoch become required and immutable thereafter. A terminal `rolled_back` validates the install receipt against its actual history: an initial-install recovery history without `installed` keeps empty hashes/null completion, while a manual-rollback history containing `installed` keeps the complete hashes/original install completion and uses `recovery.completed_epoch` for rollback completion. `probe` remains null through `probing` and becomes required at `probed`; `dry_run` remains null through `dry_run_recording` and becomes required at `dry_run_recorded`; `policy` remains null in `policy_enabling` until the enabled environment rename, parent-directory fsync, exact readback, and an atomic state-file CAS durably record the provisional applied-policy receipt while the timer is stopped and both migration locks are held. That receipt is then immutable and required at `policy_enabled`; `observation` remains null through `observing` and becomes required at `observed`. Earlier phases require every later receipt to be null, never fabricated. Stable later phases preserve all earlier receipts except for the single authorized pre-commit policy-recovery reset defined below. `rclone_evidence_groups` and `secondary_errors` are empty lists until evidence or a secondary failure exists; they are never replaced with synthetic entries. Tests construct every phase and reject a missing required receipt, an early or guard-incoherent later receipt, premature or wrong-provenance installed hashes/epochs, incoherent write-ahead cursor tuples, and every receipt regression other than that exact reset. Within `active_transaction`, `policy_environment_sha256` is required and non-null only when `kind="policy"`; it is null for `kind="probe"`, `kind="dry_run"`, and `kind="observe"`. The policy value is the lowercase SHA-256 of the exact precommitted rendered environment bytes containing `REMOTE_PRUNE_ENABLED=1`, recorded durably before the first transaction mutation. It remains immutable through `policy_enabling`, any resulting `recovery_required`, and `recovering_policy`. A successful `policy_enabling -> policy_enabled` transition must preserve that exact digest in `policy.environment_sha256` before clearing `active_transaction`; transition validation rejects any mismatch.

An absent state may become only one fully valid `source_verified` state whose history contains that initial phase. An exact no-op write is allowed and returns without creating a temporary or replacing the state file. Same-phase durable progress is allowed only in `installing`, `recovering`, `manual_rollback`, `probing`, `dry_run_recording`, `policy_enabling`, `observing`, `recovery_required`, `recovering_policy`, `recovering_probe`, and `recovering_guard`. A same-phase write does not append a duplicate `phase_history` entry; it may only advance phase-specific indices, completion fields, active-transaction guard/object booleans, and append-only evidence/error streams monotonically. During `installing`, the four `install` cursor fields form one coherent write-ahead cursor over the exact seven-target order listed above: entry records index zero with the exact first install-target tuple before mutation, and after target `i` plus its parent directory are fsynced one atomic state write advances to `i + 1` and either records the exact next tuple or clears all three tuple fields after the final target. The install cursor cannot skip, regress, reorder, rebind, or change tuple values at an unchanged index.

Every recovery attempt uses the independent four-field cursor inside `recovery`; an install receipt and its cursor remain immutable throughout `recovering` or `manual_rollback`, including the failed initial-install progress already recorded before recovery began. A newly started `kind="install"` or `kind="manual_rollback"` attempt initializes recovery index zero with the exact first restore target and previous/intended hashes before mutation. Live provenance is cursor-bound: before install recovery, targets earlier than the frozen install cursor must be installed, its current target may be snapshot or installed, and later targets must remain snapshot; during restoration, targets earlier than the recovery cursor must be snapshot, its current target may be the frozen baseline or snapshot, and later targets must remain at that frozen baseline; a terminal restore cursor requires every target to equal snapshot. Policy-origin manual rollback uses the exact enabled-policy environment digest as that target's frozen baseline. A newly started `kind="policy"` attempt uses index zero and the fixed `/etc/degen/prod-db-backup.env` tuple. If no provisional applied-policy receipt exists, `intended_sha256` is exactly `host_stage.environment_sha256` and `previous_sha256` may be exactly the disabled or enabled digest; recovery restores the exact disabled bytes and returns to `installed`. If the provisional applied-policy receipt exists, it is the commit point: `intended_sha256` and `previous_sha256` must both be the exact enabled digest, recovery never rolls it back, and it forward-completes to `policy_enabled`. Task 8 validates the live environment bytes before recording or advancing either cursor; any third digest fails closed. After that single target is durable, policy recovery advances to index one and clears the tuple. `kind="probe"` and `kind="guard"` never restore host files and require index zero with all three tuple fields null. After every restored target and its parent directory are fsynced, one atomic state write advances the recovery cursor in exact restore order and either records the next exact tuple or clears the tuple after the final target. A `recovery_required -> recovering|recovering_policy|manual_rollback|recovering_probe|recovering_guard` transition resumes the same attempt and must preserve and continue its existing cursor, `started_epoch`, and evidence digest; it never reinitializes, skips, or regresses them. `recovery.completed_epoch` remains null until all required recovery work, lock release, exact timer restoration, and the terminal receipt CAS required by the later owning task have succeeded. Stable phases are otherwise immutable until an allowed forward transition.

Allow only these phase-changing transitions: `source_verified -> staging_prepared -> snapshotted -> installing -> installed -> probing -> probed -> dry_run_recording -> dry_run_recorded -> policy_enabling -> policy_enabled -> observing -> observed`; `installing -> recovering -> rolled_back|recovery_required`; `policy_enabling -> recovering_policy -> installed|policy_enabled|recovery_required`; `probing|dry_run_recording|observing -> recovery_required`; and, after separate explicit manual-rollback approval, `installed|probed|dry_run_recorded|policy_enabled|observed -> manual_rollback -> rolled_back|recovery_required`. Every permitted phase change appends exactly one history entry whose phase equals the new state phase; every other existing history entry remains byte-for-byte stable. A `recovery_required` state transitions only to the phase matching recorded recovery kind: `recovering` for `install`, `recovering_policy` for `policy`, `manual_rollback` for `manual_rollback`, `recovering_probe` for `probe`, or `recovering_guard` for `guard`. A probe failure records `recovery.kind="probe"` and retains `active_transaction.kind="probe"` through `recovering_probe`. Guard recovery records `recovery.kind="guard"` while `active_transaction` retains the original `dry_run` or `observe` kind and its recorded prior stable phase; `recovering_guard` may return only to that recorded phase or to `recovery_required`. `recovering_probe -> installed|recovery_required`; `rolled_back` is terminal. The external Task 7 operator preflight and approval authorizes a manual rollback; Task 6A validates only the resulting transition structure and cannot grant that authorization.

The only authorized receipt regression is the successful pre-commit `recovering_policy -> installed` reset after a failed prune-enablement environment mutation. That transition atomically clears `probe`, `dry_run`, `policy`, and `observation` to JSON null, clears `active_transaction`, preserves the earlier reviewed source, staging, snapshot, prior-runtime, install, append-only phase history, rclone evidence, failure, secondary-error, and recovery evidence, and records the reset transition in phase history. All four later receipts must clear together; a partial clear or the same regression on any other transition is invalid. Returning to `installed` deliberately invalidates the old probe/dry-run approval chain, so a new probe, dry run, review, and explicit prune approval are required before enablement can be retried. Once the provisional applied-policy receipt is durable, recovery instead preserves it and the dry-run chain exactly, restores the runtime baseline, seals the policy-recovery identity into the completion evidence, and transitions only to `policy_enabled`.

Require `active_transaction` to be non-null throughout `probing`, `dry_run_recording`, `policy_enabling`, `observing`, and their applicable `recovery_required`/recovery phases, with a kind and prior stable phase matching the transition; require it null in stable phases after successful cleanup and timer restoration. Its immutable `runtime_baseline` is the full runtime captured immediately before entry; `prior_timer_enabled` and `prior_timer_active` must mirror that baseline, and every Task 8 recovery receipt must copy it exactly so process-death recovery never reconstructs PIDs or the last trigger from stale/live state. Require the final `phase_history` entry to equal `phase`, and require phase history, rclone evidence, and `secondary_errors` to be append-only with stable evidence digests. Epochs are nondecreasing within each ordered stream (`phase_history`, `secondary_errors`, and successive attempts). Enforce explicit lifecycle ordering: install start is no later than install completion; recovery start is no later than recovery completion; active-transaction start equals its entering phase epoch; policy enablement is no earlier than completed installation and its `policy_enabling` phase; and an observation run is strictly newer than the maximum of completed installation, recorded policy enablement, and any recorded pre-install trigger. The `observing` phase records later evidence capture, so it is not a lower bound on the already-completed scheduled run. `prior_runtime.preinstall_trigger_epoch` may predate operation history and is compared only by the later runtime checks that consume it, not against every operation epoch.

`recovery` is the current/latest recovery-attempt receipt, not an append-only recovery history. Within one attempt its `kind` and `started_epoch` are immutable; its index and tuple advance only through the kind-specific rules above; `completed_epoch` changes only from null to a valid later epoch after its required completion boundary; and its evidence digest remains bound to that attempt. A completed receipt remains preserved in later stable state. It may be replaced only when a later separately authorized recovery transition starts after the prior attempt has been summarized by the append-only phase history and evidence streams; replacement during an existing attempt, a `recovery_required` resume, or an ordinary same-phase write is invalid. Context-free `validate_operation_state()` derives the latest recovery-attempt start solely from the complete `phase_history`: a new attempt begins at a `recovery_required` entry reached from a non-recovery failure in `probing`, `dry_run_recording`, or `observing`; at a `recovering` or `recovering_policy` entry reached directly from `installing` or `policy_enabling`; or at a `manual_rollback` entry reached from a stable phase. A transition from `recovery_required` into its recorded recovery phase and every same-phase continuation are resumes, not new attempts, so they never reset the derived start. Whenever `recovery` is non-null, `recovery.started_epoch` must equal the epoch of that latest start entry even when `previous_state` is unavailable, including after a completed recovery receipt is preserved in a later stable or terminal state.

Require `operation_id`/`operation_dir` plus reviewed source digests to remain bound to `OperationsContext`. The first `failure.primary_error` is immutable once written. Callers must run exception-derived text through `sanitize_error_text()` before constructing `failure` or `secondary_errors`; the state validator then rejects any residual secret-like key, value, primary-error text, or secondary-error text and never silently rewrites supplied state. Rollback, recovery, cleanup, and timer-restoration errors append to `secondary_errors` in occurrence order and can never replace the primary error. State never serializes database URLs, tokens, passwords, environment-file contents, or rclone contents.

Task 6A tests reject wrong operation roots, symlink path components, owner/mode mismatch, state-file symlinks or hard links, state secrets, duplicate keys at every JSON depth, corrupt/trailing JSON, non-atomic state writes, missing or extra fields at every schema level, operation-path rebinding, altered or truncated phase/evidence/error history, replacement or reordering of `secondary_errors`, mutation of the first primary error, regressed phases, phase-invalid nullability, forbidden transitions, unauthorized or partial receipt resets, secret-like error content, bool-as-int values, finite floats, non-finite numbers, and impossible lifecycle ordering. Tests separately construct and accept both legal terminal histories: failed initial install recovery with empty installed hashes/null install completion, and manual rollback after `installed` with the exact seven provenance-bound hashes/original install completion plus a distinct recovery completion. They reject every reordered/skipped/rebound install or recovery cursor; missing/extra recovery tuple fields; a reset of cursor, start epoch, or evidence on `recovery_required` resume; a `recovery.started_epoch` that differs from the latest attempt-start epoch derived from `phase_history` when validating without `previous_state`, including a completed receipt preserved in a later state; non-null probe/guard tuples; a policy transaction with a null, malformed, or mutable enabled-environment digest; a non-policy transaction with a non-null policy digest; a successful policy receipt whose environment digest differs from the precommitted transaction digest; a policy tuple targeting anything except the fixed environment path; an uncommitted policy recovery whose index-zero intended digest is not the disabled host-stage digest; a committed policy recovery whose intended or previous digest is not the exact enabled-policy digest; a policy recovery `previous_sha256` outside its phase-valid disabled/enabled digest set; mutation of the frozen install receipt during recovery/manual rollback; premature or terminal-epoch-unbound recovery completion; impossible policy/guard milestone combinations; and any environment hash sourced from the reviewed asset manifest instead of `host_stage.environment_sha256`. Develop the one reviewed Task 6A commit in this focused TDD order without intermediate partial commits: interfaces/Python 3.10 grammar/CLI surface; operation-directory and state-file descriptor/metadata checks; strict JSON decoding and primitive schema types; exact recursive object schemas; the phase receipt/nullability matrix; active-transaction/recovery and transition/history invariants; the authorized policy reset; sanitization/secret rejection; atomic write ordering/failure/race cases; then exact `show-state` output and sanitized errors.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider
```

Expected: collection fails because `deploy/linux/degen-prod-db-backup-ops.py`, `OperationsContext`, strict phase-valid state loading, and atomic state commands do not exist.

- [ ] **Step 3: Implement strict paths and atomic state**

Expose only `show-state --operation-dir DIR` in this slice. Require `/opt/degen/backups/config/<YYYYMMDDTHHMMSSZ>` in production, effective UID zero, a real root-owned operation directory with exact mode `0700`, no symlink in any validated path component, and `operation-state.json` as a root-owned regular single-link file with exact mode `0600`. Open and bind directories/files with no-follow descriptors instead of trusting `Path.resolve()`. `show-state` performs the production lexical-root check, descriptor/metadata validation, exact schema and operation-path binding, and residual-secret rejection before emitting the exact canonical state JSON plus one LF; failures use concise sanitized stderr, emit no stdout or traceback, and never print untrusted state contents.

Validate the exact recursive schema and phase rules above before serialization and after every read. Load JSON with an all-depth duplicate-pair hook and non-finite-value rejection; finite floats also fail the integer-only schema. Canonical bytes are UTF-8 `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)` plus one LF.

Before creating a temporary, validate the replacement, require caller-derived receipt errors already sanitized through `sanitize_error_text()`, reject residual secret-like keys/values without rewriting them, and load/validate the existing state through the held operation-directory descriptor. Write through an unpredictable exclusive same-directory `0600` temporary, verify its path/FD binding, write all canonical bytes, and fsync the file. Immediately before replacement, compare the destination's identity and exact old bytes with the originally loaded state as a compare-and-swap guard. Atomically replace via the validated directory FD, fsync the parent, then reopen the destination no-follow and revalidate exact canonical bytes, metadata, inode binding, and parent binding. A failure before replacement leaves the old destination byte-for-byte unchanged; cleanup never unlinks by name after an inode race. These checks ensure immutable bindings, append-only histories, first-failure immutability, no-op/same-phase rules, and transition legality cannot be bypassed.

- [ ] **Step 4: Review, run the full suite, and commit Task 6A**

After focused tests and Python 3.10 grammar checks pass, follow the shared independent-review gate, then run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q -p no:cacheprovider
```

Commit only the two Task 6 files:

```text
feat: add strict Green backup operation state
```

#### Task 6B: Verify immutable reviewed source

- [ ] **Step 1: Write failing source-verification tests**

Add focused tests named for these contracts: archive digest fails before Git or tree access; expected commit, archive digest, and independently approved manifest digest all bind to `OperationsContext` and state; archive member names/types are exact; duplicate, extra, missing, traversal, absolute, backslash, symlink, hard-link, sparse, device, FIFO, and unknown entries fail; required Git-archive parent directory entries are accepted only when they are exact real directories; every asset hash matches; and the already extracted source tree contains exactly the reviewed regular files with no links or extras. No failure may create `operation-state.json`.

- [ ] **Step 2: Verify Task 6B RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider -k "source or archive or manifest"
```

Expected: failures identify the missing `verify-source` command, approved manifest-digest binding, archive/member verification, and exact extracted-tree validation.

- [ ] **Step 3: Implement source verification without rewriting the running source tree**

Define the source-verification interface in this slice:

```python
def verify_source_archive(
    context: OperationsContext,
    *,
    source_dir: Path,
) -> dict[str, str]: ...
```

`source_dir` is a read-only view of the already extracted tree and must equal `context.paths.source_dir`. The function obtains the archive path from `context.paths.source_archive` and binds the expected commit, archive SHA-256, and manifest SHA-256 exclusively from the same `OperationsContext`; callers cannot pass a second conflicting digest or destination.

Expose:

```text
verify-source --operation-dir DIR --archive FILE --expected-commit SHA --expected-archive-sha256 DIGEST --expected-manifest-sha256 DIGEST
```

The manifest digest argument is mandatory, is copied into `OperationsContext.expected_manifest_sha256`, and must exactly match both `reviewed_source.manifest_sha256` and the verified extracted manifest. Do not self-derive an approval anchor from untrusted archive content.

Avoid circular trust with this bootstrap sequence before executing the new helper:

1. Locally create a path-limited uncompressed `git archive` containing only the fixed manifest and seven reviewed assets; record archive SHA-256, embedded commit, and manifest SHA-256.
2. After production approval, set `UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"`, `OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"`, and `SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"`; create the root-only operation directory and transfer the archive without touching `/opt/degen/app`.
3. With existing Green tools only, verify the expected archive SHA-256 and `git get-tar-commit-id`, list the exact expected member names/types, reject links or extras, and extract once, without following links, into the new root-only `$OPERATION_DIR/source` directory.
4. Verify the extracted manifest SHA-256 supplied in the approved preflight.
5. Only then invoke `/usr/bin/python3 "$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR" --archive "$OPERATION_DIR/source.tar" --expected-commit "$REVIEWED_SHA" --expected-archive-sha256 "$ARCHIVE_SHA256" --expected-manifest-sha256 "$MANIFEST_SHA256"`.

The helper independently re-hashes the archive, sends the no-follow archive FD to `git get-tar-commit-id` through the fixed inherited-FD shim, parses the strict manifest, verifies exact archive members/types/hashes, and validates the exact already extracted `$OPERATION_DIR/source` tree. It never calls a general-purpose archive extraction API and never rewrites, renames, or replaces the directory containing the running helper. Only after every check succeeds may it atomically create the initial `source_verified` state.

Task 9 owns the real fixed manifest. Task 6B tests create exact fixture manifests, but production bootstrap and `verify-source` must fail closed while the tracked manifest is absent or its approved digest is unavailable. This dependency does not authorize archive transfer, operation-directory creation, or any other production action before Tasks 9-10 and the explicit production approval gate.

- [ ] **Step 4: Review, run the full suite, and commit Task 6B**

Follow the shared independent-review gate and full-suite command, then commit only the two Task 6 files:

```text
feat: verify immutable Green backup source
```

#### Task 6C: Prepare verified host staging

- [ ] **Step 1: Write failing host-staging tests**

Add focused tests proving: no verified existing local pair fails before staging; canonical lowercase one-record sidecar grammar, recomputed SHA-256, and `pg_restore --list` run in that order; the filename-derived prefix exactly matches independently queried `current_database()` and `hostname -s`; the database URL travels only through the bounded inherited FD and child-only `PGDATABASE`; the manifest-verified environment helper remains the sole managed-environment parser/renderer; `REMOTE_PRUNE_ENABLED=0` is forced; staged assets/modes and the host-stage manifest are exact; and no database URL or environment/rclone content reaches argv, state, logs, exceptions, or `CompletedProcess`.

- [ ] **Step 2: Verify Task 6C RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider -k "staging or existing_pair or pgdatabase or host_stage"
```

Expected: failures identify the missing `prepare-staging` command, inherited-FD database transport, verified pair/identity checks, environment rendering, and staged manifest.

- [ ] **Step 3: Implement host staging**

Expose `prepare-staging --operation-dir DIR`. Revalidate source, strict state, and context binding first. Validate the newest existing complete local pair, derive its prefix, query database and host identity independently, and stop on no verified pair or any identity/path/policy mismatch. Load the environment helper only from the manifest-verified source tree and render every managed key into a new root-only staged file with pruning disabled. Stage exact reviewed bytes and final modes separately from installed paths, generate and immediately verify a strict host-stage manifest, persist only non-secret effective configuration/hashes, and atomically transition `source_verified -> staging_prepared` after all checks succeed.

- [ ] **Step 4: Review, run the full suite, and commit Task 6C**

Follow the shared independent-review gate and full-suite command, then commit only the two Task 6 files:

```text
feat: stage verified Green backup assets
```

#### Task 6D: Snapshot current host state safely

- [ ] **Step 1: Write failing snapshot tests**

Add focused tests proving: each exact target is represented by saved regular-file bytes plus mode/uid/gid/hash or by one mutually exclusive explicit absence marker; symlink, nonregular, hard-linked, unstable, or replaced sources fail; the rclone audit copy is created before any later rclone use but is never a rollback target; JSON contains only rclone metadata/hash and never credential bytes; sorted `SHA256SUMS` covers every and only saved files/absence markers/audit evidence and is reverified before state transition; timer enabled/active state, trigger epoch, and protected service PIDs are exact; and a private test `host_root` cannot escape while production still has no override.

- [ ] **Step 2: Verify Task 6D RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider -k "snapshot or absence or rclone_audit or prior_runtime"
```

Expected: failures identify the missing `snapshot` command, safe saved-or-absent representation, rclone audit boundary, exact manifest, and prior-runtime evidence.

- [ ] **Step 3: Implement safe snapshot capture**

Define the snapshot interface in this slice:

```python
def snapshot_host_state(context: OperationsContext) -> dict[str, object]: ...
```

Expose `snapshot --operation-dir DIR`. Snapshot every install target below plus a root-only rclone audit copy. For each install target, save exact bytes/mode/owner or an explicit mutually exclusive absence marker. Build and immediately verify `SHA256SUMS`; never store credential contents in JSON and never designate `rclone.conf.audit` for automatic rollback.

```text
/usr/local/sbin/degen-prod-db-backup
/usr/local/sbin/degen-prod-db-retention
/usr/local/sbin/degen-prod-db-backup-env
/usr/local/sbin/degen-prod-db-backup-ops
/etc/systemd/system/degen-prod-db-backup.service
/etc/systemd/system/degen-prod-db-backup.timer
/etc/degen/prod-db-backup.env
```

Revalidate source, staging, state, and host-root bindings before reading host targets. Use no-follow descriptors and stable path/FD metadata, write every snapshot artifact exclusively below the root-only snapshot directory, and verify the complete manifest before atomically transitioning `staging_prepared -> snapshotted`. Capture prior timer state, the pre-install trigger, and application/PostgreSQL/web/worker/bot PID evidence without restarting or mutating any service. If an owning account, unit, or unique active PostgreSQL service cannot be identified, fail rather than guess.

- [ ] **Step 4: Review, run the full suite, and commit Task 6D**

Follow the shared independent-review gate and full-suite command, then commit only the two Task 6 files:

```text
feat: snapshot Green backup host state
```

Task 7 consumes the Task 6 atomic state store, exact snapshot representation, immutable source/staging bindings, no-shell command runner, inherited-FD contract, failure sanitization, and phase validator without weakening them. Task 8 consumes the predeclared active-transaction/rclone/probe/dry-run/policy/observation schema and the exact policy-recovery reset above. The independently reviewed Task 8 compatibility tests add `host_stage.enabled_environment_sha256`: staging derives it from the exact staged disabled bytes by replacing the single exact `REMOTE_PRUNE_ENABLED=0` assignment with `1`, binds it into the canonical host-stage manifest as `host_environment.enabled_sha256`, and leaves the disabled staged bytes and `host_stage.environment_sha256` unchanged as install/rollback provenance. Neither later task may change the state format silently; any further required schema change starts with a new failing compatibility test and independent review.

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
write-ahead cursors follow the exact fixed seven-target order with coherent index/current/previous/intended tuples
recovery uses its independent cursor and recovery_required resume never resets its cursor/start/evidence
manual rollback and initial-install recovery leave the historical install receipt/cursor immutable
failed initial-install rollback and later manual rollback preserve their distinct history-sensitive install receipts
all six repo-managed installed hashes bind to reviewed-source and host-stage assets, while the environment binds only to host_stage.environment_sha256
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

Replace each target transactionally in the exact seven-target order declared by Task 6A. Create a same-directory owned staging file with `O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC`, write exact reviewed bytes, set and verify exact owner/mode, fsync the file, and close it. Entry into a new `installing` phase atomically records index zero and the exact first-target tuple in the `install` cursor before mutation. Entry into a new install-recovery or `manual_rollback` attempt instead freezes the entire historical install receipt and atomically creates an independent `recovery` cursor at index zero with the exact first restore-target tuple. A resumed `recovery_required` attempt reuses its existing recovery cursor, `started_epoch`, and evidence digest without resetting any of them. The persisted active cursor for target `i` remains unchanged while that target is renamed or unlinked and its parent is fsynced. Only after both target and parent durability succeed may one atomic state write advance the active index to `i + 1` and replace its tuple with the exact next target, or clear the tuple after the seventh target. Recovery uses the verified current/snapshot hash or null absence marker for the exact previous/intended values. Refuse any cursor whose index, target, previous hash, or intended hash is reordered, skipped, reset, rebound, or inconsistent with the verified snapshot/staging manifests. State-file replacement itself uses a same-directory exclusive temporary file, file fsync, `os.replace`, and parent-directory fsync. Tests inject process death and reconstruct a fresh helper after every state write, staged-file fsync, target rename/unlink, parent fsync, daemon reload, validation, lock release, and timer restoration.

Install the four executable targets `0755`, the two units `0644`, and the host-derived environment `0600`; do not install the tracked environment example. The six executable/unit destination hashes must equal their corresponding reviewed-source and host-stage asset hashes. The environment destination must equal `host_stage.environment_sha256` and is never validated against `reviewed_source.asset_hashes`. Reload systemd, then run `/usr/local/sbin/degen-prod-db-backup preflight --lock-fd N`, where `N` is `MigrationLocks.runtime_fd` explicitly inherited through `pass_fds` while the parent continues holding `legacy_fd`. The child validates and shares the inherited new-lock open-file description rather than opening a competing lock. Never start the oneshot unit during install validation, and tests must prove no dump, prune, delete, or normal `ExecStart` occurs. Verify hashes/modes, but while phase remains `installing` keep `install.installed_hashes` empty and `install.completed_epoch` null.

`recover` must be invoked with the helper under the previously verified `context.paths.source_dir`, never whichever installed helper may be mixed after a crash. It revalidates the archive, reviewed source, host stage, snapshot manifest, strict state, and operation binding; captures or reuses the exact prior timer state; quiesces the timer; requires service inactivity; acquires both migration locks; and atomically restores every snapshot target or absence marker with the independent recovery cursor and the same write-ahead/fsync protocol. A newly entered install-recovery attempt initializes that cursor once; a `recovery_required -> recovering` resume preserves and continues its recorded cursor, `started_epoch`, and evidence digest. The historical install receipt/cursor never changes during either path. Recovery then runs `daemon-reload`, verifies target bytes/modes/owners/absence and unchanged application/database PIDs, removes only operation-owned staging temporaries, and persists restored-but-incomplete `recovering` evidence. Only after releasing both locks and restoring the timer's exact prior enabled/active state may it append terminal `rolled_back` and set `recovery.completed_epoch`. A failed initial installation whose history never reached `installed` keeps its frozen historical install progress, `install.installed_hashes` empty, and `install.completed_epoch` null in `rolled_back`; rollback completion must not fabricate installation completion. If target restoration, lock release, timer restoration, or final state persistence cannot complete, stop/quiesce the timer when possible, preserve the first immutable `failure.primary_error`, append each sanitized recovery failure to `secondary_errors` in occurrence order, persist `recovery_required`, and do not claim rollback complete.

Before restoring an active persistent timer after install, atomically record provisional install evidence, release both migration locks, revalidate that the backup service remains inactive, and restore the timer's exact prior state. Only after exact timer restoration may state atomically populate all seven `install.installed_hashes` with the six reviewed-source/host-stage asset hashes plus `host_stage.environment_sha256`, set `install.completed_epoch`, and transition from `installing` to `installed`. Any timer-restoration failure creates `failure.primary_error` only when no failure exists; otherwise it appends a sanitized `secondary_errors` entry without changing the first failure, then enters the same verified-source recovery flow. It does not attempt an ad hoc in-process rollback. The initial install/enable approval explicitly authorizes automatic or resumed recovery of that transaction. An unrelated later manual `rollback` still requires its own mutation preflight and approval.

Manual `rollback` revalidates the snapshot and state, quiesces the timer, requires service inactivity, acquires both migration locks, initializes and advances only the independent manual-rollback recovery cursor, restores with the same atomic protocol, reloads systemd, verifies destinations and PIDs, persists provisional recovery evidence, releases both locks, and restores the exact prior timer state before recording terminal `rolled_back` with `recovery.completed_epoch`. Because this history already contains `installed`, manual rollback preserves the complete historical install receipt/cursor, including all seven exact `install.installed_hashes` and the original `install.completed_epoch`; it never replaces the install completion with the rollback epoch. A resumed manual rollback continues the existing recovery cursor and never reinitializes it. Locally pruned and remotely deleted backup objects remain explicitly unrecoverable.

- [ ] **Step 4: Run focused/full verification and commit**

Commit:

```text
feat: add transactional Green backup installation
```

---

### Task 8: Add rclone evidence, disposable probe, enablement, and observation

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup-ops.py`
- Modify: `deploy/linux/degen-prod-db-backup.sh`
- Modify: `deploy/systemd/degen-prod-db-backup.service`
- Modify: `tests/test_degen_prod_db_backup_ops.py`
- Modify: `tests/test_degen_prod_db_backup_script.py`

- [x] **Step 1: Write failing remote/state tests**

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
- state fsynced before timer stop, each service/lock/rclone action, lock release, and timer restoration; policy temp/rename/parent-fsync/cleanup substeps run under one already-fsynced exact environment intent/cursor and are followed by explicit parent-directory durability adoption before cursor advance;
- process-death/restart recovery before and after timer stop, service check, each lock, every rclone operation, cleanup, each lock release, and timer restoration;
- probe prefix plus every intended name/digest/size persisted before the first create, with created/verified/cleaned progress persisted after each result;
- verified-source probe recovery deletes only matching state-tracked objects and proves the unique prefix empty;
- dry-run/observation guard recovery performs no remote deletion and restores the exact prior timer state;
- fresh-shell enablement repeating timer/service/lock/rollback controls;
- enablement re-inventory exactly matching the reviewed inventory and candidate hashes;
- enabled environment bytes rendered and hashed into `active_transaction.policy_environment_sha256` before the first mutation, with the digest immutable through policy recovery and copied exactly into the successful policy receipt;
- failed enablement validating live environment bytes against the exact disabled/enabled digests: before the applied-policy receipt commit point, restore and durably re-adopt the disabled environment and remove the incomplete approval chain; after that receipt, preserve the enabled environment and applied epoch and forward-complete recovery;
- process-death injection immediately after the enabled environment rename and before its parent-directory fsync/state advance, followed by verified-source policy recovery with the exact index-zero cursor tuple;
- scheduled cutoff `max(install.completed_epoch, policy.enabled_epoch, prior_runtime.preinstall_trigger_epoch)` using the actual applied-policy receipt epoch;
- observation serialized with the same timer/service/lock guard and lock-before-timer restoration order;
- success properties, trigger/start/log/artifact freshness, installed hashes, runtime rclone metadata receipts, sidecar grammar, SHA, archive listing for both retained local pairs, remote integrity, and PID parity.

- [x] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py --tb=short -q -p no:cacheprovider -k "probe or dry_run or enable or observe or rclone"
```

Expected: failures naming the missing remote probe, durable in-progress phases/active transaction, crash-safe probe and guard recovery, state-bound dry run, guarded policy enablement, fresh scheduled observation, and rclone evidence-group behavior.

- [x] **Step 3: Implement the remaining subcommands**

Expose:

```text
probe-remote --operation-dir DIR
record-dry-run --operation-dir DIR
enable-prune --operation-dir DIR
observe --operation-dir DIR
```

Every guarded command atomically writes and fsyncs `active_transaction` before its first external mutation and before every later timer, service, lock, rclone, lock-release, or timer-restoration action. The record includes exact prior stable phase, the full immutable runtime baseline, mirrored timer enabled/active state, and monotonic guard progress; its `started_epoch` equals the entering phase epoch. A forced byte-identical checkpoint is a durability fence, not a serialized `next_action`: crash recovery combines the last completed milestone with idempotent live timer/service/lock reconciliation, while each non-idempotent rclone or file mutation has its own durable operation-specific intent/progress receipt. The atomic policy environment temp, rename, parent-fsync, readback, and deterministic-temp cleanup sequence is the file-mutation exception: it runs under one already-fsynced exact policy digest/recovery cursor, does not rewrite state between those inseparable filesystem substeps, and must fsync and revalidate the parent/target again before cursor or terminal receipt advance when adopting a result left visible by an interrupted process. Every Task 8 rclone action first appends one pending evidence group with `outcome=null`, then completes only that same final group in a later same-phase write with outcome exactly `success` or `indeterminate`; the canonical audit digest includes the outcome, and completed groups globally follow phase-history attempt order. `success` is recorded only after purpose-specific semantic validation succeeds: strict recursive inventory decoding, exact downloaded SHA-256/name, empty delete output, or an actually empty final prefix. An ordinary command/semantic failure or a pending audit closed by a fresh recovery process is `indeterminate`; it remains ordered mutation intent but cannot advance progress or prove terminal cleanup. Task 8 group IDs are `task8:<kind>:<started_epoch>:<attempt_ordinal>:<group_ordinal>`, so same-second retries cannot collide. Policy enablement additionally includes the SHA-256 of the exact precommitted enabled environment bytes and must equal `host_stage.enabled_environment_sha256`; every other transaction kind records that field as null. It quiesces the timer, verifies the service inactive, acquires legacy then runtime locks through `acquire_migration_locks()`, performs the command, releases both locks, restores the exact prior runtime baseline, writes the stable receipt, clears `active_transaction`, and only then enters the next stable phase. A crash in `probing`, `dry_run_recording`, or `observing` persists `recovery_required` without discarding guard progress. Tests inject process death before and after timer stop, service check, each lock, every rclone operation, each environment rename and parent-directory fsync, each cleanup action, each lock release, and timer restoration, then resume in a fresh process through the verified source helper.

Fresh-process guard recovery treats persisted lock booleans only as historical milestones, never as proof that the new process owns either file descriptor. For an interrupted dry run or observation, verified-source `recover` first writes the deterministic immutable primary failure in the raw phase, reopens an exact proof for that new raw state, writes the separate null-cursor `kind="guard"` recovery receipt and `recovery_required` phase, reopens exact proof again, and only then enters `recovering_guard`. It locally closes any pending rclone-config before/after audit receipt without replaying the rclone command or any remote mutation, then reopens exact `recovering_guard` proof before external work. Unless `timer_restored=true` and a fresh full runtime readback already equals the immutable baseline, recovery idempotently quiesces the timer and reacquires fresh legacy then runtime locks regardless of the historical acquired/released flags. A mismatched `timer_restored=true` readback takes that same full reconciliation path. Fresh locks release runtime then legacy; every release issue is retained as its own ordered secondary receipt, and any issue blocks timer restoration. Only a clean fresh release followed by exact baseline restoration can set or confirm `timer_restored`, complete recovery, clear `active_transaction`, and return to the recorded stable phase. Ordinary failure preserves the first primary error, records later operational/release/quiesce errors in occurrence order, leaves or returns to `recovery_required`, and keeps the timer quiesced when restoration was incomplete. Process-death exceptions best-effort close live descriptors and propagate without fabricating completion or failure receipts.

`probe-remote` transitions `installed -> probing` and uses a random sibling namespace such as `onedrive:backups/degen-db-probe/<operation-id>-<32-lowercase-hex-token>/`. The dump bytes are deterministically `degen-db-remote-probe-v1\noperation_id=<operation-id>\ntoken=<token>\n`; the sidecar is the lowercase dump SHA-256 followed by two spaces, `probe.dump`, and one newline. A fresh process can therefore reconstruct their exact names, hashes, and sizes from the stable prefix. Before its first rclone create, atomically persist and fsync the unique prefix and the complete intended two-object list with each exact name, expected SHA-256, expected size, and `created=false`, `verified=false`, `cleaned=false`. The `probing` history digest commits that deterministic entry, and the final probe/history digest commits the entry plus only the exact ordered completed rclone groups for that attempt. Immediately before each strict no-existing create, verification, or cleanup call, fsync the next progress state; after each successful result, fsync the corresponding flag before another external mutation. Delete only an object named in that precommitted list whose live size/digest evidence matches state and whose strict no-existing ownership evidence is valid. Prove the prefix empty, restore the guard, then persist the stable `probe` receipt and transition `probing -> probed`. No production-prefix object is ever a probe cleanup target.

If probing crashes, verified-source `recover` revalidates archive/source/state, the unique prefix, pre-create absence evidence, strict no-existing command evidence, and each live object's exact state-tracked name/digest/size. A config-audit purpose is command intent, not semantic proof: only `outcome=success` grants authority, while indeterminate create/cleanup remains visible so recovery cannot duplicate or forget a potentially side-effecting mutation. Recovery always freshly quiesces and reacquires legacy then runtime locks, recursively lists the whole operation-bound prefix without `--files-only`, rejects directories/nesting/untracked names/case collisions/size drift, and uses `/usr/bin/rclone --config /etc/degen/rclone.conf hashsum SHA-256 <exact-state-target> --download` before any delete. An uncheckpointed successful or indeterminate create forces recovery inventory. If its exact deterministic object is live, `probe-adopt:<name>` binds a fresh exact hash before separate durable `created` and `verified` writes; if absent, no flags are fabricated and a fresh terminal empty-prefix proof closes it safely. A successful or indeterminate delete whose object is freshly absent advances cleanup without replay; a still-present exact object is rehashed before one bounded retry. Any untracked object, metadata/hash mismatch, missing pre-create/creation evidence, stale attempt, or malformed output stops recovery without deletion. Every progress write is separate from its rclone audit write and causally bound to the physical-last completed successful current-attempt purpose; a fresh-process pending audit finalizes indeterminate and never impersonates success. Both context-free stable receipt validation and transition validation require the full all-success ownership chain and physical-last current-attempt `probe-prefix-empty`, so a resealed nonterminal or indeterminate receipt is rejected after restart. After proving the prefix empty, recovery releases runtime then legacy lock, restores the exact prior timer state, clears `active_transaction`, and transitions `recovering_probe -> installed`. Failure preserves the first primary error, appends sanitized secondary errors in occurrence order, returns to `recovery_required`, and keeps the timer quiesced when restoration is incomplete. Process-death exceptions leave pending evidence/progress untouched and do not fabricate error receipts.

Slice 2C checkpoint (2026-07-01): the fresh-process probe-recovery boundary above is implemented and independently reviewed on helper SHA-256 `7c01632adbe8a85d17355eaed699354dd9822124f139360df9e68a1d5eaf02c0` and test SHA-256 `23fdab47b1343a2396b254c4b1dbe2de9c5ebe80936a5b0f0a07d85b843aa097`. Focused Task 8 verification passed `335` tests with `6` platform skips; the full helper file passed `991` tests with `71` skips on Windows and `1058` tests with `4` skips under WSL CPython 3.10. Two independent adversarial reviews reported no remaining Critical, Important, or Minor finding for this slice. This checkpoint does not mark Task 8 complete: normal probe execution, dry-run, enablement, observation, and their remaining recovery paths still follow below.

Slice 2D checkpoint (2026-07-01): normal disposable-probe execution and its complete fresh-process recovery path are implemented and independently reviewed on helper SHA-256 `c0cfd11d5dd857c7c72dd7cd08f8e8cc145be8a724a0bcc576a1de570e2c34b8` and test SHA-256 `90cbf8f420250a097cac77ef6a40a4fc46fc08559641c7538fbbe64ecc4a8ab3`. Normal execution holds an exact installed-helper path/hash proof through every audited remote action and the stable completion write; recovery remains source-helper-only. Both creation and recovery use two-level nonrecursive namespace proofs before any exact-prefix listing, then require a post-create exact owned inventory, downloaded hashes, bounded exact deletes, and a terminal current-attempt empty or absence proof. Probe payloads use deterministic root-only `O_EXCL` operation-directory source files because a real rclone 1.74.1 smoke test showed that `/proc/self/fd/<n>` is treated as a directory; every rclone child inherits the held runtime-lock descriptor so an orphaned child blocks fresh recovery until it exits. Local real-binary tests also proved strict no-existing copy behavior and inherited-lock retention without contacting Green or OneDrive. Final Task 8 verification passed `387` tests with `8` platform skips on Windows and `393` tests with `2` skips under WSL CPython 3.10; the full helper file passed `1043` tests with `73` skips on Windows and `1112` tests with `4` skips under WSL. Two independent adversarial re-reviews reported no remaining Critical, Important, or Minor finding. This checkpoint still does not mark Task 8 complete: normal dry-run, policy enablement, observation, and their remaining recovery paths follow below.

Slice 2E checkpoint (2026-07-01): the strict remote-inventory decoder, installed-script receipt-envelope decoder, independent remote retention planner, and durable dry-run receipt/evidence model are implemented and independently reviewed on helper SHA-256 `303b1426b606d5a410cc9960f6bd7c3807fda92efda86c57d238957dc8a330aa` and test SHA-256 `33ff4997c59bfc1ff6da38c028321ffef2d048ee19840ae57e289c2c0d7cdb57`. The parser accepts only canonical bounded names and JSON, exact timestamped root-owned rclone configuration receipts, calendar-valid actionable pairs, and exact protected-name reasons at an aware entry time. Context-free validation recomputes the retention plan from the entry epoch and frozen policy, binds the exact installed target/config provenance, requires the exact three-purpose current-attempt audit sequence, and opens separate purpose-specific `result_sha256` commitments for the before inventory, runtime candidates, and after inventory. Ordinary audit digests independently bind those commitments in active, interrupted, and historical recovery states; pending and indeterminate dry-run groups require a null result. Active `dry_run_recording`, `recovery_required`, and `recovering_guard` states also revalidate the deterministic entry digest before any recovery write or external action. A 1,000-case deterministic parity probe and independent 10,000-case adversarial review matched the installed planner. Final Task 8 verification passed `466` tests with `8` platform skips on Windows and `473` tests with `1` skip under WSL CPython 3.10; the full helper file passed `1149` tests with `73` skips on Windows and `1218` tests with `4` skips under WSL. Two independent final adversarial reviews reported no remaining Critical, Important, or Minor finding in this slice. This checkpoint still does not mark Task 8 complete: normal dry-run execution, policy enablement, observation, and their remaining recovery paths follow below.

Slice 2F checkpoint (2026-07-01): normal `record-dry-run` execution and its fresh-process guard-recovery boundary are implemented and independently reviewed on operations-helper SHA-256 `8f9d81c22584124d55e8d16901545500577e218fc3fc858e93bdc918deb39bef`, installed backup-shell SHA-256 `a37c4d50018082950e1cd1254fb9d3b1c77877439205106aa3a5c20239062568`, operations-test SHA-256 `61ecf2343c701826ccf47568806d48e9018f28515fa6b9dd1d21c11b46322b67`, and shell-test SHA-256 `94df2053af7c8ac494188f00c203b8bc972573c4dddbb7e2a9f8803d32183fe3`. The transaction holds and repeatedly revalidates exact live proofs for all seven installed targets, acquires the durable timer/service/two-lock guard, records exact before/runtime/after audit groups, invokes the installed planner over a bounded inherited-FD pipe, independently recomputes the plan, and requires byte/casefold inventory stability plus exact candidate equality before releasing locks, restoring the exact runtime baseline, closing target proofs, and writing the terminal receipt. The installed runtime receives the same frozen entry timestamp through strict dry-run-only `--now` parsing, and valid runtime logs use the same 8 MiB bound as their strict decoder instead of the generic 4 KiB cap. Every default child uses a fixed environment, bounded output, a 15-minute timeout, and an isolated POSIX process group; TERM-to-KILL cleanup probes the group itself so descendants that inherit pipes, redirect all streams, or run through the `pg_restore` DEVNULL path cannot retain the runtime lock. Regression coverage includes ordinary and process-death proof teardown, pending and completed audit boundaries, planner completion, each lock/timer restoration edge, the timer-restored write, and the terminal receipt write. Final exact-candidate verification passed the complete operations-helper file with `1184` tests and `77` platform skips on Windows and `1257` tests with `4` skips under WSL CPython 3.10, the complete installed-shell integration file with `212` tests, and repeated three-mode POSIX descendant probes. Three independent final adversarial reviews reported no remaining Critical, Important, or Minor finding. This checkpoint still does not mark Task 8 complete: policy enablement, observation, and their remaining recovery paths follow below.

`record-dry-run` transitions `probed -> dry_run_recording`, invokes the exact installed runtime configuration, and binds the complete case-preserving/casefolded inventory, planner keep/protected/delete sets, and candidate hashes to state. It performs no remote deletion. After releasing locks and restoring the timer it persists `dry_run` and transitions to `dry_run_recorded`. Verified-source recovery from `dry_run_recording` performs no remote deletion; it releases only operation-owned guard/lock state, restores the exact prior timer state, clears `active_transaction`, and transitions `recovering_guard -> probed`.

`enable-prune` is a separate fresh transaction after approval and must re-list the remote inventory under the same guard. It refuses any byte/name/casefold inventory change, independently reruns the planner, and proves candidates are current complete-pair inventory members disjoint from keep/protected sets; any change requires a new dry-run and approval. It requires current `REMOTE_PRUNE_ENABLED=0`. Before the first mutation it rederives the exact replacement environment bytes with `REMOTE_PRUNE_ENABLED=1`, requires their hash to equal the independently staged `host_stage.enabled_environment_sha256`, and durably records only that exact digest as `active_transaction.policy_environment_sha256`. Only its atomic environment transaction may rename those precommitted bytes into place. After the rename, parent-directory fsync, exact readback, and a state-file CAS that revalidates the environment plus all immutable installed-target proofs, it durably records `policy.environment_sha256`, the actual applied-receipt epoch, and the exact applied target while the timer is stopped and both locks remain held. That provisional receipt is the commit point because a later Persistent timer restart may immediately run pruning. A process death before the receipt causes verified-source recovery to accept only the exact disabled/enabled live digest, restore the exact host-staged disabled bytes, clear the later approval chain, and return to `installed`; a process death after the receipt accepts only the exact enabled digest, never rolls it back, and forward-completes to `policy_enabled`. Any third live digest fails closed. Explicit recovery is source-helper-routed; ordinary in-process recovery inside installed `enable-prune` remains installed-helper-routed. Both cursor and terminal state-file CAS operations revalidate the exact environment and immutable target proofs, and only the terminal receipt may precede proof closure. Keep the dedicated `policy_enabling -> recovering_policy -> installed|policy_enabled|recovery_required` path and do not reuse probe/guard recovery for an environment mutation.

`observe` transitions `policy_enabled -> observing`, captures complete local/service/journal/remote evidence under the same durable guard, performs no remote deletion, and only after lock release and exact timer restoration persists `observation` and transitions to `observed`. Verified-source recovery from `observing` performs no remote deletion and returns through `recovering_guard -> policy_enabled`. Observation requires the scheduled run epoch to be strictly newer than `max(install_completed_epoch, policy_enabled_epoch, recorded pre-install trigger)` and no later than the `observing` evidence-capture entry. It applies the same lower cutoff to trigger/start, success journal record, dump filename timestamp, dump mtime, and sidecar mtime; correlates all evidence to the same scheduled run; validates the runtime rclone before/after receipts; and applies exact lowercase one-record sidecar grammar, recomputed SHA-256, and `pg_restore --list` independently to both locally retained pairs.

- [x] **Step 4: Run focused/full verification and commit**

Final Task 8 checkpoint (2026-07-01 UTC): normal probe, dry-run, policy enablement/recovery, and scheduled observation are implemented on operations-helper SHA-256 `3b26d60419a7b4bd50e0c874a74f2fe1ff0e24f031711e4b9330d2877830735c`, installed backup-shell SHA-256 `a37c4d50018082950e1cd1254fb9d3b1c77877439205106aa3a5c20239062568`, systemd-service SHA-256 `2f8b49ea42fb4236d6130129717bc6c3ff115f05af02ace5370ecb98796174f6`, operations-test SHA-256 `e9be1966bb24ff42536575d51a8bbcf84fa7db6b009f8a372dac3f03e87122c2`, and shell-test SHA-256 `18a4b94cadd8ef0c826f51bf1264c0dcbc00b41484c1e05b745e694a668118de`. Observation uses strict recursive remote inventory, two independent content passes, and a physical-last dump stat/identity fence; it rejects directory/nesting surprises, same-name replacement, timer/service drift, manual service substitution, low disk reserve, malformed sidecars, unreadable archives, and incomplete rclone audits. The loaded service must report exact `RefuseManualStart=yes`, exact timer `TriggeredBy`, and matching realtime/monotonic trigger evidence. Strict RFC3339Nano parsing accepts rclone's real 1-9 fractional digits under CPython 3.10 while rejecting noncanonical timestamps and invalid numeric offsets without normalizing the raw identity evidence.

Final verification passed `682` Task 8 tests with `14` platform skips on Windows and `694` with `2` skips under WSL CPython 3.10; the complete operations-helper file passed `1349` tests with `83` skips on Windows and `1428` tests with `4` skips under WSL CPython 3.10; and the complete installed-shell/systemd integration file passed all `212` tests. The final repository-wide gate passed `3537` tests with `84` expected platform skips and `46` subtests. A checksum-pinned rclone `1.74.1` disposable local-backend run exercised the exact recursive `lsjson`, `lsjson --stat`, downloaded SHA-256, and sidecar `cat` commands without contacting Green or OneDrive, and its real nanosecond-offset output passed the production decoder under CPython 3.10. Three independent final adversarial reviews reported no remaining Critical or Important finding. No production host, database, timer, service, credential, OneDrive remote, or backup object was mutated during this local checkpoint; production execution remains separately approval-gated.

Commit:

```text
feat: gate Green pruning on probe and fresh state
```

---

### Task 9: Rewrite the production runbook and pin the asset manifest

**Files:**
- Modify: `.gitattributes`
- Modify: `docs/green-postgres-backup-runbook.md`
- Create: `deploy/linux/degen-prod-db-backup-assets.sha256`
- Modify: `tests/test_degen_prod_db_backup_ops.py`
- Modify: `tests/test_degen_prod_db_backup_script.py`
- Read/verify only: `docs/superpowers/plans/2026-06-29-green-backup-retention.md` (banner already owned by Task 0)
- Read/hash only: `deploy/systemd/degen-prod-db-backup.env.example` (content already owned by Task 3)

- [x] **Step 1: Write failing documentation/manifest contract tests**

Tests require the old plan's superseded banner, exact reviewed-SHA push-before-install ordering, definitions of `UTC_STAMP`, `OPERATION_DIR`, `SOURCE_OPS`, and `MANIFEST_SHA256` before transfer/bootstrap, standard-tool archive transfer/bootstrap/extraction/verification before `verify-source`, archive manifest parity, the mandatory approved `--expected-manifest-sha256` binding, separate push and production approvals plus the later prune approval, explicit timer/rclone mutation disclosure, exact `OPERATION_DIR`/`SOURCE_OPS` construction, source-helper routing for verification/staging/snapshot/install and conditional recovery, absence of `recover` from the normal success block, installed-helper use only after reviewed-manifest hash verification, no direct inline environment editor/install/rollback algorithms, and no rclone command before audit snapshot.

- [x] **Step 2: Rewrite the runbook around tested helpers**

The runbook first defines and validates the exact operation-local values before any operation-directory creation, archive transfer, bootstrap, extraction, or helper invocation:

```bash
UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"
SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"
MANIFEST_SHA256="${APPROVED_MANIFEST_SHA256:?set the approved reviewed-manifest SHA-256}"
```

Only after those four values are fixed and the production approval is active may the runbook create the root-only operation directory and transfer `source.tar`. It then uses existing standard tools to verify the approved archive SHA-256 and embedded commit, enumerate the exact member names/types, reject links or extras, extract once without following links into `$OPERATION_DIR/source`, and verify the extracted manifest against `$MANIFEST_SHA256`. Only after every bootstrap check succeeds may the normal source-routed success path invoke:

```bash
/usr/bin/python3 "$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR" --archive "$OPERATION_DIR/source.tar" --expected-commit "$REVIEWED_SHA" --expected-archive-sha256 "$ARCHIVE_SHA256" --expected-manifest-sha256 "$MANIFEST_SHA256"
/usr/bin/python3 "$SOURCE_OPS" prepare-staging --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" snapshot --operation-dir "$OPERATION_DIR"
/usr/bin/python3 "$SOURCE_OPS" install --operation-dir "$OPERATION_DIR"
```

`recover` is not a normal success-path command. The runbook puts it in a separate conditional interruption block and invokes it only when strict source-routed state inspection reports an in-progress or `recovery_required` transaction:

```bash
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

After install succeeds, the runbook compares `/usr/local/sbin/degen-prod-db-backup-ops` to the exact reviewed manifest hash. Only on an exact match may it use these installed-helper commands:

```bash
/usr/local/sbin/degen-prod-db-backup-ops probe-remote --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops record-dry-run --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops enable-prune --operation-dir "$OPERATION_DIR"
/usr/local/sbin/degen-prod-db-backup-ops observe --operation-dir "$OPERATION_DIR"
```

Only recovery from an already interrupted install, probe, dry run, policy enablement, observation, timer restoration, rollback, or recovery transaction with matching durable state uses the conditional source-routed command above; `recover` refuses absent, stable, or mismatched state and never resolves through a possibly mixed installed binary. A newly approved stable-phase manual rollback instead invokes `/usr/bin/python3 "$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"` after its separate mutation preflight and approval. The runbook documents exact expected evidence, the standard-tool bootstrap, separate push/install/prune gates, source transfer, timer/rclone effects, operation-directory recovery, and irreversible local/remote deletion limits. It explains incomplete-phase refusal, automatic/resumed recovery authorized by the original transaction approval, and the verified-source recovery command. The runbook does not duplicate parser, transaction, or rollback logic. `pg_restore --list` proves archive readability only; the canceled full logical restore rehearsal remains an accepted recovery risk, and this work must not claim end-to-end restore proof.

- [x] **Step 3: Generate the fixed asset manifest**

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

Tests recompute every digest and reject any extra/missing entry. Exact
`.gitattributes` rules pin every source asset and the manifest to LF so the
working-tree bytes hashed during review match the Git blob bytes placed in the
production archive even on Windows with `core.autocrlf=true`.

- [x] **Step 4: Run all focused, syntax, systemd, Python3.10, and full checks**

Task 9 checkpoint (2026-07-01 Pacific / 2026-07-02 UTC): the production
runbook was replaced with the three-gate immutable-source workflow on runbook
SHA-256 `d8de6d3f7d086436cbafbefefffd3f13e89434fd98946ecd18e1e363b0e57c0a`,
script-test SHA-256
`987d706e90d52f8d90e592b4da6298d4688922a4c7047b997e33e8a15cf7c296`,
operations-test SHA-256
`42c547f7cde647d0ac826a034ff2796795b4e2fa5c2f5569a87617503bd1e749`,
fixed-manifest SHA-256
`0c3f0a969e0810aa759221794b97f4788016ba8afc3a7c66ddf531acae6a9e47`,
and `.gitattributes` SHA-256
`b618df3e1be65345761ea0ea2dc24e4a23e8ce359a2172aea8be170230f2b48e`.
The five Task 9 contract tests passed under the repository interpreter and
CPython 3.10.20; the complete backup/operations gate passed `1678` tests with
`84` platform skips; and the full repository gate passed `3517` tests with
`84` platform skips and `46` subtests. `compileall app`, Bash syntax, systemd
validation, manifest recomputation, and `git diff --check` passed. Three
independent final reviews reported no remaining Critical or Important issue.
No Git remote, Green host, database, timer, service, credential, OneDrive
remote, or backup object was mutated. A review-tool-generated untracked
`typescript` transcript remains explicitly outside the manifest and intended
commit; Gate 1's clean-tree check will reject it until its separately approved
cleanup.

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

After the production `proceed`, first define `UTC_STAMP`, `OPERATION_DIR`, `SOURCE_OPS`, and `MANIFEST_SHA256` from the approved immutable-publication evidence. Then perform the standard-tool archive transfer/bootstrap/extraction/verification, and only after those checks invoke `/usr/bin/python3 "$SOURCE_OPS"` for `verify-source`, `prepare-staging`, `snapshot`, and `install`. An incomplete or `recovery_required` transaction before installed-helper verification uses the separate conditional source-routed `/usr/bin/python3 "$SOURCE_OPS" recover` path; recovery is never appended to the normal success sequence. Verify exact installed hashes/modes, timer state restoration, inactive backup service, unchanged application/PostgreSQL/web/worker/bot PIDs, operation-state completeness, and an exact SHA-256 match between `/usr/local/sbin/degen-prod-db-backup-ops` and its reviewed manifest entry.

- [ ] **Step 2: Run the disposable probe and production dry-run**

Only after the installed helper hash matches, run `/usr/local/sbin/degen-prod-db-backup-ops probe-remote --operation-dir "$OPERATION_DIR"` and `/usr/local/sbin/degen-prod-db-backup-ops record-dry-run --operation-dir "$OPERATION_DIR"`; bracket rclone metadata, verify probe cleanup, and present exact production candidates. Keep `REMOTE_PRUNE_ENABLED=0`. Any interrupted guard transaction resumes through `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"`.

- [ ] **Step 3: Obtain the second approval and enable pruning**

Even with zero candidates, wait for explicit approval. Reverify the installed helper hash, then run `/usr/local/sbin/degen-prod-db-backup-ops enable-prune --operation-dir "$OPERATION_DIR"` in a fresh invocation and verify the policy epoch, environment hash, timer state, and unchanged application/database PIDs. Recovery still uses `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"`.

- [ ] **Step 4: Observe the next scheduled run**

Do not claim operational success until hash-verified `/usr/local/sbin/degen-prod-db-backup-ops observe --operation-dir "$OPERATION_DIR"` proves a post-policy timer run, current service success, fresh verified local pair, two validated retained pairs, remote final integrity, no remaining planner deletions, disk space, installed hash parity, and unchanged application/database PIDs. An interrupted observation uses `/usr/bin/python3 "$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"` before another observation attempt.
