# Green Backup Enabled-Policy Upgrade Implementation Plan

**Status:** Implemented and locally verified on 2026-07-10. It has not been
pushed to GitHub, merged into GitHub main, deployed, or activated on Green. The
steps below preserve the TDD execution record, but their examples are
synchronized to the final post-Fable safety contract and must not be used to
bypass the current design or runbook.

> **For agentic workers:** This is a completed implementation record. For any
> follow-up change, use superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans with a newly approved plan rather than
> re-executing these steps.

**Goal:** Let a reviewed Green backup-helper upgrade transactionally stage remote pruning disabled when the verified live policy is enabled, with durable authorization evidence, pre-install drift rejection, and exact enabled-policy restoration on failure.

**Architecture:** Extend only the existing prepare-staging, host-stage manifest, snapshot, install, and recovery flow. New stages use strict host-stage manifest schema v2 with a policy-transition receipt; strict v1 stages remain readable only while the captured live environment is present and contains one strict disabled prune assignment, while operation-state schema v1 remains unchanged and binds either manifest through its existing digest. Preparation enforces the operator-approved live environment hash inside the helper, snapshot compares the exact live environment hash to the v2 receipt before install, and existing snapshot-backed recovery restores the exact prior enabled bytes on failure.

**Tech Stack:** Python 3.10-compatible standard library, pytest, PowerShell/Windows test controller, WSL Linux verification, Bash runbook snippets, Git, Claude CLI Fable.

## Global Constraints

- Implementation was isolated in branch `codex/backup-upgrade-policy-transition` at `C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.worktrees\backup-upgrade-policy-transition`; that removed worktree path is historical evidence, not a reusable production path.
- Do not mutate Green, reuse `/opt/degen/backups/config/20260710T042901Z`, push, merge, deploy, restart services, alter the timer, or issue rclone deletions during implementation.
- Keep `operation-state.json` strict schema version 1 unchanged.
- New host-stage manifests are strict schema version 2; existing exact schema version 1 remains readable but cannot authorize an enabled-to-disabled transition.
- The only new `prepare-staging` inputs are required non-secret `--expected-live-environment-sha256` and optional `--allow-live-prune-disable`; no other subcommand accepts either input.
- The flag is valid only when the verified effective live policy equals `REMOTE_PRUNE_ENABLED=1`; it fails when omitted for enabled policy and when supplied for disabled policy.
- Every production CLI preparation requires the approved live environment hash, and an enabled-policy API authorization also fails if that hash is omitted or mismatched.
- Preparation remains read-only against live host configuration.
- Successful installation leaves remote pruning disabled; failure restores exact snapshot bytes and metadata, including the prior enabled environment.
- No application secret, API key, database credential, rclone credential, tracked secret, or application-service change.
- All helper code must run on Green's Python 3.10 runtime.
- Follow TDD: add each behavioral test first, run it and observe the expected failure, then make the smallest production change that passes it.
- Run `C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe -m pytest --tb=short -q` before every commit; no known-failure exception is allowed.
- Stage only the named files for each commit and inspect `git diff --cached --check`, `git diff --cached --stat`, and `git status --short --branch` before committing.

## File Map

- Modify `deploy/linux/degen-prod-db-backup-ops.py`: required live-hash CLI binding, authorization flag and truth table, v2 receipt construction/validation, v1 adoption/snapshot guards, named environment target, and snapshot live-hash check.
- Modify `tests/test_degen_prod_db_backup_ops.py`: red-green coverage for CLI/API hash and authorization truth tables, schema v1/v2 strictness, exact stage resume, v1 strict disabled-policy enforcement, snapshot drift, successful disabled install, and exact enabled recovery.
- Modify `deploy/linux/degen-prod-db-backup-assets.sha256`: replace only the operations-helper SHA-256 after helper code settles.
- Modify `docs/green-postgres-backup-runbook.md`: new immutable-operation, live-hash, authorization, success, recovery, local-retention, and Gate 3 instructions.
- Modify `docs/superpowers/specs/2026-07-10-backup-enabled-policy-upgrade-design.md`: record written-spec approval.
- Create `docs/superpowers/plans/2026-07-10-backup-enabled-policy-upgrade.md`: this implementation plan.

---

### Task 1: Explicit authorization and strict host-stage manifest v2

**Files:**
- Modify: `tests/test_degen_prod_db_backup_ops.py:5546-7240`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py:11569-11601`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py:12231-12399`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py:12826-13361`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py:23165-23288`
- Modify: `deploy/linux/degen-prod-db-backup-assets.sha256:2`
- Create: `docs/superpowers/plans/2026-07-10-backup-enabled-policy-upgrade.md`

**Interfaces:**
- Consumes: existing `OperationsContext`, verified environment parser, `_HostStageProof`, canonical manifest encoding, strict state validation, and `host_staging_fixture`.
- Produces: `prepare_host_staging(context, *, allow_live_prune_disable: bool = False, expected_live_environment_sha256: str | None = None)`, required CLI `--expected-live-environment-sha256`, optional CLI `--allow-live-prune-disable`, strict manifest v2 `policy_transition`, and v1/v2 validation/adoption behavior used by snapshot.

- [ ] **Step 1: Update the expected-manifest test helper for explicit schema versions**

Replace the single v1-only test builder with a builder that defaults to v2 and takes the exact transition receipt:

```python
def expected_host_stage_manifest(
    context: object,
    assets: dict[str, bytes],
    environment_sha256: str,
    enabled_environment_sha256: str,
    dump_name: str,
    dump_sha256: str,
    *,
    schema_version: int = 2,
    policy_transition: dict[str, object] | None = None,
) -> dict[str, object]:
    target_by_source = dict(zip(SOURCE_ASSETS[:7], TARGETS[:7], strict=True))
    manifest: dict[str, object] = {
        "schema_version": schema_version,
        "operation": {
            "archive_sha256": context.expected_archive_sha256,
            "commit": context.expected_commit,
            "manifest_sha256": context.expected_manifest_sha256,
            "operation_dir": str(context.paths.operation_dir),
            "operation_id": context.operation_id,
        },
        "selected_pair": {
            "dump_basename": dump_name,
            "dump_sha256": dump_sha256,
        },
        "reviewed_assets": [
            {
                "mode": 0o755 if source.startswith("deploy/linux/") else 0o644,
                "sha256": hashlib.sha256(assets[source]).hexdigest(),
                "source": source,
                "staged_path": f"reviewed/{source}",
                "target": target_by_source.get(source),
            }
            for source in sorted(SOURCE_ASSETS)
        ],
        "host_environment": {
            "mode": 0o600,
            "sha256": environment_sha256,
            "enabled_sha256": enabled_environment_sha256,
            "staged_path": "host/etc/degen/prod-db-backup.env",
            "target": "/etc/degen/prod-db-backup.env",
        },
    }
    if schema_version == 2:
        assert policy_transition is not None
        manifest["policy_transition"] = policy_transition
    else:
        assert schema_version == 1
        assert policy_transition is None
    return manifest
```

- [ ] **Step 2: Write failing authorization and v2 receipt tests**

Add tests with these exact behaviors:

```python
def test_host_stage_explicitly_authorizes_enabled_policy_transition(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    managed_path = fixture["managed_path"]
    enabled = managed_path.read_bytes().replace(
        b"REMOTE_PRUNE_ENABLED=0\n", b"REMOTE_PRUNE_ENABLED=1\n"
    )
    managed_path.write_bytes(enabled)
    managed_path.chmod(0o600)

    result = module.prepare_host_staging(
        context,
        allow_live_prune_disable=True,
        expected_live_environment_sha256=hashlib.sha256(enabled).hexdigest(),
    )

    manifest = json.loads(
        (context.paths.staged_dir / "host-stage-manifest.json").read_text("ascii")
    )
    assert manifest["schema_version"] == 2
    assert manifest["policy_transition"] == {
        "live_environment_sha256": hashlib.sha256(enabled).hexdigest(),
        "live_remote_prune_enabled": True,
        "explicit_disable_authorized": True,
        "staged_remote_prune_enabled": False,
    }
    assert result["effective_config"]["REMOTE_PRUNE_ENABLED"] == "0"


def test_host_stage_rejects_unnecessary_disable_authorization(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(
        module.OperationStateError,
        match="allow-live-prune-disable requires live remote prune policy to be enabled",
    ):
        module.prepare_host_staging(context, allow_live_prune_disable=True)

    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.staged_dir.exists()
```

Strengthen the existing enabled-without-flag test to match the exact existing error. Add missing-hash and mismatched-hash tests for the authorized enabled path. Update the ordinary disabled-stage test to expect schema v2 with false/false/false transition booleans and the exact raw live-environment hash.

- [ ] **Step 3: Write failing CLI routing tests**

Change the monkeypatched observer to accept the keyword-only value and assert both invocations:

```python
observed: list[tuple[object, bool, str | None]] = []

def capture_prepare(
    context: object,
    *,
    allow_live_prune_disable: bool = False,
    expected_live_environment_sha256: str | None = None,
) -> dict[str, object]:
    observed.append(
        (context, allow_live_prune_disable, expected_live_environment_sha256)
    )
    return {"effective_config": {}, "host_stage": {}}
```

Assert prepare-staging help contains both options, every invocation passes the exact approved hash, an invocation without the authorization flag records false, and an invocation with it records true. Add subprocess parser tests proving prepare-staging rejects a missing hash and snapshot rejects either prepare-only option as unrecognized.

- [ ] **Step 4: Run the focused tests and observe RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest `
  tests/test_degen_prod_db_backup_ops.py::test_host_stage_explicitly_authorizes_enabled_policy_transition `
  tests/test_degen_prod_db_backup_ops.py::test_host_stage_enabled_authorization_requires_expected_live_environment_hash `
  tests/test_degen_prod_db_backup_ops.py::test_host_stage_rejects_operator_approved_live_environment_hash_mismatch `
  tests/test_degen_prod_db_backup_ops.py::test_host_stage_rejects_unnecessary_disable_authorization `
  tests/test_degen_prod_db_backup_ops.py::test_host_stage_refuses_to_reverse_live_enabled_prune_policy `
  tests/test_degen_prod_db_backup_ops.py::test_prepare_staging_cli_has_only_operation_dir_and_reconstructs_sealed_context `
  tests/test_degen_prod_db_backup_ops.py::test_prepare_staging_cli_requires_expected_live_environment_hash -q
```

Expected: failures for the initially missing API/CLI hash plumbing, absent authorization flag, and old schema version; the original no-flag refusal must still pass.

- [ ] **Step 5: Implement the authorization truth table**

Make `_parse_live_managed_environment` retain all parsing and validation but remove only its unconditional enabled-policy rejection. Add:

```python
def _policy_transition_receipt(
    live_environment_raw: bytes,
    live_effective: dict[str, str],
    *,
    allow_live_prune_disable: bool,
) -> dict[str, object]:
    live_enabled = live_effective.get("REMOTE_PRUNE_ENABLED") == "1"
    if live_enabled and not allow_live_prune_disable:
        raise OperationStateError(
            "live remote prune policy is enabled and cannot be silently reversed"
        )
    if not live_enabled and allow_live_prune_disable:
        raise OperationStateError(
            "allow-live-prune-disable requires live remote prune policy to be enabled"
        )
    return {
        "live_environment_sha256": hashlib.sha256(live_environment_raw).hexdigest(),
        "live_remote_prune_enabled": live_enabled,
        "explicit_disable_authorized": allow_live_prune_disable,
        "staged_remote_prune_enabled": False,
    }
```

Call this immediately after verified live parsing, before backup-pair and application-environment work. Change the public signature to:

```python
def prepare_host_staging(
    context: OperationsContext,
    *,
    allow_live_prune_disable: bool = False,
    expected_live_environment_sha256: str | None = None,
) -> dict[str, object]:
```

After the authorization truth table, validate the optional API hash as lowercase
SHA-256. Require it whenever `allow_live_prune_disable` is true, compare it with
`secrets.compare_digest` to SHA-256 of the descriptor-read live bytes, and fail
before persistent staging on omission or mismatch. Pass the exact receipt into
`_prepare_or_resume_stage` and `_host_stage_manifest`.

- [ ] **Step 6: Implement strict v2 construction and v1/v2 validation**

Make `_host_stage_manifest` require `policy_transition` and emit schema version 2. Split common manifest-field validation from version dispatch. The strict transition validator must require exactly these keys and invariants:

```python
def _validate_policy_transition(value: object) -> dict[str, object]:
    transition = _require_object(
        value,
        frozenset(
            {
                "live_environment_sha256",
                "live_remote_prune_enabled",
                "explicit_disable_authorized",
                "staged_remote_prune_enabled",
            }
        ),
        "host-stage manifest policy transition",
    )
    _require_hash(
        transition["live_environment_sha256"],
        "host-stage manifest live environment sha256",
    )
    for field in (
        "live_remote_prune_enabled",
        "explicit_disable_authorized",
        "staged_remote_prune_enabled",
    ):
        if type(transition[field]) is not bool:
            raise OperationStateError(
                "host-stage manifest policy transition is invalid"
            )
    if transition["staged_remote_prune_enabled"] is not False:
        raise OperationStateError("host-stage manifest policy transition is invalid")
    if transition["explicit_disable_authorized"] is not transition["live_remote_prune_enabled"]:
        raise OperationStateError("host-stage manifest policy transition is invalid")
    return transition
```

Refactor `_validate_existing_stage_manifest` to receive the verified `effective_config: dict[str, str]` directly instead of reading it from operation state. Snapshot passes the strict staging-prepared state's effective configuration; crash-resume preparation passes the newly verified effective configuration while state is still `source_verified`. The validator must accept exact root keys for v1 or v2, reject booleans/non-integer versions, validate v2 transition structure, and optionally compare it to the exact expected transition during preparation adoption.

- [ ] **Step 7: Make existing-stage adoption version-aware**

Extend `_host_stage_manifest` with keyword-only `schema_version: int = 2` and `policy_transition: dict[str, object] | None`. Version 2 requires the transition; version 1 requires it to be absent and is used only to reconstruct expected legacy bytes for validation. For a new stage, build and write canonical v2 bytes. For an existing exact stage, safely read and decode its stored manifest, construct the exact expected version from the current operation, assets, pair, environment, and transition context, and compare canonical bytes:

```python
if existing:
    manifest_bytes = _read_stage_file_once(
        context,
        stage_directories,
        "host-stage-manifest.json",
        maximum_size=_MAX_STAGED_MANIFEST_BYTES,
        exact_mode=0o600,
    )
    manifest = _decode_strict_manifest(manifest_bytes)
    if _canonical_host_stage_manifest(manifest) != manifest_bytes:
        raise OperationStateError("host-stage manifest is not canonical")
    schema_version = _require_int(
        manifest.get("schema_version"),
        "host-stage manifest schema version",
    )
    if schema_version == 1 and (
        policy_transition["live_remote_prune_enabled"] is not False
        or policy_transition["explicit_disable_authorized"] is not False
    ):
        raise OperationStateError(
            "host-stage manifest v1 cannot authorize live prune disable"
        )
    if schema_version == 1:
        expected_manifest = _host_stage_manifest(
            context,
            asset_hashes,
            environment_sha256,
            enabled_environment_sha256,
            pair,
            schema_version=1,
            policy_transition=None,
        )
    elif schema_version == 2:
        expected_manifest = _host_stage_manifest(
            context,
            asset_hashes,
            environment_sha256,
            enabled_environment_sha256,
            pair,
            schema_version=2,
            policy_transition=policy_transition,
        )
    else:
        raise OperationStateError("host-stage manifest schema is invalid")
    if manifest != expected_manifest:
        raise OperationStateError("preexisting host-stage manifest is not exact")
    _validate_existing_stage_manifest(
        context,
        effective_config,
        manifest,
        asset_bytes,
        environment_sha256,
        enabled_environment_sha256,
        expected_policy_transition=(
            policy_transition if schema_version == 2 else None
        ),
    )
else:
    manifest = _host_stage_manifest(
        context,
        asset_hashes,
        environment_sha256,
        enabled_environment_sha256,
        pair,
        schema_version=2,
        policy_transition=policy_transition,
    )
    manifest_bytes = _canonical_host_stage_manifest(manifest)
```

Change `_prepare_or_resume_stage` to accept the exact `policy_transition` alongside the existing verified effective configuration. Preserve canonical bytes and file identities on exact resume.

- [ ] **Step 8: Wire the required live-hash binding and authorization flag**

Add only to the prepare-staging parser:

```python
prepare_staging.add_argument(
    "--expected-live-environment-sha256",
    required=True,
    action=_StoreOnce,
    help="bind preparation to the operator-approved live environment SHA-256",
)
prepare_staging.add_argument(
    "--allow-live-prune-disable",
    action="store_true",
    help="explicitly authorize staging a currently enabled live prune policy as disabled",
)
```

Call:

```python
prepare_host_staging(
    context,
    allow_live_prune_disable=args.allow_live_prune_disable,
    expected_live_environment_sha256=args.expected_live_environment_sha256,
)
```

- [ ] **Step 9: Add strict schema mutation and v1 adoption tests**

Parameterized tests must reject each missing/extra/wrong-type transition field, inconsistent booleans, invalid hash, v1 with v2 field, v2 without transition, and unknown version. Add a helper that rewrites a freshly created disabled v2 manifest into exact canonical v1. Use it once with state reset to the original `source_verified` bytes to prove disabled/no-flag crash-resume adoption, and once with the staging-prepared state's `host_stage.manifest_sha256` updated to prove snapshot can open exact v1 evidence. Enabled live policy or a supplied authorization must reject v1 adoption.

- [ ] **Step 10: Run focused manifest and preparation tests GREEN**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest `
  tests/test_degen_prod_db_backup_ops.py -k 'host_stage or prepare_staging or prepare_host_staging' --tb=short -q
```

Expected: all selected tests pass with no failure.

- [ ] **Step 11: Run the full suite and commit Task 1**

Run the exact global full-suite command. After it reports zero failures, stage only:

```powershell
git add -- deploy/linux/degen-prod-db-backup-ops.py deploy/linux/degen-prod-db-backup-assets.sha256 tests/test_degen_prod_db_backup_ops.py docs/superpowers/plans/2026-07-10-backup-enabled-policy-upgrade.md
git diff --cached --check
git commit -m 'feat: authorize safe enabled-policy backup upgrades'
```

Expected: full suite exits 0; commit contains only the helper, its current reviewed-asset hash, its tests, and this plan.

---

### Task 2: Snapshot drift binding and exact enabled-policy recovery

**Files:**
- Modify: `tests/test_degen_prod_db_backup_ops.py:7241-8098`
- Modify: `tests/test_degen_prod_db_backup_ops.py:8176-8350`
- Modify: `tests/test_degen_prod_db_backup_ops.py:10076-10118`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py:13477-13668`

**Interfaces:**
- Consumes: strict v2 `policy_transition`, `_SnapshotTargetProof` for `_MANAGED_ENVIRONMENT_TARGET`, existing stage revalidation, `host_snapshot_fixture`, `task7_transaction_fixture`, and verified recovery.
- Produces: `_validate_snapshot_policy_transition(stage_manifest, target_proofs)` and integration tests proving drift rejection, disabled success, and exact enabled rollback.

- [ ] **Step 1: Extend fixtures for an initially enabled live policy**

Add keyword `live_prune_enabled: bool = False` to `host_snapshot_fixture` and `task7_transaction_fixture`. Before preparation, replace the one exact disabled marker with enabled when requested, preserve mode 0600, and call:

```python
module.prepare_host_staging(
    context,
    allow_live_prune_disable=live_prune_enabled,
    expected_live_environment_sha256=hashlib.sha256(
        live_environment_bytes
    ).hexdigest(),
)
```

Existing tests omit the keyword and retain ordinary disabled behavior.

- [ ] **Step 2: Write a failing post-preparation drift test**

```python
def test_snapshot_rejects_live_environment_drift_from_v2_receipt(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    environment_path = host_root_path(context.host_root, TARGETS[-1])
    environment_path.write_bytes(
        fixture["target_bytes"][TARGETS[-1]] + b"UNMANAGED_SAFE=drifted\n"
    )
    environment_path.chmod(0o600)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(
        module.OperationStateError,
        match="live managed environment no longer matches the authorized staging receipt",
    ):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.snapshot_dir.exists()
```

Expected RED: snapshot currently succeeds or fails for a different reason because it does not compare the v2 live hash.

- [ ] **Step 3: Write failing enabled success and recovery tests**

Add one successful-install test:

```python
def test_authorized_enabled_upgrade_installs_with_pruning_disabled(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(
        module, tmp_path, live_prune_enabled=True
    )

    result = module.install_host_configuration(context)

    installed = host_root_path(context.host_root, TARGETS[-1]).read_bytes()
    assert b"REMOTE_PRUNE_ENABLED=0\n" in installed
    assert b"REMOTE_PRUNE_ENABLED=1\n" not in installed
    assert result["install"]["completed_epoch"] is not None
```

Add one failure-after-environment-replacement test. The event hook must raise only after `details["target"] == TARGETS[-1]`. Assert the exact preinstall enabled bytes and metadata are restored, the timer returns to active/enabled, and state reaches the existing stable rolled-back phase.

- [ ] **Step 4: Run the three new tests and observe RED**

Run the exact node IDs for drift, successful enabled install, and failure-after-environment-replacement. Expected: drift test fails because the new receipt is not enforced; enabled fixture or recovery assertions expose any missing plumbing.

- [ ] **Step 5: Implement the snapshot receipt check**

Add:

```python
def _validate_snapshot_policy_transition(
    stage_manifest: dict[str, object],
    targets: dict[str, _SnapshotTargetProof],
) -> None:
    schema_version = _require_int(
        stage_manifest.get("schema_version"),
        "host-stage manifest schema version",
        minimum=None,
    )
    environment = targets.get(_MANAGED_ENVIRONMENT_TARGET)
    if schema_version == 1:
        try:
            live_enabled = (
                environment is None
                or environment.contents is None
                or _remote_prune_enabled_from_environment_bytes(environment.contents)
            )
        except OperationStateError:
            raise OperationStateError(
                "legacy v1 staging receipt requires live remote prune policy to remain disabled"
            ) from None
        if live_enabled:
            raise OperationStateError(
                "legacy v1 staging receipt requires live remote prune policy to remain disabled"
            )
        return
    if schema_version != 2:
        raise OperationStateError("host-stage manifest schema is invalid")
    transition = _validate_policy_transition(stage_manifest.get("policy_transition"))
    if environment is None or environment.contents is None:
        raise OperationStateError(
            "live managed environment no longer matches the authorized staging receipt"
        )
    if hashlib.sha256(environment.contents).hexdigest() != transition["live_environment_sha256"]:
        raise OperationStateError(
            "live managed environment no longer matches the authorized staging receipt"
        )
```

The helper used above must reject any malformed line mentioning
`REMOTE_PRUNE_ENABLED`, require exactly one strict assignment, accept only the
values `0` or `1`, and return true only for value `1`. For schema v1, the only
acceptable live result is exactly one strict `REMOTE_PRUNE_ENABLED=0`
assignment. Call the receipt check immediately after capturing all target
proofs and from `_revalidate_snapshot_inputs` before any snapshot state
replacement. Keep all existing identity, metadata, rclone, runtime, and stage
checks.

- [ ] **Step 6: Run focused snapshot/install/recovery tests GREEN**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest `
  tests/test_degen_prod_db_backup_ops.py -k 'snapshot or install_failure_restores or authorized_enabled_upgrade' --tb=short -q
```

Expected: all selected tests pass. Confirm the test actually ran the failure after `_TARGET_ORDER[-1]` replacement, not an earlier event.

- [ ] **Step 7: Run the full suite and commit Task 2**

After the full suite exits 0:

```powershell
git add -- deploy/linux/degen-prod-db-backup-ops.py deploy/linux/degen-prod-db-backup-assets.sha256 tests/test_degen_prod_db_backup_ops.py
git diff --cached --check
git commit -m 'fix: bind backup upgrade snapshot to live policy'
```

---

### Task 3: Asset manifest and Green operator runbook

**Files:**
- Modify: `deploy/linux/degen-prod-db-backup-assets.sha256:2`
- Modify: `docs/green-postgres-backup-runbook.md:177-285`
- Modify: `docs/green-postgres-backup-runbook.md:287-449`
- Modify: `docs/green-postgres-backup-runbook.md:502-537`
- Modify: `docs/superpowers/specs/2026-07-10-backup-enabled-policy-upgrade-design.md:1-5`

**Interfaces:**
- Consumes: final helper bytes, v2 receipt semantics, Gate 2 immutable bindings, current conditional recovery, and separate Gate 3.
- Produces: reviewed helper checksum and exact operator sequence for enabled-policy upgrades.

- [ ] **Step 1: Mark the written design approved**

Change the design status to:

```text
Status: Approved by Jeffrey; written-spec review completed 2026-07-10
```

- [ ] **Step 2: Add immutable live-policy approval inputs to Gate 2**

The preflight must record and validate two non-secret values:

```bash
APPROVED_LIVE_ENV_SHA256="${APPROVED_LIVE_ENV_SHA256:?set the approved live environment SHA-256}"
APPROVED_LIVE_REMOTE_PRUNE_ENABLED="${APPROVED_LIVE_REMOTE_PRUNE_ENABLED:?set approved 0 or 1}"
[[ "$APPROVED_LIVE_ENV_SHA256" =~ ^[0-9a-f]{64}$ ]]
case "$APPROVED_LIVE_REMOTE_PRUNE_ENABLED" in 0|1) ;; *) exit 1 ;; esac
```

Document a read-only root preflight that prints only the file SHA-256 and one validated effective prune bit, never file contents. State that changed bytes or a changed bit require a new preflight and approval.

- [ ] **Step 3: Make prepare-staging authorization explicit in the remote script**

Immediately before prepare-staging, rebind the live file hash and construct an array:

```bash
printf '%s  %s\n' "$APPROVED_LIVE_ENV_SHA256" /etc/degen/prod-db-backup.env | sha256sum --check --strict -
PREPARE_STAGING_ARGS=(
  --operation-dir "$OPERATION_DIR"
  --expected-live-environment-sha256 "$APPROVED_LIVE_ENV_SHA256"
)
if test "$APPROVED_LIVE_REMOTE_PRUNE_ENABLED" = 1; then
  PREPARE_STAGING_ARGS+=(--allow-live-prune-disable)
fi
/usr/bin/python3 "$SOURCE_OPS" prepare-staging "${PREPARE_STAGING_ARGS[@]}"
```

State that the helper itself rejects a stale combination, preparation makes no live change, snapshot rechecks the same hash, success leaves pruning disabled, and verified recovery restores exact prior enabled bytes on failure.

- [ ] **Step 4: Document operation and approval boundaries**

Add these exact rules in prose:

- Never reuse `/opt/degen/backups/config/20260710T042901Z`; retain it at `source_verified` as evidence of the safe refusal.
- Every fixed commit requires a new source archive, commit/hash bindings, transfer directory, and operation directory.
- Gate 2 authorization for enabled-to-disabled staging does not authorize Gate 3 or any remote deletion.
- Timer restoration can trigger a catch-up or scheduled backup; local newest-two retention can irreversibly delete older local pairs even while remote pruning is disabled.
- Conditional recovery uses only the exact verified source for the new operation and restores the exact snapshot; no direct environment edits or state reconstruction.

Update the stable checkpoint table so `source_verified` says to use the approved prepare-staging flag only when the bound live bit is 1.

- [ ] **Step 5: Recalculate only the operations-helper manifest line**

Compute:

```powershell
(Get-FileHash -LiteralPath 'deploy/linux/degen-prod-db-backup-ops.py' -Algorithm SHA256).Hash.ToLowerInvariant()
```

Use `apply_patch` to replace only line 2's hash. Then run a strict PowerShell parity loop over every manifest entry and fail on any missing file or mismatched SHA-256.

- [ ] **Step 6: Run focused documentation/manifest checks**

Run `git diff --check`, grep the runbook for the exact flag, old operation path, live hash variable, Gate 3 boundary, recovery language, and irreversible local-retention warning. Run the operations-helper focused tests again after the manifest update.

- [ ] **Step 7: Run the full suite and commit Task 3**

After full suite exit 0, stage only:

```powershell
git add -- deploy/linux/degen-prod-db-backup-assets.sha256 docs/green-postgres-backup-runbook.md docs/superpowers/specs/2026-07-10-backup-enabled-policy-upgrade-design.md
git diff --cached --check
git commit -m 'docs: govern enabled-policy backup upgrades'
```

---

### Task 4: Linux compatibility, Fable review, and final branch verification

**Files:**
- Modify only if verification or review proves a material defect in the files already listed.

**Interfaces:**
- Consumes: final branch diff and all prior test evidence.
- Produces: fresh Windows, Linux/Python 3.10, asset-manifest, and independent read-only review evidence suitable for an integration preflight.

- [ ] **Step 1: Run Python compile and focused Windows verification**

Run compileall for the helper and the exact operations-helper test file, then run the full operations-helper test module. Expected: zero failures.

- [ ] **Step 2: Run WSL/Linux and Python 3.10 compatibility verification**

Use WSL read-only against the worktree bytes. Prove the helper parses under Python 3.10 and run the Linux-specific operations-helper selection needed for descriptor, mode, symlink, fsync, and CLI behavior. Do not access Green for this step.

- [ ] **Step 3: Run exact reviewed-asset manifest parity**

Verify all eight manifest entries against current worktree bytes and confirm only the operations-helper hash changed from the branch parent unless another reviewed asset was intentionally modified.

- [ ] **Step 4: Run the final full repository suite**

Run the global full-suite command fresh. Record exact passed, skipped, warning, subtest, duration, and exit-code evidence.

- [ ] **Step 5: Invoke Claude CLI Fable read-only review**

Give Fable the exact implementation base `d4aa6b2db27b8095a0d1f0ae660121169ab42c6a`, current HEAD, approved design, plan, and diff. Ask it to audit authorization scope, schema strictness/backward compatibility, crash residue, snapshot race closure, recovery exactness, CLI isolation, runbook safety, secrets, and Python 3.10 compatibility. It must not edit files.

- [ ] **Step 6: Resolve material findings test-first**

For each valid defect, add a failing regression test, observe RED, implement the minimal fix, rerun focused and full suites, update the helper manifest if helper bytes changed, and commit only after zero failures. Document non-actionable suggestions with evidence.

- [ ] **Step 7: Review final diff and stop at integration readiness**

Run:

```powershell
git status --short --branch
git log --oneline --decorate d4aa6b2db27b8095a0d1f0ae660121169ab42c6a..HEAD
git diff --check d4aa6b2db27b8095a0d1f0ae660121169ab42c6a..HEAD
git diff --stat d4aa6b2db27b8095a0d1f0ae660121169ab42c6a..HEAD
git diff --name-status d4aa6b2db27b8095a0d1f0ae660121169ab42c6a..HEAD
```

Expected: clean worktree and only the approved helper, test, checksum, runbook, design, and plan files changed. Do not push, merge, deploy, or start a new production operation without the next explicit integration/production approval.
