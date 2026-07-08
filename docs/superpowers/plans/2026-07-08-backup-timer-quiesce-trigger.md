# Backup Timer Quiesce Trigger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow the Green backup operations helper to quiesce a systemd timer that legitimately clears `LastTriggerUSec` when stopped, without weakening protection against timer races, PID drift, or a different non-empty trigger.

**Architecture:** Preserve the immutable full runtime baseline, including the pre-install trigger used by later observation cutoffs. Change only the quiesced readback comparison: the observed trigger may equal the baseline or be `None`; every other runtime field remains exact, and any different non-empty trigger remains a hard failure. Restoration continues to require the exact prior runtime baseline.

**Tech Stack:** Python 3.10+, pytest, systemd 249, SHA-256 asset manifest, PowerShell/WSL verification.

## Global Constraints

- Do not mutate Green, its timer, its services, OneDrive, credentials, or `/opt/degen/backups/config/20260707T231959Z` during implementation.
- Do not invoke recovery on the existing `recovery_required` operation.
- Do not relax protected PID checks, timer enablement/activity checks, or rejection of a different non-empty trigger.
- Use a failing regression before editing production code.
- Update the reviewed deployment-asset manifest after the helper bytes change.
- Run the focused tests, full backup-related tests, the repository-wide suite, WSL/Python 3.10 checks, syntax checks, and a read-only Claude/Fable review before integration.
- Production installation remains separately approval-gated.

---

### Task 1: Reproduce and fix systemd clearing `LastTriggerUSec`

**Files:**
- Modify: `tests/test_degen_prod_db_backup_ops.py`
- Modify: `deploy/linux/degen-prod-db-backup-ops.py`

**Interfaces:**
- Consumes: `_quiesce_backup_timer(context, prior_runtime, before_action=None)` and the existing runtime schema.
- Produces: the same function signature and state schema, with a narrowly tolerant quiesced trigger comparison.

- [x] **Step 1: Write the failing regression**

Add a test that supplies a non-null baseline trigger, simulates the timer becoming disabled/inactive with `preinstall_trigger_epoch=None`, and expects `_quiesce_backup_timer()` to succeed while preserving the existing action sequence:

```python
def test_task8_quiesce_accepts_systemd_clearing_last_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    baseline = recovery_runtime_baseline()
    baseline["preinstall_trigger_epoch"] = 1_783_419_518
    observed = copy.deepcopy(baseline)
    observed["timer_enabled"] = False
    observed["timer_active"] = False
    observed["preinstall_trigger_epoch"] = None
    actions: list[str] = []

    monkeypatch.setattr(
        module,
        "_task7_systemctl",
        lambda _context, action: actions.append(action),
    )
    monkeypatch.setattr(module, "_require_backup_service_inactive", lambda _context: None)
    monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: observed)

    module._quiesce_backup_timer(object(), baseline)

    assert actions == ["disable", "stop"]
```

- [x] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_prod_db_backup_ops.py::test_task8_quiesce_accepts_systemd_clearing_last_trigger --tb=short -q
```

Expected: one failure with `backup timer did not reach the exact quiesced runtime state`.

- [x] **Step 3: Implement the minimal comparison change**

After building the disabled/inactive expected runtime, normalize only an observed null trigger:

```python
expected = copy.deepcopy(prior_runtime)
expected["timer_enabled"] = False
expected["timer_active"] = False
if observed["preinstall_trigger_epoch"] is None:
    expected["preinstall_trigger_epoch"] = None
if observed != expected:
    raise OperationStateError(
        "backup timer did not reach the exact quiesced runtime state"
    )
```

This accepts the live Green behavior but still rejects a different non-null trigger through the existing whole-record comparison.

- [x] **Step 4: Verify GREEN and adjacent fail-closed behavior**

Run the new test and the existing tests for stubborn timer actions, protected-PID drift, non-empty trigger drift, callbacks, and timer restoration. Expected: all selected tests pass.

---

### Task 2: Document the volatile systemd field and bind reviewed bytes

**Files:**
- Modify: `docs/green-postgres-backup-runbook.md`
- Modify: `deploy/linux/degen-prod-db-backup-assets.sha256`

**Interfaces:**
- Consumes: the reviewed operations-helper bytes and deployment archive contract.
- Produces: an exact helper digest in the asset manifest and an operator-visible explanation of the allowed quiesced state.

- [x] **Step 1: Update the runbook**

Document that systemd 249 may clear `LastTriggerUSec` while a timer is disabled/inactive. State that quiesced validation accepts only the original trigger or no trigger, still requires exact disabled/inactive state and protected PIDs, and still rejects any different non-empty trigger.

- [x] **Step 2: Recompute the operations-helper digest**

Compute SHA-256 for `deploy/linux/degen-prod-db-backup-ops.py` and replace only its existing line in `deploy/linux/degen-prod-db-backup-assets.sha256`.

- [x] **Step 3: Verify manifest and documentation tests**

Run the focused manifest/runbook tests and confirm the asset list remains sorted and exact.

---

### Task 3: Verify and independently review the final patch

**Files:**
- Verify: all modified files

**Interfaces:**
- Consumes: the exact working-tree diff.
- Produces: fresh test, syntax, manifest, and independent read-only review evidence suitable for an integration preflight.

- [x] **Step 1: Run local static and focused verification**

Run `git diff --check`, Python compilation for the helper, focused pytest selections, all backup-related test files, and the manifest verifier.

- [x] **Step 2: Run WSL/Python 3.10 and systemd verification**

Run the operations-helper tests under WSL CPython 3.10 where available, Bash syntax checks, and `systemd-analyze verify` for the backup service, timer, and alert unit.

- [x] **Step 3: Run the full repository suite**

Run the Windows virtual environment's complete `pytest --tb=short -q` suite. Any failure must be investigated and either fixed or shown to be unrelated before integration.

- [x] **Step 4: Obtain a read-only Claude/Fable review**

Give Claude CLI with Fable the exact final diff and live failure evidence. Require a severity-ranked verdict and no workspace edits. Address every material finding and rerun affected verification.

Verification checkpoint (2026-07-08): the regression failed on the original
helper with the exact Green error and passed after the minimal change. The
complete backup gate passed `1734 passed, 85 skipped`; the repository gate
passed `3573 passed, 85 skipped, 46 subtests passed`. CPython 3.10.20 passed
the new regression and adjacent drift checks. Its seven unrelated Windows-only
full-file failures reproduced identically on untouched `origin/main`. Linux
POSIX smoke, Bash syntax, isolated-root `systemd-analyze verify`, manifest
parity, and `git diff --check` passed. Claude CLI 2.1.198 with model `fable`
returned `VERDICT: CLEAR`, with no Critical, High, or Medium issue and one
pre-existing Low availability note about exact restoration if the persistent
timer stamp is unavailable or an immediate catch-up fires.

- [ ] **Step 5: Prepare the integration and production preflights**

Use the finishing-a-development-branch workflow. Before any push, merge, Green operation-directory creation, archive transfer, timer change, or install, report exact targets, reversibility, rollback, hashes, and post-action verification, then obtain the approval required for that boundary.
