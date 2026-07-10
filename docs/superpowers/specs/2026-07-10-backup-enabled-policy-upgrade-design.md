# Green Backup Enabled-Policy Upgrade Design

Date: 2026-07-10
Status: Approved by Jeffrey; written-spec review completed 2026-07-10

## Problem

The Green PostgreSQL backup operations helper intentionally stages every installation with remote pruning disabled. That is safe for a first installation, but the current preparation path also requires the live configuration to already have REMOTE_PRUNE_ENABLED=0. Green now has a successfully observed retention policy with REMOTE_PRUNE_ENABLED=1, so a reviewed helper upgrade cannot enter Gate 2 without first reversing the live policy outside the transaction.

The current helper fails closed with:

    live remote prune policy is enabled and cannot be silently reversed

That refusal is correct for an unapproved transition. The missing capability is an explicit, auditable, race-safe way to authorize a reviewed upgrade to stage a disabled configuration, snapshot the exact previously enabled configuration, install transactionally, and automatically restore that exact enabled configuration if the upgrade fails.

Directly editing /etc/degen/prod-db-backup.env, using an old operation rollback as a policy toggle, or resuming a source-verified operation with different reviewed bytes would bypass the helper's operation binding and recovery contract. Those are not acceptable fixes.

## Confirmed Current State

- Production is Green on Brev host openclaw-9902ae. The application remains under /opt/degen/app.
- The live backup environment has REMOTE_PRUNE_ENABLED=1.
- The scheduled backup service continues to complete successfully, the timer is enabled and active, and the public health check is healthy.
- Existing operation /opt/degen/backups/config/20260710T042901Z is stable at source_verified with no active transaction, no failure receipt, and no staging directory.
- That operation stopped before snapshot, installation, timer mutation, service restart, policy change, or remote deletion.
- The existing operation is cryptographically bound to its original reviewed commit, source archive, and asset manifest. It must remain preserved and must not be resumed with the future fixed source.
- The current prepare_host_staging implementation validates the live environment and rejects an enabled prune flag before creating staging.
- The current staging logic always renders REMOTE_PRUNE_ENABLED=0, computes disabled and future-enabled environment hashes, writes a strict host-stage manifest schema v1, and binds the manifest hash into strict operation-state schema v1.
- Existing installation and recovery logic already snapshots managed targets and restores snapshotted bytes and runtime state when an installation transaction fails.
- Green's system Python is 3.10, so all changed helper code must remain Python 3.10 compatible.
- The implementation branch starts from origin/main commit d2f3c1d85d691a0762cf9a1167ebfd6a2311417d. Production rollout will use a later immutable reviewed implementation commit and a new archive, manifest digest, and operation directory.

## Decision Summary

Add one narrow authorization flag:

    prepare-staging --allow-live-prune-disable

The flag authorizes only this transition:

1. The verified live configuration is currently enabled.
2. Preparation records the exact live environment SHA-256 and the explicit authorization in a strict host-stage manifest v2.
3. Staging renders the reviewed configuration with remote pruning disabled.
4. Snapshot proves the live environment still matches the authorized receipt before accepting the snapshot.
5. Installation uses the existing guarded timer, service, lock, snapshot, and recovery transaction.
6. Failure restores the exact snapshotted enabled environment automatically.
7. Success intentionally leaves remote pruning disabled until the existing dry-run and separate Gate 3 approval flow re-enables it.

The flag is not a general force option. It does not authorize direct configuration edits, remote deletion, Gate 3, changed-source resume, or bypass of any validation.

## Success Criteria

1. An enabled Green policy can enter Gate 2 only when the operator supplies the explicit transition flag.
2. Omitting the flag while the live policy is enabled preserves today's exact fail-closed behavior and creates no staging or state change.
3. Supplying the flag while the live policy is already disabled fails as an unnecessary or stale authorization and creates no staging or state change.
4. Preparation itself performs no live mutation, timer change, service action, lock acquisition, backup run, local deletion, or remote operation.
5. Every new stage records a strict, manifest-bound policy-transition receipt containing the exact live environment SHA-256, the live enabled state, the explicit authorization state, and the required disabled staged state.
6. Snapshot fails before committing a snapshot receipt if the live environment no longer matches the authorized staging receipt.
7. Installation failure after any target replacement restores the exact prior enabled environment bytes, metadata, and timer state through verified recovery.
8. Successful installation leaves REMOTE_PRUNE_ENABLED=0 and requires the existing probe, dry run, and separate Gate 3 approval before pruning can be enabled again.
9. Existing host-stage manifest v1 operations remain strictly readable and resumable under their original semantics; they are never silently upgraded or rewritten.
10. Strict operation-state schema v1 remains unchanged and binds the new receipt through the existing host_stage.manifest_sha256 field.
11. No application code, database credentials, application secret, API key, PostgreSQL data, or application service configuration changes.
12. The preserved 20260710T042901Z operation remains unchanged; production use of the fixed helper starts with a new immutable reviewed archive and new operation directory.

## Scope

- Extend the operations helper CLI and preparation API with the narrow authorization flag.
- Separate live environment parsing from the current unconditional enabled-policy refusal so the caller can apply the explicit truth table.
- Add strict host-stage manifest schema v2 with a policy_transition receipt.
- Keep strict validation support for existing host-stage manifest schema v1.
- Bind snapshot acceptance to the exact live environment SHA-256 recorded during authorized preparation.
- Reuse and strengthen the existing transactional install and recovery paths; do not introduce a parallel installer.
- Add focused regression, schema, race, and failure-recovery tests.
- Update the reviewed asset manifest because the operations helper bytes change.
- Update the Green PostgreSQL backup runbook with the new flag, new-operation requirement, recovery behavior, irreversible local-retention warning, and unchanged Gate 3 boundary.
- Obtain a read-only Claude CLI Fable review after implementation and test verification.

## Non-Scope

- Changing retention counts, backup schedules, remote paths, local paths, database targets, or backup naming.
- Generating or rotating an application secret, API key, database credential, rclone credential, or OAuth token.
- Editing /etc/degen/prod-db-backup.env outside the verified helper transaction.
- Enabling or disabling policy on Green during implementation.
- Reusing or modifying operation /opt/degen/backups/config/20260710T042901Z.
- Changing the application, worker, ops bot, PostgreSQL, or deployment service.
- Starting the backup service manually, altering the systemd timer, or issuing direct rclone deletions.
- Merging, pushing, deploying, or starting a production operation without the later explicit gates required by AGENTS.md and the runbook.
- Claiming end-to-end database restore proof.
- Refactoring unrelated backup-helper code or changing operation-state schema v1.

## Constraints

- The helper and tests must run on Python 3.10.
- All schemas remain strict: exact keys, exact types, lowercase fixed-length hashes, and rejected unknown fields.
- Secrets must never enter argv, operation output, manifests, test diagnostics, or tracked files.
- The source archive, reviewed asset manifest, operation directory, and operation state remain immutable-identity boundaries.
- The timer, service-inactive guard, protected process checks, root-only lock, staged replacement, snapshot validation, and recovery mechanics remain authoritative.
- Production source must come from one exact reviewed Git commit and its exact exported archive; branch labels are not immutable evidence.
- A changed reviewed commit requires a new archive, new digests, and new operation directory.
- Gate 2 never authorizes Gate 3 or remote retention deletion.

## User-Facing Command Contract

The CLI becomes:

    degen-prod-db-backup-ops.py prepare-staging \
      --operation-dir /opt/degen/backups/config/<UTC stamp> \
      [--allow-live-prune-disable]

The flag is a boolean acknowledgment with no value. It is valid only when the verified effective live configuration has REMOTE_PRUNE_ENABLED=1.

The exact decision table is:

| Live policy | Flag | Result |
|---|---:|---|
| disabled | absent | Prepare a normal disabled stage and record no transition authorization |
| disabled | present | Fail before persistent staging because the authorization is unnecessary or stale |
| enabled | absent | Fail with the existing silent-reversal error and make no persistent change |
| enabled | present | Prepare a disabled stage and durably record the authorized enabled-to-disabled transition |

The default remains safe for callers that do not know about the new flag. No existing automation can silently disable a live enabled policy merely because the helper was upgraded.

## Architecture

### 1. Live environment classification

The verified environment parser continues to enforce file identity, stable bytes, strict syntax, safe paths, effective configuration validity, and secret-safe errors. It returns the parsed and effective configuration without unconditionally rejecting REMOTE_PRUNE_ENABLED=1.

prepare_host_staging applies the decision table after parsing. Its public Python signature becomes prepare_host_staging(context, *, allow_live_prune_disable: bool = False). The CLI passes the parsed flag explicitly. Internal callers and existing tests that omit it retain fail-closed behavior.

No other command receives this flag. In particular, snapshot, install, recover, probe-remote, record-dry-run, enable-prune, and observe cannot use it as a bypass.

### 2. Host-stage manifest schema v2

All newly created host stages use schema_version 2. The v2 root has the existing operation, selected_pair, reviewed_assets, and host_environment objects plus one required policy_transition object.

The exact new object is:

    "policy_transition": {
      "live_environment_sha256": "<64 lowercase hex>",
      "live_remote_prune_enabled": true | false,
      "explicit_disable_authorized": true | false,
      "staged_remote_prune_enabled": false
    }

Validation enforces these invariants:

- live_environment_sha256 equals SHA-256 of the exact verified live environment bytes read during preparation.
- staged_remote_prune_enabled is exactly false.
- The staged host environment independently parses to REMOTE_PRUNE_ENABLED=0 and remains bound by host_environment.sha256.
- If live_remote_prune_enabled is true, explicit_disable_authorized must be true.
- If live_remote_prune_enabled is false, explicit_disable_authorized must be false.
- Boolean fields must be actual booleans, not integers or strings.
- The object has exactly the four named keys.

This receipt contains no environment contents and no secret values.

### 3. Backward compatibility

The manifest validator dispatches only on integer schema_version:

- Schema v1 requires today's exact root key set and today's exact validation behavior.
- Schema v2 requires the exact v2 root key set and the policy-transition invariants above.
- Every other version, missing field, extra field, wrong type, or inconsistent combination fails closed.

Existing v1 stages are accepted only as v1 and only under the legacy-safe condition that the verified live policy is disabled and the new authorization flag is absent. A v1 stage can never authorize an enabled-to-disabled transition because it has no transition receipt. V1 stages are not rewritten, augmented, or rehashed. All new stages are v2, including the ordinary live-disabled path, so future receipts have one consistent shape.

The stage path distinguishes creation from adoption before constructing expected manifest bytes. A new stage always builds canonical v2. For an already-existing exact stage, the helper safely reads the stored manifest first, dispatches on its recorded version, and constructs the matching version-specific expected object from the current verified operation, reviewed assets, selected backup pair, staged environment, and authorization context. It never constructs v2 and compares those bytes to a stored v1 manifest. Existing operation-state manifest hashes must still match the stored canonical bytes where a state receipt already exists.

Operation-state schema_version remains 1. Its strict host_stage receipt remains unchanged. The existing manifest_sha256 field binds the complete canonical v2 manifest, including policy_transition, without expanding the operation-state schema.

### 4. Preparation and crash resume

Preparation reads and validates the live managed environment once, captures its SHA-256, derives the live policy boolean, applies the authorization table, renders the staged environment with pruning disabled, and writes the canonical v2 manifest.

Before committing staging_prepared, existing identity and byte revalidation runs again for the reviewed source, backup pair, live managed environment, application environment, and stage. If any input changes, preparation fails closed.

If a crash leaves an exact stage while operation state remains source_verified, retry must supply the same authorization implied by current live state and must reproduce the exact canonical stage. Residue, changed live bytes, changed policy state, or changed authorization fails closed rather than adopting a mixed stage.

### 5. Snapshot authorization check

Before snapshot artifacts or a snapshotted state receipt are accepted, snapshot opens the live /etc/degen/prod-db-backup.env through the existing safe target-capture path and computes its exact SHA-256.

For manifest v2, that target hash must equal policy_transition.live_environment_sha256. A mismatch means the authorized source state drifted after preparation; snapshot stops with a secret-safe error and does not begin installation.

The receipt check is in addition to, not instead of, existing path identity, ownership, mode, content, timer, service, process, rclone-audit, and manifest checks.

For manifest v1, snapshot uses the existing behavior because v1 has no transition receipt and could only have been created by the old helper after proving the live policy disabled.

### 6. Installation and recovery

Install continues to use the existing verified stage, snapshot, timer quiescence, service-inactive proof, backup/deployment lock, protected-process baseline, atomic target replacements, systemd validation, and timer restoration.

The staged v2 environment always has remote pruning disabled. If install succeeds, the durable phase advances under the existing state machine and the live environment remains disabled for the probe and dry-run gates.

If an exception or interruption occurs after mutation begins, recovery validates the exact source, stage, operation state, and snapshot before restoring targets. The environment is restored from the snapshot's exact prior bytes and metadata. Therefore an upgrade that started from an enabled policy returns to that exact enabled configuration on failure; recovery does not synthesize a new environment from defaults or toggle one line in place.

Recovery also restores the timer's exact prior state and preserves the existing protected-process and service guards. It does not delete backup dumps or remote objects.

### 7. Post-install policy path

A successful Gate 2 upgrade deliberately creates a safe disabled interval. The installed helper must then pass the existing disposable remote probe and production-prefix dry run. Gate 2 stops at dry_run_recorded.

Re-enabling remote pruning remains a separate Gate 3 decision after reviewing exact candidates and consequences. The new preparation flag is not evidence of Gate 3 approval and is not consulted by enable-prune.

## State and Data Flow

1. A future reviewed commit is exported into a new source archive and bound to a new operation.
2. verify-source records source_verified under the existing operation-state schema v1.
3. prepare-staging reads the live environment, applies the authorization table, and writes a strict v2 stage manifest.
4. operation state records staging_prepared and binds the v2 manifest through host_stage.manifest_sha256.
5. snapshot reopens the live environment and rejects any hash drift from the v2 receipt.
6. snapshot records exact targets, metadata, rclone audit evidence, runtime state, and protected process evidence.
7. install transactionally replaces reviewed targets, including the disabled environment.
8. On failure, verified recovery restores the exact snapshot, including the originally enabled environment.
9. On success, probe-remote and record-dry-run run with pruning disabled.
10. A later, separately approved Gate 3 may enable pruning and then require a fresh scheduled-run observation.

The existing 20260710T042901Z operation does not enter this flow. It remains preserved at source_verified as evidence of the safe refusal.

## Error Handling

Errors remain fail-closed, secret-safe, and phase-specific.

- Enabled live policy without the flag keeps the exact current error: live remote prune policy is enabled and cannot be silently reversed.
- Flag supplied for a disabled live policy reports: allow-live-prune-disable requires live remote prune policy to be enabled.
- Invalid or inconsistent v2 receipt reports a policy-transition manifest validation error without printing configuration contents.
- Snapshot drift reports that the live managed environment no longer matches the authorized staging receipt without printing either version.
- A preparation error leaves operation state at source_verified and creates no accepted stage receipt.
- A snapshot authorization error leaves operation state at staging_prepared and does not enter installing.
- An install failure uses the existing recovery_required and recovering flow and must not be reported as stable until exact restoration is verified.
- If verified recovery cannot prove exact restoration, evidence is preserved and the operation remains stopped for investigation; operators must not edit state or configuration manually.

No error path invokes rclone deletion, starts the backup service manually, alters the timer outside the existing guarded transaction, or restarts application services.

## Verification and Test Design

Implementation follows test-driven development. Focused tests are written or changed before production-helper behavior.

### CLI and authorization tests

- CLI help documents --allow-live-prune-disable only on prepare-staging.
- The flag is rejected on every other subcommand.
- Existing callers without the flag retain the enabled-policy refusal.
- Enabled plus flag prepares successfully and records the exact receipt.
- Disabled without flag prepares successfully with both live and authorization booleans false.
- Disabled plus flag fails before state or stage acceptance.
- The four authorization-table cases assert state bytes and stage inventory, not only exception text.

### Manifest tests

- Newly prepared stages use canonical strict schema v2.
- v2 manifest and operation-state manifest_sha256 binding are deterministic.
- The live environment hash is computed from exact raw bytes, while the staged environment hash independently binds the disabled rendered bytes.
- Every missing, extra, mistyped, non-boolean, invalid-hash, or logically inconsistent policy_transition value is rejected.
- Existing exact schema v1 fixtures remain accepted and unchanged.
- V1 adoption succeeds only with a verified disabled live policy and no transition flag; enabled live policy or a supplied flag rejects v1 adoption.
- v1 with v2 fields, v2 without v2 fields, and unknown schema versions fail closed.
- Preexisting staged residue or a manifest that does not reproduce exact expected bytes is rejected.

### Race and snapshot tests

- Live environment byte drift during preparation is rejected by existing revalidation.
- Policy or unrelated-byte drift after preparation but before snapshot is rejected by the v2 live hash.
- Path replacement, symlink, inode, metadata, and read-time drift remain rejected.
- A snapshot mismatch does not advance state or start install.
- Retry after an interrupted preparation adopts only an exact authorized stage and rejects changed authorization or live state.

### Install and recovery tests

- Failure before the first target replacement leaves the enabled live environment untouched.
- Failure after another target replacement but before environment replacement restores all changed targets.
- Failure immediately after disabled environment replacement restores the exact prior enabled bytes and metadata.
- Failure during post-install validation restores the exact prior enabled environment and runtime state.
- Interrupted recovery can resume only from an allowed in-progress phase and converges on the exact snapshot.
- Successful install leaves the rendered environment disabled and does not silently restore the enabled policy.
- Timer state, service inactivity, protected PIDs, and no application/PostgreSQL restart assertions remain intact.
- No failure-injection test permits remote deletion or direct backup-service start.

### Repository verification

- Compile the changed Python helper and tests.
- Run all focused operations-helper tests.
- Run the complete repository test suite with no accepted failures before each commit that contains implementation.
- Run the Linux helper tests under WSL and prove Python 3.10 compatibility for changed syntax and behavior.
- Recalculate deploy/linux/degen-prod-db-backup-assets.sha256 and verify exact manifest parity.
- Validate shell snippets, systemd assumptions, and runbook command sequencing.
- Run a read-only Claude CLI Fable audit against the exact final diff and resolve or explicitly disposition material findings.
- Verify Git status and staged paths narrowly before every commit.

## Runbook Changes

The Green runbook will:

1. Explain that a previously enabled policy requires the new explicit flag for a reviewed helper upgrade.
2. Require a fresh immutable commit, archive, manifest digest, transfer directory, and operation directory; the blocked 20260710T042901Z operation is evidence only.
3. Add the flag only after the Gate 2 preflight proves the effective live policy is enabled and identifies the exact live environment hash without printing contents.
4. State that prepare-staging makes no live change and that snapshot must bind the same live hash.
5. State that failed installation restores the exact prior enabled policy automatically through verified recovery.
6. State that successful installation intentionally leaves remote pruning disabled.
7. Preserve the separate probe, dry-run, Gate 3 approval, scheduled-run observation, and recovery boundaries.
8. Repeat that a restored timer can trigger a catch-up or scheduled backup. Even while remote pruning is disabled, the approved local newest-two policy can irreversibly delete older local dump pairs; rollback cannot restore them.
9. Prohibit direct environment edits, manual backup-service starts, timer improvisation, direct rclone deletions, changed-source resume, and reuse of an old operation directory.

## Production Rollout Gates

This design and its implementation do not authorize production mutation.

Before a future Green rollout:

1. The written design and implementation plan are approved.
2. Focused and full tests pass.
3. Fable and human diff review are complete.
4. The implementation is committed, pushed, reviewed, merged only under the requested Git workflow, and CI is green.
5. A new immutable source archive and exact hashes are generated from the reviewed commit.
6. Read-only Green preflight reconfirms host routing, live environment hash and enabled policy, timer/service state, protected services, backup integrity, disk, and health.
7. A new Gate 2 preflight states exact targets, mutations, reversible effects, irreversible local-retention risk, rollback, and verification, then waits for explicit approval.
8. Gate 2 stops at dry_run_recorded.
9. Gate 3 remains a separate explicit approval for future remote deletions.

## Risks and Mitigations

### Authorization flag used casually

Risk: An operator treats the flag as force and uses it without understanding the policy transition.

Mitigation: The flag is scoped to one command, has a strict live-state truth table, is durably recorded, fails when unnecessary, and grants no authority to later commands.

### Live configuration changes after approval

Risk: Preparation was approved for one enabled configuration but snapshot captures another.

Mitigation: The exact live SHA-256 is recorded in the v2 manifest and must match snapshot's safely opened target before install can begin.

### Upgrade fails after disabling pruning

Risk: A failed install accidentally leaves policy disabled or leaves a mixed installation.

Mitigation: The exact enabled environment is part of the verified snapshot. Existing transactional recovery restores all targets and timer state, with explicit failure-injection coverage after environment replacement.

### Old operations become unreadable

Risk: A strict v2-only validator strands v1 operation evidence.

Mitigation: Validation explicitly supports exact v1 and exact v2 schemas; v1 is never rewritten.

### State schema expansion destabilizes recovery

Risk: Adding top-level operation-state fields affects every phase validator and recovery path.

Mitigation: Operation-state schema v1 remains unchanged. Its existing manifest digest cryptographically binds the new receipt.

### Disabled interval surprises operators

Risk: Successful installation leaves remote pruning off until Gate 3.

Mitigation: This is intentional, visible in state and environment verification, documented in Gate 2, and required so probe and dry-run evidence precede deletion authority.

### Local retention remains irreversible

Risk: Timer restoration can lead to a scheduled backup that prunes older local pairs even though remote pruning is disabled.

Mitigation: Repeat this consequence in the production preflight, validate retained pairs, verify disk and timer timing, and never claim rollback can restore deleted dumps.

## Rollback

### Before production

The code change is isolated on a feature branch. It can be abandoned or reverted without touching Green. The preserved operation remains unchanged.

### During a future Gate 2 attempt

- Before mutation, stop on any binding, environment, timer, service, process, backup, disk, manifest, or snapshot mismatch.
- After mutation begins, use only verified-source conditional recovery for the exact new operation.
- Recovery restores exact snapshotted target bytes and metadata, including the previously enabled environment, then restores and verifies the timer's prior state.
- Do not reconstruct state, copy files manually, or use the installed helper when source/installed identity is unclear.
- If recovery cannot prove exact restoration, preserve evidence and stop.

Rollback cannot restore local dump pairs removed by a later scheduled retention run or remote objects deleted after a separately approved Gate 3. Those effects remain explicitly irreversible.

## Open Questions

None. The authorization truth table, schema compatibility strategy, success behavior, failure rollback behavior, operation boundary, test scope, and later approval gates were resolved during design review.
