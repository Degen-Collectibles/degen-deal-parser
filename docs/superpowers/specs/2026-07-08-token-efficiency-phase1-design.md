# Token Efficiency Phase 1 Design

## Problem

The production parser is spending most of its model budget on repeated vision-heavy parses rather than new Discord messages. Two periodic queue loops repeatedly requeue unchanged rows, and both loops decrement `parse_attempts` before the worker claims the row. That defeats the retry ceiling and permits the same input to be charged indefinitely.

The current deploy script also rewrites the primary NVIDIA model to the non-ECCN default on every automatic deployment. Separately, a Gmail-owned SortSwift transaction can be fed through the Discord parser and deleted by Discord transaction synchronization. PostgreSQL rejects that delete because `gmail_receipts.transaction_id` still references the transaction. The failed session then strands the remainder of the claimed worker batch in `processing`.

## Confirmed Current State

- Production is Green/Brev `openclaw-9902ae`; the application directory is `/opt/degen/app`.
- The approved production model mapping is:
  - primary: `us/azure/openai/eccn-gpt-5.5`
  - fast: `us/azure/openai/eccn-gpt-5.4-nano`
- `scripts/redeploy-linux.sh` currently defaults `DEGEN_PRIMARY_NVIDIA_MODEL` to `openai/openai/gpt-5.5` and writes that value to `/opt/degen/web.env`, `/opt/degen/worker.env`, and `/opt/degen/.env`.
- Over the inspected 30-day production window, the parser recorded 65,410,239 tokens across 6,871 metered calls:
  - input: 61,235,150 tokens (93.62%)
  - output: 4,175,089 tokens
  - cached input: 621,568 tokens (1.02% of input)
  - attachment-bearing calls: 63,459,914 tokens (97.02% of all tokens)
  - repeat calls after the first lifetime call for a message: 57,990,801 tokens (88.66%)
  - calls associated with `queue.reprocess_queued`: 25,686,737 tokens (39.27%)
  - calls associated with `queue.recent_stitch_audit_requeued`: 30,911,362 tokens (47.26%)
  - combined periodic-loop share: 56,598,099 tokens (86.53%)
- `PARSER_REPROCESS_ENABLED` defaults to `true`, every four hours, and `PERIODIC_STITCH_AUDIT_ENABLED` defaults to `true`, every 45 minutes.
- Automatic stitch audit and automatic reprocess both decrement `parse_attempts`; stale and orphan recovery paths also decrement attempts. A row can therefore be reclaimed indefinitely without reaching `PARSER_MAX_ATTEMPTS`.
- Production contained 144,963 `ParseAttempt` rows for 1,036 messages. Of those, 137,531 non-success rows were stale-processing recoveries affecting 134 messages.
- The Gmail failure is a source-ownership bug:
  1. SortSwift ingestion creates a synthetic `DiscordMessage` and a `Transaction` with `source_kind="gmail_sortswift"`.
  2. `GmailReceipt.transaction_id` references that transaction.
  3. A periodic Discord reparse can classify the synthetic row as non-transactional.
  4. `sync_transaction_from_message()` treats the transaction as Discord-owned and deletes it.
  5. PostgreSQL rejects the delete through `gmail_receipts_transaction_id_fkey`.
  6. The failed transaction prevents the current exception handler from recording a clean failure, and later rows claimed in the batch remain `processing`.
- The isolated baseline branch is `codex/token-efficiency-phase1` in `.worktrees/token-efficiency-phase1`, based on `origin/main` at `b0c81e7`.
- Test collection succeeds with 3,659 tests across 138 files. The first 15-minute diagnostic was too short and emitted no partial output through the app runner. A canonical single-process baseline completed in 26:10 with 3,574 passed, 85 skipped, 46 subtests passed, and three warnings. The seven Phase 1-adjacent files pass: 30 tests in 14.90 seconds.

## Success Criteria

1. Every ordinary deployment preserves the approved ECCN primary model unless an explicit `DEGEN_PRIMARY_NVIDIA_MODEL` override is supplied.
2. Automatic parser reprocess and periodic stitch-audit inference are disabled by default and written as disabled into all three production environment files by the deploy script unless an explicit deploy-time override is supplied.
3. A previously successful, unchanged parser payload is not sent to the model again by an automatic requeue.
4. Explicit operator reparses and materially changed inputs can still invoke the parser.
5. Every automatic worker claim consumes one retry attempt. No automatic queue or recovery path decrements attempts or silently grants another model call.
6. Only an explicit operator reset or a genuinely edited/refreshed source input may reset attempts.
7. Discord transaction synchronization never mutates or deletes a transaction owned by Gmail or another non-Discord source.
8. Synthetic external-source rows are not sent through the Discord AI parser.
9. A database or synchronization error for one row is rolled back, recorded against that row, and cannot prevent later claimed rows from being attempted.
10. Existing parsing, stitching, Gmail ingestion, transaction reconciliation, and deployment-script tests remain green, with new regression tests covering each invariant.
11. No API key or credential value is added to Git, logs, tests, or operator output.

## Scope

- Change the deploy-time primary model default to `us/azure/openai/eccn-gpt-5.5`.
- Add validated deploy-time defaults that write both periodic inference toggles as `false` to web, worker, and legacy environment files.
- Change application configuration defaults for both periodic inference loops to `false`.
- Add unchanged-input fingerprinting plus persisted terminal status for successful Discord parses and an automatic no-op path before attachment materialization and model invocation.
- Make retry accounting monotonic for automatic claims and recovery.
- Add external-source ownership guards in the worker and transaction synchronizer.
- Make row-level worker failure handling rollback-safe and continue processing later rows.
- Add focused tests and update operator-facing configuration documentation if the affected variables are documented in the repo.
- After a separate production preflight and explicit approval, deploy through the normal `origin/main` path and verify live settings, queue health, model identity, and token counters.

## Non-Scope

- Enabling a rules-only parser gate. The observed 72.3% rules/AI agreement sample has no reviewed ground truth and is not sufficient to bypass AI safely.
- Changing image dimensions or compression. That is a high-value Phase 2 optimization but can change recognition quality and needs its own measured evaluation.
- Routing ordinary parser traffic to GLM or another new model. ECCN availability, quality, and chargeback must be measured with a labeled canary first.
- Changing parser prompts, learned-rule behavior, or stitching semantics.
- Reprocessing historical rows merely to populate fingerprints.
- Deleting historical `ParseAttempt` records or other audit data.
- Correcting the existing cost dashboard's fallback price for ECCN aliases without an authoritative ECCN chargeback schedule.
- Storing or rotating the staged ECCN API key in Git or chat.

## Constraints

- `DiscordMessage` remains the immutable raw audit source; this change may add parser metadata but must not rewrite raw message content or attachments.
- Parsing remains rules-first with AI fallback; Phase 1 changes invocation eligibility, not parse interpretation.
- SQLite local development and PostgreSQL production both require additive schema support.
- Production is read-only until an explicit deployment preflight is presented and Jeffrey says `proceed`.
- The existing dirty/diverged main checkout is not an implementation surface. All work remains in the isolated worktree and stages only intended files.
- Production credentials remain host-managed. Tests use placeholders only.

## Design

### 1. Durable ECCN and periodic-loop deploy defaults

`scripts/redeploy-linux.sh` will use `us/azure/openai/eccn-gpt-5.5` as the fallback for `DEGEN_PRIMARY_NVIDIA_MODEL`. It will also derive `PARSER_REPROCESS_ENABLED` and `PERIODIC_STITCH_AUDIT_ENABLED` from deploy-only override variables, default both to `false`, validate that each value is exactly `true` or `false`, and write them with `set_env_var` to:

- `/opt/degen/web.env`
- `/opt/degen/worker.env`
- `/opt/degen/.env` when present

Application defaults in `app/config.py` will also be `false`. The deploy script is the durable production control; the application defaults protect new/local environments and installations that do not use the deploy script.

### 2. Unchanged-input idempotency

Add nullable `last_parse_input_fingerprint` and `last_successful_parse_status` metadata to `DiscordMessage` with additive SQLite and PostgreSQL migrations. The status field is required because queue transitions intentionally clear `needs_review`; a skipped row cannot otherwise distinguish its prior `parsed`, `review_required`, or grouped-child `ignored` state.

The fingerprint is a versioned SHA-256 digest of the actual parser inputs that can change the result:

- fingerprint schema version
- selected provider and model
- ordered grouped message IDs
- exact content of each grouped message
- ordered attachment source URLs for each grouped message
- author and channel values passed to the parser

The fingerprint is computed after stitch-group selection but before attachment bytes are materialized or encoded. After a successful parse it and each row's terminal status are stored on every row in the effective group.

When an automatic requeue reaches `process_row()` and the computed fingerprint matches the stored successful fingerprint, the worker will:

- skip attachment materialization and `parse_message()` entirely;
- preserve the existing parsed fields and restore the appropriate parsed/review status for the claimed row;
- finish the already-created `ParseAttempt` as a successful zero-token no-op;
- emit a structured `parse_skipped_unchanged` event with the fingerprint version, never raw input;
- commit and return without transaction reconstruction.

Explicit operator reparses, active range-reparse runs, and input-edit refreshes bypass this no-op. Model/provider changes alter the fingerprint automatically. Parser-code or prompt experiments that keep the same model must use an explicit reparse, which is intentional.

### 3. Retry accounting invariant

`parse_attempts` means the number of automatic claims since the last authorized reset. The worker increments it once when claiming a row. No automatic path subtracts from it.

Remove decrements from:

- recent stitch-audit requeue;
- automatic reprocess requeue;
- stale unfinished-attempt recovery;
- orphaned-processing recovery;
- the compatibility branch that currently lowers an exhausted stale row to `max_attempts - 1`.

Manual forced reparse and a confirmed source edit may reset attempts because they represent operator authorization or new input. Retry exhaustion remains visible as `failed`; it is not silently recycled.

### 4. External-source transaction ownership

Before invoking the Discord parser, the worker will look for an existing transaction by `source_message_id`. If that transaction's `source_kind` is not `discord`, the worker treats the row as externally owned:

- no Discord AI parse;
- no Discord transaction synchronization;
- restore the synthetic row to the external transaction's parse/review state;
- finish the current attempt as a successful zero-token no-op;
- log `processing_skipped_external_source` with source kind and IDs.

`sync_transaction_from_message()` will independently refuse to update or delete an existing non-Discord transaction. This second guard protects direct/manual call paths and prevents the foreign-key failure even if queue eligibility changes later.

The Gmail ingestion path remains responsible for creating, updating, approving, or explicitly removing Gmail-owned transactions.

### 5. Rollback-safe per-row failure isolation

The generic `process_row()` failure path currently tries to write failure state through a session that may already be in PostgreSQL's aborted-transaction state. It will be changed to:

1. capture the original exception text;
2. call `session.rollback()`;
3. reload the `DiscordMessage` and latest unfinished `ParseAttempt` in a clean transaction;
4. mark only that row/attempt failed and clear its active reparse run;
5. write the structured failure event and commit;
6. report the reparse-run outcome.

`process_once()` will also wrap each `await process_row(row_id)` so an unexpected failure that escapes the row handler is logged and the loop continues to the next claimed ID. This is defense in depth; it does not convert failed rows into success.

## Alternatives Considered

1. **Only turn off the two loops in production:** rejected as incomplete. A future env drift or manual enable would recreate the cost problem, and retry accounting would remain unsafe.
2. **Use timestamps instead of a content fingerprint:** rejected because `last_seen_at` can change without parser input changing, while attachment/group changes can matter independently of a timestamp.
3. **Store fingerprints only in `ParseAttempt`:** rejected because a stitch group can be claimed through different member rows, making lookup and status restoration more fragile. Group rows should carry the last successful effective-input identity.
4. **Null `GmailReceipt.transaction_id` before deleting the transaction:** rejected because the Discord worker does not own Gmail's transaction lifecycle; unlinking would silently destroy the finance record that Gmail ingestion intentionally created.
5. **Catch the Gmail foreign-key exception and continue:** rejected because it preserves the ownership violation and turns a deterministic data bug into recurring log noise.
6. **Make retries unlimited but idempotent:** rejected because parser inputs or failure modes can change, and a bounded visible failure remains an essential operational safeguard.

## Risks and Mitigations

- **False idempotency after parser-code changes:** automatic requeues remain no-ops for identical inputs/model. Explicit operator reparses bypass the guard; the fingerprint schema version can be bumped for an intentional global semantic change.
- **Stitch-group status restoration error:** tests will cover primary, grouped-child, review-required, and changed-group cases. The no-op runs before stale-group mutation or transaction synchronization.
- **More rows reach retry exhaustion:** this is intended. The current system hides persistent failures by refunding attempts. Exhausted rows remain visible and can be explicitly reset after diagnosis.
- **External-source guard masks a malformed transaction:** the guard logs every skip and preserves source ownership. Gmail repair remains in the Gmail workflow instead of the Discord parser.
- **Additive schema deployment fails:** migration tests cover both nullable columns in the SQLite and PostgreSQL migration maps. Production preflight verifies migration behavior and a database backup before deployment.
- **Deploy override typo disables intended behavior:** boolean deploy overrides are validated and fail closed before environment files or services are changed.

## Verification

Implementation is test-driven. Each behavior gets a failing regression test before production code changes.

Focused verification will cover:

- deploy default and explicit override behavior for ECCN and both loop toggles;
- configuration defaults for both loops;
- stable fingerprint, changed text, changed attachment, changed group, and changed model cases;
- unchanged automatic requeue invokes no attachment build and no model call;
- explicit reparse bypasses idempotency;
- successful parse stores the fingerprint on all grouped rows;
- automatic requeue/recovery never decrements attempts;
- manual reset and edited-input reset still work;
- Gmail-owned transaction cannot be changed or deleted by Discord synchronization;
- synthetic Gmail source is skipped before AI invocation;
- a transaction-sync database failure is rolled back, recorded, and does not block the next row;
- existing targeted suites for deploy, AI usage, recent stitch audit, worker/stitching, worker entrypoint, Gmail financials, and attachment repair.

After focused tests, run compile verification and the canonical full suite with at least the observed 26-minute baseline allowance. A parallel file split is not an equivalent substitute: baseline investigation exposed existing cross-file environment/import-order coupling in employee-portal tests. No commit may claim a full-suite pass without a completed canonical result.

Ruflo diff review will run after implementation and before handoff.

## Production Preflight and Rollback

No production mutation is part of design approval or local implementation.

Before deployment, present a separate preflight naming:

- exact reviewed commit and files;
- Green target `openclaw-9902ae` and `/opt/degen/app`;
- the three environment files that the deploy script will update;
- additive database column migration;
- web/worker service restarts performed by the normal deploy;
- current queue counts, unfinished attempts, active model settings, and current loop settings;
- database and environment-file backup paths;
- post-deploy checks for health, model identity, loop settings, queue drainage, Gmail FK errors, and token deltas.

Rollback is to the previous reviewed application commit plus the snapshotted environment files, followed by the normal web/worker restart and verification. The two additive nullable parse-identity columns may remain; old code ignores them. Historical audit rows are not deleted. Re-enabling either periodic inference loop after rollback requires an explicit decision because those loops caused 86.53% of observed tokens.

## Open Questions

None for local Phase 1 implementation. Production deployment remains a separate, explicit approval checkpoint.
