# Security Hardening Phase 2B Discord Revision Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve every accepted Discord source version and prevent an edited message's stale transaction from remaining reportable while reparsing is pending or failed.

**Architecture:** Add an append-only `DiscordMessageRevision` audit table and bind the mutable current-message projection plus normalized `Transaction` to an exact revision ID. Revision creation and stale-transaction invalidation occur in the same database transaction as accepting an edit. Existing databases are upgraded additively for SQLite and PostgreSQL, and legacy rows are lazily given an initial revision when first touched or synchronized.

**Tech Stack:** Python 3.14, SQLModel, SQLite/PostgreSQL additive migrations, Discord ingestion, pytest

---

### Task 1: Add the append-only revision model and dual-engine schema support

**Files:**
- Modify: `app/models.py:113-178,241-274`
- Modify: `app/db.py:85-120,572-613`
- Create: `app/discord/message_revisions.py`
- Test: `tests/test_discord_message_revisions.py`

- [ ] **Step 1: Write failing revision-model tests**

Create tests proving a new message gets revision 1, an edit appends revision 2 without changing revision 1, identical content/attachment refreshes do not duplicate a revision, and a legacy row with no revision lazily captures its pre-edit content before the new revision. Assert monotonic revision numbers, deterministic hashes, and exact content/attachment snapshots.

- [ ] **Step 2: Run the new tests and verify the model/helper is absent**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_discord_message_revisions.py -q
```

Expected: collection/import or behavior failures before implementation.

- [ ] **Step 3: Implement the revision model and helper**

Add `DiscordMessageRevision` with `message_id`, `revision_number`, `content`, `attachment_urls_json`, `source_edited_at`, `captured_at`, and a deterministic SHA-256 snapshot hash; enforce unique `(message_id, revision_number)`. Add nullable `current_revision_id` to `DiscordMessage` and nullable `source_revision_id` to `Transaction`. The helper only inserts or reads revisions—there is no update/delete API—and hashes the exact stored attachment JSON so legacy evidence is never silently rewritten.

- [ ] **Step 4: Add additive SQLite/PostgreSQL columns and indexes**

Add both nullable revision-ID columns to the existing migration maps and indexes. Rely on normal SQLModel `create_all` for the new table; do not run a startup-wide data rewrite. Verify a legacy SQLite schema gains columns without losing rows and a PostgreSQL migration statement is generated for each column.

- [ ] **Step 5: Verify model/helper and migration tests**

Run the new revision tests plus existing DB/schema tests. Expected: all pass; old rows remain readable with null revision IDs until lazy capture.

### Task 2: Capture revisions and invalidate stale money atomically on edit

**Files:**
- Modify: `app/discord/discord_ingest.py:617-671`
- Modify: `app/discord/transactions.py:128-231`
- Test: `tests/test_discord_ingest.py:105-175`
- Test: `tests/test_audit_production_correctness.py:918-1070`

- [ ] **Step 1: Write failing edit-integrity tests**

Prove an edit preserves original content/attachments in revision 1, appends the edited source as revision 2, updates the projection's `current_revision_id`, and immediately makes the old transaction non-reportable. Attach transaction items plus bookkeeping/bank matches and assert edit acceptance removes items and unmatches references in the same commit.

- [ ] **Step 2: Write the parse-failure regression**

Simulate edit acceptance followed by failed reparse. Assert the prior transaction remains tombstoned/non-reportable and remains bound to the old revision rather than silently representing edited content.

- [ ] **Step 3: Run tests and verify stale money currently survives**

Run focused ingestion/audit tests. Expected: current code overwrites source content, has no revisions, and leaves the old transaction reportable until later sync.

- [ ] **Step 4: Integrate revision capture and atomic invalidation**

For an existing row, lazily persist the pre-edit snapshot before mutation, append the changed snapshot, set `current_revision_id`, reset parse state, call the shared transaction-dependent cleanup helper, and tombstone the exact existing transaction before the single commit. For a new row, flush then create revision 1 before commit. Do not mutate revision rows.

- [ ] **Step 5: Bind successful normalized transactions to the active revision**

Before transaction sync, ensure the current projection has a revision. Set `Transaction.source_revision_id` to that exact revision and revive `is_deleted=False` only after the message is again in an importable parsed/review state. Replace items from the newly parsed source.

- [ ] **Step 6: Verify focused, phase, and full-suite behavior**

Run revision, ingestion, worker/parser, transaction-audit, compileall, and full pytest suites. Check SQLite and PostgreSQL migration paths. Inspect `git diff --check`. Do not run migrations against production, deploy, or repair historical rows without a separate preflight.

### Task 3: Remove the cross-process stale financial-report cache

**Files:**
- Modify: `app/routers/reports.py:250-410`
- Modify: `tests/test_cache.py`

- [ ] **Step 1: Reproduce the deployment-boundary failure**

Confirm the hosted web and worker launch as separate processes. A worker-local `cache_invalidate()` or cache generation cannot clear module-global state in the web process, so an edited or newly reparsed transaction can remain visible or hidden for the cache TTL.

- [ ] **Step 2: Disable only the affected financial route caches**

Make `/reports` and `/finance` build fresh snapshots from the database on every request. Do not add a process-local generation protocol that cannot satisfy the deployed topology, and do not introduce a new Redis/database cache dependency in this security patch.

- [ ] **Step 3: Add route-level regressions**

Assert both routes neither read nor populate the process-local cache while preserving their normal snapshot/query behavior. Keep generic cache behavior unchanged for unrelated callers.

### Task 4: Extend exact provenance and stale-result rejection across stitched groups

**Files:**
- Modify: `app/models.py`
- Modify: `app/db.py`
- Modify: `app/discord/discord_ingest.py`
- Modify: `app/discord/transactions.py`
- Modify: `app/discord/worker.py`
- Modify: `tests/test_discord_message_revisions.py`
- Modify: `tests/test_discord_ingest.py`
- Modify: `tests/test_queue_reparse_validation.py`
- Modify: `tests/test_audit_production_correctness.py`

- [ ] **Step 1: Reproduce stitched-child edit, deletion, and stale-worker failures**

Create a transaction parsed from a primary plus child message. Prove the vulnerable implementation binds only the primary revision, leaves primary money reportable when the child is edited or deleted, and allows an in-flight old group parse or ignored result to overwrite newer state. Include a newer ParseAttempt created during asynchronous attachment preparation.

- [ ] **Step 2: Bind every constituent revision**

Add a normalized current-provenance association from a Transaction to each `(message_id, revision_id)` used by the parse, with deterministic source order, uniqueness, and composite foreign-key integrity. Keep the primary `source_revision_id` for compatibility. Preserve old associations while a transaction is tombstoned; replace the set atomically only after a successful current-source parse. Store the exact combined group text used by the parser as transaction source content.

- [ ] **Step 3: Invalidate through provenance on any constituent change**

When any bound message is edited or deleted, find the dependent transaction through the association, remove reconciliation dependents, and tombstone it in the same commit. Include a bounded stitched-group fallback for legacy transactions that predate association rows. Reporting must fail closed when a bound message is deleted or no longer points at the bound revision.

- [ ] **Step 4: Claim attempts and guard the complete async source snapshot**

Atomically transition a candidate row into processing with a persisted attempt claim so competing worker processes cannot both own it. Have `process_once` pass the exact newly created ParseAttempt ID to `process_row`, validate that claim before any await, and include workflow/claim state in the final conditional lock. Snapshot every group row the worker reads or mutates before awaiting attachment preparation or parsing. After awaits and before any parse outcome, attempt completion, row mutation, transaction mutation, or flush, conditionally lock every snapshot in deterministic ID order. Apply the same check to ignored results and parser/input exceptions; discard stale work without changing the newer pending state or newer attempt. Overlapping workers that begin from different stitched constituents must not both publish.

- [ ] **Step 5: Cover uncached Discord lifecycle events**

Look up an existing tracked message before applying new-message watched-channel, bot, or empty-payload filters. Derive whether source content/attachments actually changed so duplicate cached/raw delivery is idempotent while edit-to-empty and edits after a channel is disabled still append and invalidate. Add raw edit, raw delete, and raw bulk-delete handling for messages outside discord.py's cache; fetch full current content for raw edits and invalidate persisted IDs directly for deletes.

- [ ] **Step 6: Make destructive admin clears compatible with append-only evidence**

The clear-all and clear-channel routes must not crash into revision foreign keys or bypass append-only triggers. Return a deterministic conflict (or another explicitly non-destructive result) when immutable revisions exist, with route tests for JSON and form entry points.

- [ ] **Step 7: Re-run all Phase 2B gates**

Run the expanded revision/ingestion/worker/reporting/schema suite, compileall, both independent reviews, and the full repository suite. No stitched-child security gap may be deferred as a single-message assumption.
