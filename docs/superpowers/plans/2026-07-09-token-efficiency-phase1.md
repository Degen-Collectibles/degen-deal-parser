# Token Efficiency Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop repeated unchanged parser inference, make retries bounded, preserve Gmail-owned transactions, and keep Green deployments on the approved ECCN model defaults.

**Architecture:** Deploy and application defaults disable the two high-cost periodic inference loops. The Discord worker records a versioned SHA-256 identity plus terminal status after successful parses, skips unchanged automatic work before attachment encoding, and honors source ownership before Discord parsing or transaction synchronization. Retry counters become monotonic between explicit resets, while rollback-safe row failure recording and per-row loop isolation prevent one PostgreSQL failure from stranding a batch.

**Tech Stack:** Python 3.14, FastAPI worker code, SQLModel/SQLAlchemy, SQLite/PostgreSQL additive migrations, Bash deployment script, pytest/unittest.

## Global Constraints

- Work only in `.worktrees/token-efficiency-phase1` on `codex/token-efficiency-phase1`; never edit the dirty main checkout.
- Do not mutate production, credentials, host environment files, services, or database state during implementation.
- Keep `DiscordMessage` raw content and attachments immutable; only parser metadata may be added.
- Keep parsing semantics, prompts, rules, stitching decisions, and TikTok webhook code unchanged.
- Use TDD for every behavior: write a focused test, observe the expected failure, implement the smallest fix, and rerun the focused set.
- Stage only named files. The repo requires a completed canonical full suite before every commit, so implementation changes receive one final commit after the 26-minute full-suite gate rather than intermediate commits that lack that gate.
- Do not store or print the ECCN API key.

## File Map

- `scripts/redeploy-linux.sh`: durable ECCN primary-model and periodic-loop environment defaults, including boolean validation.
- `app/config.py`: safe application defaults for both periodic inference loops.
- `app/models.py`: nullable successful-parse fingerprint and terminal-status metadata on `DiscordMessage`.
- `app/db.py`: matching additive SQLite and PostgreSQL columns.
- `app/discord/worker.py`: fingerprint construction, unchanged-input no-op, external-source no-op, monotonic retry accounting, and rollback-safe row isolation.
- `app/discord/transactions.py`: hard source-ownership guard for non-Discord transactions.
- `tests/test_redeploy_linux_script.py`: deploy/config contract tests.
- `tests/test_recent_stitch_audit.py`: recent-audit retry accounting regression.
- `tests/test_queue_reparse_validation.py`: automatic-reprocess, stale/orphan recovery, and process-loop isolation regressions.
- `tests/test_token_efficiency_phase1.py`: schema, fingerprint, unchanged-input, external-source, transaction-ownership, and rollback behavior.
- `docs/superpowers/specs/2026-07-08-token-efficiency-phase1-design.md`: approved design; update only if implementation evidence changes a stated interface.

---

### Task 1: Lock Safe Deploy and Configuration Defaults

**Files:**
- Modify: `tests/test_redeploy_linux_script.py`
- Modify: `scripts/redeploy-linux.sh`
- Modify: `app/config.py`

**Interfaces:**
- Consumes: existing `set_env_var(env_file, key, value)` Bash helper.
- Produces: `DEGEN_PRIMARY_NVIDIA_MODEL` default `us/azure/openai/eccn-gpt-5.5`; deploy-only boolean overrides `DEGEN_PARSER_REPROCESS_ENABLED` and `DEGEN_PERIODIC_STITCH_AUDIT_ENABLED`; application defaults `False`.

- [ ] **Step 1: Add failing deploy/config tests**

Add assertions that the script contains the approved primary fallback, validates both deploy booleans, and writes both keys to all three environment files. Add this configuration test:

```python
from app.config import Settings


def test_periodic_inference_defaults_are_disabled():
    settings = Settings(_env_file=None)
    assert settings.parser_reprocess_enabled is False
    assert settings.periodic_stitch_audit_enabled is False
```

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -q tests/test_redeploy_linux_script.py
```

Expected: failures show the old `openai/openai/gpt-5.5` fallback, missing loop writes, and `True` application defaults.

- [ ] **Step 3: Implement minimal deploy and config changes**

In `app/config.py`, change only these defaults:

```python
parser_reprocess_enabled: bool = Field(default=False, alias="PARSER_REPROCESS_ENABLED")
periodic_stitch_audit_enabled: bool = Field(default=False, alias="PERIODIC_STITCH_AUDIT_ENABLED")
```

In `scripts/redeploy-linux.sh`, add a strict helper and values:

```bash
require_bool() {
  local key="$1"
  local value="$2"
  if [[ "$value" != "true" && "$value" != "false" ]]; then
    echo "ERROR: $key must be true or false" >&2
    exit 2
  fi
}

PRIMARY_NVIDIA_MODEL="${DEGEN_PRIMARY_NVIDIA_MODEL:-us/azure/openai/eccn-gpt-5.5}"
PARSER_REPROCESS_VALUE="${DEGEN_PARSER_REPROCESS_ENABLED:-false}"
STITCH_AUDIT_VALUE="${DEGEN_PERIODIC_STITCH_AUDIT_ENABLED:-false}"
require_bool DEGEN_PARSER_REPROCESS_ENABLED "$PARSER_REPROCESS_VALUE"
require_bool DEGEN_PERIODIC_STITCH_AUDIT_ENABLED "$STITCH_AUDIT_VALUE"
```

Write `NVIDIA_MODEL`, `PARSER_REPROCESS_ENABLED`, and `PERIODIC_STITCH_AUDIT_ENABLED` to `/opt/degen/web.env`, `/opt/degen/worker.env`, and `/opt/degen/.env` through `set_env_var`.

- [ ] **Step 4: Verify GREEN**

Run the Task 1 test command and `bash -n scripts/redeploy-linux.sh` when Bash is available. Expected: all tests pass and shell syntax is valid.

### Task 2: Make Automatic Retry Accounting Monotonic

**Files:**
- Modify: `tests/test_recent_stitch_audit.py`
- Modify: `tests/test_queue_reparse_validation.py`
- Modify: `app/discord/worker.py`

**Interfaces:**
- Consumes: `parse_attempts`, `settings.parser_max_attempts`, `reset_for_reprocess()`.
- Produces: automatic queue and recovery paths never decrement/reset attempts; manual and edited-input resets remain unchanged.

- [ ] **Step 1: Add failing retry-invariant tests**

Add these behavior checks:

```python
self.assertEqual(watched_row.parse_attempts, 1)
```

after recent stitch-audit requeue; an automatic reprocess test that starts at one and remains one while pending; a stale unfinished attempt that starts at two and remains two after recovery; and an orphaned processing row that starts at two and remains two after recovery.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -q tests/test_recent_stitch_audit.py tests/test_queue_reparse_validation.py
```

Expected: new assertions receive zero or one because current code refunds attempts.

- [ ] **Step 3: Remove automatic attempt refunds**

Delete the decrements in `queue_recent_stitch_audit_candidates()`, `queue_auto_reprocess_candidates()`, stale unfinished-attempt recovery, and orphaned-processing recovery. Delete the compatibility block that lowers stale rows to `parser_max_attempts - 1`. Preserve `reset_attempts=True` for explicit manual reparses and `_requeue_refreshed_message()` because those represent authorization or changed source input.

- [ ] **Step 4: Verify GREEN**

Run the Task 2 test command. Expected: existing and new queue tests pass.

### Task 3: Add Successful-Input Identity Metadata

**Files:**
- Create: `tests/test_token_efficiency_phase1.py`
- Modify: `app/models.py`
- Modify: `app/db.py`
- Modify: `app/discord/worker.py`

**Interfaces:**
- Produces: `DiscordMessage.last_parse_input_fingerprint: Optional[str]`, `DiscordMessage.last_successful_parse_status: Optional[str]`, and `build_parse_input_fingerprint(group_rows, provider=None, model=None) -> str`.
- Fingerprint payload: version `1`, effective provider/model, ordered row IDs, exact content, parsed ordered attachment URLs, author name, and channel name.

- [ ] **Step 1: Add failing schema and fingerprint tests**

Create tests that assert both columns exist in the SQLModel table and both additive migration dictionaries. Add deterministic fingerprint tests:

```python
first = build_parse_input_fingerprint([row], provider="nvidia", model="eccn")
second = build_parse_input_fingerprint([row], provider="nvidia", model="eccn")
assert first == second
row.content = "changed"
assert build_parse_input_fingerprint([row], provider="nvidia", model="eccn") != first
```

Also cover changed attachment order/group membership and changed model.

- [ ] **Step 2: Verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -q tests/test_token_efficiency_phase1.py
```

Expected: import/attribute failures identify the missing fields and helper.

- [ ] **Step 3: Implement additive metadata and canonical fingerprint**

Add nullable text fields to `DiscordMessage` and `TEXT` declarations under `discordmessage` in both migration maps. In `worker.py`, use `hashlib.sha256` over compact, sorted-key UTF-8 JSON:

```python
payload = {
    "version": 1,
    "provider": provider or get_provider(),
    "model": model or get_model(),
    "rows": [
        {
            "id": item.id,
            "content": item.content or "",
            "attachments": json.loads(item.attachment_urls_json or "[]"),
            "author_name": item.author_name or "",
            "channel_name": item.channel_name or "",
        }
        for item in group_rows
    ],
}
return hashlib.sha256(
    json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
).hexdigest()
```

Normalize malformed/non-list attachment JSON to an empty list instead of failing the worker.

- [ ] **Step 4: Verify GREEN**

Run the Task 3 test command. Expected: schema and fingerprint tests pass.

### Task 4: Enforce External-Source Ownership Before AI or Sync

**Files:**
- Modify: `tests/test_token_efficiency_phase1.py`
- Modify: `app/discord/transactions.py`
- Modify: `app/discord/worker.py`

**Interfaces:**
- Produces: `external_source_transaction(session, row) -> Optional[Transaction]`; non-Discord transactions are returned unchanged by `sync_transaction_from_message()`; attempts skipped for external sources finish successfully with zero tokens.

- [ ] **Step 1: Add failing Gmail ownership tests**

Test that a `gmail_sortswift` transaction referenced by `GmailReceipt.transaction_id` survives `sync_transaction_from_message()` even when its synthetic row is ignored. Test `process_row()` with an external transaction and assert `parse_message` and `build_parser_attachment_inputs` are not called, the row returns to the transaction's terminal status, and the unfinished attempt is completed with all token fields zero.

- [ ] **Step 2: Verify RED**

Run the Task 3 test file. Expected: the sync test deletes or attempts to delete the transaction, and the worker invokes the parser.

- [ ] **Step 3: Implement source guards**

Immediately after fetching an existing transaction in `sync_transaction_from_message()`:

```python
if existing is not None and (existing.source_kind or "discord") != "discord":
    return existing
```

In `process_row()`, fetch the latest attempt before group/attachment work. If `external_source_transaction()` returns a non-Discord transaction, restore `transaction.parse_status`, set `row.needs_review` from the transaction, clear `active_reparse_run_id`, finish the attempt as a zero-token success, log `processing_skipped_external_source`, commit, record successful reparse outcome, and return.

- [ ] **Step 4: Verify GREEN**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -q tests/test_token_efficiency_phase1.py tests/test_gmail_financials.py
```

Expected: ownership and existing Gmail tests pass.

### Task 5: Skip Unchanged Automatic Parses and Isolate Row Failures

**Files:**
- Modify: `tests/test_token_efficiency_phase1.py`
- Modify: `tests/test_queue_reparse_validation.py`
- Modify: `app/discord/worker.py`

**Interfaces:**
- Produces: `explicit_reparse_requested(row, active_reparse_run_id) -> bool`; `finish_attempt_as_noop(attempt) -> None`; `record_row_failure(session, row_id, error, active_reparse_run_id) -> None`.
- Automatic unchanged skip requires every effective group row to have the same current fingerprint and a stored terminal status; reasons beginning with `manual ` or `cli ` and active reparse runs bypass the skip.

- [ ] **Step 1: Add failing idempotency tests**

Cover a solo automatic row with matching fingerprint/status, changed content, changed model, and `manual row reparse`. For the matching automatic case, assert attachment materialization and `parse_message()` are not called; the attempt succeeds with zero tokens; `parse_attempts` is not refunded; and `parse_skipped_unchanged` is logged. For an actual successful two-row parse, assert the fingerprint and each row's final terminal status are stored on both rows.

- [ ] **Step 2: Verify idempotency RED**

Run the token-efficiency test file. Expected: current worker always materializes attachments and calls the parser, and no metadata is stored.

- [ ] **Step 3: Implement unchanged-input no-op**

After stitch-group selection but before stale-group mutation and attachment materialization, compute the fingerprint. When all group rows match and the reparse is not explicit, restore the claimed row from `last_successful_parse_status`, finish the current attempt with zero token fields, log `parse_skipped_unchanged`, clear the active run, commit, record success, and return. After a real successful parse and child marking, store the same fingerprint and each row's `canonical_status()` on every effective group row.

- [ ] **Step 4: Add failing rollback/isolation tests**

Use a real SQLite session error such as `session.exec(text("SELECT * FROM table_that_does_not_exist"))` from a patched transaction sync to force an aborted row transaction. Assert `process_row()` rolls back, reloads, and records only that row as failed. Patch `process_row` so the first ID raises and the second returns; assert `process_once()` awaits both IDs and logs the escaped first-row failure.

- [ ] **Step 5: Verify isolation RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -q tests/test_token_efficiency_phase1.py tests/test_queue_reparse_validation.py
```

Expected: failure recording attempts to commit an invalid session or the first escaped row prevents the second call.

- [ ] **Step 6: Implement rollback-safe failure handling**

In the generic `process_row()` exception path, capture the message, call `session.rollback()`, reload the row and latest unfinished attempt, mark them failed, log, and commit in a clean transaction. If failure recording itself fails, roll back again, log without the broken session, and re-raise. Wrap each `await process_row(row_id)` in `process_once()` with an exception handler that logs `row_processing_unhandled` and continues.

- [ ] **Step 7: Verify GREEN**

Run the Task 5 test command plus `tests/test_worker_stitching.py`. Expected: all tests pass.

### Task 6: Whole-Change Verification, Ruflo Review, and Single Commit

**Files:**
- Review every file listed in the File Map.
- Modify the approved design only if actual interfaces differ; keep the reason explicit.

**Interfaces:**
- Produces: one reviewed implementation commit; no push, merge, deployment, or production restart.

- [ ] **Step 1: Run compile verification**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m compileall app
```

Expected: exit 0.

- [ ] **Step 2: Run the focused Phase 1 suite**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q tests/test_redeploy_linux_script.py tests/test_ai_usage_costs.py tests/test_recent_stitch_audit.py tests/test_worker_stitching.py tests/test_worker_service_entrypoint.py tests/test_gmail_financials.py tests/test_attachment_repair_audit.py tests/test_queue_reparse_validation.py tests/test_token_efficiency_phase1.py
```

Expected: all pass.

- [ ] **Step 3: Run the canonical full suite**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest -p no:cacheprovider --tb=short -q
```

Expected baseline: 3,574 passed, 85 skipped, 46 subtests passed; new tests increase the pass count. Allow at least 30 minutes.

- [ ] **Step 4: Run diff and credential checks**

Run `git diff --check`, inspect `git diff --stat` and the complete diff, and search changed files for `nvapi-`, bearer tokens, or API-key assignments. Expected: no whitespace errors, unrelated files, or credential material.

- [ ] **Step 5: Run the Ruflo review signal**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' scripts/ruflo_pilot.py review-diff --apply
```

Expected: review completes; investigate any concrete finding before commit.

- [ ] **Step 6: Stage exact files and commit**

Stage only the named source, test, design-if-needed, and plan files. Confirm `git diff --cached --check` and `git diff --cached --stat`, then commit:

```powershell
git commit -m "fix: bound parser inference retries"
```

- [ ] **Step 7: Stop before production**

Report the commit, tests, token-saving mechanisms, and remaining risks. Present a separate production preflight later; do not push, merge, deploy, edit `/opt/degen`, or restart services without explicit approval.

## Self-Review

- Spec coverage: deploy drift, disabled loops, unchanged-input idempotency, manual bypass, monotonic retries, Gmail ownership, external-source AI skip, rollback-safe row isolation, tests, and production stop boundary all map to tasks.
- Placeholder scan: the plan contains no deferred implementation markers or unspecified error-handling steps.
- Type consistency: model fields, helper signatures, event names, override names, and test paths are consistent across tasks.
- Scope decision: `last_successful_parse_status` is added because current pending/processing transitions erase `needs_review`; without persisted terminal status, the approved no-op cannot restore review-required rows safely.
