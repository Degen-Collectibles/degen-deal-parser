# TikTok Webhook Enrichment Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent TikTok webhook bursts and token failures from exhausting the
production web process's threads or SQLAlchemy connection pool.

**Architecture:** Keep the durable enrichment table as the source of truth.
Webhook requests validate, persist the thin order, enqueue enrichment, and
acknowledge only after the enqueue succeeds. One lifespan-managed consumer
drains bounded batches, and every database transaction is released before a
TikTok HTTP or token-refresh call.

**Tech Stack:** Python 3.11/3.14, FastAPI, asyncio, SQLModel/SQLAlchemy,
PostgreSQL/SQLite, httpx, pytest, unittest.mock.

## Global Constraints

- Do not modify TikTok webhook signature verification, header parsing, HMAC
  inputs, or the proven `app_secret` / `app_key + raw_body` algorithm.
- Do not change the TikTok auth schema, payload normalization, SQLAlchemy pool
  size, Shopify behavior, deployment configuration, or production state.
- Preserve durable deduplication by order ID, retry backoff, stale-processing
  recovery, SQLite support, and PostgreSQL atomic claims.
- Return HTTP 200 only after the thin order and enrichment enqueue have
  succeeded; return HTTP 500 if the durable enqueue fails.
- Run no more than one enrichment consumer per web process, with a maximum
  normal wake delay of five seconds.
- Keep credentials, signatures, customer PII, and order contents out of new
  logs and tests.
- Preserve unrelated work by modifying only the isolated worktree
  `codex/fix-tiktok-enrichment-concurrency`.
- The repository requires the complete test suite before every commit. Do not
  make per-task commits; make one final implementation commit after focused,
  compile, and complete partitioned-suite verification.
- Do not push, open a PR, merge, deploy, or restart production without a
  separate explicit approval.

---

## File Map

- `app/tiktok_enrichment_queue.py`
  - Owns durable enqueue, atomic claim, retry/backoff, and terminal job-state
    transitions.
  - Must release its transaction before invoking an external enrichment
    callback.
- `app/shared.py`
  - Owns the single lifespan consumer, TikTok credential selection, order-detail
    HTTP execution, and enriched-order persistence.
  - Must not create a thread per webhook or hold a transaction across HTTP.
- `app/tiktok/tiktok_auth_refresh.py`
  - Owns serialized shop/creator token refresh.
  - Must copy refresh inputs, release the caller session, perform HTTP, then
    reopen a short transaction to persist results.
- `app/routers/tiktok_orders.py`
  - Owns the TikTok order webhook response.
  - Must return 500 when durable enrichment enqueue fails.
- `tests/test_tiktok_reporting.py`
  - Covers webhook acknowledgement, queue behavior, consumer concurrency,
    credential fallback, and connection lifecycle.
- `tests/test_tiktok_auth_refresh.py` or the existing auth-refresh test module
  selected during implementation
  - Covers session release during shop and creator refresh HTTP calls.

---

### Task 1: Release the Queue Transaction Before Enrichment

**Files:**
- Modify: `app/tiktok_enrichment_queue.py:137-211`
- Test: `tests/test_tiktok_reporting.py:1191-1389`

**Interfaces:**
- Consumes:
  `process_due_tiktok_webhook_enrichment_jobs(session: Session, *,
  enrich_fn: Callable[[str], None], limit: int = 10) -> int`
- Produces: the same public signature and retry semantics, with the invariant
  `session.in_transaction() is False` while `enrich_fn(order_id)` runs.

- [ ] **Step 1: Add a failing transaction-release regression test**

Add a test beside the existing queue retry tests:

```python
def test_tiktok_enrichment_queue_releases_transaction_before_callback(self) -> None:
    now = utcnow()
    with Session(self.engine) as session:
        enqueue_tiktok_webhook_enrichment(session, "tt-release-1", now=now)
        session.commit()

        callback_transactions: list[bool] = []

        def enrich(_order_id: str) -> None:
            callback_transactions.append(session.in_transaction())
            with Session(self.engine) as probe:
                probe.exec(select(TikTokWebhookEnrichmentJob)).first()

        attempted = process_due_tiktok_webhook_enrichment_jobs(
            session,
            now=now,
            enrich_fn=enrich,
            limit=1,
        )

    self.assertEqual(attempted, 1)
    self.assertEqual(callback_transactions, [False])
```

- [ ] **Step 2: Run the new test and verify the current implementation fails**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k releases_transaction_before_callback -vv
```

Expected: FAIL because the callback observes an active transaction after
`session.get(...)`.

- [ ] **Step 3: Capture scalar claim data before commit**

In `process_due_tiktok_webhook_enrichment_jobs`, capture `job_id` and
`tiktok_order_id` before committing the atomic claim. Remove the pre-callback
`session.get(...)`. The structure must be:

```python
job_id = due_job.id
order_id = due_job.tiktok_order_id
claim_result = session.execute(...)
if getattr(claim_result, "rowcount", 0) != 1:
    session.rollback()
    continue
session.commit()

attempted += 1
try:
    enrich_fn(order_id)
except Exception as exc:
    job = session.get(TikTokWebhookEnrichmentJob, job_id)
    ...
else:
    job = session.get(TikTokWebhookEnrichmentJob, job_id)
    ...
```

The commit must occur immediately before the callback, and no ORM access may
occur between that commit and `enrich_fn`.

- [ ] **Step 4: Run the queue regression group**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k "enrichment_queue" --tb=short -q
```

Expected: all selected tests PASS.

---

### Task 2: Remove Thread-Per-Webhook Execution

**Files:**
- Modify: `app/shared.py:3975-4060`
- Modify: `app/routers/tiktok_orders.py:419-488`
- Test: `tests/test_tiktok_reporting.py:700-810`
- Test: `tests/test_tiktok_reporting.py:1351-1393`

**Interfaces:**
- Produces:
  `_start_tiktok_webhook_enrichment(order_id: str) -> bool`
- Produces:
  `TIKTOK_WEBHOOK_ENRICHMENT_INTERVAL_SECONDS = 5.0`
- Consumes: the existing lifespan task
  `tiktok_webhook_enrichment_queue_loop(stop_event: asyncio.Event) -> None`

- [ ] **Step 1: Add a failing no-thread regression test**

```python
def test_tiktok_webhook_enqueue_does_not_start_a_thread(self) -> None:
    with patch.object(
        shared_module,
        "_enqueue_tiktok_webhook_enrichment_job",
        return_value=True,
    ) as enqueue, patch.object(shared_module.threading, "Thread") as thread:
        result = shared_module._start_tiktok_webhook_enrichment("tt-burst-1")

    self.assertTrue(result)
    enqueue.assert_called_once_with("tt-burst-1")
    thread.assert_not_called()
```

- [ ] **Step 2: Add a failing enqueue-failure webhook test**

Reuse the signed webhook fixture and patch the router lookup location:

```python
with patch.object(
    tiktok_orders_module,
    "_start_tiktok_webhook_enrichment",
    return_value=False,
):
    response = self.client.post(
        "/webhooks/tiktok/orders",
        content=raw_body,
        headers=signed_headers,
    )
self.assertEqual(response.status_code, 500)
```

Also assert the thin order remains persisted and the failure is represented by
the existing sanitized structured log path.

- [ ] **Step 3: Run both new tests and verify they fail**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k "does_not_start_a_thread or enqueue_failure" -vv
```

Expected: the thread assertion fails and the route currently returns 200.

- [ ] **Step 4: Make enrichment start enqueue-only**

Change `_start_tiktok_webhook_enrichment` to:

```python
def _start_tiktok_webhook_enrichment(order_id: str) -> bool:
    if not order_id or _fetch_tiktok_order_details is None:
        return False
    try:
        _enqueue_tiktok_webhook_enrichment_job(order_id)
    except Exception as exc:
        print(structured_log_line(...))
        return False
    return True
```

Delete the `threading.Thread(...).start()` block.

- [ ] **Step 5: Make the route fail closed on enqueue failure**

At the existing post-upsert call site:

```python
if not _start_tiktok_webhook_enrichment(enrich_order_id):
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.webhook.enrichment_enqueue_failed",
            success=False,
            tiktok_order_id=enrich_order_id,
            error="Durable enrichment enqueue failed",
        )
    )
    return Response(status_code=500)
```

Do not alter any code before successful signature/payload validation.

- [ ] **Step 6: Bound the single consumer interval**

Add:

```python
TIKTOK_WEBHOOK_ENRICHMENT_INTERVAL_SECONDS = 5.0
```

Update the loop to process one bounded batch, then wait on `stop_event` for the
constant interval. Keep `asyncio.to_thread` so synchronous DB/API work does not
block the event loop.

- [ ] **Step 7: Run webhook and queue tests**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k "webhook or enrichment_queue" --tb=short -q
```

Expected: all selected tests PASS.

---

### Task 3: Release Database Resources During TikTok Order HTTP

**Files:**
- Modify: `app/shared.py:3754-3972`
- Test: `tests/test_tiktok_reporting.py:1394-1566`

**Interfaces:**
- Consumes:
  `_enrich_tiktok_order_from_api(order_id: str, *, raise_errors: bool = False)
  -> None`
- Produces: the same signature and credential-fallback behavior, with no active
  SQLAlchemy transaction while `_fetch_tiktok_order_details(...)` runs.

- [ ] **Step 1: Add a failing order-fetch transaction test**

Patch `managed_session` to yield a real tracked session, then patch
`_fetch_tiktok_order_details`:

```python
fetch_transaction_states: list[bool] = []

def fake_fetch(*args, **kwargs):
    fetch_transaction_states.append(tracked_session.in_transaction())
    return [valid_order_payload]

with patch.object(shared_module, "managed_session", tracked_managed_session), \
     patch.object(shared_module, "_fetch_tiktok_order_details", fake_fetch):
    shared_module._enrich_tiktok_order_from_api("tt-release-http", raise_errors=True)

self.assertEqual(fetch_transaction_states, [False])
```

Use the existing credential and payload fixtures from the neighboring
enrichment tests. Do not place real token values in the fixture.

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k release_http -vv
```

Expected: FAIL because the session still owns the transaction created by the
credential/order queries.

- [ ] **Step 3: Release before every order-detail HTTP attempt**

In `_enrich_tiktok_order_from_api`:

- Copy the existing order's `shop_id` and `shop_cipher` into scalar strings.
- Call `session.close()` after credential/order reads and before
  `_fetch_details_with_token`.
- After a 401 refresh and fresh credential lookup, call `session.close()` again
  before the retry HTTP request.
- Reuse the SQLAlchemy session only after HTTP completes, to upsert and commit
  the enriched order.
- Do not access detached ORM objects after `session.close()`; use the copied
  scalar values.

- [ ] **Step 4: Release the outer processor's startup transaction**

In `_process_tiktok_webhook_enrichment_queue_once`, commit immediately after
`requeue_interrupted_tiktok_webhook_enrichment_jobs(session)`, even when the
requeue count is zero. This releases the `managed_session` health-check
transaction before the callback processor begins.

- [ ] **Step 5: Run the credential and enrichment tests**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py -k "enrichment_uses or enrichment_falls or release_http or processor_recovers" --tb=short -q
```

Expected: all selected tests PASS.

---

### Task 4: Release Database Resources During Token Refresh HTTP

**Files:**
- Modify: `app/tiktok/tiktok_auth_refresh.py:41-211`
- Test: `tests/test_tiktok_token_refresh.py`

**Interfaces:**
- Consumes:
  `refresh_tiktok_auth_if_needed(session: Session, *, runtime_name: str,
  force: bool = False, shop_id: Optional[str] = None,
  shop_cipher: Optional[str] = None, resolve_base_url: Callable[[], str],
  update_state: Optional[Callable[..., None]] = None) -> Optional[dict]`
- Produces: the same public signature, refresh lock, selection policy, returned
  result shape, and persisted token fields.

- [ ] **Step 1: Confirm the existing auth-refresh test baseline**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_token_refresh.py --tb=short -q
```

Expected: the existing token-refresh module passes before adding the lifecycle
regressions.

- [ ] **Step 2: Add failing shop and creator transaction tests**

For shop refresh:

```python
refresh_transaction_states: list[bool] = []

def fake_refresh(*args, **kwargs):
    refresh_transaction_states.append(session.in_transaction())
    return valid_shop_refresh_payload

result = refresh_tiktok_auth_if_needed(
    session,
    runtime_name="test",
    force=True,
    resolve_base_url=lambda: "https://example.invalid",
)
assert refresh_transaction_states == [False]
assert result is not None
```

Add the equivalent assertion for a creator refresh row. Use fake tokens such as
`"test-access"` and `"test-refresh"`.

- [ ] **Step 3: Run the new tests and verify they fail**

Run the two exact node IDs with `-vv`.

Expected: FAIL because `_refresh_fn` currently runs while the session owns the
auth-row query transaction.

- [ ] **Step 4: Split read, HTTP, and persistence phases**

While holding `_tiktok_auth_refresh_lock`:

1. Query the selected shop row and creator rows.
2. Copy every value required for HTTP and persistence into plain dictionaries.
3. Call `session.close()` before the first `_refresh_fn` invocation.
4. Perform shop refresh HTTP.
5. Reuse the session to call `upsert_tiktok_auth_from_callback`, then commit.
6. Before each creator refresh HTTP call, ensure the preceding persistence
   transaction has committed and the session has no active transaction.
7. Persist each creator result and commit.

Do not keep ORM objects across `session.close()`. Preserve the nonblocking lock,
the `force` calculation, `update_state`, and the returned `result` fields.

- [ ] **Step 5: Run all token-refresh tests**

Run:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_token_refresh.py --tb=short -q
```

Expected: all tests PASS.

---

### Task 5: Full Verification and Single Implementation Commit

**Files:**
- Modify only the files listed in Tasks 1-4
- Include:
  `docs/superpowers/plans/2026-07-24-tiktok-webhook-enrichment-reliability.md`

**Interfaces:**
- Produces: one locally committed, fully verified implementation branch.
- Does not produce: a push, PR, merge, deployment, or production restart.

- [ ] **Step 1: Run the complete TikTok reporting suite**

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py --tb=short -q
```

Expected: all tests PASS.

- [ ] **Step 2: Run the complete auth-refresh suite**

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_token_refresh.py --tb=short -q
```

Expected: all tests PASS.

- [ ] **Step 3: Run the compile check**

```powershell
& '..\..\.venv\Scripts\python.exe' -m compileall app
```

Expected: exit code 0.

- [ ] **Step 4: Run the complete repository suite in bounded partitions**

Use the verified baseline partitioning:

```powershell
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_degen_prod_db_backup_ops.py --tb=short -q
& '..\..\.venv\Scripts\python.exe' -m pytest tests\test_tiktok_reporting.py --tb=short -q
& '..\..\.venv\Scripts\python.exe' -m pytest tests --ignore=tests\test_degen_prod_db_backup_ops.py --ignore=tests\test_tiktok_reporting.py --tb=short -q
```

Expected: every partition exits 0. Record pass, skip, warning, and subtest
counts.

- [ ] **Step 5: Inspect the exact diff**

```powershell
git status --short --branch
git diff --check
git diff --stat
git diff -- app/tiktok_enrichment_queue.py app/shared.py app/tiktok/tiktok_auth_refresh.py app/routers/tiktok_orders.py tests docs/superpowers/plans
```

Verify there is no diff in `app/tiktok/tiktok_ingest.py` signature functions or
any unrelated file.

- [ ] **Step 6: Stage only intended files and inspect the cached diff**

```powershell
git add -- app/tiktok_enrichment_queue.py app/shared.py app/tiktok/tiktok_auth_refresh.py app/routers/tiktok_orders.py tests/test_tiktok_reporting.py tests/test_tiktok_token_refresh.py docs/superpowers/plans/2026-07-24-tiktok-webhook-enrichment-reliability.md
git diff --cached --check
git diff --cached --stat
```

- [ ] **Step 7: Commit once after all verification passes**

```powershell
git commit -m "fix: bound TikTok webhook enrichment"
```

- [ ] **Step 8: Verify the final local branch**

```powershell
git status --short --branch
git log -2 --oneline
```

Expected: clean worktree, two local commits ahead of `origin/main` (design and
implementation). Stop and request separate approval before any external action.
