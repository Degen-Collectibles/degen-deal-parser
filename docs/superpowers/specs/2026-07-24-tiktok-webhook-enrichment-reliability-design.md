# TikTok Webhook Enrichment Reliability

## Problem

The production Ops web process can become unavailable during a TikTok webhook
burst. Each accepted order webhook currently enqueues durable enrichment work
and immediately starts a new daemon thread to drain that queue. The queue
processor keeps one SQLAlchemy session open while invoking enrichment, and the
enrichment path opens another session while making TikTok HTTP and token-refresh
calls. Slow or failing TikTok calls therefore consume database connections in
proportion to webhook concurrency.

On 2026-07-24 this exhausted the production SQLAlchemy pool and made the site
unresponsive even though systemd still reported the web service as running.

## Current State and Evidence

- Production commit during the incident:
  `4a1d8b2530087cc1a5e46d24a24d027314ddb5a3`.
- `degen-web.service` remained active, while local and public `/health` timed
  out.
- The Python process reached approximately 9 GB resident memory.
- Port 8000 accumulated approximately 750 queued accepts and 704
  `CLOSE_WAIT` sockets.
- All 15 SQLAlchemy connections (`pool_size=5`, `max_overflow=10`) were checked
  out. PostgreSQL showed 15 `idle in transaction` sessions.
- Most held sessions were associated with TikTok order enrichment. One older
  `AppSetting` transaction was also present.
- Logs showed a burst of TikTok order webhooks, repeated HTTP 401 expired-token
  responses, and `QueuePool ... connection timed out` errors.
- Restarting only `degen-web.service` restored public health and cleared the
  socket and database-session buildup.

Relevant current behavior:

- `app/routers/tiktok_orders.py` validates and acknowledges TikTok webhooks,
  upserts the thin order payload, and invokes
  `_start_tiktok_webhook_enrichment`.
- `app/shared.py::_start_tiktok_webhook_enrichment` creates one daemon thread
  per accepted order webhook.
- `app/shared.py::_process_tiktok_webhook_enrichment_queue_once` owns an outer
  database session while `process_due_tiktok_webhook_enrichment_jobs` invokes
  enrichment.
- `app/shared.py::_enrich_tiktok_order_from_api` owns a nested database session
  while performing TikTok order-detail and token-refresh HTTP calls.
- A durable enrichment queue and retry/backoff model already exist and should
  be improved rather than replaced.

## Success Criteria

1. A burst of at least 100 accepted TikTok order webhooks creates no unbounded
   threads and runs at most one enrichment consumer per web process.
2. No SQLAlchemy session or transaction remains checked out while waiting on a
   TikTok order-detail or token-refresh HTTP request.
3. The webhook continues to validate, persist the thin order, durably enqueue
   enrichment, and acknowledge promptly.
4. Normal enrichment begins within five seconds without requiring another
   webhook.
5. Duplicate webhook delivery remains deduplicated by order ID.
6. HTTP 401 and other retryable enrichment failures remain visible and follow
   the existing durable retry/backoff policy.
7. A failed enrichment cannot exhaust the web process database pool or prevent
   `/health` from acquiring a connection.
8. Interrupted `processing` jobs remain recoverable using the existing stale
   processing policy.
9. Existing TikTok reporting, webhook, queue, token-refresh, and dual
   SQLite/PostgreSQL behavior remain covered by tests.

## Scope

### Queue execution

- Remove thread-per-webhook execution.
- Keep `_start_tiktok_webhook_enrichment` as the durable enqueue boundary and
  make its success/failure result explicit to the route.
- Let the existing lifespan-managed enrichment loop be the only consumer in a
  web process.
- Poll or wake the consumer on a bounded interval of no more than five seconds.
- Drain only bounded batches so shutdown and other background work remain
  responsive.

### Database-session lifecycle

- Split queue claim, external enrichment, and job finalization into separate
  phases.
- Claim work and commit in a short database session, then close it.
- Perform TikTok HTTP work without an open SQLAlchemy session.
- Reopen a short session to persist the enriched order and finalize the durable
  job state.
- Read refresh inputs in a short session, perform token refresh outside the
  session while retaining the existing refresh serialization lock, then persist
  refreshed credentials in a new short session.

### Tests and observability

- Add regression coverage for bounded concurrency and thread creation.
- Prove with instrumentation or a one-connection test engine that TikTok HTTP
  callbacks execute after the claim/read session has closed.
- Preserve structured success/failure logs and add a concise queue-consumer
  failure record where needed.
- Keep queue status counts visible on existing status surfaces.

## Non-Scope

- No change to TikTok webhook signature verification, header parsing, HMAC
  inputs, or the proven `app_secret` / `app_key + raw_body` algorithm.
- No TikTok auth schema migration or cleanup of existing auth rows.
- No change to webhook payload interpretation or order-normalization rules.
- No increase to SQLAlchemy pool size as a substitute for fixing connection
  ownership.
- No generic background-job framework or new external queue.
- No Shopify behavior changes.
- No worker `stitch-audit` repair.
- No log rotation, PostgreSQL global timeout, service watchdog, or memory
  monitoring changes in this PR.
- No production deployment, service restart, push, merge, or PR creation
  without a separate explicit approval.

## Constraints

- Preserve the durable queue and its unique order-ID behavior.
- Preserve quick webhook acknowledgement so TikTok does not retry-storm the
  endpoint.
- Preserve SQLite test/local-dev support and PostgreSQL production behavior.
- Do not expose access tokens, refresh tokens, signatures, customer PII, or
  order contents in new logs or tests.
- Keep the change narrow to the existing queue/auth modules plus the route's
  enqueue-result handling; do not refactor unrelated TikTok routes.
- Preserve graceful application shutdown and stale-job recovery.
- Work from an isolated worktree based on current `origin/main`; the primary
  checkout contains unrelated work and must remain untouched.

## Recommended Design

### 1. Webhook path

The route retains its current signature validation and thin-order upsert. The
enrichment start function performs only a durable enqueue and returns whether
that enqueue succeeded. It does not create a thread and does not call TikTok.
The route returns 200 only after the enqueue succeeds; an enqueue failure is
logged and returns 500 so TikTok can retry. This keeps acknowledgement work
bounded, durable, and reproducible.

### 2. Single bounded consumer

The existing application lifespan owns one enrichment consumer task. It checks
for due jobs at least every five seconds, processes a bounded batch, and checks
the stop event between batches. A second trigger while the consumer is active
only leaves durable work in the queue; it cannot create another consumer.

The durable table, not an in-memory list, remains the source of truth. A process
restart can therefore recover queued work.

### 3. Claim / execute / finalize boundary

Queue processing becomes three explicit operations:

1. **Claim:** open a short session, atomically move one due job from `pending`
   to `processing`, commit, copy the identifiers required for execution, and
   close the session.
2. **Execute:** resolve credential inputs through short reads, close those
   sessions, make TikTok HTTP calls, and build the enriched order record without
   any checked-out database connection.
3. **Finalize:** open a short session and mark the job `succeeded`, or record the
   sanitized error and next retry time. Persist the enriched order in a short
   transaction.

Only one consumer runs in the web process, but atomic claims remain necessary so
future multi-process deployment or manual queue processing cannot duplicate
work.

### 4. Token refresh boundary

The existing refresh lock remains the single-flight control. Refresh handling is
split into:

- short database read of the selected auth row and refresh inputs;
- locked HTTP refresh with no open session;
- short database write of the returned token fields;
- fresh credential read before retrying the order request.

This changes resource ownership, not token selection policy, OAuth endpoints, or
credential schema.

### 5. Error behavior

- Thin-order persistence or durable enqueue failure remains visible. An
  enrichment enqueue failure returns 500 rather than acknowledging work that
  was not durably queued.
- External enrichment failures never roll back the already-acknowledged webhook
  or thin order.
- Retryable failures return the job to `pending` with existing exponential
  backoff.
- Terminal failures remain visible as `failed`.
- Errors are sanitized with the existing redaction helpers.

## Alternatives Considered

### Semaphore around existing threads

Rejected. It limits concurrent HTTP calls but preserves thread-per-webhook
allocation and long-held nested transactions. It contains symptoms without
correcting ownership.

### Increase the SQLAlchemy pool

Rejected. A larger pool only postpones exhaustion and increases the number of
long-lived PostgreSQL transactions.

### Move all enrichment to a new service or external queue

Deferred. It can provide stronger process isolation, but the existing durable
queue and lifespan task are sufficient for the current scale. A new deployment
unit would expand operational and rollback risk without being required to fix
the demonstrated incident.

## Implementation Plan

The implementation plan will be written only after this design is approved.
Expected work units are:

1. Add failing tests for burst behavior, bounded consumption, and session
   release across external calls.
2. Refactor durable queue operations into short claim and finalize
   transactions.
3. Remove daemon-thread creation and use the single bounded consumer loop.
4. Split enrichment reads, HTTP work, and persistence.
5. Split token-refresh read, HTTP, and write phases while preserving the lock
   and selection semantics.
6. Run focused TikTok tests, compile checks, and the full repository suite.
7. Inspect the exact diff and commit only intended files.

## Risks and Mitigations

- **Enrichment latency increases:** poll no slower than five seconds and process
  bounded batches continuously while work remains due.
- **Job lost between claim and finalize:** retain `processing` status and
  existing stale-processing requeue recovery.
- **Duplicate processing across consumers:** retain atomic status transition
  and row-count claim check.
- **Token refresh race:** preserve the existing refresh lock and reload
  credentials after persistence.
- **SQLite/PostgreSQL semantic differences:** keep current SQLModel-compatible
  operations and run both existing queue tests and PostgreSQL-oriented
  lifecycle tests where practical.
- **Shutdown during external call:** use existing HTTP timeouts; leave the job
  recoverable through stale-processing requeue.
- **Protected webhook regression:** do not edit signature code and run the
  existing signed-webhook tests unchanged.

## Verification

### Automated

- Focused queue and webhook tests in `tests/test_tiktok_reporting.py`.
- Focused token-refresh tests for the split refresh lifecycle.
- Regression test submitting at least 100 enqueue triggers and asserting no
  thread-per-webhook creation and a single active consumer.
- Regression test demonstrating a health/database checkout succeeds while a
  fake TikTok request is blocked.
- Existing duplicate, backoff, stale-processing, redaction, credential
  fallback, and webhook signature tests.
- `.\.venv\Scripts\python.exe -m compileall app`
- `.\.venv\Scripts\python.exe -m pytest --tb=short -q`

### Pre-deployment, if later approved

- Confirm exact branch, commit SHA, destination, clean tracked diff, and passing
  checks.
- Confirm no signature-related diff.
- Record current production SHA as the rollback anchor.

### Post-deployment, if later approved

- Verify local and public `/health`.
- Verify web PID, memory, task count, port-8000 listener backlog, and
  `CLOSE_WAIT` count.
- Verify PostgreSQL has no long-lived production `idle in transaction`
  enrichment sessions.
- Send or observe a controlled webhook/enrichment cycle and confirm bounded
  queue processing.
- Observe through at least one token-refresh or retry interval and inspect
  sanitized logs for pool timeouts.

## Rollback

The code change requires no schema or data migration.

- Before any deployment, record the current production commit.
- Roll back by redeploying the prior commit through the normal GitHub/Green
  deployment path.
- Existing queue rows remain compatible. Jobs left in `processing` are
  recovered by the existing stale-processing mechanism.
- Do not delete queue rows or modify production data as part of rollback.
- A production rollback or service restart requires its own explicit approval
  and the same health, socket, and database-session verification.

## Open Questions and Decisions

- **Consumer interval:** decided at a maximum of five seconds.
- **Consumer concurrency:** decided at one per web process for this fix.
- **Deployment:** explicitly deferred until separate approval.
- **Operational cleanup:** token-row cleanup, log rotation, transaction
  timeouts, memory monitoring, and the worker restart loop will be separate
  follow-up work.
