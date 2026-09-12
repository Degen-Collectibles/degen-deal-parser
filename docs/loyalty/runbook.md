# Shopify POS loyalty — internal first release

Local implementation is authorized by Jeffrey's September 10, 2026 instructions and the approved refreshed PRD. This worktree started at `cbc7896fd054881ce1620b3e5ffa7051040da535`. Deployment, subscriptions/access changes, spending and activation remain separate approval gates. Nothing in this document authorizes them.

## Scope and accounting contract

`/loyalty` is an internal Ops ledger, with lookup by numeric Shopify customer/order ID or its correctly typed GID. Staff attach an **existing** customer in native Shopify POS using email **or** phone. The account key is `(configured shop domain, canonical Customer GID)`; the reader also verifies the configured Shop GID against Shopify. No names, contacts, addresses, contact hashes or customer directory are fetched or retained by loyalty. Losing an email or phone does not affect eligibility. No Shopify mutation, inventory handler change, TikTok webhook change, Smile change, public site or POS extension is included.

Only non-test `sourceName=pos`, USD shop AND presentment currency, and explicitly approved retail locations qualify. Missing retail location is review; transaction/restock locations are not substitutes. Both order creation and processing, plus successful payment timestamps, must be at/after the approved UTC launch cutoff. Missing or contradictory timestamps are review. Offline purchases with uncertain chronology remain review. No prelaunch or imported historical credit.

The basis is **original** line total minus every discount allocation, minus original line tax only when the order includes tax. Original quantity/line ID/discount/tax/classification evidence is immutable after a successful calculation. Current subtotal/current quantity and approximate discounted unit prices are not used. Gift-card issuance is excluded; payment by gift card is allowed. Physical product-variant evidence is required for automated merchandise classification; custom or non-shippable service/tip/fee lines remain review rather than being counted as merchandise. Shipping, tax and non-merchandise charges never earn points.

Money remains exact integer USD cents, with BIGINT monetary/point columns and checked integer storage. Net order entitlement is `(net_cents + 50) // 100`. The ledger posts only `new entitlement − previous posted entitlement`. For $19.99, successive $0.25 merchandise refunds leave 20 then 19 points; a final full refund reverses the remaining 19. No line-by-line or refund-by-refund point rounding.

Refund automation requires complete associated **successful REFUND transactions**, explicit original-line allocation, and exact agreement with reimbursed line/tax/shipping components. Pending/failed/mixed-success reimbursements, amount-only refunds, duties/adjustments, exchanges, edited orders, over-refunds, changed original basis and missing prior refund history enter review without changing posted points. Ordinary Return objects with no exchange items do not block earning or completed, clearly allocated refunds. Return connections are paginated; any exchange line item requires review. Tax-only and shipping-only reimbursements do not reduce entitlement. Both inclusive/exclusive refund subtotal interpretations are accepted only when the associated transaction total disambiguates them exactly. This is conservative: an unusual valid provider shape can remain review instead of being approximated.


Successful SALE/CAPTURE transactions establish payment; terminal FAILURE/ERROR attempts are ignored when the successful payment is complete. Pending or unknown attempts remain review. Payment requires a paid/refunded financial status, zero outstanding balance, and exact transaction/order money conservation. The reader uses `Order.totalCashRoundingAdjustment` and `OrderTransaction.amountRoundingSet`, including signed cents; no arbitrary penny tolerance is used. The calculator accepts nominal or explicitly rounded cash totals only when the aggregate declared adjustment reconciles exactly. This accommodates cash reporting representations without awarding rounding adjustments as merchandise. Refund rounding is similarly reconciled; two plausible tax interpretations go to review. Live nominal/rounded representations remain a shadow-verification gate.

## Attachment evidence and identity holds

Automatic attachment evidence is the **trusted local time of a canonical read** that observes the matching Shopify Customer ID. Observation by the successful-payment-plus-seven-day deadline proves that association already existed by that deadline. The earliest matching observation is retained, including when payment is initially pending. Later missing contact fields cannot invalidate it. Both ordinary `orders/updated` intake and reconciliation can establish this proof.

Neither `updatedAt`, a delivery timestamp, an `orders/create` topic with an old `created_at`, nor unsigned `X-Shopify-Triggered-At` establishes historical attachment time. Delayed reads first seeing association after the deadline remain visible review. Administrators with `admin.loyalty.correct` can verify an actual within-window attachment time against authoritative case evidence using `/loyalty/verify-attachment`. This is an audited attestation, with a case reference, retained canonical fingerprint, current revision and order lease check. The customer is fixed from canonical evidence; no customer picker is provided. The full calculator must approve channel, location, launch, payment, line basis and refund evidence before a proof can be saved. It queues a fresh canonical read and never posts directly, including when no account exists yet. Posting still requires its separate gate.

Every merge state produces a minimized visible review receipt. **Only completed/succeeded merges hold both identities**; failed, pending, in-progress and unknown outcomes do not freeze awards/refunds. No aliases or transfers are created. Observed customer reassignment latches an order identity hold, including before its first award. A later matching event cannot silently clear that hold. Successful-merge and identity hold release remain a manual operational gate requiring a separately reviewed procedure; this release has no release button. Balances retain original attribution.

## Tables, transactions and schema readiness

`app/loyalty/models.py` defines five additive SQLModel tables: account, order entitlement, append-only ledger, durable inbox and reconciliation state. `app/db.py` imports them before `SQLModel.metadata.create_all`. `install_schema` creates only loyalty tables and ledger immutability triggers; repeated installation is idempotent. It does not repair or destructively rebuild a malformed preexisting loyalty schema.

Readiness independently inspects required columns, type affinity and BIGINT capacity, string capacity, nullability, primary/unique/foreign keys, required indexes, normalized complete CHECK expressions and append-only trigger bodies. PostgreSQL additionally verifies index validity/readiness, validated constraints and enabled unconditional row triggers. Receiving, worker startup and every write transaction require readiness. The legacy database startup fallback cannot make loyalty ready. An incomplete schema blocks loyalty while leaving the existing app startup path intact. Deploying even with flags off adds these tables and permission defaults, so deployment still needs approval.

Uniqueness covers shop/customer identity, shop/order, shop/delivery, shop/business-posting key and entitlement/revision. Account balances are ledger sums, not a second mutable balance cache. SQLite uses `BEGIN IMMEDIATE`; PostgreSQL uses conflict-safe inserts and row locks. Transaction retries rerun the complete operation on SQLite contention or PostgreSQL serialization/deadlock/lock errors. Per-order and per-inbox leases carry the same random fence token; completion also checks the expected entitlement revision. An expired/superseded worker cannot complete a replacement's work. Ledger entry, order revision and inbox completion commit atomically.

The program's shop-wide policy hash pins shop/location/cutoff/rule configuration as soon as evaluation or reconciliation begins. Changing those settings requires a reviewed migration; changing an environment string cannot enable a new historical cutoff silently.

## Receiving, reading and recovery

Dedicated endpoint: `POST /webhooks/shopify/loyalty`. It uses the existing raw-body SHA-256 HMAC verifier, exact configured shop allowlist, topic allowlist, delivery/event-ID bounds, bounded streaming body and JSON shape validation. It returns 200 only after the minimized receipt commits. A failed commit returns 503; a reused delivery ID with different body/topic returns 409. HMAC verifies the raw body; routing headers never select a new API destination or token.

Allowed topics are `orders/create`, `orders/paid`, `orders/updated`, `orders/cancelled`, `refunds/create`, `customers/merge`. Actual subscription inventory and access are **unverified**. Provisioning subscriptions is not implemented or authorized here. A refund receipt requires its specific Refund GID to be present in the fetched canonical history before evaluation succeeds. The original inventory receiver is independent.

The reader reuses `shopify_graphql_request` and `resolve_shopify_access_token`, pins the shared `2026-04` version, requests no contacts and performs no mutations. It validates shop identity/currency, exhausts child connections, verifies exact transaction counts, rejects partial GraphQL errors, duplicate nodes, absent/repeated cursors, and inconsistent refund transaction history. Order headers are reread after child pagination; a changed header or stale/equal-time conflicting canonical read enters review/retry. Requests have timeouts, bounded retries, throttle handling, connection-page limits and a per-cycle request budget.

The worker evaluates up to ten receipts per cycle, with a five-minute lease and a four-minute read deadline. Failed reads have bounded exponential retries; after eight attempts the receipt stays visibly in review. Known-order recovery revisits at most 25 due rows per cycle, including pending, shadow and previously awarded orders, and coalesces existing queued work. It never imports historical balances.

Reconciliation restricts discovery to `source_name:pos` and independently rechecks canonical `sourceName`. It uses `updated_at` and `UPDATED_AT` order sorting in bounded UTC day windows, with a ten-minute overlap and a short trailing delay. Each page's work and checkpoint commit together; durable cursor history rejects cycles across separate loop iterations and caps each unresolved window at 10,000 continuation pages. Interrupted scan leases resume the stored window/cursor. Scan errors retain their boundary and appear in Ops. Old-order access is not assumed: a scan beyond the default 60-day order-access horizon or an older retained award requires explicitly verified `read_all_orders` access. Known-order recovery continues independently of scan failures.

These intervals are implementation safety/work bounds, not approved service-level guarantees. Capacity and API-cost behavior must be measured before activation. No polling implementation can promise perfect reconciliation against a provider that withholds required history.

## Flags and unfilled activation decisions

All flags default **false**:

| Setting | Effect |
|---|---|
| `LOYALTY_RECEIVING_ENABLED` | Allow authenticated durable intake |
| `LOYALTY_PROCESSING_ENABLED` | Start local canonical-read/reconciliation worker |
| `LOYALTY_POSTING_ENABLED` | Permit point postings, including compensating corrections |

Unset values remain unset in source. They must be supplied and separately approved before the applicable mode runs:

| Settings | Required evidence/decision |
|---|---|
| `LOYALTY_SHOP_DOMAIN`, `LOYALTY_SHOP_ID` | Exact approved `.myshopify.com` domain and Shop ID; domain must match existing transport configuration |
| `LOYALTY_LOCATION_IDS` | Explicit comma-separated approved retail Location IDs; no inferred primary location |
| `LOYALTY_LAUNCH_AT` | Explicit immutable UTC timestamp (`Z` or `+00:00`) |
| `LOYALTY_READ_ACCESS_VERIFIED` | Confirm required read access and query support; token presence is insufficient |
| `LOYALTY_READ_ALL_ORDERS_VERIFIED` | Confirm approved older-order access when required |
| `LOYALTY_EXCEPTION_OWNER` | Named accountable exception owner |
| `LOYALTY_RECEIPT_RETENTION_DAYS` | Receipt evidence retention decision; required for receiving |
| `LOYALTY_EVIDENCE_RETENTION_DAYS` | Canonical evidence policy; required for processing |
| `LOYALTY_LEDGER_RETENTION_DAYS`, `LOYALTY_BACKUP_RETENTION_DAYS` | Accounting evidence and backup policy; required for posting |
| `LOYALTY_BUDGET_CENTS` | Explicit approved spending budget in integer cents; unset is not zero approval |

Existing shared Shopify credentials are reused by code only. No credentials were read or changed for this implementation. `LOYALTY_BODY_LIMIT_BYTES` defaults to 262,144 (hard configurable maximum 1 MiB); `LOYALTY_API_REQUEST_LIMIT` defaults to 100 (maximum 500). These are protective software bounds, not a spending authorization. The code does not purchase anything or enforce the organization's external billing budget.

## Ops, corrections and retention

The existing matrix registers `ops.loyalty.view`, `admin.loyalty.reconcile`, and `admin.loyalty.correct`. Defaults allow only admin, with explicit admin denies respected. Defaults are seeded even when the employee portal is disabled, and existing grants/denies are preserved. Manager/cashier roles do not inherit Ops access through a role hierarchy. Navigation uses the same explicit permission.

September 11 policy approval: all cashiers may view balances/history inside POS through the phase-two verified app/shop authenticated-session policy; corrections remain restricted to authorized Ops users under the existing matrix. This does not grant cashier access to Ops or verify PIN identity. Read-only Shopify setup inspection is approved for the parent’s browser workflow. Runtime flags remain OFF and native testing/installation/activation are separately gated; see the [phase-two runbook](phase-two-runbook.md) and [native-test preflight](native-test-preflight.md).

Every Ops request reloads active session identity and honors password/session invalidation; actions also recheck authorization inside the write transaction. Mutations use the existing CSRF route convention and AuditLog. Lookup responses are `Cache-Control: no-store`.

- Replay: up to 25 explicit existing entitlement IDs; canonical refetch through the durable inbox. Reusing the request key is idempotent. Replay does not remove identity or correction holds.
- Correction: existing attributable order, expected revision, integer target, reason and case reference; delta bounded to 10,000 points and target bounded by retained calculated/posted evidence. It cannot create an account, award historical purchases, transfer identity or grant arbitrary credit. No-op and stale/busy corrections are rejected. Correction ledger and AuditLog commit together.
- A correction latches a **manual hold**. Future reads and refunds remain visible for review and cannot automatically overwrite the correction. Further bounded corrections remain possible. There is no automatic hold-release workflow in this release.
- Attachment verification: audited evidence attestation and queued recheck as described above, including before an account or eligible basis exists. It cannot clear identity/correction holds.
- Rebuild check: compares entitlement posted totals against ledger sums, reports discrepancies and audits the check. It does not silently rewrite history.

Receipt minimization is bounded and shop-specific: after the configured retention period, only completed non-admin receipt evidence is cleared, while delivery IDs/hashes remain dedupe tombstones. Queued/processing/review receipts and admin idempotency metadata remain intact. Canonical/order/ledger evidence, account identity, AuditLog and backups are **not automatically deleted**. Their configured retention values are activation-policy gates; implementing approved archival/deletion or backup cleanup remains a separate reviewed operational action, because append-only accounting evidence must remain reproducible. No point expiration is implemented.

## Deployment / activation preflight (not authorized)

1. Parent reviews the uncommitted diff and verification evidence; resolve any failures attributable to loyalty.
2. Approve exact commit, app target, additive schema/permission changes, database backup and restore rehearsal. No production-host edits or restarts are part of this work.
3. Deploy with all three flags off; inspect actual schema readiness independently and confirm existing inventory/TikTok behavior.
4. Verify real app distribution, `read_orders`, customer-ID access, location access, **`read_returns`** for Return/exchange queries, and required older-order access. The current reader needs `read_orders` plus `read_returns`; `read_customers`/`read_locations` and protected customer-ID access must be verified for the installed app and these query paths, not assumed from token presence. `read_all_orders` requires separate approval to recover orders beyond 60 days. No customer contacts or write scopes are needed for this reader. Confirm precise subscription targets separately.
5. Supply all exact activation decisions above. Independently sample real canonical shapes and inclusive-tax/refund accounting in authorized shadow mode. Synthetic tests do not establish installed scopes or live data completeness.
6. Approve posting explicitly only after shadow discrepancies, retention/backup procedure, exception owner and capacity/budget are resolved.

## Rollback

Pause **posting** for accounting defects; pause **processing** for worker defects; pause **receiving** for receiver/security defects. Keep healthy intake when practical. Show balances as potentially stale and retain backlog/evidence. Revert compatible application code without dropping loyalty tables. Repair incorrect awards with authorized compensating entries, never destructive ledger edits or a database restore over later valid awards. Existing inventory subscriptions, TikTok and Smile stay outside this rollback.

## Official API references checked during implementation

- [Order 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/Order): original/current totals, POS origin/location, edited state, transaction count and returns.
- [LineItem 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/LineItem): original totals, allocations including refunded quantities, original taxes, gift-card and physical-variant fields. `priceAfterAllDiscountsBeforeTaxesSet` is not used.
- [Refund 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/Refund), [RefundLineItem](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/RefundLineItem), [RefundShippingLine](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/RefundShippingLine), [OrderAdjustment](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/OrderAdjustment): refund component and transaction completeness.
- [Webhook reference 2026-04](https://shopify.dev/docs/api/webhooks/2026-04): topic names and kept/deleted customer GIDs plus merge status.
- [Delivery verification](https://shopify.dev/docs/apps/build/webhooks/verify-deliveries), [access scopes](https://shopify.dev/docs/api/usage/access-scopes): receiver requirements and access review. These sources establish API contracts, not Degen's installed state.

- [CashRoundingAdjustment 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/CashRoundingAdjustment), [OrderTransaction 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/OrderTransaction), [POS cash rounding](https://help.shopify.com/en/manual/sell-in-person/shopify-pos/cash-rounding-on-pos): signed payment/refund adjustments; cash-only rounding after discounts and tax.
- [Return 2026-04](https://shopify.dev/docs/api/admin-graphql/2026-04/objects/Return): `read_returns` and `exchangeLineItems` connection.

## Local verification (synthetic data only)

Use the existing interpreter with `scripts/run_loyalty_tests.py`. This runner suppresses dotenv, clears inherited application/provider variables, creates a fresh temporary database, disables external workers and rejects external sockets. No production or Shopify API was accessed. The WSL dependency environment supports domain/reader/SQLite/PostgreSQL tests; it lacks `itsdangerous` for Ops collection, so endpoint and browser checks use the existing Windows venv.

```text
<python> scripts/run_loyalty_tests.py tests/test_loyalty_config.py tests/test_loyalty_domain.py tests/test_loyalty_reader.py tests/test_loyalty_service.py tests/test_loyalty_lifecycle.py tests/test_loyalty_ops.py tests/test_loyalty_webhook.py -q --tb=short
<python> scripts/run_loyalty_tests.py --loyalty-pg-socket /tmp/loyalty-worker-pg-<fresh-cluster> tests/test_loyalty_service.py tests/test_loyalty_lifecycle.py -q --tb=short
<Windows python> scripts/run_loyalty_tests.py --loyalty-ui tests/test_loyalty_visual.py -q
<Windows python> scripts/run_loyalty_tests.py --loyalty-max-seconds 7200 --loyalty-chromium <existing-chrome.exe> --loyalty-browser-cache <existing-ms-playwright-directory> -v --tb=short
<python> -m compileall -q app
```

PostgreSQL tests require a **fresh disposable** local cluster, Unix socket under the shown prefix, database `loyalty_test`, role `loyalty_test`, port 55439, and no TCP listener. They create a unique synthetic schema per case. Keep the cluster and tests in the same controlling process in this tool environment; process cleanup terminates child servers between commands. Never point these tests at existing application databases. The tests in the repo reproduce concurrent same-delivery inserts, distinct delivery/order races, correction/replay fencing, immutable ledger checks, BIGINT capacity and schema corruption. The verification cluster is stopped after tests; the parent's separate server is untouched.

Offline Chromium uses synthetic TestClient HTML and local static assets; other requests are aborted. Both administrator and explicitly granted cashier layouts were exercised at 1440px and 390px. Screenshots are `verification/desktop.png`, `mobile.png`, `cashier-desktop.png`, and `cashier-mobile.png`. The screenshots were opened and inspected: dollar amounts, compact IDs, points visible without horizontal cropping, permission-restricted collapsed diagnostics, cashier navigation and “Never run” scan text are present.

Retention limitation remains explicit: configured evidence/ledger/backup durations are **not enforced disposal schedules**. Before activation, approve archival/disposal and backup handling while preserving non-expiring balances, dedupe identity and reproducible accounting. Do not turn deletion of evidence into point expiration.

### Results after parent review

- Final focused feature suite: **140 passed, 1 skipped** (16.24s). The skip is the PostgreSQL-only numeric-corruption case in the SQLite invocation.
- Isolated PostgreSQL 16 + SQLite service/lifecycle suite: **78 passed, 1 skipped** (32.08s). The same PostgreSQL-only case is skipped in the SQLite parameter and passes in the PostgreSQL parameter. The disposable cluster was stopped normally; no parent server was used.
- Offline Chromium: **1 passed** (4.87s), four screenshots regenerated and visually inspected.
- Existing admin-detail plus loyalty Ops regression group: **26 passed** (15.34s), after fixing navigation handling of lightweight user objects without an ID. The original full-run process had imported the earlier helper; its four corresponding failures are superseded by this passing targeted check.
- `compileall -q app` and `git diff --check` passed. No commit, push, deployment, subscription, activation or live API verification occurred.

The full-suite MCP smoke failure `test_smoke_database_url_env_missing_fails` also reproduces on clean commit `cbc7896` with the exact same isolation runner: the stripped environment makes `Path.home()` unavailable. No unrelated app change was made. The clean source copy lives under `/tmp/loyalty-clean-base-4m75ihwx`, with no other worktree touched. Its admin routing fixtures additionally encounter Windows/UNC SQLite locking; those results are not used to classify the navigation regression. Current-worktree admin routing is verified by the 26 passing tests above.

The final full-suite attempt stopped at its configured **600-second bound**: **2,097 passed, 85 skipped, 5 failed, 27 subtests passed** in 600.70s, approximately half the suite. It is **incomplete, not a full-suite pass**. Four failures are the navigation regression fixed and retested above; the remaining MCP smoke failure reproduces on the clean base. No other failures were observed before interruption. The earlier `/tmp/loyalty-full-tests-final.log` run had already stopped at the parent interruption and was not left running.

Exact review logs in this WSL session:

- `/tmp/loyalty-focused-final.log` — 140 passed, 1 skipped.
- `/tmp/loyalty-review-pg-final.log` — 78 passed, 1 skipped, clean server shutdown.
- `/tmp/loyalty-ui-review.log` — offline browser test and screenshots.
- `/tmp/loyalty-navigation-regression.log` — 26 passing navigation/Ops regressions after the final fix.
- `/tmp/loyalty-full-review-final.log` — bounded full-suite result and the pre-fix failure tracebacks.
- `/tmp/loyalty-baseline-comparison-clean.log` — clean-base MCP reproduction and unrelated UNC SQLite fixture limitation.

Residual activation gates are unchanged: exact shop/location IDs and domain, UTC cutoff, named exception owner, approved retention/backup handling and spending budget, verified installed scopes including `read_returns` and older-order approval where needed, live canonical/shadow validation, deployment and subscription approvals, and explicit processing/posting activation. Successful identity-merge release is a reviewed manual gate. None was supplied or enabled by implementation.

### Final interactive walkthrough and full-suite follow-up

This section supersedes the earlier ten-minute partial-run status above. The local server walkthrough uses actual routes, authentication, CSRF and forms on synthetic data. See [staff-walkthrough.md](staff-walkthrough.md) for the launcher and URL, and [release-review.md](release-review.md) for the final packet and gates. Screenshots `verification/demo-cashier-desktop.png`, `demo-cashier-mobile.png`, and `demo-admin-desktop.png` were regenerated and opened for inspection.

- Final feature/home/endpoint/browser group: **155 passed, 1 skipped** in 19.89s (`/tmp/loyalty-feature-release-final.log`).
- One complete full run: **3,931 passed, 221 failed, 89 skipped, 191 subtests passed**, 29m19s (`/tmp/loyalty-full-release-final.log`).
- All original failed cases plus the triggering browser case after harness repairs: **221 passed, 1 failed, 48 subtests passed**, 83.44s (`/tmp/loyalty-failure-focused-release-second.log`).
- The remaining employee-hours week-boundary test makes an unmocked Clockify request and is blocked by the synthetic network guard. It reproduces on clean base `cbc7896` with the exact final harness (`/tmp/loyalty-baseline-release-final.log`). It is a baseline/isolation blocker, not a waiver of the required full-suite gate.

The harness now assigns synthetic home/app-data/cache paths, lets `MEDIA_ROOT` inherit scratch `DATA_ROOT`, accepts explicit existing Chromium and Playwright binary-cache paths, and allows only loopback listeners created by the test process. Browser profiles remain temporary; external browser requests are blocked. An existing browser fixture's failed startup leaked its asyncio loop and caused the original cascade; no unrelated test or application source was modified. Use the full command above with actual existing binary paths from the staff walkthrough environment when reproducing. Core PostgreSQL/ledger code is unchanged since the earlier 78-pass disposable PostgreSQL/SQLite check.

Phase-two planning now exists in [phase-two-prd.md](phase-two-prd.md) and [phase-two-plan.md](phase-two-plan.md). No extension code, Shopify setup, deployment or activation occurred. The PRD requires review of the authenticated-user versus PIN-staff boundary before implementation.

Final demo-only recheck after clarifying the sample descriptions: **1 passed** in 5.35s (`/tmp/loyalty-demo-release-final.log`); screenshots regenerated and inspected. Windows `compileall` and `git diff --check` passed afterward. All work remains uncommitted.
