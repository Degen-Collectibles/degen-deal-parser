# Shopify POS loyalty implementation plan

Approved specification: `/mnt/c/Users/jeffr/AppData/Local/Temp/degen-custom-loyalty-prd-refreshed.md`, as superseded by Jeffrey's September 10 implementation instructions. Execute inline as one worker; no commits, external data, deployment, provisioning, spending, or activation.

Architecture: independent minimized inbox → leased canonical read → exact deterministic calculation → serialized entitlement and append-only integer ledger. Reconciliation uses updated-order windows plus scheduled recovery of known orders. Ops uses existing authenticated sessions, permission matrix, CSRF and AuditLog. No coupling to inventory stock processing or TikTok.

- [x] Accounting: add `app/loyalty/domain.py`, `models.py`, `schema.py`, `config.py`, `service.py`; register tables in `app/db.py`; write synthetic calculator, schema and transactional tests. Verify original discounted line basis, inclusive taxes, refund success, cumulative whole-point rounding, unique identities/revisions, first inserts, leases and stale evidence.
- [x] Intake and recovery: add `shopify_reader.py`, `worker.py`, `reconciliation.py`, dedicated webhook router. Test raw HMAC, size/shop/topic bounds, minimized durable acknowledgment, duplicate deliveries, complete paginated reads, throttling, retries, scanner crash recovery and known-order recovery. All three flags default off; require actual schema readiness and configured activation policy.
- [x] Ops: add loyalty router/template, permission catalog/defaults and sidebar link. Test live session/RBAC including explicit admin denial, CSRF, bounded replay, correction idempotency and atomic audit entries. Balances derive from ledger; review states visible without fabricated points.
- [x] Release validation: compile, focused suite, full suite as feasible in isolated synthetic environment with dotenv and external connections blocked. Review final diff, document exact results, limitations, activation gates, retention and rollback in `docs/loyalty/runbook.md`.

Required invariants: points are whole signed integers; money exact cents; one account per shop/customer ID; POS/USD/approved location/prospective created+processed+payment timestamps; seven-day authoritative attachment evidence, no inference from updatedAt/receipt; gift issuance excluded, gift tender allowed. Unset IDs/cutoff/owner/retention/budget prevent activation. Unsupported edits/exchanges/refunds/identity changes remain visible review. No contacts fetched or retained.

Implemented locally; actual verification results and incomplete full-suite coverage are recorded in `runbook.md`. Deployment and activation remain unapproved and disabled.
