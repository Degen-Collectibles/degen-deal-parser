# Security Hardening Phased Design

## Problem

The completed Codex Security scan `505f45f1-7b31-4823-81a0-90a32c98d07e` validated 29 medium/low findings. The highest-priority work is not a single exploit; it is a set of trust-boundary failures that can turn authenticated access, external content, or delegated permissions into script execution, account takeover, financial-data corruption, resource exhaustion, or secret disclosure.

## Current State

The repository is at `e83f93222f8a5d388b7d8fb14d6dd9d1522366f4`. The original checkout contains unrelated untracked work, so implementation is isolated on branch `codex/security-hardening` under `.worktrees/security-hardening`. No production state, credentials, database rows, or deployment configuration will be changed by this work.

## Success Criteria

1. Every addressed finding has a regression test that fails against the vulnerable behavior and passes after the minimal fix.
2. The original exploit or misuse path is replayed at the closest practical interface.
3. Existing authorized workflows continue to pass focused and full-suite tests.
4. Unrelated files and the original checkout remain unchanged.
5. Operational follow-ups such as credential rotation, deployment, or production data migration are explicitly separated from code remediation.

## Scope and Order

### Phase 1A: Browser script execution

- Stop Discord-hosted active content from executing inline on the authenticated application origin.
- Reject non-local and non-HTTP(S) deal return targets, including `javascript:` URLs.
- Render TikTok LIVE titles as text rather than HTML.

### Phase 1B: Account and role security

- Require a fresh proof of identity before changing a recovery email.
- Prevent delegated employee editors from granting roles or permissions above their own effective authority, including promotion of a different account to administrator.

### Phase 2: Financial source integrity

- Authenticate inbound financial email senders instead of trusting attacker-controlled display/header fields.
- Preserve immutable Discord financial source evidence when upstream messages are edited or deleted.

### Phase 3: Path and resource bounds

- Constrain capture identifiers and filesystem resolution to the intended capture root.
- Apply explicit size, count, complexity, and date-range limits to uploads, images, spreadsheets, regex-like search, and expensive report queries.

### Phase 4: Secrets and server-side sensitive calculations

- Remove TikTok and Clockify secrets from URLs and logs; redact credential-bearing failures.
- Treat client-submitted buylist prices as advisory input and calculate authoritative payouts server-side.

## Non-Scope

- Production deployment, service restart, credential rotation, or data repair.
- Broad authentication, router, model, or template refactors.
- Changing the proven TikTok Shop webhook HMAC algorithm.
- Reclassifying scan severity or suppressing findings without a code/test basis.

## Design

Each finding is handled as a small security invariant at the nearest shared boundary. Input validation belongs at the first trusted server boundary; output encoding belongs at the rendering sink; authorization compares requested authority with the actor's effective authority; resource controls reject work before expensive parsing or allocation; secret handling uses headers or redacted structured logs; monetary totals are derived from server-owned data.

Where a behavior has multiple call sites, the fix goes into the shared helper rather than one route. Compatibility is preserved for known-good inputs. Rejected requests use the repository's existing HTTP error or validation conventions and must not leak the rejected secret or payload.

## Error Handling and Auditability

- Security rejections are explicit and deterministic.
- Logs identify the failed control without including credentials, raw sensitive payloads, or customer PII.
- Authorization failures return the existing forbidden response style.
- Resource-limit failures occur before parsing and return a client-actionable 4xx response.
- Existing audit records remain intact; financial source changes add provenance rather than rewriting historical evidence.

## Verification

For each finding: add the failing regression test, run it alone to confirm the expected failure, implement the smallest fix, run the focused test, run the owning module's nearby tests, and replay the exploit/misuse case. At phase boundaries, run `python -m compileall app` and the full `pytest --tb=short -q` suite. A full-suite baseline is established before the first production-code change.

## Risks and Mitigations

- **Workflow breakage:** preserve safe local return paths, ordinary image types, authorized manager edits, and normal report ranges in positive tests.
- **Schema/migration risk:** pause before any irreversible model change; prefer existing audit/version structures when available.
- **Hidden production assumptions:** keep all work local and require a separate deployment preflight.
- **Over-broad fixes:** one finding, one invariant, and focused tests before moving on.

## Rollback

All work is confined to `codex/security-hardening`. Individual finding patches can be reverted independently because tests and implementation are kept narrowly paired. The original `main` checkout remains untouched. No operational rollback is needed until a later, separately approved deployment.

## Open Operational Actions

After code verification, Jeffrey must separately approve any deployment, credential rotation, production audit, or historical data repair. Those actions are not implied by approval to edit and test this branch.
