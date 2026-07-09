# Security Hardening Phase 3A Capture Path Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent an employee-controlled Degen Eye `capture_id` from selecting or rewriting any capture except that employee's exact canonical metadata file.

**Architecture:** Replace recursive glob lookup with one strict capture-ID parser and one canonical, root-confined resolver shared by every capture metadata operation. Before the batch route mutates inventory, validate every supplied capture reference against the authenticated user's immutable numeric ID; repeat the ownership check inside the post-commit metadata update to fail closed across the check/use boundary. Rows without a v2 capture ID retain the existing v1/manual intake behavior.

**Tech Stack:** Python 3.14, FastAPI, pathlib, JSON capture metadata, SQLModel, pytest

---

### Task 1: Make capture resolution exact and root-confined

**Files:**
- Modify: `app/inventory/degen_eye_v2_training.py:84-102,191-227`
- Create: `tests/test_degen_eye_v2_capture_security.py`

- [ ] **Step 1: Write failing resolver tests**

Create a temporary capture root and prove the current resolver accepts `*` and `../` payloads. Add table-driven rejection coverage for wildcards, separators, backslashes, absolute/drive paths, whitespace, uppercase or short UUIDs, invalid calendar dates, non-string values, and a canonical-name symlink that resolves outside the root. Assert rejected inputs leave in-root and sibling JSON bytes unchanged.

- [ ] **Step 2: Run the new tests and confirm the exploit paths fail before implementation**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_degen_eye_v2_capture_security.py -q
```

Expected: wildcard/traversal and malformed-ID assertions fail against the current glob-based resolver.

- [ ] **Step 3: Implement a canonical parser and exact resolver**

Accept only `^[0-9]{8}_[0-9a-f]{32}$`, validate the date prefix with `datetime.strptime`, and construct only `capture_root / YYYY-MM-DD / <capture_id>.json`. Resolve the configured root and candidate, require `candidate.relative_to(root)`, require a regular file, and require the loaded JSON object's `capture_id` to equal the request. Remove the recursive glob fallback. Do not normalize attacker input into a valid ID.

- [ ] **Step 4: Verify every metadata call site uses the strict resolver**

Cover `attach_prediction`, `attach_confirmed_label`, and `mark_training_indexed` with canonical positive tests plus malformed/traversal negative tests. Preserve server-generated capture IDs and the stable `YYYY-MM-DD` layout.

### Task 2: Bind batch confirmation to the authenticated capture owner

**Files:**
- Modify: `app/inventory/degen_eye_v2_training.py:204-346`
- Modify: `app/inventory/routes.py:5832-5982`
- Modify: `tests/test_employee_ops_access.py:180-450`
- Test: `tests/test_degen_eye_v2_capture_security.py`

- [ ] **Step 1: Write failing ownership and atomic-preflight tests**

Prove an ordinary employee can currently submit another employee's valid capture, a wildcard, or traversal value and still create inventory or alter metadata. Add tests for own capture success; other-owner, missing/malformed owner, payload-ID mismatch, wildcard, traversal, and non-string rejection; a mixed batch whose later row is invalid; and a row with no capture ID. Assert rejected batches create no `InventoryItem` or stock movement and modify no JSON.

- [ ] **Step 2: Add a shared immutable-ID ownership predicate**

Read `payload["employee"]["id"]` and compare it to the authenticated `User.id` as an actual integer (reject booleans, strings, missing data, and malformed metadata). Extend `attach_confirmed_label` with a required `expected_employee_id` for route-originated confirmations and recheck ownership immediately before applying the metadata update. Keep `confirmed_by` as audit data only; never use username, display name, or role as identity.

- [ ] **Step 3: Preflight all capture references before database mutation**

After authentication and body-shape checks, inspect every processable batch row that supplies `_v2_capture_id` or `capture_id`. Reject invalid, missing, and other-user captures with one generic 4xx response before calling either stock-receive helper. Preserve existing behavior for rows without capture IDs. Use the same capture-ID extraction rule during preflight and update scheduling so a value cannot be checked one way and consumed another way.

- [ ] **Step 4: Make post-commit metadata failure visible without duplicating inventory**

Check every `attach_confirmed_label` result. Keep the database-first order so a later database failure cannot leave metadata pointing to an uncommitted inventory ID, but include a deterministic warning/error field and structured log if an owned capture disappears or fails its repeated check after commit. Do not turn a committed inventory receive into a retry-inducing generic failure.

- [ ] **Step 5: Verify focused, adjacent, and full-suite behavior**

Run the new capture-security tests, `tests/test_employee_ops_access.py`, nearby Degen Eye/inventory tests, `python -m compileall -q app`, `git diff --check`, then the full suite. Do not migrate or rewrite production captures, deploy, or prune files in this task.

## Compatibility and deployment notes

- Canonically generated captures continue to work; manually relocated metadata and legacy files missing numeric `employee.id` fail closed and should be audited separately before deployment.
- A shared browser may retain another employee's local batch after logout; the server rejection is intentional. A UI cleanup message can be added separately if live usage shows confusion.
- This closes the remote employee path. It does not treat direct filesystem access as an untrusted boundary.
