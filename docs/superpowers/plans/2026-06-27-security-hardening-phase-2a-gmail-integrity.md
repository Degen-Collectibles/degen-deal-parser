# Security Hardening Phase 2A Gmail Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent unauthenticated or sender-spoofed email from creating SortSwift financial transactions while retaining quarantined evidence for review.

**Architecture:** Extract and evaluate Gmail's receiver-generated authentication results before entering the SortSwift transaction path. Exact sender identity and aligned SPF/DKIM evidence are required for automatic transaction creation; untrusted messages remain visible as quarantined Gmail receipts but never create or update a financial `Transaction`.

**Tech Stack:** Python 3.14, Gmail API message metadata, SQLModel, pytest

---

### Task 1: Carry authentication evidence through Gmail sync

**Files:**
- Modify: `app/discord/gmail_financials.py:403-604,721-762`
- Test: `tests/test_gmail_financials.py`

- [ ] **Step 1: Write failing source-authentication tests**

Add fixtures for: an attacker sender with SortSwift body; spoofed `From` with failed SPF/DKIM; exact `no-reply@mail.sortswift.com` with Gmail `Authentication-Results` showing aligned pass; and exact sender with no trusted authentication result. Assert only the aligned pass can create a `Transaction`; all others persist evidence with a quarantined/untrusted status and no transaction ID.

- [ ] **Step 2: Run focused tests and verify vulnerable transaction creation**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_gmail_financials.py -k 'sortswift or authentication' -q
```

Expected: attacker/body-only and failed-auth cases currently create a transaction or lack quarantine state.

- [ ] **Step 3: Implement exact sender and aligned receiver-auth checks**

Parse the `From` mailbox as a single addr-spec and require exact lowercase `no-reply@mail.sortswift.com`. From the full Gmail payload, collect `Authentication-Results` values and accept only a receiver result for Google's auth service that records `dkim=pass` with an aligned `header.i`/`header.d` domain or `spf=pass` with an aligned `smtp.mailfrom` domain. Fail closed when evidence is absent, malformed, ambiguous, or failed. Pass only a boolean/trust result and non-sensitive reason code into the receipt upsert path.

- [ ] **Step 4: Quarantine before the transaction sink**

For a body recognized as SortSwift but lacking trusted source evidence, create/update the `GmailReceipt` as quarantined/untrusted with parsed evidence for operator review, but do not call `_upsert_sortswift_transaction` and do not leave an older transaction linked after trust becomes invalid. Do not log raw headers or message bodies.

- [ ] **Step 5: Verify focused and nearby Gmail behavior**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_gmail_financials.py -q
```

Expected: authenticated SortSwift dedupe and line-item behavior remains green; generic receipts remain reviewable; spoofed content creates no financial transaction.

- [ ] **Step 6: Verify the phase and checkpoint**

Run compileall, the full suite, `git diff --check`, and inspect only the Gmail/test diff. Do not connect to live Gmail, send mail, deploy, rotate OAuth credentials, or alter production receipt rows.
