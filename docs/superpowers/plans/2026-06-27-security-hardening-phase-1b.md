# Security Hardening Phase 1B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent a stolen session from rebinding password recovery and prevent delegated employee editors from creating an administrator through a second account.

**Architecture:** Reuse the existing current-password verification primitive for sensitive self-service email changes, apply strict single-mailbox validation, and enforce a centralized role-transition lattice at the POST mutation boundary. UI options mirror the server predicate but never replace it.

**Tech Stack:** Python 3.14, FastAPI, SQLModel, existing password hashing/auth helpers, Jinja2, unittest/pytest

---

### Task 1: Require fresh authentication for recovery-email changes

**Files:**
- Modify: `app/routers/team.py:1971-2058`
- Modify: `app/templates/team/profile.html:33-80`
- Test: `tests/test_employee_portal_pii_capture.py:399-456`

- [ ] **Step 1: Extend profile tests before production code**

Change the existing happy-path email test to submit the correct current password. Add tests proving a changed or cleared email with a missing/wrong password is rejected and leaves both `email_ciphertext` and `email_lookup_hash` unchanged. Add comma-separated, CR/LF, and malformed-address cases and assert they are rejected as non-single-mailbox values.

- [ ] **Step 2: Run the focused tests and confirm the vulnerable behavior**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_employee_portal_pii_capture.py -k 'ProfileSelfEditEmailTests' -q
```

Expected: missing/wrong-password and at least comma-separated-address tests fail before the fix.

- [ ] **Step 3: Implement fresh-password and mailbox validation**

Add `current_password` to the profile form. When the normalized email differs from the persisted email, verify the submitted password against the freshly loaded user before any mutation. Accept exactly one syntactically valid mailbox; reject separators, CR/LF, and parser results containing anything other than a single address. Do not log the password or address. Preserve edits to unrelated profile fields when the email is unchanged and no password is supplied.

- [ ] **Step 4: Verify profile and reset-delivery behavior**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_employee_portal_pii_capture.py -k 'ProfileSelfEditEmailTests' -q
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_employee_invite_sms.py -k 'PasswordResetEmailTests' -q
```

Expected: all selected tests pass, legitimate single-address reset delivery remains intact, and the old address remains authoritative after failed changes.

- [ ] **Step 5: Checkpoint the narrow diff**

Run `git diff --check` and inspect only the profile route, template, and tests. Do not commit, push, deploy, or send real email.

### Task 2: Enforce role hierarchy on delegated editors

**Files:**
- Modify: `app/routers/team_admin_employees.py:95-118,595-680,1820-1899`
- Test: `tests/test_employee_portal_wave4_hardening.py:251-327`

- [ ] **Step 1: Write the missing delegated-editor tests**

Add a manager/reviewer actor with both employee-edit and permission-edit grants. Assert attempts to promote a different employee to `admin`, modify a peer, or modify an existing administrator return `403` and do not persist. Assert an allowed lower-authority transition such as `employee -> viewer` succeeds. Keep the existing administrator-to-target role-change test unchanged.

- [ ] **Step 2: Run the focused tests and confirm indirect promotion is currently possible**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_employee_portal_wave4_hardening.py -k 'ProfileUpdateGateTests' -q
```

Expected: second-account promotion/peer-target tests fail before the fix; existing default-deny and self-role tests pass.

- [ ] **Step 3: Implement a shared role-transition predicate**

Define actor authority using the existing role relationship where `manager` and `reviewer` are peers below `admin`. Allow an administrator to change another account under the existing self guard. For a non-admin delegated editor, require both the current target role and requested role to be strictly below the actor. Apply this check in the POST handler before assignment. Use the same predicate to filter role options in detail context, while retaining the POST check as authoritative.

- [ ] **Step 4: Verify focused employee authorization tests**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_employee_portal_wave4_hardening.py -k 'ProfileUpdateGateTests' -q
```

Expected: all selected tests pass, including legitimate administrator behavior and allowed lower-role delegated edits.

- [ ] **Step 5: Verify the complete account-security phase**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m compileall app
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q
```

Expected: compile succeeds and the full suite passes. Inspect `git diff --check` and do not commit, push, deploy, migrate data, or invalidate production sessions.
