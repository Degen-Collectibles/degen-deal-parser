# Brev CLI Re-Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refresh the local WSL Brev CLI API login without discarding still-working cached SSH access or exposing credentials.

**Architecture:** Treat authentication repair as a local, interactive operational workflow separate from repository deployment. Verify cached access first, invoke `brev login` without logout/deregister, pause for Jeffrey if browser or device approval is required, then verify both API listing and read-only SSH execution without logged-out warnings.

**Tech Stack:** Windows PowerShell, WSL, NVIDIA Brev CLI, cached SSH configuration.

## Global Constraints

- Do not run `brev logout`, `brev deregister`, delete Brev state, or rotate unrelated credentials.
- Never paste a device code, browser token, API key, or authentication response into repository files or chat.
- Preserve cached SSH access until replacement API authentication is proven.
- Use only read-only production commands during verification.
- Stop if authentication requires a credential or authority not already approved by Jeffrey.
- Do not delegate the interactive credential step to a subagent.

## File Map

- No repository files are modified by execution.
- This plan file is the durable operational checklist.

---

### Task 1: Capture the current authentication boundary

**Files:**
- No files modified.

**Interfaces:**
- Consumes: current WSL Brev CLI state and cached SSH configuration.
- Produces: evidence that cached SSH works and API authentication is the failing layer.

- [ ] **Step 1: Verify cached read-only SSH execution**

Run:

```powershell
wsl.exe -e brev exec openclaw-9902ae "printf 'cached_ssh=ok\n'; git -C /opt/degen/app rev-parse --abbrev-ref HEAD; curl -fsS -o /dev/null -w 'health_http=%{http_code}\n' http://127.0.0.1:8000/health"
```

Expected remote evidence:

```text
cached_ssh=ok
main
health_http=200
```

The current broken state may append `You are currently logged out` warnings after that successful remote output. Do not interpret those warnings as SSH failure.

- [ ] **Step 2: Verify the API-auth symptom without mutation**

Run:

```powershell
wsl.exe -e brev ls
```

Expected before repair: Brev requests login or fails to list instances because its API token is unavailable or expired. Record the exact error without copying credentials.

### Task 2: Refresh Brev login without destroying cached access

**Files:**
- No files modified.

**Interfaces:**
- Consumes: Brev's interactive login flow.
- Produces: refreshed local Brev API authentication.

- [ ] **Step 1: Start the supported login command**

Run:

```powershell
wsl.exe -e brev login
```

Do not precede this with logout or deregistration.

- [ ] **Step 2: Handle interactive approval safely**

If Brev opens a browser, complete the NVIDIA/Brev login in that browser. If it prints a browser URL or device-approval instruction, tell Jeffrey only that interactive approval is waiting and identify the destination domain; do not copy a secret device code into chat.

If the non-interactive terminal cannot complete the flow, stop the command cleanly and have Jeffrey run `wsl.exe -e brev login` in a visible PowerShell or Windows Terminal session. Resume verification only after he confirms completion.

- [ ] **Step 3: Treat login failure as non-destructive**

If login fails, do not run cleanup commands. Re-run the cached read-only SSH command from Task 1 to verify that existing access remains available, then report the login error as the blocker.

### Task 3: Verify repaired API and SSH access

**Files:**
- No files modified.

**Interfaces:**
- Consumes: refreshed Brev login.
- Produces: evidence that API listing and cached SSH execution both work without authentication warnings.

- [ ] **Step 1: Verify API instance listing**

Run:

```powershell
wsl.exe -e brev ls
```

Expected: command exits 0, lists `openclaw-9902ae`, and does not prompt for login.

- [ ] **Step 2: Verify read-only production execution**

Run:

```powershell
wsl.exe -e brev exec openclaw-9902ae "printf 'brev_auth=ok\n'; git -C /opt/degen/app rev-parse HEAD; systemctl is-active degen-web.service; systemctl is-active degen-worker.service; curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/health"
```

Expected:

```text
brev_auth=ok
active
active
200
```

Between `brev_auth=ok` and the two `active` lines, expect one line containing the current 40-character hexadecimal production SHA.

The command must exit 0 without the post-command logged-out warning.

- [ ] **Step 3: Close out without repository mutation**

Report the successful `brev ls` and read-only execution evidence. Confirm `git status --short --branch` in the deploy-hardening worktree remains unchanged by the credential repair.
