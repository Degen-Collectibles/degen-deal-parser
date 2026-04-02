# CONTROL.md

## Current Phase
Testing & Stabilization

## Active Agents

### ⚙️ QUEUE — Worker & Processing
Status: In progress
Task: Fix state machine + stuck queue
Next: Verify with tester

### 🔁 REPARSE — Replay System
Status: Needs fix
Task: Fix legacy alias bug (needs_review, deleted)
Next: Patch + rerun tests

### 🧪 TEST — Validation
Status: Running
Task: Validate queue + reparse
Blocking: REPARSE fix

### 🌐 INFRA — Debugging
Status: Partial
Task: Add debug/admin page

### 🧠 LEARNING — Corrections
Status: Paused
Task: Wait for reparse to stabilize

### 🖥️ UI — UX Improvements
Status: Not started
Task: Analyze UI

---

## Current Priority (ONLY ONE)

Fix reparse alias bug.

---

## Next Actions

1. Send fix prompt to REPARSE agent
2. Wait for result
3. Send result to TEST agent
4. Confirm all tests pass

---

## Rules

- Only work on ONE priority at a time
- Do NOT switch agents randomly
- Do NOT copy/paste old prompts manually
- Always update this file before doing anything