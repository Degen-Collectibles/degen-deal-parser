# Degen Ops Agent Team Rollout PRD

## Problem

Jeffrey needs a Degen Ops chatbot that owners, partners, and eventually employees can talk to for evidence-backed business decisions. The local Hermes/Codex MCP pilot works, but team rollout still needs a safe live-data access topology.

## Current State

- Local MCP server: `scripts/degen_ops_mcp.py`
- Local no-GUI chat shell: `scripts/degen_ops_chat.py`
- Live read-only data verifier: `scripts/degen_ops_live_data.py`
- Local one-command gate: `scripts/degen_ops_local_gate.py`
- Local smoke/preflight: `scripts/degen_ops_mcp_smoke.py`
- Answer-quality eval: `scripts/degen_ops_answer_eval.py`
- Change manifest: `scripts/degen_ops_change_manifest.py`
- Scoped config generator: `scripts/degen_ops_mcp_config.py`
- Scoped access-package generator: `scripts/degen_ops_team_package.py`
- Static readiness audit: `scripts/degen_ops_readiness.py`
- Completion audit: `scripts/degen_ops_completion_audit.py`
- MVP success-criteria audit: `scripts/degen_ops_mvp_audit.py`
- Scope-boundary audit: `scripts/degen_ops_scope_audit.py`
- One-command rollout gate: `scripts/degen_ops_rollout_gate.py`
- Topology planner: `scripts/degen_ops_topology_plan.py`
- Live-data approval packet: `scripts/degen_ops_approval_packet.py`
- Green-hosted pilot packet: `scripts/degen_ops_green_pilot_packet.py`
- Launch checklist: `scripts/degen_ops_launch_checklist.py`
- Local Hermes/Codex scopes: `owner`, `partner`, `employee`
- Missing or blank scope defaults to `employee`; owner and partner scope must be explicit.
- Agent prompt: `docs/ops/degen-ops-agent-instructions.md`
- Green/Brev can reach live DB read-only from `/opt/degen/web.env`.
- Jeffrey's local inherited Render-style `DATABASE_URL` is not reachable from this Windows shell.

## Success Criteria

- Owners and partners can ask the core buy question against current live data.
- Partner scope can answer buy questions and draft updates without exposing raw cash balances or owner loan/payback tools.
- Employees can ask inventory/channel-velocity questions without cash, bank, loan, owner-payback, or private finance tools.
- Missing or misconfigured scope fails safe to employee-level tools, not owner-level tools.
- Every recommendation cites MCP evidence and app surfaces.
- A non-GUI local chat shell can answer through the same scoped read-only tools for owner/partner testing.
- No money movement, inventory mutation, production write, Shopify/TikTok write, Discord/customer message, or partner/employee communication occurs without explicit approval.
- Scope boundaries are verified before access is given.

## Scope

- Configure a safe way for chat clients to reach the Degen Ops MCP tools with current data.
- Keep tool surface read-only.
- Maintain separate owner/partner/employee scope configs.
- Provide repeatable smoke checks before and after rollout.

## Non-Scope

- No production writes.
- No database migrations.
- No public internet MCP endpoint without a separate security review.
- No customer or partner messages sent by the agent.
- No autonomous inventory, Shopify, TikTok, Discord, banking, or payroll actions.

## Options

### Option A: Local Clients With Read-Only DB URL

Give approved machines a dedicated read-only DB URL or replica URL and install only the correct scoped MCP config.

Pros:
- Simple MCP stdio model.
- No new hosted service.
- Easy to smoke with `degen_ops_mcp_smoke.py`.

Cons:
- Requires database network access from each machine.
- Must manage DB credentials per audience/machine.

### Option B: Run Chat Client On Green/Brev

Run the MCP-aware client in the same environment that already reaches live DB.

Pros:
- Live data access already works from Green.
- Avoids exposing DB directly to laptops.

Cons:
- Less ergonomic for employees.
- Needs careful account/session boundaries.

### Option C: Hosted Read-Only MCP Gateway

Expose a remote MCP endpoint near the app/database with authentication, audit logging, and per-scope access.

Pros:
- Better team UX.
- Centralized access and revocation.
- No direct DB credentials on employee machines.

Cons:
- New production-facing service.
- Requires security review, auth design, logging, deployment, and rollback plan.

## Recommended Path

1. Keep Jeffrey's local owner pilot on local SQLite until live URL access is solved.
2. For partner testing, use `degen_ops_partner` with either a read-only DB URL or a Green-hosted client session. This scope excludes raw cash and owner loan/payback tools by default.
3. For employee testing, install only `degen_ops_employee`.
4. Use `scripts/degen_ops_chat.py` for quick no-GUI owner/partner tests when Hermes is not the right shell.
5. Generate per-audience access packages instead of copying Jeffrey's config by hand.
6. Do not build a hosted MCP gateway until Jeffrey approves Option C after reviewing auth, audit logging, and rollback.

## Scoped Access Packages

Generate a paste-safe package for the intended audience:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_team_package.py --scope employee --client hermes --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
.\.venv\Scripts\python.exe scripts\degen_ops_team_package.py --scope partner --client codex --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The package lists the expected tools, guardrails, config-generation command, chat preflight, and chat command. It intentionally references the DB URL by environment variable and does not print a raw database URL.

Before staging or handing off the work, generate the read-only change manifest:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_change_manifest.py --summary --json
```

Use summary mode for review in noisy worktrees. Run without `--summary` only when you need the full intended file list and explicit staging command. Do not use `git add -A` in a mixed worktree.

Run the completion audit when deciding whether the overall goal is actually done:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_completion_audit.py --json
```

The completion audit should report `code_ready: true` while `goal_complete: false` until partner/employee live access is approved and verified from the approved environment.

Generate the approval packet before choosing live-data topology:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_approval_packet.py --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The approval packet is the human sign-off aid for Option A, Option B, or Option C. It lists constraints, verification commands, rollback, and the exact open decisions. Do not implement the chosen topology until Jeffrey reviews it and says `proceed`.

Generate a topology recommendation before the approval discussion:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_launch_checklist.py --audience partner --client hermes
.\.venv\Scripts\python.exe scripts\degen_ops_topology_plan.py --audience partner --client hermes --json
.\.venv\Scripts\python.exe scripts\degen_ops_green_pilot_packet.py --audience partner --client hermes
```

By default, the planner recommends a Green-hosted first pilot until a direct read-only DB credential is both approved and verified. It does not create credentials, edit production, or install config. The launch checklist aggregates readiness, scope, the exact approval phrase, and the post-approval Green command block. The Green packet is the exact approval target: session boundaries, rollback, and verification commands.
The command above can render the packet from this Windows checkout. The generated packet's verification block is Bash-oriented for the approved Green/Brev shell under `/opt/degen/app`; use that generated block for Green execution.

## Read-Only DB Credential Template

Use `docs/ops/degen-ops-readonly-db-role.sql` as the starting point for a dedicated Postgres read-only role. It is a template only; do not run it in production without approval. After creating a credential, verify it with:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both --scope employee --read-check --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The MCP harness also starts DB sessions as read-only, but a dedicated read-only DB role is still required for non-owner machines.

## Verification

Run the local one-command gate before handoff:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_local_gate.py --json
```

This compiles the Ops paths, runs focused tests, smoke-tests Hermes/Codex MCP scopes, runs the static rollout gate, prints the compact change manifest, and checks Degen Ops diffs. It is read-only and does not approve Green, credentials, production writes, or team access.

Run focused tests:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_ops_mcp.py tests\test_ops_agent.py tests\test_degen_ops_mcp_smoke.py tests\test_degen_ops_mcp_config.py tests\test_degen_ops_chat.py tests\test_degen_ops_team_package.py -q
```

Run the static readiness audit:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_readiness.py
.\.venv\Scripts\python.exe scripts\degen_ops_readiness.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_completion_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_mvp_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_scope_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_launch_checklist.py --audience partner --client hermes --json
.\.venv\Scripts\python.exe scripts\degen_ops_topology_plan.py --audience partner --client hermes --json
.\.venv\Scripts\python.exe scripts\degen_ops_green_pilot_packet.py --audience partner --client hermes
.\.venv\Scripts\python.exe scripts\degen_ops_rollout_gate.py --json
```

The audit can report `code_ready: true` while `team_rollout_ready: false`. That is expected until the live-data access topology and read-only credential are approved.

The MVP audit maps the original success criteria to current repo evidence. It can report `mvp_code_ready: true` while still leaving team live rollout pending.

The completion audit maps the broader chatbot/MCP/Hermes/Green goal and should keep `goal_complete: false` until live team access has been approved and proven.

Run local config scope smoke:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both
```

Run DB-backed smoke for the target audience:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both --scope employee --read-check --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

Run scope-specific live-data verification after the read-only DB URL or Green-hosted environment is chosen:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_live_data.py --scope employee --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
.\.venv\Scripts\python.exe scripts\degen_ops_live_data.py --scope partner --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

For employees, the verifier must only check manifest, inventory snapshot, and channel velocity. For partners, it checks finance, inventory, channel velocity, and the redacted buy/update workflows. For owners, it also checks cash and loan/payback snapshots. The command does not print the raw database URL.

Run the no-LLM buy-decision pilot demo for owner/partner scope:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_pilot_demo.py --scope partner --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

This exercises the core buy workflow through read-only tools without making an LLM API call. Use `--scenario-json` to override the default demo lot with real lot numbers.

Run the one-command rollout gate with live checks once a read-only DB URL or Green-hosted environment is available:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_rollout_gate.py --run-live --run-pilot --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

This aggregates readiness, MVP, scope-boundary, live-data, and no-LLM pilot checks. It still does not mark team rollout approved; it only proves the selected access path works.

Run no-GUI chat one-shot after the AI provider key is configured:

```powershell
$env:AI_PROVIDER = "nvidia"
$env:NVIDIA_API_KEY = "..."
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope owner --preflight --read-check
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope owner --prompt "What evidence can you use for a buy decision?"
```

Chat `--preflight --read-check` exercises every tool exposed to the selected scope. Partner preflight must show 6 checks and must not include `get_cash_snapshot` or `get_loan_and_payback_snapshot`.

Before using a new prompt style in front of partners or employees, run candidate answers through the answer-quality eval:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_answer_eval.py --answers-file .\docs\ops\degen-ops-answer-examples.json --json
```

The eval is intentionally simple. It catches missing decision/evidence/routing/payback/risk language and blocks obvious partner cash leaks or employee loan/payback language.

Expected employee tools:

```text
get_ops_agent_manifest
get_inventory_snapshot
get_channel_velocity
```

Expected partner tools:

```text
get_ops_agent_manifest
get_finance_snapshot
get_inventory_snapshot
get_channel_velocity
evaluate_inventory_buy
generate_partner_update
```

## Rollback

- Remove the `degen_ops_*` MCP block from Hermes/Codex config.
- Restore the timestamped config backup if the local config was edited.
- Revoke any issued read-only DB credentials.
- For a future hosted MCP gateway, stop the gateway service and revoke its credentials.

## Open Questions

- Should any specific partner be granted owner scope after explicit approval, or should all partner machines stay on the redacted partner scope?
- Should employee access use local installed clients, a Green-hosted session, or a future hosted MCP gateway?
- What read-only DB credential or replica should be used for non-owner machines?
- Who approves partner-ready update drafts before sending?
