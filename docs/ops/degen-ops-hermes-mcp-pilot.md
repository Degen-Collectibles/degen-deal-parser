# Degen Ops Hermes MCP Pilot

## Goal

Expose Degen business evidence to Hermes, Codex, and other MCP-aware agents through a bounded read-only MCP server.

This is not a production write path. The server does not expose arbitrary SQL, inventory mutation, money movement, Shopify/TikTok writes, Discord/customer messages, or employee/partner communications.

## Architecture

```text
Hermes / Codex / MCP client
        |
        | stdio MCP
        v
scripts/degen_ops_mcp.py
        |
        v
app.ops_mcp.DegenOpsMcpHarness
        |
        v
Existing Degen app models and reporting helpers
```

The LLM lives in Hermes or Codex. The Degen MCP server is the read-only tool harness.

The MCP harness also starts database sessions in read-only mode (`SET TRANSACTION READ ONLY` for Postgres, `PRAGMA query_only = ON` for SQLite). For production-like use, still provide a read-only database role or replica in `DATABASE_URL`.

Use `docs/ops/degen-ops-agent-instructions.md` as the chat agent prompt. The MCP server supplies evidence; the prompt tells the LLM how to ask questions, call tools, cite evidence, and stay inside approval gates.

## Tool Scopes

Set `DEGEN_OPS_MCP_SCOPE` before starting the MCP server. If the scope is missing or blank, the server defaults to `employee` instead of `owner`.

| Scope | Intended audience | Tools exposed |
|---|---|---|
| `owner` | Jeffrey / owners | All read-only tools |
| `partner` | business partners | Finance summary, inventory, channel velocity, redacted buy evaluation, partner update draft |
| `employee` | staff / stream operators | Manifest, inventory snapshot, channel velocity |

Unknown scopes fail at startup instead of widening access. Owner and partner access must be explicit in config or CLI arguments.

## Tools

Owner scope:
- `get_ops_agent_manifest`
- `get_finance_snapshot`
- `get_cash_snapshot`
- `get_inventory_snapshot`
- `get_channel_velocity`
- `get_loan_and_payback_snapshot`
- `evaluate_inventory_buy`
- `generate_partner_update`

Partner scope:
- `get_ops_agent_manifest`
- `get_finance_snapshot`
- `get_inventory_snapshot`
- `get_channel_velocity`
- `evaluate_inventory_buy`
- `generate_partner_update`

Partner buy evaluations hide raw cash balances, account balances, reserve-gap dollars, and owner loan/payback totals. Install `owner` scope only when that visibility is explicitly approved.

Employee scope:
- `get_ops_agent_manifest`
- `get_inventory_snapshot`
- `get_channel_velocity`

## Local Setup

Install dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Local Terminal Chat

For a no-GUI chat shell that uses the same read-only Degen Ops harness, run:

```powershell
$env:AI_PROVIDER = "nvidia"
$env:NVIDIA_API_KEY = "..."
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope owner
```

The chat shell also defaults to `employee` when `--scope` is omitted. Use `--scope owner` or `--scope partner` only on approved machines.

Preflight the terminal chat setup without making an LLM call:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope employee --preflight
```

Add a read-only DB check when the configured database should be reachable:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope employee --preflight --read-check
```

`--read-check` calls each tool available in the selected scope. For partner scope it checks finance, inventory, velocity, redacted buy evaluation, and partner-update drafting while excluding owner-only cash and loan snapshots.

One-shot prompt mode:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope owner --prompt "Should we buy a Pokemon sealed lot for $2,000 if expected revenue is $3,600 over 40 units?"
```

For live data without writing a DB URL into local config, pass a read-only database URL by environment variable:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_chat.py --scope employee --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The chat script uses the repo's existing OpenAI-compatible AI settings (`AI_PROVIDER`, `NVIDIA_API_KEY`, `NVIDIA_BASE_URL`, `NVIDIA_FAST_MODEL`, or OpenAI equivalents) and the same scoped tool list as MCP. It exits before chatting if the selected provider has no API key.

Run the MCP server directly:

```powershell
$env:DEGEN_OPS_MCP_SCOPE = "owner"
.\.venv\Scripts\python.exe scripts\degen_ops_mcp.py
```

For employee-limited testing:

```powershell
$env:DEGEN_OPS_MCP_SCOPE = "employee"
.\.venv\Scripts\python.exe scripts\degen_ops_mcp.py
```

## Hermes Config

Hermes MCP docs use `mcp_servers` for local stdio servers. On this Windows Hermes Desktop install, the active config file is:

```text
C:\Users\jeffr\AppData\Local\hermes\config.yaml
```

This machine has local pilot entries named `degen_ops_owner`, `degen_ops_partner`, and `degen_ops_employee`. The config was backed up before editing using the `config.yaml.bak-*-degen-ops-mcp` and `config.yaml.bak-*-degen-ops-scopes` naming patterns.

Add one server per audience. Keep the database target explicit; Hermes filters inherited environment variables before launching stdio MCP servers.

This machine also has a named Hermes personality, `degen_ops`, installed under `agent.personalities`. It is not the global default; select it in a Degen Ops chat with `/personality degen_ops`. The personality config was backed up before editing using the `config.yaml.bak-*-degen-ops-personality` naming pattern.

Owner-only pilot:

```yaml
mcp_servers:
  degen_ops_owner:
    command: "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
    args:
      - "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"
    env:
      DEGEN_OPS_MCP_SCOPE: "owner"
      DATABASE_URL: "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
      LOG_TO_FILE: "false"
    timeout: 120
    connect_timeout: 60
```

Employee-limited pilot:

```yaml
mcp_servers:
  degen_ops_employee:
    command: "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
    args:
      - "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"
    env:
      DEGEN_OPS_MCP_SCOPE: "employee"
      DATABASE_URL: "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
      LOG_TO_FILE: "false"
    timeout: 120
    connect_timeout: 60
```

Partner pilot:

```yaml
mcp_servers:
  degen_ops_partner:
    command: "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
    args:
      - "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"
    env:
      DEGEN_OPS_MCP_SCOPE: "partner"
      DATABASE_URL: "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
      LOG_TO_FILE: "false"
    timeout: 120
    connect_timeout: 60
```

For live data, replace the local SQLite `DATABASE_URL` with a reachable read-only database URL. Do not point a shared employee Hermes instance at owner scope or owner-only finance/cash/loan data.

## Codex MCP Config

Use the same stdio command in the Codex MCP config for this repo. On this machine, the active config is:

```text
C:\Users\jeffr\.codex\config.toml
```

This machine has local pilot entries named `degen_ops_owner`, `degen_ops_partner`, and `degen_ops_employee`. The config was backed up before editing using the `config.toml.bak-*-degen-ops-mcp` and `config.toml.bak-*-degen-ops-scopes` naming patterns.

```toml
[mcp_servers.degen_ops_owner]
command = "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
args = ["C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"]

[mcp_servers.degen_ops_owner.env]
DEGEN_OPS_MCP_SCOPE = "owner"
DATABASE_URL = "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
LOG_TO_FILE = "false"

[mcp_servers.degen_ops_partner]
command = "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
args = ["C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"]

[mcp_servers.degen_ops_partner.env]
DEGEN_OPS_MCP_SCOPE = "partner"
DATABASE_URL = "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
LOG_TO_FILE = "false"

[mcp_servers.degen_ops_employee]
command = "C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\.venv\\Scripts\\python.exe"
args = ["C:\\Users\\jeffr\\discord-deal-parser\\live-deal-parser\\scripts\\degen_ops_mcp.py"]

[mcp_servers.degen_ops_employee.env]
DEGEN_OPS_MCP_SCOPE = "employee"
DATABASE_URL = "sqlite:///C:/Users/jeffr/discord-deal-parser/live-deal-parser/data/degen_live.db"
LOG_TO_FILE = "false"
```

## Pilot Prompts

Before using Hermes for Degen Ops, select the Degen Ops personality if it is installed:

```text
/personality degen_ops
```

Owner:
- "Should we buy a Pokemon sealed lot for $2,000 if expected revenue is $3,600 over 40 units?"
- "What is our current cash, revenue, profit, inventory deployed, and loan/payback posture?"
- "Give me a partner-ready weekly update for this proposed buy."

Employee:
- "What categories are moving fastest by channel?"
- "What inventory lane should streamers prioritize this week?"
- "What products look stale or low-velocity based on current evidence?"

## Rollout Gate

Before exposing this to anyone besides Jeffrey:
- Verify `DEGEN_OPS_MCP_SCOPE=employee` only registers employee-safe tools.
- Verify the configured `DATABASE_URL` is local SQLite for dry runs or a read-only live database role for production-like use.
- Confirm no raw SQL tool exists.
- Confirm no tool writes to database, Shopify, TikTok, Discord, bank, or inventory state.
- Confirm answers cite evidence and include uncertainty where data is thin.
- Run the focused tests:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_ops_mcp.py tests\test_ops_agent.py tests\test_degen_ops_mcp_smoke.py tests\test_degen_ops_chat.py -q
```

Protocol smoke from the active config should list tools and return a read-only manifest before inviting anyone else into the workflow.

Run the static readiness audit before team pilots:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_local_gate.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_readiness.py
.\.venv\Scripts\python.exe scripts\degen_ops_change_manifest.py --summary --json
.\.venv\Scripts\python.exe scripts\degen_ops_completion_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_mvp_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_scope_audit.py --json
.\.venv\Scripts\python.exe scripts\degen_ops_launch_checklist.py --audience partner --client hermes --json
.\.venv\Scripts\python.exe scripts\degen_ops_topology_plan.py --audience partner --client hermes --json
.\.venv\Scripts\python.exe scripts\degen_ops_green_pilot_packet.py --audience partner --client hermes
.\.venv\Scripts\python.exe scripts\degen_ops_rollout_gate.py --json
```

`team_rollout_ready: false` is expected until the live-data access topology and read-only credential are approved.

`goal_complete: false` in `scripts/degen_ops_completion_audit.py --json` is also expected until live team access has been explicitly approved and verified from the approved environment.

The topology planner is the pre-approval decision aid. It recommends Green-hosted first pilot access by default unless direct read-only DB access has already been approved and verified.

The launch checklist aggregates readiness, scope, the exact approval phrase, and the post-approval Green command block. It is still pre-approval evidence only; it does not make the team rollout approved.

The Green-hosted pilot packet is the approval target. It states the exact `proceed` phrase, session boundaries, rollback, and verification commands without creating credentials or editing production.
The command above renders the packet from this Windows checkout, but the packet's verification block is Bash-oriented for the approved Green/Brev shell under `/opt/degen/app`. Do not run the local PowerShell snippets as the Green execution plan.

The change manifest is read-only. Use summary mode for review, then run without `--summary` only when you need the full intended file list for explicit staging.

Generate the live-data approval packet before choosing a team access topology:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_approval_packet.py --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The packet lists Option A, Option B, Option C, rollback, and the exact verification commands. Do not implement a chosen topology until Jeffrey says `proceed`.

Run the reusable smoke script:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both
```

Add `--read-check` when the configured `DATABASE_URL` should be reachable and safe to query:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both --read-check
```

Run the lightweight answer-quality eval against any canned partner or employee answer examples before using them as team guidance:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_answer_eval.py --answers-file .\docs\ops\degen-ops-answer-examples.json --json
```

To test a live read-only database URL without editing Hermes or Codex config, set an environment variable and pass it as a temporary override:

```powershell
$env:DEGEN_OPS_READONLY_DATABASE_URL = "postgresql+psycopg://..."
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_smoke.py --config both --scope employee --read-check --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The smoke script redacts database credentials from error output. A failed `--read-check` means the chat shell may have the right tools but cannot answer from the configured data source.

Once a read-only live-data path is chosen, verify the actual tool reads for the intended scope:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_live_data.py --scope employee --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
.\.venv\Scripts\python.exe scripts\degen_ops_live_data.py --scope partner --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

The live-data verifier does not print the raw database URL. Employee scope must only verify manifest, inventory, and channel velocity.

Before asking the LLM to answer a real buy question, run the no-LLM buy-decision pilot demo for owner or partner scope:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_pilot_demo.py --scope partner --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

For partner scope, this exercises finance, inventory, velocity, redacted buy evaluation, and partner-update drafting through the read-only harness without making an LLM API call. Owner scope also exercises raw cash and loan/payback snapshots.

To run the combined gate after a read-only DB URL is available:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_rollout_gate.py --run-live --run-pilot --database-url-env DEGEN_OPS_READONLY_DATABASE_URL --json
```

Live DB evidence from Green/Brev, checked read-only on June 4, 2026:

```text
APP_HEAD=3da079c
WEB_ACTIVE=active
WORKER_ACTIVE=active
DATABASE_URL_SOURCE=/opt/degen/web.env
READ_ONLY_CONNECT_OK database=degen_green_prod user=degen_green
```

That proves Green can reach live data from its production environment. It does not prove employee laptops can reach the live database directly.

Generate scoped config snippets for another machine instead of copying Jeffrey's owner config:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_config.py --client hermes --scope employee --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
.\.venv\Scripts\python.exe scripts\degen_ops_mcp_config.py --client codex --scope partner --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

For employee machines, generate and install only the `employee` scope.

Generate a fuller scoped access package with guardrails, expected tools, config commands, preflight, and chat commands:

```powershell
.\.venv\Scripts\python.exe scripts\degen_ops_team_package.py --scope employee --client hermes --database-url-env DEGEN_OPS_READONLY_DATABASE_URL
```

The access package references the database through an environment variable. It should not contain a raw database URL or owner-scope config when generated for employees.

Expected scoped discovery:

```text
owner: 8 tools
partner: 6 tools
employee: 3 tools
```

Partner scope must not expose:

```text
get_cash_snapshot
get_loan_and_payback_snapshot
```

Employee scope must only expose:

```text
get_ops_agent_manifest
get_inventory_snapshot
get_channel_velocity
```

## Current Limitations

- Loan repayment is a planning model from observed loan/payback rows, not a formal amortization ledger.
- Cash comes from latest bank rows with balances when available; missing balance feeds produce incomplete cash snapshots.
- Channel velocity depends on line-item/title quality in TikTok, Shopify, and transaction rows.
- Employee/partner identity is enforced by which Hermes server config is used, not by per-user auth inside the MCP server.
