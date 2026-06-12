# Degen Ops Agent — Product PRD and Implementation Plan

Status: DRAFT — open questions 1–5 resolved by Jeffrey 2026-06-11; pending final sign-off
before implementation.
Date: 2026-06-11
Supersedes nothing. Builds on `docs/ops/degen-ops-team-rollout-prd.md` (access topology) and
`docs/ops/degen-ops-discord-partner-bot.md` (Discord service shape). Those stay valid; this PRD
covers the product surface and the remaining build-out.

---

## 1. PRD

### Problem

Owners, partners, and eventually employees need plain-English answers to business questions
("how much did we make today", "should we buy this lot", "has X clocked out") backed by real
system evidence, without anyone touching raw SQL, dashboards they don't have access to, or
owner-only financial data. Today the answers live across `/reports`, `/finance`, `/tiktok/*`,
`/inventory`, `/bookkeeping/bank`, and `/team`, and only Jeffrey can stitch them together.

### Current state (what already exists — do not rebuild)

| Asset | What it does |
|---|---|
| `app/ops_mcp.py` (`DegenOpsMcpHarness`) | Read-only tool harness over existing models/reporting. Sessions forced read-only (`SET TRANSACTION READ ONLY` / `PRAGMA query_only`). |
| Existing tools | `get_finance_snapshot`, `get_cash_snapshot`, `get_inventory_snapshot`, `get_channel_velocity`, `get_loan_and_payback_snapshot`, `evaluate_inventory_buy`, `generate_partner_update`, manifest, plus 7 TikTok tools (orders, products, buyer insights, product performance, live snapshot, status). |
| Scopes | `owner` (15 tools), `partner` (6), `employee` (3), `tiktok` (8). Unknown/blank scope fails safe to `employee`. Partner scope redacts raw cash, balances, reserve gaps, loan/payback. |
| `scripts/degen_ops_discord_bot.py` | Discord bot: channel + user allowlists, rate limit, prompt cap, JSONL audit log, credential redaction in logs, owner-gated partner-channel setup with confirmation phrase. **Hardcoded to partner scope.** |
| `scripts/degen_ops_chat.py` | No-GUI terminal chat over the same harness (owner/partner testing). |
| Gate/audit suite | `degen_ops_local_gate.py`, `degen_ops_readiness.py`, `degen_ops_scope_audit.py`, `degen_ops_rollout_gate.py`, `degen_ops_mcp_smoke.py`, `degen_ops_answer_eval.py`, `degen_ops_live_data.py`, config/package generators, launch checklist, topology planner. |
| Agent prompt | `docs/ops/degen-ops-agent-instructions.md`. |
| Live-data proof | Green/Brev reaches the prod DB read-only (`degen_green` user, verified 2026-06-04). Employee laptops do not. |
| Data models | `Transaction`/`TransactionItem` (Discord deals), `TikTokOrder`/`TikTokProduct`, `ShopifyOrder`, `InventoryItem`, `BankTransaction`, ledger/bookkeeping, `EmployeeProfile` (PII-encrypted), `ClockifyTimeEntry`, `TimecardApproval`, `SupplyRequest`, stream schedules. |

### Gap summary (what this PRD actually builds)

1. **Per-user scope in Discord.** The bot serves one scope per process (partner). Owners asking in
   Discord get partner-redacted answers. Need scope resolution per (user, channel).
2. **Sales question coverage.** No cross-channel `sales_summary` ("how much today across TikTok /
   Shopify / Discord / shows"), no product-level Shopify sales, no Discord deal summary tool, no
   profit/expense breakdown tool. TikTok product performance exists but isn't filterable by
   product name for questions like "Pokemon 151 packs in the last 7 days".
3. **Employee/timecard/task answers.** No tools over `ClockifyTimeEntry`, `TimecardApproval`,
   `SupplyRequest`, schedules.
4. **Durable audit.** Audit is a JSONL file on the bot host. No DB-backed who-asked-what trail
   queryable from the app.
5. **Proactive reporting.** `generate_partner_update` exists but nothing schedules/posts it.

### Users and roles

| Role | Who | Surface |
|---|---|---|
| Owner | Jeffrey + co-owners | Discord owner channel, local Hermes/Codex MCP, terminal chat |
| Partner | Business partners (capital/inventory partners) | Private per-partner Discord channels |
| Manager (new, Phase 3) | Shift leads who may see team clock/task status | Discord staff channel |
| Employee | Streamers/staff | Discord staff channel (later) |
| Admin | Whoever operates the bot service | Green/Brev, env config |

### Success criteria

- Owners get correct, evidence-cited answers to the core question set (sales by channel/day,
  product sales, cash, profit, loan posture, buy decisions) in Discord within ~30s.
- Partner answers never contain raw cash balances, bank balances, loan/payback dollars,
  payroll/PII, or other partners' data — verified by tests, not by prompt hopes.
- Employees can ask inventory/velocity/own-schedule questions and nothing else.
- Every answer ends with an evidence block (tools called, date ranges, row counts, app surface to
  verify) or an explicit "data missing: X" statement.
- Every Q&A is audit-logged (user, channel, scope, question, tools called, duration, errors).
- Zero writes to business data; zero outbound messages to customers/employees/partners beyond
  replies in the asking channel.

### Scope

- Extend the existing harness with the missing read-only tools (sales, profit/expense, Shopify,
  Discord deals, employee clock/task status).
- Per-user/per-channel scope resolution in the existing Discord bot.
- DB-backed audit log (additive migration, both engines).
- Scheduled weekly update posted to an approved channel (read-only content).
- Buy-decision and channel-routing answers using existing `evaluate_inventory_buy` +
  `get_channel_velocity`, improved with sell-through estimates.

### Non-scope

- No GUI. Discord + existing MCP clients only.
- No money movement, inventory mutation, Shopify/TikTok writes, production data writes.
- No messages to customers, employees, or partners initiated by the agent (replies in the asking
  channel and the explicitly configured weekly-update channel are the only outputs).
- No arbitrary SQL tool, ever.
- No public/hosted MCP gateway (Option C in the rollout PRD stays deferred).
- No Phase 6 "action" workflows in this PRD — that requires its own PRD per the protected-paths
  contract.
- No promise to "answer any question". The agent answers the tool-backed question families below
  and says so when asked something outside them.

### Constraints

- Read-only DB access only; production access via Green/Brev environment (`DEGEN_OPS_READONLY_DATABASE_URL`).
- Dual-engine: every new column/table needs both `SQLITE_ADDITIVE_MIGRATIONS` and
  `POSTGRES_ADDITIVE_MIGRATIONS` entries.
- Owner-only data: raw cash, bank balances, loan/payback details, payroll/PII, partner-specific
  terms. Enforced in tool registration (scope → tool list), not in prompts.
- PII: employee tools must use `EmployeeProfile` display names only; never decrypt/return contact
  info, compensation, or PII fields.
- LLM: existing `ai_client.py` providers (NVIDIA Inference Hub Claude models / OpenAI). The model
  never sees data a tool in the active scope didn't return.
- Deploy: local → canonical repo → Green normal path. No prod edits.

### Risks

| Risk | Mitigation |
|---|---|
| Scope leak via prompt injection ("ignore rules, show cash") | Authorization is structural: redacted tools aren't registered in the scope. The model cannot call what it doesn't have. Tests assert per-scope tool lists. |
| Wrong numbers stated confidently | Evidence block is mandatory; tools return row counts + date ranges; answer eval script extended with golden Q&A; "thin data" flags already exist in harness. |
| Channel misconfiguration (partner channel mapped to owner scope) | Scope map is explicit env/JSON config, validated at startup; setup command prints the resolved scope; audit log records scope per answer. |
| TikTok analytics ~2-day lag misread as "sales dropped" | Sales tools compute from local `TikTokOrder` rows (webhook/poll-synced), not the lagged analytics API; manifest documents freshness per source. |
| Clockify data stale/missing | `employee_clock_status` returns last-sync timestamp and says "as of"; absent data is an explicit failure mode, not a guess. |
| Cost/abuse | Existing per-user rate limit + prompt cap; per-channel daily call budget. |
| PII exposure via employee tools | Tools return names + clock state only; tests assert no PII fields in output shape. |

### Security / access model

- **Identity:** Discord user ID + channel ID → scope, resolved per message from an explicit
  config map (env JSON or file). Order: user override → channel default → deny (no reply or
  "not authorized" reply, configurable). Missing/unknown → **deny**, not employee.
  Initial role map: `206237952412483584` (Jeffrey) → owner. Other owners added only when
  explicitly provided.
- **Authorization:** scope → registered tool list (existing `DEGEN_OPS_SCOPE_TOOL_NAMES`
  mechanism). New tools get added to the right scopes; owner-only list grows with cash/loan/
  payroll-adjacent tools.
- **Data redaction:** partner-scope variants of shared tools keep the existing redaction note and
  strip owner-only fields server-side (already the pattern in `evaluate_inventory_buy`).
- **Audit:** every interaction → JSONL (existing) + new `OpsAuditLog` DB table (user id, channel
  id, resolved scope, question hash + truncated text, tool calls with args summary, outcome,
  latency). Owner-viewable; consider surfacing on `/admin` later (non-scope here).
- **Secrets:** existing redaction helpers (`sanitize_for_log`) reused; no DB URLs or keys in
  answers or logs.

### Data sources (existing only — no new pipelines)

| Question family | Source |
|---|---|
| TikTok sales/orders/products/live | `TikTokOrder`, `TikTokProduct`, `app/reporting.py` helpers (paid-like statuses per established semantics) |
| Shopify sales | `ShopifyOrder` |
| Discord deals / show sales | `Transaction`/`TransactionItem` (normalized layer; raw `DiscordMessage` untouched) |
| Profit/expense/revenue | `app/reporting.py` + `app/financials.py` (loan_proceeds stays non-operating) |
| Cash/bank | `BankTransaction` latest balances (owner only) |
| Loans/paybacks | loan/payback rows (owner only) |
| Inventory | `InventoryItem`, `PriceHistory` |
| Clock status | `ClockifyTimeEntry` (+ sync freshness) |
| Tasks | `SupplyRequest`, staff buylist, `TimecardApproval` — i.e., only tasks tracked in the app |
| Schedules | stream-manager/team schedule models |

### Tool/harness architecture

Unchanged from the pilot: LLM (Discord bot or MCP client) ↔ `DegenOpsMcpHarness` ↔ existing
models/reporting helpers, with scope decided before the LLM sees any tools. The Discord bot
gains a scope resolver and a tool-call recorder; the harness gains new tools. No new services
besides the already-specced `degen-ops-discord-bot.service`.

### Discord UX

See section 5. Summary: mention-or-DM-free natural language in allowlisted channels, threaded
replies with an evidence footer, follow-ups via short per-channel conversation memory, explicit
owner-gated admin commands for partner-channel setup and weekly-update scheduling.

### Verification plan

- Unit tests per new tool (scope registration, output shape, redaction, empty-data behavior).
- Extend `degen_ops_scope_audit.py` expectations for the new per-scope tool counts.
- Golden Q&A set in `degen_ops_answer_eval.py` covering each example question, including ones the
  agent must refuse or partially answer.
- Bot tests: scope resolution matrix (owner user in partner channel, unknown user, unknown
  channel), audit rows written, rate limits.
- `compileall` + full pytest before every commit (baseline 826 pass / 8 known mobile-only fails).
- Pre-rollout: existing `degen_ops_rollout_gate.py --run-live --run-pilot` from Green.

### Rollback plan

- Bot: stop/disable `degen-ops-discord-bot.service`, blank `DEGEN_OPS_DISCORD_BOT_TOKEN`. No data
  rollback needed (read-only).
- New tools: revert the commit; tools are additive and unreferenced elsewhere.
- `OpsAuditLog` table: additive, ignorable; drop only with explicit approval.
- Weekly update: remove the schedule config; no state.

### Resolved decisions (Jeffrey, 2026-06-11)

1. **Owner Discord IDs:** Jeffrey = `206237952412483584`. Add other owners only when explicitly
   provided. Owner channel ID still needs to be confirmed in Phase 0.
2. **Task source:** MVP answers app-tracked work only (`SupplyRequest`, staff buylist,
   `TimecardApproval`). Linear read-only integration is a later phase, only if tasks actually
   live there.
3. **Clock visibility:** owner/admin see all employees; manager sees assigned/team employees
   only (once manager scope exists); employee sees self only.
4. **Weekly update:** manual-first, not scheduled. Triggered by asking for a "weekly update"
   in an owner/partner-approved channel; the bot posts only in approved channels. Scheduling is
   deferred until the manual flow proves out.
5. **Reserve floor:** configured via `DEGEN_OPS_MIN_CASH_RESERVE_USD`; the model never invents
   one. Until set, buy recommendations may say "safe/risky based on available evidence, but
   reserve floor is not configured" — they must not claim reserve safety.

### Remaining open questions

1. Which Discord channel is the owner channel (ID needed for the scope map)?
2. Manager → team assignment source: schedule/stream-account assignments, or an explicit
   config map? Needed before Phase 3 manager scope ships.

---

## 2. Phased implementation plan

**Phase 0 — Inventory & confirm (no code).**
Verify current bot behavior on Green (service exists? running?), confirm owner/partner Discord
IDs and channels, re-run `degen_ops_local_gate.py` + `degen_ops_scope_audit.py`, confirm
`ShopifyOrder` and `ClockifyTimeEntry` data freshness in prod, answer open questions 1–4.
Exit: written config map of users/channels/scopes; gaps confirmed.

**Phase 1 — Read-only owner Q&A in Discord.**
Per-user scope resolution (owner scope for owner IDs in the owner channel), `sales_summary`,
`tiktok_product_sales` filterable by product text, `profit_summary`/`expense_summary`,
`discord_deal_summary`, `OpsAuditLog` table. Owner can ask every money/sales example question.
Exit: golden Q&A passes for owner; audit rows visible; partner channels unaffected.

**Phase 2 — Partner-safe Q&A.**
Register the partner-safe subset of the new tools (sales/product/velocity/buy-eval; no cash,
bank, loan, payroll, profit-with-cash-context). Redaction tests. Partner pilot in one channel
using the existing launch checklist / green pilot packet flow.
Exit: scope audit shows partner cannot reach owner-only tools; partner golden Q&A passes,
including refusals.

**Phase 3 — Employee/timecard/task Q&A.**
`employee_clock_status`, `employee_task_status`, schedule lookup; new `manager` scope; employee
scope gets self-only clock status. PII-shape tests.
Exit: manager can ask "has X clocked out"; employee asking about a peer is refused.

**Phase 4 — Buy-decision workflow.**
Improve `evaluate_inventory_buy` with sell-through estimate (`how fast can we sell this lot`)
derived from `get_channel_velocity` history; add `channel_routing_recommendation` as a thin
wrapper that turns velocity into a recommendation with stated assumptions; owner version includes
cash-reserve check, partner version stays redacted.
Exit: the three buy example questions produce decision + assumptions + evidence; payback/budget
question answers only when the owner-configured reserve floor exists.

**Phase 5 — Weekly update (manual-first).**
"Weekly update" asked in an approved channel calls `weekly_business_update` and posts the
scope-appropriate version in that channel only (owner version in owner channel, partner version
in partner channels; unapproved channels refused). No scheduler in this phase — scheduling is a
follow-up only after the manual flow proves out.
Exit: on-demand owner and partner posts verified; refusal in unapproved channel verified;
content passes answer eval.

**Phase 3b — Discord DM access (with Phase 3 rollout).**
Allow DMs to the bot for explicitly granted users. DM = message with no guild; effective scope =
the sender's mapped role (no channel min), unknown sender = silent deny. Gated by
`DEGEN_OPS_DISCORD_ALLOW_DMS` plus a per-user DM-allowed flag in the role map, so DM access is
granted person-by-person. Audit logging unchanged (DM channel IDs are recorded like any channel).
Exit: granted owner/manager/employee can DM and get scope-correct answers; ungranted mapped user
and unknown user are both denied; audit rows written.

**Future surface — iMessage via Hermes (not scheduled).**
The harness is a client-agnostic stdio MCP server, so an MCP-aware client with an iMessage
bridge (Hermes) can front the same scoped tools with no new Degen code. Blocked on three
decisions before any planning: (1) verify Hermes' actual iMessage capability and whether it can
scope per sender (phone number/Apple ID → role, deny-by-default) — if not, owner-only or
one-number-per-scope are the only safe shapes; (2) iMessage requires an always-on signed-in Mac
host (no Apple API) — new infrastructure to own; (3) audit parity — the Discord audit path
doesn't cover Hermes sessions, so either Hermes-side logging is accepted or a logging shim wraps
the MCP server. Start owner-only (Jeffrey's phone, owner scope) if pursued.

**Phase 6 — Approved action workflows (NOT in this PRD).**
Anything that writes (draft Shopify listing, ping an employee, create a Linear issue) needs a
separate PRD, explicit approval gates, and per the operating contract starts with
propose-and-confirm, never autonomous. Do not start without sign-off.

---

## 3. Tool catalog

Legend: **EXISTS** = already in `app/ops_mcp.py`; **NEW** = to build. Access: O=owner,
P=partner, M=manager, E=employee. All tools are read-only and return an `evidence` object
(date range, row counts, source tables/surfaces, freshness timestamp) plus `data_gaps: []`.

### Existing (keep, possibly extend)

| Tool | Access | Notes |
|---|---|---|
| `get_ops_agent_manifest` | O P M E | Capability + guardrail manifest |
| `get_finance_snapshot(days)` | O P | Revenue/finance rollup; partner-redacted |
| `get_cash_snapshot()` | O | Latest bank balances; incomplete-feed flagging exists |
| `get_inventory_snapshot()` | O P M E | Counts/value by status/category |
| `get_channel_velocity(days, category)` | O P M E | Per-channel sell-through |
| `get_loan_and_payback_snapshot(days)` | O | Planning model, not amortization ledger |
| `evaluate_inventory_buy(scenario, days)` | O P(redacted) | Buy decision w/ margin + payback |
| `generate_partner_update(scenario, days)` | O P | Partner-safe update draft |
| TikTok suite (7 tools) | O (+`tiktok` scope) | orders, products, buyers, product perf, live snapshot, status |

### New tools

**`sales_summary`** — NEW
- Purpose: "How much did we make today / this week" across channels.
- Inputs: `date_range` (start/end or preset like `today`, `last_7d`), `channels` (subset of
  tiktok/shopify/discord/show), `timezone` (default business TZ).
- Output: per-channel `{gross, orders, items, refunds}` + total; per-day buckets when range > 1d.
- Source: `TikTokOrder` (paid-like statuses, `subtotal_price` GMV rule), `ShopifyOrder`,
  `Transaction` (sells; show sales = Discord-source transactions per existing reporting rules).
- Access: O, P (P gets revenue, not cash framing). Evidence: row counts per channel, status
  filter used, sync freshness. Failure modes: channel table empty → named in `data_gaps`;
  ambiguous "made" (revenue vs profit) → tool returns revenue, agent prompt says to ask which.

**`tiktok_product_sales`** — NEW (thin layer over `build_tiktok_product_performance` + order line scan)
- Purpose: "How many Pokemon 151 packs sold in last 7 days on TikTok."
- Inputs: `product_query` (text matched against product title/SKU), `days`, `limit`.
- Output: matched products with qty, revenue, order count, per-day sparkline data.
- Source: `TikTokOrder` line items + `TikTokProduct`. Access: O, P, M, E.
- Evidence: match terms, matched product IDs/titles, date range. Failure modes: no title match
  (returns nearest candidates + says no exact match); title quality issues (known limitation).

**`shopify_product_sales`** — NEW
- Same shape as above over `ShopifyOrder` line items. Access: O, P, M, E.
- Failure: SKU-only rows without titles → reported as such.

**`discord_deal_summary`** — NEW
- Purpose: buy/sell/trade activity from parsed deals ("what did we buy this week", show sales).
- Inputs: `date_range`, `kind` (buy/sell/trade/all), `channel` (optional).
- Output: counts + dollar totals by kind, top items, per-day buckets.
- Source: `Transaction`/`TransactionItem` only (never raw `DiscordMessage`). Access: O; P gets
  aggregates only (no counterparty/channel detail). Evidence: transaction row counts, channels
  included, review-queue caveat (rows pending review are flagged, not silently included or
  dropped — count reported separately). Failure: unparsed/review backlog → stated.

**`profit_summary`** — NEW
- Purpose: "How much money have we *made*" when the user means profit.
- Inputs: `date_range`. Output: revenue, COGS/inventory expense, opex, net; `loan_proceeds`
  excluded from operating revenue (hard rule).
- Source: `app/reporting.py` / `financials.py` rollups. Access: O only (P gets
  `get_finance_snapshot` instead). Evidence: category totals + rule notes. Failure:
  unreconciled bank rows → stated margin of error.

**`expense_summary`** — NEW
- Inputs: `date_range`, `category`. Output: expenses by category, top vendors/descriptors.
- Source: ledger/bookkeeping + `BankTransaction` categories. Access: O. Failure: uncategorized
  rows count surfaced.

**`loan_repayment_model`** — EXTEND `get_loan_and_payback_snapshot`
- Purpose: "What weekly payback keeps us safe."
- Inputs: `weekly_payment` (optional). Reserve floor read from `DEGEN_OPS_MIN_CASH_RESERVE_USD`
  config, never from the model or the asker.
- Output: payoff timeline scenarios vs trailing cash-flow, reserve breach warnings.
- Access: O only. Evidence: loan rows used, trailing inflow/outflow window. Failure: reserve
  floor unset → returns scenarios with the explicit caveat "safe/risky based on available
  evidence, but reserve floor is not configured"; never claims reserve safety.

**`employee_clock_status`** — NEW
- Inputs: `employee_name_or_id` (optional; omitted = currently clocked-in list), `date`.
- Output: clock-in/out events, current state, hours today/this week; display name only.
- Source: `ClockifyTimeEntry` + sync freshness. Access: O/admin all employees; M assigned/team
  employees only; E self-only (both enforced by binding the tool variant to the asker's mapped
  employee id / team list, not by prompt).
- Evidence: last Clockify sync time, entry ids/count. Failure: stale sync → "as of" warning;
  unknown name → candidate list.

**`employee_task_status`** — NEW (narrow, app-tracked work only)
- Inputs: `employee`, `date_range`. Output: supply requests, buylist submissions, timecard
  approvals with statuses.
- Source: `SupplyRequest`, staff buylist, `TimecardApproval` — app-tracked work only (decided
  2026-06-11). Access: O, M (team); E self-only.
- Failure: question about untracked work → explicit "the app does not track that task". Linear
  read-only integration is a later phase, only if tasks actually live there.

**`buy_lot_analysis`** — EXTEND `evaluate_inventory_buy`
- Add: sell-through estimate (days/weeks to clear at current per-channel velocity for matching
  category), confidence band, channel mix suggestion. Owner adds cash-reserve check against
  `DEGEN_OPS_MIN_CASH_RESERVE_USD`; when unset, the verdict carries "reserve floor is not
  configured" instead of a reserve-safety claim.
- Access: O, P(redacted). Failure: thin category history → wide band + low-confidence flag
  (pattern already exists in harness).

**`channel_routing_recommendation`** — NEW (thin wrapper)
- Inputs: `category`/`product_query`, `quantity`, `target_days` (optional).
- Output: recommended channel split with velocity evidence and stated assumptions.
- Source: `get_channel_velocity` + product sales tools. Access: O, P, M, E. Failure: no category
  history → says so, no recommendation invented.

**`weekly_business_update`** — EXTEND `generate_partner_update`
- Add owner variant (includes cash/loan posture) and per-channel sales week-over-week from
  `sales_summary`. Access: O (full), P (existing redacted form).

---

## 4. Access-control design

Resolution: explicit config map `{discord_user_id → role}` + `{channel_id → max_scope}`;
effective scope = min(user role, channel max). Unknown user or channel → **deny**. No
"allow any user" in channels mapped above employee scope.

| Capability | Owner | Partner | Manager | Employee |
|---|---|---|---|---|
| Sales by channel/product/day | ✅ | ✅ | ✅ | ✅ |
| Inventory snapshot / velocity / routing | ✅ | ✅ | ✅ | ✅ |
| Finance snapshot (redacted) | ✅ | ✅ | ❌ | ❌ |
| Profit / expense detail | ✅ | ❌ | ❌ | ❌ |
| Raw cash / bank balances | ✅ | ❌ | ❌ | ❌ |
| Loan / payback / repayment model | ✅ | ❌ | ❌ | ❌ |
| Buy evaluation | ✅ full | ✅ redacted | ❌ | ❌ |
| Clock status — others | ✅ all | ❌ | ✅ team only | ❌ |
| Clock status — self | ✅ | ❌ | ✅ | ✅ |
| Task status (app-tracked) | ✅ | ❌ | ✅ team | ✅ self |
| Payroll / compensation / PII | ❌ exposed to no scope via this agent | ❌ | ❌ | ❌ |
| Weekly update | full | partner version | ❌ | ❌ |
| Partner-channel setup commands | ✅ (confirmation phrase) | ❌ | ❌ | ❌ |

Examples:

- Partner: "What's our cash balance?" → "Cash and bank balances are owner-only. I can share the
  finance snapshot: revenue last 30d $X across N orders (evidence: …)."
- Partner: "What did Partner B's lot sell for?" → refusal; partner channels only see their own
  scenario data plus business-wide aggregates.
- Employee: "Has Mike clocked out?" → "I can only show your own timecard. Ask a manager or
  owner for team status."
- Manager: "Has Mike clocked out?" → "Mike clocked in 9:04 AM, no clock-out yet; 5.2h today
  (Clockify, synced 12 min ago, entry #…)."
- Owner: "Should we buy this $2k lot?" → full margin + payback + cash-reserve check + sell-through
  estimate with evidence.
- Anyone: "What's Jeffrey's home address?" → no tool returns PII; refusal.

---

## 5. Discord command / chat design

Conversational-first: plain messages in allowlisted channels (existing behavior). Replies in a
thread to keep channels clean. Every answer ends with an evidence footer:

> 📎 Evidence: tiktok_product_sales (last 7d, 3 products matched, 41 orders) · synced 14 min ago
> · verify at /tiktok/clients

Natural language:
- "How many Pokemon 151 packs did we sell on TikTok in the last 7 days?"
- "How much money did we make today?" → agent answers revenue and asks "revenue or profit?" if
  the asker is owner-scope.
- "What were today's TikTok, Shopify, Discord, and show sales?"
- "Has Mike clocked out?" / "Did Sarah finish her supply requests this week?"
- "Should we buy this lot: $2,000, ~40 sealed packs, Pokemon 151?"
- "How fast can we sell it and where should it go?"

Follow-ups: short per-channel/thread memory (last N exchanges) so "what about Shopify?" after a
TikTok question reuses the date range. Memory is conversation context only — never widens scope.

Admin/owner commands (prefix `!ops`, owner-gated, existing confirmation-phrase pattern):
- `!ops setup-partner <name> <@user> <channel-slug>` — existing flow; prints plan, requires
  confirmation phrase.
- "weekly update" (natural language, any approved channel) — posts the scope-appropriate
  weekly update in that channel; refused in unapproved channels. No scheduler yet (decided
  2026-06-11: manual-first).
- `!ops whoami` — show the asker's resolved role/scope (debugging misconfig).
- `!ops audit <n>` — owner-only: last n audit entries summary.

---

## 6. Engineering plan (small, reviewable PRs)

Run before every commit: `.\.venv\Scripts\python.exe -m compileall app` + full pytest.

**PR1 — Discord scope resolution.** `scripts/degen_ops_discord_bot.py`: config map
(`DEGEN_OPS_DISCORD_ROLE_MAP` JSON or file path), resolver `resolve_scope(user_id, channel_id)`,
deny-by-default, `!ops whoami`. Tests: matrix (owner/partner/unknown user × owner/partner/unknown
channel), deny default, partner channel can never yield owner scope even for owner-listed users
unless channel max allows it.

**PR2 — `sales_summary` tool.** New harness method + scope registration (owner+partner).
Tests: per-channel totals against seeded SQLite fixtures, paid-like TikTok status filter,
`subtotal_price` GMV rule, timezone bucketing, empty-channel `data_gaps`, partner registration.

**PR3 — `tiktok_product_sales` + `shopify_product_sales`.** Text match over line items.
Tests: "151" matches titled products, no-match returns candidates not zeros, day buckets,
all-scope registration, evidence fields present.

**PR4 — `discord_deal_summary` + `profit_summary` + `expense_summary`.** Reuse
`reporting.py`/`financials.py`. Tests: loan_proceeds excluded from operating revenue,
review-pending rows counted separately, partner gets aggregate-only variant, owner-only
registration for profit/expense.

**PR5 — `OpsAuditLog`.** Model + both additive-migration dicts + bot write path + `!ops audit`.
Tests: row written per Q&A with scope/tools/latency, secrets redacted, SQLite + (mocked)
Postgres migration entries exist (extend the existing dual-migration test pattern).

**PR6 — `employee_clock_status` + `manager` scope.** Harness tool over `ClockifyTimeEntry`;
new scope in `DEGEN_OPS_SCOPE_TOOL_NAMES`; employee self-binding via role-map employee id.
Tests: no PII fields in output shape, stale-sync warning, self-only enforcement for employee
scope, scope audit counts updated.

**PR7 — `employee_task_status`.** Over `SupplyRequest`/buylist/`TimecardApproval`. Tests:
untracked-task refusal message, manager-vs-employee visibility.

**PR8 — `buy_lot_analysis` sell-through + `channel_routing_recommendation`.** Extend
`evaluate_inventory_buy`; wrapper tool. Tests: thin-history low-confidence flag, partner
redaction preserved, routing refuses without category history.

**PR9 — Manual weekly update + reserve floor.** `weekly_business_update` owner variant; "weekly
update" trigger in the bot, gated to approved channels (`DEGEN_OPS_WEEKLY_UPDATE_CHANNEL_IDS`);
`DEGEN_OPS_MIN_CASH_RESERVE_USD` wiring into `loan_repayment_model` and `buy_lot_analysis`.
Tests: no reserve-safety claim when the floor is unset (caveat string present); owner vs partner
content difference; refusal in unapproved channels; no scheduler code paths.

**PR10 — Golden Q&A eval expansion.** Extend `degen_ops_answer_eval.py` answers file with the
example-question set including required refusals; update `degen_ops_scope_audit.py` expected
tool counts; run `degen_ops_rollout_gate.py`.

Order: PR1–PR5 = Phase 1; PR6–PR7 = Phase 3; PR8 = Phase 4; PR9 = Phase 5. Phase 2 is config +
pilot using PR1–PR4 output, no new code beyond registration lists.
