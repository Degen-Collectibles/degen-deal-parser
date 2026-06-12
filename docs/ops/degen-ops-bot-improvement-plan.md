# Degen Ops Bot Improvement Plan

## Goal

Turn the Degen Ops Discord bot into a useful read-only operator assistant for owners, partners, and employees while keeping the protected boundaries intact: no money movement, no inventory mutation, no listings mutation, no customer or partner messages without approval, and evidence behind every recommendation.

## Current State

- Discord bot exists and answers through the Degen Ops chat harness.
- Tool-backed reads exist for finance, inventory, channel velocity, cross-channel sales summary, TikTok product sales, TikTok top products, Shopify product sales, Shopify top products, internal price lookup, market trends, owner-only employee clock status, and public web search.
- Scoping exists for owner, partner, employee, and TikTok contexts.
- Audit logging exists as JSONL on the bot host.
- The bot previously did not include Discord conversation context, so follow-ups like "No, I mean on TikTok" were ambiguous.

## Rollout Plan

### Phase 0: Stabilize The Current Bot

**Target time:** 0.5-1 day
**Local implementation status:** Implemented locally; needs commit, deploy, and bot restart.

**Deliverables:**
- Deploy short-term Discord context memory.
- Deploy public read-only web search.
- Smoke test core prompts in the live Discord channel.
- Confirm role/scope mapping for owner vs partner vs employee channels.
- Expand answer-quality eval for core owner/partner/employee prompts.

**Verification prompts:**
- "Top 5 selling products"
- "No, I mean on TikTok"
- "How many 151 packs sold on TikTok in the last 7 days?"
- "What's the market price of Pokemon 151 booster packs?"
- "Has Alex clocked out?"
- "How much money have we made today?"
- "Draft this week's partner update."

**Risk:** Low. Ephemeral context only; no durable memory or writes.

### Phase 1: Better Sales Intelligence

**Target time:** 2-4 days
**Local implementation status:** Core MVP implemented locally for TikTok top products, Shopify product sales, Shopify top products, cross-channel sales summary, and Discord/show sales drilldown by keyword/category/channel text. Remaining work is richer show-event labeling where the source data has explicit show metadata.

**Deliverables:**
- Cross-channel sales summary: TikTok, Shopify, Discord, shows.
- Shopify product sales lookup by title, SKU, and date range.
- "Top products on TikTok" and "Top products on Shopify" dedicated tools.
- Discord/show deal summary by category/product keyword.

**Example prompts:**
- "What are our top 10 TikTok products this week?"
- "How many 151 packs did Shopify sell this month?"
- "Compare TikTok vs Shopify for sealed Pokemon."

**Risk:** Medium. Revenue definitions must stay clear: GMV, paid revenue, payout cash, and profit are different.

### Phase 2: Market Pricing Upgrade

**Target time:** 3-6 days
**Local implementation status:** Partial implementation locally. Price lookup now combines inventory/list price, stored price history, TikTok sale comps, Shopify sale comps, and Discord/show sale comps. Market trend lookup now compares cross-channel sale prices across TikTok, Shopify, and Discord/show rows, then falls back to TikTok-only and stored price history. Remaining work is richer external single-card/slab comp integrations and stronger card-query parsing.

**Deliverables:**
- External market price lookup through existing card/scanner/pricing providers where possible.
- Better single-card query parsing: card name, set, number, variant, condition, grader, grade.
- Source and freshness labels for every comp.
- Separate sealed-product, raw-single, and slab answer formats.

**Example prompts:**
- "What's the price of Charizard ex 199/165?"
- "What's PSA 10 Moonbreon trending at?"
- "Is 151 booster pack price going up or down?"

**Risk:** Medium-high. External pricing sources can be stale, blocked, noisy, or not comparable. The bot must say when comps are weak.

### Phase 3: Employee Ops Questions

**Target time:** 3-5 days
**Local implementation status:** Owner-only cached Clockify clock-in/out lookup and owner-only employee ops queue status implemented locally. Manager/employee visibility rules and any external task-system integration remain.

**Deliverables:**
- Clockify/timecard read tools.
- Employee clock-in/clock-out status.
- Supply request status.
- Buylist submission status.
- Time-off request status.
- Manager-safe team summary, with employee self-view restrictions if needed.

**Example prompts:**
- "Has Alex clocked out?"
- "Who is currently clocked in?"
- "What supply requests are still open?"

**Risk:** Medium. Employee visibility rules need owner sign-off before partner/employee access.

### Phase 4: Partner-Ready Business Updates

**Target time:** 2-3 days
**Local implementation status:** Read-only weekly partner update draft implemented locally and callable from partner/owner scopes. Scheduling, posting, owner approval UI, and "what changed since last week" remain.

**Deliverables:**
- Scheduled weekly partner update draft.
- Owner approval before posting.
- Channel-specific redaction.
- "What changed since last week?" summary.

**Example prompts:**
- "Draft this week's partner update."
- "What risks should we tell partners about?"
- "Summarize sales and cash safety without raw bank balances."

**Risk:** Medium. Partner updates must avoid exposing owner-only cash and loan details.

### Phase 5: Durable Memory

**Target time:** 3-7 days
**Local implementation status:** DB model plus scoped read tool and owner-only read-only memory proposal tool implemented locally. Actual add/edit/delete memory writes are not implemented yet.

**Deliverables:**
- DB-backed bot memory table for preferences and recurring business assumptions.
- Owner-only commands to view, add, edit, and delete memory.
- Scope-aware retrieval so employee memory cannot expose owner/partner-only facts.
- Staleness labels and audit logs.

**Example memories:**
- "When Jeffrey asks top products without a channel, ask whether he means TikTok, Shopify, Discord, or all channels."
- "Weekly partner update cadence is Monday morning."
- "Default cash reserve floor is owner-configured."

**Risk:** High if built casually. Durable memory can become stale, leak scoped information, or override real tool data. It should never store secrets or raw customer PII.

### Phase 6: Approval-Gated Actions

**Target time:** 1-2 weeks after read-only behavior is trusted

**Deliverables:**
- Draft-only action tools.
- Approval workflow for owner confirmation.
- Separate audit table for proposed actions and approvals.
- Hard blocks for money movement unless explicitly scoped in a separate PRD.

**Example prompts:**
- "Draft a partner update for approval."
- "Prepare a Shopify repricing plan, but don't apply it."
- "Draft a Discord announcement, wait for approval."

**Risk:** High. This should get a separate PRD and security review before implementation.

## Recommended Next Three PRs

1. **Deploy short-term context and web search**
   - Files: `scripts/degen_ops_discord_bot.py`, `app/ops_mcp.py`, `app/ops_chat.py`, tests.
   - Outcome: The live bot can handle follow-up context and public market lookup.

2. **Add TikTok top-products tool**
   - Files: `app/ops_mcp.py`, `app/ops_chat.py`, `tests/test_ops_mcp.py`, `tests/test_degen_ops_chat.py`.
   - Outcome: Direct answer for "top selling products on TikTok" without relying on broad channel velocity.

3. **Add Shopify product sales tool**
   - Files: existing Shopify/order reporting modules first, then ops harness.
   - Outcome: Direct answer for "how many X did we sell on Shopify?"

## Guardrails

- Read-only remains the default.
- Tool data beats Discord context and durable memory.
- Every recommendation cites evidence.
- Unknown or weak data must be stated plainly.
- Partner and employee scopes must not expose owner-only cash, loan, payroll, PII, or raw bank details.
- Web search is public lookup only; no logins, no checkout, no customer messages, no scraping behind protected sites.
