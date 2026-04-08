# AGENTS.md

## Project

Degen Collectibles — Discord deal parser + TikTok Shop livestream platform.

This project has two major verticals:
1. **Discord deal parsing** — ingests Discord deal-log messages, stores raw messages, parses them into structured transactions, normalizes them for financial reporting
2. **TikTok Shop livestream tools** — order sync, live streamer dashboard, analytics, and product management

Current stack:
- Python 3.14, FastAPI, Uvicorn
- discord.py for Discord ingestion
- PostgreSQL (production on Machine B) / SQLite (local dev on Machine A) with SQLModel ORM
- OpenAI API (parser fallback only)
- TikTok Shop Open Platform API (orders, products, live analytics)
- TikSync SDK (live chat WebSocket)
- Jinja2 templates with vanilla JS + Chart.js
- Role-based auth (admin, reviewer, viewer)
## Core Principles (VERY IMPORTANT)

### 1. Source of truth
- `DiscordMessage` is immutable audit log
- All parsing must be reproducible from raw messages

### 2. Determinism first
- Prefer rule-based parsing over AI
- AI is fallback, not primary logic

### 3. No silent failures
- Every failure must be visible via logs or UI

### 4. Do not guess
- If behavior is unclear, inspect and explain before coding

### 5. No broad refactors unless explicitly requested

## Run

**Local dev (web-only, no Discord/worker):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1
```

**Production (Machine B — web + worker + auto-restart):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1
```

**Quick compile check after code changes:**
```powershell
.\.venv\Scripts\python.exe -m compileall app
```

### Main pages

Discord side:
- `/table` — main deal queue
- `/review-table` — review queue
- `/reports` — financial reports
- `/bookkeeping` — sheet import + reconciliation

TikTok side:
- `/tiktok/orders` — order listing with livestream filter
- `/tiktok/streamer` — live streamer dashboard (real-time orders, GMV, chat)
- `/tiktok/analytics` — stream analytics with charts and growth metrics
- `/tiktok/streamer/config` — manual stream time configuration

Admin:
- `/admin/home` — admin dashboard
- `/admin/debug` — system diagnostics
- `/admin/users` — user management

## Core Architecture

Main flow:
1. `discord_ingest.py`
   - listens to Discord
   - stores raw `DiscordMessage` rows
   - can auto-import public Google Sheets bookkeeping links
2. `worker.py`
   - processes queued/failed rows
   - stitches nearby messages into one deal when appropriate
   - calls parser + financial normalization
3. `parser.py`
   - rule-first parsing
   - OpenAI fallback for ambiguous/image-heavy cases
   - store-specific override logic
4. `transactions.py`
   - syncs normalized `Transaction` and `TransactionItem` rows
5. `reporting.py`
   - financial/report summaries
6. `bookkeeping.py`
   - bookkeeping sheet import + reconciliation

Design principle:
- `DiscordMessage` is the raw audit trail
- `Transaction` is the normalized reporting layer
- parser rules should beat AI when store shorthand is explicit

## Important Store Rules

These conventions should be preserved unless the user explicitly asks to change them:

- `out` means items leaving the store
- `in` means items coming into the store
- `top out bottom in` means a trade
- `plus 195 zelle` in a trade usually means money to the store
- `tap` means card
- payment-only logs like `$11 zelle` or `zelle $11` default to a sell unless stronger context says otherwise
- image-first + follow-up payment/message can be one transaction
- unrelated nearby deals should not be stitched together
- card buys/sells/trades should use `expense_category = inventory`

## Current UX / Data Behavior

- main queue is `/table`
- review queue is `/review-table`
- review table supports inline row editing for common fields
- grouped/stiched messages are shown in the table
- child rows in stitched groups should be cleared and marked `ignored`
- filter channel list should only include watched channels or channels with stored messages

## Bookkeeping

Current bookkeeping behavior:
- `/bookkeeping` supports `.csv` and `.xlsx` upload
- public Google Sheets links posted in watched Discord channels should auto-import
- bookkeeping imports are used as ground truth for reconciliation, not direct model training

Important:
- prefer sheet export/import data over screenshot previews
- reconciliation should compare against normalized `Transaction` rows

## Module Ownership Suggestions

When using multiple agents, keep write scopes separate.

Parser / Stitching agent:
- `app/parser.py`
- `app/worker.py`

UI / Review Workflow agent:
- `app/templates/messages_table.html`
- `app/templates/message_detail.html`
- `app/templates/reports.html`
- `app/templates/bookkeeping.html`

Data / Reporting agent:
- `app/models.py`
- `app/transactions.py`
- `app/reporting.py`
- `app/bookkeeping.py`
- `app/financials.py`

Infra / Routing agent:
- `app/main.py`
- `app/channels.py`
- `app/discord_ingest.py`
- `app/db.py`
- `app/config.py`

TikTok / Streamer agent:
- `app/tiktok_ingest.py`
- `app/tiktok_auth_refresh.py`
- `app/tiktok_live_chat.py`
- `scripts/tiktok_backfill.py`
- `app/templates/tiktok_streamer.html`
- `app/templates/tiktok_analytics.html`
- `app/templates/tiktok_orders.html`

Do not assign overlapping files to multiple agents at the same time.

**WARNING: `app/main.py` is 10,000+ lines.** It contains routes for both Discord and TikTok features, plus all background task logic. This is the highest-priority refactoring target. Be careful when editing — changes can have wide blast radius.

## Safe Change Priorities

Preferred order of operations for parser changes:
1. improve rule-based detection
2. improve stitch heuristics
3. improve explicit text overrides
4. improve correction memory usage
5. only then adjust prompts / AI reliance

Preferred order for review/reporting changes:
1. preserve current working review flow
2. keep the main table readable
3. add reporting/reconciliation without breaking ingestion

## Testing / Verification

Minimum verification after code changes:

```powershell
.\.venv\Scripts\python.exe -m compileall app
```

When touching parser/stitching logic, also sanity-check:
- image-first then text
- payment-only sell default
- explicit buy/sell text overriding trade-like image guesses
- grouped child rows no longer producing duplicate transactions

When touching bookkeeping:
- upload `.xlsx`
- upload `.csv`
- public Google Sheets auto-import path
- reconciliation page loads

## TikTok Shop Integration

**Full API reference: see `TIKTOK_API.md` in project root.**

That file documents every endpoint, auth flow, response shape, and known gotcha.
Read it before touching any TikTok code.

### Current state (as of 2026-04-08)

Everything is working:
- OAuth token exchange + auto-refresh (background loop every 30 min)
- Order sync (backfill + periodic poll + webhook enrichment)
- Live streamer dashboard with real-time orders, GMV, chat, refund alerts
- Live analytics (stream session list, per-stream GMV, per-minute charts)
- Analytics page with daily trends and growth metrics
- Product sync and management

### Key files

| File | Role |
|---|---|
| `scripts/tiktok_backfill.py` | API signing, order/product fetching, all analytics API calls |
| `app/tiktok_ingest.py` | OAuth token exchange, webhook parsing, order normalization |
| `app/tiktok_auth_refresh.py` | Background token refresh logic |
| `app/tiktok_live_chat.py` | TikSync WebSocket for live chat + room ID capture |
| `app/models.py` | `TikTokOrder`, `TikTokAuth`, `TikTokProduct`, `AppSetting` models |
| `app/reporting.py` | TikTok order reporting/summary functions |
| `app/main.py` | All TikTok routes, streamer dashboard, analytics, background pollers |
| `app/templates/tiktok_streamer.html` | Streamer dashboard (orders, GMV, chat, top sellers) |
| `app/templates/tiktok_analytics.html` | Analytics page (charts, stream selector, KPIs) |
| `app/templates/tiktok_orders.html` | Order listing with livestream filter |
| `TIKTOK_API.md` | Complete API reference for all endpoints |

### Two token types

1. **Shop token** (Seller, `user_type=0`) — used for orders, products, shop analytics. Auth at `auth.tiktok-shops.com`.
2. **Creator token** (`user_type=1`) — used for real-time `live_core_stats`. Auth at `open.tiktokapis.com`. Separate OAuth flow.

Both are stored in the `TikTokAuth` DB table and auto-refreshed.

### Critical rules

- **DO NOT use `open.tiktokapis.com` for Shop auth** — Shop uses `auth.tiktok-shops.com`
- **DO NOT mix token types** — Shop token cannot call Creator endpoints and vice versa
- **DO NOT use V1 API paths** — they return 410
- **DO NOT use `today=true`** on overview_performance — causes 66007001 Rpc error
- **GMV = `subtotal_price`** (product value only), not `total_price` (includes tax + shipping)
- **Webhook payloads are incomplete** — always fetch full details from API after receiving
- See `TIKTOK_API.md` for the complete list of gotchas

## Notes For Future Agents

- Database schema has evolved additively; avoid reset-based development if possible
- Production uses PostgreSQL; local dev uses SQLite. The app auto-detects via `DATABASE_URL`
- Both engines are fully supported — `app/db.py` handles connection config, pooling, and migrations for each
- do not break working buys/sells/trades while improving expense handling
- if a row looks wrong in the UI, always check whether the real issue is:
  - bad stitching
  - image-only AI guess
  - stale child grouped row data
  - transaction sync not being removed
- if debugging parser results, prefer fixing deterministic rules before making the AI prompt more complex

## Queue / Processing State Model (CRITICAL)

Each DiscordMessage MUST have a clear processing state.

Valid states:
- `pending` → waiting to be processed
- `processing` → currently being worked on
- `parsed` → successfully parsed
- `failed` → parsing or transaction failed
- `review_required` → needs human review
- `ignored` → intentionally skipped

Rules:
- no message should remain indefinitely in `processing`
- failures must move to `failed` with error reason
- parser changes should allow reprocessing of `parsed` rows
- worker must not silently skip rows without logging why

Definition of "stuck":
- message remains in `pending` or `processing` without progress
- message repeatedly fails without visibility

## Reparse / Replay Rules (CRITICAL)

The system MUST support reprocessing old messages.

Important distinction:
- "seen before" != "correct under latest parser logic"

Requirements:
- allow reprocessing of previously parsed messages
- reparsing must NOT create duplicate transactions
- parser output must be replaceable or refreshable
- reparsing should be possible:
  - by date range
  - by channel
  - by explicit selection

Preferred approach:
- raw DiscordMessage remains source of truth
- normalized Transaction layer is derived and replaceable

Never assume parsed data is final.

## Observability / Logging (CRITICAL)

Logging must make debugging possible without reading code.

Every processing step MUST log:

- message_id
- channel
- current state
- action being performed
- success/failure
- error message (if any)
- timestamp

Required log events:
- message queued
- message picked up by worker
- parsing started
- parsing success
- parsing failure
- transaction sync started
- transaction sync success
- transaction sync failure
- message marked for review

No silent skips:
- if a message is skipped, log WHY

System must support:
- viewing recent failures
- counting messages by state
- identifying stuck messages