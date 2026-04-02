# AGENTS.md

## Project

Degen Collectibles Discord deal / ledger parser.

This project ingests Discord deal-log messages, stores raw messages in SQLite, parses them into structured transactions, normalizes them for financial reporting, and provides a FastAPI/Jinja review UI.

Current stack:
- Python
- FastAPI
- discord.py
- SQLite
- OpenAI API
- HTML/Jinja templates
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

Use the venv Python directly on Windows:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Main pages:
- `/table`
- `/review-table`
- `/reports`
- `/bookkeeping`

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

Do not assign overlapping files to multiple agents at the same time.

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

## Notes For Future Agents

- SQLite schema has evolved additively; avoid reset-based development if possible
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