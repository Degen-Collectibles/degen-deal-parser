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

## TikTok Shop Integration (CRITICAL — read before touching TikTok code)

### Current state (as of 2026-04-03)

TikTok Shop order sync is **working**. The backfill script pulls orders from the
TikTok Shop Open Platform **V2 API** and stores them in the `TikTokOrder` model.
Orders are visible on `/reports`.

The OAuth callback (`/integrations/tiktok/callback`) and token refresh are also
working. They use the **TikTok Shop-specific auth endpoints** at
`auth.tiktok-shops.com`, not the generic TikTok OAuth at `open.tiktokapis.com`.

### Files

| File | Role |
|---|---|
| `scripts/tiktok_backfill.py` | CLI backfill — fetches historical orders from TikTok Shop API |
| `app/tiktok_ingest.py` | Shared utilities: token exchange, webhook parsing, order normalization |
| `app/models.py` | `TikTokOrder` and `TikTokAuth` SQLModel definitions |
| `app/reporting.py` | TikTok order reporting/summary functions |
| `app/main.py` | FastAPI routes for `/integrations/tiktok/callback` and `/webhooks/tiktok/orders` |
| `tests/test_tiktok_reporting.py` | 17 regression tests covering auth, webhooks, upsert, and reporting |

### TikTok Shop API V2 — what you must know

TikTok **deprecated V1 entirely** (HTTP 410). All API calls must use V2.
The V1→V2 migration involved several breaking changes that are easy to get wrong:

1. **Base URL**: `https://open-api.tiktokglobalshop.com` (global, no `.us` subdomain).
   The `.us` regional domain returns 503 for V2 paths.

2. **Endpoint paths include the version**: e.g. `/order/202309/orders/search`.
   V1 paths like `/api/orders/search` return 410.

3. **`version=202309` query parameter**: must be present in every request AND
   included in the HMAC signature (it is NOT excluded like `access_token`).

4. **Auth via header, not query param**: V2 requires `x-tts-access-token: <token>`
   as a request header. The `access_token` query param is still sent (for
   backwards compat / load balancer routing) but is excluded from the signature.

5. **Pagination is in query params, not body**: `page_size` and `page_token`
   go in the URL query string. The POST body contains only search filters.

6. **Signature algorithm** (HMAC-SHA256):
   ```
   canonical = sorted query params (excluding: sign, access_token)
   string_to_sign = app_secret + path + key1val1key2val2... + body_json + app_secret
   sign = HMAC-SHA256(app_secret, string_to_sign).hex()
   ```
   - Body MUST be the exact JSON bytes sent in the request (use pre-serialized `raw_body`).
   - `version`, `shop_cipher`, `shop_id`, `app_key`, `timestamp`, `page_size`,
     `page_token` are all included in the signature.

7. **Response field names (V2)**:
   - `payment` (not `payment_info`) — contains `total_amount`, `sub_total`, `tax`
   - `line_items` (not `item_list`) — each has `product_name`, `sale_price`, `sku_id`
   - `id` (not `order_id`) at the order level
   - Pagination: `next_page_token` in `data` object
   - `buyer_nickname` for customer display name

8. **V2 search returns full order details** — line items, payment, addresses
   are all included in `/order/202309/orders/search` results. A separate
   detail fetch is unnecessary for backfill.

### Running the backfill

```powershell
# IMPORTANT: override system DATABASE_URL if it points to old Postgres
$env:DATABASE_URL = "sqlite:///data/degen_live.db"

# Dry run first
.\.venv\Scripts\python.exe scripts\tiktok_backfill.py --dry-run --limit 5

# Real sync
.\.venv\Scripts\python.exe scripts\tiktok_backfill.py --limit 500
```

### Required env vars

```
TIKTOK_APP_KEY          — TikTok Shop app key
TIKTOK_APP_SECRET       — TikTok Shop app secret
TIKTOK_SHOP_ID          — numeric shop ID (e.g. 7495987383262087496)
TIKTOK_SHOP_CIPHER      — shop cipher from API Testing Tool
TIKTOK_ACCESS_TOKEN     — access token from API Testing Tool
```

Optional:
```
TIKTOK_SHOP_API_BASE_URL — override base URL (default: https://open-api.tiktokglobalshop.com)
TIKTOK_REFRESH_TOKEN     — for automated token refresh (not yet implemented)
```

### TikTok Shop auth protocol (CRITICAL — different from generic TikTok)

TikTok Shop uses a **completely different auth endpoint** from generic TikTok OAuth.
This is the single most common mistake when implementing TikTok Shop auth.

| | Generic TikTok OAuth (WRONG for Shop) | TikTok Shop (CORRECT) |
|---|---|---|
| Auth host | `open.tiktokapis.com` | `auth.tiktok-shops.com` |
| Token exchange path | `/v2/oauth/token/` | `/api/v2/token/get` |
| Token refresh path | `/v2/oauth/token/` | `/api/v2/token/refresh` |
| HTTP method | POST with form body | GET with query params |
| App key param | `client_key` | `app_key` |
| App secret param | `client_secret` | `app_secret` |
| Auth code param | `code` | `auth_code` |
| Grant type | `authorization_code` | `authorized_code` |
| redirect_uri | required | not used |

Token exchange example URL:
```
https://auth.tiktok-shops.com/api/v2/token/get?app_key=XXX&app_secret=XXX&auth_code=XXX&grant_type=authorized_code
```

The response wraps data in a `data` object with `code: 0` for success:
```json
{"code": 0, "message": "success", "data": {"access_token": "...", "access_token_expire_in": 7200, ...}}
```

### Known gotchas for future agents

- **DO NOT use `open.tiktokapis.com` for Shop auth**. Shop auth lives at `auth.tiktok-shops.com`.
- **DO NOT use `client_key`/`client_secret`/`code`/`authorization_code`** for Shop auth. Use `app_key`/`app_secret`/`auth_code`/`authorized_code`.
- **DO NOT POST to Shop auth endpoints**. They expect GET with query params.
- **DO NOT revert to V1 paths** (`/api/orders/search`). They are dead (410).
- **DO NOT use the `.us` subdomain** (`open-api.us.tiktokglobalshop.com`). It 503s.
- **DO NOT put `page_size` in the POST body**. V2 wants it as a query param.
- **DO NOT omit `x-tts-access-token` header**. V2 requires it.
- **DO NOT omit `version` from the signature**. Only `sign` and `access_token` are excluded.
- **System `DATABASE_URL` env var** may override `.env`. Check with `echo $env:DATABASE_URL`.
- Auth codes expire in ~60 seconds. The exchange must happen immediately on callback.
- Access tokens expire. When they do, the user must get a new one from the
  API Testing Tool (or implement refresh token flow using `TIKTOK_REFRESH_TOKEN`).

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