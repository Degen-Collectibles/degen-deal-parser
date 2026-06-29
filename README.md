# Degen Deal Parser

Internal platform for Degen Collectibles — Discord deal parsing + TikTok Shop livestream tools.

## Quick Start

### Windows

**Local dev (web-only, no Discord bot):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1
```

**Production (web + worker + auto-restart):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1
```

**Compile check after code changes:**
```powershell
.\.venv\Scripts\python.exe -m compileall app
```

### macOS

**First-time setup (install dependencies into your active conda env):**
```bash
pip install -r requirements.txt
```

**Local dev (web-only, no Discord bot):**
```bash
bash scripts/run_local_web.sh
```

**Production (web + worker — run in two separate terminals):**
```bash
# Terminal 1
bash scripts/run_hosted_web.sh

# Terminal 2
bash scripts/run_hosted_worker.sh
```

**Compile check after code changes:**
```bash
python -m compileall app
```

Then open: `http://127.0.0.1:8000/login`

## What This App Does

**Discord side:**
- Ingests watched Discord deal-log channels
- Parses buys, sales, trades, and expenses (rule-based + OpenAI fallback)
- Review/approval workflow, financial reporting, bookkeeping reconciliation

**TikTok side:**
- TikTok Shop order sync (API + webhooks)
- Live streamer dashboard with real-time orders, GMV, chat, goal bar, high-value/VIP alerts, velocity sparkline, post-stream summary, leaderboard drilldowns
- Stream analytics with buyer tracking, product performance, stream-over-stream comparison
- Client & product intelligence with buyer/product drilldowns
- Product management

**Inventory:**
- Card inventory management with barcode generation, camera scanning, slab cert lookup
- Auto-pricing, Shopify integration, label printing
- **Degen Eye multi-TCG scanner** (`/degen_eye`) — camera + text-based card search across Pokemon, Magic, Yu-Gi-Oh, One Piece, Lorcana, Dragon Ball, etc. Uses Ximilar visual recognition plus dedicated per-TCG card APIs (Scryfall, YGOPRODeck, OPTCG, Lorcast, TCGdex, PokemonTCG) with TCGTracking for variant + condition-level pricing. Selectable **Fast / Balanced / Accurate** scanner mode:
  - **Fast**: Ximilar only (2-4s, 0 AI calls)
  - **Balanced** (default): Ximilar first; HIGH short-circuits; otherwise Haiku + Gemini Flash run in parallel in the background, 3-way majority vote
  - **Accurate**: Ximilar first; HIGH short-circuits; MEDIUM backgrounds a single Opus call; LOW blocks on Opus + Gemini Pro tiebreaker on disagreement
- **Degen Eye v2 Pokemon scanner** (`/degen_eye/v2`) — separate local-first Pokemon scanner targeting sub-1-second scan-to-result. Runs OpenCV card detection + a perceptual-hash nearest-neighbor lookup against a pre-built index of every Pokemon card. No per-scan LLM call on the happy path; Ximilar fallback only when pHash is LOW confidence. Streaming SSE response so name lands in ~500ms, price in ~800ms. Two capture modes: Tap (shutter button) and Auto (continuous detection, auto-triggers on 3 consecutive stable frames). Bootstrap: `python scripts/build_phash_index.py` (~20-40 min one-time for the full Pokemon catalog).

## Key Pages

| Page | Purpose |
|---|---|
| `/table` | Main deal queue |
| `/review-table` | Review queue |
| `/reports` | Financial reports |
| `/bookkeeping` | Sheet import + reconciliation |
| `/tiktok/orders` | TikTok order listing |
| `/tiktok/streamer` | Live streamer dashboard (orders, GMV, goal bar, alerts, sparkline) |
| `/tiktok/analytics` | Stream analytics, buyer tracking, product performance, comparison |
| `/tiktok/clients` | Client & product intelligence with drilldowns |
| `/tiktok/streamer/config` | Stream time config + GMV goal + alert thresholds |
| `/inventory` | Inventory management, scanning, labels |
| `/stream-manager` | Multi-stream team scheduling |
| `/admin/home` | Admin dashboard |
| `/admin/debug` | System diagnostics |

## Required Env Vars

Core:
```
# Local dev (SQLite):
DATABASE_URL=sqlite:///data/degen_live.db
# Production (Postgres on Machine B):
# DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/degen_live
SESSION_SECRET=<strong random secret>
DISCORD_BOT_TOKEN=<discord bot token>
OPENAI_API_KEY=<openai key>
ADMIN_USERNAME=admin
ADMIN_PASSWORD=<strong password>
```

AI provider (optional — defaults to OpenAI):
```
AI_PROVIDER=openai                 # or "nvidia"
# If AI_PROVIDER=nvidia:
NVIDIA_API_KEY=<nvidia inference hub key>
NVIDIA_BASE_URL=https://inference-api.nvidia.com/v1     # inference-api, NOT integrate.api — integrate accepts text but 404s on multimodal
NVIDIA_MODEL=aws/anthropic/bedrock-claude-opus-4-7      # heavy model (vision identification, Accurate mode)
NVIDIA_FAST_MODEL=aws/anthropic/claude-haiku-4-5-v1    # fast model (text query parsing, Balanced mode vote A)
NVIDIA_TIEBREAKER_MODEL=gcp/google/gemini-3.1-pro-preview  # Accurate mode tiebreaker on Ximilar+Opus disagreement
NVIDIA_GEMINI_FLASH_MODEL=gcp/google/gemini-3-flash-preview  # Balanced mode vote B (parallel with Haiku)
```

Card scanner (optional — only needed for Degen Eye `/degen_eye`):
```
XIMILAR_API_TOKEN=<ximilar collectibles token>  # visual card recognition
POKEMON_TCG_API_KEY=<pokemontcg.io key>         # higher rate limits for PokemonTCG API
```
TCGTracking's public API is currently unauthenticated, so no key is needed for variant/condition pricing.

Signing and webhook-verification keys:
```
BUYLIST_QUOTE_SIGNING_KEYS=<dedicated random 32+ character current key[,previous key...]>
CLOCKIFY_WEBHOOK_SIGNING_SECRETS=<Clockify-generated webhook authToken[,previous authToken during a deliberate rotation/retry window only]>
```

`BUYLIST_QUOTE_SIGNING_KEYS` is an app-generated random signing key; do not reuse
`SESSION_SECRET`. `CLOCKIFY_WEBHOOK_SIGNING_SECRETS` is different: copy the
`authToken` generated by Clockify when the configured webhook is created.
Clockify includes that verification token in each webhook delivery header. It is
not `CLOCKIFY_API_KEY`, a new API key, or an arbitrary app secret, and it must
never be placed in the callback URL. No new token is needed merely to configure
this deployment; generate one only when enabling, repairing, or deliberately
rotating the webhook. During rotation, put the current token first and retain
the previous token only for a short in-flight delivery/retry window, then remove
it; Clockify's token-rotation endpoint invalidates the prior token.

TikTok (needed for order sync and streamer dashboard):
```
TIKTOK_APP_KEY=<tiktok partner center app key>
TIKTOK_APP_SECRET=<tiktok partner center app secret>
TIKTOK_TOKEN_ENCRYPTION_KEYS=<dedicated random 32+ character current key[,previous key...]>
TIKTOK_REDIRECT_URI=<oauth callback url>
TIKTOK_SHOP_CIPHER=<from oauth response>
```

`TIKTOK_TOKEN_ENCRYPTION_KEYS` is required before starting against a database
that contains OAuth tokens. Put the current key first and retain previous keys
until startup has migrated all rows; never reuse `SESSION_SECRET`.

The first encrypted-token rollout must be a coordinated, non-rolling migration:
stop every old web/worker process that can refresh TikTok tokens, configure the
dedicated key, start one upgraded instance and let startup migrate the rows,
verify the stored token columns use the `enc:v1:` prefix, then start the other
upgraded instances. Old code can still write plaintext after a new instance has
migrated a row, so it must not overlap this first rollout. Later key rotations
may be rolling as long as every running instance already includes encrypted
token storage and the previous key remains in the ring.

If startup reports a token migration conflict, leave that instance stopped and
retry after the active refresh finishes; the compare-and-set guard intentionally
refuses to overwrite the newer token. Encryption does not erase plaintext from
historical backups, PostgreSQL WAL, or SQLite free pages. After the code rollout,
revoke and reauthorize both Seller and Creator OAuth grants, apply the backup
retention policy, and checkpoint/VACUUM any SQLite database that held tokens.

TikTok Live Chat (optional):
```
TIKTOK_LIVE_API_KEY=<tiksync api key>
TIKTOK_LIVE_USERNAME=<tiktok username>
```

See `app/config.py` for the complete list of all settings.

## Documentation

| File | What It Covers |
|---|---|
| `AGENTS.md` | Project rules, architecture, coding conventions (read by AI tools automatically) |
| `TIKTOK_API.md` | Every TikTok API endpoint, auth flow, response shape, and known gotcha |
| `PROJECT_STATUS.md` | Current deployment setup, what's working, known issues, recent changes |

## Deployment

Currently deployed on a Windows PC ("Machine B") running 24/7:
- Web + worker via `scripts/run_hosted.ps1` with auto-restart
- Exposed via Cloudflare tunnel at `ops.degencollectibles.com`
- HTTPS-only session cookies

Local development on a separate Windows PC:
- Web-only via `scripts/run_local_web.ps1`
- Discord ingest and worker disabled
- Machine B uses **PostgreSQL** (production data); Machine A uses **SQLite** (local dev copy, not synced)

## Debugging

Start with `/admin/debug` before reading code:
1. Check web app and worker heartbeat indicators
2. Check queue state counts
3. Check "Stuck Processing" section
4. Check "Recent Worker Failures"
5. Logs: `logs/app.log` (web) and `logs/worker.log` (worker)
