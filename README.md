# Degen Deal Parser

Internal platform for Degen Collectibles — Discord deal parsing + TikTok Shop livestream tools.

## Quick Start

**Local dev (web-only, no Discord bot):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1
```
Then open: `http://127.0.0.1:8000/login`

**Production (web + worker + auto-restart):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1
```

**Compile check after code changes:**
```powershell
.\.venv\Scripts\python.exe -m compileall app
```

## What This App Does

**Discord side:**
- Ingests watched Discord deal-log channels
- Parses buys, sales, trades, and expenses (rule-based + OpenAI fallback)
- Review/approval workflow, financial reporting, bookkeeping reconciliation

**TikTok side:**
- TikTok Shop order sync (API + webhooks)
- Live streamer dashboard with real-time orders, GMV, chat, goal bar, high-value/VIP alerts, velocity sparkline, post-stream summary
- Stream analytics with buyer tracking, product performance, stream-over-stream comparison
- Product management

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
| `/tiktok/streamer/config` | Stream time config + GMV goal + alert thresholds |
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

TikTok (needed for order sync and streamer dashboard):
```
TIKTOK_APP_KEY=<tiktok partner center app key>
TIKTOK_APP_SECRET=<tiktok partner center app secret>
TIKTOK_REDIRECT_URI=<oauth callback url>
TIKTOK_SHOP_CIPHER=<from oauth response>
```

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
