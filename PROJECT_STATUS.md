# Project Status

Last updated: 2026-04-08

## Deployment

### Machine A — Local Dev (Windows PC, travels with owner)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1`
- Web-only mode: Discord ingest, parser worker, and backfill are **disabled**
- Used for UI development and testing
- **SQLite** database at `data/degen_live.db` (local copy, not synced with production)
- Access at `http://127.0.0.1:8000`

### Machine B — Production (Windows PC, runs 24/7)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1`
- Runs both web server and worker process with auto-restart (up to 20 restarts)
- Discord bot, parser worker, TikTok order sync, webhook listener, live chat all run here
- **PostgreSQL** database (`postgresql+psycopg://...@localhost:5432/degen_live`)
- Exposed via **Cloudflare tunnel** at `ops.degencollectibles.com`
- HTTPS-only session cookies, domain-scoped auth
- Health check loop monitors `/health` endpoint

Both machines share the same codebase via git pull. Machine B uses Postgres (production data). Machine A uses a local SQLite copy for dev/testing — it does not share data with production.

## What's Working

### Discord Side
- Discord message ingestion from watched channels
- Rule-based parsing with OpenAI fallback for ambiguous cases
- Message stitching (grouping nearby related messages into one deal)
- Transaction normalization and financial reporting
- Review/approval workflow (`/table`, `/review-table`)
- Bookkeeping import (CSV, XLSX, Google Sheets auto-import) and reconciliation
- Ops log with filtering and pagination

### TikTok Shop Side
- OAuth flow for Shop tokens (auto-refresh every 30 min)
- Separate Creator OAuth flow (for real-time live analytics)
- Order sync: startup backfill + periodic polling + webhook enrichment
- **Streamer dashboard** (`/tiktok/streamer`):
  - Real-time order feed with toast notifications and sound alerts
  - Today's GMV + Stream GMV (manual or auto-detected range)
  - TikTok official GMV from API
  - Top Sellers and Top Buyers with Today/Stream toggle
  - Live chat panel (via TikSync WebSocket), collapsible
  - Refund alerts highlighted in red
  - Stream dividers between orders from different livestreams
  - LIVE/OFFLINE badge (dynamic, based on actual stream status)
  - "Log a Hit" feature for streamers
  - Copy-to-clipboard for customer labels
- **Analytics page** (`/tiktok/analytics`):
  - Daily GMV trend chart (7d/30d/60d/90d)
  - Stream session list with GMV, duration, revenue/hour, % change
  - Per-minute GMV chart for individual streams
  - Top sellers/buyers per stream
  - Local data fallback when TikTok API is unavailable
- **Orders page** (`/tiktok/orders`):
  - Full order listing with date filters
  - Livestream filter dropdown (auto-sets date range to stream window)
- Product sync and management
- TikTok webhook endpoint with signature verification

### Infrastructure
- Role-based auth (admin, reviewer, viewer)
- User management UI (`/admin/users`)
- **Production: PostgreSQL** with connection pooling (QueuePool, pool_size=5, max_overflow=10), TCP keepalives, pool_pre_ping
- **Local dev: SQLite** with WAL mode, busy_timeout, retry logic
- The app auto-detects which DB engine to use based on `DATABASE_URL`
- Structured JSON logging
- Auto-restart scripts for production stability
- CI workflow (GitHub Actions)

## Known Issues / Technical Debt

### Critical
- **`app/main.py` is 10,487 lines.** Contains all routes, background tasks, TikTok integration, streamer dashboard logic in a single file. This is the #1 refactoring target.
- **`app/templates/tiktok_streamer.html` is ~2,400 lines** with all CSS and JS inline. Should be split into separate assets eventually.

### Important
- **Database backups** — production Postgres (`degen_live`) on Machine B has no automated `pg_dump` schedule yet. Should be added.
- **TikTok analytics data is delayed ~2 days** for the `overview_performance` endpoint. The `performance` (session list) endpoint is near-real-time.
- **Webhook payloads arrive incomplete** ($0.00, "Guest", no items). The app works around this by fetching full order details async, but there's a brief window where incomplete data shows.
- **Creator token** for `live_core_stats` requires manual OAuth (separate from Shop token) and a `live_room_id` captured from TikSync WebSocket. This is fragile.

### Nice to Have
- `scripts/tiktok_backfill.py` is ~1,700 lines and could be split into modules
- README.md has some stale Render deployment info
- No automated end-to-end tests for the streamer dashboard
- Chat panel connection depends on TikSync third-party service availability

## Recent Changes (Last 20 Commits)

1. TikTok analytics page with Chart.js charts, stream selector, KPI cards
2. Livestream filter on orders page
3. Stream dividers on streamer dashboard
4. Collapsible chat panel (closed by default)
5. Dynamic LIVE/OFFLINE badge
6. TIKTOK_API.md documentation
7. Parser stitch tolerance improvements
8. Stream team deployment (admin users, route auth, viewer accounts)
9. Live Hit Tracker and Stream Manager
10. Cloudflare timeout fixes
11. TikTok webhook signature verification fixes
12. Image loading optimizations (thumbnails, lazy loading, ETag, caching)
13. CI workflow, dashboard widget, confidence scoring
14. Ops log filters, pagination, caching, auto-promote, TikTok refresh
15. Security and stability audit
16. Auto-restart logic for production scripts

## Key Documentation

| File | What It Covers |
|---|---|
| `AGENTS.md` | Project rules, architecture, coding conventions for AI tools |
| `TIKTOK_API.md` | Every TikTok endpoint, auth flow, response shape, known gotcha |
| `PROJECT_STATUS.md` | This file — current state, deployment, known issues |
| `README.md` | Setup instructions, env vars, deployment options |

## Env Vars Quick Reference

Essential for TikTok features:
```
TIKTOK_APP_KEY, TIKTOK_APP_SECRET, TIKTOK_REDIRECT_URI
TIKTOK_SHOP_CIPHER (needed for analytics APIs)
TIKTOK_LIVE_API_KEY, TIKTOK_LIVE_USERNAME (for live chat)
```

Tokens are stored in the DB after initial OAuth — not needed in .env after first auth.

See `app/config.py` for the complete list of all settings.
