# Test Layout

Tests are grouped by the same feature areas used by the application:

- `admin/` - admin routes, admin tools, supply/admin workflows
- `auth/` - login, sessions, auth key separation, security hardening
- `bookkeeping/` - imports, reconciliation, bank feeds
- `discord/` - Discord ingest, attachments, queue repair/reparse, recovery audits
- `finance/` - ledger, finance views, AI usage costs
- `infra/` - app/runtime config, middleware, cache, split status
- `inventory/` - inventory CRUD, barcode labels, Shopify inventory sync
- `ops/` - navigation, ops log, review queue and portal audits
- `parser/` - parser rules, corrections, pass2, stitching, table transaction behavior
- `scanner/` - Degen Eye/card scanner behavior and scoring
- `shopify/` - Shopify ingest and sync schemas
- `team/` - employee portal, schedules, Clockify, buylist, time off, team admin
- `tiktok/` - TikTok orders, reporting, products, token refresh, backfill
- `tools/` - CLI and standalone validation scripts
