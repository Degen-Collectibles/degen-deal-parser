# Linux production migration plan

Date: 2026-04-27 UTC / 2026-04-26 PT  
Owner: OpenClaw / Jeffrey  
Status: Phase 0 inventory draft — **read-only only, no production changes made**

## Goal

Migrate the Degen app from the current Windows production host (`desktop-ppf7vk9`, Machine B) to the Ubuntu/OpenClaw host with as little downtime as practical.

The recommended approach is blue/green:

- **Blue**: current production on Machine B / Windows.
- **Green**: new Linux production stack on the Ubuntu/OpenClaw host.
- Build and rehearse Green while Blue keeps serving traffic.
- Final cutover should be a short write pause, final DB dump/restore + final media sync, then Cloudflare tunnel cutover.

## Non-goals for Phase 0

- Do not stop/restart Machine B services.
- Do not flip Cloudflare tunnel/DNS.
- Do not mutate the production database.
- Do not copy secrets into docs or chat.
- Do not edit production files except future explicit deploy/cutover steps.

## Phase 0 inventory summary

### Current production host

- Hostname: `DESKTOP-PPF7VK9`
- OS: Windows 10 Pro
- Tailscale/IP used by OpenClaw: `100.110.34.106`
- SSH user used for inventory: `Degen`
- Repo path: `C:\Users\Degen\degen-deal-parser`
- Repo remote: `https://github.com/jmanballa/degen-deal-parser.git`
- Production HEAD observed: `f32165a7ac21587dd8de525ee44dba4158885993`
- Local prod working tree has untracked operational/debug files:
  - `_check_ghost.py`
  - `_scout_health.ps1`
  - `_scout_health2.ps1`
  - `_scout_query.py`
  - `app/data/hit_images/`
  - `scripts/verify_match.py`

These untracked files need a review before cutover. Some may be disposable scratch; some may contain operational logic that should be either committed, migrated to `/opt/degen/data`, or deliberately dropped.

### Current health

Read-only health check on Machine B returned HTTP 200 from `http://127.0.0.1:8000/health`:

```json
{"ok":true,"db_ok":true,"local_runtime_status":"running","local_runtime_label":"Running","local_runtime_needs_attention":false,"error":null}
```

### Runtime shape

Machine B currently uses PowerShell launcher scripts rather than Linux service files.

Observed scripts:

- `scripts/run_hosted.ps1`
  - Supervises both web and worker processes.
  - Starts web first.
  - Waits for `/health` at `http://127.0.0.1:8000/health`.
  - Starts worker after web is healthy.
  - Restarts web/worker on crashes up to `20` restarts.
  - Runs liveness checks every `60s`; restarts web after `3` consecutive failures.
- `scripts/run_hosted_web.ps1`
  - Sets:
    - `DISCORD_INGEST_ENABLED=false`
    - `PARSER_WORKER_ENABLED=false`
    - `STARTUP_BACKFILL_ENABLED=false`
    - `RUNTIME_NAME=hosted_web`
    - `RUNTIME_LABEL=Hosted Web`
    - `WORKER_RUNTIME_NAME=hosted_worker`
  - Runs:
    - `.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000`
- `scripts/run_hosted_worker.ps1`
  - Sets:
    - `DISCORD_INGEST_ENABLED=true`
    - `PARSER_WORKER_ENABLED=true`
    - `STARTUP_BACKFILL_ENABLED=true`
    - `STARTUP_BACKFILL_LOOKBACK_HOURS=24`
    - `RUNTIME_NAME=hosted_worker`
    - `RUNTIME_LABEL=Hosted Worker`
  - Runs:
    - `.venv\Scripts\python.exe -m app.worker_service`
- `scripts/redeploy.ps1`
  - Writes `logs/deploy.stamp`.
  - Stops scheduled task `DegenParser`.
  - Kills matching `python.exe` and `powershell.exe` process trees via CIM.
  - Waits for port `8000` to free.
  - Starts scheduled task `DegenParser`.
- `scripts/backup_pg.ps1`
  - Uses PostgreSQL 17 `pg_dump.exe`.
  - Dumps database `degen_live` as user `degen` on `127.0.0.1:5432`.
  - Writes local backups under `C:\backups\degen-db`.
  - Keeps 7 local backups.
  - Uploads to rclone remote `onedrive:backups/degen-db`.
  - Prunes remote backups older than 30 days.

### Windows scheduled task

Observed scheduled task:

- Task name: `\DegenParser`
- Task to run: `powershell.exe -ExecutionPolicy Bypass -File C:\Users\Degen\degen-deal-parser\scripts\run_hosted.ps1`
- Start in: `C:\Users\Degen\degen-deal-parser`
- Run as user: `Degen`
- Schedule type: on-demand only
- Stop if runs longer than: `8760:00:00`
- State: enabled

The task query reported `Status: Ready` while the app was healthy/listening, so status may not fully reflect the live process tree from this SSH context. Verify interactively before final cutover.

### Ports

Observed listening ports / connections from `netstat`:

- `0.0.0.0:8000` listening — app web process.
- `0.0.0.0:5432` and `[::]:5432` listening — PostgreSQL.
- Multiple local connections between app processes and Postgres on `5432`.
- Several outbound `:443` connections, likely Cloudflare/TikTok/Shopify/API/Discord activity.

Process listing via `tasklist`/CIM was access-denied in this SSH context, but `netstat` was enough to confirm app + Postgres listeners.

### Cloudflare tunnel

Cloudflare files observed:

- `C:\Users\Degen\.cloudflared\cert.pem`
- `C:\Users\Degen\.cloudflared\config.yml`
- `C:\Users\Degen\.cloudflared\degen-ops-token.txt`

Redacted config shape:

```yaml
tunnel: ***REDACTED***
credentials-file: ***REDACTED***

ingress:
  - hostname: ops.degencollectibles.com
    service: http://localhost:8000
  - service: http_status:404
```

Cutover will need either:

1. Move/recreate the same tunnel credentials on Green, then stop Blue tunnel and start Green tunnel, or
2. Create a new Cloudflare tunnel on Green and update the `ops.degencollectibles.com` route to point at it.

Option 2 is cleaner and safer for rehearsal because Green can get a temporary staging hostname first.

### Database

Production database inventory from read-only `psql` queries:

- PostgreSQL: `17.9` on Windows, 64-bit
- Database: `degen_live`
- DB user: `degen`
- Database size: `3466 MB`
- Tables observed include:
  - `discordmessage`
  - `attachmentasset`
  - `transaction`
  - `tiktok_orders`
  - `shopify_orders`
  - `user`
  - employee portal tables (`employeeprofile`, `shift_entry`, `time_off_request`, `timecard_approval`, etc.)
  - runtime/worker tables (`runtimeheartbeat`, `backfillrequest`, `reparserun`, etc.)
- Counts observed:
  - `discordmessage`: `2149`
  - `attachmentasset`: `908`
  - attachment BLOB data: `3082 MB`
  - `user`: `1`

One query against `tiktokorder` failed because the actual table name is `tiktok_orders`. Use the constants in `app/db.py` (`TIKTOK_ORDERS_TABLE = "tiktok_orders"`) for future queries.

### Filesystem data / media

Important: Discord images are first-class production data.

Observed Machine B data directories:

- `data/attachments/`
- `data/attachments/thumbs/`
- `data/v2_pending_scans/`
- `data/degen_live.db` (old SQLite file; likely historical/local artifact)
- `data/v2_scan_history.jsonl`
- `app/data/hit_images/`
- `app/data/pokemon_nicknames.json`
- `logs/`

Observed sizes/counts:

- `data/`: `2074` files, `8,798,311,424` bytes (~8.8GB)
- `app/data/`: `10` files, `6,471,131` bytes
- `logs/`: `7` files, `40,182,201` bytes
- `data/attachments/thumbs`: `181` files, `2,118,146` bytes
- DB `attachmentasset` table has `908` rows and `3082 MB` of BLOB data.

The app stores attachment BLOBs in Postgres (`AttachmentAsset.data`) and lazily writes disk cache files under `data/attachments` via `app/attachment_storage.py`. That means the DB is the source of truth for Discord attachments, but the disk cache should still be pre-copied to reduce first-load latency and preserve thumbnails/cache state.

Attachment storage code uses repo-relative paths:

- `ATTACHMENT_CACHE_DIR = BASE_DIR / "data" / "attachments"`
- `THUMBNAIL_CACHE_DIR = BASE_DIR / "data" / "attachments" / "thumbs"`

For Linux prod, either keep this repo-relative `data/` behavior under `/opt/degen/app/data`, or better move durable data out of the checkout and symlink/bind-mount:

```text
/opt/degen/data/attachments -> /opt/degen/app/data/attachments
/opt/degen/data/v2_pending_scans -> /opt/degen/app/data/v2_pending_scans
/opt/degen/data/v2_scan_history.jsonl -> /opt/degen/app/data/v2_scan_history.jsonl
```

Longer-term improvement: introduce `MEDIA_ROOT` / `DATA_ROOT` env vars so prod data is not inside the git checkout.

### Environment variables

`.env` exists on Machine B. Values were not copied into this document. Redacted key names observed include:

- Core/auth/session:
  - `DATABASE_URL`
  - `PUBLIC_BASE_URL`
  - `SESSION_SECRET`
  - `SESSION_DOMAIN`
  - `SESSION_HTTPS_ONLY`
  - `SESSION_SAME_SITE`
  - `ADMIN_USERNAME`
  - `ADMIN_PASSWORD`
  - `ADMIN_DISPLAY_NAME`
  - `REVIEWER_USERNAME`
  - `REVIEWER_PASSWORD`
  - `REVIEWER_DISPLAY_NAME`
- Runtime:
  - `RUNTIME_NAME`
  - `RUNTIME_LABEL`
  - `LOG_TO_FILE`
  - `LOG_DIR`
- Discord/parser:
  - `DISCORD_BOT_TOKEN`
  - `DISCORD_INGEST_ENABLED`
  - `PARSER_WORKER_ENABLED`
  - `STARTUP_BACKFILL_ENABLED`
  - `STARTUP_BACKFILL_LOOKBACK_HOURS`
  - `STARTUP_BACKFILL_LIMIT_PER_CHANNEL`
  - `STARTUP_BACKFILL_OLDEST_FIRST`
- AI/provider:
  - `AI_PROVIDER`
  - `OPENAI_API_KEY`
  - `NVIDIA_API_KEY`
  - `NVIDIA_MODEL`
  - `NVIDIA_FAST_MODEL`
  - `GOOGLE_VISION_API_KEY`
  - `XIMILAR_API_TOKEN`
- Employee portal:
  - `EMPLOYEE_PORTAL_ENABLED`
  - `EMPLOYEE_PII_KEY`
  - `EMPLOYEE_EMAIL_HASH_SALT`
  - `EMPLOYEE_TOKEN_HMAC_KEY`
  - `CLOCKIFY_API_KEY`
  - `CLOCKIFY_WORKSPACE_ID`
  - `CLOCKIFY_BASE_URL`
  - `CLOCKIFY_TIMEOUT_SECONDS`
  - `CLOCKIFY_TIMEZONE`
  - `CLOCKIFY_WEBHOOK_SECRET`
  - `CLOCKIFY_WEBHOOK_SIGNING_SECRET`
  - `CLOCKIFY_WEBHOOK_SIGNING_SECRETS`
- Shopify/TikTok:
  - `SHOPIFY_API_KEY`
  - `SHOPIFY_STORE_DOMAIN`
  - `SHOPIFY_WEBHOOK_SECRET`
  - `TIKTOK_APP_KEY`
  - `TIKTOK_APP_SECRET`
  - `TIKTOK_BASE_URL`
  - `TIKTOK_REDIRECT_URI`
  - `TIKTOK_SHOP_ID`
  - `TIKTOK_SHOP_CIPHER`
  - `TIKTOK_ACCESS_TOKEN`
  - `TIKTOK_LIVE_API_KEY`
  - `TIKTOK_LIVE_USERNAME`
- Alerts:
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_ALERT_CHAT_ID`
  - `TELEGRAM_ALERT_TOPIC_ID`
- Scanner/data:
  - `DEGEN_EYE_V2_CAPTURE_ENABLED`
  - `DEGEN_EYE_V2_CAPTURE_DIR`
  - `DEGEN_EYE_V2_INDEX_PATH`

### Current local/dev repo on Ubuntu

Local dev clone path:

- `/home/ubuntu/degen-deal-parser`

Observed local HEAD:

- `f32165a7ac21587dd8de525ee44dba4158885993`

Current untracked docs in local dev clone at inventory time:

- `docs/audit-2026-04-25.md`
- `docs/audit-2026-04-26.md`
- `docs/portal-audit-2026-04-25.md`
- `docs/ux-audit-forge-2026-04-24.md`
- this file: `docs/linux-prod-migration-plan.md`

Do not confuse this dev clone with future Linux prod checkout.

## Recommended Green architecture

Start with systemd because it is closest to the current PowerShell supervisor and fastest to reason about. Docker Compose remains the cleaner longer-term option once prod is stable.

Proposed layout:

```text
/opt/degen/
  app/                         # prod checkout, tracks origin/main only
  .env                         # prod app secrets, chmod 600, not committed
  data/
    attachments/
    v2_pending_scans/
    v2_training_scans/
    exports/
  backups/
    local-staging-only/
  deploy/
    deploy.sh
    rollback.sh
    smoke-test.sh
    preflight.sh
  releases/                    # optional previous checkout/archive metadata
/var/log/degen/                # app + worker logs if not kept under /opt/degen/logs
```

Services:

- `degen-web.service`
  - Runs uvicorn on `127.0.0.1:8000` or `0.0.0.0:8000` depending on tunnel model.
  - `DISCORD_INGEST_ENABLED=false`
  - `PARSER_WORKER_ENABLED=false`
  - `STARTUP_BACKFILL_ENABLED=false`
- `degen-worker.service`
  - Runs `python -m app.worker_service`.
  - `DISCORD_INGEST_ENABLED=true`
  - `PARSER_WORKER_ENABLED=true`
  - `STARTUP_BACKFILL_ENABLED=true`
  - `STARTUP_BACKFILL_LOOKBACK_HOURS=24`
- `postgresql` service or local Postgres 17 container/service.
- `cloudflared` service for `ops.degencollectibles.com` after cutover.
- Optional later: `degen-deploy-webhook.service` if GitHub webhook deploy is used.

## Low-downtime migration phases

### Phase 1 — Build Green skeleton

No Blue downtime.

1. Create `/opt/degen` directories.
2. Create Linux user/group if desired, e.g. `degen`.
3. Clone repo to `/opt/degen/app`.
4. Create Python venv.
5. Install dependencies.
6. Create `/opt/degen/.env` from Machine B `.env`, with Linux-adjusted paths and no committed secrets.
7. Install Postgres 17 locally or via Docker Compose.
8. Create `degen_live` DB/user.
9. Add systemd units for web + worker, but do not expose externally yet.

### Phase 2 — Restore DB snapshot to Green staging

No Blue downtime.

1. Take a custom-format dump from Blue:
   - `pg_dump -Fc -Z6 -f degen_live_<timestamp>.dump degen_live`
2. Transfer dump to Green over Tailscale/SSH.
3. Restore to Green Postgres.
4. Start Green web only first.
5. Run smoke tests against private/Tailscale URL.
6. Start Green worker only after making sure it will not double-ingest live Discord/TikTok against Blue unless intentionally testing.

Important: during staging, avoid two live workers writing to equivalent production data or consuming the same external webhook/Discord flows. For staging, web-only is safest; worker can run with Discord/TikTok ingest disabled unless specifically testing.

### Phase 3 — Media/cache pre-copy

No Blue downtime.

Pre-copy these from Blue to Green:

- `C:\Users\Degen\degen-deal-parser\data\attachments\`
- `C:\Users\Degen\degen-deal-parser\data\v2_pending_scans\`
- `C:\Users\Degen\degen-deal-parser\data\v2_scan_history.jsonl`
- `C:\Users\Degen\degen-deal-parser\app\data\hit_images\`
- `C:\Users\Degen\degen-deal-parser\app\data\pokemon_nicknames.json`
- recent `logs/` if needed for history/debugging

Use a resumable/copy-preserving tool. On Windows-to-Linux over SSH, options include:

- `rsync` from Linux if rsync is available on Windows, or
- `scp`/`sftp` for first pass, plus a final incremental pass, or
- archive to `.tar`/`.zip` on Blue then transfer/extract on Green.

Because attachment BLOBs are already in Postgres, missing cache files should regenerate lazily, but pre-copying avoids poor first-load behavior and preserves generated thumbnails.

### Phase 4 — Auto-deploy pipeline

No Blue downtime.

Deploy flow:

```text
/dev clone -> commit -> push origin/main -> /opt/degen/app deploy target fetch/reset/restart/health-check
```

Minimum `deploy.sh` behavior:

1. Write deploy stamp.
2. Record previous commit SHA for rollback.
3. `git fetch origin main`.
4. `git reset --hard origin/main`.
5. Recreate/install venv dependencies if `requirements.txt` changed.
6. Run a quick syntax/import check.
7. Restart `degen-web`.
8. Wait for `/health`.
9. Restart `degen-worker`.
10. If health fails, rollback to previous SHA and restart.
11. Alert Telegram/Mission Control.

Rule: never edit `/opt/degen/app` directly. All code changes happen in `/home/ubuntu/degen-deal-parser` or another dev clone, then flow through Git.

### Phase 5 — Backups before cutover

No Blue downtime.

Green must have working backups before production traffic moves:

- Nightly `pg_dump -Fc -Z6`.
- Keep roughly 7 daily / 4 weekly / 3 monthly.
- Push off-machine, not only local disk.
- Alert on backup failure.
- Test restore once before cutover.

Potential off-machine targets:

- Existing OneDrive/rclone target, if creds can be migrated safely.
- Cloudflare R2.
- Backblaze B2.
- S3-compatible storage.

### Phase 6 — Cutover rehearsal

No Blue downtime, except staging restarts.

1. Take fresh Blue dump.
2. Restore to Green staging.
3. Pre-copy media/cache.
4. Run full smoke test.
5. Measure dump/transfer/restore time.
6. Write exact cutover runbook with measured timings.
7. Write rollback runbook.

This determines expected real downtime.

### Phase 7 — Final cutover

Small downtime window.

1. Choose quiet window.
2. Put Blue app into maintenance/no-write state, or stop Blue web+worker if no maintenance mode exists.
3. Take final DB dump.
4. Transfer + restore to Green.
5. Final incremental media/cache sync.
6. Start Green web.
7. Start Green worker.
8. Flip Cloudflare tunnel/hostname route to Green.
9. Smoke-test production URL.
10. Watch logs for 1–2 hours.
11. Keep Blue frozen as rollback for 24–72 hours.

Expected downtime is mostly final DB dump/transfer/restore time. With the observed DB size around 3.5GB plus 3.1GB attachment BLOBs, a rehearsed dump/restore is likely minutes rather than hours, but measure it in Phase 6.

## Smoke test checklist

Run on Green staging and immediately after final cutover:

- `/health` returns `ok=true`, `db_ok=true`.
- Login works.
- Admin dashboard loads.
- Employee portal loads.
- Team/admin employee pages load.
- Payroll/timecards pages load.
- Discord messages page loads.
- Existing attachment images render via `/attachments/{asset_id}`.
- Attachment thumbnails render via `/attachments/{asset_id}/thumb`.
- TikTok orders page loads and recent orders appear.
- Shopify-related pages/webhooks are not erroring.
- Clockify integration endpoints/settings do not error.
- Worker heartbeat appears as expected.
- Logs have no repeated stack traces for DB, file paths, Discord, TikTok, Shopify, Clockify, or PII crypto.

## Rollback strategy

Until Green has served cleanly for 24–72 hours, keep Blue intact.

Fast rollback path:

1. Stop Green worker first to avoid duplicate ingest.
2. Point Cloudflare hostname/tunnel back to Blue.
3. Start/verify Blue app + worker.
4. Smoke-test `ops.degencollectibles.com`.
5. Decide whether any writes made on Green need reconciliation back to Blue.

To minimize rollback reconciliation pain, keep the final cutover window short and monitor immediately.

## Risks / open questions

1. **Cloudflare cutover mechanism**
   - Need decide whether to reuse current tunnel credentials or create a new Green tunnel and swap hostname routing.
   - Safer: new tunnel + staging hostname first.

2. **Admin limitations from SSH**
   - Some Windows process/service APIs returned access denied via SSH.
   - Before final cutover, verify interactively or via elevated shell how DegenParser/cloudflared processes are actually started and stopped.

3. **Untracked Machine B files**
   - Need classify untracked scripts/files as keep/drop/commit.

4. **Data root currently repo-relative**
   - Attachment cache path is `BASE_DIR / data / attachments`.
   - Good enough for first migration if `/opt/degen/app/data` is persistent, but long-term should move to configurable `DATA_ROOT` / `MEDIA_ROOT`.

5. **Duplicate worker risk**
   - Blue and Green workers must not both ingest/write as production during staging/cutover.
   - Keep Green worker disabled or pointed at staging data until cutover.

6. **Postgres table naming**
   - Use actual table names such as `tiktok_orders`, not model-style `tiktokorder`.

7. **Backups**
   - Existing Blue backup script uses rclone/OneDrive and requires `PGPASSWORD` in environment.
   - Need confirm backup job schedule and whether recent backups are healthy.
   - Green backup must be tested before cutover.

8. **Near-zero downtime option**
   - If rehearsal shows dump/restore is too slow, evaluate Postgres logical replication from Blue to Green.
   - Do not start with replication unless needed; it adds moving parts.

## Immediate next actions

1. Confirm Jeffrey wants **systemd-first** Green stack rather than Docker Compose-first.
2. Create `/opt/degen` skeleton on Ubuntu.
3. Install/confirm Postgres 17 on Ubuntu, or choose containerized Postgres.
4. Create Green staging hostname/tunnel plan.
5. Build initial `deploy.sh`, `rollback.sh`, and `smoke-test.sh` in a branch/dev clone.
6. Run first non-production DB dump/restore rehearsal.
7. Pre-copy attachment cache/media to `/opt/degen/data` once target dirs exist.

## Commands used for Phase 0 inventory

Representative read-only commands used:

```bash
ssh Degen@100.110.34.106 "powershell -NoProfile -Command 'hostname; ...'"
ssh Degen@100.110.34.106 "powershell -NoProfile -EncodedCommand <redacted>"
```

Read-only checks included:

- `git rev-parse HEAD`
- `git status --short`
- `git remote -v`
- `netstat -ano`
- `schtasks /Query /TN DegenParser /V /FO LIST`
- `Invoke-WebRequest http://127.0.0.1:8000/health`
- `psql -Atc <SELECT-only queries>`
- directory listings and file counts under repo/data/logs

No destructive commands were run.
