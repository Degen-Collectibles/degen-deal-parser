# Phase 1 — Green Linux skeleton

Date: 2026-04-27 UTC / 2026-04-26 PT  
Status: scaffold created on Ubuntu/OpenClaw host — **not production**

## Scope

This phase creates the local Linux/Green production skeleton for the Degen app migration. It does not modify Machine B, does not start Green services, does not change Cloudflare, and does not write to the production database.

## Created host layout

Created on the Ubuntu/OpenClaw host:

```text
/opt/degen/
  README.md
  app/
  backups/
  cloudflared/
  data/
    attachments/
      thumbs/
    exports/
    v2_pending_scans/
    v2_training_scans/
  deploy/
    deploy.sh
    preflight.sh
    rollback.sh
    smoke-test.sh
    state/
/var/log/degen/
```

Ownership is currently `ubuntu:ubuntu` so OpenClaw can prepare staging. Before real production cutover, consider creating a dedicated `degen` Linux user/group and moving ownership accordingly.

## Versioned templates added

The following files were added to the repo so the Green setup is auditable and repeatable:

```text
deploy/linux/deploy.sh
deploy/linux/preflight.sh
deploy/linux/rollback.sh
deploy/linux/smoke-test.sh
deploy/systemd/degen-web.service.example
deploy/systemd/degen-worker.service.example
deploy/systemd/cloudflared-degen.service.example
```

The scripts were mirrored into `/opt/degen/deploy/` for later use, but they have not been used to deploy or restart services.


## Green app checkout and venv status

After Jeffrey approved the next safe Phase 1 step, `/opt/degen/app` was cloned from `origin/main` at commit `f32165a7ac21587dd8de525ee44dba4158885993`.

A Python virtualenv was created with Python 3.11.15. Important discovery: the host default `python3` is 3.10.12, but `browser-use` requires Python 3.11+, so Green deployment scripts/templates should use Python 3.11 or newer.

Dependency status:

- Core `requirements.txt` no longer includes `tiksync`; TikSync is tracked as an optional integration in `requirements/optional-tiktok-live-chat.txt`.
- Public dependencies installed successfully without requiring TikSync.
- `pip check` passed after installing public dependencies.
- Import-only checks for `app.main` and `app.worker_service` passed using explicit non-production placeholder secrets and SQLite test DB path.
- No service was started and no production secrets were written.

Open dependency gap before real staging: decide whether TikTok live chat is required on Green and, if so, configure the private/package source for `requirements/optional-tiktok-live-chat.txt`. Core deploy no longer blocks on it.

## Service model

Initial recommendation remains **systemd-first**:

- `degen-web.service` runs FastAPI/Uvicorn on `127.0.0.1:8000`.
- `degen-worker.service` runs `python -m app.worker_service`.
- `cloudflared-degen.service` is an optional future template for staging/cutover tunnel.

During staging, keep the worker disabled unless deliberately testing external ingest. Running Blue and Green workers at the same time against production integrations can cause duplicate ingestion or conflicting writes.

## What was not done

- Did not clone the production app into `/opt/degen/app` yet.
- Did not create `/opt/degen/.env`.
- Did not install Postgres on Green.
- Did not restore a database snapshot.
- Did not copy Machine B media/cache yet.
- Did not install systemd units.
- Did not start/restart any Degen service.
- Did not touch Machine B.
- Did not change Cloudflare.

## Next Phase 1/2 steps

1. Decide whether to create a dedicated Linux user/group `degen` before continuing.
2. Clone `origin/main` into `/opt/degen/app`.
3. Create Python venv and install dependencies.
4. Create `/opt/degen/.env` from redacted/controlled production secret transfer.
5. Install or configure Postgres 17 on Green.
6. Restore a fresh Blue DB dump into Green staging.
7. Start Green web only on a private/Tailscale URL and run `smoke-test.sh`.
8. Pre-copy media/cache from Machine B to `/opt/degen/data`.
9. Only after staging passes: plan Cloudflare staging hostname and final cutover rehearsal.
