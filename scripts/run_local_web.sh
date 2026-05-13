#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$REPO_ROOT/data/degen_live.db"

export DATABASE_URL="sqlite:///$DB_PATH"
export DISCORD_INGEST_ENABLED=false
export PARSER_WORKER_ENABLED=false
export STARTUP_BACKFILL_ENABLED=false
export SESSION_HTTPS_ONLY=false
export SESSION_DOMAIN=none
export RUNTIME_NAME=local_web
export RUNTIME_LABEL="Local Web"
export WORKER_RUNTIME_NAME=local_worker
export WORKER_RUNTIME_LABEL="Local Worker"

echo "Starting local web-only host mode."
echo "Discord ingest, backfill execution, and parser worker are disabled for this session."
echo "Session cookie is set for localhost (HTTPS-only and domain overridden)."
echo "Using local SQLite database at $DB_PATH"

exec python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
