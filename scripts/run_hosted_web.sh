#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

export DISCORD_INGEST_ENABLED=false
export PARSER_WORKER_ENABLED=false
export STARTUP_BACKFILL_ENABLED=false
export RUNTIME_NAME=hosted_web
export RUNTIME_LABEL="Hosted Web"
export WORKER_RUNTIME_NAME=hosted_worker
export WORKER_RUNTIME_LABEL="Hosted Worker"

echo "Starting hosted web process."
echo "Discord ingest, backfill execution, and parser worker are disabled for this process."
echo "DATABASE_URL loaded from .env (not overridden)."

exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
