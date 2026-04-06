$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

$env:DISCORD_INGEST_ENABLED = "false"
$env:PARSER_WORKER_ENABLED = "false"
$env:STARTUP_BACKFILL_ENABLED = "false"
$env:RUNTIME_NAME = "hosted_web"
$env:RUNTIME_LABEL = "Hosted Web"
$env:WORKER_RUNTIME_NAME = "hosted_worker"
$env:WORKER_RUNTIME_LABEL = "Hosted Worker"

Write-Host "Starting hosted web process."
Write-Host "Discord ingest, backfill execution, and parser worker are disabled for this process."
Write-Host "DATABASE_URL loaded from .env (not overridden)."

& "$repoRoot\.venv\Scripts\python.exe" -m uvicorn app.main:app --host 0.0.0.0 --port 8000
