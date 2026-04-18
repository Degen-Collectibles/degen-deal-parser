$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting redeploy..."

# --- Write deploy stamp (monitor correlates restarts within window to this) ---
# Contains: UTC ISO timestamp, current git HEAD, current git branch.
# Monitors should treat any app/worker restart within ~180s after this stamp
# as an expected deploy, not a crash.
try {
    $logsDir = Join-Path $repoRoot "logs"
    if (-not (Test-Path $logsDir)) {
        New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
    }
    $stampPath = Join-Path $logsDir "deploy.stamp"
    $utcNow = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    Push-Location $repoRoot
    $gitSha = (& git rev-parse HEAD 2>$null) -join ""
    $gitBranch = (& git rev-parse --abbrev-ref HEAD 2>$null) -join ""
    Pop-Location
    if (-not $gitSha) { $gitSha = "unknown" }
    if (-not $gitBranch) { $gitBranch = "unknown" }
    $stamp = [pscustomobject]@{
        timestamp_utc = $utcNow
        git_sha       = $gitSha.Trim()
        git_branch    = $gitBranch.Trim()
        host          = $env:COMPUTERNAME
        user          = $env:USERNAME
        reason        = "redeploy.ps1"
    } | ConvertTo-Json -Compress
    Set-Content -Path $stampPath -Value $stamp -Encoding utf8 -Force
    Write-Host "Wrote deploy stamp: $stampPath ($gitSha)"
} catch {
    Write-Host "WARN: failed to write deploy stamp: $_"
}

# --- Stop scheduled task first (kills its process tree) ---
$task = Get-ScheduledTask -TaskName "DegenParser" -ErrorAction SilentlyContinue
if ($task -and $task.State -ne 'Ready') {
    Write-Host "Stopping DegenParser scheduled task..."
    Stop-ScheduledTask -TaskName "DegenParser" -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

# --- Kill remaining processes via WMI (reliable CommandLine access) ---
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'uvicorn.*app\.main|app\.worker_service' } |
    ForEach-Object {
        Write-Host "Stopping python PID $($_.ProcessId): $($_.CommandLine)"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }

Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'run_hosted' } |
    ForEach-Object {
        Write-Host "Stopping supervisor PID $($_.ProcessId)"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }

# --- Wait for port 8000 to free up ---
$deadline = (Get-Date).AddSeconds(15)
while ((Get-Date) -lt $deadline) {
    $conn = Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue
    if (-not $conn) { break }
    Write-Host "Port 8000 still in use, waiting..."
    Start-Sleep -Seconds 2
}
Start-Sleep -Seconds 2

# --- Relaunch via scheduled task (runs in interactive session) ---

Stop-ScheduledTask -TaskName "DegenParser" -ErrorAction SilentlyContinue
Start-ScheduledTask -TaskName "DegenParser"

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] DegenParser scheduled task triggered. Web + Worker will start automatically."
