$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting redeploy..."

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
