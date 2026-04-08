$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting redeploy..."

# --- Kill existing processes ---

Get-Process -Name python -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'uvicorn.*app\.main' } |
    ForEach-Object {
        Write-Host "Stopping web process PID $($_.Id)"
        Stop-Process -Id $_.Id -Force
    }

Get-Process -Name python -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'app\.worker_service' } |
    ForEach-Object {
        Write-Host "Stopping worker process PID $($_.Id)"
        Stop-Process -Id $_.Id -Force
    }

Get-Process -Name powershell -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'run_hosted\.ps1' } |
    ForEach-Object {
        Write-Host "Stopping supervisor PID $($_.Id)"
        Stop-Process -Id $_.Id -Force
    }

Write-Host "Waiting for ports to free up..."
Start-Sleep -Seconds 3

# --- Relaunch via scheduled task (runs in interactive session) ---

Stop-ScheduledTask -TaskName "DegenParser" -ErrorAction SilentlyContinue
Start-ScheduledTask -TaskName "DegenParser"

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] DegenParser scheduled task triggered. Web + Worker will start automatically."
