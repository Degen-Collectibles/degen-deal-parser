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

# --- Relaunch supervisor detached ---

Start-Process -FilePath "powershell.exe" `
    -ArgumentList @("-ExecutionPolicy", "Bypass", "-File", "$repoRoot\scripts\run_hosted.ps1") `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden

Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Supervisor relaunched. Web + Worker will start automatically."
