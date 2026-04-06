$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$webScript = Join-Path $PSScriptRoot "run_hosted_web.ps1"
$workerScript = Join-Path $PSScriptRoot "run_hosted_worker.ps1"
$healthUrl = "http://127.0.0.1:8000/health"

function Wait-ForHealth {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    return $false
}

Write-Host "Starting hosted web process first."
$webProcess = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList @("-ExecutionPolicy", "Bypass", "-File", $webScript) `
    -WorkingDirectory $repoRoot `
    -PassThru

Write-Host "Waiting for web health at $healthUrl"
if (-not (Wait-ForHealth -Url $healthUrl -TimeoutSeconds 90)) {
    try { Stop-Process -Id $webProcess.Id -Force } catch {}
    throw "Web process did not become healthy within 90 seconds."
}

Write-Host "Web is healthy. Starting worker process."
$workerProcess = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList @("-ExecutionPolicy", "Bypass", "-File", $workerScript) `
    -WorkingDirectory $repoRoot `
    -PassThru

Write-Host "Web PID: $($webProcess.Id)"
Write-Host "Worker PID: $($workerProcess.Id)"
Write-Host "Press Ctrl+C in this window to stop both processes."

try {
    while ($true) {
        if ($webProcess.HasExited) {
            throw "Web process exited unexpectedly."
        }
        if ($workerProcess.HasExited) {
            throw "Worker process exited unexpectedly."
        }
        Start-Sleep -Seconds 5
    }
} finally {
    foreach ($proc in @($workerProcess, $webProcess)) {
        try {
            if ($proc -and -not $proc.HasExited) {
                Stop-Process -Id $proc.Id -Force
            }
        } catch {}
    }
}
