$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$webScript = Join-Path $PSScriptRoot "run_local_web.ps1"
$workerScript = Join-Path $PSScriptRoot "run_local_worker.ps1"

if (-not (Test-Path $webScript)) {
    throw "Missing launcher script: $webScript"
}

if (-not (Test-Path $workerScript)) {
    throw "Missing launcher script: $workerScript"
}

Write-Host "Starting local split host stack..."
Write-Host "Web launcher:    $webScript"
Write-Host "Worker launcher: $workerScript"

$webProcess = Start-Process powershell `
    -ArgumentList "-ExecutionPolicy Bypass -File `"$webScript`"" `
    -WorkingDirectory $repoRoot `
    -PassThru

Start-Sleep -Seconds 2

$workerProcess = Start-Process powershell `
    -ArgumentList "-ExecutionPolicy Bypass -File `"$workerScript`"" `
    -WorkingDirectory $repoRoot `
    -PassThru

Write-Host ""
Write-Host "Started local split host."
Write-Host "Web PID:    $($webProcess.Id)"
Write-Host "Worker PID: $($workerProcess.Id)"
Write-Host ""
Write-Host "If you want to stop them later:"
Write-Host "Stop-Process -Id $($webProcess.Id),$($workerProcess.Id)"
