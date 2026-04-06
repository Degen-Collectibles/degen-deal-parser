@echo off
cd /d "%~dp0.."
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_host.ps1
pause
