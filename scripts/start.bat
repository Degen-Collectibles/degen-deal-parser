@echo off
cd /d "%~dp0.."
powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1
pause
