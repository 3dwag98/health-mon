@echo off
REM Thin wrapper so cmd.exe users can type `run up` instead of invoking
REM PowerShell explicitly. Bypasses the local execution policy for this call
REM only -- it does not change any machine setting.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run.ps1" %*
