# run_weekly_dedup.ps1 - weekly local agent dedup-resolution pass.
# Registered in Windows Task Scheduler as "ActionBuilder Sync - weekly dedup agent"
# (Mondays 07:23 local). Runs the weekly-dedup-resolve skill headless via claude -p;
# the skill judges new dedup_unresolved pairs and writes decisions to
# dedup_resolutions. See .claude/skills/weekly-dedup-resolve/SKILL.md (rubric) and
# KL entry 'local-scheduled-claude-agents-task-scheduler-the-pattern-for-recurring-agentic-p'.
# NOTE: keep this file pure ASCII (PS 5.1 reads BOM-less files as ANSI).

$ErrorActionPreference = 'Continue'
$proj = 'C:\Users\RobKerth\OneDrive - Common Cause Education Fund\Documents\Action Builder\ActionBuilder Sync'
$claude = "$env:USERPROFILE\.local\bin\claude.exe"

Set-Location -LiteralPath $proj
if (-not (Test-Path 'logs')) { New-Item -ItemType Directory -Path 'logs' | Out-Null }
$stamp = Get-Date -Format 'yyyy-MM-dd'
$log = Join-Path $proj "logs\weekly_dedup_$stamp.log"

"=== weekly-dedup-resolve run $(Get-Date -Format s) ===" | Out-File -FilePath $log -Encoding utf8

# Empty pipe = closed stdin (claude warns and waits 3s otherwise).
'' | & $claude -p "/weekly-dedup-resolve" 2>&1 |
    ForEach-Object { $_.ToString() } |
    Out-File -FilePath $log -Encoding utf8 -Append

"=== exit code: $LASTEXITCODE at $(Get-Date -Format s) ===" |
    Out-File -FilePath $log -Encoding utf8 -Append

# Prune run logs older than 90 days (only this job's files).
Get-ChildItem -Path (Join-Path $proj 'logs') -Filter 'weekly_dedup_*.log' |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-90) } |
    Remove-Item -Force
