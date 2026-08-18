# run_sync_freshness_check.ps1 - daily dead-man's check on the nightly AB sync.
# Registered in Windows Task Scheduler as "ActionBuilder Sync - freshness check"
# (daily 09:00 local). Runs scripts/check_sync_freshness.py; on failure it writes
# logs/SYNC_ALERT.txt and emails Rob via Outlook COM.
#
# Runs LOCALLY on purpose. The thing being watched is Civis; a check hosted on Civis
# would have been just as dead as the nightly it was meant to catch (see the
# 2026-07-13 outage, unnoticed for five weeks).
#
# NOTE: keep this file pure ASCII (PS 5.1 reads BOM-less files as ANSI).

$ErrorActionPreference = 'Continue'
$proj = 'C:\Users\RobKerth\OneDrive - Common Cause Education Fund\Documents\Action Builder\ActionBuilder Sync'
$python = 'C:\Users\RobKerth\AppData\Local\Programs\Python\Python312\python.exe'

Set-Location -LiteralPath $proj
if (-not (Test-Path 'logs')) { New-Item -ItemType Directory -Path 'logs' | Out-Null }
$stamp = Get-Date -Format 'yyyy-MM-dd'
$log = Join-Path $proj "logs\sync_freshness_$stamp.log"
$alert = Join-Path $proj 'logs\SYNC_ALERT.txt'

"=== sync freshness check $(Get-Date -Format s) ===" | Out-File -FilePath $log -Encoding utf8

$out = & $python (Join-Path $proj 'scripts\check_sync_freshness.py') 2>&1 |
    ForEach-Object { $_.ToString() }
$code = $LASTEXITCODE

$out | Out-File -FilePath $log -Encoding utf8 -Append
"=== exit code: $code at $(Get-Date -Format s) ===" | Out-File -FilePath $log -Encoding utf8 -Append

if ($code -eq 0) {
    # Clear any stale alert from a previous bad day so the file always means "live problem".
    if (Test-Path $alert) { Remove-Item $alert -Force }
} else {
    $body = @()
    $body += "ActionBuilder nightly sync looks UNHEALTHY (exit $code)."
    $body += "Checked $(Get-Date -Format 'yyyy-MM-dd HH:mm')."
    $body += ""
    $body += $out
    $body += ""
    $body += "Log: $log"
    $body += "Civis workflow: https://platform.civisanalytics.com/spa/#/workflows/119217"
    $text = $body -join "`r`n"

    $text | Out-File -FilePath $alert -Encoding utf8

    # Outlook COM is the channel most likely to actually be seen. Best effort:
    # a mail failure must never mask the underlying alert, which is already on disk.
    try {
        $ol = New-Object -ComObject Outlook.Application
        $mail = $ol.CreateItem(0)
        $mail.To = 'rkerth@commoncause.org'
        $mail.Subject = "[AB Sync] Nightly looks unhealthy - $stamp"
        $mail.Body = $text
        $mail.Send()
        "alert emailed via Outlook" | Out-File -FilePath $log -Encoding utf8 -Append
    } catch {
        "Outlook email FAILED: $($_.Exception.Message)" | Out-File -FilePath $log -Encoding utf8 -Append
    }
}

# Prune run logs older than 90 days (only this job's files).
Get-ChildItem -Path (Join-Path $proj 'logs') -Filter 'sync_freshness_*.log' |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-90) } |
    Remove-Item -Force

exit $code
