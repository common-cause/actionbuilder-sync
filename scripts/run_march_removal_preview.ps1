# run_march_removal_preview.ps1 - one-shot reminder for clearing the March on
# Washington recruitment tag after the 2026-08-28 march.
#
# Registered in Windows Task Scheduler as "ActionBuilder Sync - march removal preview"
# with a ONE-TIME trigger on 2026-08-29 09:00 local.
#
# Runs the removal op in --dry-run only and emails Rob the preview plus the exact
# command to execute. It deliberately does NOT delete anything: removals in this
# project stay human-supervised (same doctrine as the weekly dedup job). The point
# is that an 8-day-out cleanup does not get forgotten, not that it runs itself.
#
# The dry-run builds a READ-ONLY AB client - it must read to resolve live tagging
# ids - so this job does touch the AB API, but only with GETs.
#
# NOTE: keep this file pure ASCII (PS 5.1 reads BOM-less files as ANSI).

$ErrorActionPreference = 'Continue'
$proj = 'C:\Users\RobKerth\OneDrive - Common Cause Education Fund\Documents\Action Builder\ActionBuilder Sync'
$python = 'C:\Users\RobKerth\AppData\Local\Programs\Python\Python312\python.exe'

Set-Location -LiteralPath $proj
if (-not (Test-Path 'logs')) { New-Item -ItemType Directory -Path 'logs' | Out-Null }
$stamp = Get-Date -Format 'yyyy-MM-dd'
$log = Join-Path $proj "logs\march_removal_preview_$stamp.log"

"=== march removal preview $(Get-Date -Format s) ===" | Out-File -FilePath $log -Encoding utf8

$out = & $python (Join-Path $proj 'scripts\sync.py') 'remove_list_taggings' '--dry-run' '--delay' '0.2' 2>&1 |
    ForEach-Object { $_.ToString() }
$code = $LASTEXITCODE

$out | Out-File -FilePath $log -Encoding utf8 -Append
"=== exit code: $code at $(Get-Date -Format s) ===" | Out-File -FilePath $log -Encoding utf8 -Append

# "deleted=N" in the summary line is the count that WOULD be deleted under --dry-run.
$pending = 0
$summary = $out | Where-Object { $_ -match 'remove_list_taggings: done\.' } | Select-Object -Last 1
if ($summary -match 'deleted=(\d+)') { $pending = [int]$Matches[1] }

$body = @()
if ($code -ne 0) {
    $subject = "[AB Sync] March removal preview FAILED - $stamp"
    $body += "The dry-run exited $code. Nothing was deleted."
    $body += "Investigate before running the real removal."
} elseif ($pending -eq 0) {
    $subject = "[AB Sync] March on Washington tag already clear - $stamp"
    $body += "Nothing pending - the March on Washington tag appears to be cleared already."
    $body += "No action needed. You can delete the scheduled task."
} else {
    $subject = "[AB Sync] Ready to clear $pending March on Washington taggings - $stamp"
    $body += "The march was 2026-08-28. $pending tagging(s) are ready to be removed."
    $body += ""
    $body += "This was a DRY RUN - nothing has been deleted."
    $body += "To execute, run from the project directory:"
    $body += ""
    $body += "    python scripts/sync.py remove_list_taggings --delay 0.3"
    $body += ""
    $body += "Removes the tag only. Campaign-26 membership and the organizer"
    $body += "connection to Carlos are left in place by design."
}
$body += ""
$body += "Full preview log: $log"
$body += "Detail: docs/march_on_washington_list.md"
$text = $body -join "`r`n"

$text | Out-File -FilePath (Join-Path $proj 'logs\MARCH_REMOVAL_DUE.txt') -Encoding utf8

# Outlook COM is the channel most likely to actually be seen. Best effort:
# a mail failure must never mask the reminder, which is already on disk.
try {
    $ol = New-Object -ComObject Outlook.Application
    $mail = $ol.CreateItem(0)
    $mail.To = 'rkerth@commoncause.org'
    $mail.Subject = $subject
    $mail.Body = $text
    $mail.Send()
    "reminder emailed via Outlook" | Out-File -FilePath $log -Encoding utf8 -Append
} catch {
    "Outlook email FAILED: $($_.Exception.Message)" | Out-File -FilePath $log -Encoding utf8 -Append
}

exit $code
