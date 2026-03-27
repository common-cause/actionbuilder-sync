# update_records Incident — 2026-03-21

## Timeline

### 2026-03-21 ~00:00–01:56 UTC — snapshot_tag_state (Civis)

First-ever production run of `snapshot_tag_state` across all campaigns.
Read current tag state from AB API for each entity and logged `add_tagging`
entries to `sync_log`. Completed successfully for 10 campaigns (New Mexico
through Wisconsin), writing 15,318 tag rows. The 13 earlier campaigns
(Arizona through Nebraska) either failed silently or were not reached.

### 2026-03-21 12:10 UTC — update_records fails (Civis)

First production run of `update_records` via `civis/update_records.sh`.

**Root cause:** `update_records` did not accept the `delay` parameter.
The CLI parsed `--delay 0.3` but the dispatch code (line 1230) only
passed `delay` to `remove_records`, `prepare_email_data`,
`prepare_phone_data`, and `snapshot_tag_state` — not `update_records`.
The function signature also lacked a `delay` parameter. Result: the
script called the AB API at full speed with no throttling.

**Failure sequence:**

| Time (UTC) | Event |
|------------|-------|
| 12:10:03 | Arizona starts: 1,425 entity groups to process |
| 12:10:10 | First `RateLimitError` on `delete_tagging` — retry in 62s |
| 12:11:18 | Second `RateLimitError` — retry in 62s |
| 12:12:23 | `SyncLogger.flush()` fails: BQ rejects entire 100-row batch |

**BQ insert error:** ~44 rows in the batch had empty `status` field
(REQUIRED NOT NULL), causing BQ to reject those rows as "invalid" and
halt the remaining ~56 rows as "stopped". The entire batch was lost.

The exact code path producing empty `status` was not identified — all
visible code paths hardcode `'ok'` or return `'ok'`/`'404'` from the
connector. Likely an edge case during the rate-limit retry storm.

**Impact:** A handful of Arizona entities (~5-10) had their tags
successfully written to AB but the corresponding sync_log rows were
lost. Rob cancelled the Civis job after seeing the errors.

### 2026-03-21 — diagnosis and fix (Claude Code session)

Investigated sync_log in BQ. Found:
- 10 campaigns logged successfully (New Mexico through Wisconsin)
- 13 campaigns missing entirely (Arizona through Nebraska)
- No orphaned `delete_tagging` rows (no split-entity problem)

Identified the missing `delay` bug and the batch-rejection risk.

**Commit `f3e035d`** — three fixes pushed to master:

1. Added `delay` parameter to `update_records()` function signature
2. Added `update_records` to the CLI dispatch list that passes `delay`
3. Added `status or 'unknown'` guard in `SyncLogger.log()` to prevent
   a single bad row from killing an entire batch
4. Promoted `SyncLogger.flush()` failure logging from WARNING to ERROR

### 2026-03-22 — identified duplicate-tagging risk

Realized the "self-healing" assumption was wrong. If update_records
re-ran without fresh state, the sequence would be:

1. Try to delete old tagging (already deleted) → 404 → treated as success
2. Add new tag value → succeeds → but value already exists from prior run
3. **Duplicate tagging created**

The `delete_tagging` 404-is-success logic (correct for idempotent deletes)
becomes dangerous when paired with a re-add, because the sync_log gap
means `current_tag_values` doesn't know the tag was already replaced.

**Decision:** Re-run `snapshot_tag_state` before `update_records` to
capture current AB state and heal the sync_log gap.

### 2026-03-23 03:40–08:40 UTC — snapshot_tag_state re-run (Civis)

Fresh snapshot across all 22 production campaigns. 58,629 `add_tagging`
rows written to sync_log, all with `status='ok'`. No errors.

### 2026-03-23 — verification

Confirmed via BQ:
- `current_tag_values` pulling from fresh snapshot timestamps (03:40+ UTC)
- `updates_needed` showing legitimate deltas only:
  - 24,122 "New Value" (mostly AN Actions on entities never tagged)
  - 9,958 "Update Value" (small numeric deltas, avg ±1 to ±16)
  - 1,352 "Clear Value"
- No signs of snapshot gaps or stale overlay data

### 2026-03-23 — update_records re-run (Civis)

Re-launched with delay fix deployed. Running.

## Lessons

1. **Test the full CLI dispatch path, not just the function.** The
   `update_records` function never had a `delay` parameter, but the
   Civis script passed `--delay 0.3` without error because argparse
   accepted it globally. The gap between "CLI accepts it" and "function
   receives it" was invisible.

2. **Batch inserts need row-level error isolation.** A single bad row
   in a 100-row batch shouldn't kill 99 good rows. The `status or 'unknown'`
   guard is a band-aid; longer-term, consider inserting failed rows
   individually or using a retry-with-subset strategy in `flush()`.

3. **sync_log is load-bearing while the AB mirror is down.** Lost
   sync_log rows aren't just an observability gap — they can cause
   duplicate taggings on re-run. Any future sync_log failures should
   be treated as blocking, not warning-level.

4. **snapshot_tag_state is the recovery tool.** When sync_log has gaps,
   re-running snapshot captures ground truth from the AB API and heals
   the overlay. This should be the standard pre-flight check before
   re-running a failed update_records.
